// Package http provides HTTP reading functionality for REST APIs.
package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
)

// HttpReader reads data from an HTTP API endpoint.
type HttpReader struct {
	config config.Device
	logger zerolog.Logger
	client *http.Client
}

// NewHttpReader creates a new HttpReader.
func NewHttpReader(deviceConfig config.Device, logger *zerolog.Logger) (*HttpReader, error) {
	httpConfig := deviceConfig.DeviceSpecific.Http

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: httpConfig.Insecure,
		},
	}

	r := &HttpReader{
		config: deviceConfig,
		logger: logger.With().Str("datasource", "http").Str("device", deviceConfig.Name).Logger(),
		client: &http.Client{
			Transport: transport,
			Timeout:   deviceConfig.Timeout,
		},
	}

	return r, nil
}

// Name returns the device name.
func (r *HttpReader) Name() string {
	return r.config.Name
}

// Validate validates the HTTP reader configuration.
func (r *HttpReader) Validate() error {
	// Configuration was already validated when creating the reader
	return nil
}

// Start starts the polling loop and returns channels for data, done, and errors.
func (r *HttpReader) Start(ctx context.Context) (<-chan datasource.DataPoint, <-chan struct{}, <-chan error) {
	dataCh := make(chan datasource.DataPoint)
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)

	go r.pollLoop(ctx, dataCh, doneCh, errCh)

	return dataCh, doneCh, errCh
}

// pollLoop runs the main polling loop for the HTTP device.
func (r *HttpReader) pollLoop(
	ctx context.Context,
	dataCh chan<- datasource.DataPoint,
	doneCh chan<- struct{},
	errCh chan<- error,
) {
	defer close(doneCh)

	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.logger.Debug().Msg("Starting HTTP poll")

			// Read all points (timestamp captured inside readAllPoints after response)
			points, err := r.readAllPoints(ctx)
			if err != nil {
				// Check if this is a context cancellation error (expected during shutdown)
				if ctx.Err() != nil {
					r.logger.Debug().Err(err).Msg("HTTP poll cancelled during shutdown")
				} else {
					r.logger.Error().Err(err).Msg("Failed to read HTTP points")
				}
				continue
			}

			r.logger.Debug().Int("count", len(points)).Msg("HTTP poll completed")

			// Send each point to channel
			for _, dp := range points {
				dataCh <- dp
			}
		}
	}
}

// readAllPoints makes an HTTP request and parses all configured points.
// Timestamp is captured when the HTTP response is received.
func (r *HttpReader) readAllPoints(ctx context.Context) ([]datasource.DataPoint, error) {
	// Make the HTTP request
	req, err := r.createRequest()
	if err != nil {
		return nil, err
	}

	resp, err := r.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Capture accurate timestamp when response was received
	// Use this instead of the poll start time for better accuracy
	// Use UTC to ensure consistency with InfluxDB expectations
	actualTimestamp := time.Now().UTC()

	// Parse points - with parallelism if configured > 1
	// All points will use actualTimestamp (when response was received)
	if r.config.Parallelism > 1 {
		return r.parsePointsParallel(body, actualTimestamp)
	}
	return r.parsePointsSequential(body, actualTimestamp)
}

// createRequest creates an HTTP request based on the configuration.
func (r *HttpReader) createRequest() (*http.Request, error) {
	httpConfig := r.config.DeviceSpecific.Http
	method := strings.ToUpper(httpConfig.Method)
	if method == "" {
		method = "GET"
	}

	var bodyReader io.Reader
	if method == "POST" && httpConfig.Body != "" {
		bodyReader = strings.NewReader(httpConfig.Body)
	}

	req, err := http.NewRequest(method, httpConfig.Address, bodyReader)
	if err != nil {
		return nil, err
	}

	// Add headers
	for k, v := range httpConfig.Headers {
		req.Header.Set(k, v)
	}

	// Set content type for POST
	if method == "POST" && bodyReader != nil {
		if _, ok := req.Header["Content-Type"]; !ok {
			req.Header.Set("Content-Type", "application/json")
		}
	}

	return req, nil
}

// parsePointsSequential parses points sequentially from the response body.
func (r *HttpReader) parsePointsSequential(body []byte, timestamp time.Time) ([]datasource.DataPoint, error) {
	var results []datasource.DataPoint

	for _, point := range r.config.Points {
		value, err := r.extractValue(body, point)
		if err != nil {
			r.logger.Warn().
				Str("point", point.Name).
				Err(err).
				Msg("Failed to extract value")
			continue
		}

		results = append(results, datasource.DataPoint{
			DeviceName: r.config.Name,
			PointName:  point.Name,
			Value:      value,
			Timestamp:  timestamp,
			Unit:       point.Unit,
		})
	}

	return results, nil
}

// parsePointsParallel parses points in parallel from the response body.
func (r *HttpReader) parsePointsParallel(body []byte, timestamp time.Time) ([]datasource.DataPoint, error) {
	var results []datasource.DataPoint
	var mu sync.Mutex
	sem := make(chan struct{}, r.config.Parallelism)

	var wg sync.WaitGroup

	for _, point := range r.config.Points {
		wg.Add(1)
		go func(p config.Point) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			value, err := r.extractValue(body, p)
			if err != nil {
				r.logger.Warn().
					Str("point", p.Name).
					Err(err).
					Msg("Failed to extract value")
				return
			}

			mu.Lock()
			results = append(results, datasource.DataPoint{
				DeviceName: r.config.Name,
				PointName:  p.Name,
				Value:      value,
				Timestamp:  timestamp,
				Unit:       p.Unit,
			})
			mu.Unlock()
		}(point)
	}

	wg.Wait()
	return results, nil
}

// extractValue extracts a value from the response body based on the point configuration.
func (r *HttpReader) extractValue(body []byte, point config.Point) (interface{}, error) {
	httpConfig := r.config.DeviceSpecific.Http

	switch httpConfig.ResponseType {
	case "json":
		return r.extractJSONValue(body, point)
	case "text":
		// For text, just use the whole body
		return string(body), nil
	case "xml":
		return nil, fmt.Errorf("XML parsing not implemented")
	default:
		return nil, fmt.Errorf("unknown response type: %s", httpConfig.ResponseType)
	}
}

// extractJSONValue extracts a value from JSON using JSONPath.
func (r *HttpReader) extractJSONValue(body []byte, point config.Point) (interface{}, error) {
	if point.JsonPath == "" {
		return nil, fmt.Errorf("json_path required for JSON response")
	}

	result := gjson.GetBytes(body, point.JsonPath)
	if !result.Exists() {
		return nil, fmt.Errorf("JSONPath %s not found", point.JsonPath)
	}

	return r.convertJSONResult(result, point.Type)
}

// convertJSONResult converts a gjson.Result to the appropriate type.
func (r *HttpReader) convertJSONResult(result gjson.Result, targetType string) (interface{}, error) {
	switch targetType {
	case "float32", "float64":
		return result.Float(), nil
	case "int16", "int32", "int64":
		return int64(result.Int()), nil
	case "uint16", "uint32", "uint64":
		return uint64(result.Uint()), nil
	case "bool":
		return result.Bool(), nil
	case "string":
		return result.String(), nil
	default:
		return r.autoDetectJSONType(result)
	}
}

// autoDetectJSONType auto-detects the type from the JSON value.
func (r *HttpReader) autoDetectJSONType(result gjson.Result) (interface{}, error) {
	if result.IsBool() {
		return result.Bool(), nil
	}
	if result.Type == gjson.Number {
		return result.Float(), nil
	}
	return result.Value(), nil
}

// MarshalJSON implements json.Marshaler for HttpReader (optional, not strictly needed).
func (r *HttpReader) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name string `json:"name"`
	}{Name: r.config.Name})
}
