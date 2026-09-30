// Package http provides HTTP reading functionality for REST APIs.
package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
)

// Backoff after failed polls: starts at initialBackoff and doubles up to maxBackoff.
const (
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

// Reader reads data from an HTTP API endpoint.
type Reader struct {
	config      config.Device
	logger      zerolog.Logger
	client      *http.Client
	failCount   int
	lastError   error
	backoffWait time.Duration
}

// New creates a new Reader.
func New(deviceConfig config.Device, logger *zerolog.Logger) (*Reader, error) {
	httpConfig := deviceConfig.DeviceSpecific.HTTP

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: httpConfig.Insecure,
		},
	}

	r := &Reader{
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
func (r *Reader) Name() string {
	return r.config.Name
}

// shouldReconnect reports whether err is a transport error that warrants a new client.
// HTTP status and parse errors do not; every error from the transport (a *url.Error,
// which is a net.Error) and truncated bodies do.
func (r *Reader) shouldReconnect(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

// applyBackoff applies exponential backoff to the wait duration.
func (r *Reader) applyBackoff() {
	r.backoffWait *= backoffFactor
	if r.backoffWait == 0 {
		r.backoffWait = initialBackoff
	}
	if r.backoffWait > maxBackoff {
		r.backoffWait = maxBackoff
	}
}

// resetBackoff resets the backoff state.
func (r *Reader) resetBackoff() {
	r.failCount = 0
	r.backoffWait = 0
	r.lastError = nil
}

// reconnect recreates the HTTP client for reconnection.
func (r *Reader) reconnect() error {
	r.logger.Warn().Msg("Attempting to reconnect HTTP client")

	httpConfig := r.config.DeviceSpecific.HTTP

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: httpConfig.Insecure,
		},
	}

	// Close existing client
	if r.client != nil {
		r.client.CloseIdleConnections()
	}

	// Create new client
	r.client = &http.Client{
		Transport: transport,
		Timeout:   r.config.Timeout,
	}

	r.logger.Info().Msg("HTTP client reconnected successfully")
	return nil
}

// handleReconnect attempts to reconnect the HTTP client.
func (r *Reader) handleReconnect() {
	if reconnectErr := r.reconnect(); reconnectErr != nil {
		r.logger.Error().Err(reconnectErr).Msg("HTTP reconnection failed")
	} else {
		r.resetBackoff()
		r.logger.Info().Msg("HTTP reconnected successfully")
	}
}

// handlePollError handles an error from a poll attempt.
func (r *Reader) handlePollError(err error) {
	r.failCount++
	r.lastError = err

	r.logger.Error().
		Err(err).
		Int("failure_count", r.failCount).
		Msg("Failed to read HTTP points")

	// Apply exponential backoff
	r.applyBackoff()

	// Try to reconnect if connection-related error
	if r.shouldReconnect(err) {
		r.handleReconnect()
	}
}

// Start starts the polling loop and returns channels for data, done, and errors.
func (r *Reader) Start(
	ctx context.Context,
) (<-chan datasource.DataPoint, <-chan struct{}, <-chan error) {
	dataCh := make(chan datasource.DataPoint)
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)

	go r.pollLoop(ctx, dataCh, doneCh, errCh)

	return dataCh, doneCh, errCh
}

// pollLoop runs the main polling loop for the HTTP device.
func (r *Reader) pollLoop(
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
			r.pollOnce(ctx, dataCh)
		}
	}
}

// pollOnce runs one poll: wait out the backoff, read all points and send them.
func (r *Reader) pollOnce(ctx context.Context, dataCh chan<- datasource.DataPoint) {
	if r.backoffWait > 0 {
		r.logger.Warn().Dur("wait", r.backoffWait).Msg("Waiting before retry after a failure")
		time.Sleep(r.backoffWait)
	}

	r.logger.Debug().Msg("Starting HTTP poll")

	// Timestamp is captured inside readAllPoints when the response arrives.
	points, err := r.readAllPoints(ctx)
	if err != nil {
		if ctx.Err() != nil {
			r.logger.Debug().Err(err).Msg("HTTP poll cancelled during shutdown")
		} else {
			r.handlePollError(err)
		}
		return
	}

	r.resetBackoff()
	r.logger.Debug().Int("count", len(points)).Msg("HTTP poll completed")

	for _, dp := range points {
		dataCh <- dp
	}
}

// readAllPoints makes an HTTP request and parses all configured points.
// Timestamp is captured when the HTTP response is received.
func (r *Reader) readAllPoints(ctx context.Context) ([]datasource.DataPoint, error) {
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
func (r *Reader) createRequest() (*http.Request, error) {
	httpConfig := r.config.DeviceSpecific.HTTP
	method := requestMethod(httpConfig.Method)

	var bodyReader io.Reader
	hasBody := method == http.MethodPost && httpConfig.Body != ""
	if hasBody {
		bodyReader = strings.NewReader(httpConfig.Body)
	}

	req, err := http.NewRequest(method, httpConfig.Address, bodyReader)
	if err != nil {
		return nil, err
	}

	for k, v := range httpConfig.Headers {
		req.Header.Set(k, v)
	}

	if hasBody && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

// requestMethod returns the upper-case HTTP method, GET when none is configured.
func requestMethod(method string) string {
	if method == "" {
		return http.MethodGet
	}
	return strings.ToUpper(method)
}

// parsePointsSequential parses points sequentially from the response body.
func (r *Reader) parsePointsSequential(
	body []byte,
	timestamp time.Time,
) ([]datasource.DataPoint, error) {
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
func (r *Reader) parsePointsParallel(
	body []byte,
	timestamp time.Time,
) ([]datasource.DataPoint, error) {
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
func (r *Reader) extractValue(body []byte, point config.Point) (interface{}, error) {
	httpConfig := r.config.DeviceSpecific.HTTP

	switch httpConfig.ResponseType {
	case "json":
		return r.extractJSONValue(body, point)
	case "text":
		// For text, just use the whole body
		return string(body), nil
	case "xml":
		return nil, errors.New("XML parsing not implemented")
	default:
		return nil, fmt.Errorf("unknown response type: %s", httpConfig.ResponseType)
	}
}

// extractJSONValue extracts a value from JSON using JSONPath.
func (r *Reader) extractJSONValue(body []byte, point config.Point) (interface{}, error) {
	if point.JSONPath == "" {
		return nil, errors.New("json_path required for JSON response")
	}

	result := gjson.GetBytes(body, point.JSONPath)
	if !result.Exists() {
		return nil, fmt.Errorf("JSONPath %s not found", point.JSONPath)
	}

	return r.convertJSONResult(result, point.Type)
}

// convertJSONResult converts a gjson.Result to the appropriate type.
func (r *Reader) convertJSONResult(
	result gjson.Result,
	targetType string,
) (interface{}, error) {
	switch targetType {
	case "float32", "float64":
		return result.Float(), nil
	case "int16", "int32", "int64":
		return result.Int(), nil
	case "uint16", "uint32", "uint64":
		return result.Uint(), nil
	case "bool":
		return result.Bool(), nil
	case "string":
		return result.String(), nil
	default:
		return r.autoDetectJSONType(result)
	}
}

// autoDetectJSONType auto-detects the type from the JSON value.
func (r *Reader) autoDetectJSONType(result gjson.Result) (interface{}, error) {
	if result.IsBool() {
		return result.Bool(), nil
	}
	if result.Type == gjson.Number {
		return result.Float(), nil
	}
	return result.Value(), nil
}

// MarshalJSON implements json.Marshaler for Reader (optional, not strictly needed).
func (r *Reader) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name string `json:"name"`
	}{Name: r.config.Name})
}
