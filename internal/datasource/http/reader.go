// Package http reads values from HTTP APIs (JSON via gjson paths, or the whole body as
// text).
package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// Backoff after failed polls: starts at initialBackoff and doubles up to maxBackoff.
const (
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

var (
	// ErrMissingDependency is returned by New when a required dependency is missing.
	ErrMissingDependency = errors.New("http: missing dependency")

	// ErrInvalidSettings is returned by New for settings the reader cannot work with.
	ErrInvalidSettings = errors.New("http: invalid settings")
)

// Settings configures a Reader.
type Settings struct {
	Name         string
	PollInterval time.Duration
	Address      string
	Method       string // GET or POST
	Headers      map[string]string
	Body         string // POST only
	ResponseType string // json or text
	Points       []Point
}

// Point is one value taken from the response.
type Point struct {
	Name     string
	JSONPath string // json only
	Type     string // optional conversion: float*, int*, uint*, bool, string
	Scale    float64
	Offset   float64
	Unit     string
}

// Deps are the dependencies of a Reader; all are required.
type Deps struct {
	Settings Settings
	Client   Client
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Reader polls one HTTP endpoint.
type Reader struct {
	settings  Settings
	client    Client
	clock     clock.Clock
	logger    zerolog.Logger
	failCount int
	backoff   time.Duration

	failedPoints map[string]bool // points whose last extraction failed
}

// New creates a Reader. It checks that a request can be built from the settings but
// does not send one.
func New(d Deps) (*Reader, error) {
	var missing []string
	if d.Client == nil {
		missing = append(missing, "Client")
	}
	if d.Clock == nil {
		missing = append(missing, "Clock")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingDependency, strings.Join(missing, ", "))
	}

	r := &Reader{
		settings: d.Settings,
		client:   d.Client,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "http").Str("device", d.Settings.Name).Logger(),

		failedPoints: make(map[string]bool),
	}
	if d.Settings.PollInterval <= 0 || len(d.Settings.Points) == 0 {
		return nil, fmt.Errorf("%w: poll interval and points are required", ErrInvalidSettings)
	}
	if _, err := r.newRequest(context.Background()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSettings, err)
	}
	return r, nil
}

// Name returns the device name.
func (r *Reader) Name() string {
	return r.settings.Name
}

// Start starts the poll loop; the data channel is closed when the loop ends after ctx
// is cancelled.
func (r *Reader) Start(ctx context.Context) <-chan []datasource.DataPoint {
	dataCh := make(chan []datasource.DataPoint)
	go r.pollLoop(ctx, dataCh)
	return dataCh
}

// pollLoop polls on every tick until ctx is cancelled.
func (r *Reader) pollLoop(ctx context.Context, dataCh chan<- []datasource.DataPoint) {
	defer close(dataCh)

	ticker := r.clock.NewTicker(r.settings.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			r.pollOnce(ctx, dataCh)
		}
	}
}

// pollOnce waits out the backoff, requests the endpoint and sends the parsed points.
func (r *Reader) pollOnce(ctx context.Context, dataCh chan<- []datasource.DataPoint) {
	if r.backoff > 0 {
		r.logger.Warn().Dur("wait", r.backoff).Msg("Waiting before retry after a failure")
		if !clock.Sleep(ctx, r.clock, r.backoff) {
			return
		}
	}

	r.logger.Debug().Msg("Starting HTTP poll")
	points, err := r.poll(ctx)
	if err != nil {
		if ctx.Err() != nil {
			r.logger.Debug().Err(err).Msg("HTTP poll cancelled during shutdown")
			return
		}
		r.handlePollError(err)
		return
	}
	r.handlePollSuccess(len(points))
	send(dataCh, points)
}

// send delivers the points of a finished poll as one batch, also during shutdown: the
// router reads until the reader closes the channel, so data that was already read is
// never lost. A poll without points sends nothing.
func send(dataCh chan<- []datasource.DataPoint, points []datasource.DataPoint) {
	if len(points) > 0 {
		dataCh <- points
	}
}

// poll sends the request and parses all points; they share the receive timestamp.
func (r *Reader) poll(ctx context.Context) ([]datasource.DataPoint, error) {
	req, err := r.newRequest(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return r.parsePoints(body, r.clock.Now().UTC()), nil
}

// newRequest builds the configured request.
func (r *Reader) newRequest(ctx context.Context) (*http.Request, error) {
	method := strings.ToUpper(r.settings.Method)
	var body io.Reader
	hasBody := method == http.MethodPost && r.settings.Body != ""
	if hasBody {
		body = strings.NewReader(r.settings.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, r.settings.Address, body)
	if err != nil {
		return nil, err
	}
	for k, v := range r.settings.Headers {
		req.Header.Set(k, v)
	}
	if hasBody && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// parsePoints extracts every point; a point that fails is logged and skipped.
func (r *Reader) parsePoints(body []byte, received time.Time) []datasource.DataPoint {
	results := make([]datasource.DataPoint, 0, len(r.settings.Points))
	for _, p := range r.settings.Points {
		value, err := r.extractValue(body, p)
		if err != nil {
			r.pointFailed(p.Name, err)
			continue
		}
		r.pointFound(p.Name)
		results = append(results, datasource.DataPoint{
			DeviceName: r.settings.Name,
			PointName:  p.Name,
			Value:      value,
			Timestamp:  received,
			Unit:       p.Unit,
		})
	}
	return results
}

// pointFailed warns once per point until it can be read again; repeats go to debug, so
// a json_path that the device never returns does not flood the log.
func (r *Reader) pointFailed(name string, err error) {
	if r.failedPoints[name] {
		r.logger.Debug().Str("point", name).Err(err).Msg("Failed to extract value")
		return
	}
	r.failedPoints[name] = true
	r.logger.Warn().Str("point", name).Err(err).
		Msg("Failed to extract value; further failures of this point are logged at debug")
}

// pointFound logs at info when a point that failed before can be read again.
func (r *Reader) pointFound(name string) {
	if r.failedPoints[name] {
		delete(r.failedPoints, name)
		r.logger.Info().Str("point", name).Msg("Value can be extracted again")
	}
}

// handlePollError logs the error, grows the backoff and drops idle connections after a
// transport error.
func (r *Reader) handlePollError(err error) {
	r.failCount++
	r.logger.Error().Err(err).Int("failure_count", r.failCount).Msg("HTTP poll failed")

	r.backoff = min(max(r.backoff*backoffFactor, initialBackoff), maxBackoff)

	if isTransportError(err) {
		r.client.CloseIdleConnections()
	}
}

// handlePollSuccess resets the backoff and logs a recovery at info.
func (r *Reader) handlePollSuccess(count int) {
	if r.failCount > 0 {
		r.logger.Info().Int("count", count).Msg("HTTP poll recovered from previous error")
	} else {
		r.logger.Debug().Int("count", count).Msg("HTTP poll completed")
	}
	r.failCount = 0
	r.backoff = 0
}

// isTransportError reports whether err came from the connection: every error of
// http.Client.Do (a *url.Error, which is a net.Error) and truncated bodies. HTTP status
// and parse errors do not.
func isTransportError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
