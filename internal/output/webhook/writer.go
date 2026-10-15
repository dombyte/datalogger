// Package webhook pushes data points as a JSON array to an HTTP endpoint.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// Defaults for settings that are empty or 0.
const (
	DefaultMethod     = http.MethodPost
	DefaultMaxBatch   = 1000
	DefaultMaxRetries = 3
	DefaultRetryDelay = 1 * time.Second
)

const (
	// maxDrainSize is read from every response so the transport can reuse the
	// connection; the rest of a larger body is discarded with the connection.
	maxDrainSize = 64 << 10

	// maxErrorBody is the part of an error response shown in the error message.
	maxErrorBody = 256
)

var (
	// ErrMissingDependency is returned by New when a required dependency is missing.
	ErrMissingDependency = errors.New("webhook: missing dependency")

	// ErrInvalidSettings is returned by New for settings no request can be sent with.
	ErrInvalidSettings = errors.New("webhook: invalid settings")
)

// Settings configures a Writer; zero values use the defaults.
type Settings struct {
	Name       string
	URL        string
	Method     string // POST or PUT
	Headers    map[string]string
	MaxBatch   int // readings per request
	MaxRetries int
	RetryDelay time.Duration
}

// Deps are the dependencies of a Writer; all are required.
type Deps struct {
	Settings Settings
	Client   Client
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Writer sends the points of each poll as one request; polls that queue up behind a
// slow endpoint are combined, up to MaxBatch readings per request.
type Writer struct {
	settings Settings
	client   Client
	clock    clock.Clock
	logger   zerolog.Logger
	batch    [][]byte // JSON-encoded readings
	failed   bool     // the previous batch failed
}

// reading is one element of the JSON array; the fields match the API and MQTT payloads.
type reading struct {
	Device    string `json:"device"`
	Point     string `json:"point"`
	Value     any    `json:"value"`
	Unit      string `json:"unit"`
	Timestamp string `json:"timestamp"`
}

// New creates a Writer. It checks the URL and method but sends no request.
func New(d Deps) (*Writer, error) {
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

	s := d.Settings
	s.Method = strings.ToUpper(s.Method)
	if s.Method == "" {
		s.Method = DefaultMethod
	}
	s.MaxBatch = orDefault(s.MaxBatch, DefaultMaxBatch)
	s.MaxRetries = orDefault(s.MaxRetries, DefaultMaxRetries)
	s.RetryDelay = orDefault(s.RetryDelay, DefaultRetryDelay)
	if err := checkSettings(s); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSettings, err)
	}

	return &Writer{
		settings: s,
		client:   d.Client,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "webhook").Str("output", s.Name).Logger(),
		batch:    make([][]byte, 0, s.MaxBatch),
	}, nil
}

// checkSettings checks the method and that the URL is an absolute http(s) URL; the
// client would only reject anything else when the first batch is sent.
func checkSettings(s Settings) error {
	if s.Method != http.MethodPost && s.Method != http.MethodPut {
		return fmt.Errorf("method must be POST or PUT, got %q", s.Method)
	}
	u, err := url.Parse(s.URL)
	if err != nil {
		return redactURLError(err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("url %s must be http:// or https:// with a host", redactURL(s.URL))
	}
	return nil
}

// orDefault returns def for values <= 0.
func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.settings.Name
}

// Start sends points until input is closed (or ctx is cancelled), then closes the
// returned channel. A poll is sent at once unless more polls are queued; those are
// added to the same request.
func (w *Writer) Start(ctx context.Context, input <-chan []datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case points, ok := <-input:
				if !ok {
					return // the last poll was sent: nothing was queued behind it
				}
				w.addAll(ctx, points)
				if len(input) == 0 {
					w.flush(ctx)
				}
			}
		}
	}()
	return done
}

// addAll appends the points to the batch and sends it whenever it is full.
func (w *Writer) addAll(ctx context.Context, points []datasource.DataPoint) {
	for _, dp := range points {
		w.add(dp)
		if len(w.batch) >= w.settings.MaxBatch {
			w.flush(ctx)
		}
	}
}

// add encodes dp and appends it to the batch. A value JSON cannot encode is skipped:
// it would fail the whole request, which holds the points of every queued poll.
func (w *Writer) add(dp datasource.DataPoint) {
	b, err := json.Marshal(reading{
		Device:    dp.DeviceName,
		Point:     dp.PointName,
		Value:     dp.JSONValue(), // NaN and ±Inf have no JSON form: null
		Unit:      dp.Unit,
		Timestamp: dp.Timestamp.Format(time.RFC3339Nano),
	})
	if err != nil {
		w.logger.Warn().Str("device", dp.DeviceName).Str("point", dp.PointName).Err(err).
			Msg("Skipping point with a value JSON cannot encode")
		return
	}
	w.batch = append(w.batch, b)
}

// flush sends the batch with retries; after the last retry the batch is dropped.
func (w *Writer) flush(ctx context.Context) {
	if len(w.batch) == 0 {
		return
	}
	defer func() { w.batch = w.batch[:0] }()

	log := w.logger.With().Int("points", len(w.batch)).Logger()
	err := w.sendWithRetry(ctx)
	switch {
	case err != nil:
		log.Error().Err(err).Msg("Dropping batch after failed requests")
		w.failed = true
	case w.failed:
		log.Info().Msg("Sent batch to webhook (recovered from previous error)")
		w.failed = false
	default:
		log.Debug().Msg("Sent batch to webhook")
	}
}

// sendWithRetry sends the batch as a JSON array 1 + MaxRetries times, RetryDelay apart.
func (w *Writer) sendWithRetry(ctx context.Context) error {
	body := append([]byte("["), bytes.Join(w.batch, []byte(","))...)
	body = append(body, ']')
	var err error
	for attempt := range w.settings.MaxRetries + 1 {
		if attempt > 0 {
			w.logger.Warn().Int("attempt", attempt+1).Err(err).Msg("Retrying webhook request")
			if !clock.Sleep(ctx, w.clock, w.settings.RetryDelay) {
				return fmt.Errorf("cancelled while retrying: %w", err)
			}
		}
		if err = w.send(ctx, body); err == nil {
			return nil
		}
		if !retryable(err) {
			return fmt.Errorf("not retried: %w", err)
		}
	}
	return fmt.Errorf("after %d retries: %w", w.settings.MaxRetries, err)
}

// send sends one request with body; any 2xx status is a success.
func (w *Writer) send(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, w.settings.Method, w.settings.URL,
		bytes.NewReader(body))
	if err != nil {
		//nolint:forbidigo // invariant: New checked the method and URL
		panic(fmt.Sprintf("webhook: build request: %v", err))
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range w.settings.Headers {
		req.Header.Set(k, v)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return redactURLError(err)
	}
	defer resp.Body.Close()

	rest := io.LimitReader(resp.Body, maxDrainSize)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		// The start of the body usually says why (bad token, rejected payload). A
		// failed read only shortens the message.
		msg, _ := io.ReadAll(io.LimitReader(rest, maxErrorBody)) //nolint:errcheck // see above
		discard(rest)
		return &statusError{code: resp.StatusCode, body: strings.TrimSpace(string(msg))}
	}
	discard(rest)
	return nil
}

// discard reads r to the end so the transport can reuse the connection; if that fails
// it opens a new one, so there is nothing to handle.
func discard(r io.Reader) {
	_, _ = io.Copy(io.Discard, r) //nolint:errcheck // only lets the connection be reused
}

// statusError is a response outside 2xx.
type statusError struct {
	code int
	body string // start of the response body
}

func (e *statusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("HTTP status %d", e.code)
	}
	return fmt.Sprintf("HTTP status %d: %s", e.code, e.body)
}

// retryable reports whether a request may succeed when repeated: network errors,
// server errors (5xx) and 429. Other statuses (bad token, rejected payload) fail the
// same way again.
func retryable(err error) bool {
	var statusErr *statusError
	if !errors.As(err, &statusErr) {
		return true
	}
	return statusErr.code >= http.StatusInternalServerError ||
		statusErr.code == http.StatusTooManyRequests
}

// redactURLError returns err with the URL of a *url.Error reduced to scheme, host and
// path. net/http only hides the password; tokens in the query (?token=…) would
// otherwise be logged with every failed request.
func redactURLError(err error) error {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	redacted := *urlErr
	redacted.URL = redactURL(urlErr.URL)
	return &redacted
}

// redactURL returns scheme, host and path of raw; a URL that cannot be parsed is not
// shown at all.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid url>"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}
