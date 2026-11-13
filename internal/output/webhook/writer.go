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
	"strconv"
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

	// maxRetryAfter bounds the wait between attempts, also one a Retry-After asks for:
	// an endpoint that is back is noticed within a minute.
	maxRetryAfter = time.Minute

	// backoffFactor doubles the wait between attempts while the endpoint is unavailable.
	backoffFactor = 2
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
	failed   bool // the previous batch failed

	unavailable bool // the batch failed MaxRetries retries and is still retried

	// The JSON array of the batch: "[" and the readings so far, without the closing "]";
	// reused by every batch, so encoding a reading allocates nothing.
	body    bytes.Buffer
	enc     *json.Encoder // writes to body
	reading int           // readings in body

	// The points of one poll share their timestamp: the last one is formatted once.
	lastTime  time.Time
	lastStamp string
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

	w := &Writer{
		settings: s,
		client:   d.Client,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "webhook").Str("output", s.Name).Logger(),
	}
	w.enc = json.NewEncoder(&w.body)
	return w, nil
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
				w.dropOpenBatch()
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

// dropOpenBatch logs the readings of a batch that is dropped because the writer was
// cancelled (at the drain deadline) before it was sent.
func (w *Writer) dropOpenBatch() {
	if w.reading > 0 {
		w.logger.Warn().Int("points", w.reading).Msg("Dropping batch, output cancelled")
	}
}

// addAll appends the points to the batch and sends it whenever it is full.
func (w *Writer) addAll(ctx context.Context, points []datasource.DataPoint) {
	for _, dp := range points {
		w.add(dp)
		if w.reading >= w.settings.MaxBatch {
			w.flush(ctx)
		}
	}
}

// add encodes dp and appends it to the batch. A value JSON cannot encode is skipped:
// it would fail the whole request, which holds the points of every queued poll.
func (w *Writer) add(dp datasource.DataPoint) {
	mark := w.body.Len()
	if w.reading == 0 {
		w.body.WriteByte('[')
	} else {
		w.body.WriteByte(',')
	}
	// Encode writes nothing when it fails, and ends a value with a newline.
	err := w.enc.Encode(reading{
		Device:    dp.DeviceName,
		Point:     dp.PointName,
		Value:     dp.JSONValue(), // NaN and ±Inf have no JSON form: null
		Unit:      dp.Unit,
		Timestamp: w.timestamp(dp.Timestamp),
	})
	if err != nil {
		w.body.Truncate(mark)
		w.logger.Warn().Str("device", dp.DeviceName).Str("point", dp.PointName).Err(err).
			Msg("Skipping point with a value JSON cannot encode")
		return
	}
	w.body.Truncate(w.body.Len() - 1)
	w.reading++
}

// timestamp returns t formatted for a reading, reusing the last result for the same time.
func (w *Writer) timestamp(t time.Time) string {
	// == instead of Equal: the same instant in another location formats differently.
	if t != w.lastTime || w.lastStamp == "" {
		w.lastTime, w.lastStamp = t, t.Format(time.RFC3339Nano)
	}
	return w.lastStamp
}

// flush sends the batch with retries; after the last retry the batch is dropped.
func (w *Writer) flush(ctx context.Context) {
	if w.reading == 0 {
		return
	}
	defer func() {
		w.body.Reset()
		w.reading = 0
	}()

	log := w.logger.With().Int("points", w.reading).Logger()
	w.body.WriteByte(']')
	// A copy for the requests: the transport may still read a body after Do returned
	// (an early answer), while body already takes the next batch.
	err := w.sendUntilDone(ctx, bytes.Clone(w.body.Bytes()))
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

// sendUntilDone sends body, the JSON array of the batch, until it is accepted, fails
// with an answer a retry cannot fix, or ctx ends. The first MaxRetries retries wait
// RetryDelay; then the endpoint counts as unavailable: that is logged once, the batch
// is kept and the wait doubles up to maxRetryAfter. Meanwhile the input fills and the
// router drops (or spools) new polls. The InfluxDB writer retries the same way.
func (w *Writer) sendUntilDone(ctx context.Context, body []byte) error {
	for attempt := 1; ; attempt++ {
		err := w.send(ctx, body)
		if err == nil {
			w.available(attempt)
			return nil
		}
		if !retryable(err) {
			return fmt.Errorf("not retried: %w", err)
		}
		delay := w.retryDelay(attempt, err)
		w.logRetry(attempt, delay, err)
		if !clock.Sleep(ctx, w.clock, delay) {
			return fmt.Errorf("cancelled while retrying: %w", err)
		}
	}
}

// logRetry logs a failed attempt: a warning per retry at RetryDelay, one error when
// the endpoint counts as unavailable, then debug until the batch is accepted.
func (w *Writer) logRetry(attempt int, delay time.Duration, err error) {
	event := w.logger.Debug()
	msg := "Retrying webhook request"
	switch {
	case attempt <= w.settings.MaxRetries:
		event = w.logger.Warn()
	case !w.unavailable:
		w.unavailable = true
		event = w.logger.Error()
		msg = "Webhook unavailable, keeping the batch and retrying until it is accepted"
	}
	event.Int("attempt", attempt+1).Stringer("retry_in", delay).Err(err).Msg(msg)
}

// available logs at info when the endpoint is back after it counted as unavailable.
func (w *Writer) available(attempts int) {
	if w.unavailable {
		w.unavailable = false
		w.logger.Info().Int("attempts", attempts).Msg("Webhook available again, batch sent")
	}
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
	setHeaders(req, w.settings.Headers)

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
		return &statusError{
			code:       resp.StatusCode,
			body:       strings.TrimSpace(string(msg)),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), w.clock.Now()),
		}
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
	code       int
	body       string        // start of the response body
	retryAfter time.Duration // the Retry-After of the response, 0 without one
}

func (e *statusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("HTTP status %d", e.code)
	}
	return fmt.Sprintf("HTTP status %d: %s", e.code, e.body)
}

// retryDelay returns the wait before the next attempt: RetryDelay for the first
// MaxRetries retries, then doubled per attempt up to maxRetryAfter; the Retry-After of
// the answer (at most maxRetryAfter) wins if it is longer: retrying earlier only adds
// load to a server that is busy.
func (w *Writer) retryDelay(attempt int, err error) time.Duration {
	delay := w.settings.RetryDelay
	for i := w.settings.MaxRetries; i < attempt && delay < maxRetryAfter; i++ {
		delay *= backoffFactor
	}
	delay = max(w.settings.RetryDelay, min(delay, maxRetryAfter))
	var statusErr *statusError
	if !errors.As(err, &statusErr) {
		return delay
	}
	return max(delay, statusErr.retryAfter)
}

// parseRetryAfter returns the wait of a Retry-After header (seconds or an HTTP date),
// at most maxRetryAfter; 0 when it is missing or invalid.
func parseRetryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		// Capped before the conversion, which would overflow for huge values.
		return time.Duration(min(max(seconds, 0), int(maxRetryAfter/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return min(max(at.Sub(now), 0), maxRetryAfter)
	}
	return 0
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

// setHeaders sets the configured headers on req. Go sends req.Host, not a Host header,
// so a configured Host (a virtual host behind an IP address) is set there.
func setHeaders(req *http.Request, headers map[string]string) {
	for k, v := range headers {
		if http.CanonicalHeaderKey(k) == "Host" {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
}
