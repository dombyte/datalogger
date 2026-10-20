package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output/webhook"
	"github.com/dombyte/datalogger/internal/output/webhook/mocks"
)

var start = time.Date(2026, 10, 15, 8, 23, 20, 123000000, time.UTC)

const hookURL = "https://ingest.example.com/hook?token=secret"

type harness struct {
	t        *testing.T
	clock    *clocktest.Fake
	client   *mocks.MockClient
	input    chan []datasource.DataPoint
	done     <-chan error
	requests chan request
	logs     *bytes.Buffer // read only after stop: the writer goroutine has ended
	cancel   context.CancelFunc
}

// request is what the client received.
type request struct {
	method string
	url    string
	header http.Header
	body   []map[string]any
	raw    string
}

// newHarness starts a writer with the queued polls already in its input.
func newHarness(t *testing.T, s webhook.Settings, queued ...[]datasource.DataPoint) *harness {
	t.Helper()
	h := &harness{
		t:        t,
		clock:    clocktest.NewFake(start),
		client:   mocks.NewMockClient(t),
		input:    make(chan []datasource.DataPoint, 10),
		requests: make(chan request, 10),
		logs:     &bytes.Buffer{},
	}
	if s.URL == "" {
		s.URL = hookURL
	}
	w, err := webhook.New(webhook.Deps{
		Settings: s, Client: h.client, Clock: h.clock, Log: zerolog.New(h.logs),
	})
	require.NoError(t, err)
	assert.Equal(t, s.Name, w.Name())
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(cancel)
	for _, poll := range queued {
		h.input <- poll
	}
	h.done = w.Start(ctx, h.input)
	return h
}

// silent is a status code answered with an empty body.
type silent int

// busy is a status code answered with a Retry-After header.
type busy struct {
	code       int
	retryAfter string
}

// expect answers the next requests in order: an int is a status code with a short
// body, a silent one without body, an error a transport error.
func (h *harness) expect(answers ...any) {
	for _, answer := range answers {
		h.client.EXPECT().Do(mock.Anything).RunAndReturn(
			func(req *http.Request) (*http.Response, error) {
				h.requests <- h.record(req)
				if err, ok := answer.(error); ok {
					return nil, err
				}
				body, header := " bad token \n", http.Header{}
				code, ok := answer.(int)
				if s, isSilent := answer.(silent); !ok && isSilent {
					code, body = int(s), ""
				}
				if b, isBusy := answer.(busy); isBusy {
					code = b.code
					header.Set("Retry-After", b.retryAfter)
				}
				return &http.Response{
					StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			}).Once()
	}
}

func (h *harness) record(req *http.Request) request {
	body, err := io.ReadAll(req.Body)
	require.NoError(h.t, err)
	var readings []map[string]any
	require.NoError(h.t, json.Unmarshal(body, &readings))
	return request{
		method: req.Method, url: req.URL.String(), header: req.Header, body: readings,
		raw: string(body),
	}
}

func (h *harness) request() request {
	h.t.Helper()
	select {
	case r := <-h.requests:
		return r
	case <-time.After(time.Second):
		h.t.Fatal("no request")
		return request{}
	}
}

// advance waits for the retry delay and lets it pass.
func (h *harness) advance(d time.Duration) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.clock.Waiters() >= 1 },
		time.Second, time.Millisecond)
	h.clock.Advance(d)
}

// stop closes the input and waits for the writer to finish.
func (h *harness) stop() {
	close(h.input)
	for err := range h.done {
		require.NoError(h.t, err)
	}
}

func point(name string, value any) datasource.DataPoint {
	return datasource.DataPoint{
		DeviceName: "meter", PointName: name, Value: value, Timestamp: start, Unit: "W",
	}
}

// names returns the point names of a request body.
func names(r request) []string {
	out := make([]string, len(r.body))
	for i, reading := range r.body {
		out[i], _ = reading["point"].(string)
	}
	return out
}

func TestNewChecksDependencies(t *testing.T) {
	t.Parallel()
	_, err := webhook.New(webhook.Deps{Clock: clocktest.NewFake(start)})
	assert.ErrorIs(t, err, webhook.ErrMissingDependency)
	_, err = webhook.New(webhook.Deps{Client: mocks.NewMockClient(t)})
	assert.ErrorIs(t, err, webhook.ErrMissingDependency)
}

func TestNewRejectsInvalidSettings(t *testing.T) {
	t.Parallel()
	for name, s := range map[string]webhook.Settings{
		"method GET":   {URL: hookURL, Method: "GET"},
		"no scheme":    {URL: "ingest.example.com/hook?token=secret"},
		"other scheme": {URL: "ftp://ingest.example.com/hook?token=secret"},
		"no host":      {URL: "https:///hook?token=secret"},
		"unparsable":   {URL: "https://[::1/hook?token=secret"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := webhook.New(webhook.Deps{
				Settings: s, Client: mocks.NewMockClient(t), Clock: clocktest.NewFake(start),
			})
			require.ErrorIs(t, err, webhook.ErrInvalidSettings)
			assert.NotContains(t, err.Error(), "secret", "tokens in the URL are not shown")
		})
	}
}

func TestSendsPollAsJSONArray(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{Name: "backend", Headers: map[string]string{
		"Authorization": "Bearer xyz", "Content-Type": "application/vnd.api+json",
	}})
	h.expect(http.StatusNoContent)

	h.input <- []datasource.DataPoint{point("power", 1234.5), point("broken", math.NaN())}
	r := h.request()
	h.stop()

	assert.Equal(t, http.MethodPost, r.method, "default method")
	assert.Equal(t, hookURL, r.url)
	assert.Equal(t, "Bearer xyz", r.header.Get("Authorization"))
	assert.Equal(t, "application/vnd.api+json", r.header.Get("Content-Type"),
		"headers override the default content type")
	assert.Equal(t, []map[string]any{
		{
			"device": "meter", "point": "power", "value": 1234.5, "unit": "W",
			"timestamp": "2026-10-15T08:23:20.123Z",
		},
		{
			"device": "meter", "point": "broken", "value": nil, "unit": "W",
			"timestamp": "2026-10-15T08:23:20.123Z",
		},
	}, r.body)
}

// The body format is a public interface: a compact array, keys in field order. A point
// skipped in the middle of a batch leaves no gap in it.
func TestBodyBytes(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{})
	h.expect(http.StatusOK)

	h.input <- []datasource.DataPoint{point("a", 1.0), point("odd", make(chan int)), point("b", true)}
	r := h.request()
	h.stop()

	const stamp = `"timestamp":"2026-10-15T08:23:20.123Z"`
	assert.Equal(t, `[{"device":"meter","point":"a","value":1,"unit":"W",`+stamp+`},`+
		`{"device":"meter","point":"b","value":true,"unit":"W",`+stamp+`}]`, r.raw)
}

func TestSendsJSONContentTypeAndPUT(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{Method: "put"})
	h.expect(http.StatusOK)

	h.input <- []datasource.DataPoint{point("power", 1.0)}
	r := h.request()
	h.stop()

	assert.Equal(t, http.MethodPut, r.method)
	assert.Equal(t, "application/json", r.header.Get("Content-Type"))
}

// Polls that queue up behind a slow endpoint go out in one request.
func TestCombinesQueuedPolls(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{},
		[]datasource.DataPoint{point("a", 1.0)}, []datasource.DataPoint{point("b", 2.0)})
	h.expect(http.StatusOK)
	h.stop()

	assert.Equal(t, []string{"a", "b"}, names(h.request()))
}

func TestSplitsAtMaxBatch(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{MaxBatch: 2})
	h.expect(http.StatusOK, http.StatusOK)

	h.input <- []datasource.DataPoint{point("a", 1.0), point("b", 2.0), point("c", 3.0)}
	h.stop()

	assert.Equal(t, []string{"a", "b"}, names(h.request()))
	assert.Equal(t, []string{"c"}, names(h.request()))
}

func TestRetriesServerErrorsRateLimitsAndTransportErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{MaxRetries: 3, RetryDelay: time.Second})
	refused := &url.Error{Op: "Post", URL: hookURL, Err: errors.New("connection refused")}
	h.expect(http.StatusServiceUnavailable, http.StatusTooManyRequests, refused, http.StatusOK)

	h.input <- []datasource.DataPoint{point("power", 1.0)}
	h.request()
	for range 3 {
		h.advance(time.Second)
		assert.Equal(t, []string{"power"}, names(h.request()))
	}
	h.stop()

	assert.NotContains(t, h.logs.String(), "secret", "tokens in the URL are not logged")
}

// Regression: the webhook ignored Retry-After and retried a busy endpoint after
// retry_delay; it waits like the InfluxDB writer now: the longer of the two, the
// Retry-After (seconds or a date) at most a minute.
func TestRetryWaitsForRetryAfter(t *testing.T) {
	t.Parallel()
	retryAt := start.Add(4 * time.Second).Truncate(time.Second) // dates have seconds only
	tests := []struct {
		name       string
		retryAfter string
		wait       time.Duration
	}{
		{name: "seconds", retryAfter: "5", wait: 5 * time.Second},
		{name: "date", retryAfter: retryAt.Format(http.TimeFormat), wait: retryAt.Sub(start)},
		{name: "shorter than retry_delay", retryAfter: "0", wait: time.Second},
		{name: "invalid", retryAfter: "soon", wait: time.Second},
		{name: "capped", retryAfter: "86400", wait: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, webhook.Settings{MaxRetries: 1, RetryDelay: time.Second})
			h.expect(busy{code: http.StatusTooManyRequests, retryAfter: tt.retryAfter},
				http.StatusOK)

			h.input <- []datasource.DataPoint{point("power", 1.0)}
			h.request()
			if tt.wait > time.Second {
				h.advance(tt.wait - time.Nanosecond)
				select {
				case r := <-h.requests:
					t.Fatalf("retried before the Retry-After: %v", r)
				case <-time.After(20 * time.Millisecond):
				}
				h.advance(time.Nanosecond)
			} else {
				h.advance(tt.wait)
			}
			h.request()
			h.stop()
		})
	}
}

func TestDropsAfterRetriesAndLogsRecovery(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{MaxRetries: 1, RetryDelay: time.Second})
	h.expect(silent(http.StatusBadGateway), errors.New("connection reset"), http.StatusOK)

	h.input <- []datasource.DataPoint{point("lost", 1.0)}
	h.request()
	h.advance(time.Second)
	h.request() // last attempt: the batch is dropped
	h.input <- []datasource.DataPoint{point("next", 2.0)}
	assert.Equal(t, []string{"next"}, names(h.request()))
	h.stop()

	assert.Contains(t, h.logs.String(), "HTTP status 502\"")
	assert.Contains(t, h.logs.String(), "after 1 retries: connection reset")
	assert.Contains(t, h.logs.String(), "recovered from previous error")
}

// A 4xx answer (bad token, rejected payload) fails the same way again: no retry.
func TestDoesNotRetryClientErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{MaxRetries: 3, RetryDelay: time.Hour})
	h.expect(http.StatusUnauthorized, http.StatusOK)

	h.input <- []datasource.DataPoint{point("lost", 1.0)}
	h.request()
	h.input <- []datasource.DataPoint{point("next", 2.0)} // no retry delay in between
	assert.Equal(t, []string{"next"}, names(h.request()))
	h.stop()

	assert.Contains(t, h.logs.String(), "not retried: HTTP status 401: bad token")
}

// A value JSON cannot encode is skipped; the other points of the request are sent.
func TestSkipsValuesJSONCannotEncode(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{},
		[]datasource.DataPoint{point("odd", make(chan int))},
		[]datasource.DataPoint{point("next", 2.0)})
	h.expect(http.StatusOK)
	h.stop()

	assert.Equal(t, []string{"next"}, names(h.request()))
	assert.Contains(t, h.logs.String(), "Skipping point with a value JSON cannot encode")
}

// A poll whose every point is skipped sends no request (the mock fails on any call).
func TestSendsNothingWithoutReadings(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{}, []datasource.DataPoint{point("odd", make(chan int))})
	h.stop()
}

func TestCancelEndsRetries(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{MaxRetries: 5, RetryDelay: time.Hour})
	h.expect(http.StatusServiceUnavailable)

	h.input <- []datasource.DataPoint{point("power", 1.0)}
	h.request()
	require.Eventually(t, func() bool { return h.clock.Waiters() >= 1 },
		time.Second, time.Millisecond)
	h.cancel()
	for err := range h.done {
		require.NoError(t, err)
	}
	assert.Contains(t, h.logs.String(), "cancelled while retrying")
}

func TestStopsOnCancel(t *testing.T) {
	t.Parallel()
	h := newHarness(t, webhook.Settings{})
	h.cancel()
	for err := range h.done {
		require.NoError(t, err)
	}
}

func TestNewClient(t *testing.T) {
	t.Parallel()
	c := webhook.NewClient(0, true)
	assert.Equal(t, webhook.DefaultTimeout, c.Timeout)
	transport, ok := c.Transport.(*http.Transport)
	require.True(t, ok)
	assert.True(t, transport.TLSClientConfig.InsecureSkipVerify)

	assert.Equal(t, 3*time.Second, webhook.NewClient(3*time.Second, false).Timeout)
}
