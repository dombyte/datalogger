package http_test

import (
	"context"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/datasource/http"
	"github.com/dombyte/datalogger/internal/datasource/http/mocks"
)

const interval = time.Second

var start = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

type harness struct {
	t      *testing.T
	clock  *clocktest.Fake
	data   <-chan datasource.DataPoint
	cancel context.CancelFunc
}

func settings(address string, points ...http.Point) http.Settings {
	return http.Settings{
		Name: "meter", PollInterval: interval, Address: address,
		Method: "GET", ResponseType: "json", Points: points,
	}
}

func newHarness(t *testing.T, s http.Settings, client http.Client) *harness {
	t.Helper()
	h := &harness{t: t, clock: clocktest.NewFake(start)}
	r, err := http.New(http.Deps{Settings: s, Client: client, Clock: h.clock, Log: zerolog.Nop()})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.data = r.Start(ctx)
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.cancel()
	for range h.data { // drain until the reader closes the channel
	}
}

func (h *harness) advance(n int, d time.Duration) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.clock.Waiters() >= n },
		time.Second, time.Millisecond)
	h.clock.Advance(d)
}

func (h *harness) receive() datasource.DataPoint {
	h.t.Helper()
	select {
	case dp := <-h.data:
		return dp
	case <-time.After(time.Second):
		h.t.Fatal("no data point")
		return datasource.DataPoint{}
	}
}

func (h *harness) expectNothing() {
	h.t.Helper()
	select {
	case dp := <-h.data:
		h.t.Fatalf("unexpected data point %v", dp)
	case <-time.After(50 * time.Millisecond):
	}
}

func server(t *testing.T, handler nethttp.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func TestNewChecksDependencies(t *testing.T) {
	t.Parallel()
	point := http.Point{Name: "p", JSONPath: "p", Scale: 1}

	_, err := http.New(http.Deps{Settings: settings("http://x", point), Clock: clocktest.NewFake(start)})
	assert.ErrorIs(t, err, http.ErrMissingDependency)

	_, err = http.New(http.Deps{Settings: settings("http://x", point), Client: mocks.NewClient(t)})
	assert.ErrorIs(t, err, http.ErrMissingDependency)

	bad := settings("http://x", point)
	bad.Method = "BAD METHOD"
	_, err = http.New(http.Deps{
		Settings: bad, Client: mocks.NewClient(t),
		Clock: clocktest.NewFake(start),
	})
	assert.ErrorIs(t, err, http.ErrInvalidSettings)

	_, err = http.New(http.Deps{
		Settings: settings("http://x"), Client: mocks.NewClient(t),
		Clock: clocktest.NewFake(start),
	})
	assert.ErrorIs(t, err, http.ErrInvalidSettings, "no points")
}

func TestPollsJSONWithHeadersAndBody(t *testing.T) {
	t.Parallel()
	srv := server(t, func(w nethttp.ResponseWriter, r *nethttp.Request) {
		body, _ := io.ReadAll(r.Body)
		assert.Equal(t, nethttp.MethodPost, r.Method)
		assert.Equal(t, `{"id":1}`, string(body))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer x", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"power": 1500, "missing_is_skipped": null}`))
	})
	s := settings(srv.URL,
		http.Point{Name: "power", JSONPath: "power", Scale: 0.001, Unit: "kW"},
		http.Point{Name: "gone", JSONPath: "nope", Scale: 1},
	)
	s.Method, s.Body = "POST", `{"id":1}`
	s.Headers = map[string]string{"Authorization": "Bearer x"}
	h := newHarness(t, s, http.NewClient(time.Second, false))

	h.advance(1, interval)
	assert.Equal(t, datasource.DataPoint{
		DeviceName: "meter", PointName: "power", Value: 1.5,
		Timestamp: start.Add(interval), Unit: "kW",
	}, h.receive())
	h.expectNothing()
}

func TestTextResponse(t *testing.T) {
	t.Parallel()
	srv := server(t, func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		_, _ = w.Write([]byte("OK 42"))
	})
	s := settings(srv.URL, http.Point{Name: "status", Scale: 1})
	s.ResponseType = "text"
	h := newHarness(t, s, http.NewClient(time.Second, false))

	h.advance(1, interval)
	assert.Equal(t, "OK 42", h.receive().Value)
}

func TestStatusErrorBacksOffAndRecovers(t *testing.T) {
	t.Parallel()
	calls := make(chan int, 2)
	var n atomic.Int32
	status := []int{nethttp.StatusInternalServerError, nethttp.StatusOK}
	srv := server(t, func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		code := status[n.Add(1)-1]
		calls <- code
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"v": 1}`))
	})
	// A mock-free client: a status error must not close connections either way.
	h := newHarness(t, settings(srv.URL, http.Point{Name: "v", JSONPath: "v", Scale: 1}),
		http.NewClient(time.Second, false))

	h.advance(1, interval)
	assert.Equal(t, nethttp.StatusInternalServerError, <-calls)
	h.expectNothing()

	h.advance(1, interval)
	h.advance(2, 100*time.Millisecond) // backoff
	assert.Equal(t, "v", h.receive().PointName)
}

func TestTransportErrorClosesIdleConnections(t *testing.T) {
	t.Parallel()
	client := mocks.NewClient(t)
	refused := &url.Error{Op: "Get", URL: "http://device", Err: syscall.ECONNREFUSED}
	client.EXPECT().Do(mock.Anything).Return(nil, refused).Once()
	closed := make(chan struct{})
	client.EXPECT().CloseIdleConnections().Run(func() { close(closed) }).Once()

	h := newHarness(t, settings("http://device", http.Point{Name: "v", JSONPath: "v", Scale: 1}),
		client)
	h.advance(1, interval)
	<-closed
	h.stop()
}

func TestStatusErrorKeepsConnections(t *testing.T) {
	t.Parallel()
	client := mocks.NewClient(t)
	done := make(chan struct{})
	client.EXPECT().Do(mock.Anything).RunAndReturn(
		func(*nethttp.Request) (*nethttp.Response, error) {
			defer close(done)
			return &nethttp.Response{
				StatusCode: nethttp.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}).Once()
	// CloseIdleConnections is not expected: the mock fails the test if it is called.

	h := newHarness(t, settings("http://device", http.Point{Name: "v", JSONPath: "v", Scale: 1}),
		client)
	h.advance(1, interval)
	<-done
	h.stop()
}

func TestStopClosesChannels(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings("http://device", http.Point{Name: "v", JSONPath: "v", Scale: 1}),
		mocks.NewClient(t))
	h.stop()
	_, open := <-h.data
	assert.False(t, open)
}
