package api

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
)

// fakeListener accepts no connections: Accept blocks until Close, or fails at once
// with acceptErr. Close returns closeErr. accepting is closed on the first Accept.
type fakeListener struct {
	acceptErr  error
	closeErr   error
	accepting  chan struct{}
	closed     chan struct{}
	acceptOnce sync.Once
	closeOnce  sync.Once
}

func newFakeListener(acceptErr, closeErr error) *fakeListener {
	return &fakeListener{
		acceptErr: acceptErr, closeErr: closeErr,
		accepting: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepting) })
	if l.acceptErr != nil {
		return nil, l.acceptErr
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *fakeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return l.closeErr
}

func (l *fakeListener) Addr() net.Addr { return &net.TCPAddr{} }

func newWriter(t *testing.T, ln net.Listener, clk *clocktest.Fake) *Writer {
	t.Helper()
	w, err := New(Deps{
		Settings: Settings{Name: "api", Devices: []string{"inverter"}},
		Listener: ln,
		Clock:    clk,
		Log:      zerolog.Nop(),
	})
	require.NoError(t, err)
	return w
}

// errorsOf waits until done is closed and returns everything sent on it.
func errorsOf(t *testing.T, done <-chan error) []error {
	t.Helper()
	var errs []error
	timeout := time.After(5 * time.Second)
	for {
		select {
		case err, ok := <-done:
			if !ok {
				return errs
			}
			errs = append(errs, err)
		case <-timeout:
			t.Fatal("writer did not stop")
		}
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{})
	require.ErrorIs(t, err, ErrMissingDependency)
	assert.ErrorContains(t, err, "Listener, Clock")
}

func TestListen(t *testing.T) {
	t.Parallel()
	ln, err := Listen("127.0.0.1:0")
	require.NoError(t, err)

	_, err = Listen(ln.Addr().String())
	require.ErrorContains(t, err, "api: listen")
	require.NoError(t, ln.Close())
}

func TestServesUntilInputIsClosed(t *testing.T) {
	t.Parallel()
	ln, err := Listen("127.0.0.1:0")
	require.NoError(t, err)
	w := newWriter(t, ln, clocktest.NewFake(start))
	assert.Equal(t, "api", w.Name())

	in := make(chan datasource.DataPoint)
	done := w.Start(context.Background(), in)
	in <- point("inverter", "power", 42.0, "W")

	url := "http://" + ln.Addr().String() + "/api/devices/inverter/power"
	require.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && len(body) > 0
	}, 5*time.Second, 10*time.Millisecond)

	close(in)
	assert.Empty(t, errorsOf(t, done))
	_, err = http.Get(url)
	assert.Error(t, err, "server is stopped")
}

func TestStopsWhenContextIsCancelled(t *testing.T) {
	t.Parallel()
	w := newWriter(t, newFakeListener(nil, nil), clocktest.NewFake(start))
	ctx, cancel := context.WithCancel(context.Background())

	done := w.Start(ctx, make(chan datasource.DataPoint))
	cancel()

	assert.Empty(t, errorsOf(t, done))
}

func TestServeFailureStopsWriter(t *testing.T) {
	t.Parallel()
	w := newWriter(t, newFakeListener(errors.New("accept failed"), nil), clocktest.NewFake(start))

	errs := errorsOf(t, w.Start(context.Background(), make(chan datasource.DataPoint)))

	require.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "api: serve: accept failed")
}

func TestListenerCloseErrorIsNotFatal(t *testing.T) {
	t.Parallel()
	ln := newFakeListener(nil, errors.New("close failed"))
	w := newWriter(t, ln, clocktest.NewFake(start))
	in := make(chan datasource.DataPoint)

	done := w.Start(context.Background(), in)
	<-ln.accepting // Shutdown closes only listeners that Serve already uses
	close(in)

	assert.Empty(t, errorsOf(t, done))
}

// startWithOpenRequest starts a writer on a local listener and opens a connection with
// an unfinished request, which keeps the server's shutdown waiting.
func startWithOpenRequest(
	t *testing.T,
	ctx context.Context,
	clk *clocktest.Fake,
) (chan datasource.DataPoint, <-chan error) {
	t.Helper()
	ln, err := Listen("127.0.0.1:0")
	require.NoError(t, err)
	w := newWriter(t, ln, clk)
	in := make(chan datasource.DataPoint)
	done := w.Start(ctx, in)

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = io.WriteString(conn, "GET /api/devices HTTP/1.1\r\n")
	require.NoError(t, err)
	return in, done
}

func TestShutdownClosesOpenRequestsAfterGrace(t *testing.T) {
	t.Parallel()
	clk := clocktest.NewFake(start)
	in, done := startWithOpenRequest(t, context.Background(), clk)

	close(in)
	require.Eventually(t, func() bool { return clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
	clk.Advance(shutdownGrace)

	assert.Empty(t, errorsOf(t, done))
}

func TestCancelDuringShutdownClosesOpenRequests(t *testing.T) {
	t.Parallel()
	clk := clocktest.NewFake(start)
	ctx, cancel := context.WithCancel(context.Background())
	in, done := startWithOpenRequest(t, ctx, clk)

	close(in)
	require.Eventually(t, func() bool { return clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
	cancel()

	assert.Empty(t, errorsOf(t, done))
}
