package http

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
)

// countingBody records how much of the body was read and whether it was closed.
type countingBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func (b *countingBody) Close() error {
	b.closed = true
	return nil
}

func bodyReader(t *testing.T, resp *http.Response) *Reader {
	t.Helper()
	r, err := New(Deps{
		Settings: Settings{
			Name: "meter", PollInterval: time.Second, Address: "http://device", Method: "GET",
			ResponseType: "text", Points: []Point{{Name: "body", Scale: 1}},
		},
		Client: stubClient{resp: resp},
		Clock:  clocktest.NewFake(time.Time{}),
		Log:    zerolog.Nop(),
	})
	require.NoError(t, err)
	return r
}

// Regression: the body was read without a limit, so a broken or hostile endpoint could
// make the process use any amount of memory.
func TestBodyAboveLimitIsAnError(t *testing.T) {
	t.Parallel()
	body := &countingBody{Reader: strings.NewReader(strings.Repeat("x", maxBodySize+100))}
	r := bodyReader(t, &http.Response{StatusCode: http.StatusOK, Body: body})

	_, err := r.poll(context.Background())
	require.ErrorContains(t, err, "response larger than")
	assert.LessOrEqual(t, body.read, maxBodySize+1, "reading stops after the limit")
	assert.True(t, body.closed)
}

func TestBodyAtLimitIsRead(t *testing.T) {
	t.Parallel()
	body := &countingBody{Reader: strings.NewReader(strings.Repeat("x", maxBodySize))}
	r := bodyReader(t, &http.Response{StatusCode: http.StatusOK, Body: body})

	points, err := r.poll(context.Background())
	require.NoError(t, err)
	assert.Len(t, points[0].Value, maxBodySize)
}

// Regression: the body of a non-200 response was closed unread, so the connection
// could not be reused for the next poll.
func TestStatusErrorDrainsBody(t *testing.T) {
	t.Parallel()
	body := &countingBody{Reader: strings.NewReader(`{"error": "busy"}`)}
	r := bodyReader(t, &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body})

	_, err := r.poll(context.Background())
	require.ErrorContains(t, err, "HTTP error: 503")
	assert.Equal(t, len(`{"error": "busy"}`), body.read)
	assert.True(t, body.closed)
}
