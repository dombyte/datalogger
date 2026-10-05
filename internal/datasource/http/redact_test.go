package http

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
)

// stubClient returns err for every request.
type stubClient struct{ err error }

func (s stubClient) Do(*http.Request) (*http.Response, error) { return nil, s.err }
func (stubClient) CloseIdleConnections()                      {}

// Regression: a failed poll logged the URL of the *url.Error, so an API key in the
// query was written to the log with every failed poll.
func TestPollErrorDoesNotLogSecrets(t *testing.T) {
	t.Parallel()
	const address = "https://user:pw-secret@api.example.com/v1/data?appid=key-secret#frag"
	doErr := &url.Error{Op: "Get", URL: address, Err: syscall.ECONNREFUSED}

	var logs bytes.Buffer
	r, err := New(Deps{
		Settings: Settings{
			Name: "weather", PollInterval: time.Second, Address: address, Method: "GET",
			ResponseType: "json", Points: []Point{{Name: "t", JSONPath: "t", Scale: 1}},
		},
		Client: stubClient{err: doErr},
		Clock:  clocktest.NewFake(time.Time{}),
		Log:    zerolog.New(&logs),
	})
	require.NoError(t, err)

	_, err = r.poll(context.Background())
	require.Error(t, err)
	r.handlePollError(err)

	assert.Contains(t, logs.String(), "https://api.example.com/v1/data")
	assert.NotContains(t, logs.String(), "secret")
	assert.True(t, isTransportError(err), "still a transport error")
	assert.ErrorIs(t, err, syscall.ECONNREFUSED)
}

func TestRedactURLError(t *testing.T) {
	t.Parallel()
	other := errors.New("other")
	assert.Same(t, other, redactURLError(other), "not a *url.Error")

	err := redactURLError(&url.Error{Op: "parse", URL: "http://[::1", Err: other})
	assert.EqualError(t, err, `parse "<invalid address>": other`)
}
