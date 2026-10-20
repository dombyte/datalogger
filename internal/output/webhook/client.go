package webhook

import (
	"crypto/tls"
	"net/http"
	"time"
)

const (
	// DefaultTimeout bounds one request when no timeout is set.
	DefaultTimeout = 10 * time.Second

	// idleConnTimeout closes a kept-alive connection that was idle this long, before
	// most servers close it themselves: a request that reuses a connection the server
	// is closing fails, and Go does not resend a POST.
	idleConnTimeout = 30 * time.Second
)

// Client sends the requests; *http.Client implements it.
type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

// NewClient returns an *http.Client with the request timeout (0 = DefaultTimeout).
// insecure skips TLS certificate verification (the documented opt-in for self-signed
// endpoints).
func NewClient(timeout time.Duration, insecure bool) *http.Client {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			IdleConnTimeout: idleConnTimeout,
		},
		Timeout: timeout,
	}
}
