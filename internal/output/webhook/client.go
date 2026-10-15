package webhook

import (
	"crypto/tls"
	"net/http"
	"time"
)

// DefaultTimeout bounds one request when no timeout is set.
const DefaultTimeout = 10 * time.Second

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
		},
		Timeout: timeout,
	}
}
