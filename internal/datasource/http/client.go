package http

import (
	"crypto/tls"
	"net/http"
	"time"
)

// idleConnTimeout closes a kept-alive connection that was idle this long, before most
// servers close it themselves: a request that reuses a connection the server is closing
// fails, and Go does not resend a POST.
const idleConnTimeout = 30 * time.Second

// Client sends the poll requests; *http.Client implements it.
type Client interface {
	Do(req *http.Request) (*http.Response, error)
	CloseIdleConnections()
}

// NewClient returns an *http.Client with the device timeout. insecure skips TLS
// certificate verification (the documented opt-in for self-signed devices).
func NewClient(timeout time.Duration, insecure bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			IdleConnTimeout: idleConnTimeout,
		},
		Timeout: timeout,
	}
}
