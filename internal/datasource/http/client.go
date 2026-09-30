package http

import (
	"crypto/tls"
	"net/http"
	"time"
)

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
		},
		Timeout: timeout,
	}
}
