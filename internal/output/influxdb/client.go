package influxdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

const (
	// gzipThreshold enables gzip for write bodies above this size (library default).
	gzipThreshold = 1000

	// DefaultTimeout bounds one write request when ConnSettings.Timeout is 0.
	DefaultTimeout = 10 * time.Second

	// idleConnTimeout closes a kept-alive connection that was idle this long, before
	// most servers close it themselves: a request that reuses a connection the server
	// is closing fails, and Go does not resend a POST.
	idleConnTimeout = 30 * time.Second
)

// Client writes points to InfluxDB; *influxdb3.Client implements it.
type Client interface {
	WritePoints(ctx context.Context, points []*influxdb3.Point, opts ...influxdb3.WriteOption) error
	Close() error
}

// ConnSettings configures the connection to the database.
type ConnSettings struct {
	Address  string
	Token    string
	Database string
	Insecure bool          // skip TLS certificate verification
	Timeout  time.Duration // per request; 0 = DefaultTimeout
	// NoSync makes the server answer before the write is persisted in its WAL.
	NoSync bool
}

// NewClient creates an InfluxDB 3 client using the v3 write API. It does not connect;
// an error means the settings are unusable.
func NewClient(s ConnSettings) (*influxdb3.Client, error) {
	timeout := orDefault(s.Timeout, DefaultTimeout)
	// From the library defaults, so an option set here is the only one changed: an empty
	// WriteOptions turned AcceptPartial off, and the server then rejected a whole batch
	// for one bad line.
	options := influxdb3.DefaultWriteOptions
	options.UseV2Api = false // v3 endpoint /api/v3/write_lp
	options.GzipThreshold = gzipThreshold
	options.NoSync = s.NoSync
	cfg := influxdb3.ClientConfig{
		Host:         s.Address,
		Token:        s.Token,
		Database:     s.Database,
		WriteTimeout: timeout,
		WriteOptions: &options,
	}
	if s.Insecure {
		// The library applies WriteTimeout only to a client it creates itself.
		cfg.HTTPClient = &http.Client{Timeout: timeout, Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: s.Insecure},
			IdleConnTimeout: idleConnTimeout,
		}}
	}

	client, err := influxdb3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	return client, nil
}
