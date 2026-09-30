package influxdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

// gzipThreshold enables gzip for write bodies above this size (library default).
const gzipThreshold = 1000

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
	Insecure bool // skip TLS certificate verification
}

// NewClient creates an InfluxDB 3 client using the v3 write API. It does not connect;
// an error means the settings are unusable.
func NewClient(s ConnSettings) (*influxdb3.Client, error) {
	cfg := influxdb3.ClientConfig{
		Host:     s.Address,
		Token:    s.Token,
		Database: s.Database,
		WriteOptions: &influxdb3.WriteOptions{
			UseV2Api:      false, // v3 endpoint /api/v3/write_lp
			GzipThreshold: gzipThreshold,
		},
	}
	if s.Insecure {
		cfg.HTTPClient = &http.Client{Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: s.Insecure},
		}}
	}

	client, err := influxdb3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	return client, nil
}
