// Package datasource provides the core types and interfaces for data sources.
package datasource

import (
	"context"
	"time"
)

// DataPoint represents a single data reading from a device.
// The Timestamp is set when data is received from the device (not when poll starts),
// ensuring data is timestamped with the actual reading time.
// Timestamps are stored in UTC to ensure consistency with InfluxDB expectations.
type DataPoint struct {
	DeviceName string      `json:"device"`
	PointName  string      `json:"point"`
	Value      interface{} `json:"value"` // float64, int64, uint64, bool, string
	Timestamp  time.Time   `json:"timestamp"`
	Unit       string      `json:"unit"`
}

// DeviceReader is the interface that all device readers must implement.
type DeviceReader interface {
	// Name returns the device name
	Name() string

	// Start begins polling the device at its configured interval
	// Returns channel for DataPoints, done channel, and error channel
	Start(ctx context.Context) (<-chan DataPoint, <-chan struct{}, <-chan error)

	// Validate checks configuration before startup
	Validate() error
}
