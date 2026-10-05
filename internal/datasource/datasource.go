// Package datasource provides the core types and interfaces for data sources.
package datasource

import (
	"context"
	"math"
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

// JSONValue returns the value for a JSON document: NaN and ±Inf (possible for float32
// registers) have no JSON form and become nil (null); other values are unchanged.
func (dp DataPoint) JSONValue() any {
	if f, ok := dp.Value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
		return nil
	}
	return dp.Value
}

// DeviceReader polls one device.
type DeviceReader interface {
	// Name returns the device name.
	Name() string

	// Start starts polling in the background and returns the data channel, which
	// carries the points of one poll per send (never an empty slice), so a consumer
	// sees a consistent snapshot of the device. The reader owns the channel and closes
	// it when it stops, which happens after ctx is cancelled; it never stops on its own
	// because of read errors.
	Start(ctx context.Context) <-chan []DataPoint
}
