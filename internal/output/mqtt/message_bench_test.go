package mqtt

import (
	"testing"
	"time"

	"github.com/dombyte/datalogger/internal/datasource"
)

// BenchmarkMessage encodes the topic and payload of one point.
func BenchmarkMessage(b *testing.B) {
	w := &Writer{settings: Settings{Name: "broker", Topic: "datalogger"}}
	dp := datasource.DataPoint{
		DeviceName: "meter", PointName: "voltage", Value: 230.5, Unit: "V",
		Timestamp: time.Date(2026, 10, 5, 10, 15, 2, 123000000, time.UTC),
	}

	for b.Loop() {
		if _, _, err := w.message(dp); err != nil {
			b.Fatal(err)
		}
	}
}
