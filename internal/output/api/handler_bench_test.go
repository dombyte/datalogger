package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// BenchmarkDevicesList answers GET /api/devices for 10 devices with 50 points each.
func BenchmarkDevicesList(b *testing.B) {
	var devices []string
	var points []datasource.DataPoint
	for d := range 10 {
		name := "device" + strconv.Itoa(d)
		devices = append(devices, name)
		for p := range 50 {
			points = append(points, datasource.DataPoint{
				DeviceName: name, PointName: "p" + strconv.Itoa(p), Value: 1.0,
				Timestamp: time.Date(2026, 10, 5, 10, 15, 2, 0, time.UTC),
			})
		}
	}
	w := &Writer{store: newStore(devices), clock: clock.Real{}, logger: zerolog.Nop()}
	w.store.set(points)
	req := httptest.NewRequest(http.MethodGet, "/api/devices", nil)

	for b.Loop() {
		w.handleDevices(httptest.NewRecorder(), req)
	}
}
