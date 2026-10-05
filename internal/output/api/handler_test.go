package api

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
)

var start = time.Date(2026, 10, 5, 10, 15, 2, 123e6, time.UTC)

func point(device, name string, value any, unit string) datasource.DataPoint {
	return datasource.DataPoint{
		DeviceName: device, PointName: name, Value: value, Timestamp: start, Unit: unit,
	}
}

// newTestWriter returns a writer for the devices inverter and meter whose clock is
// 820 ms after the points' timestamp.
func newTestWriter(t *testing.T, token string, points ...datasource.DataPoint) *Writer {
	t.Helper()
	clk := clocktest.NewFake(start)
	w, err := New(Deps{
		Settings: Settings{Name: "api", Devices: []string{"meter", "inverter"}, Token: token},
		Listener: newFakeListener(nil, nil),
		Clock:    clk,
		Log:      zerolog.Nop(),
	})
	require.NoError(t, err)
	for _, dp := range points {
		w.store.set(dp)
	}
	clk.Advance(820 * time.Millisecond)
	return w
}

// serve sends one request to the writer's handler.
func serve(w *Writer, method, path, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	w.server.Handler.ServeHTTP(rec, req)
	return rec
}

func TestRoutes(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, "",
		point("inverter", "power", 1234.5, "W"),
		point("inverter", "status_text", "running", ""),
		point("inverter", "broken", math.NaN(), "V"),
		point("inverter", "power", 1300.0, "W"), // replaces the first value
		point("other", "power", 1.0, "W"),       // device not served
	)

	tests := []struct {
		name   string
		method string
		path   string
		status int
		body   string // JSON; empty: not checked
	}{
		{
			name: "device list", method: http.MethodGet, path: "/api/devices", status: 200,
			body: `{"devices":[
				{"device":"inverter","points":3,"age_ms":820},
				{"device":"meter","points":0,"age_ms":null}]}`,
		},
		{
			name: "device", method: http.MethodGet, path: "/api/devices/inverter", status: 200,
			body: `{"device":"inverter","points":{
				"power":{"value":1300,"unit":"W","timestamp":"2026-10-05T10:15:02.123Z","age_ms":820},
				"status_text":{"value":"running","unit":"",
					"timestamp":"2026-10-05T10:15:02.123Z","age_ms":820},
				"broken":{"value":null,"unit":"V","timestamp":"2026-10-05T10:15:02.123Z","age_ms":820}
			}}`,
		},
		{
			name: "device without reading", method: http.MethodGet, path: "/api/devices/meter",
			status: 200, body: `{"device":"meter","points":{}}`,
		},
		{
			name: "point", method: http.MethodGet, path: "/api/devices/inverter/power", status: 200,
			body: `{"device":"inverter","point":"power","value":1300,"unit":"W",
				"timestamp":"2026-10-05T10:15:02.123Z","age_ms":820}`,
		},
		{
			name: "unknown device", method: http.MethodGet, path: "/api/devices/other", status: 404,
			body: `{"error":"unknown device other"}`,
		},
		{
			name: "point of unknown device", method: http.MethodGet,
			path: "/api/devices/other/power", status: 404, body: `{"error":"unknown device other"}`,
		},
		{
			name: "point without reading", method: http.MethodGet,
			path: "/api/devices/meter/power", status: 404, body: `{"error":"no value for point power"}`,
		},
		{name: "wrong method", method: http.MethodPost, path: "/api/devices", status: 405},
		{name: "unknown path", method: http.MethodGet, path: "/api/other", status: 404},
		{name: "health", method: http.MethodGet, path: "/healthz", status: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := serve(w, tt.method, tt.path, "")
			assert.Equal(t, tt.status, rec.Code)
			if tt.body != "" {
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				assert.JSONEq(t, tt.body, rec.Body.String())
			}
		})
	}
}

func TestToken(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, "secret")

	tests := []struct {
		name   string
		path   string
		auth   string
		status int
	}{
		{name: "missing", path: "/api/devices", status: 401},
		{name: "wrong", path: "/api/devices", auth: "Bearer wrong", status: 401},
		{name: "without scheme", path: "/api/devices", auth: "secret", status: 401},
		{name: "valid", path: "/api/devices", auth: "Bearer secret", status: 200},
		{name: "health needs none", path: "/healthz", status: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := serve(w, http.MethodGet, tt.path, tt.auth)
			assert.Equal(t, tt.status, rec.Code)
			if tt.status == http.StatusUnauthorized {
				assert.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
				assert.JSONEq(t, `{"error":"unauthorized"}`, rec.Body.String())
			}
		})
	}
}

func TestUnencodableValueIsInternalError(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, "", point("inverter", "odd", make(chan int), ""))

	rec := serve(w, http.MethodGet, "/api/devices/inverter/odd", "")

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}
