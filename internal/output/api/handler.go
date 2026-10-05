package api

import (
	"cmp"
	"crypto/subtle"
	"encoding/json"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/dombyte/datalogger/internal/datasource"
)

// pointResponse is one point in a response; AgeMS is the time since the reading.
type pointResponse struct {
	Value     any       `json:"value"`
	Unit      string    `json:"unit"`
	Timestamp time.Time `json:"timestamp"`
	AgeMS     int64     `json:"age_ms"`
}

// deviceResponse is the body of GET /api/devices/{device}.
type deviceResponse struct {
	Device string                   `json:"device"`
	Points map[string]pointResponse `json:"points"`
}

// singlePointResponse is the body of GET /api/devices/{device}/{point}.
type singlePointResponse struct {
	Device string `json:"device"`
	Point  string `json:"point"`
	pointResponse
}

// deviceSummary is one entry of GET /api/devices; AgeMS is the age of the newest
// point, null before the first reading.
type deviceSummary struct {
	Device string `json:"device"`
	Points int    `json:"points"`
	AgeMS  *int64 `json:"age_ms"`
}

// devicesResponse is the body of GET /api/devices.
type devicesResponse struct {
	Devices []deviceSummary `json:"devices"`
}

// errorResponse is the body of every error response.
type errorResponse struct {
	Error string `json:"error"`
}

// routes returns the handler of the server; /healthz needs no token.
func (w *Writer) routes() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/devices", w.handleDevices)
	api.HandleFunc("GET /api/devices/{device}", w.handleDevice)
	api.HandleFunc("GET /api/devices/{device}/{point}", w.handlePoint)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})
	mux.Handle("/api/", w.authorize(api))
	return mux
}

// authorize rejects requests without the configured bearer token; without a token
// every request is allowed.
func (w *Writer) authorize(next http.Handler) http.Handler {
	if w.settings.Token == "" {
		return next
	}
	want := []byte("Bearer " + w.settings.Token)
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			rw.Header().Set("WWW-Authenticate", "Bearer")
			w.writeJSON(rw, http.StatusUnauthorized, errorResponse{Error: "unauthorized"})
			return
		}
		next.ServeHTTP(rw, r)
	})
}

// handleDevices lists the served devices, sorted by name.
func (w *Writer) handleDevices(rw http.ResponseWriter, _ *http.Request) {
	now := w.clock.Now()
	resp := devicesResponse{Devices: []deviceSummary{}}
	for name, points := range w.store.devices() {
		resp.Devices = append(resp.Devices, deviceSummary{
			Device: name, Points: len(points), AgeMS: newestAge(points, now),
		})
	}
	slices.SortFunc(resp.Devices, func(a, b deviceSummary) int {
		return cmp.Compare(a.Device, b.Device)
	})
	w.writeJSON(rw, http.StatusOK, resp)
}

// newestAge returns the age of the newest point in ms, nil if there is none.
func newestAge(points map[string]datasource.DataPoint, now time.Time) *int64 {
	if len(points) == 0 {
		return nil
	}
	var newest time.Time
	for _, dp := range points {
		if dp.Timestamp.After(newest) {
			newest = dp.Timestamp
		}
	}
	age := now.Sub(newest).Milliseconds()
	return &age
}

// handleDevice returns the latest value of every point of a device.
func (w *Writer) handleDevice(rw http.ResponseWriter, r *http.Request) {
	device := r.PathValue("device")
	points, ok := w.store.device(device)
	if !ok {
		w.writeJSON(rw, http.StatusNotFound, errorResponse{Error: "unknown device " + device})
		return
	}
	now := w.clock.Now()
	resp := deviceResponse{Device: device, Points: make(map[string]pointResponse, len(points))}
	for name, dp := range points {
		resp.Points[name] = newPointResponse(dp, now)
	}
	w.writeJSON(rw, http.StatusOK, resp)
}

// handlePoint returns the latest value of one point.
func (w *Writer) handlePoint(rw http.ResponseWriter, r *http.Request) {
	device, point := r.PathValue("device"), r.PathValue("point")
	points, ok := w.store.device(device)
	if !ok {
		w.writeJSON(rw, http.StatusNotFound, errorResponse{Error: "unknown device " + device})
		return
	}
	dp, ok := points[point]
	if !ok {
		w.writeJSON(rw, http.StatusNotFound, errorResponse{Error: "no value for point " + point})
		return
	}
	w.writeJSON(rw, http.StatusOK, singlePointResponse{
		Device: device, Point: point, pointResponse: newPointResponse(dp, w.clock.Now()),
	})
}

// newPointResponse converts a point; NaN and ±Inf (possible for float32 registers)
// have no JSON form and become null.
func newPointResponse(dp datasource.DataPoint, now time.Time) pointResponse {
	value := dp.Value
	if f, ok := value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
		value = nil
	}
	return pointResponse{
		Value:     value,
		Unit:      dp.Unit,
		Timestamp: dp.Timestamp,
		AgeMS:     now.Sub(dp.Timestamp).Milliseconds(),
	}
}

// writeJSON writes v as the JSON body with the given status.
func (w *Writer) writeJSON(rw http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to encode API response")
		http.Error(rw, "internal error", http.StatusInternalServerError)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	// A failed write means the client went away; there is nobody left to tell.
	if _, err := rw.Write(append(body, '\n')); err != nil {
		w.logger.Debug().Err(err).Msg("Failed to write API response")
	}
}
