package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dombyte/datalogger/config"
	"github.com/rs/zerolog"
)

// TestNewHttpReader tests creating a new HTTP reader
func TestNewHttpReader(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test_http",
		Type:         "http",
		PollInterval: time.Second,
		Timeout:      5 * time.Second,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	if reader == nil {
		t.Fatal("Reader is nil")
	}

	if reader.Name() != "test_http" {
		t.Errorf("Name() = %v, want %v", reader.Name(), "test_http")
	}
}

// TestHttpReaderName tests the Name method
func TestHttpReaderName(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name: "my_http_device",
		Type: "http",
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	if reader.Name() != "my_http_device" {
		t.Errorf("Name() = %v, want %v", reader.Name(), "my_http_device")
	}
}

// TestHttpReaderValidate tests the Validate method
func TestHttpReaderValidate(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test_http",
		Type:         "http",
		PollInterval: time.Second,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	if err := reader.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}
}

// TestHttpReaderStart tests the Start method
func TestHttpReaderStart(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test_http",
		Type:         "http",
		PollInterval: 100 * time.Millisecond,
		Timeout:      5 * time.Second,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	dataCh, doneCh, errCh := reader.Start(ctx)

	if dataCh == nil {
		t.Fatal("dataCh is nil")
	}

	if doneCh == nil {
		t.Fatal("doneCh is nil")
	}

	if errCh == nil {
		t.Fatal("errCh is nil")
	}

	cancel()

	select {
	case <-doneCh:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for done channel")
	}
}

// TestCreateRequest tests the createRequest method
func TestCreateRequest(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name       string
		httpConfig config.HttpConfig
		wantMethod string
	}{
		{
			name: "GET request",
			httpConfig: config.HttpConfig{
				Address: "http://localhost:8080/api",
				Method:  "GET",
			},
			wantMethod: "GET",
		},
		{
			name: "POST request",
			httpConfig: config.HttpConfig{
				Address: "http://localhost:8080/api",
				Method:  "POST",
				Body:    `{"key": "value"}`,
			},
			wantMethod: "POST",
		},
		{
			name: "GET request with default method",
			httpConfig: config.HttpConfig{
				Address: "http://localhost:8080/api",
			},
			wantMethod: "GET",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deviceConfig := config.Device{
				Name: "test",
				Type: "http",
				DeviceSpecific: config.DeviceSpecific{
					Http: tt.httpConfig,
				},
			}

			reader, err := NewHttpReader(deviceConfig, &logger)
			if err != nil {
				t.Fatalf("Failed to create HTTP reader: %v", err)
			}

			req, err := reader.createRequest()
			if err != nil {
				t.Fatalf("createRequest() error = %v", err)
			}

			if req.Method != tt.wantMethod {
				t.Errorf("Method = %v, want %v", req.Method, tt.wantMethod)
			}
		})
	}
}

// TestExtractJSONValue tests JSON value extraction
func TestExtractJSONValue(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name: "test",
		Type: "http",
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	tests := []struct {
		name    string
		body    string
		point   config.Point
		want    interface{}
		wantErr bool
	}{
		{
			name:    "simple json float",
			body:    `{"temperature": 23.5}`,
			point:   config.Point{Name: "temp", JsonPath: "temperature", Type: "float64"},
			want:    float64(23.5),
			wantErr: false,
		},
		{
			name:    "nested json",
			body:    `{"sensor": {"temperature": 25.0}}`,
			point:   config.Point{Name: "temp", JsonPath: "sensor.temperature", Type: "float64"},
			want:    float64(25.0),
			wantErr: false,
		},
		{
			name:    "json array",
			body:    `{"values": [1, 2, 3]}`,
			point:   config.Point{Name: "val", JsonPath: "values.1", Type: "int64"},
			want:    int64(2),
			wantErr: false,
		},
		{
			name:    "json bool",
			body:    `{"enabled": true}`,
			point:   config.Point{Name: "enabled", JsonPath: "enabled", Type: "bool"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "json string",
			body:    `{"name": "test"}`,
			point:   config.Point{Name: "name", JsonPath: "name", Type: "string"},
			want:    "test",
			wantErr: false,
		},
		{
			name:    "missing json path",
			body:    `{"temperature": 23.5}`,
			point:   config.Point{Name: "temp", JsonPath: "missing", Type: "float64"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty json path",
			body:    `{"temperature": 23.5}`,
			point:   config.Point{Name: "temp", JsonPath: "", Type: "float64"},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reader.extractJSONValue([]byte(tt.body), tt.point)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractJSONValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.want != nil {
				if got != tt.want {
					t.Errorf("extractJSONValue() = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
				}
			}
		})
	}
}

// TestReadAllPointsWithServer tests reading points from a real HTTP server
func TestReadAllPointsWithServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response := map[string]interface{}{
			"temperature": 23.5,
			"humidity":    60.0,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test",
		Type:         "http",
		PollInterval: time.Second,
		Timeout:      5 * time.Second,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      server.URL,
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64", Unit: "C"},
			{Name: "humidity", JsonPath: "humidity", Type: "float64", Unit: "%"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	ctx := context.Background()
	points, err := reader.readAllPoints(ctx)
	if err != nil {
		t.Fatalf("readAllPoints() error = %v", err)
	}

	if len(points) != 2 {
		t.Errorf("readAllPoints() returned %d points, want 2", len(points))
	}

	for _, p := range points {
		if p.Timestamp.Location() != time.UTC {
			t.Errorf("Timestamp should be in UTC, got location: %v", p.Timestamp.Location())
		}
	}
}

// TestReadAllPointsError tests error handling in readAllPoints
func TestReadAllPointsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}))
	defer server.Close()

	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test",
		Type:         "http",
		PollInterval: time.Second,
		Timeout:      5 * time.Second,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      server.URL,
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	ctx := context.Background()
	_, err = reader.readAllPoints(ctx)
	if err == nil {
		t.Error("readAllPoints() expected error for HTTP 500")
	}
}

// TestReadAllPointsTimeout tests timeout handling
func TestReadAllPointsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test",
		Type:         "http",
		PollInterval: time.Second,
		Timeout:      50 * time.Millisecond,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      server.URL,
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	ctx := context.Background()
	_, err = reader.readAllPoints(ctx)
	if err == nil {
		t.Error("readAllPoints() expected timeout error")
	}
}

// TestTextResponseType tests handling of text response type
func TestTextResponseType(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name: "test",
		Type: "http",
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "text",
			},
		},
		Points: []config.Point{
			{Name: "text", JsonPath: "", Type: "string"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	body := []byte("Hello, World!")
	timestamp := time.Now().UTC()

	results, err := reader.parsePointsSequential(body, timestamp)
	if err != nil {
		t.Fatalf("parsePointsSequential() error = %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].Value != "Hello, World!" {
		t.Errorf("Value = %v, want %v", results[0].Value, "Hello, World!")
	}
}

// TestParsePointsParallel tests the parsePointsParallel function
func TestParsePointsParallel(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test",
		Type:         "http",
		PollInterval: time.Second,
		Timeout:      5 * time.Second,
		Parallelism:  2, // Enable parallelism
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64"},
			{Name: "humidity", JsonPath: "humidity", Type: "float64"},
			{Name: "pressure", JsonPath: "pressure", Type: "float64"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	body := []byte(`{"temperature": 23.5, "humidity": 60.0, "pressure": 1013.25}`)
	timestamp := time.Now().UTC()

	results, err := reader.parsePointsParallel(body, timestamp)
	if err != nil {
		t.Fatalf("parsePointsParallel() error = %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Check that all points have the same timestamp
	for _, result := range results {
		if !result.Timestamp.Equal(timestamp) {
			t.Errorf("Timestamp mismatch for point %s", result.PointName)
		}
	}
}

// TestMarshalJSON tests the MarshalJSON method
func TestMarshalJSON(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name: "test_json",
		Type: "http",
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	jsonBytes, err := reader.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}

	// Check that the JSON contains the expected name
	if !strings.Contains(string(jsonBytes), "test_json") {
		t.Errorf("MarshalJSON() should contain device name, got: %s", string(jsonBytes))
	}

	// Check that it's valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Errorf("MarshalJSON() produced invalid JSON: %v", err)
	}

	if result["name"] != "test_json" {
		t.Errorf("JSON name = %v, want %v", result["name"], "test_json")
	}
}

// TestPollLoopShutdown tests that pollLoop can be shut down
func TestPollLoopShutdown(t *testing.T) {
	logger := zerolog.Nop()
	deviceConfig := config.Device{
		Name:         "test",
		Type:         "http",
		PollInterval: 100 * time.Millisecond,
		Timeout:      5 * time.Second,
		Parallelism:  1,
		DeviceSpecific: config.DeviceSpecific{
			Http: config.HttpConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
		},
		Points: []config.Point{
			{Name: "temp", JsonPath: "temperature", Type: "float64"},
		},
	}

	reader, err := NewHttpReader(deviceConfig, &logger)
	if err != nil {
		t.Fatalf("Failed to create HTTP reader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	dataCh, doneCh, _ := reader.Start(ctx)

	// Wait for a short time to ensure the poll loop has started
	time.Sleep(200 * time.Millisecond)

	// The poll loop should be running and will exit when ctx is done
	// We can't easily verify the loop is running, but we can check it exits cleanly

	// Drain the data channel
	select {
	case <-dataCh:
		// Got data (might happen if there's a server)
	default:
		// No data yet (expected since we don't have a real server)
	}

	// The test will complete when ctx times out
	// The poll loop should exit cleanly

	select {
	case <-doneCh:
		// Success - poll loop exited
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for poll loop to exit")
	}
}
