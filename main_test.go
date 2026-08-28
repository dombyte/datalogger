package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
	"github.com/dombyte/datalogger/output"
	"github.com/rs/zerolog"
)

// mockOutputWriterForMap is a mock that implements the output.OutputWriter interface for testing
type mockOutputWriterForMap struct {
	name    string
	devices []string
}

func (w *mockOutputWriterForMap) Name() string {
	return w.name
}

func (w *mockOutputWriterForMap) Devices() []string {
	return w.devices
}

func (w *mockOutputWriterForMap) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	ch := make(chan error, 1)
	close(ch)
	return ch
}

func (w *mockOutputWriterForMap) Validate() error {
	return nil
}

// mockOutputWriterForBuffer is a mock that implements the full output.OutputWriter interface
type mockOutputWriterForBuffer struct {
	name string
}

func (w *mockOutputWriterForBuffer) Name() string {
	return w.name
}

func (w *mockOutputWriterForBuffer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	ch := make(chan error, 1)
	go func() {
		for range input {
			// do nothing
		}
		close(ch)
	}()
	return ch
}

func (w *mockOutputWriterForBuffer) Validate() error {
	return nil
}

func (w *mockOutputWriterForBuffer) Devices() []string {
	return []string{"device1"}
}

// TestSetupLogger tests the setupLogger function
func TestSetupLogger(t *testing.T) {
	tests := []struct {
		name     string
		debug    bool
		logLevel zerolog.Level
	}{
		{
			name:     "debug enabled",
			debug:    true,
			logLevel: zerolog.DebugLevel,
		},
		{
			name:     "debug disabled",
			debug:    false,
			logLevel: zerolog.InfoLevel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := setupLogger(tt.debug)

			if logger == nil {
				t.Fatal("setupLogger returned nil")
			}

			// Check log level
			if logger.GetLevel() != tt.logLevel {
				t.Errorf("Log level = %v, want %v", logger.GetLevel(), tt.logLevel)
			}
		})
	}
}

// TestMonitorDevice tests the monitorDevice function
func TestMonitorDevice(t *testing.T) {
	logger := zerolog.Nop()

	// Test done channel
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		close(doneCh)
	}()

	monitorDevice("test_device", doneCh, errCh, &logger)

	// Test error channel
	doneCh2 := make(chan struct{})
	errCh2 := make(chan error, 1)
	errCh2 <- fmt.Errorf("test error")
	close(errCh2)

	monitorDevice("test_device", doneCh2, errCh2, &logger)

	// Both tests should complete without panic
}

// TestBuildDeviceOutputMap tests the buildDeviceOutputMap function
func TestBuildDeviceOutputMap(t *testing.T) {
	outputWriters := []output.OutputWriter{
		&mockOutputWriterForMap{name: "output1", devices: []string{"device1", "device2"}},
		&mockOutputWriterForMap{name: "output2", devices: []string{"device2", "device3"}},
		&mockOutputWriterForMap{name: "output3", devices: []string{"device1"}},
	}

	outputChannels := map[string]chan<- datasource.DataPoint{
		"output1": make(chan datasource.DataPoint, 10),
		"output2": make(chan datasource.DataPoint, 10),
		"output3": make(chan datasource.DataPoint, 10),
	}

	// Test with device1
	result1 := buildDeviceOutputMap("device1", outputWriters, outputChannels)
	if len(result1) != 2 {
		t.Errorf("buildDeviceOutputMap for device1 returned %d outputs, want 2", len(result1))
	}
	if _, ok := result1["output1"]; !ok {
		t.Error("output1 should be in device1 outputs")
	}
	if _, ok := result1["output3"]; !ok {
		t.Error("output3 should be in device1 outputs")
	}

	// Test with device2
	result2 := buildDeviceOutputMap("device2", outputWriters, outputChannels)
	if len(result2) != 2 {
		t.Errorf("buildDeviceOutputMap for device2 returned %d outputs, want 2", len(result2))
	}
	if _, ok := result2["output1"]; !ok {
		t.Error("output1 should be in device2 outputs")
	}
	if _, ok := result2["output2"]; !ok {
		t.Error("output2 should be in device2 outputs")
	}

	// Test with device4 (no outputs)
	result3 := buildDeviceOutputMap("device4", outputWriters, outputChannels)
	if len(result3) != 0 {
		t.Errorf("buildDeviceOutputMap for device4 returned %d outputs, want 0", len(result3))
	}
}

// TestSendDataPointToOutputs tests the sendDataPointToOutputs function
func TestSendDataPointToOutputs(t *testing.T) {
	logger := zerolog.Nop()

	dp := datasource.DataPoint{
		DeviceName: "device1",
		PointName:  "temp",
		Value:      23.5,
		Timestamp:  time.Now().UTC(),
		Unit:       "C",
	}

	outputCh := make(chan datasource.DataPoint, 10)
	deviceOutputs := map[string]chan<- datasource.DataPoint{
		"output1": outputCh,
	}

	// This should send the data point successfully
	sendDataPointToOutputs(dp, deviceOutputs, &logger)

	// Check that the data point was sent
	select {
	case received := <-outputCh:
		if received.DeviceName != dp.DeviceName {
			t.Errorf("Received device name = %v, want %v", received.DeviceName, dp.DeviceName)
		}
		if received.PointName != dp.PointName {
			t.Errorf("Received point name = %v, want %v", received.PointName, dp.PointName)
		}
	default:
		t.Error("Data point was not sent to output channel")
	}

	// Test with full channel
	fullCh := make(chan datasource.DataPoint, 1)
	fullCh <- datasource.DataPoint{}
	deviceOutputsFull := map[string]chan<- datasource.DataPoint{
		"output1": fullCh,
	}

	// This should log a warning about channel being full
	sendDataPointToOutputs(dp, deviceOutputsFull, &logger)
}

// TestLogChannelFullWarning tests the logChannelFullWarning function
func TestLogChannelFullWarning(t *testing.T) {
	logger := zerolog.Nop()

	dp := datasource.DataPoint{
		DeviceName: "device1",
		PointName:  "temp",
		Value:      23.5,
		Timestamp:  time.Now().UTC(),
		Unit:       "C",
	}

	// This should not panic
	logChannelFullWarning(dp, "test_output", &logger)
}

// TestCreateSingleDeviceReader tests the createSingleDeviceReader function
func TestCreateSingleDeviceReader(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name        string
		deviceType string
		wantNil    bool
		wantName   string
	}{
		{
			name:        "unknown device type",
			deviceType: "unknown_type",
			wantNil:    true,
		},
		{
			name:        "http device",
			deviceType: "http",
			wantNil:    false,
			wantName:   "http_device",
		},
		{
			name:        "modbus device - will fail without real connection",
			deviceType: "modbus",
			wantNil:    true, // Will fail to create without real Modbus connection
			wantName:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deviceConfig := &config.Device{
				Name: "http_device",
				Type: tt.deviceType,
				PollInterval: time.Second,
				Timeout:      5 * time.Second,
				Parallelism:  1,
			}

			// Set device name for the test
			if tt.wantName != "" {
				deviceConfig.Name = tt.wantName
			}

			// For http device, add required config
			if tt.deviceType == "http" {
				deviceConfig.DeviceSpecific = config.DeviceSpecific{
					Http: config.HttpConfig{
						Address:     "http://localhost:8080",
						Method:      "GET",
						ResponseType: "json",
					},
				}
				deviceConfig.Points = []config.Point{
					{Name: "temp", JsonPath: "temperature", Type: "float64"},
				}
			}

			// For modbus device, add required config
			if tt.deviceType == "modbus" {
				deviceConfig.DeviceSpecific = config.DeviceSpecific{
					Modbus: config.ModbusConfig{
						Address:      "tcp://localhost:502",
						SlaveID:      1,
						RegisterMode: "direct",
					},
				}
				deviceConfig.Points = []config.Point{
					{Name: "temp", Register: 0, Type: "int16"},
				}
			}

			result := createSingleDeviceReader(deviceConfig, &logger)

			if tt.wantNil {
				if result != nil {
					t.Errorf("createSingleDeviceReader() should return nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("createSingleDeviceReader() should return non-nil, got nil")
				} else if result.Name() != tt.wantName {
					t.Errorf("createSingleDeviceReader() Name() = %v, want %v", result.Name(), tt.wantName)
				}
			}
		})
	}
}

// TestCreateOutputWriterByType tests the createOutputWriterByType function
func TestCreateOutputWriterByType(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name    string
		outType string
		wantNil bool
		wantErr bool
	}{
		{
			name:    "unknown output type",
			outType: "unknown_type",
			wantNil: true,
			wantErr: true,
		},
		{
			name:    "csv output type",
			outType: "csv",
			wantNil: false,
			wantErr: false,
		},
		{
			name:    "influxdb output type - creates writer but will fail on Start",
			outType: "influxdb",
			wantNil: false, // Writer is created, but will fail on Start
			wantErr: false,
		},
		{
			name:    "mqtt output type - creates writer but will fail on Start",
			outType: "mqtt",
			wantNil: false, // Writer is created, but will fail on Start
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputConfig := &config.Output{
				Name:   "test_output",
				Type:   tt.outType,
				Devices: []string{"device1"},
			}

			// Add required config for each type
			if tt.outType == "csv" {
				outputConfig.OutputSpecific = config.OutputSpecific{
					Csv: config.CsvConfig{
						FilePath: "/tmp/test.csv",
					},
				}
			}
			if tt.outType == "influxdb" {
				outputConfig.OutputSpecific = config.OutputSpecific{
					Influxdb: config.InfluxdbConfig{
						Address:  "http://localhost:8086",
						Token:    "test-token",
						Database: "test-db",
						Insecure: true,
					},
				}
			}
			if tt.outType == "mqtt" {
				outputConfig.OutputSpecific = config.OutputSpecific{
					Mqtt: config.MqttConfig{
						Address:  "tcp://localhost:1883",
						ClientID: "test-client",
						Topic:    "datalogger",
						QoS:      1,
						Retain:   false,
					},
				}
			}

			result, err := createOutputWriterByType(outputConfig, &logger)

			if tt.wantNil {
				if result != nil {
					t.Errorf("createOutputWriterByType() result = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Errorf("createOutputWriterByType() result = nil, want non-nil")
				}
			}

			if tt.wantErr {
				if err == nil {
					t.Errorf("createOutputWriterByType() error = nil, want non-nil")
				}
			} else {
				if err != nil {
					t.Errorf("createOutputWriterByType() error = %v, want nil", err)
				}
			}
		})
	}
}

// TestWriterBufferSize tests the writerBufferSize function
func TestWriterBufferSize(t *testing.T) {
	cfg := &config.Config{
		Outputs: []config.Output{
			{
				Name:      "output1",
				Type:      "csv",
				BufferSize: 200,
				Devices:   []string{"device1"},
			},
			{
				Name:      "output2",
				Type:      "csv",
				BufferSize: 0, // Will use default of 1000
				Devices:   []string{"device1"},
			},
		},
	}

	// Test with output that has specific buffer size
	w1 := &mockOutputWriterForBuffer{name: "output1"}
	size1 := writerBufferSize(w1, cfg)
	if size1 != 200 {
		t.Errorf("writerBufferSize for output1 = %d, want 200", size1)
	}

	// Test with output that uses default buffer size
	w2 := &mockOutputWriterForBuffer{name: "output2"}
	size2 := writerBufferSize(w2, cfg)
	if size2 != 1000 {
		t.Errorf("writerBufferSize for output2 = %d, want 1000", size2)
	}

	// Test with no specific buffer size (should default to 1000)
	cfg2 := &config.Config{
		Outputs: []config.Output{
			{
				Name:      "output3",
				Type:      "csv",
				BufferSize: 0,
				Devices:   []string{"device1"},
			},
		},
	}

	w3 := &mockOutputWriterForBuffer{name: "output3"}
	size3 := writerBufferSize(w3, cfg2)
	if size3 != 1000 {
		t.Errorf("writerBufferSize for output3 with defaults = %d, want 1000", size3)
	}
}

// TestRouteDeviceToOutputs tests the routeDeviceToOutputs function
func TestRouteDeviceToOutputs(t *testing.T) {
	logger := zerolog.Nop()

	// Create a channel with some data
	deviceCh := make(chan datasource.DataPoint, 5)
	deviceCh <- datasource.DataPoint{
		DeviceName: "device1",
		PointName:  "temp",
		Value:      23.5,
		Timestamp:  time.Now().UTC(),
		Unit:       "C",
	}
	close(deviceCh)

	// Create mock output writers and channels
	outputWriters := []output.OutputWriter{
		&mockOutputWriterForMap{name: "output1", devices: []string{"device1"}},
	}

	// Use bidirectional channel for testing
	outputCh := make(chan datasource.DataPoint, 10)
	outputChannels := map[string]chan<- datasource.DataPoint{
		"output1": outputCh,
	}

	// This should process the data points
	routeDeviceToOutputs("device1", deviceCh, outputWriters, outputChannels, &logger)

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)

	// Check that the data point was routed
	select {
	case dp := <-outputCh:
		if dp.DeviceName != "device1" {
			t.Errorf("Routed device name = %v, want %v", dp.DeviceName, "device1")
		}
	default:
		t.Error("Data point was not routed to output channel")
	}
}

// TestProcessDataPoints tests the processDataPoints function
func TestProcessDataPoints(t *testing.T) {
	logger := zerolog.Nop()

	// Create a channel with some data
	deviceCh := make(chan datasource.DataPoint, 3)
	deviceCh <- datasource.DataPoint{DeviceName: "device1", PointName: "temp", Value: 23.5, Timestamp: time.Now().UTC(), Unit: "C"}
	deviceCh <- datasource.DataPoint{DeviceName: "device1", PointName: "humidity", Value: 60.0, Timestamp: time.Now().UTC(), Unit: "%"}
	close(deviceCh)

	// Use bidirectional channel for testing
	outputCh := make(chan datasource.DataPoint, 10)
	deviceOutputs := map[string]chan<- datasource.DataPoint{
		"output1": outputCh,
	}

	// This should process all data points
	processDataPoints(deviceCh, deviceOutputs, &logger)

	// Check that both data points were processed
	count := 0
	for {
		select {
		case <-outputCh:
			count++
		default:
			if count >= 2 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if count != 2 {
		t.Errorf("Processed %d data points, want 2", count)
	}
}

// TestStartOutputWriters tests the startOutputWriters function
func TestStartOutputWriters(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()

	// Create a temporary directory for CSV output
	tempDir := t.TempDir()
	filePath := tempDir + "/test.csv"

	cfg := &config.Config{
		Outputs: []config.Output{
			{
				Name:   "csv_output",
				Type:   "csv",
				Devices: []string{"device1"},
				OutputSpecific: config.OutputSpecific{
					Csv: config.CsvConfig{
						FilePath: filePath,
					},
				},
			},
		},
	}

	// This should work since CSV writer doesn't require external connections
	outputWriters, outputChannels, _ := startOutputWriters(cfg, ctx, &logger)

	if len(outputWriters) != 1 {
		t.Errorf("Expected 1 output writer, got %d", len(outputWriters))
	}

	if len(outputChannels) != 1 {
		t.Errorf("Expected 1 output channel, got %d", len(outputChannels))
	}

	if outputWriters[0].Name() != "csv_output" {
		t.Errorf("Writer name = %v, want %v", outputWriters[0].Name(), "csv_output")
	}

	// Cleanup: cancel context and wait for goroutines
	time.Sleep(100 * time.Millisecond)
}

// TestStartRouting tests the startRouting function
func TestStartRouting(t *testing.T) {
	logger := zerolog.Nop()

	// Create mock channels
	deviceCh := make(<-chan datasource.DataPoint)
	
	outputWriters := []output.OutputWriter{
		&mockOutputWriterForMap{name: "output1", devices: []string{"device1"}},
	}

	outputChannels := map[string]chan<- datasource.DataPoint{
		"output1": make(chan datasource.DataPoint, 10),
	}

	// This should start routing without panic
	startRouting(map[string]<-chan datasource.DataPoint{"device1": deviceCh}, outputWriters, outputChannels, &logger)

	// Give some time for goroutines to start
	time.Sleep(50 * time.Millisecond)
}

// TestLoadAndValidateConfig tests the loadAndValidateConfig function
func TestLoadAndValidateConfig(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := tempDir + "/test.yaml"
	filePath := tempDir + "/output.csv"

	// Write a minimal valid config
	configContent := "version: \"1.0\"\ndevices:\n  - name: test_device\n    type: http\n    poll_interval: 1s\n    timeout: 5s\n    parallelism: 1\n    device_specific:\n      http:\n        address: http://localhost:8080\n        method: GET\n        response_type: json\n    points:\n      - name: temp\n        json_path: temperature\n        type: float64\n        unit: C\n\noutputs:\n  - name: csv_output\n    type: csv\n    devices: [test_device]\n    output_specific:\n      csv:\n        file_path: " + filePath + "\n"
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	logger := zerolog.Nop()

	// Test with valid config
	cfg := loadAndValidateConfig(configPath, &logger)
	if cfg == nil {
		t.Fatal("loadAndValidateConfig returned nil")
	}

	if len(cfg.Devices) != 1 {
		t.Errorf("Devices count = %d, want 1", len(cfg.Devices))
	}

	if cfg.Devices[0].Name != "test_device" {
		t.Errorf("Device name = %v, want %v", cfg.Devices[0].Name, "test_device")
	}
}

// TestLoadAndValidateConfigInvalidPath tests loadAndValidateConfig with invalid path
func TestLoadAndValidateConfigInvalidPath(t *testing.T) {
	// Test with non-existent config file - should exit
	// We can't easily test os.Exit in tests, so we'll test the Load function directly
	// which is what loadAndValidateConfig calls
	_, err := config.Load("/nonexistent/path/to/config.yaml")
	if err == nil {
		t.Error("Expected error for non-existent config file")
	}
}

// TestLoadAndValidateConfigInvalidYAML tests loadAndValidateConfig with invalid YAML
func TestLoadAndValidateConfigInvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	configPath := tempDir + "/invalid.yaml"
	
	// Write invalid YAML
	invalidYAML := "this is not valid yaml: [[["
	if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	
	// Test Load directly
	_, err := config.Load(configPath)
	if err == nil {
		t.Error("Expected error for invalid YAML")
	}
}

// TestCreateDeviceReaders tests the createDeviceReaders function
func TestCreateDeviceReaders(t *testing.T) {
	logger := zerolog.Nop()

	cfg := &config.Config{
		Devices: []config.Device{
			{
				Name:         "http_device",
				Type:         "http",
				PollInterval: time.Second,
				Timeout:      5 * time.Second,
				Parallelism:  1,
				DeviceSpecific: config.DeviceSpecific{
					Http: config.HttpConfig{
						Address:     "http://localhost:8080",
						Method:      "GET",
						ResponseType: "json",
					},
				},
				Points: []config.Point{
					{Name: "temp", JsonPath: "temperature", Type: "float64"},
				},
			},
		},
	}

	// This should create HTTP readers
	readers := createDeviceReaders(cfg, &logger)

	if len(readers) != 1 {
		t.Errorf("Expected 1 reader, got %d", len(readers))
	}

	if readers[0].Name() != "http_device" {
		t.Errorf("Reader name = %v, want %v", readers[0].Name(), "http_device")
	}
}

// TestCreateOutputWriters tests the createOutputWriters function
func TestCreateOutputWriters(t *testing.T) {
	logger := zerolog.Nop()

	cfg := &config.Config{
		Outputs: []config.Output{
			{
				Name:   "csv_output",
				Type:   "csv",
				Devices: []string{"device1"},
				OutputSpecific: config.OutputSpecific{
					Csv: config.CsvConfig{
						FilePath: "/tmp/test.csv",
					},
				},
			},
		},
	}

	// This should work since CSV writer doesn't require external connections
	writers := createOutputWriters(cfg, &logger)

	if len(writers) != 1 {
		t.Errorf("Expected 1 writer, got %d", len(writers))
	}

	if writers[0].Name() != "csv_output" {
		t.Errorf("Writer name = %v, want %v", writers[0].Name(), "csv_output")
	}
}

// TestCreateSingleOutputWriter tests the createSingleOutputWriter function
func TestCreateSingleOutputWriter(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name       string
		outputType string
		configFn   func() config.Output
		wantNil    bool
	}{
		{
			name:       "csv output - success",
			outputType: "csv",
			configFn: func() config.Output {
				return config.Output{
					Name:   "csv_output",
					Type:   "csv",
					Devices: []string{"device1"},
					OutputSpecific: config.OutputSpecific{
						Csv: config.CsvConfig{
							FilePath: "/tmp/test.csv",
						},
					},
				}
			},
			wantNil: false,
		},
		{
			name:       "unknown output type - returns nil from createOutputWriterByType",
			outputType: "unknown",
			configFn: func() config.Output {
				return config.Output{
					Name:   "unknown_output",
					Type:   "unknown",
					Devices: []string{"device1"},
				}
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputConfig := tt.configFn()
			writer := createSingleOutputWriter(&outputConfig, &logger)

			if tt.wantNil {
				if writer != nil {
					t.Errorf("createSingleOutputWriter() = %v, want nil", writer)
				}
			} else {
				if writer == nil {
					t.Fatal("createSingleOutputWriter() returned nil")
				}
				if writer.Name() != outputConfig.Name {
					t.Errorf("Writer name = %v, want %v", writer.Name(), outputConfig.Name)
				}
			}
		})
	}
}

// TestStartDeviceReadersWithRealHTTP tests startDeviceReaders with real HTTP reader
func TestStartDeviceReadersWithRealHTTP(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()

	cfg := &config.Config{
		Devices: []config.Device{
			{
				Name:         "http_device",
				Type:         "http",
				PollInterval: time.Second,
				Timeout:      5 * time.Second,
				Parallelism:  1,
				DeviceSpecific: config.DeviceSpecific{
					Http: config.HttpConfig{
						Address:     "http://localhost:8080",
						Method:      "GET",
						ResponseType: "json",
					},
				},
				Points: []config.Point{
					{Name: "temp", JsonPath: "temperature", Type: "float64"},
				},
			},
		},
	}

	readers := createDeviceReaders(cfg, &logger)
	if len(readers) == 0 {
		t.Fatal("Failed to create readers")
	}

	// Start the readers
	deviceChannels, _ := startDeviceReaders(cfg, ctx, &logger)
	
	if len(deviceChannels) != 1 {
		t.Errorf("Expected 1 device channel, got %d", len(deviceChannels))
	}

	// Cleanup: close context
	// Note: We can't easily test the actual data flow without a real HTTP server
}

// TestRouteDeviceToOutputsNoOutputs tests routeDeviceToOutputs with no matching outputs
func TestRouteDeviceToOutputsNoOutputs(t *testing.T) {
	logger := zerolog.Nop()

	// Create a channel with some data
	deviceCh := make(chan datasource.DataPoint, 5)
	deviceCh <- datasource.DataPoint{
		DeviceName: "device1",
		PointName:  "temp",
		Value:      23.5,
		Timestamp:  time.Now().UTC(),
		Unit:       "C",
	}
	close(deviceCh)

	// Create mock output writers and channels for a different device
	outputWriters := []output.OutputWriter{
		&mockOutputWriterForMap{name: "output1", devices: []string{"device2"}}, // Different device
	}

	// Use bidirectional channel for testing
	outputCh := make(chan datasource.DataPoint, 10)
	outputChannels := map[string]chan<- datasource.DataPoint{
		"output1": outputCh,
	}

	// This should not route any data points since device1 doesn't match any outputs
	routeDeviceToOutputs("device1", deviceCh, outputWriters, outputChannels, &logger)

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)

	// Check that no data was routed to output1
	select {
	case dp := <-outputCh:
		t.Errorf("Data point was routed to output1 but shouldn't be: %v", dp)
	default:
		// Success - no data was routed
	}
}

// TestParseFlags tests the parseFlags function
// Note: This test is tricky because parseFlags uses global variables and flag.CommandLine
// which can only be initialized once. We test the flag parsing logic indirectly.
func TestParseFlags(t *testing.T) {
	// Save original args and flags
	originalArgs := os.Args
	originalConfigPath := configPath
	originalDebugFlag := debugFlag
	originalVersionFlag := versionFlag
	
	// Restore original values after test
	defer func() {
		os.Args = originalArgs
		configPath = originalConfigPath
		debugFlag = originalDebugFlag
		versionFlag = originalVersionFlag
	}()
	
	// Test with -config flag
	os.Args = []string{"datalogger", "-config", "test.yaml"}
	
	// Reset global flag variables
	configPath = ""
	debugFlag = false
	versionFlag = false
	
	// We can't call parseFlags directly in a test because it registers
	// flags with the global flag.CommandLine, which can only be done once
	// So we'll test the flag parsing logic by checking os.Args parsing
	// This is a limitation of the flag package design
	t.Skip("Skipping parseFlags test - requires flag package reset between tests")
}



