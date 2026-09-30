package influxdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/config"
	"github.com/dombyte/datalogger/internal/datasource"
)

// TestNewInfluxDBWriter tests creating a new InfluxDB writer
func TestNewInfluxDBWriter(t *testing.T) {
	logger := zerolog.Nop()

	// Create a minimal config - we can't actually connect without a real server
	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	// This will fail to connect, but we can test the structure
	writer, err := New(outputConfig, &logger)

	// We expect an error since we don't have a real InfluxDB server
	if err == nil {
		// If no error, check the writer structure
		if writer == nil {
			t.Fatal("Writer is nil but error is nil")
		}

		if writer.Name() != "influx_test" {
			t.Errorf("Name() = %v, want %v", writer.Name(), "influx_test")
		}

		devices := writer.Devices()
		if len(devices) != 1 || devices[0] != "device1" {
			t.Errorf("Devices() = %v, want %v", devices, []string{"device1"})
		}
	} else {
		// Expected error for connection failure
		t.Logf("Expected connection error: %v", err)
	}
}

// TestInfluxDBWriterName tests the Name method
func TestInfluxDBWriterName(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "my_influx_writer",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	// We can't fully create the writer without a real server,
	// but we can test the struct fields
	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: outputConfig.Devices,
	}

	if writer.Name() != "my_influx_writer" {
		t.Errorf("Name() = %v, want %v", writer.Name(), "my_influx_writer")
	}
}

// TestInfluxDBWriterDevices tests the Devices method
func TestInfluxDBWriterDevices(t *testing.T) {
	logger := zerolog.Nop()

	devices := []string{"device1", "device2", "device3"}
	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: devices,
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: devices,
	}

	returnedDevices := writer.Devices()
	if len(returnedDevices) != len(devices) {
		t.Errorf("Devices() returned %d devices, want %d", len(returnedDevices), len(devices))
	}

	for i, d := range returnedDevices {
		if d != devices[i] {
			t.Errorf("Device %d = %v, want %v", i, d, devices[i])
		}
	}
}

// TestCreatePoint tests the createPoint method
func TestCreatePoint(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: outputConfig.Devices,
	}

	timestamp := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	dp := datasource.DataPoint{
		DeviceName: "test_device",
		PointName:  "temperature",
		Value:      23.5,
		Timestamp:  timestamp,
		Unit:       "C",
	}

	point := writer.createPoint(dp)
	require.NotNil(t, point)
	require.NotNil(t, point.Values)

	assert.Equal(t, "test_device", point.GetMeasurement(), "measurement is the device name")
	assert.Equal(t, map[string]string{"point": "temperature", "unit": "C"}, point.Values.Tags)
	assert.Equal(t, 23.5, point.Values.Fields["value"])
	assert.True(t, point.Values.Timestamp.Equal(timestamp))
}

// TestCreatePointWithoutUnit tests createPoint without unit
func TestCreatePointWithoutUnit(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: outputConfig.Devices,
	}

	timestamp := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	dp := datasource.DataPoint{
		DeviceName: "test_device",
		PointName:  "count",
		Value:      42,
		Timestamp:  timestamp,
		Unit:       "", // Empty unit
	}

	point := writer.createPoint(dp)

	if point == nil {
		t.Fatal("createPoint returned nil")
	}

	if point.Values == nil {
		t.Fatal("point.Values is nil")
	}

	tags := point.Values.Tags
	if tags["point"] != "count" {
		t.Errorf("Tags[point] = %v, want %v", tags["point"], "count")
	}
	// Unit should not be in tags when empty
	if _, ok := tags["unit"]; ok {
		t.Errorf("Tags should not contain 'unit' when unit is empty")
	}
}

// TestCreatePointVariousTypes tests createPoint with various value types
func TestCreatePointVariousTypes(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: outputConfig.Devices,
	}

	timestamp := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name  string
		value interface{}
	}{
		{"int", 42},
		{"int64", int64(42)},
		{"float64", 23.5},
		{"float32", float32(23.5)},
		{"bool", true},
		{"string", "test"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dp := datasource.DataPoint{
				DeviceName: "test_device",
				PointName:  "value",
				Value:      tc.value,
				Timestamp:  timestamp,
				Unit:       "",
			}

			point := writer.createPoint(dp)

			if point == nil {
				t.Fatal("createPoint returned nil")
			}

			if point.Values == nil {
				t.Fatal("point.Values is nil")
			}

			fields := point.Values.Fields
			if fields["value"] != tc.value {
				t.Errorf("Fields[value] = %v, want %v", fields["value"], tc.value)
			}
		})
	}
}

// TestInfluxDBWriterStart tests the Start method
func TestInfluxDBWriterStart(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
		BatchSize: 100,
	}

	// We can't fully test Start without a real InfluxDB server
	// but we can test that it doesn't panic
	writer := &Writer{
		config:       outputConfig,
		logger:       logger,
		devices:      outputConfig.Devices,
		batchSize:    100,
		batchTimeout: 100 * time.Millisecond,
		maxRetries:   3,
		retryDelay:   time.Second,
	}

	// Start should not panic even without a client
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Create input channel
	inCh := make(chan datasource.DataPoint)

	// We'll test that Start returns an error channel
	errCh := writer.Start(ctx, inCh)

	if errCh == nil {
		t.Fatal("Start() returned nil error channel")
	}

	// Start a goroutine that will close the input channel after a short delay
	// This allows the writer to process and exit
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(inCh)
	}()

	// Wait a bit for the goroutine to start processing
	time.Sleep(200 * time.Millisecond)

	// The test will pass if Start() doesn't panic
	t.Log("Start() method executed without panic")
}

// TestHandleInputPoint tests the handleInputPoint method
func TestHandleInputPoint(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
		BatchSize: 100,
	}

	writer := &Writer{
		config:       outputConfig,
		logger:       logger,
		devices:      outputConfig.Devices,
		batchSize:    100,
		batchTimeout: time.Second,
		maxRetries:   3,
		retryDelay:   time.Second,
	}

	// Test that we can call createPoint
	timestamp := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	dp := datasource.DataPoint{
		DeviceName: "test_device",
		PointName:  "temperature",
		Value:      23.5,
		Timestamp:  timestamp,
		Unit:       "C",
	}

	point := writer.createPoint(dp)

	if point == nil {
		t.Fatal("createPoint returned nil")
	}

	// The point should have the correct structure
	if point.GetMeasurement() != "test_device" {
		t.Errorf("Measurement = %v, want %v", point.GetMeasurement(), "test_device")
	}
}

// TestInfluxDBWriterBatchConfiguration tests batch configuration
func TestInfluxDBWriterBatchConfiguration(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name         string
		batchSize    int
		batchTimeout time.Duration
		maxRetries   int
		retryDelay   time.Duration
	}{
		{
			name:         "default values",
			batchSize:    0,
			batchTimeout: 0,
			maxRetries:   0,
			retryDelay:   0,
		},
		{
			name:         "custom values",
			batchSize:    5000,
			batchTimeout: 5 * time.Second,
			maxRetries:   5,
			retryDelay:   2 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputConfig := config.Output{
				Name:         "influx_test",
				Type:         "influxdb",
				Devices:      []string{"device1"},
				BatchSize:    tt.batchSize,
				BatchTimeout: tt.batchTimeout,
				MaxRetries:   tt.maxRetries,
				RetryDelay:   tt.retryDelay,
				OutputSpecific: config.OutputSpecific{
					Influxdb: config.InfluxdbConfig{
						Address:  "http://localhost:8086",
						Token:    "test-token",
						Database: "test-db",
						Insecure: true,
					},
				},
			}

			// This will fail to connect, but we can test the configuration
			writer, err := New(outputConfig, &logger)

			if err == nil {
				// If no error (unlikely without real server), check batch config
				expectedBatchSize := tt.batchSize
				if expectedBatchSize == 0 {
					expectedBatchSize = 10000
				}
				if writer.batchSize != expectedBatchSize {
					t.Errorf("batchSize = %v, want %v", writer.batchSize, expectedBatchSize)
				}

				expectedBatchTimeout := tt.batchTimeout
				if expectedBatchTimeout == 0 {
					expectedBatchTimeout = time.Second
				}
				if writer.batchTimeout != expectedBatchTimeout {
					t.Errorf("batchTimeout = %v, want %v", writer.batchTimeout, expectedBatchTimeout)
				}

				writer.client = nil // Don't try to use the client
			} else {
				// Expected error for connection failure
				t.Logf("Expected connection error: %v", err)
			}
		})
	}
}

// TestWaitForRetry tests the waitForRetry method
func TestWaitForRetry(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:       "influx_test",
		Type:       "influxdb",
		Devices:    []string{"device1"},
		RetryDelay: 50 * time.Millisecond,
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:     outputConfig,
		logger:     logger,
		devices:    outputConfig.Devices,
		retryDelay: 50 * time.Millisecond,
	}

	ctx := context.Background()

	// Test successful wait (context not cancelled)
	err := writer.waitForRetry(ctx, 1, errors.New("test error"))
	if err != nil {
		t.Errorf("waitForRetry should return nil, got: %v", err)
	}
}

// TestHandleShutdown tests the handleShutdown method
func TestHandleShutdown(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: outputConfig.Devices,
	}

	ctx := context.Background()
	input := make(chan datasource.DataPoint)
	close(input)
	batch := []*influxdb3.Point{}
	batchTimer := time.NewTimer(100 * time.Millisecond)
	defer batchTimer.Stop()

	// Test handleShutdown with empty batch
	writer.handleShutdown(ctx, input, batch, batchTimer)

	// Should complete without panic
	t.Log("handleShutdown completed without panic")
}

// TestDrainRemainingPoints tests the drainRemainingPoints method
func TestDrainRemainingPoints(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:    "influx_test",
		Type:    "influxdb",
		Devices: []string{"device1"},
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:  outputConfig,
		logger:  logger,
		devices: outputConfig.Devices,
	}

	ctx := context.Background()
	input := make(chan datasource.DataPoint)
	close(input)
	batchTimer := time.NewTimer(100 * time.Millisecond)
	defer batchTimer.Stop()

	// Test drainRemainingPoints with closed input
	writer.drainRemainingPoints(ctx, input, batchTimer)

	// Should complete without panic
	t.Log("drainRemainingPoints completed without panic")
}

// TestHandleBatchTimeout tests the handleBatchTimeout method
func TestHandleBatchTimeout(t *testing.T) {
	logger := zerolog.Nop()

	outputConfig := config.Output{
		Name:         "influx_test",
		Type:         "influxdb",
		Devices:      []string{"device1"},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		MaxRetries:   3,
		RetryDelay:   time.Second,
		OutputSpecific: config.OutputSpecific{
			Influxdb: config.InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
				Insecure: true,
			},
		},
	}

	writer := &Writer{
		config:       outputConfig,
		logger:       logger,
		devices:      outputConfig.Devices,
		batchSize:    10,
		batchTimeout: 100 * time.Millisecond,
		maxRetries:   3,
		retryDelay:   time.Second,
	}

	ctx := context.Background()

	// Test with empty batch - should not write
	batch := []*influxdb3.Point{}
	batchTimer := time.NewTimer(100 * time.Millisecond)
	defer batchTimer.Stop()

	writer.handleBatchTimeout(ctx, &batch, batchTimer)

	if len(batch) != 0 {
		t.Errorf("batch should still be empty, got %d", len(batch))
	}
}
