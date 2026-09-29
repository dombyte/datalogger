package output

import (
	"context"
	"testing"

	"github.com/dombyte/datalogger/datasource"
)

// MockOutputWriter is a mock implementation of OutputWriter for testing
type MockOutputWriter struct {
	name        string
	devices     []string
	validateErr error
	startErr    error
}

func (m *MockOutputWriter) Name() string {
	return m.name
}

func (m *MockOutputWriter) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	if m.startErr != nil {
		ch := make(chan error, 1)
		ch <- m.startErr
		close(ch)
		return ch
	}
	return make(<-chan error)
}

func (m *MockOutputWriter) Validate() error {
	return m.validateErr
}

func (m *MockOutputWriter) Devices() []string {
	return m.devices
}

// TestOutputWriterInterface tests that the OutputWriter interface is properly defined
func TestOutputWriterInterface(t *testing.T) {
	// Create a mock writer
	mock := &MockOutputWriter{
		name:    "mock",
		devices: []string{"device1", "device2"},
	}

	// Verify it implements OutputWriter interface
	var _ OutputWriter = mock

	// Test Name method
	if mock.Name() != "mock" {
		t.Errorf("Name() = %v, want %v", mock.Name(), "mock")
	}

	// Test Devices method
	devices := mock.Devices()
	if len(devices) != 2 {
		t.Errorf("Devices() returned %d devices, want 2", len(devices))
	}

	// Test Validate method
	if err := mock.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}

	// Test Start method (can't fully test without context)
	ch := mock.Start(nil, nil)
	if ch == nil {
		t.Error("Start() returned nil channel")
	}
}

// TestDataPointStruct tests that DataPoint from datasource package works correctly
func TestDataPointStruct(t *testing.T) {
	// This is more of a compilation test - verify we can use datasource.DataPoint
	dp := datasource.DataPoint{
		DeviceName: "test_device",
		PointName:  "test_point",
		Value:      42.0,
		Unit:       "V",
	}

	if dp.DeviceName != "test_device" {
		t.Errorf("DeviceName = %v, want %v", dp.DeviceName, "test_device")
	}

	if dp.PointName != "test_point" {
		t.Errorf("PointName = %v, want %v", dp.PointName, "test_point")
	}

	if dp.Value != 42.0 {
		t.Errorf("Value = %v, want %v", dp.Value, 42.0)
	}

	if dp.Unit != "V" {
		t.Errorf("Unit = %v, want %v", dp.Unit, "V")
	}
}

// TestOutputWriterWithErrors tests error handling in OutputWriter
func TestOutputWriterWithErrors(t *testing.T) {
	tests := []struct {
		name        string
		validateErr error
		wantErr     bool
	}{
		{
			name:        "no error",
			validateErr: nil,
			wantErr:     false,
		},
		{
			name:        "validation error",
			validateErr: &testError{msg: "validation failed"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &MockOutputWriter{
				name:        "mock",
				devices:     []string{"device1"},
				validateErr: tt.validateErr,
			}

			err := mock.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// testError is a simple error for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// TestOutputWriterDevices tests the Devices method
func TestOutputWriterDevices(t *testing.T) {
	tests := []struct {
		name    string
		devices []string
	}{
		{
			name:    "empty devices",
			devices: []string{},
		},
		{
			name:    "single device",
			devices: []string{"device1"},
		},
		{
			name:    "multiple devices",
			devices: []string{"device1", "device2", "device3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &MockOutputWriter{
				name:    "mock",
				devices: tt.devices,
			}

			devices := mock.Devices()
			if len(devices) != len(tt.devices) {
				t.Errorf("Devices() returned %d devices, want %d", len(devices), len(tt.devices))
			}

			for i, d := range devices {
				if d != tt.devices[i] {
					t.Errorf("Device %d = %v, want %v", i, d, tt.devices[i])
				}
			}
		})
	}
}

// TestOutputWriterName tests the Name method
func TestOutputWriterName(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"writer1"},
		{"my_output_writer"},
		{""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &MockOutputWriter{
				name:    tt.name,
				devices: []string{},
			}

			name := mock.Name()
			if name != tt.name {
				t.Errorf("Name() = %v, want %v", name, tt.name)
			}
		})
	}
}
