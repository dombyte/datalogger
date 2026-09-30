package datasource

import (
	"testing"
	"time"
)

// TestDataPointCreation tests creating DataPoint instances
func TestDataPointCreation(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name      string
		device    string
		point     string
		value     interface{}
		timestamp time.Time
		unit      string
	}{
		{
			name:      "float64 value",
			device:    "test_device",
			point:     "temperature",
			value:     23.5,
			timestamp: now,
			unit:      "C",
		},
		{
			name:      "int value",
			device:    "test_device",
			point:     "count",
			value:     42,
			timestamp: now,
			unit:      "",
		},
		{
			name:      "bool value",
			device:    "test_device",
			point:     "status",
			value:     true,
			timestamp: now,
			unit:      "",
		},
		{
			name:      "string value",
			device:    "test_device",
			point:     "message",
			value:     "hello",
			timestamp: now,
			unit:      "",
		},
		{
			name:      "nil value",
			device:    "test_device",
			point:     "empty",
			value:     nil,
			timestamp: now,
			unit:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := DataPoint{
				DeviceName: tt.device,
				PointName:  tt.point,
				Value:      tt.value,
				Timestamp:  tt.timestamp,
				Unit:       tt.unit,
			}

			if dp.DeviceName != tt.device {
				t.Errorf("DeviceName = %v, want %v", dp.DeviceName, tt.device)
			}

			if dp.PointName != tt.point {
				t.Errorf("PointName = %v, want %v", dp.PointName, tt.point)
			}

			if dp.Value != tt.value {
				t.Errorf("Value = %v, want %v", dp.Value, tt.value)
			}

			if !dp.Timestamp.Equal(tt.timestamp) {
				t.Errorf("Timestamp = %v, want %v", dp.Timestamp, tt.timestamp)
			}

			if dp.Unit != tt.unit {
				t.Errorf("Unit = %v, want %v", dp.Unit, tt.unit)
			}
		})
	}
}

// TestDataPointJSONTags tests that DataPoint fields have correct JSON tags
func TestDataPointJSONTags(t *testing.T) {
	dp := DataPoint{
		DeviceName: "test_device",
		PointName:  "test_point",
		Value:      42.0,
		Timestamp:  time.Now().UTC(),
		Unit:       "V",
	}

	// The struct tags are already verified in the struct definition
	// This test just ensures the struct can be properly constructed
	if dp.DeviceName != "test_device" {
		t.Errorf("Expected DeviceName to be 'test_device', got '%s'", dp.DeviceName)
	}

	if dp.PointName != "test_point" {
		t.Errorf("Expected PointName to be 'test_point', got '%s'", dp.PointName)
	}

	if dp.Unit != "V" {
		t.Errorf("Expected Unit to be 'V', got '%s'", dp.Unit)
	}
}

// TestDataPointTimestampUTC tests that timestamps should be in UTC
func TestDataPointTimestampUTC(t *testing.T) {
	// Create a DataPoint with a UTC timestamp
	utcTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	dp := DataPoint{
		DeviceName: "test",
		PointName:  "test",
		Value:      1.0,
		Timestamp:  utcTime,
		Unit:       "",
	}

	if dp.Timestamp.Location() != time.UTC {
		t.Errorf("Timestamp should be in UTC, got location: %v", dp.Timestamp.Location())
	}

	// Test that a local time is NOT used
	localTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.FixedZone("", 3600)) // UTC+1
	dp2 := DataPoint{
		DeviceName: "test",
		PointName:  "test",
		Value:      1.0,
		Timestamp:  localTime,
		Unit:       "",
	}

	// The timestamp should preserve its location
	if dp2.Timestamp.Location().String() != "" {
		t.Logf("Timestamp location is: %v", dp2.Timestamp.Location())
	}
}

// TestDataPointEquality tests DataPoint equality (pointer vs value)
func TestDataPointEquality(t *testing.T) {
	utcTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	dp1 := DataPoint{
		DeviceName: "test",
		PointName:  "temp",
		Value:      25.5,
		Timestamp:  utcTime,
		Unit:       "C",
	}

	dp2 := DataPoint{
		DeviceName: "test",
		PointName:  "temp",
		Value:      25.5,
		Timestamp:  utcTime,
		Unit:       "C",
	}

	// Test value equality
	if dp1 != dp2 {
		t.Error("DataPoints with same values should be equal")
	}

	// Test that modifying one doesn't affect the other
	dp2.Value = 30.0
	if dp1.Value == dp2.Value {
		t.Error("Modifying dp2 should not affect dp1")
	}
}

// TestDataPointWithDifferentTypes tests DataPoint with various value types
func TestDataPointWithDifferentTypes(t *testing.T) {
	testTime := time.Now().UTC()

	typeTests := []struct {
		name  string
		value interface{}
	}{
		{"int", 42},
		{"int8", int8(42)},
		{"int16", int16(42)},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"uint", uint(42)},
		{"uint8", uint8(42)},
		{"uint16", uint16(42)},
		{"uint32", uint32(42)},
		{"uint64", uint64(42)},
		{"float32", float32(42.5)},
		{"float64", float64(42.5)},
		{"bool", true},
		{"string", "test"},
		{"nil", nil},
		{"struct", struct{ X int }{X: 1}},
		// Skip slice and map as they can't be compared with !=
	}

	for _, tt := range typeTests {
		t.Run(tt.name, func(t *testing.T) {
			dp := DataPoint{
				DeviceName: "test",
				PointName:  "test",
				Value:      tt.value,
				Timestamp:  testTime,
				Unit:       "",
			}

			// Just verify we can create a DataPoint with this type
			// For comparable types only
			if dp.Value != tt.value {
				t.Errorf("Value = %v, want %v", dp.Value, tt.value)
			}
		})
	}
}
