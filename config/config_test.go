package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLoadConfig tests loading a valid configuration file
func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
devices:
  - name: test_device
    type: http
    poll_interval: 1s
    timeout: 5s
    parallelism: 1
    device_specific:
      http:
        address: "http://localhost:8080"
        method: GET
        response_type: json
    points:
      - name: temp
        json_path: "temperature"
        type: float64
        unit: "C"
outputs:
  - name: test_output
    type: csv
    devices:
      - test_device
    output_specific:
      csv:
        file_path: "/tmp/test.csv"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg == nil {
		t.Fatal("Config is nil")
	}

	if len(cfg.Devices) != 1 {
		t.Errorf("Expected 1 device, got %d", len(cfg.Devices))
	}

	if cfg.Devices[0].Name != "test_device" {
		t.Errorf("Expected device name 'test_device', got '%s'", cfg.Devices[0].Name)
	}

	if len(cfg.Outputs) != 1 {
		t.Errorf("Expected 1 output, got %d", len(cfg.Outputs))
	}
}

// TestLoadConfigFileNotFound tests error handling for missing config file
func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("Expected error for nonexistent file")
	}
}

// TestLoadConfigInvalidYAML tests error handling for invalid YAML
func TestLoadConfigInvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "invalid.yaml")

	// Write invalid YAML
	if err := os.WriteFile(configPath, []byte("invalid: yaml: content:"), 0o644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Fatal("Expected error for invalid YAML")
	}
}

// TestConfigValidate tests the Validate method
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Devices: []Device{
					{
						Name:         "test",
						Type:         "http",
						PollInterval: 1 * time.Second,
						Parallelism:  1,
						DeviceSpecific: DeviceSpecific{
							HTTP: HTTPConfig{
								Address:      "http://localhost",
								Method:       "GET",
								ResponseType: "json",
							},
						},
						Points: []Point{{Name: "temp", JSONPath: "temp"}},
					},
				},
				Outputs: []Output{
					{
						Name:    "csv_out",
						Type:    "csv",
						Devices: []string{"test"},
						OutputSpecific: OutputSpecific{
							Csv: CsvConfig{
								FilePath: "test.csv",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "duplicate device names",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second, Parallelism: 1},
					{Name: "test", Type: "http", PollInterval: 1 * time.Second, Parallelism: 1},
				},
			},
			wantErr: true,
		},
		{
			name: "empty device name",
			config: &Config{
				Devices: []Device{
					{Name: "", Type: "http", PollInterval: 1 * time.Second, Parallelism: 1},
				},
			},
			wantErr: true,
		},
		{
			name: "output references unknown device",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second, Parallelism: 1},
				},
				Outputs: []Output{
					{Name: "out", Type: "csv", Devices: []string{"unknown"}},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid device type",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "invalid", PollInterval: 1 * time.Second, Parallelism: 1},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid output type",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second, Parallelism: 1},
				},
				Outputs: []Output{
					{Name: "out", Type: "invalid", Devices: []string{"test"}},
				},
			},
			wantErr: true,
		},
		{
			name: "zero poll interval",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 0, Parallelism: 1},
				},
			},
			wantErr: true,
		},
		{
			name: "parallelism out of range (0)",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second, Parallelism: 0},
				},
			},
			wantErr: true,
		},
		{
			name: "parallelism out of range (101)",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second, Parallelism: 101},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestDeviceValidate tests Device.Validate
func TestDeviceValidate(t *testing.T) {
	tests := []struct {
		name    string
		device  Device
		wantErr bool
	}{
		{
			name: "valid http device",
			device: Device{
				Name:           "test",
				Type:           "http",
				PollInterval:   1 * time.Second,
				Parallelism:    1,
				DeviceSpecific: DeviceSpecific{HTTP: HTTPConfig{Address: "http://localhost"}},
				Points:         []Point{{Name: "temp", JSONPath: "temp"}},
			},
			wantErr: false,
		},
		{
			name: "valid modbus device",
			device: Device{
				Name:         "test",
				Type:         "modbus",
				PollInterval: 1 * time.Second,
				Parallelism:  1,
				DeviceSpecific: DeviceSpecific{
					Modbus: ModbusConfig{
						Address:      "tcp://localhost:502",
						SlaveID:      1,
						RegisterMode: "direct",
					},
				},
				Points: []Point{{Name: "power", Register: 1, Type: "uint16", Scale: 1}},
			},
			wantErr: false,
		},
		{
			name:    "empty type",
			device:  Device{Name: "test", PollInterval: 1 * time.Second, Parallelism: 1},
			wantErr: true,
		},
		{
			name:    "zero poll interval",
			device:  Device{Name: "test", Type: "http", PollInterval: 0, Parallelism: 1},
			wantErr: true,
		},
		{
			name:    "negative poll interval",
			device:  Device{Name: "test", Type: "http", PollInterval: -1 * time.Second, Parallelism: 1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.device.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestModbusConfigValidate tests ModbusConfig.Validate
func TestModbusConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  ModbusConfig
		wantErr bool
	}{
		{
			name: "valid modbus tcp",
			config: ModbusConfig{
				Address:      "tcp://localhost:502",
				SlaveID:      1,
				RegisterMode: "direct",
			},
			wantErr: false,
		},
		{
			name: "valid modbus rtu",
			config: ModbusConfig{
				Address:      "rtu:///dev/ttyUSB0",
				SlaveID:      1,
				RegisterMode: "direct",
				Speed:        9600,
				DataBits:     8,
				Parity:       "N",
				StopBits:     1,
			},
			wantErr: false,
		},
		{
			name: "valid range mode",
			config: ModbusConfig{
				Address:      "tcp://localhost:502",
				SlaveID:      1,
				RegisterMode: "range",
				Ranges:       []string{"100-200", "300-400"},
			},
			wantErr: false,
		},
		{
			name:    "empty address",
			config:  ModbusConfig{SlaveID: 1, RegisterMode: "direct"},
			wantErr: true,
		},
		{
			name:    "zero slave ID",
			config:  ModbusConfig{Address: "tcp://localhost:502", RegisterMode: "direct"},
			wantErr: true,
		},
		{
			name:    "empty register mode",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1},
			wantErr: true,
		},
		{
			name:    "invalid register mode",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "invalid"},
			wantErr: true,
		},
		{
			name:    "range mode without ranges",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range"},
			wantErr: true,
		},
		{
			name:    "invalid range format",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range", Ranges: []string{"invalid"}},
			wantErr: true,
		},
		{
			name:    "range start > end",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range", Ranges: []string{"200-100"}},
			wantErr: false, // parseRanges doesn't validate start > end, validateAddresses does
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestHttpConfigValidate tests HTTPConfig.Validate
func TestHttpConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  HTTPConfig
		wantErr bool
	}{
		{
			name: "valid http config",
			config: HTTPConfig{
				Address:      "http://localhost:8080",
				Method:       "GET",
				ResponseType: "json",
			},
			wantErr: false,
		},
		{
			name: "valid http config with defaults",
			config: HTTPConfig{
				Address: "http://localhost:8080",
			},
			wantErr: false,
		},
		{
			name:    "empty address",
			config:  HTTPConfig{Method: "GET"},
			wantErr: true,
		},
		{
			name:    "invalid method",
			config:  HTTPConfig{Address: "http://localhost:8080", Method: "PUT"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestOutputValidate tests Output.Validate
func TestOutputValidate(t *testing.T) {
	tests := []struct {
		name    string
		output  Output
		wantErr bool
	}{
		{
			name: "valid influxdb output",
			output: Output{
				Name:    "influx",
				Type:    "influxdb",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Influxdb: InfluxdbConfig{
						Address:  "http://localhost:8086",
						Token:    "test-token",
						Database: "test-db",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid mqtt output",
			output: Output{
				Name:    "mqtt",
				Type:    "mqtt",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Mqtt: MqttConfig{
						Address: "tcp://localhost:1883",
						Topic:   "test",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid csv output",
			output: Output{
				Name:    "csv",
				Type:    "csv",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Csv: CsvConfig{
						FilePath: "test.csv",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "empty name",
			output:  Output{Type: "csv", Devices: []string{"test"}},
			wantErr: true,
		},
		{
			name:    "empty type",
			output:  Output{Name: "test", Devices: []string{"test"}},
			wantErr: true,
		},
		{
			name:    "empty devices",
			output:  Output{Name: "test", Type: "csv"},
			wantErr: true,
		},
		{
			name: "influxdb missing address",
			output: Output{
				Name:    "influx",
				Type:    "influxdb",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Influxdb: InfluxdbConfig{
						Token:    "test-token",
						Database: "test-db",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "influxdb missing token",
			output: Output{
				Name:    "influx",
				Type:    "influxdb",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Influxdb: InfluxdbConfig{
						Address:  "http://localhost:8086",
						Database: "test-db",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "influxdb missing database",
			output: Output{
				Name:    "influx",
				Type:    "influxdb",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Influxdb: InfluxdbConfig{
						Address: "http://localhost:8086",
						Token:   "test-token",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "mqtt missing address",
			output: Output{
				Name:    "mqtt",
				Type:    "mqtt",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Mqtt: MqttConfig{
						Topic: "test",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "mqtt invalid qos",
			output: Output{
				Name:    "mqtt",
				Type:    "mqtt",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Mqtt: MqttConfig{
						Address: "tcp://localhost:1883",
						Topic:   "test",
						QoS:     3,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "csv missing file path",
			output: Output{
				Name:    "csv",
				Type:    "csv",
				Devices: []string{"test"},
				OutputSpecific: OutputSpecific{
					Csv: CsvConfig{},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.output.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestMqttConfigValidate tests MqttConfig.Validate
func TestMqttConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  MqttConfig
		wantErr bool
	}{
		{
			name: "valid mqtt config",
			config: MqttConfig{
				Address:  "tcp://localhost:1883",
				Topic:    "test",
				ClientID: "client-1",
				QoS:      1,
			},
			wantErr: false,
		},
		{
			name: "valid mqtt with defaults",
			config: MqttConfig{
				Address: "tcp://localhost:1883",
			},
			wantErr: false,
		},
		{
			name:    "empty address",
			config:  MqttConfig{Topic: "test"},
			wantErr: true,
		},
		{
			name: "invalid qos (negative)",
			config: MqttConfig{
				Address: "tcp://localhost:1883",
				QoS:     -1,
			},
			wantErr: true,
		},
		{
			name: "invalid qos (too high)",
			config: MqttConfig{
				Address: "tcp://localhost:1883",
				QoS:     3,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCsvConfigValidate tests CsvConfig.Validate
func TestCsvConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  CsvConfig
		wantErr bool
	}{
		{
			name: "valid csv config",
			config: CsvConfig{
				FilePath: "test.csv",
			},
			wantErr: false,
		},
		{
			name:    "empty file path",
			config:  CsvConfig{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestInfluxdbConfigValidate tests InfluxdbConfig.Validate
func TestInfluxdbConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  InfluxdbConfig
		wantErr bool
	}{
		{
			name: "valid influxdb config",
			config: InfluxdbConfig{
				Address:  "http://localhost:8086",
				Token:    "test-token",
				Database: "test-db",
			},
			wantErr: false,
		},
		{
			name:    "empty address",
			config:  InfluxdbConfig{Token: "test", Database: "test"},
			wantErr: true,
		},
		{
			name:    "empty token",
			config:  InfluxdbConfig{Address: "http://localhost:8086", Database: "test"},
			wantErr: true,
		},
		{
			name:    "empty database",
			config:  InfluxdbConfig{Address: "http://localhost:8086", Token: "test"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
