package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
						Timeout:      time.Second,
						HTTP: &HTTPConfig{
							Address:      "http://localhost",
							Method:       "GET",
							ResponseType: "json",
						},
						Points: []Point{{Name: "temp", JSONPath: "temp"}},
					},
				},
				Outputs: []Output{
					{
						Name:    "csv_out",
						Type:    "csv",
						Devices: []string{"test"},
						Csv: &CsvConfig{
							FilePath: "test.csv",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "no devices",
			config:  &Config{Outputs: []Output{{Name: "out", Type: "csv"}}},
			wantErr: true,
		},
		{
			name: "no outputs",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second},
				},
			},
			wantErr: true,
		},
		{
			name: "duplicate device names",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second},
					{Name: "test", Type: "http", PollInterval: 1 * time.Second},
				},
			},
			wantErr: true,
		},
		{
			name: "empty device name",
			config: &Config{
				Devices: []Device{
					{Name: "", Type: "http", PollInterval: 1 * time.Second},
				},
			},
			wantErr: true,
		},
		{
			name: "output references unknown device",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second},
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
					{Name: "test", Type: "invalid", PollInterval: 1 * time.Second},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid output type",
			config: &Config{
				Devices: []Device{
					{Name: "test", Type: "http", PollInterval: 1 * time.Second},
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
					{Name: "test", Type: "http", PollInterval: 0},
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
				Name:         "test",
				Type:         "http",
				PollInterval: 1 * time.Second,
				Timeout:      time.Second,
				HTTP: &HTTPConfig{
					Address: "http://localhost", Method: "GET", ResponseType: "json",
				},
				Points: []Point{{Name: "temp", JSONPath: "temp"}},
			},
			wantErr: false,
		},
		{
			name: "valid modbus device",
			device: Device{
				Name:         "test",
				Type:         "modbus",
				PollInterval: 1 * time.Second,
				Timeout:      time.Second,
				Modbus: &ModbusConfig{
					Address:      "tcp://localhost:502",
					SlaveID:      1,
					RegisterMode: "direct",
				},
				Points: []Point{{Name: "power", Register: 1, Type: "uint16", Scale: 1}},
			},
			wantErr: false,
		},
		{
			name: "no timeout (Load fills the default)",
			device: Device{
				Name: "test", Type: "http", PollInterval: time.Second,
				Points: []Point{{Name: "temp", JSONPath: "temp"}},
			},
			wantErr: true,
		},
		{
			name:    "empty type",
			device:  Device{Name: "test", PollInterval: 1 * time.Second},
			wantErr: true,
		},
		{
			name:    "zero poll interval",
			device:  Device{Name: "test", Type: "http", PollInterval: 0},
			wantErr: true,
		},
		{
			name:    "negative poll interval",
			device:  Device{Name: "test", Type: "http", PollInterval: -1 * time.Second},
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
		// Regression: these were accepted and failed only at construction, or were
		// ignored silently (a parity typo read without parity, ranges in direct mode).
		{
			name:    "range start > end",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range", Ranges: []string{"200-100"}},
			wantErr: true,
		},
		{
			name:    "range not a number",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range", Ranges: []string{"a-100"}},
			wantErr: true,
		},
		{
			name:    "range end beyond last register",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range", Ranges: []string{"0-65536"}},
			wantErr: true,
		},
		{
			name:    "range up to last register",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "range", Ranges: []string{"65000-65535"}},
			wantErr: false,
		},
		{
			name:    "ranges in direct mode",
			config:  ModbusConfig{Address: "tcp://localhost:502", SlaveID: 1, RegisterMode: "direct", Ranges: []string{"0-10"}},
			wantErr: true,
		},
		{
			name:    "unknown parity",
			config:  ModbusConfig{Address: "rtu:///dev/ttyUSB0", SlaveID: 1, RegisterMode: "direct", Parity: "X"},
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
			name: "empty method (defaults are applied by Load)",
			config: HTTPConfig{
				Address: "http://localhost:8080",
			},
			wantErr: true,
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
				Influxdb: &InfluxdbConfig{
					Address:  "http://localhost:8086",
					Token:    "test-token",
					Database: "test-db",
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
				Mqtt: &MqttConfig{
					Address: "tcp://localhost:1883",
					Topic:   "test",
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
				Csv: &CsvConfig{
					FilePath: "test.csv",
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
				Influxdb: &InfluxdbConfig{
					Token:    "test-token",
					Database: "test-db",
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
				Influxdb: &InfluxdbConfig{
					Address:  "http://localhost:8086",
					Database: "test-db",
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
				Influxdb: &InfluxdbConfig{
					Address: "http://localhost:8086",
					Token:   "test-token",
				},
			},
			wantErr: true,
		},
		{
			name: "influxdb negative timeout",
			output: Output{
				Name:    "influx",
				Type:    "influxdb",
				Devices: []string{"test"},
				Influxdb: &InfluxdbConfig{
					Address:  "http://localhost:8086",
					Token:    "test-token",
					Database: "test-db",
					Timeout:  -time.Second,
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
				Mqtt: &MqttConfig{
					Topic: "test",
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
				Mqtt: &MqttConfig{
					Address: "tcp://localhost:1883",
					Topic:   "test",
					QoS:     3,
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
				Csv:     &CsvConfig{},
			},
			wantErr: true,
		},
		{
			name: "valid api output",
			output: Output{
				Name:    "api",
				Type:    "api",
				Devices: []string{"test"},
				API:     &APIConfig{Listen: ":8080"},
			},
			wantErr: false,
		},
		{
			name: "api missing listen",
			output: Output{
				Name:    "api",
				Type:    "api",
				Devices: []string{"test"},
			},
			wantErr: true,
		},
		{
			name: "valid webhook output",
			output: Output{
				Name: "hook", Type: "webhook", Devices: []string{"test"},
				Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "put"},
			},
			wantErr: false,
		},
		{
			name: "webhook missing url",
			output: Output{
				Name: "hook", Type: "webhook", Devices: []string{"test"},
				Webhook: &WebhookConfig{},
			},
			wantErr: true,
		},
		{
			name: "webhook method GET",
			output: Output{
				Name: "hook", Type: "webhook", Devices: []string{"test"},
				Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "GET"},
			},
			wantErr: true,
		},
		{
			name: "webhook negative timeout",
			output: Output{
				Name: "hook", Type: "webhook", Devices: []string{"test"},
				Webhook: &WebhookConfig{URL: "https://example.com/hook", Timeout: -time.Second},
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

// TestAPIConfigValidate tests APIConfig.Validate
func TestAPIConfigValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		config  APIConfig
		wantErr string
	}{
		{name: "all interfaces", config: APIConfig{Listen: ":8080"}},
		{name: "host and token", config: APIConfig{Listen: "127.0.0.1:8080", Token: "t"}},
		{name: "empty listen", config: APIConfig{}, wantErr: "listen required"},
		{name: "no port", config: APIConfig{Listen: "8080"}, wantErr: "listen must be host:port"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.config.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
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

// TestLoadExampleConfig keeps example/config.yaml loadable and valid.
func TestLoadExampleConfig(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "example", "config.yaml"))
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Devices)
	assert.NotEmpty(t, cfg.Outputs)
}

// TestApplyDefaults checks the defaults and that set values are kept.
func TestApplyDefaults(t *testing.T) {
	cfg := &Config{
		Devices: []Device{
			{Name: "defaults", Type: "http", HTTP: &HTTPConfig{}},
			{Name: "set", Type: "http", HTTP: &HTTPConfig{
				Method: "POST", ResponseType: "text",
			}},
			{Name: "no http block", Type: "modbus", Modbus: &ModbusConfig{}},
		},
		Outputs: []Output{
			{Name: "defaults", Type: "mqtt", Mqtt: &MqttConfig{}},
			{Name: "set", Type: "mqtt", Mqtt: &MqttConfig{
				Topic: "custom", ClientID: "fixed",
			}},
		},
	}

	cfg.applyDefaults()

	assert.Equal(t, "GET", cfg.Devices[0].HTTP.Method)
	assert.Equal(t, "json", cfg.Devices[0].HTTP.ResponseType)
	assert.Equal(t, "POST", cfg.Devices[1].HTTP.Method)
	assert.Equal(t, "text", cfg.Devices[1].HTTP.ResponseType)
	assert.Nil(t, cfg.Devices[2].HTTP, "defaults never add a block of another type")

	assert.Equal(t, "datalogger", cfg.Outputs[0].Mqtt.Topic)
	assert.Regexp(t, `^logger-[a-z2-7]{8}$`, cfg.Outputs[0].Mqtt.ClientID)
	assert.Equal(t, "custom", cfg.Outputs[1].Mqtt.Topic)
	assert.Equal(t, "fixed", cfg.Outputs[1].Mqtt.ClientID)
}

// TestValidateTypeBlock checks that exactly the block named by type is set: a block of
// another type would otherwise be ignored without a word.
func TestValidateTypeBlock(t *testing.T) {
	t.Parallel()
	httpDevice := func(change func(*Device)) *Device {
		d := Device{
			Name: "d", Type: "http", PollInterval: time.Second, Timeout: time.Second,
			HTTP:   &HTTPConfig{Address: "http://localhost", Method: "GET", ResponseType: "json"},
			Points: []Point{{Name: "p", JSONPath: "p"}},
		}
		change(&d)
		return &d
	}
	csvOutput := func(change func(*Output)) *Output {
		o := Output{
			Name: "o", Type: "csv", Devices: []string{"d"},
			Csv: &CsvConfig{FilePath: "x.csv"},
		}
		change(&o)
		return &o
	}
	tests := map[string]struct {
		validate func() error
		wantErr  string
	}{
		"device without its block": {
			validate: httpDevice(func(d *Device) { d.HTTP = nil }).Validate,
			wantErr:  "http block required for type http",
		},
		"device with another block": {
			validate: httpDevice(func(d *Device) { d.Modbus = &ModbusConfig{} }).Validate,
			wantErr:  "modbus block not allowed for type http",
		},
		"device of unknown type": {
			validate: httpDevice(func(d *Device) { d.Type = "snmp" }).Validate,
			wantErr:  "unknown device type: snmp",
		},
		"output without its block": {
			validate: csvOutput(func(o *Output) { o.Csv = nil }).Validate,
			wantErr:  "csv block required for type csv",
		},
		"output with another block": {
			validate: csvOutput(func(o *Output) { o.API = &APIConfig{Listen: ":80"} }).Validate,
			wantErr:  "api block not allowed for type csv",
		},
		"output of unknown type": {
			validate: csvOutput(func(o *Output) { o.Type = "kafka" }).Validate,
			wantErr:  "unknown output type: kafka",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.EqualError(t, tt.validate(), tt.wantErr)
		})
	}
}

// TestValidateHasNoSideEffects checks that Validate leaves the config unchanged.
func TestValidateHasNoSideEffects(t *testing.T) {
	m := MqttConfig{Address: "tcp://localhost:1883"}
	require.NoError(t, m.Validate())
	assert.Equal(t, MqttConfig{Address: "tcp://localhost:1883"}, m)
}

// TestValidatePoints covers the per-type point rules.
func TestValidatePoints(t *testing.T) {
	t.Parallel()

	modbus := func(points ...Point) Device {
		return Device{Type: "modbus", Points: points}
	}
	http := func(responseType string, points ...Point) Device {
		return Device{
			Type:   "http",
			HTTP:   &HTTPConfig{ResponseType: responseType},
			Points: points,
		}
	}

	tests := []struct {
		name    string
		device  Device
		wantErr string
	}{
		{name: "modbus uint16", device: modbus(Point{Name: "p", Type: "uint16"})},
		{name: "modbus float32 count 2", device: modbus(Point{Name: "p", Type: "float32", Count: 2})},
		{
			name:    "modbus float32 without count",
			device:  modbus(Point{Name: "p", Type: "float32"}),
			wantErr: "points[0] p: type float32 needs count: 2",
		},
		{
			name:    "modbus unknown type",
			device:  modbus(Point{Name: "p", Type: "string"}),
			wantErr: `type must be int16, uint16, int32, uint32, float32 or bool, got "string"`,
		},
		{
			name:    "modbus bad function code",
			device:  modbus(Point{Name: "p", Type: "int16", FunctionCode: 6}),
			wantErr: "function_code must be 3 or 4, got 6",
		},
		{
			name: "modbus mixed function codes",
			device: modbus(
				Point{Name: "a", Type: "int16", FunctionCode: 3},
				Point{Name: "b", Type: "int16"},
				Point{Name: "c", Type: "int16", FunctionCode: 4},
			),
			wantErr: "points[2] c: function_code 4 mixed with 3",
		},
		// Regression: register + count past 65535 wrapped around to register 0, and a
		// count above the protocol limit failed every poll instead of startup.
		{
			name:   "modbus last register",
			device: modbus(Point{Name: "p", Type: "float32", Register: 65534, Count: 2}),
		},
		{
			name:    "modbus past the last register",
			device:  modbus(Point{Name: "p", Type: "float32", Register: 65535, Count: 2}),
			wantErr: "registers 65535-65536 exceed the last register 65535",
		},
		{name: "modbus count 125", device: modbus(Point{Name: "p", Type: "uint16", Count: 125})},
		{
			name:    "modbus count above 125",
			device:  modbus(Point{Name: "p", Type: "uint16", Count: 126}),
			wantErr: "count must be at most 125",
		},
		{name: "http json with path", device: http("json", Point{Name: "p", JSONPath: "a.b"})},
		{name: "http text without path", device: http("text", Point{Name: "p"})},
		{
			name:    "http json without path",
			device:  http("json", Point{Name: "p"}),
			wantErr: "points[0] p: json_path required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.device.validatePoints()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestHTTPResponseType checks that only json and text are accepted.
func TestHTTPResponseType(t *testing.T) {
	t.Parallel()

	for _, rt := range []string{"json", "text"} {
		h := HTTPConfig{Address: "http://x", Method: "GET", ResponseType: rt}
		assert.NoError(t, h.Validate(), rt)
	}
	h := HTTPConfig{Address: "http://x", Method: "GET", ResponseType: "xml"}
	assert.ErrorContains(t, h.Validate(), "response_type must be json or text")
}

// TestApplyDefaultsScale checks that an omitted scale becomes 1 and a set one is kept.
func TestApplyDefaultsScale(t *testing.T) {
	t.Parallel()

	cfg := &Config{Devices: []Device{{
		Type:   "modbus",
		Points: []Point{{Name: "unset"}, {Name: "set", Scale: 0.1}},
	}}}
	cfg.applyDefaults()
	assert.InDelta(t, 1.0, cfg.Devices[0].Points[0].Scale, 0)
	assert.InDelta(t, 0.1, cfg.Devices[0].Points[1].Scale, 0)
}

// TestApplyDefaultsTimeout checks the timeout default: the poll interval, 3-10 s.
func TestApplyDefaultsTimeout(t *testing.T) {
	t.Parallel()

	cfg := &Config{Devices: []Device{
		{Name: "fast", PollInterval: 5 * time.Second},
		{Name: "slow", PollInterval: time.Minute},
		{Name: "set", PollInterval: time.Minute, Timeout: 2 * time.Second},
		{Name: "every-second", PollInterval: time.Second},
	}}
	cfg.applyDefaults()
	assert.Equal(t, 5*time.Second, cfg.Devices[0].Timeout)
	assert.Equal(t, 10*time.Second, cfg.Devices[1].Timeout)
	assert.Equal(t, 2*time.Second, cfg.Devices[2].Timeout)
	assert.Equal(t, 3*time.Second, cfg.Devices[3].Timeout, "fast polls get at least 3 s")
}

// namesConfig returns a valid config with one HTTP device "meter" (points a and b) and
// one CSV output, changed by change.
func namesConfig(change func(*Config)) *Config {
	cfg := &Config{
		Devices: []Device{{
			Name: "meter", Type: "http", PollInterval: time.Second, Timeout: time.Second,
			HTTP: &HTTPConfig{
				Address: "http://localhost", Method: "GET", ResponseType: "json",
			},
			Points: []Point{{Name: "a", JSONPath: "a"}, {Name: "b", JSONPath: "b"}},
		}},
		Outputs: []Output{{
			Name: "out", Type: "csv", Devices: []string{"meter"},
			Csv: &CsvConfig{FilePath: "x.csv"},
		}},
	}
	change(cfg)
	return cfg
}

// TestValidateNamesAndExcludePoints checks point names and exclude_points references.
func TestValidateNamesAndExcludePoints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{name: "valid", change: func(*Config) {}},
		{
			name:   "valid exclude",
			change: func(c *Config) { c.Outputs[0].ExcludePoints = []string{"meter/b"} },
		},
		{
			name:    "device name with slash",
			change:  func(c *Config) { c.Devices[0].Name = "a/b" },
			wantErr: `device name "a/b" must not contain "/"`,
		},
		{
			name:    "point name with slash",
			change:  func(c *Config) { c.Devices[0].Points[1].Name = "x/y" },
			wantErr: `name must not contain "/"`,
		},
		{
			name:    "empty point name",
			change:  func(c *Config) { c.Devices[0].Points[1].Name = "" },
			wantErr: "points[1]: name required",
		},
		{
			name:    "duplicate point name",
			change:  func(c *Config) { c.Devices[0].Points[1].Name = "a" },
			wantErr: "duplicate point name: a",
		},
		{
			name:    "exclude without device",
			change:  func(c *Config) { c.Outputs[0].ExcludePoints = []string{"b"} },
			wantErr: `"b" must be device/point`,
		},
		{
			name:    "exclude device not routed to the output",
			change:  func(c *Config) { c.Outputs[0].ExcludePoints = []string{"other/b"} },
			wantErr: "device other is not in devices",
		},
		{
			name:    "exclude unknown point",
			change:  func(c *Config) { c.Outputs[0].ExcludePoints = []string{"meter/c"} },
			wantErr: "unknown point c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := namesConfig(tt.change).Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// toMQTT turns the output of namesConfig into an MQTT output.
func toMQTT(c *Config) {
	c.Outputs[0].Type = "mqtt"
	c.Outputs[0].Csv = nil
	c.Outputs[0].Mqtt = &MqttConfig{Address: "tcp://localhost:1883", Topic: "logger"}
}

// Regression: + and # are wildcards, not allowed in a published topic; the broker
// closed the connection and the writer reconnected for every such point.
func TestValidateMQTTTopicNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{name: "valid", change: toMQTT},
		{
			name:   "wildcards are fine for other outputs",
			change: func(c *Config) { c.Devices[0].Name, c.Outputs[0].Devices = "m+1", []string{"m+1"} },
		},
		{
			name: "excluded point is never published",
			change: func(c *Config) {
				toMQTT(c)
				c.Devices[0].Points[1].Name = "l1+l2"
				c.Outputs[0].ExcludePoints = []string{"meter/l1+l2"}
			},
		},
		{
			name: "device name",
			change: func(c *Config) {
				toMQTT(c)
				c.Devices[0].Name, c.Outputs[0].Devices = "m#1", []string{"m#1"}
			},
			wantErr: `device name "m#1" must not contain + or # for MQTT`,
		},
		{
			name:    "point name",
			change:  func(c *Config) { toMQTT(c); c.Devices[0].Points[1].Name = "l1+l2" },
			wantErr: "point meter/l1+l2 must not contain + or # for MQTT",
		},
		{
			name:    "topic prefix",
			change:  func(c *Config) { toMQTT(c); c.Outputs[0].Mqtt.Topic = "home/#" },
			wantErr: `topic "home/#" must not contain + or #`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := namesConfig(tt.change).Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// toInfluxDB turns the output of namesConfig into an InfluxDB output.
func toInfluxDB(c *Config) {
	c.Outputs[0].Type = "influxdb"
	c.Outputs[0].Csv = nil
	c.Outputs[0].Influxdb = &InfluxdbConfig{Address: "http://db:8181", Token: "t", Database: "d"}
}

// withPoints gives the device of namesConfig n points.
func withPoints(c *Config, n int) {
	c.Devices[0].Points = nil
	for i := range n {
		name := fmt.Sprintf("p%d", i)
		c.Devices[0].Points = append(c.Devices[0].Points, Point{Name: name, JSONPath: name})
	}
}

// Every point is a column of the device's InfluxDB table, which holds at most 500
// columns, one of them time.
func TestValidateInfluxDBColumns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{name: "valid", change: toInfluxDB},
		{name: "499 points fit", change: func(c *Config) { toInfluxDB(c); withPoints(c, 499) }},
		{
			name:    "500 points do not fit",
			change:  func(c *Config) { toInfluxDB(c); withPoints(c, 500) },
			wantErr: "device meter sends 500 points, an InfluxDB table holds at most 499",
		},
		{
			name: "excluded points do not count",
			change: func(c *Config) {
				toInfluxDB(c)
				withPoints(c, 500)
				c.Outputs[0].ExcludePoints = []string{"meter/p0"}
			},
		},
		{
			name:   "other outputs have no limit",
			change: func(c *Config) { withPoints(c, 500) },
		},
		{
			name:    "point named time",
			change:  func(c *Config) { toInfluxDB(c); c.Devices[0].Points[1].Name = "time" },
			wantErr: `point meter/time: "time" is the InfluxDB time column`,
		},
		{
			name: "excluded point named time",
			change: func(c *Config) {
				toInfluxDB(c)
				c.Devices[0].Points[1].Name = "time"
				c.Outputs[0].ExcludePoints = []string{"meter/time"}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := namesConfig(tt.change).Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// rtuConfig returns a valid config with two Modbus devices on one serial port, changed
// by change.
func rtuConfig(change func(*Config)) *Config {
	device := func(name string, slave uint8) Device {
		return Device{
			Name: name, Type: "modbus", PollInterval: time.Second, Timeout: time.Second,
			Modbus: &ModbusConfig{
				Address: "rtu:///dev/ttyUSB0", SlaveID: slave, Speed: 9600, DataBits: 8,
				Parity: "N", StopBits: 1, RegisterMode: "direct",
			},
			Points: []Point{{Name: "p", Register: 1, Type: "uint16", Scale: 1}},
		}
	}
	cfg := &Config{
		Devices: []Device{device("meter", 1), device("inverter", 2)},
		Outputs: []Output{{
			Name: "out", Type: "csv", Devices: []string{"meter", "inverter"},
			Csv: &CsvConfig{FilePath: "x.csv"},
		}},
	}
	change(cfg)
	return cfg
}

// Devices on one serial port share its connection, so they need the same serial
// settings.
func TestValidateSerialPorts(t *testing.T) {
	t.Parallel()
	const wantErr = "devices meter and inverter share rtu:///dev/ttyUSB0 with different " +
		"serial settings"
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{name: "same settings, other slaves", change: func(*Config) {}},
		{
			name:   "same slave: a device split in two",
			change: func(c *Config) { c.Devices[1].Modbus.SlaveID = 1 },
		},
		{
			name:   "other ports",
			change: func(c *Config) { c.Devices[1].Modbus.Address = "rtu:///dev/ttyUSB1"; c.Devices[1].Modbus.Speed = 19200 },
		},
		{
			name: "tcp devices are not checked",
			change: func(c *Config) {
				for i := range c.Devices {
					c.Devices[i].Modbus.Address = "tcp://gateway:502"
				}
				c.Devices[1].Modbus.Speed = 19200
			},
		},
		{
			name:    "speed",
			change:  func(c *Config) { c.Devices[1].Modbus.Speed = 19200 },
			wantErr: wantErr,
		},
		{
			name:    "data bits",
			change:  func(c *Config) { c.Devices[1].Modbus.DataBits = 7 },
			wantErr: wantErr,
		},
		{
			name:    "parity",
			change:  func(c *Config) { c.Devices[1].Modbus.Parity = "E" },
			wantErr: wantErr,
		},
		{
			name:    "stop bits",
			change:  func(c *Config) { c.Devices[1].Modbus.StopBits = 2 },
			wantErr: wantErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := rtuConfig(tt.change).Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestLoadExprAndLookups checks that expressions, hex lookup codes and exclude_points
// are read from YAML.
func TestLoadExprAndLookups(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
lookups:
  status:
    0x0003: Generating
    0x1004: Grid Off
devices:
  - name: meter
    type: http
    poll_interval: 1s
    http:
      address: "http://localhost"
    points:
      - name: status
        json_path: status
        expr: 'lookups.status[int(value)] ?? "unknown"'
outputs:
  - name: out
    type: csv
    devices: [meter]
    exclude_points: [meter/status]
    csv:
      file_path: x.csv
`), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.Equal(t, map[string]map[int]string{"status": {3: "Generating", 0x1004: "Grid Off"}},
		cfg.Lookups)
	assert.Equal(t, `lookups.status[int(value)] ?? "unknown"`, cfg.Devices[0].Points[0].Expr)
	assert.Equal(t, []string{"meter/status"}, cfg.Outputs[0].ExcludePoints)
}

// TestValidateErrors checks error paths of Validate on an otherwise valid config.
func TestValidateErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{name: "no outputs", change: func(c *Config) { c.Outputs = nil }, wantErr: "at least one output"},
		{
			name:    "duplicate device",
			change:  func(c *Config) { c.Devices = append(c.Devices, c.Devices[0]) },
			wantErr: "duplicate device name: meter",
		},
		{
			// Regression: app keys output channels by name, so the first output got no
			// data and the second every point twice.
			name:    "duplicate output",
			change:  func(c *Config) { c.Outputs = append(c.Outputs, c.Outputs[0]) },
			wantErr: "duplicate output name: out",
		},
		{
			name:    "invalid output",
			change:  func(c *Config) { c.Outputs[0].Type = "kafka" },
			wantErr: "output out: unknown output type: kafka",
		},
		{
			name:    "unknown device in output",
			change:  func(c *Config) { c.Outputs[0].Devices = []string{"meter", "nope"} },
			wantErr: "references unknown device: nope",
		},
		{
			name:    "unknown device type",
			change:  func(c *Config) { c.Devices[0].Type = "snmp" },
			wantErr: "unknown device type: snmp",
		},
		{
			name:    "no points",
			change:  func(c *Config) { c.Devices[0].Points = nil },
			wantErr: "at least one point",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.ErrorContains(t, namesConfig(tt.change).Validate(), tt.wantErr)
		})
	}
}

// TestLoadErrors checks that decode and validation errors are returned by Load.
func TestLoadErrors(t *testing.T) {
	t.Parallel()
	for name, tt := range map[string]struct{ content, wantErr string }{
		"decode": {content: "devices:\n  - poll_interval: soon\n", wantErr: "config: parse"},
		// Regression: unknown keys were ignored, so a typo fell back to the default.
		"unknown key": {
			content: "devices:\n  - name: m\n    poll_intervall: 1s\n",
			wantErr: "poll_intervall",
		},
		"unknown nested key": {
			content: "outputs:\n  - csv:\n      max_backup: 7\n",
			wantErr: "max_backup",
		},
		"validate": {content: "outputs: []\n", wantErr: "at least one device required"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))
			_, err := Load(path)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
