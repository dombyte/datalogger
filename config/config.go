// Package config provides configuration loading and validation for the datalogger.
package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config is the root configuration structure.
type Config struct {
	Devices []Device `mapstructure:"devices"`
	Outputs []Output `mapstructure:"outputs"`
}

// Device represents a device configuration.
type Device struct {
	Name         string             `mapstructure:"name"`
	Type         string             `mapstructure:"type"` // "modbus" or "http"
	PollInterval time.Duration      `mapstructure:"poll_interval"`
	Timeout      time.Duration      `mapstructure:"timeout"`
	Parallelism  int                `mapstructure:"parallelism"` // 1-100, controls concurrent reads within device poll
	DeviceSpecific DeviceSpecific   `mapstructure:"device_specific"`
	Points       []Point            `mapstructure:"points"`
}

// Point represents a data point configuration.
type Point struct {
	Name         string   `mapstructure:"name"`
	Register    uint16   `mapstructure:"register"`    // Modbus: starting register address
	Count       uint16   `mapstructure:"count"`        // Modbus: number of registers to read
	Type        string   `mapstructure:"type"`         // int16, uint16, int32, uint32, float32, bool, string
	FunctionCode uint8    `mapstructure:"function_code"` // Modbus: 3 or 4 (optional for HTTP)
	JsonPath    string   `mapstructure:"json_path"`    // HTTP: "main.temp" or "a_voltage"
	Scale      float64  `mapstructure:"scale"`
	Offset     float64  `mapstructure:"offset"`
	Unit       string   `mapstructure:"unit"`
}

// DeviceSpecific contains device-type-specific configuration.
type DeviceSpecific struct {
	Modbus ModbusConfig `mapstructure:"modbus"`
	Http   HttpConfig   `mapstructure:"http"`
}

// ModbusConfig contains Modbus-specific configuration.
type ModbusConfig struct {
	Address      string   `mapstructure:"address"`        // "tcp://192.168.1.100:502" or "rtu:///dev/ttyUSB0"
	SlaveID      uint8    `mapstructure:"slave_id"`
	Speed        int      `mapstructure:"speed"`         // RTU only
	DataBits     int      `mapstructure:"data_bits"`    // RTU only, default 8
	Parity       string   `mapstructure:"parity"`        // RTU only: "N", "E", "O"
	StopBits     int      `mapstructure:"stop_bits"`    // RTU only, default 1
	RegisterMode string   `mapstructure:"register_mode"` // "direct" or "range"
	Ranges       []string `mapstructure:"ranges"`        // Manual ranges: ["3000-3011", "3011-3060"]
}

// HttpConfig contains HTTP-specific configuration.
type HttpConfig struct {
	Address     string            `mapstructure:"address"`
	Method      string            `mapstructure:"method"` // GET or POST
	Headers     map[string]string `mapstructure:"headers"`
	Body        string            `mapstructure:"body"`   // Raw body for POST
	ResponseType string          `mapstructure:"response_type"` // json, text, xml
	Insecure    bool              `mapstructure:"insecure"`
}

// Output represents an output configuration.
type Output struct {
	Name          string          `mapstructure:"name"`
	Type          string          `mapstructure:"type"` // influxdb, mqtt, csv
	BufferSize    int             `mapstructure:"buffer_size"`
	BatchSize     int             `mapstructure:"batch_size"`    // Number of points per batch (0 = default)
	BatchTimeout  time.Duration   `mapstructure:"batch_timeout"` // Max time before flushing batch (0 = default)
	MaxRetries    int             `mapstructure:"max_retries"`   // Max retry attempts for failed writes (0 = default)
	RetryDelay    time.Duration   `mapstructure:"retry_delay"`   // Delay between retries (0 = default)
	Devices       []string        `mapstructure:"devices"`
	OutputSpecific OutputSpecific `mapstructure:"output_specific"`
}

// OutputSpecific contains output-type-specific configuration.
type OutputSpecific struct {
	Influxdb InfluxdbConfig `mapstructure:"influxdb"`
	Mqtt     MqttConfig     `mapstructure:"mqtt"`
	Csv      CsvConfig       `mapstructure:"csv"`
}

// InfluxdbConfig contains InfluxDB3-specific configuration.
type InfluxdbConfig struct {
	Address  string `mapstructure:"address"`
	Token    string `mapstructure:"token"`
	Database string `mapstructure:"database"`
	Insecure bool   `mapstructure:"insecure"`
}

// MqttConfig contains MQTT-specific configuration.
type MqttConfig struct {
	Address   string `mapstructure:"address"`
	ClientID  string `mapstructure:"client_id"`
	Topic     string `mapstructure:"topic"`    // Prefix: "datalogger"
	QoS       int    `mapstructure:"qos"`      // 0, 1, or 2
	Retain    bool   `mapstructure:"retain"`
	Username  string `mapstructure:"username"` // Optional
	Password  string `mapstructure:"password"` // Optional
	Insecure  bool   `mapstructure:"insecure"`
}

// CsvConfig contains CSV-specific configuration.
type CsvConfig struct {
	FilePath   string `mapstructure:"file_path"`
	MaxAge     time.Duration `mapstructure:"max_age"`     // Max age before rotation
	MaxBackups int            `mapstructure:"max_backups"` // Maximum number of rotated files to keep (0 = keep none, < 0 = keep all)
}

// Load loads the configuration from a YAML file.
func Load(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.AutomaticEnv()
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}
