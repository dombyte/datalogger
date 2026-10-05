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
	// Lookups are named tables (integer code → text) that point expressions use as
	// `lookups.<name>[code]`. Viper lowercases the names.
	Lookups map[string]map[int]string `mapstructure:"lookups"`
}

// Device represents a device configuration.
type Device struct {
	Name         string        `mapstructure:"name"`
	Type         string        `mapstructure:"type"` // "modbus" or "http"
	PollInterval time.Duration `mapstructure:"poll_interval"`
	Timeout      time.Duration `mapstructure:"timeout"`
	// Parallelism (1-100) limits the concurrent reads within one device poll.
	Parallelism    int            `mapstructure:"parallelism"`
	DeviceSpecific DeviceSpecific `mapstructure:"device_specific"`
	Points         []Point        `mapstructure:"points"`
}

// Point represents a data point configuration.
type Point struct {
	Name         string  `mapstructure:"name"`
	Register     uint16  `mapstructure:"register"`      // Modbus: starting register address
	Count        uint16  `mapstructure:"count"`         // Modbus: number of registers to read
	Type         string  `mapstructure:"type"`          // int16/uint16/int32/uint32/float32/bool
	FunctionCode uint8   `mapstructure:"function_code"` // Modbus: 3 or 4
	JSONPath     string  `mapstructure:"json_path"`     // HTTP: "main.temp"
	Scale        float64 `mapstructure:"scale"`
	Offset       float64 `mapstructure:"offset"`
	Unit         string  `mapstructure:"unit"`
	// Expr is an optional expr-lang expression that replaces the value after scale and
	// offset; compiled at startup (internal/transform).
	Expr string `mapstructure:"expr"`
}

// DeviceSpecific contains device-type-specific configuration.
type DeviceSpecific struct {
	Modbus ModbusConfig `mapstructure:"modbus"`
	HTTP   HTTPConfig   `mapstructure:"http"`
}

// ModbusConfig contains Modbus-specific configuration.
type ModbusConfig struct {
	// Address is "tcp://host:port" or "rtu:///dev/ttyUSB0".
	Address      string   `mapstructure:"address"`
	SlaveID      uint8    `mapstructure:"slave_id"`
	Speed        int      `mapstructure:"speed"`         // RTU only
	DataBits     int      `mapstructure:"data_bits"`     // RTU only, default 8
	Parity       string   `mapstructure:"parity"`        // RTU only: "N", "E", "O"
	StopBits     int      `mapstructure:"stop_bits"`     // RTU only, default 1
	RegisterMode string   `mapstructure:"register_mode"` // "direct" or "range"
	Ranges       []string `mapstructure:"ranges"`        // e.g. ["3000-3011", "3012-3060"]
}

// HTTPConfig contains HTTP-specific configuration.
type HTTPConfig struct {
	Address      string            `mapstructure:"address"`
	Method       string            `mapstructure:"method"` // GET or POST
	Headers      map[string]string `mapstructure:"headers"`
	Body         string            `mapstructure:"body"`          // Raw body for POST
	ResponseType string            `mapstructure:"response_type"` // json, text, xml
	Insecure     bool              `mapstructure:"insecure"`
}

// Output represents an output configuration.
type Output struct {
	Name       string `mapstructure:"name"`
	Type       string `mapstructure:"type"` // influxdb, mqtt, csv
	BufferSize int    `mapstructure:"buffer_size"`
	// BatchSize, BatchTimeout, MaxRetries and RetryDelay tune InfluxDB writes; 0 = default.
	BatchSize    int           `mapstructure:"batch_size"`
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
	MaxRetries   int           `mapstructure:"max_retries"`
	RetryDelay   time.Duration `mapstructure:"retry_delay"`
	Devices      []string      `mapstructure:"devices"`
	// ExcludePoints lists "device/point" names this output does not receive.
	ExcludePoints  []string       `mapstructure:"exclude_points"`
	OutputSpecific OutputSpecific `mapstructure:"output_specific"`
}

// OutputSpecific contains output-type-specific configuration.
type OutputSpecific struct {
	Influxdb InfluxdbConfig `mapstructure:"influxdb"`
	Mqtt     MqttConfig     `mapstructure:"mqtt"`
	Csv      CsvConfig      `mapstructure:"csv"`
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
	Address  string `mapstructure:"address"`
	ClientID string `mapstructure:"client_id"`
	Topic    string `mapstructure:"topic"` // Prefix: "datalogger"
	QoS      int    `mapstructure:"qos"`   // 0, 1, or 2
	Retain   bool   `mapstructure:"retain"`
	Username string `mapstructure:"username"` // Optional
	Password string `mapstructure:"password"` // Optional
	Insecure bool   `mapstructure:"insecure"`
}

// CsvConfig contains CSV-specific configuration.
type CsvConfig struct {
	FilePath string        `mapstructure:"file_path"`
	MaxAge   time.Duration `mapstructure:"max_age"` // Max age before rotation
	// MaxBackups is the number of rotated files to keep (0 = keep none, < 0 = keep all).
	MaxBackups int `mapstructure:"max_backups"`
}

// Load reads the configuration from a YAML file, fills in defaults and validates it.
// It uses its own viper instance and no environment overrides; secrets live in the
// (gitignored) config file.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if err := config.applyDefaults(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	return &config, nil
}
