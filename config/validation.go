package config

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
)

// Validate validates the entire configuration.
func (c *Config) Validate() error {
	// Validate devices
	deviceNames := make(map[string]bool)
	for _, d := range c.Devices {
		if d.Name == "" {
			return fmt.Errorf("device name cannot be empty")
		}
		if deviceNames[d.Name] {
			return fmt.Errorf("duplicate device name: %s", d.Name)
		}
		deviceNames[d.Name] = true

		if err := d.Validate(); err != nil {
			return fmt.Errorf("device %s: %w", d.Name, err)
		}
	}

	// Validate outputs
	for _, o := range c.Outputs {
		if err := o.Validate(); err != nil {
			return fmt.Errorf("output %s: %w", o.Name, err)
		}

		// Check that output devices exist
		for _, deviceName := range o.Devices {
			if !deviceNames[deviceName] {
				return fmt.Errorf("output %s references unknown device: %s", o.Name, deviceName)
			}
		}
	}

	return nil
}

// Validate validates a device configuration.
func (d *Device) Validate() error {
	if d.Type == "" {
		return fmt.Errorf("type required")
	}

	if d.PollInterval <= 0 {
		return fmt.Errorf("poll_interval must be positive")
	}

	if d.Parallelism < 1 || d.Parallelism > 100 {
		return fmt.Errorf("parallelism must be between 1 and 100")
	}

	// Note: For Modbus devices, parallelism > 1 may cause device errors
	// as many Modbus devices can only handle one request at a time.
	// For HTTP devices, higher parallelism is generally safe.

	// Validate that device has at least one point
	if len(d.Points) == 0 {
		return fmt.Errorf("device must have at least one point")
	}

	switch d.Type {
	case "modbus":
		return d.DeviceSpecific.Modbus.Validate()
	case "http":
		return d.DeviceSpecific.Http.Validate()
	default:
		return fmt.Errorf("unknown device type: %s", d.Type)
	}
}

// Validate validates Modbus configuration.
func (m *ModbusConfig) Validate() error {
	if m.Address == "" {
		return fmt.Errorf("address required")
	}

	if m.SlaveID == 0 {
		return fmt.Errorf("slave_id required")
	}

	if err := m.validateRegisterMode(); err != nil {
		return err
	}

	if m.RegisterMode == "range" {
		if err := m.validateRanges(); err != nil {
			return err
		}
	}

	return nil
}

// validateRegisterMode validates the register mode configuration.
func (m *ModbusConfig) validateRegisterMode() error {
	if m.RegisterMode == "" {
		return fmt.Errorf("register_mode required")
	}

	if m.RegisterMode != "direct" && m.RegisterMode != "range" {
		return fmt.Errorf("register_mode must be 'direct' or 'range'")
	}

	return nil
}

// validateRanges validates the range configuration for range mode.
func (m *ModbusConfig) validateRanges() error {
	if len(m.Ranges) == 0 {
		return fmt.Errorf("ranges required for range mode")
	}

	for _, r := range m.Ranges {
		if !strings.Contains(r, "-") {
			return fmt.Errorf("range must be in format 'start-end': %s", r)
		}
	}

	return nil
}

// Validate validates HTTP configuration.
func (h *HttpConfig) Validate() error {
	if h.Address == "" {
		return fmt.Errorf("address required")
	}

	if h.Method == "" {
		h.Method = "GET"
	}

	method := strings.ToUpper(h.Method)
	if method != "GET" && method != "POST" {
		return fmt.Errorf("method must be GET or POST")
	}

	if h.ResponseType == "" {
		h.ResponseType = "json"
	}

	return nil
}

// Validate validates an output configuration.
func (o *Output) Validate() error {
	if o.Name == "" {
		return fmt.Errorf("name required")
	}

	if o.Type == "" {
		return fmt.Errorf("type required")
	}

	if len(o.Devices) == 0 {
		return fmt.Errorf("devices required")
	}

	switch o.Type {
	case "influxdb":
		return o.OutputSpecific.Influxdb.Validate()
	case "mqtt":
		return o.OutputSpecific.Mqtt.Validate()
	case "csv":
		return o.OutputSpecific.Csv.Validate()
	default:
		return fmt.Errorf("unknown output type: %s", o.Type)
	}
}

// Validate validates InfluxDB3 configuration.
func (i *InfluxdbConfig) Validate() error {
	if i.Address == "" {
		return fmt.Errorf("address required")
	}
	if i.Token == "" {
		return fmt.Errorf("token required")
	}
	if i.Database == "" {
		return fmt.Errorf("database required")
	}
	return nil
}

// generateRandomString generates a random alphanumeric string of the given length.
func generateRandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		b[i] = letters[num.Int64()]
	}
	return string(b)
}

// Validate validates MQTT configuration.
func (m *MqttConfig) Validate() error {
	if m.Address == "" {
		return fmt.Errorf("address required")
	}
	if m.Topic == "" {
		m.Topic = "datalogger"
	}
	if m.ClientID == "" {
		m.ClientID = "logger-" + generateRandomString(8)
	}
	if m.QoS < 0 || m.QoS > 2 {
		return fmt.Errorf("qos must be 0, 1, or 2")
	}
	return nil
}

// Validate validates CSV configuration.
func (c *CsvConfig) Validate() error {
	if c.FilePath == "" {
		return fmt.Errorf("file_path required")
	}
	return nil
}
