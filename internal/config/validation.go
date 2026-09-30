package config

import (
	"errors"
	"fmt"
	"strings"
)

const (
	minParallelism = 1
	maxParallelism = 100

	// maxQoS is the highest MQTT quality-of-service level.
	maxQoS = 2
)

// Validate validates the entire configuration.
func (c *Config) Validate() error {
	if len(c.Devices) == 0 {
		return errors.New("at least one device required")
	}
	deviceNames, err := c.validateDevices()
	if err != nil {
		return err
	}
	if len(c.Outputs) == 0 {
		return errors.New("at least one output required")
	}
	return c.validateOutputs(deviceNames)
}

// validateDevices validates every device and returns the set of device names.
func (c *Config) validateDevices() (map[string]bool, error) {
	deviceNames := make(map[string]bool)
	for _, d := range c.Devices {
		if d.Name == "" {
			return nil, errors.New("device name cannot be empty")
		}
		if deviceNames[d.Name] {
			return nil, fmt.Errorf("duplicate device name: %s", d.Name)
		}
		deviceNames[d.Name] = true

		if err := d.Validate(); err != nil {
			return nil, fmt.Errorf("device %s: %w", d.Name, err)
		}
	}
	return deviceNames, nil
}

// validateOutputs validates every output and checks that it only references known devices.
func (c *Config) validateOutputs(deviceNames map[string]bool) error {
	for _, o := range c.Outputs {
		if err := o.Validate(); err != nil {
			return fmt.Errorf("output %s: %w", o.Name, err)
		}

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
	if err := d.validateCommon(); err != nil {
		return err
	}

	switch d.Type {
	case "modbus":
		return d.DeviceSpecific.Modbus.Validate()
	case "http":
		return d.DeviceSpecific.HTTP.Validate()
	default:
		return fmt.Errorf("unknown device type: %s", d.Type)
	}
}

// validateCommon validates the settings shared by all device types.
func (d *Device) validateCommon() error {
	if d.Type == "" {
		return errors.New("type required")
	}

	if d.PollInterval <= 0 {
		return errors.New("poll_interval must be positive")
	}

	if d.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	if d.Parallelism < minParallelism || d.Parallelism > maxParallelism {
		return fmt.Errorf("parallelism must be between %d and %d", minParallelism, maxParallelism)
	}

	if len(d.Points) == 0 {
		return errors.New("device must have at least one point")
	}

	return d.validatePoints()
}

// Validate validates Modbus configuration.
func (m *ModbusConfig) Validate() error {
	if m.Address == "" {
		return errors.New("address required")
	}

	if m.SlaveID == 0 {
		return errors.New("slave_id required")
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
		return errors.New("register_mode required")
	}

	if m.RegisterMode != "direct" && m.RegisterMode != "range" {
		return errors.New("register_mode must be 'direct' or 'range'")
	}

	return nil
}

// validateRanges validates the range configuration for range mode.
func (m *ModbusConfig) validateRanges() error {
	if len(m.Ranges) == 0 {
		return errors.New("ranges required for range mode")
	}

	for _, r := range m.Ranges {
		if !strings.Contains(r, "-") {
			return fmt.Errorf("range must be in format 'start-end': %s", r)
		}
	}

	return nil
}

// Validate validates HTTP configuration.
func (h *HTTPConfig) Validate() error {
	if h.Address == "" {
		return errors.New("address required")
	}

	method := strings.ToUpper(h.Method)
	if method != "GET" && method != "POST" {
		return errors.New("method must be GET or POST")
	}

	if h.ResponseType != "json" && h.ResponseType != "text" {
		return fmt.Errorf("response_type must be json or text, got %q", h.ResponseType)
	}

	return nil
}

// Validate validates an output configuration.
func (o *Output) Validate() error {
	if o.Name == "" {
		return errors.New("name required")
	}

	if o.Type == "" {
		return errors.New("type required")
	}

	if len(o.Devices) == 0 {
		return errors.New("devices required")
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
		return errors.New("address required")
	}
	if i.Token == "" {
		return errors.New("token required")
	}
	if i.Database == "" {
		return errors.New("database required")
	}
	return nil
}

// Validate validates MQTT configuration.
func (m *MqttConfig) Validate() error {
	if m.Address == "" {
		return errors.New("address required")
	}
	if m.QoS < 0 || m.QoS > maxQoS {
		return errors.New("qos must be 0, 1, or 2")
	}
	return nil
}

// Validate validates CSV configuration.
func (c *CsvConfig) Validate() error {
	if c.FilePath == "" {
		return errors.New("file_path required")
	}
	return nil
}
