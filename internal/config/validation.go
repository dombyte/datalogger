package config

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
)

const (
	minParallelism = 1
	maxParallelism = 100

	// maxQoS is the highest MQTT quality-of-service level.
	maxQoS = 2

	// pathSeparator separates device and point in exclude_points ("device/point").
	pathSeparator = "/"
)

// Validate validates the entire configuration.
func (c *Config) Validate() error {
	if len(c.Devices) == 0 {
		return errors.New("at least one device required")
	}
	devices, err := c.validateDevices()
	if err != nil {
		return err
	}
	if len(c.Outputs) == 0 {
		return errors.New("at least one output required")
	}
	return c.validateOutputs(devices)
}

// validateDevices validates every device and returns the point names of each device.
func (c *Config) validateDevices() (map[string]map[string]bool, error) {
	devices := make(map[string]map[string]bool)
	for _, d := range c.Devices {
		if d.Name == "" {
			return nil, errors.New("device name cannot be empty")
		}
		if devices[d.Name] != nil {
			return nil, fmt.Errorf("duplicate device name: %s", d.Name)
		}
		// "/" separates device and point in exclude_points and MQTT topics.
		if strings.Contains(d.Name, pathSeparator) {
			return nil, fmt.Errorf("device name %q must not contain %q", d.Name, pathSeparator)
		}

		if err := d.Validate(); err != nil {
			return nil, fmt.Errorf("device %s: %w", d.Name, err)
		}
		devices[d.Name] = make(map[string]bool, len(d.Points))
		for _, p := range d.Points {
			devices[d.Name][p.Name] = true
		}
	}
	return devices, nil
}

// validateOutputs validates every output, checks that names are unique and that it
// only references known devices and points.
func (c *Config) validateOutputs(devices map[string]map[string]bool) error {
	seen := make(map[string]bool, len(c.Outputs))
	for _, o := range c.Outputs {
		if err := o.Validate(); err != nil {
			return fmt.Errorf("output %s: %w", o.Name, err)
		}
		// app routes to the outputs by name.
		if seen[o.Name] {
			return fmt.Errorf("duplicate output name: %s", o.Name)
		}
		seen[o.Name] = true

		for _, deviceName := range o.Devices {
			if devices[deviceName] == nil {
				return fmt.Errorf("output %s references unknown device: %s", o.Name, deviceName)
			}
		}
		if err := o.validateExcludePoints(devices); err != nil {
			return fmt.Errorf("output %s: %w", o.Name, err)
		}
	}
	return nil
}

// validateExcludePoints checks that every entry is "device/point" with a device of
// this output and a point of that device.
func (o *Output) validateExcludePoints(devices map[string]map[string]bool) error {
	for _, entry := range o.ExcludePoints {
		device, point, ok := strings.Cut(entry, pathSeparator)
		if !ok {
			return fmt.Errorf("exclude_points: %q must be device/point", entry)
		}
		if !slices.Contains(o.Devices, device) {
			return fmt.Errorf("exclude_points: %q: device %s is not in devices", entry, device)
		}
		if !devices[device][point] {
			return fmt.Errorf("exclude_points: %q: unknown point %s", entry, point)
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

	return o.OutputSpecific.validate(o.Type)
}

// validate validates the settings of the given output type.
func (s *OutputSpecific) validate(outputType string) error {
	switch outputType {
	case "influxdb":
		return s.Influxdb.Validate()
	case "mqtt":
		return s.Mqtt.Validate()
	case "csv":
		return s.Csv.Validate()
	case "api":
		return s.API.Validate()
	default:
		return fmt.Errorf("unknown output type: %s", outputType)
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

// Validate validates the API configuration.
func (a *APIConfig) Validate() error {
	if a.Listen == "" {
		return errors.New("listen required")
	}
	if _, _, err := net.SplitHostPort(a.Listen); err != nil {
		return fmt.Errorf("listen must be host:port: %w", err)
	}
	return nil
}
