package config

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
)

const (
	// maxQoS is the highest MQTT quality-of-service level.
	maxQoS = 2

	// pathSeparator separates device and point in exclude_points ("device/point").
	pathSeparator = "/"

	// mqttWildcards are not allowed in the topic names an MQTT client publishes to.
	mqttWildcards = "+#\x00"

	// maxInfluxDBFields is the number of points a device may send to InfluxDB: each point
	// is a column of the device's table, which holds at most 500 columns (the InfluxDB 3
	// default), one of them the time column.
	maxInfluxDBFields = 499

	// influxDBTimeColumn is the time column of every InfluxDB table.
	influxDBTimeColumn = "time"
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
		if err := o.validateReferences(devices); err != nil {
			return err
		}
	}
	return nil
}

// validateReferences checks the devices, exclude_points and, for MQTT, the topic names
// of an output, for InfluxDB the columns.
func (o *Output) validateReferences(devices map[string]map[string]bool) error {
	for _, deviceName := range o.Devices {
		if devices[deviceName] == nil {
			return fmt.Errorf("output %s references unknown device: %s", o.Name, deviceName)
		}
	}
	if err := o.validateExcludePoints(devices); err != nil {
		return fmt.Errorf("output %s: %w", o.Name, err)
	}
	var err error
	switch o.Type {
	case "mqtt":
		err = o.validateTopicNames(devices)
	case "influxdb":
		err = o.validateColumns(devices)
	}
	if err != nil {
		return fmt.Errorf("output %s: %w", o.Name, err)
	}
	return nil
}

// validateColumns checks that every device of an InfluxDB output fits one table: at
// most maxInfluxDBFields points that are not excluded, none named like the time column.
func (o *Output) validateColumns(devices map[string]map[string]bool) error {
	for _, device := range o.Devices {
		fields := 0
		for point := range devices[device] {
			entry := device + pathSeparator + point
			if slices.Contains(o.ExcludePoints, entry) {
				continue
			}
			if point == influxDBTimeColumn {
				return fmt.Errorf("point %s: %q is the InfluxDB time column; rename it or "+
					"list it in exclude_points", entry, point)
			}
			fields++
		}
		if fields > maxInfluxDBFields {
			return fmt.Errorf("device %s sends %d points, an InfluxDB table holds at most %d; "+
				"split the device or list points in exclude_points",
				device, fields, maxInfluxDBFields)
		}
	}
	return nil
}

// validateTopicNames checks that the device and point names published by an MQTT output
// are valid in a topic name: a broker closes the connection on + or #. Excluded points
// are never published.
func (o *Output) validateTopicNames(devices map[string]map[string]bool) error {
	for _, device := range o.Devices {
		if strings.ContainsAny(device, mqttWildcards) {
			return fmt.Errorf("device name %q must not contain + or # for MQTT", device)
		}
		for _, point := range slices.Sorted(maps.Keys(devices[device])) {
			entry := device + pathSeparator + point
			excluded := slices.Contains(o.ExcludePoints, entry)
			if strings.ContainsAny(point, mqttWildcards) && !excluded {
				return fmt.Errorf("point %s must not contain + or # for MQTT; rename it or "+
					"list it in exclude_points", entry)
			}
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

// validateTypeBlock checks that the settings block of the type is set and no other one:
// a block of another type would be ignored silently. blocks maps each block name to
// whether it is set; an unknown type is left to the caller.
func validateTypeBlock(typ string, blocks map[string]bool) error {
	set, known := blocks[typ]
	if !known {
		return nil
	}
	if !set {
		return fmt.Errorf("%s block required for type %s", typ, typ)
	}
	for _, name := range slices.Sorted(maps.Keys(blocks)) {
		if blocks[name] && name != typ {
			return fmt.Errorf("%s block not allowed for type %s", name, typ)
		}
	}
	return nil
}

// Validate validates a device configuration.
func (d *Device) Validate() error {
	// Before the points: HTTP points depend on the http block.
	err := validateTypeBlock(d.Type, map[string]bool{
		"modbus": d.Modbus != nil,
		"http":   d.HTTP != nil,
	})
	if err != nil {
		return err
	}
	if err := d.validateCommon(); err != nil {
		return err
	}

	switch d.Type {
	case "modbus":
		return d.Modbus.Validate()
	case "http":
		return d.HTTP.Validate()
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

	return o.validateTypeSettings()
}

// validateTypeSettings validates the settings block of the output type.
func (o *Output) validateTypeSettings() error {
	err := validateTypeBlock(o.Type, map[string]bool{
		"influxdb": o.Influxdb != nil,
		"mqtt":     o.Mqtt != nil,
		"csv":      o.Csv != nil,
		"api":      o.API != nil,
		"webhook":  o.Webhook != nil,
	})
	if err != nil {
		return err
	}
	switch o.Type {
	case "influxdb":
		return o.Influxdb.Validate()
	case "mqtt":
		return o.Mqtt.Validate()
	case "csv":
		return o.Csv.Validate()
	case "api":
		return o.API.Validate()
	case "webhook":
		return o.Webhook.Validate()
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
	if i.Timeout < 0 {
		return errors.New("timeout must not be negative")
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
	if strings.ContainsAny(m.Topic, mqttWildcards) {
		return fmt.Errorf("topic %q must not contain + or #", m.Topic)
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

// Validate validates the webhook configuration. The webhook package checks the URL
// itself when it is created.
func (w *WebhookConfig) Validate() error {
	if w.URL == "" {
		return errors.New("url required")
	}
	method := strings.ToUpper(w.Method)
	if method != "" && method != "POST" && method != "PUT" {
		return errors.New("method must be POST or PUT")
	}
	if w.Timeout < 0 {
		return errors.New("timeout must not be negative")
	}
	return nil
}
