package config

import (
	"crypto/rand"
	"strings"
	"time"
)

const (
	defaultHTTPMethod   = "GET"
	defaultResponseType = "json"
	defaultMQTTTopic    = "datalogger"

	// Serial defaults of the Modbus library, filled in so that devices that share a port
	// compare equal whether they set them or not.
	defaultSpeed          = 19200
	defaultDataBits       = 8
	defaultParity         = "N"
	stopBitsWithoutParity = 2 // the Modbus serial line spec: no parity, two stop bits
	stopBitsWithParity    = 1

	// clientIDSuffixLen is the length of the random part of a generated MQTT client ID.
	clientIDSuffixLen = 8

	// The default device timeout is the poll interval, kept between these bounds: devices
	// that are polled every second still answer late now and then, which only delays
	// the next poll.
	minDefaultTimeout = 3 * time.Second
	maxDefaultTimeout = 10 * time.Second
)

// applyDefaults fills in optional settings that were omitted. Load runs it before
// Validate, so Validate only checks and never changes the config.
func (c *Config) applyDefaults() {
	for i := range c.Devices {
		c.Devices[i].applyDefaults()
	}
	for i := range c.Outputs {
		c.Outputs[i].applyDefaults()
	}
}

// applyDefaults fills in the timeout, the point scale, the HTTP method and the response
// type.
func (d *Device) applyDefaults() {
	// Without a timeout a hanging device would block its poll loop forever.
	if d.Timeout == 0 {
		d.Timeout = min(max(d.PollInterval, minDefaultTimeout), maxDefaultTimeout)
	}

	for i := range d.Points {
		// A scale of 0 would turn every value into the offset, so 0 means "not set".
		if d.Points[i].Scale == 0 {
			d.Points[i].Scale = 1
		}
	}

	if m := d.Modbus; m != nil && strings.HasPrefix(m.Address, rtuScheme) {
		m.applySerialDefaults()
	}

	if d.HTTP != nil {
		d.HTTP.applyDefaults()
	}
}

// applyDefaults fills in the method and the response type.
func (h *HTTPConfig) applyDefaults() {
	if h.Method == "" {
		h.Method = defaultHTTPMethod
	}
	if h.ResponseType == "" {
		h.ResponseType = defaultResponseType
	}
}

// applySerialDefaults fills in the serial settings the Modbus library would use when
// they are omitted: 19200 baud, 8 data bits, no parity, and 2 stop bits without parity
// (1 with parity).
func (m *ModbusConfig) applySerialDefaults() {
	if m.Speed == 0 {
		m.Speed = defaultSpeed
	}
	if m.DataBits == 0 {
		m.DataBits = defaultDataBits
	}
	if m.Parity == "" {
		m.Parity = defaultParity
	}
	if m.StopBits == 0 {
		m.StopBits = stopBitsWithParity
		if m.Parity == defaultParity {
			m.StopBits = stopBitsWithoutParity
		}
	}
}

// applyDefaults fills in the MQTT topic and a random client ID.
func (o *Output) applyDefaults() {
	m := o.Mqtt
	if m == nil {
		return
	}
	if m.Topic == "" {
		m.Topic = defaultMQTTTopic
	}
	if m.ClientID == "" {
		// rand.Text is base32 (A-Z, 2-7) and never fails.
		m.ClientID = "logger-" + strings.ToLower(rand.Text()[:clientIDSuffixLen])
	}
}
