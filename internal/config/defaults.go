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

	h := &d.DeviceSpecific.HTTP
	if h.Method == "" {
		h.Method = defaultHTTPMethod
	}
	if h.ResponseType == "" {
		h.ResponseType = defaultResponseType
	}
}

// applyDefaults fills in the MQTT topic and a random client ID.
func (o *Output) applyDefaults() {
	m := &o.OutputSpecific.Mqtt
	if m.Topic == "" {
		m.Topic = defaultMQTTTopic
	}
	if m.ClientID == "" {
		// rand.Text is base32 (A-Z, 2-7) and never fails.
		m.ClientID = "logger-" + strings.ToLower(rand.Text()[:clientIDSuffixLen])
	}
}
