package config

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	defaultHTTPMethod   = "GET"
	defaultResponseType = "json"
	defaultMQTTTopic    = "datalogger"

	// clientIDSuffixLen is the length of the random part of a generated MQTT client ID.
	clientIDSuffixLen = 8
)

// applyDefaults fills in optional settings that were omitted. Load runs it before
// Validate, so Validate only checks and never changes the config.
func (c *Config) applyDefaults() error {
	for i := range c.Devices {
		c.Devices[i].applyDefaults()
	}
	for i := range c.Outputs {
		if err := c.Outputs[i].applyDefaults(); err != nil {
			return fmt.Errorf("output %s: %w", c.Outputs[i].Name, err)
		}
	}
	return nil
}

// applyDefaults fills in the point scale, the HTTP method and the response type.
func (d *Device) applyDefaults() {
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
func (o *Output) applyDefaults() error {
	m := &o.OutputSpecific.Mqtt
	if m.Topic == "" {
		m.Topic = defaultMQTTTopic
	}
	if m.ClientID == "" {
		suffix, err := generateRandomString(clientIDSuffixLen)
		if err != nil {
			return fmt.Errorf("client_id: %w", err)
		}
		m.ClientID = "logger-" + suffix
	}
	return nil
}

// generateRandomString generates a random alphanumeric string of the given length.
func generateRandomString(n int) (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", fmt.Errorf("generate random string: %w", err)
		}
		b[i] = letters[num.Int64()]
	}
	return string(b), nil
}
