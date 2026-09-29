package modbus

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"
)

// Client is the part of an open Modbus connection the reader uses.
type Client interface {
	ReadRegisters(addr, quantity uint16, regType modbus.RegType) ([]uint16, error)
	Close() error
}

// Dialer opens a new connection to the device.
type Dialer interface {
	Dial() (Client, error)
}

// ConnSettings configures the connection to one device.
type ConnSettings struct {
	Address  string // tcp://host:port or rtu:///dev/ttyUSB0
	SlaveID  uint8
	Speed    int // RTU only
	DataBits int // RTU only
	Parity   string
	StopBits int // RTU only
	Timeout  time.Duration
}

// LibDialer dials with github.com/simonvetter/modbus.
type LibDialer struct {
	conf    modbus.ClientConfiguration
	slaveID uint8
}

// NewDialer checks the connection settings without connecting. An error here is a
// configuration error (bad URL scheme, bad serial settings).
func NewDialer(s ConnSettings, logger zerolog.Logger) (*LibDialer, error) {
	d := &LibDialer{
		conf: modbus.ClientConfiguration{
			URL:      s.Address,
			Speed:    uint(s.Speed),
			DataBits: uint(s.DataBits),
			Parity:   parseParity(s.Parity),
			StopBits: uint(s.StopBits),
			Timeout:  s.Timeout,
			Logger:   log.New(debugWriter{logger}, "", 0),
		},
		slaveID: s.SlaveID,
	}
	if _, err := modbus.NewClient(&d.conf); err != nil {
		return nil, fmt.Errorf("modbus: %w", err)
	}
	return d, nil
}

// Dial opens a connection and selects the slave ID.
func (d *LibDialer) Dial() (Client, error) {
	client, err := modbus.NewClient(&d.conf)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	if err := client.SetUnitId(d.slaveID); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("set unit ID: %w", err)
	}
	return client, nil
}

// parseParity converts N/E/O to the library constant (default: none).
func parseParity(parity string) uint {
	switch parity {
	case "E":
		return modbus.PARITY_EVEN
	case "O":
		return modbus.PARITY_ODD
	default:
		return modbus.PARITY_NONE
	}
}

// debugWriter forwards the library's log output to zerolog at debug level.
type debugWriter struct {
	logger zerolog.Logger
}

func (w debugWriter) Write(p []byte) (int, error) {
	w.logger.Debug().Msg(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}
