package modbus

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"
)

// Client is the part of an open Modbus connection the reader uses. Read reads quantity
// registers (or bits, as 0 or 1 each) of the table from addr on; it returns when ctx
// ends, also while it is in flight.
type Client interface {
	Read(ctx context.Context, table Table, addr, quantity uint16) ([]uint16, error)
	Close() error
}

// Dialer opens a connection to the device; *BusDialer implements it.
type Dialer interface {
	Dial(ctx context.Context) (Client, error)
}

// ConnSettings configures one connection: a TCP device or a serial port.
type ConnSettings struct {
	Address  string // tcp://host:port or rtu:///dev/ttyUSB0
	Speed    int    // RTU only
	DataBits int    // RTU only
	Parity   string
	StopBits int // RTU only
	Timeout  time.Duration
}

// LibDialer opens connections with github.com/simonvetter/modbus, for a Bus.
type LibDialer struct {
	conf modbus.ClientConfiguration
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
	}
	if _, err := modbus.NewClient(&d.conf); err != nil {
		return nil, fmt.Errorf("modbus: %w", err)
	}
	return d, nil
}

// Open opens a connection without a fixed slave, for a Bus: it selects the slave per
// request.
func (d *LibDialer) Open() (BusClient, error) {
	client := d.newClient()
	if err := client.Open(); err != nil {
		return nil, err // the Bus wraps it with the operation
	}
	return libBusClient{client: client}, nil
}

// newClient creates a client from the checked configuration.
func (d *LibDialer) newClient() *modbus.ModbusClient {
	client, err := modbus.NewClient(&d.conf)
	if err != nil {
		//nolint:forbidigo // invariant: NewDialer created a client from the same configuration
		panic(fmt.Sprintf("modbus: create client: %v", err))
	}
	return client
}

// setUnitID selects the slave of the next requests.
func setUnitID(client *modbus.ModbusClient, slaveID uint8) {
	if err := client.SetUnitId(slaveID); err != nil {
		//nolint:forbidigo // invariant: SetUnitId only stores the ID and never fails
		panic(fmt.Sprintf("modbus: set unit ID: %v", err))
	}
}

// libBusClient reads from any slave of an open library client.
type libBusClient struct {
	client *modbus.ModbusClient
}

// ReadSlave selects the slave and reads its registers, or its bits as 0 or 1.
func (c libBusClient) ReadSlave(
	slaveID uint8, table Table, addr, quantity uint16,
) ([]uint16, error) {
	setUnitID(c.client, slaveID)
	values, err := c.read(table, addr, quantity)
	if err != nil {
		return nil, fmt.Errorf("slave %d: %w", slaveID, err)
	}
	return values, nil
}

// read reads from the table with the function code of it.
func (c libBusClient) read(table Table, addr, quantity uint16) ([]uint16, error) {
	switch table {
	case Coils:
		return bitValues(c.client.ReadCoils(addr, quantity))
	case DiscreteInputs:
		return bitValues(c.client.ReadDiscreteInputs(addr, quantity))
	case InputRegisters:
		return c.client.ReadRegisters(addr, quantity, modbus.INPUT_REGISTER)
	default:
		return c.client.ReadRegisters(addr, quantity, modbus.HOLDING_REGISTER)
	}
}

// bitValues turns coils or discrete inputs into one value of 0 or 1 each, so they are
// decoded like registers.
func bitValues(bits []bool, err error) ([]uint16, error) {
	if err != nil {
		return nil, err
	}
	values := make([]uint16, len(bits))
	for i, b := range bits {
		if b {
			values[i] = 1
		}
	}
	return values, nil
}

// Close closes the connection.
func (c libBusClient) Close() error {
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
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
