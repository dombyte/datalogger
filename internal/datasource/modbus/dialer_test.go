package modbus_test

import (
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	lib "github.com/simonvetter/modbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/datasource/modbus"
)

func TestNewDialerRejectsBadAddress(t *testing.T) {
	t.Parallel()
	_, err := modbus.NewDialer(modbus.ConnSettings{Address: "invalid://host:502"}, zerolog.Nop())
	assert.Error(t, err)
}

func TestOpenConnectsAndFailsWhenNothingListens(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	d, err := modbus.NewDialer(modbus.ConnSettings{
		Address: "tcp://" + addr, Timeout: time.Second,
	}, zerolog.Nop())
	require.NoError(t, err, "NewDialer does not connect")

	client, err := d.Open()
	require.NoError(t, err)
	require.NoError(t, client.Close())

	require.NoError(t, listener.Close())
	_, err = d.Open()
	assert.Error(t, err)
}

func TestOpenLeavesWrappingToTheBus(t *testing.T) {
	t.Parallel()
	d, err := modbus.NewDialer(modbus.ConnSettings{
		Address: "rtu://" + t.TempDir() + "/missing", Timeout: time.Second,
	}, zerolog.Nop())
	require.NoError(t, err)

	_, err = d.Open()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "open connection", "the Bus names the operation")
}

func TestNewDialerAcceptsRTUParities(t *testing.T) {
	t.Parallel()
	for _, parity := range []string{"N", "E", "O"} {
		_, err := modbus.NewDialer(modbus.ConnSettings{
			Address: "rtu:///dev/ttyUSB0", Speed: 9600, DataBits: 8,
			Parity: parity, StopBits: 1, Timeout: time.Second,
		}, zerolog.Nop())
		assert.NoError(t, err, "parity %s; the serial port is only opened on Open", parity)
	}
}

// unitHandler answers every holding register read with the unit ID of the request.
type unitHandler struct{}

func (unitHandler) HandleCoils(*lib.CoilsRequest) ([]bool, error) {
	return nil, lib.ErrIllegalFunction
}

func (unitHandler) HandleDiscreteInputs(*lib.DiscreteInputsRequest) ([]bool, error) {
	return nil, lib.ErrIllegalFunction
}

func (unitHandler) HandleHoldingRegisters(req *lib.HoldingRegistersRequest) ([]uint16, error) {
	return []uint16{uint16(req.UnitId)}, nil
}

func (unitHandler) HandleInputRegisters(*lib.InputRegistersRequest) ([]uint16, error) {
	return nil, lib.ErrIllegalFunction
}

// Open serves a Bus: each read selects its own slave on the one connection.
func TestOpenReadsFromEverySlave(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "tcp://" + listener.Addr().String()
	require.NoError(t, listener.Close()) // free the port for the server

	server, err := lib.NewServer(&lib.ServerConfiguration{
		URL: url, Timeout: time.Second, Logger: log.New(io.Discard, "", 0),
	}, unitHandler{})
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() { assert.NoError(t, server.Stop()) })

	d, err := modbus.NewDialer(modbus.ConnSettings{Address: url, Timeout: time.Second},
		zerolog.Nop())
	require.NoError(t, err)
	client, err := d.Open()
	require.NoError(t, err)
	for _, slave := range []uint8{1, 7} {
		values, err := client.ReadSlave(slave, 0, 1, lib.HOLDING_REGISTER)
		require.NoError(t, err)
		assert.Equal(t, []uint16{uint16(slave)}, values)
	}
	_, err = client.ReadSlave(1, 0, 1, lib.INPUT_REGISTER)
	assert.ErrorContains(t, err, "slave 1")
	require.NoError(t, client.Close())
	assert.Error(t, client.Close(), "closed twice")

	require.NoError(t, server.Stop())
	_, err = d.Open()
	assert.Error(t, err, "nothing listens any more")
}
