package modbus_test

import (
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/datasource/modbus"
)

func TestNewDialerRejectsBadAddress(t *testing.T) {
	t.Parallel()
	_, err := modbus.NewDialer(modbus.ConnSettings{Address: "invalid://host:502"}, zerolog.Nop())
	assert.Error(t, err)
}

func TestDialConnectsAndFailsWhenNothingListens(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	d, err := modbus.NewDialer(modbus.ConnSettings{
		Address: "tcp://" + addr, SlaveID: 1, Timeout: time.Second,
	}, zerolog.Nop())
	require.NoError(t, err, "NewDialer does not connect")

	client, err := d.Dial()
	require.NoError(t, err)
	require.NoError(t, client.Close())

	require.NoError(t, listener.Close())
	_, err = d.Dial()
	assert.Error(t, err)
}

func TestNewDialerAcceptsRTUParities(t *testing.T) {
	t.Parallel()
	for _, parity := range []string{"N", "E", "O"} {
		_, err := modbus.NewDialer(modbus.ConnSettings{
			Address: "rtu:///dev/ttyUSB0", SlaveID: 1, Speed: 9600, DataBits: 8,
			Parity: parity, StopBits: 1, Timeout: time.Second,
		}, zerolog.Nop())
		assert.NoError(t, err, "parity %s; the serial port is only opened on Dial", parity)
	}
}
