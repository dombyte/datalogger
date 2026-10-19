package modbus_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	lib "github.com/simonvetter/modbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource/modbus"
	"github.com/dombyte/datalogger/internal/datasource/modbus/mocks"
)

// startBus starts a Bus on mocks; it is stopped when the test ends.
func startBus(t *testing.T) (*modbus.Bus, *mocks.MockBusOpener, *mocks.MockBusClient) {
	t.Helper()
	opener := mocks.NewMockBusOpener(t)
	client := mocks.NewMockBusClient(t)
	bus, err := modbus.NewBus(modbus.BusDeps{Port: "rtu:///dev/ttyUSB0", Opener: opener})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := bus.Start(ctx)
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return bus, opener, client
}

func TestNewBusChecksDependencies(t *testing.T) {
	t.Parallel()
	_, err := modbus.NewBus(modbus.BusDeps{})
	assert.ErrorIs(t, err, modbus.ErrMissingDependency)
}

// Devices on one port share its connection: it is opened once and every request names
// the slave of its device.
func TestBusSharesOneConnection(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Once()
	client.EXPECT().ReadSlave(uint8(1), uint16(10), uint16(2), lib.HOLDING_REGISTER).
		Return([]uint16{1, 2}, nil).Once()
	client.EXPECT().ReadSlave(uint8(2), uint16(10), uint16(2), lib.HOLDING_REGISTER).
		Return([]uint16{3, 4}, nil).Once()
	client.EXPECT().Close().Return(nil).Once()

	for slave, want := range map[uint8][]uint16{1: {1, 2}, 2: {3, 4}} {
		c, err := bus.Dialer(slave).Dial()
		require.NoError(t, err)
		values, err := c.ReadRegisters(10, 2, lib.HOLDING_REGISTER)
		require.NoError(t, err)
		assert.Equal(t, want, values)
		assert.NoError(t, c.Close(), "closing a device's client keeps the port open")
	}
}

func TestBusReopensAfterConnectionError(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Twice()
	client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
		Return(nil, io.EOF).Once()
	client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{7}, nil).Once()
	client.EXPECT().Close().Return(errors.New("already closed")).Twice() // only logged

	c, err := bus.Dialer(1).Dial()
	require.NoError(t, err)
	_, err = c.ReadRegisters(0, 1, lib.HOLDING_REGISTER)
	require.ErrorIs(t, err, io.EOF)

	values, err := c.ReadRegisters(0, 1, lib.HOLDING_REGISTER)
	require.NoError(t, err)
	assert.Equal(t, []uint16{7}, values)
}

func TestBusTimeoutKeepsConnection(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Once()
	client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
		Return(nil, lib.ErrRequestTimedOut).Twice()
	client.EXPECT().Close().Return(nil).Once()

	c, err := bus.Dialer(1).Dial()
	require.NoError(t, err)
	for range 2 {
		_, err = c.ReadRegisters(0, 1, lib.HOLDING_REGISTER)
		assert.ErrorIs(t, err, lib.ErrRequestTimedOut)
	}
}

func TestBusDialFailsWhenPortCannotBeOpened(t *testing.T) {
	t.Parallel()
	bus, opener, _ := startBus(t)
	opener.EXPECT().Open().Return(nil, errors.New("no such device")).Once()

	_, err := bus.Dialer(1).Dial()
	assert.ErrorContains(t, err, "open port: no such device")
}

func TestStoppedBusFailsRequests(t *testing.T) {
	t.Parallel()
	bus, err := modbus.NewBus(modbus.BusDeps{Opener: mocks.NewMockBusOpener(t)})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := bus.Start(ctx)
	cancel()
	<-done

	_, err = bus.Dialer(1).Dial()
	assert.ErrorIs(t, err, modbus.ErrBusStopped)
}

// Requests of devices that poll at the same time run one after the other: two frames
// on a half-duplex bus at once would garble both.
func TestBusRunsOneRequestAtATime(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Once()
	var inFlight, maxInFlight atomic.Int32
	client.EXPECT().ReadSlave(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(uint8, uint16, uint16, lib.RegType) ([]uint16, error) {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			if n > maxInFlight.Load() {
				maxInFlight.Store(n)
			}
			return []uint16{0}, nil
		})
	client.EXPECT().Close().Return(nil).Once()

	var wg sync.WaitGroup
	for slave := range uint8(8) {
		wg.Go(func() {
			c, err := bus.Dialer(slave + 1).Dial()
			assert.NoError(t, err)
			for range 20 {
				_, err := c.ReadRegisters(0, 1, lib.HOLDING_REGISTER)
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), maxInFlight.Load())
}

// A Reader works on a bus like on its own connection.
func TestReaderReadsThroughBus(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Once()
	client.EXPECT().ReadSlave(uint8(3), uint16(100), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{42}, nil).Once()
	client.EXPECT().Close().Return(nil).Once()

	r, err := modbus.New(modbus.Deps{
		Settings: settings(modbus.Point{Name: "p", Register: 100, Type: "uint16", Scale: 1}),
		Dialer:   bus.Dialer(3), Clock: clocktest.NewFake(start), Log: zerolog.Nop(),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	data := r.Start(ctx)
	points := <-data
	cancel()
	for range data { // drain until the reader closes the channel
	}
	require.Len(t, points, 1)
	assert.InDelta(t, 42.0, points[0].Value, 0)
}
