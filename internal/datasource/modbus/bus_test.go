package modbus_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"syscall"
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

// startBus starts the Bus of a TCP connection on mocks; it is stopped when the test
// ends.
func startBus(t *testing.T) (*modbus.Bus, *mocks.MockBusOpener, *mocks.MockBusClient) {
	t.Helper()
	return startBusOn(t, modbus.BusDeps{Address: "tcp://inverter:502"})
}

// startSerialBus starts the Bus of a serial port on mocks.
func startSerialBus(t *testing.T) (*modbus.Bus, *mocks.MockBusOpener, *mocks.MockBusClient) {
	t.Helper()
	return startBusOn(t, modbus.BusDeps{Address: "rtu:///dev/ttyUSB0", Serial: true})
}

// startBusOn starts a Bus with d on mocks; it is stopped when the test ends.
func startBusOn(
	t *testing.T,
	d modbus.BusDeps,
) (*modbus.Bus, *mocks.MockBusOpener, *mocks.MockBusClient) {
	t.Helper()
	opener := mocks.NewMockBusOpener(t)
	client := mocks.NewMockBusClient(t)
	d.Opener = opener
	bus, err := modbus.NewBus(d)
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
		c, err := bus.Dialer(slave).Dial(context.Background())
		require.NoError(t, err)
		values, err := c.ReadRegisters(context.Background(), 10, 2, lib.HOLDING_REGISTER)
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

	c, err := bus.Dialer(1).Dial(context.Background())
	require.NoError(t, err)
	_, err = c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
	require.ErrorIs(t, err, io.EOF)

	values, err := c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
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

	c, err := bus.Dialer(1).Dial(context.Background())
	require.NoError(t, err)
	for range 2 {
		_, err = c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
		assert.ErrorIs(t, err, lib.ErrRequestTimedOut)
	}
}

// Regression: a serial frame has no transaction ID, and the port stayed open after a
// timeout. An answer that arrived late was taken for the answer to the next request,
// and every later request got the answer before it. The port is reopened, which
// discards what is on the line; so is a port that only times out because it went away.
func TestSerialBusReopensAfterLateAnswerErrors(t *testing.T) {
	t.Parallel()
	for name, readErr := range map[string]error{
		"timeout":                 fmt.Errorf("slave 1: %w", lib.ErrRequestTimedOut),
		"answer of another slave": fmt.Errorf("slave 1: %w", lib.ErrBadUnitId),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bus, opener, client := startSerialBus(t)
			opener.EXPECT().Open().Return(client, nil).Twice()
			client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
				Return(nil, readErr).Once()
			client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
				Return([]uint16{7}, nil).Once()
			client.EXPECT().Close().Return(nil).Twice()

			c, err := bus.Dialer(1).Dial(context.Background())
			require.NoError(t, err)
			_, err = c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
			require.ErrorIs(t, err, readErr, "the reader still sees the timeout")

			values, err := c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
			require.NoError(t, err)
			assert.Equal(t, []uint16{7}, values)
		})
	}
}

// Regression: a serial port that went away (an unplugged USB adapter) answers with
// errors of the device file, which were no connection errors: the bus kept the dead
// port until the process was restarted.
func TestBusReopensPortThatWentAway(t *testing.T) {
	t.Parallel()
	for _, errno := range []syscall.Errno{
		syscall.EIO, syscall.ENXIO, syscall.ENODEV, syscall.EBADF,
	} {
		t.Run(errno.Error(), func(t *testing.T) {
			t.Parallel()
			bus, opener, client := startSerialBus(t)
			opener.EXPECT().Open().Return(client, nil).Twice()
			client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
				Return(nil, fmt.Errorf("slave 1: %w", errno)).Once()
			client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
				Return([]uint16{7}, nil).Once()
			client.EXPECT().Close().Return(nil).Twice()

			c, err := bus.Dialer(1).Dial(context.Background())
			require.NoError(t, err)
			_, err = c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
			require.ErrorIs(t, err, errno)

			values, err := c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
			require.NoError(t, err)
			assert.Equal(t, []uint16{7}, values)
		})
	}
}

// Regression: a port that could not be opened was no connection error, so a
// direct-mode poll tried to open it for every point and logged each one.
func TestReaderSkipsRestWhenPortCannotBeOpened(t *testing.T) {
	t.Parallel()
	bus, opener, client := startSerialBus(t)
	gone := &fs.PathError{Op: "open", Path: "/dev/ttyUSB0", Err: syscall.ENOENT}
	opener.EXPECT().Open().Return(client, nil).Once()
	opener.EXPECT().Open().Return(nil, gone).Once() // not once more for d
	client.EXPECT().ReadSlave(uint8(3), uint16(1), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{42}, nil).Once()
	client.EXPECT().ReadSlave(uint8(3), uint16(2), uint16(1), lib.HOLDING_REGISTER).
		Return(nil, lib.ErrRequestTimedOut).Once() // the adapter is unplugged
	client.EXPECT().Close().Return(nil).Once()

	r, err := modbus.New(modbus.Deps{
		Settings: settings(
			modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1},
			modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
			modbus.Point{Name: "c", Register: 3, Type: "uint16", Scale: 1},
			modbus.Point{Name: "d", Register: 4, Type: "uint16", Scale: 1},
		),
		Dialer: bus.Dialer(3), Clock: clocktest.NewFake(start), Log: zerolog.Nop(),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	data := r.Start(ctx)
	points := <-data // sent when the poll is over
	cancel()
	for range data { // drain until the reader closes the channel
	}
	assert.Equal(t, []string{"a"}, names(points))
}

func TestBusDialFailsWhenPortCannotBeOpened(t *testing.T) {
	t.Parallel()
	bus, opener, _ := startBus(t)
	opener.EXPECT().Open().Return(nil, errors.New("no such device")).Once()

	_, err := bus.Dialer(1).Dial(context.Background())
	assert.ErrorContains(t, err, "open connection: no such device")
}

func TestStoppedBusFailsRequests(t *testing.T) {
	t.Parallel()
	bus, err := modbus.NewBus(modbus.BusDeps{Opener: mocks.NewMockBusOpener(t)})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := bus.Start(ctx)
	cancel()
	<-done

	_, err = bus.Dialer(1).Dial(context.Background())
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
			c, err := bus.Dialer(slave + 1).Dial(context.Background())
			assert.NoError(t, err)
			for range 20 {
				_, err := c.ReadRegisters(context.Background(), 0, 1, lib.HOLDING_REGISTER)
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), maxInFlight.Load())
}

// Regression: a device on a busy bus waited for the request in flight, up to the
// timeout of the slowest slave, also at shutdown. Its call returns when its ctx ends,
// both while queued and while its own request runs; the bus finishes it alone.
func TestBusCallReturnsWhenContextEnds(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Once()
	reading, release := make(chan struct{}), make(chan struct{})
	client.EXPECT().ReadSlave(uint8(1), uint16(0), uint16(1), lib.HOLDING_REGISTER).
		RunAndReturn(func(uint8, uint16, uint16, lib.RegType) ([]uint16, error) {
			close(reading)
			<-release // a slave that does not answer until its timeout
			return nil, lib.ErrRequestTimedOut
		}).Once()
	client.EXPECT().Close().Return(nil).Once()

	busy, err := bus.Dialer(1).Dial(context.Background())
	require.NoError(t, err)
	queued, err := bus.Dialer(2).Dial(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	inFlight := make(chan error, 1)
	go func() {
		_, err := busy.ReadRegisters(ctx, 0, 1, lib.HOLDING_REGISTER)
		inFlight <- err
	}()
	<-reading

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	waiting := make(chan error, 1)
	go func() {
		_, err := queued.ReadRegisters(queuedCtx, 0, 1, lib.HOLDING_REGISTER)
		waiting <- err
	}()

	cancel()
	cancelQueued()
	require.ErrorIs(t, <-inFlight, context.Canceled, "returns while its request runs")
	require.ErrorIs(t, <-waiting, context.Canceled, "returns while queued")
	close(release) // the bus finishes the request without its caller
}

// Regression: a request whose caller has already gone still held the bus.
func TestBusSkipsRequestOfCancelledCaller(t *testing.T) {
	t.Parallel()
	bus, opener, client := startBus(t)
	opener.EXPECT().Open().Return(client, nil).Once()
	client.EXPECT().Close().Return(nil).Once()
	c, err := bus.Dialer(1).Dial(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.ReadRegisters(ctx, 0, 1, lib.HOLDING_REGISTER) // the mock fails on a read
	assert.ErrorIs(t, err, context.Canceled)
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
