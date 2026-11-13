package modbus_test

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	lib "github.com/simonvetter/modbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/datasource/modbus"
	"github.com/dombyte/datalogger/internal/datasource/modbus/mocks"
)

const interval = time.Second

var start = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

// harness runs a Reader against mocks and a fake clock.
type harness struct {
	t      *testing.T
	clock  *clocktest.Fake
	dialer *mocks.MockDialer
	client *mocks.MockClient
	data   <-chan []datasource.DataPoint
	// pending holds the rest of the last received poll; receive returns it point by point.
	pending []datasource.DataPoint
	reader  datasource.DeviceReader
	cancel  context.CancelFunc
}

func settings(points ...modbus.Point) modbus.Settings {
	return modbus.Settings{Name: "inverter", PollInterval: interval, Points: points}
}

func newHarness(t *testing.T, s modbus.Settings) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		clock:  clocktest.NewFake(start),
		dialer: mocks.NewMockDialer(t),
		client: mocks.NewMockClient(t),
	}
	r, err := modbus.New(modbus.Deps{
		Settings: s, Dialer: h.dialer, Clock: h.clock, Log: zerolog.Nop(),
	})
	require.NoError(t, err)
	h.reader = r
	t.Cleanup(h.stop)
	return h
}

// start starts the reader, which polls at once; set the expectations of the first
// poll before.
func (h *harness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.data = h.reader.Start(ctx)
}

// advance waits until the reader blocks on n clock waiters, then moves time by d.
func (h *harness) advance(n int, d time.Duration) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.clock.Waiters() >= n },
		time.Second, time.Millisecond)
	h.clock.Advance(d)
}

// receive returns the next data point.
func (h *harness) receive() datasource.DataPoint {
	h.t.Helper()
	if len(h.pending) == 0 {
		h.pending = h.receivePoll()
	}
	dp := h.pending[0]
	h.pending = h.pending[1:]
	return dp
}

// receivePoll returns the points of the next poll.
func (h *harness) receivePoll() []datasource.DataPoint {
	h.t.Helper()
	select {
	case points := <-h.data:
		return points
	case <-time.After(time.Second):
		h.t.Fatal("no data point")
		return nil
	}
}

func (h *harness) stop() {
	if h.cancel == nil {
		return // never started
	}
	h.cancel()
	for range h.data { // drain until the reader closes the channel
	}
}

// signal returns a channel closed when a mock call runs.
func signal(call interface {
	Run(func(mock.Arguments)) *mock.Call
},
) <-chan struct{} {
	ch := make(chan struct{})
	call.Run(func(mock.Arguments) { close(ch) })
	return ch
}

func TestNewChecksDependencies(t *testing.T) {
	t.Parallel()

	point := modbus.Point{Name: "p", Register: 10, Type: "uint16", Scale: 1}
	valid := modbus.Deps{
		Settings: settings(point),
		Dialer:   mocks.NewMockDialer(t),
		Clock:    clocktest.NewFake(start),
	}
	tests := []struct {
		name    string
		change  func(*modbus.Deps)
		wantErr error
	}{
		{name: "valid", change: func(*modbus.Deps) {}},
		{
			name: "no dialer", change: func(d *modbus.Deps) { d.Dialer = nil },
			wantErr: modbus.ErrMissingDependency,
		},
		{
			name: "no clock", change: func(d *modbus.Deps) { d.Clock = nil },
			wantErr: modbus.ErrMissingDependency,
		},
		{
			name: "no poll interval", change: func(d *modbus.Deps) { d.Settings.PollInterval = 0 },
			wantErr: modbus.ErrInvalidSettings,
		},
		{
			name: "no points", change: func(d *modbus.Deps) { d.Settings.Points = nil },
			wantErr: modbus.ErrInvalidSettings,
		},
		{name: "bad range", change: func(d *modbus.Deps) {
			d.Settings.RangeMode, d.Settings.Ranges = true, []string{"10"}
		}, wantErr: modbus.ErrInvalidSettings},
		{name: "point outside ranges", change: func(d *modbus.Deps) {
			d.Settings.RangeMode, d.Settings.Ranges = true, []string{"0-9"}
		}, wantErr: modbus.ErrInvalidSettings},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := valid
			tt.change(&d)
			_, err := modbus.New(d)
			if tt.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestDirectModeReadsScaledPointsAndStaysConnected(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "temp", Register: 100, Type: "int16", Scale: 0.1, Unit: "C"},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(100), uint16(1)).
		Return([]uint16{0xFF10}, nil).Twice() // -240
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	assert.Equal(t, datasource.DataPoint{
		DeviceName: "inverter",
		PointName:  "temp",
		Value:      -24.0,
		Timestamp:  start, // the first poll runs at once
		Unit:       "C",
	}, h.receive())

	h.advance(1, interval)
	assert.Equal(t, start.Add(interval), h.receive().Timestamp)
}

func TestDirectModeSkipsFailedPoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "bad", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "good", Register: 2, Type: "uint16", Scale: 1, FunctionCode: 4},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.InputRegisters, uint16(1), uint16(1)).
		Return(nil, lib.ErrIllegalDataAddress).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.InputRegisters, uint16(2), uint16(1)).
		Return([]uint16{7}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once() // only on stop, not after the failure

	h.start()
	dp := h.receive()
	assert.Equal(t, "good", dp.PointName)
	assert.InDelta(t, 7.0, dp.Value, 0)
}

func TestReconnectsAfterConnectionError(t *testing.T) {
	t.Parallel()
	point := modbus.Point{Name: "p", Register: 5, Type: "uint16", Scale: 1}
	h := newHarness(t, settings(point))
	second := mocks.NewMockClient(t)

	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(5), uint16(1)).
		Return(nil, io.EOF).Once()
	closed := signal(h.client.EXPECT().Close().Return(nil).Once())

	h.start()
	<-closed // the broken connection is dropped at once

	h.dialer.EXPECT().Dial(mock.Anything).Return(second, nil).Once()
	second.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(5), uint16(1)).
		Return([]uint16{42}, nil).Once()
	second.EXPECT().Close().Return(nil).Once()

	h.advance(2, 100*time.Millisecond) // the retry runs after the initial backoff
	assert.InDelta(t, 42.0, h.receive().Value, 0)
	h.stop() // before second's expectations are checked
}

func TestUnreachableDeviceRecovers(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(modbus.Point{Name: "p", Register: 5, Type: "uint16", Scale: 1}))

	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	failed := signal(h.dialer.EXPECT().Dial(mock.Anything).Return(nil, refused).Once())
	h.start()
	<-failed

	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(5), uint16(1)).
		Return([]uint16{1}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	// Regression: the retry waited for the next tick, then the backoff. It runs after the
	// backoff alone now (ticker + backoff wait), long before the poll interval.
	h.advance(2, 100*time.Millisecond)
	assert.Equal(t, "p", h.receive().PointName)
}

func TestRangeModeReadsInChunksAndDecodes(t *testing.T) {
	t.Parallel()
	s := settings(
		modbus.Point{Name: "pi", Register: 124, Count: 2, Type: "float32", Scale: 1},
		modbus.Point{Name: "last", Register: 299, Type: "uint16", Scale: 2},
	)
	s.RangeMode, s.Ranges = true, []string{"0-299"}
	h := newHarness(t, s)

	chunk := func(n int) []uint16 { return make([]uint16, n) }
	first, third := chunk(125), chunk(50)
	first[124] = 0x4049
	third[49] = 21
	second := chunk(125)
	second[0] = 0x0fdb

	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(0), uint16(125)).
		Return(first, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(125), uint16(125)).
		Return(second, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(250), uint16(50)).
		Return(third, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	got := map[string]any{}
	for range 2 {
		dp := h.receive()
		got[dp.PointName] = dp.Value
	}
	assert.Equal(t, map[string]any{"pi": float64(float32(3.1415927)), "last": 42.0}, got)
}

func TestRangeModeFailedChunkFailsPoll(t *testing.T) {
	t.Parallel()
	s := settings(modbus.Point{Name: "p", Register: 0, Type: "uint16", Scale: 1})
	s.RangeMode, s.Ranges = true, []string{"0-199"}
	h := newHarness(t, s)

	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(0), uint16(125)).
		Return(make([]uint16, 125), nil).Once()
	failed := signal(h.client.EXPECT().
		Read(mock.Anything, modbus.HoldingRegisters, uint16(125), uint16(75)).
		Return(nil, lib.ErrRequestTimedOut).Once())
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-failed
	select {
	case points := <-h.data:
		t.Fatalf("unexpected data points %v", points)
	case <-time.After(50 * time.Millisecond):
	}
}

// A chunk with fewer registers than requested fails the poll: the registers it lacks
// would hold the values of the previous poll.
func TestRangeModeShortChunkFailsPoll(t *testing.T) {
	t.Parallel()
	s := settings(modbus.Point{Name: "p", Register: 0, Type: "uint16", Scale: 1})
	s.RangeMode, s.Ranges = true, []string{"0-9"}
	h := newHarness(t, s)
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	failed := signal(h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(0), uint16(10)).Return(make([]uint16, 9), nil).Once())
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-failed
	select {
	case points := <-h.data:
		t.Fatalf("unexpected data points %v", points)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestStopClosesChannelsAndConnection(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(modbus.Point{Name: "p", Register: 1, Type: "uint16", Scale: 1}))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(1), uint16(1)).
		Return([]uint16{1}, nil).Once()
	h.client.EXPECT().Close().Return(errors.New("already closed")).Once()

	h.start()
	h.receive()
	h.stop()

	_, open := <-h.data
	assert.False(t, open, "data channel is closed")
}

// Regression: a read in flight at shutdown held the shutdown for up to the device
// timeout, longer than its deadline. The reader stops waiting for it at once (the Bus
// returns when ctx ends); the points read before are delivered, reads not started yet
// are skipped.
func TestReadInFlightAtShutdownIsAbandoned(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
		modbus.Point{Name: "c", Register: 3, Type: "uint16", Scale: 1},
	))
	reading := make(chan struct{})
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(1), uint16(1)).
		Return([]uint16{1}, nil).Once()
	// The second read only ends with ctx, like a Bus call; the third is not started.
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(2), uint16(1)).
		RunAndReturn(func(ctx context.Context, _ modbus.Table, _, _ uint16) ([]uint16, error) {
			close(reading)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-reading
	h.cancel()

	var got [][]string
	for points := range h.data {
		got = append(got, names(points))
	}
	assert.Equal(t, [][]string{{"a"}}, got, "the poll arrives as one batch without b and c")
}

// Regression: a device that stopped answering held every poll for points × timeout,
// longer than the shutdown deadline; after two timeouts in a row the rest is skipped.
func TestDirectModeSkipsRestAfterTimeoutsInRow(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
		modbus.Point{Name: "c", Register: 3, Type: "uint16", Scale: 1},
		modbus.Point{Name: "d", Register: 4, Type: "uint16", Scale: 1},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	read := make(chan struct{}, 2)
	// Two reads only: the mock fails the test on a third.
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, mock.Anything, uint16(1)).
		Run(func(context.Context, modbus.Table, uint16, uint16) { read <- struct{}{} }).
		Return(nil, lib.ErrRequestTimedOut).Twice()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-read
	<-read
	// The failed poll waits out the backoff (ticker + backoff).
	require.Eventually(t, func() bool { return h.clock.Waiters() >= 2 },
		time.Second, time.Millisecond)
}

// One timeout (a register the device ignores) does not cost the other points.
func TestDirectModeSingleTimeoutKeepsReading(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "ignored", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
		modbus.Point{Name: "c", Register: 3, Type: "uint16", Scale: 1},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, mock.Anything, uint16(1)).
		RunAndReturn(func(_ context.Context, _ modbus.Table, register, _ uint16) ([]uint16, error) {
			if register == 1 {
				return nil, lib.ErrRequestTimedOut
			}
			return []uint16{register}, nil
		}).Times(3)
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	assert.ElementsMatch(t, []string{"b", "c"}, names(h.receivePoll()))
}

// Regression: after a connection error every further read of the poll failed the same
// way; they are skipped now and the next poll reconnects.
func TestDirectModeSkipsRestAfterConnectionError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
		modbus.Point{Name: "c", Register: 3, Type: "uint16", Scale: 1},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, mock.Anything, uint16(1)).
		Return(nil, io.EOF).Once()
	closed := signal(h.client.EXPECT().Close().Return(nil).Once())

	h.start()
	<-closed
}

// Regression: a failed chunk fails the whole range poll, but the other ranges were
// still read; they are skipped now.
func TestRangeModeStopsAfterFailedChunk(t *testing.T) {
	t.Parallel()
	s := settings(
		modbus.Point{Name: "a", Register: 0, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 200, Type: "uint16", Scale: 1},
	)
	s.RangeMode, s.Ranges = true, []string{"0-9", "200-209"}
	h := newHarness(t, s)
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	failed := signal(h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, mock.Anything, uint16(10)).
		Return(nil, lib.ErrRequestTimedOut).Once()) // the mock fails on a second chunk
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-failed
	// The reader waits out the backoff, so the poll has ended.
	require.Eventually(t, func() bool { return h.clock.Waiters() >= 2 },
		time.Second, time.Millisecond)
}

// A range poll cut short by shutdown reads no further chunk and delivers nothing: only
// a complete poll is a consistent snapshot.
func TestRangeModeShutdownSkipsRemainingChunks(t *testing.T) {
	t.Parallel()
	s := settings(
		modbus.Point{Name: "a", Register: 0, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 200, Type: "uint16", Scale: 1},
	)
	s.RangeMode, s.Ranges = true, []string{"0-9", "200-209"}
	h := newHarness(t, s)
	reading, release := make(chan struct{}), make(chan struct{})
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, mock.Anything, uint16(10)).
		RunAndReturn(func(context.Context, modbus.Table, uint16, uint16) ([]uint16, error) {
			close(reading)
			<-release
			return make([]uint16, 10), nil
		}).Once() // the mock fails the test on a second chunk
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-reading
	h.cancel()
	close(release)
	for points := range h.data {
		t.Fatalf("unexpected data points %v", points)
	}
}

// Regression: a direct-mode poll that returned points with a connection error during
// shutdown dropped them, although they had been read.
func TestPointsReadBeforeShutdownErrorAreDelivered(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
	))
	reading, release := make(chan struct{}), make(chan struct{})
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(1), uint16(1)).
		Return([]uint16{1}, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(2), uint16(1)).
		RunAndReturn(func(context.Context, modbus.Table, uint16, uint16) ([]uint16, error) {
			close(reading)
			<-release
			return nil, io.EOF
		}).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-reading
	h.cancel()
	close(release)

	var got []string
	for points := range h.data {
		got = append(got, names(points)...)
	}
	assert.Equal(t, []string{"a"}, got)
}

func TestNameAndStopBeforeFirstPoll(t *testing.T) {
	t.Parallel()
	r, err := modbus.New(modbus.Deps{
		Settings: settings(modbus.Point{Name: "p", Register: 1, Type: "uint16", Scale: 1}),
		Dialer:   mocks.NewMockDialer(t), // never dialed, so nothing to close
		Clock:    clocktest.NewFake(start),
		Log:      zerolog.Nop(),
	})
	require.NoError(t, err)
	assert.Equal(t, "inverter", r.Name())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // before Start: the first poll would otherwise run at once
	_, open := <-r.Start(ctx)
	assert.False(t, open)
}

func TestDirectModeAllPointsFailedBacksOff(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1, FunctionCode: 3},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, mock.Anything, uint16(1)).
		Return(nil, lib.ErrRequestTimedOut).Twice()
	// A timeout keeps the connection: Close only on stop.
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	// The reader waits out the backoff (ticker + backoff): the poll failed.
	require.Eventually(t, func() bool { return h.clock.Waiters() >= 2 },
		time.Second, time.Millisecond)
	h.stop() // ends the backoff wait at once
	assert.Empty(t, h.pending)
}

func TestRangeModeSkipsPointThatCannotBeDecoded(t *testing.T) {
	t.Parallel()
	s := settings(
		modbus.Point{Name: "odd", Register: 0, Type: "int64", Scale: 1}, // unknown type
		modbus.Point{Name: "ok", Register: 1, Type: "uint16", Scale: 1},
	)
	s.RangeMode, s.Ranges = true, []string{"0-1"}
	h := newHarness(t, s)
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(0), uint16(2)).
		Return([]uint16{1, 2}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	assert.Equal(t, []string{"ok"}, names(h.receivePoll()))
}

func TestShutdownDuringFailingReadIsNotAnError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(modbus.Point{Name: "p", Register: 1, Type: "uint16", Scale: 1}))
	reading, release := make(chan struct{}), make(chan struct{})
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(1), uint16(1)).
		RunAndReturn(func(context.Context, modbus.Table, uint16, uint16) ([]uint16, error) {
			close(reading)
			<-release // the read fails only after shutdown started
			return nil, io.EOF
		}).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	<-reading
	h.cancel()
	close(release)
	h.stop()
	assert.Empty(t, h.pending)
}

// names returns the point names of a poll.
func names(points []datasource.DataPoint) []string {
	out := make([]string, len(points))
	for i, dp := range points {
		out[i] = dp.PointName
	}
	return out
}

// readTakes makes a read take d of fake time, so reads of one poll receive at different times.
func (h *harness) readTakes(call *mocks.MockClient_Read_Call, values []uint16,
	d time.Duration,
) {
	call.RunAndReturn(func(context.Context, modbus.Table, uint16, uint16) ([]uint16, error) {
		h.clock.Advance(d)
		return values, nil
	}).Once()
}

// Regression: direct mode stamped every point with the time of the last read of its
// poll. Each read keeps its own receive time now; range mode reads one block of
// registers, so all its points share the receive time of the last chunk.
func TestPollTimestamps(t *testing.T) {
	t.Parallel()
	points := []modbus.Point{
		{Name: "a", Register: 10, Type: "uint16", Scale: 1},
		{Name: "b", Register: 20, Type: "uint16", Scale: 1},
	}
	tests := []struct {
		name   string
		ranges []string
		want   map[string]time.Time
	}{
		{name: "direct mode", want: map[string]time.Time{
			"a": start.Add(10 * time.Millisecond), "b": start.Add(20 * time.Millisecond),
		}},
		{name: "range mode", ranges: []string{"10-10", "20-20"}, want: map[string]time.Time{
			"a": start.Add(20 * time.Millisecond), "b": start.Add(20 * time.Millisecond),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := settings(points...)
			s.RangeMode, s.Ranges = tt.ranges != nil, tt.ranges
			h := newHarness(t, s)
			h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
			for _, reg := range []uint16{10, 20} {
				h.readTakes(h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, reg, uint16(1)), []uint16{1}, 10*time.Millisecond)
			}
			h.client.EXPECT().Close().Return(nil).Once()

			h.start()
			got := map[string]time.Time{}
			for _, dp := range h.receivePoll() {
				got[dp.PointName] = dp.Timestamp
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// A tick that comes while the reader waits out its backoff does not start another
// poll right after the retry: the next poll is on the tick after it.
func TestRetryDropsTickDuringBackoff(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(modbus.Point{Name: "p", Register: 5, Type: "uint16", Scale: 1}))
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	failed := signal(h.dialer.EXPECT().Dial(mock.Anything).Return(nil, refused).Once())
	h.start()
	<-failed

	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	read := h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(5), uint16(1)).Return([]uint16{1}, nil)
	read.Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.advance(2, interval) // the backoff and a tick pass together
	assert.Equal(t, "p", h.receive().PointName)

	// The second read only comes with the next tick; a poll on the dropped tick would
	// read before it.
	read.Return([]uint16{2}, nil).Once()
	h.advance(1, interval)
	assert.InDelta(t, 2.0, h.receive().Value, 0)
	h.stop()
}

// Coils are read with their own function code; a range of coils is read in chunks of
// up to 2000, and every coil is a bool point (1 or 0).
func TestRangeModeReadsCoils(t *testing.T) {
	t.Parallel()
	s := settings(
		modbus.Point{Name: "relay", Register: 10, Type: "bool", FunctionCode: 1, Scale: 1},
		modbus.Point{Name: "far", Register: 2100, Type: "bool", FunctionCode: 1, Scale: 1},
	)
	s.RangeMode, s.Ranges = true, []string{"0-2199"}
	h := newHarness(t, s)
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	first := make([]uint16, 2000)
	first[10] = 1
	h.client.EXPECT().Read(mock.Anything, modbus.Coils, uint16(0), uint16(2000)).
		Return(first, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.Coils, uint16(2000), uint16(200)).
		Return(make([]uint16, 200), nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	poll := h.receivePoll()
	require.Len(t, poll, 2)
	assert.InDelta(t, 1.0, poll[0].Value, 0)
	assert.InDelta(t, 0.0, poll[1].Value, 0)
	h.stop()
}

// A string is decoded as text and never scaled; byte_order applies per point.
func TestDirectModeStringsAndByteOrder(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "serial", Register: 1, Count: 2, Type: "string", Scale: 10},
		modbus.Point{
			Name: "energy", Register: 3, Count: 2, Type: "uint32", ByteOrder: "CDAB",
			Scale: 0.5,
		},
	))
	h.dialer.EXPECT().Dial(mock.Anything).Return(h.client, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(1), uint16(2)).
		Return([]uint16{0x5341, 0x3100}, nil).Once()
	h.client.EXPECT().Read(mock.Anything, modbus.HoldingRegisters, uint16(3), uint16(2)).
		Return([]uint16{0x0002, 0x0001}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.start()
	poll := h.receivePoll()
	require.Len(t, poll, 2)
	assert.Equal(t, "SA1", poll[0].Value)
	assert.InDelta(t, float64(0x10002)*0.5, poll[1].Value, 0)
	h.stop()
}
