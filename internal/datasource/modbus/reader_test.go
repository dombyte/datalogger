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
	data   <-chan datasource.DataPoint
	cancel context.CancelFunc
}

func settings(points ...modbus.Point) modbus.Settings {
	return modbus.Settings{Name: "inverter", PollInterval: interval, Parallelism: 1, Points: points}
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

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.data = r.Start(ctx)
	t.Cleanup(h.stop)
	return h
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
	select {
	case dp := <-h.data:
		return dp
	case <-time.After(time.Second):
		h.t.Fatal("no data point")
		return datasource.DataPoint{}
	}
}

func (h *harness) stop() {
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
	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(100), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{0xFF10}, nil).Twice() // -240
	h.client.EXPECT().Close().Return(nil).Once()

	h.advance(1, interval)
	assert.Equal(t, datasource.DataPoint{
		DeviceName: "inverter",
		PointName:  "temp",
		Value:      -24.0,
		Timestamp:  start.Add(interval),
		Unit:       "C",
	}, h.receive())

	h.advance(1, interval)
	assert.Equal(t, start.Add(2*interval), h.receive().Timestamp)
}

func TestDirectModeSkipsFailedPoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "bad", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "good", Register: 2, Type: "uint16", Scale: 1, FunctionCode: 4},
	))
	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(1), uint16(1), lib.INPUT_REGISTER).
		Return(nil, lib.ErrIllegalDataAddress).Once()
	h.client.EXPECT().ReadRegisters(uint16(2), uint16(1), lib.INPUT_REGISTER).
		Return([]uint16{7}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once() // only on stop, not after the failure

	h.advance(1, interval)
	dp := h.receive()
	assert.Equal(t, "good", dp.PointName)
	assert.InDelta(t, 7.0, dp.Value, 0)
}

func TestReconnectsAfterConnectionError(t *testing.T) {
	t.Parallel()
	point := modbus.Point{Name: "p", Register: 5, Type: "uint16", Scale: 1}
	h := newHarness(t, settings(point))
	second := mocks.NewMockClient(t)

	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(5), uint16(1), lib.HOLDING_REGISTER).
		Return(nil, io.EOF).Once()
	closed := signal(h.client.EXPECT().Close().Return(nil).Once())

	h.advance(1, interval)
	<-closed // the broken connection is dropped at once

	h.dialer.EXPECT().Dial().Return(second, nil).Once()
	second.EXPECT().ReadRegisters(uint16(5), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{42}, nil).Once()
	second.EXPECT().Close().Return(nil).Once()

	h.advance(1, interval)             // next tick, then the reader waits out the backoff
	h.advance(2, 100*time.Millisecond) // initial backoff
	assert.InDelta(t, 42.0, h.receive().Value, 0)
	h.stop() // before second's expectations are checked
}

func TestUnreachableDeviceRecovers(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(modbus.Point{Name: "p", Register: 5, Type: "uint16", Scale: 1}))

	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	failed := signal(h.dialer.EXPECT().Dial().Return(nil, refused).Once())
	h.advance(1, interval)
	<-failed

	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(5), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{1}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.advance(1, interval)
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

	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(0), uint16(125), lib.HOLDING_REGISTER).
		Return(first, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(125), uint16(125), lib.HOLDING_REGISTER).
		Return(second, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(250), uint16(50), lib.HOLDING_REGISTER).
		Return(third, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.advance(1, interval)
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

	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(0), uint16(125), lib.HOLDING_REGISTER).
		Return(make([]uint16, 125), nil).Once()
	failed := signal(h.client.EXPECT().
		ReadRegisters(uint16(125), uint16(75), lib.HOLDING_REGISTER).
		Return(nil, lib.ErrRequestTimedOut).Once())
	h.client.EXPECT().Close().Return(nil).Once()

	h.advance(1, interval)
	<-failed
	select {
	case dp := <-h.data:
		t.Fatalf("unexpected data point %v", dp)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestStopClosesChannelsAndConnection(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(modbus.Point{Name: "p", Register: 1, Type: "uint16", Scale: 1}))
	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(1), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{1}, nil).Once()
	h.client.EXPECT().Close().Return(errors.New("already closed")).Once()

	h.advance(1, interval)
	h.receive()
	h.stop()

	_, open := <-h.data
	assert.False(t, open, "data channel is closed")
}

func TestPollInProgressAtShutdownIsDelivered(t *testing.T) {
	t.Parallel()
	h := newHarness(t, settings(
		modbus.Point{Name: "a", Register: 1, Type: "uint16", Scale: 1},
		modbus.Point{Name: "b", Register: 2, Type: "uint16", Scale: 1},
	))
	reading, release := make(chan struct{}), make(chan struct{})
	h.dialer.EXPECT().Dial().Return(h.client, nil).Once()
	h.client.EXPECT().ReadRegisters(uint16(1), uint16(1), lib.HOLDING_REGISTER).
		RunAndReturn(func(uint16, uint16, lib.RegType) ([]uint16, error) {
			close(reading)
			<-release // the read finishes only after shutdown started
			return []uint16{1}, nil
		}).Once()
	h.client.EXPECT().ReadRegisters(uint16(2), uint16(1), lib.HOLDING_REGISTER).
		Return([]uint16{2}, nil).Once()
	h.client.EXPECT().Close().Return(nil).Once()

	h.advance(1, interval)
	<-reading
	h.cancel()
	close(release)

	var got []string
	for dp := range h.data {
		got = append(got, dp.PointName)
	}
	assert.ElementsMatch(t, []string{"a", "b"}, got, "both points of the running poll arrive")
}
