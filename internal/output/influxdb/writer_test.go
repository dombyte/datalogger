package influxdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output/influxdb"
	"github.com/dombyte/datalogger/internal/output/influxdb/mocks"
)

var start = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

type harness struct {
	t      *testing.T
	clock  *clocktest.Fake
	client *mocks.Client
	input  chan datasource.DataPoint
	done   <-chan error
	writes chan []string // line protocol of every WritePoints call
}

func newHarness(t *testing.T, s influxdb.Settings) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		clock:  clocktest.NewFake(start),
		client: mocks.NewClient(t),
		input:  make(chan datasource.DataPoint),
		writes: make(chan []string, 10),
	}
	w, err := influxdb.New(influxdb.Deps{
		Settings: s, Client: h.client, Clock: h.clock, Log: zerolog.Nop(),
	})
	require.NoError(t, err)
	h.done = w.Start(context.Background(), h.input)
	return h
}

// expectWrites records the next n WritePoints calls, answering with errs in order.
func (h *harness) expectWrites(errs ...error) {
	for _, err := range errs {
		h.client.EXPECT().WritePoints(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, points []*influxdb3.Point, _ ...influxdb3.WriteOption) error {
				lines := make([]string, len(points))
				for i, p := range points {
					b, mErr := p.MarshalBinary(influxdb3.Nanosecond)
					require.NoError(h.t, mErr)
					lines[i] = string(b)
				}
				h.writes <- lines
				return err
			}).Once()
	}
}

func (h *harness) write() []string {
	h.t.Helper()
	select {
	case lines := <-h.writes:
		return lines
	case <-time.After(time.Second):
		h.t.Fatal("no write")
		return nil
	}
}

func (h *harness) advance(n int, d time.Duration) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.clock.Waiters() >= n },
		time.Second, time.Millisecond)
	h.clock.Advance(d)
}

// stop closes the input and waits for the writer to finish.
func (h *harness) stop() {
	h.client.EXPECT().Close().Return(nil).Once()
	close(h.input)
	for err := range h.done {
		require.NoError(h.t, err)
	}
}

func point(name string, value any, unit string) datasource.DataPoint {
	return datasource.DataPoint{
		DeviceName: "meter", PointName: name, Value: value, Timestamp: start, Unit: unit,
	}
}

func TestNewChecksDependencies(t *testing.T) {
	t.Parallel()
	_, err := influxdb.New(influxdb.Deps{Clock: clocktest.NewFake(start)})
	assert.ErrorIs(t, err, influxdb.ErrMissingDependency)
	_, err = influxdb.New(influxdb.Deps{Client: mocks.NewClient(t)})
	assert.ErrorIs(t, err, influxdb.ErrMissingDependency)
}

func TestWritesFullBatchWithSchema(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{Name: "db", BatchSize: 2})
	h.expectWrites(nil)

	h.input <- point("power", 1.5, "W")
	h.input <- point("on", true, "")

	assert.Equal(t, []string{
		"meter,point=power,unit=W value=1.5 1790683200000000000\n",
		"meter,point=on value=true 1790683200000000000\n",
	}, h.write())
	h.stop()
}

func TestFlushesPartialBatchEveryTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 100, BatchTimeout: time.Second})
	h.expectWrites(nil)

	h.input <- point("power", 1.0, "")
	h.advance(1, time.Second)
	assert.Len(t, h.write(), 1)
	h.stop()
}

func TestWritesTheRestWhenInputCloses(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 100})
	h.expectWrites(nil)

	for range 3 {
		h.input <- point("power", 1.0, "")
	}
	h.stop()
	assert.Len(t, h.write(), 3)
}

func TestRetriesThenDropsAndRecovers(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 2, RetryDelay: time.Second})
	down := errors.New("503")
	h.expectWrites(down, down, down, nil)

	h.input <- point("lost", 1.0, "")
	h.write()
	h.advance(2, time.Second) // ticker + retry delay
	h.write()
	h.advance(2, time.Second)
	h.write() // third and last attempt: the batch is dropped

	h.input <- point("next", 2.0, "")
	assert.Contains(t, h.write()[0], "point=next")
	h.stop()
}
