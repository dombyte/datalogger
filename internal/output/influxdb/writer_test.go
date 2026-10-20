package influxdb_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
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
	client *mocks.MockClient
	input  chan []datasource.DataPoint
	done   <-chan error
	writes chan []string // line protocol of every WritePoints call
	logs   *bytes.Buffer // read only after stop: the writer goroutine has ended
}

func newHarness(t *testing.T, s influxdb.Settings) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		clock:  clocktest.NewFake(start),
		client: mocks.NewMockClient(t),
		input:  make(chan []datasource.DataPoint),
		writes: make(chan []string, 10),
		logs:   &bytes.Buffer{},
	}
	w, err := influxdb.New(influxdb.Deps{
		Settings: s, Client: h.client, Clock: h.clock, Log: zerolog.New(h.logs),
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
	_, err = influxdb.New(influxdb.Deps{Client: mocks.NewMockClient(t)})
	assert.ErrorIs(t, err, influxdb.ErrMissingDependency)
}

// A poll is one line: table = device, one field per point, no tags.
func TestWritesOneLinePerPoll(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{Name: "db", BatchSize: 2})
	h.expectWrites(nil)

	h.input <- []datasource.DataPoint{
		point("power", 1.5, "W"), point("count", int64(3), ""),
		point("on", true, ""), point("state", "ok", ""), point("missing", nil, ""),
	}
	h.input <- []datasource.DataPoint{point("power", 2.5, "W")}

	assert.Equal(t, []string{
		"meter count=3i,on=true,power=1.5,state=\"ok\" 1790683200000000000\n",
		"meter power=2.5 1790683200000000000\n",
	}, h.write())
	h.stop()
}

// A poll whose points were read at different times (Modbus direct mode) is one line per
// timestamp, so every value keeps its own reading time; points that share a time share
// a line.
func TestWritesOneLinePerTimestamp(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1})
	h.expectWrites(nil)

	at := func(dp datasource.DataPoint, d time.Duration) datasource.DataPoint {
		dp.Timestamp = start.Add(d)
		return dp
	}
	h.input <- []datasource.DataPoint{
		at(point("voltage", 230.1, "V"), 20*time.Millisecond),
		at(point("current", 4.2, "A"), 40*time.Millisecond),
		at(point("power", 966.0, "W"), 40*time.Millisecond),
	}

	assert.Equal(t, []string{
		"meter voltage=230.1 1790683200020000000\n",
		"meter current=4.2,power=966 1790683200040000000\n",
	}, h.write())
	h.stop()
}

func TestFlushesPartialBatchEveryTimeout(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 100, BatchTimeout: time.Second})
	h.expectWrites(nil)

	h.input <- []datasource.DataPoint{point("power", 1.0, "")}
	h.advance(1, time.Second)
	assert.Len(t, h.write(), 1)
	h.stop()
}

func TestWritesTheRestWhenInputCloses(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 100})
	h.expectWrites(nil)

	for range 3 {
		h.input <- []datasource.DataPoint{point("power", 1.0, "")}
	}
	h.stop()
	assert.Len(t, h.write(), 3)
}

func TestRetriesThenDropsAndRecovers(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 2, RetryDelay: time.Second})
	down := errors.New("503")
	h.expectWrites(down, down, down, nil)

	h.input <- []datasource.DataPoint{point("lost", 1.0, "")}
	h.write()
	h.advance(2, time.Second) // ticker + retry delay
	h.write()
	h.advance(2, time.Second)
	h.write() // third and last attempt: the batch is dropped

	h.input <- []datasource.DataPoint{point("next", 2.0, "")}
	assert.Contains(t, h.write()[0], "meter next=2")
	h.stop()
}

// startCancellable starts a writer with a cancellable context; the client is closed
// with closeErr when the writer stops.
func startCancellable(t *testing.T, s influxdb.Settings, closeErr error) (
	*harness, context.CancelFunc,
) {
	t.Helper()
	h := &harness{
		t: t, clock: clocktest.NewFake(start), client: mocks.NewMockClient(t),
		input: make(chan []datasource.DataPoint), writes: make(chan []string, 10),
	}
	w, err := influxdb.New(influxdb.Deps{
		Settings: s, Client: h.client, Clock: h.clock, Log: zerolog.Nop(),
	})
	require.NoError(t, err)
	assert.Equal(t, s.Name, w.Name())
	h.client.EXPECT().Close().Return(closeErr).Once()
	ctx, cancel := context.WithCancel(context.Background())
	h.done = w.Start(ctx, h.input)
	return h, cancel
}

func TestStopsOnCancelEvenWhenCloseFails(t *testing.T) {
	t.Parallel()
	h, cancel := startCancellable(t, influxdb.Settings{Name: "db"}, errors.New("closed"))

	cancel()
	for err := range h.done {
		require.NoError(t, err, "a failed close is only logged")
	}
}

func TestCancelEndsRetries(t *testing.T) {
	t.Parallel()
	h, cancel := startCancellable(t,
		influxdb.Settings{BatchSize: 1, MaxRetries: 5, RetryDelay: time.Hour}, nil)
	h.expectWrites(errors.New("503"))

	h.input <- []datasource.DataPoint{point("power", 1.0, "")}
	h.write()
	require.Eventually(t, func() bool { return h.clock.Waiters() >= 2 }, // ticker + retry
		time.Second, time.Millisecond)
	cancel()
	for err := range h.done {
		require.NoError(t, err)
	}
}

// Regression: a 4xx answer (bad token, rejected lines) was retried MaxRetries times,
// blocking the writer for nothing; it is now dropped at once.
func TestDoesNotRetryClientErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 3, RetryDelay: time.Hour})
	h.expectWrites(&influxdb3.ServerError{StatusCode: 401, Message: "unauthorized"}, nil)

	h.input <- []datasource.DataPoint{point("lost", 1.0, "")}
	h.write()
	h.input <- []datasource.DataPoint{point("next", 2.0, "")} // no retry delay in between
	assert.Contains(t, h.write()[0], "meter next=2")
	h.stop()
}

func TestRetriesServerErrorsAndRateLimits(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 2, RetryDelay: time.Second})
	h.expectWrites(&influxdb3.ServerError{StatusCode: 503}, &influxdb3.ServerError{StatusCode: 429}, nil)

	h.input <- []datasource.DataPoint{point("power", 1.0, "")}
	h.write()
	h.advance(2, time.Second) // ticker + retry delay
	h.write()
	h.advance(2, time.Second)
	assert.Contains(t, h.write()[0], "meter power=1")
	h.stop()
}

// Regression: a value the client cannot encode (e.g. a JSON object from an HTTP
// device) failed WritePoints for the whole batch, with the points of every device.
func TestSkipsValuesLineProtocolCannotEncode(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 2})
	h.expectWrites(nil)

	h.input <- []datasource.DataPoint{point("object", map[string]any{"a": 1.0}, "")}
	h.input <- []datasource.DataPoint{point("list", []any{1.0}, "")}
	h.input <- []datasource.DataPoint{point("power", 1.5, "W")}
	h.input <- []datasource.DataPoint{point("count", int64(3), "")}
	assert.Equal(t, []string{
		"meter power=1.5 1790683200000000000\n",
		"meter count=3i 1790683200000000000\n",
	}, h.write())
	h.stop()
}

func partialWrite(line int) *influxdb3.PartialWriteError {
	return &influxdb3.PartialWriteError{
		ServerError: influxdb3.ServerError{
			StatusCode: 400, Message: "partial write of line protocol occurred",
		},
		LineErrors: []influxdb3.PartialWriteLineError{{
			LineNumber: line, ErrorMessage: "invalid column type", OriginalLine: "meter bad",
		}},
	}
}

// Regression: a partial write was logged as a dropped batch, although the server had
// stored every line it did not reject.
func TestPartialWriteCountsAsWritten(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 3, RetryDelay: time.Hour})
	h.expectWrites(partialWrite(1), partialWrite(1), nil)

	for _, name := range []string{"first", "second", "third"} {
		h.input <- []datasource.DataPoint{point(name, 1.0, "")} // no retry delay in between
		h.write()
	}
	h.stop()

	logs := h.logs.String()
	assert.NotContains(t, logs, "Dropping batch")
	assert.Equal(t, 1, strings.Count(logs, `"level":"warn"`), "warned once, then debug")
	assert.Contains(t, logs, `"rejected":1,"line":1,"error":"invalid column type"`)
	assert.Contains(t, logs, "InfluxDB accepts every line again")
}

// Regression: a Retry-After longer than RetryDelay was ignored, so the writer retried a
// busy server too early.
func TestRetryWaitsForRetryAfter(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 1, RetryDelay: time.Second})
	h.client.EXPECT().WritePoints(mock.Anything, mock.Anything).
		Return(&influxdb3.ServerError{StatusCode: 429, RetryAfter: 5}).Once()
	retried := make(chan time.Time, 1)
	h.client.EXPECT().WritePoints(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, []*influxdb3.Point, ...influxdb3.WriteOption) error {
			retried <- h.clock.Now()
			return nil
		}).Once()

	h.input <- []datasource.DataPoint{point("power", 1.0, "")}
	h.advance(2, time.Second) // ticker + retry wait: RetryDelay has passed
	h.advance(2, 4*time.Second)
	assert.Equal(t, start.Add(5*time.Second), <-retried)
	h.stop()
}

// A Retry-After is waited for at most a minute: meanwhile the input fills and drops
// new polls.
func TestRetryAfterIsCapped(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 1, MaxRetries: 1, RetryDelay: time.Second})
	h.client.EXPECT().WritePoints(mock.Anything, mock.Anything).
		Return(&influxdb3.ServerError{StatusCode: 503, RetryAfter: 86400}).Once()
	retried := make(chan time.Time, 1)
	h.client.EXPECT().WritePoints(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, []*influxdb3.Point, ...influxdb3.WriteOption) error {
			retried <- h.clock.Now()
			return nil
		}).Once()

	h.input <- []datasource.DataPoint{point("power", 1.0, "")}
	h.advance(2, time.Minute) // ticker + retry wait
	assert.Equal(t, start.Add(time.Minute), <-retried)
	h.stop()
}

// InfluxDB keeps one type per column and rejects a line with a value of another type,
// which would lose the whole poll; only that value is skipped.
func TestSkipsValueOfAnotherColumnType(t *testing.T) {
	t.Parallel()
	h := newHarness(t, influxdb.Settings{BatchSize: 3})
	h.expectWrites(nil)

	h.input <- []datasource.DataPoint{point("a", 1.5, ""), point("b", "x", "")}
	h.input <- []datasource.DataPoint{point("a", "N/A", ""), point("b", "y", "")}
	h.input <- []datasource.DataPoint{point("a", 2.5, ""), point("b", "z", "")}
	assert.Equal(t, []string{
		"meter a=1.5,b=\"x\" 1790683200000000000\n",
		"meter b=\"y\" 1790683200000000000\n",
		"meter a=2.5,b=\"z\" 1790683200000000000\n",
	}, h.write())
	h.stop()

	logs := h.logs.String()
	assert.Contains(t, logs, `"column_type":"float","value_type":"string"`)
	assert.Contains(t, logs, "Point has its column type again")
}
