package spool

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
)

var start = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

// poll returns a poll of one point with value n.
func poll(n int) []datasource.DataPoint {
	return []datasource.DataPoint{{
		DeviceName: "meter", PointName: "p", Value: float64(n), Timestamp: start, Unit: "W",
	}}
}

// harness is a started spool with its channels.
type harness struct {
	spool  *Spool
	in     chan []datasource.DataPoint
	out    chan []datasource.DataPoint
	cancel context.CancelFunc
	done   <-chan struct{}
	logs   *bytes.Buffer
}

func startSpool(t *testing.T, path string, maxBytes int64, outSize int) *harness {
	t.Helper()
	logs := &bytes.Buffer{}
	s, err := New(Deps{
		Settings: Settings{Name: "db", Path: path, MaxBytes: maxBytes},
		Clock:    clocktest.NewFake(start), Log: zerolog.New(logs),
	})
	require.NoError(t, err)
	h := &harness{
		spool: s, in: make(chan []datasource.DataPoint), out: make(chan []datasource.DataPoint, outSize),
		logs: logs,
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = s.Start(ctx, h.in, h.out)
	return h
}

// stop closes the input and waits until the spool closed its output.
func (h *harness) stop() {
	close(h.in)
	for range h.out {
	}
}

func value(t *testing.T, p []datasource.DataPoint) float64 {
	t.Helper()
	require.Len(t, p, 1)
	v, ok := p[0].Value.(float64)
	require.True(t, ok)
	return v
}

// While the output has room, polls go straight to it and nothing is written.
func TestPassesThroughWhileOutputHasRoom(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db.spool")
	h := startSpool(t, path, 1<<20, 2)
	h.in <- poll(1)
	assert.InDelta(t, 1.0, value(t, <-h.out), 0)
	h.stop()
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Zero(t, info.Size())
	assert.Equal(t, int64(1), h.spool.Delivered())
}

// Polls that find the output full are spooled and handed over in order; the file is
// emptied once everything was handed over.
func TestSpoolsWhileOutputIsFullAndForwardsInOrder(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db.spool")
	h := startSpool(t, path, 1<<20, 1)
	for n := 1; n <= 4; n++ {
		h.in <- poll(n) // 1 fills the output, 2-4 go to disk
	}
	require.Eventually(t, func() bool { return h.spool.Backlog() >= 2 }, time.Second, time.Millisecond)
	for n := 1; n <= 4; n++ {
		assert.InDelta(t, float64(n), value(t, <-h.out), 0, "in order")
	}
	h.in <- poll(5) // nothing spooled any more: straight through
	assert.InDelta(t, 5.0, value(t, <-h.out), 0)
	h.stop()
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Zero(t, info.Size(), "emptied after the backlog")
	assert.Zero(t, h.spool.Backlog())
}

// The backlog survives a restart: the next spool hands it over first.
func TestBacklogSurvivesRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "spool", "db.spool")
	h := startSpool(t, path, 1<<20, 1)
	for n := 1; n <= 3; n++ {
		h.in <- poll(n)
	}
	require.Eventually(t, func() bool { return h.spool.Backlog() == 2 }, time.Second, time.Millisecond)
	assert.InDelta(t, 1.0, value(t, <-h.out), 0)
	// The output's queue takes 2 and is full again: 3 stays on disk when it stops.
	require.Eventually(t, func() bool { return h.spool.Backlog() == 1 }, time.Second, time.Millisecond)
	h.cancel()
	<-h.done // not read from out: a free slot would let 3 through before the stop

	next := startSpool(t, path, 1<<20, 4)
	assert.Equal(t, int64(1), next.spool.Backlog())
	assert.InDelta(t, 3.0, value(t, <-next.out), 0, "the poll left over, once")
	next.in <- poll(4)
	assert.InDelta(t, 4.0, value(t, <-next.out), 0)
	next.stop()
	assert.Contains(t, next.logs.String(), "Spool holds polls from the last run")
}

// A full spool drops new polls, warned once, and logs when it takes polls again.
func TestFullSpoolDrops(t *testing.T) {
	t.Parallel()
	line, err := encodePoll(poll(2))
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "db.spool")
	h := startSpool(t, path, int64(len(line)), 1)
	h.in <- poll(1) // the output
	h.in <- poll(2) // fills the spool
	h.in <- poll(3) // dropped
	h.in <- poll(4) // dropped
	assert.InDelta(t, 1.0, value(t, <-h.out), 0)
	assert.InDelta(t, 2.0, value(t, <-h.out), 0)
	h.in <- poll(5) // room again
	assert.InDelta(t, 5.0, value(t, <-h.out), 0)
	h.stop()
	assert.Equal(t, 1, bytes.Count(h.logs.Bytes(), []byte("Cannot spool")), "warned once")
	assert.Contains(t, h.logs.String(), `"dropped":2`)
}

// A line cut short by a crash is removed; a line that cannot be decoded is skipped.
func TestRepairsAndSkipsDamagedLines(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db.spool")
	good, err := encodePoll(poll(7))
	require.NoError(t, err)
	content := append([]byte("not json\n"), good...)
	content = append(content, []byte(`[{"d":"meter"`)...) // cut short
	require.NoError(t, os.WriteFile(path, content, 0o600))

	h := startSpool(t, path, 1<<20, 4)
	assert.Equal(t, int64(2), h.spool.Backlog(), "the cut line is gone")
	assert.InDelta(t, 7.0, value(t, <-h.out), 0)
	h.stop()
	assert.Contains(t, h.logs.String(), "Skipping a poll the spool cannot decode")
}

// A saved read position that is past the file (another file, a lost write) replays
// from the end, not from a wrong place.
func TestOffsetIsClampedToTheFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db.spool")
	require.NoError(t, os.WriteFile(path+offsetSuffix, []byte(strconv.Itoa(1<<30)), 0o600))
	h := startSpool(t, path, 1<<20, 1)
	assert.Zero(t, h.spool.Backlog())
	h.stop()
}

func TestValuesKeepTheirType(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 11, 13, 12, 0, 0, 123456789, time.UTC)
	in := []datasource.DataPoint{
		{DeviceName: "d", PointName: "f", Value: 1.5, Timestamp: ts, Unit: "W"},
		{DeviceName: "d", PointName: "nan", Value: math.NaN(), Timestamp: ts},
		{DeviceName: "d", PointName: "inf", Value: math.Inf(-1), Timestamp: ts},
		{DeviceName: "d", PointName: "i", Value: int64(math.MaxInt64), Timestamp: ts},
		{DeviceName: "d", PointName: "u", Value: uint64(math.MaxUint64), Timestamp: ts},
		{DeviceName: "d", PointName: "b", Value: true, Timestamp: ts},
		{DeviceName: "d", PointName: "s", Value: "a \"b\"\n", Timestamp: ts},
	}
	line, err := encodePoll(in)
	require.NoError(t, err)
	out, err := decodePoll(line)
	require.NoError(t, err)
	require.Len(t, out, len(in))
	assert.True(t, math.IsNaN(out[1].Value.(float64)))
	assert.True(t, math.IsInf(out[2].Value.(float64), -1))
	out[1].Value, in[1].Value = nil, nil
	assert.Equal(t, in, out)

	_, err = encodePoll([]datasource.DataPoint{{Value: []int{1}}})
	assert.ErrorContains(t, err, "cannot spool a value of type []int")
	_, err = decodePoll([]byte(`[{"k":"x","t":"2026-11-13T12:00:00Z"}]`))
	assert.ErrorContains(t, err, "unknown value kind")
}

func TestNewChecksDependencies(t *testing.T) {
	t.Parallel()
	_, err := New(Deps{})
	assert.ErrorIs(t, err, ErrMissingDependency)
	_, err = New(Deps{Settings: Settings{Path: t.TempDir()}, Clock: clocktest.NewFake(start)})
	assert.ErrorContains(t, err, "spool: open", "a directory is no spool file")
}

// A poll that cannot be written (a disk that failed) is dropped and warned about.
func TestFailedWriteDrops(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db.spool")
	h := startSpool(t, path, 1<<20, 1)
	h.in <- poll(1) // the output
	h.cancel()
	<-h.done

	var logs bytes.Buffer
	s, err := New(Deps{
		Settings: Settings{Name: "db", Path: path, MaxBytes: 1 << 20},
		Clock:    clocktest.NewFake(start), Log: zerolog.New(&logs),
	})
	require.NoError(t, err)
	require.NoError(t, s.file.Close()) // every write fails now
	s.append(poll(2))
	s.append(poll(3))
	assert.Zero(t, s.Backlog())
	assert.Equal(t, 1, bytes.Count(logs.Bytes(), []byte("Cannot spool")), "warned once")
	_, err = encodePoll([]datasource.DataPoint{{Value: struct{}{}}})
	assert.Error(t, err)
}
