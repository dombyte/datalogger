package clocktest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestFakeAfter(t *testing.T) {
	t.Parallel()
	f := clocktest.NewFake(start)
	ch := f.After(time.Second)
	assert.Equal(t, 1, f.Waiters())

	f.Advance(999 * time.Millisecond)
	assert.Empty(t, ch)

	f.Advance(time.Millisecond)
	assert.Equal(t, start.Add(time.Second), <-ch)
	assert.Equal(t, 0, f.Waiters())
}

func TestFakeTicker(t *testing.T) {
	t.Parallel()
	f := clocktest.NewFake(start)
	tk := f.NewTicker(time.Second)

	f.Advance(time.Second)
	assert.Equal(t, start.Add(time.Second), <-tk.C())

	f.Advance(3 * time.Second) // one tick per Advance, like time.Ticker drops ticks
	assert.Equal(t, start.Add(4*time.Second), <-tk.C())
	assert.Empty(t, tk.C())

	tk.Stop()
	assert.Equal(t, 0, f.Waiters(), "a stopped ticker is not counted")
	f.Advance(time.Second)
	assert.Empty(t, tk.C())
	assert.Equal(t, 0, f.Waiters())
}

func TestFakeNowAndUnreadTick(t *testing.T) {
	t.Parallel()
	f := clocktest.NewFake(start)
	tk := f.NewTicker(time.Second)

	f.Advance(time.Second)
	f.Advance(time.Second) // the first tick was not read: this one is dropped
	assert.Equal(t, start.Add(2*time.Second), f.Now())
	assert.Equal(t, start.Add(time.Second), <-tk.C())
	assert.Empty(t, tk.C())
}
