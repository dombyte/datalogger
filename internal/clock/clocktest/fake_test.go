package clocktest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/clock/clocktest"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestFakeAfter(t *testing.T) {
	t.Parallel()
	f := clocktest.NewFake(start)
	ch := f.After(time.Second)

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
	f.Advance(time.Second)
	assert.Empty(t, tk.C())
	assert.Equal(t, 0, f.Waiters())
}

func TestSleep(t *testing.T) {
	t.Parallel()
	f := clocktest.NewFake(start)

	done := make(chan bool)
	go func() { done <- clock.Sleep(context.Background(), f, time.Second) }()
	assert.Eventually(t, func() bool { return f.Waiters() == 1 }, time.Second, time.Millisecond)
	f.Advance(time.Second)
	assert.True(t, <-done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, clock.Sleep(ctx, f, time.Hour))
}
