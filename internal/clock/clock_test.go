package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/clock/clocktest"
)

func TestSleep(t *testing.T) {
	t.Parallel()
	f := clocktest.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	done := make(chan bool)
	go func() { done <- clock.Sleep(context.Background(), f, time.Second) }()
	assert.Eventually(t, func() bool { return f.Waiters() == 1 }, time.Second, time.Millisecond)
	f.Advance(time.Second)
	assert.True(t, <-done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, clock.Sleep(ctx, f, time.Hour))
}

// TestReal checks the wiring to package time without waiting: After(0) fires at once
// and the ticker is only created and stopped.
func TestReal(t *testing.T) {
	t.Parallel()
	var c clock.Clock = clock.Real{}

	assert.WithinDuration(t, time.Now(), c.Now(), time.Minute)
	<-c.After(0)
	tk := c.NewTicker(time.Hour)
	assert.NotNil(t, tk.C())
	tk.Stop()
}
