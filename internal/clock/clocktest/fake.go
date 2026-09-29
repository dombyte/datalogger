// Package clocktest provides a manually advanced clock.Clock for tests.
package clocktest

import (
	"sync"
	"time"

	"github.com/dombyte/datalogger/internal/clock"
)

// Fake is a clock.Clock whose time only moves on Advance. Tickers and After channels
// fire when Advance passes their deadline.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	deadline time.Time
	interval time.Duration // 0 for After
	ch       chan time.Time
	stopped  bool
}

// NewFake returns a Fake set to start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After returns a channel that receives once Advance passes now+d.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.add(d, 0).ch
}

// NewTicker returns a ticker that fires every d of fake time.
func (f *Fake) NewTicker(d time.Duration) clock.Ticker {
	return &fakeTicker{f: f, w: f.add(d, d)}
}

// Waiters returns the number of pending After channels and running tickers, so tests can
// wait until a component blocks on the clock before calling Advance.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, w := range f.waiters {
		if !w.stopped {
			n++
		}
	}
	return n
}

// Advance moves the time forward by d and fires every deadline it passes. A ticker
// fires at most once per Advance (like time.Ticker, it drops ticks nobody reads).
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)

	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if w.stopped {
			continue
		}
		if f.now.Before(w.deadline) {
			kept = append(kept, w)
			continue
		}
		select {
		case w.ch <- f.now:
		default:
		}
		if w.interval > 0 {
			for !f.now.Before(w.deadline) {
				w.deadline = w.deadline.Add(w.interval)
			}
			kept = append(kept, w)
		}
	}
	f.waiters = kept
}

func (f *Fake) add(d, interval time.Duration) *waiter {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &waiter{deadline: f.now.Add(d), interval: interval, ch: make(chan time.Time, 1)}
	f.waiters = append(f.waiters, w)
	return w
}

type fakeTicker struct {
	f *Fake
	w *waiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.w.stopped = true
}
