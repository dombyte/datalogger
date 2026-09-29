// Package clock abstracts time for components with waiting loops, so tests can drive
// time with clocktest.Fake instead of sleeping.
package clock

import (
	"context"
	"time"
)

// Clock is the time source injected into components.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
	After(d time.Duration) <-chan time.Time
}

// Ticker delivers ticks at a fixed interval until it is stopped.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real is the Clock backed by package time.
type Real struct{}

// Now returns the current time.
func (Real) Now() time.Time { return time.Now() }

// NewTicker returns a time.Ticker.
func (Real) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

// After returns time.After(d).
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Sleep waits for d on c; it returns false if ctx ends first.
func Sleep(ctx context.Context, c Clock, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-c.After(d):
		return true
	}
}
