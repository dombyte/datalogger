package app

import (
	"sync/atomic"
	"time"

	"github.com/dombyte/datalogger/internal/status"
)

// deviceState counts the polls of one device; its router writes it, Status reads it.
type deviceState struct {
	polls    atomic.Int64
	points   atomic.Int64 // in the last poll
	lastPoll atomic.Int64 // UnixNano of the last poll, 0 before the first
	stopped  atomic.Bool  // the reader stopped while the app was running
}

// polled records a poll with n points received at t.
func (d *deviceState) polled(n int, t time.Time) {
	d.polls.Add(1)
	d.points.Store(int64(n))
	d.lastPoll.Store(t.UnixNano())
}

// outputState counts the polls handed to one output; its routers write it, Status reads
// it. progress is Status's own: when the output last took a poll.
type outputState struct {
	sent    atomic.Int64 // polls put into the queue
	dropped atomic.Int64 // points dropped because the queue was full
	stopped atomic.Bool  // the writer stopped while the app was running

	progress struct { // guarded by App.statusMu
		taken int64     // polls the writer had taken at since
		since time.Time // when taken last changed, or the queue was last empty
	}
}

// Status returns the state of every device and output. It can be called at any time
// from any goroutine. Whether an output is stuck is measured between calls: the time
// since its queue last moved, as seen by the calls (the healthcheck calls it regularly).
func (a *App) Status() status.Snapshot {
	now := a.clock.Now()
	a.statusMu.Lock()
	defer a.statusMu.Unlock()

	snap := status.Snapshot{Time: now.UTC(), Stopping: a.stopping.Load()}
	for _, s := range a.sources {
		snap.Devices = append(snap.Devices, deviceStatus(s))
	}
	for i, o := range a.outputs {
		snap.Outputs = append(snap.Outputs, outputStatus(o, len(a.inputs[i]), now))
	}
	return snap
}

// deviceStatus returns the state of one device.
func deviceStatus(s source) status.Device {
	d := status.Device{
		Name:    s.reader.Name(),
		Polls:   s.state.polls.Load(),
		Points:  s.state.points.Load(),
		Stopped: s.state.stopped.Load(),
	}
	if last := s.state.lastPoll.Load(); last != 0 {
		t := time.Unix(0, last).UTC()
		d.LastPoll = &t
	}
	return d
}

// outputStatus returns the state of one output with queued polls in its queue, and
// updates its progress.
func outputStatus(o outputRoute, queued int, now time.Time) status.Output {
	st := o.state
	sent := st.sent.Load()
	var spooled int64
	if o.spool != nil {
		// The routers send to the spool; the writer's queue is filled by it.
		sent, spooled = o.spool.Delivered(), o.spool.Backlog()
	}
	taken := sent - int64(queued)
	if queued == 0 || taken != st.progress.taken || st.progress.since.IsZero() {
		st.progress.taken, st.progress.since = taken, now
	}
	out := status.Output{
		Name:     o.writer.Name(),
		Queued:   queued,
		Capacity: o.bufferSize,
		Sent:     sent,
		Spooled:  spooled,
		Dropped:  st.dropped.Load(),
		Stopped:  st.stopped.Load(),
	}
	if queued > 0 {
		out.StalledSeconds = int64(now.Sub(st.progress.since) / time.Second)
	}
	return out
}
