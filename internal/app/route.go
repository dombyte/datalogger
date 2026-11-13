package app

import (
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/transform"
)

// target is the input of one output with the points of the device it skips. Each
// router has its own targets, so dropped needs no lock; state is shared by the routers
// of the output and read by Status.
type target struct {
	name    string
	in      chan<- []datasource.DataPoint
	exclude map[string]bool // point names
	dropped int             // points dropped since the input was last free
	state   *outputState
}

// deviceRoute is what the router of one device needs: its transformer, its targets
// and the state Status reports.
type deviceRoute struct {
	name      string
	transform *transform.Transformer
	targets   []target
	state     *deviceState
}

// route applies the transformer to every poll from data and sends its points to each
// target until data is closed. A full target drops the poll for that target only, so
// a slow output never blocks a device or the other outputs.
func route(data <-chan []datasource.DataPoint, d deviceRoute, clk clock.Clock, log zerolog.Logger) {
	log = log.With().Str("device", d.name).Logger()
	for points := range data {
		d.state.polled(len(points), clk.Now())
		points = d.transform.Apply(points)
		for i := range d.targets {
			d.targets[i].send(points, log)
		}
	}
}

// send offers the points the target does not exclude, as one batch of its own,
// without blocking. A full target is warned about once; the points dropped until it
// takes data again are counted and logged then, so a stalled output does not log a
// line per poll.
func (t *target) send(points []datasource.DataPoint, log zerolog.Logger) {
	batch := t.filter(points)
	if len(batch) == 0 {
		return
	}
	select {
	case t.in <- batch:
		t.state.sent.Add(1)
		if t.dropped > 0 {
			log.Info().Str("output", t.name).Int("dropped", t.dropped).
				Msg("Output channel takes data points again")
			t.dropped = 0
		}
	default:
		if t.dropped == 0 {
			log.Warn().Str("output", t.name).
				Msg("Output channel full, dropping data points until it has room")
		}
		t.dropped += len(batch)
		t.state.dropped.Add(int64(len(batch)))
	}
}

// filter returns the points the target does not exclude. Without excludes it is the
// poll itself, shared with the other outputs (writers only read it); with excludes a
// new slice.
func (t *target) filter(points []datasource.DataPoint) []datasource.DataPoint {
	if len(t.exclude) == 0 {
		return points
	}
	batch := make([]datasource.DataPoint, 0, len(points))
	for _, dp := range points {
		if !t.exclude[dp.PointName] {
			batch = append(batch, dp)
		}
	}
	return batch
}
