package app

import (
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/transform"
)

// target is the input of one output with the points of the device it skips. Each
// router has its own targets, so dropped needs no lock.
type target struct {
	name    string
	in      chan<- []datasource.DataPoint
	exclude map[string]bool // point names
	dropped int             // points dropped since the input was last free
}

// route applies the transformer to every poll from data and sends its points to each
// target until data is closed. A full target drops the poll for that target only, so
// a slow output never blocks a device or the other outputs.
func route(
	data <-chan []datasource.DataPoint,
	tr *transform.Transformer,
	device string,
	targets []target,
	log zerolog.Logger,
) {
	log = log.With().Str("device", device).Logger()
	for points := range data {
		points = tr.Apply(points)
		for i := range targets {
			targets[i].send(points, log)
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
	}
}

// filter returns a new slice with the points the target does not exclude: every
// output owns the batch it receives.
func (t *target) filter(points []datasource.DataPoint) []datasource.DataPoint {
	batch := make([]datasource.DataPoint, 0, len(points))
	for _, dp := range points {
		if !t.exclude[dp.PointName] {
			batch = append(batch, dp)
		}
	}
	return batch
}
