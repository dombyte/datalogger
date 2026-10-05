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
	in      chan<- datasource.DataPoint
	exclude map[string]bool // point names
	dropped int             // points dropped since the input was last free
}

// route applies the transformer to every poll from data and copies its points to each
// target until data is closed. A full target drops the point for that target only, so
// a slow output never blocks a device or the other outputs.
func route(
	data <-chan []datasource.DataPoint,
	tr *transform.Transformer,
	targets []target,
	log zerolog.Logger,
) {
	for points := range data {
		for _, dp := range tr.Apply(points) {
			send(dp, targets, log)
		}
	}
}

// send offers dp to every target that does not exclude it, without blocking. A full
// target is warned about once; the points dropped until it takes points again are
// counted and logged then, so a stalled output does not log a line per point.
func send(dp datasource.DataPoint, targets []target, log zerolog.Logger) {
	for i := range targets {
		t := &targets[i]
		if t.exclude[dp.PointName] {
			continue
		}
		select {
		case t.in <- dp:
			if t.dropped > 0 {
				log.Info().Str("device", dp.DeviceName).Str("output", t.name).
					Int("dropped", t.dropped).Msg("Output channel takes data points again")
				t.dropped = 0
			}
		default:
			if t.dropped == 0 {
				log.Warn().Str("device", dp.DeviceName).Str("output", t.name).
					Msg("Output channel full, dropping data points until it has room")
			}
			t.dropped++
		}
	}
}
