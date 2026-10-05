package app

import (
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/transform"
)

// target is the input of one output with the points of the device it skips.
type target struct {
	name    string
	in      chan<- datasource.DataPoint
	exclude map[string]bool // point names
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

// send offers dp to every target that does not exclude it, without blocking.
func send(dp datasource.DataPoint, targets []target, log zerolog.Logger) {
	for _, t := range targets {
		if t.exclude[dp.PointName] {
			continue
		}
		select {
		case t.in <- dp:
		default:
			log.Warn().
				Str("device", dp.DeviceName).
				Str("point", dp.PointName).
				Str("output", t.name).
				Msg("Output channel full, dropping data point")
		}
	}
}
