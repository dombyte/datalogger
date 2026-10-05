package app

import (
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/datasource"
)

// route copies every point of each poll from data to each target until data is
// closed. A full target drops the point for that target only, so a slow output never
// blocks a device or the other outputs.
func route(
	data <-chan []datasource.DataPoint,
	targets map[string]chan<- datasource.DataPoint,
	log zerolog.Logger,
) {
	for points := range data {
		for _, dp := range points {
			send(dp, targets, log)
		}
	}
}

// send offers dp to every target without blocking.
func send(
	dp datasource.DataPoint,
	targets map[string]chan<- datasource.DataPoint,
	log zerolog.Logger,
) {
	for name, target := range targets {
		select {
		case target <- dp:
		default:
			log.Warn().
				Str("device", dp.DeviceName).
				Str("point", dp.PointName).
				Str("output", name).
				Msg("Output channel full, dropping data point")
		}
	}
}
