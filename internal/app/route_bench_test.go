package app

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/datasource"
)

// BenchmarkSend routes one poll of 50 points to three outputs, one of them with an
// exclude.
func BenchmarkSend(b *testing.B) {
	points := make([]datasource.DataPoint, 50)
	for i := range points {
		points[i] = datasource.DataPoint{
			DeviceName: "inverter", PointName: fmt.Sprintf("p%d", i), Value: float64(i),
			Timestamp: time.Unix(0, 0),
		}
	}
	targets := make([]target, 3)
	inputs := make([]chan []datasource.DataPoint, len(targets))
	for i := range targets {
		inputs[i] = make(chan []datasource.DataPoint, 1)
		targets[i] = target{
			name: strconv.Itoa(i), in: inputs[i], exclude: map[string]bool{}, state: &outputState{},
		}
	}
	targets[2].exclude["p7"] = true
	log := zerolog.Nop()

	for b.Loop() {
		for i := range targets {
			targets[i].send(points, log)
			<-inputs[i]
		}
	}
}
