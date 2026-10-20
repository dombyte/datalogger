package modbus

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"

	"github.com/dombyte/datalogger/internal/clock"
)

// instantClient answers every read at once with zeros, so the benchmark measures the
// reader, not the device.
type instantClient struct{}

func (instantClient) ReadRegisters(
	_ context.Context, _, quantity uint16, _ modbus.RegType,
) ([]uint16, error) {
	return make([]uint16, quantity), nil
}

func (instantClient) Close() error { return nil }

// instantDialer connects instantClient.
type instantDialer struct{}

func (instantDialer) Dial(context.Context) (Client, error) { return instantClient{}, nil }

// BenchmarkRangePoll reads and decodes one range-mode poll like an inverter: two
// ranges of 250 and 100 registers (four chunks), 80 points of mixed types.
func BenchmarkRangePoll(b *testing.B) {
	var points []Point
	for i := range 60 {
		points = append(points, Point{
			Name: "a", Register: uint16(i * 4), Count: 2,
			Type: "float32", Scale: 1,
		})
	}
	for i := range 20 {
		points = append(points, Point{
			Name: "b", Register: uint16(1000 + i*5), Type: "int16",
			Scale: 0.1,
		})
	}
	r, err := New(Deps{
		Settings: Settings{
			Name: "inverter", PollInterval: time.Second, RangeMode: true,
			Ranges: []string{"0-249", "1000-1099"}, Points: points,
		},
		Dialer: instantDialer{}, Clock: clock.Real{}, Log: zerolog.Nop(),
	})
	if err != nil {
		b.Fatal(err)
	}
	r.client = instantClient{}
	ctx := context.Background()

	for b.Loop() {
		if got, err := r.readRangeMode(ctx); err != nil || len(got) != len(points) {
			b.Fatal(err, len(got))
		}
	}
}
