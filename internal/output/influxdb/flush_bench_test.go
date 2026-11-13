package influxdb

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// flakyClient fails the first two writes of every batch with a 503.
type flakyClient struct{ calls int }

func (c *flakyClient) Write(context.Context, []byte, ...influxdb3.WriteOption) error {
	c.calls++
	if c.calls%3 != 0 {
		return &influxdb3.ServerError{StatusCode: 503}
	}
	return nil
}

func (*flakyClient) Close() error { return nil }

// BenchmarkFlushWithRetries batches 100 polls of 20 points and writes them, the write
// failing twice before it is accepted.
func BenchmarkFlushWithRetries(b *testing.B) {
	w, err := New(Deps{
		Settings: Settings{BatchSize: 1000, MaxRetries: 3, RetryDelay: time.Nanosecond},
		Client:   &flakyClient{}, Clock: clock.Real{}, Log: zerolog.Nop(),
	})
	if err != nil {
		b.Fatal(err)
	}
	polls := make([][]datasource.DataPoint, 100)
	for i := range polls {
		ts := time.Unix(int64(i), 0)
		for p := range 20 {
			polls[i] = append(polls[i], datasource.DataPoint{
				DeviceName: "inverter", PointName: "p" + strconv.Itoa(p),
				Value: float64(p) * 1.5, Timestamp: ts,
			})
		}
	}
	ctx := context.Background()

	for b.Loop() {
		for _, poll := range polls {
			w.addAll(ctx, poll)
		}
		w.flush(ctx)
	}
}
