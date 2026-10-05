// Package influxdb writes data points to InfluxDB 3 in batches.
package influxdb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// Defaults for settings that are 0.
const (
	DefaultBatchSize    = 10000
	DefaultBatchTimeout = 1 * time.Second
	DefaultMaxRetries   = 3
	DefaultRetryDelay   = 1 * time.Second
)

// ErrMissingDependency is returned by New when a required dependency is missing.
var ErrMissingDependency = errors.New("influxdb: missing dependency")

// Settings configures a Writer; zero values use the defaults.
type Settings struct {
	Name         string
	BatchSize    int
	BatchTimeout time.Duration
	MaxRetries   int
	RetryDelay   time.Duration
}

// Deps are the dependencies of a Writer; all are required.
type Deps struct {
	Settings Settings
	Client   Client
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Writer batches points and writes them with retries. Schema: measurement = device,
// tags point and unit (unit only when set), field value, timestamp = reading time.
type Writer struct {
	settings Settings
	client   Client
	clock    clock.Clock
	logger   zerolog.Logger
	batch    []*influxdb3.Point
	failed   bool // the previous batch failed
}

// New creates a Writer; it does not connect.
func New(d Deps) (*Writer, error) {
	var missing []string
	if d.Client == nil {
		missing = append(missing, "Client")
	}
	if d.Clock == nil {
		missing = append(missing, "Clock")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingDependency, strings.Join(missing, ", "))
	}

	s := d.Settings
	s.BatchSize = orDefault(s.BatchSize, DefaultBatchSize)
	s.BatchTimeout = orDefault(s.BatchTimeout, DefaultBatchTimeout)
	s.MaxRetries = orDefault(s.MaxRetries, DefaultMaxRetries)
	s.RetryDelay = orDefault(s.RetryDelay, DefaultRetryDelay)

	return &Writer{
		settings: s,
		client:   d.Client,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "influxdb").Str("output", s.Name).Logger(),
		batch:    make([]*influxdb3.Point, 0, s.BatchSize),
	}, nil
}

// orDefault returns def for values <= 0.
func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.settings.Name
}

// Start batches points until input is closed, writes the last batch, closes the client
// and closes the returned channel. A full batch is written at once, a partial one every
// BatchTimeout.
func (w *Writer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer w.closeClient()
		w.loop(ctx, input)
	}()
	return done
}

// loop is the batching loop.
func (w *Writer) loop(ctx context.Context, input <-chan datasource.DataPoint) {
	ticker := w.clock.NewTicker(w.settings.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case dp, ok := <-input:
			if !ok {
				w.flush(ctx)
				return
			}
			w.add(dp)
			if len(w.batch) >= w.settings.BatchSize {
				w.flush(ctx)
			}
		case <-ticker.C():
			w.flush(ctx)
		}
	}
}

// add appends dp to the batch. A value line protocol cannot encode is skipped: the
// client would fail the whole batch, which holds the points of every device.
func (w *Writer) add(dp datasource.DataPoint) {
	switch dp.Value.(type) {
	case float64, int64, uint64, bool, string, nil:
		w.batch = append(w.batch, newPoint(dp))
	default:
		w.logger.Warn().Str("device", dp.DeviceName).Str("point", dp.PointName).
			Str("type", fmt.Sprintf("%T", dp.Value)).
			Msg("Skipping point with a value InfluxDB cannot store")
	}
}

// newPoint maps a data point onto the InfluxDB schema.
func newPoint(dp datasource.DataPoint) *influxdb3.Point {
	tags := map[string]string{"point": dp.PointName}
	if dp.Unit != "" {
		tags["unit"] = dp.Unit
	}
	return influxdb3.NewPoint(dp.DeviceName, tags, map[string]any{"value": dp.Value}, dp.Timestamp)
}

// flush writes the batch with retries; after the last retry the batch is dropped.
func (w *Writer) flush(ctx context.Context) {
	if len(w.batch) == 0 {
		return
	}
	defer func() { w.batch = w.batch[:0] }()

	log := w.logger.With().Int("points", len(w.batch)).Logger()
	err := w.writeWithRetry(ctx)
	switch {
	case err != nil:
		log.Error().Err(err).Msg("Dropping batch after failed writes")
		w.failed = true
	case w.failed:
		log.Info().Msg("Wrote batch to InfluxDB (recovered from previous error)")
		w.failed = false
	default:
		log.Debug().Msg("Wrote batch to InfluxDB")
	}
}

// writeWithRetry tries the write 1 + MaxRetries times, RetryDelay apart.
func (w *Writer) writeWithRetry(ctx context.Context) error {
	var err error
	for attempt := range w.settings.MaxRetries + 1 {
		if attempt > 0 {
			w.logger.Warn().Int("attempt", attempt+1).Err(err).Msg("Retrying batch write")
			if !clock.Sleep(ctx, w.clock, w.settings.RetryDelay) {
				return fmt.Errorf("cancelled while retrying: %w", err)
			}
		}
		if err = w.client.WritePoints(ctx, w.batch); err == nil {
			return nil
		}
		if !retryable(err) {
			return fmt.Errorf("not retried: %w", err)
		}
	}
	return fmt.Errorf("after %d retries: %w", w.settings.MaxRetries, err)
}

// retryable reports whether a write may succeed when repeated: network errors, server
// errors (5xx) and 429. Other HTTP errors (bad token, unknown database, rejected lines
// of a partial write) fail the same way again.
func retryable(err error) bool {
	var serverErr *influxdb3.ServerError
	if !errors.As(err, &serverErr) {
		return true
	}
	return serverErr.StatusCode >= http.StatusInternalServerError ||
		serverErr.StatusCode == http.StatusTooManyRequests
}

// closeClient releases the client.
func (w *Writer) closeClient() {
	if err := w.client.Close(); err != nil {
		w.logger.Warn().Err(err).Msg("Closing InfluxDB client failed")
	}
}
