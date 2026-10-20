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

// maxRetryAfter bounds the wait a Retry-After asks for: while the writer waits, its input
// fills up and drops new polls, so a longer wait would lose more than a retry that
// fails again.
const maxRetryAfter = time.Minute

// ErrMissingDependency is returned by New when a required dependency is missing.
var ErrMissingDependency = errors.New("influxdb: missing dependency")

// Settings configures a Writer; zero values use the defaults.
type Settings struct {
	Name         string
	BatchSize    int // lines (polls) per write
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

// Writer batches polls and writes them with retries. Schema: one line per poll, table =
// device, one field per point, no tags, timestamp = the poll's reading time.
type Writer struct {
	settings Settings
	client   Client
	clock    clock.Clock
	logger   zerolog.Logger
	batch    []*influxdb3.Point // one line per poll
	columns  map[column]*columnType
	failed   bool // the previous batch failed
	partial  bool // the server rejected lines of the previous batch
}

// column is a field column of a device's table: one point.
type column struct {
	device, point string
}

// columnType is the field type a column was first written with in this run.
type columnType struct {
	name     string
	mismatch bool // the last value of the column had another type
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
		columns:  make(map[column]*columnType),
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
func (w *Writer) Start(ctx context.Context, input <-chan []datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer w.closeClient()
		w.loop(ctx, input)
	}()
	return done
}

// loop is the batching loop.
func (w *Writer) loop(ctx context.Context, input <-chan []datasource.DataPoint) {
	ticker := w.clock.NewTicker(w.settings.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(w.batch) > 0 {
				w.logger.Warn().Int("lines", len(w.batch)).Msg("Dropping batch, output cancelled")
			}
			return
		case points, ok := <-input:
			if !ok {
				w.flush(ctx)
				return
			}
			w.addAll(ctx, points)
		case <-ticker.C():
			w.flush(ctx)
		}
	}
}

// addAll appends the poll as one line to the batch and writes the batch when it is full.
func (w *Writer) addAll(ctx context.Context, points []datasource.DataPoint) {
	if row := w.newRow(points); row != nil {
		w.batch = append(w.batch, row)
	}
	if len(w.batch) >= w.settings.BatchSize {
		w.flush(ctx)
	}
}

// newRow maps a poll onto one line: table = device, one field per point, time = the
// poll's timestamp (a reader sends one device's poll, all points stamped alike). It
// returns nil when no point of the poll can be written.
func (w *Writer) newRow(points []datasource.DataPoint) *influxdb3.Point {
	row := influxdb3.NewPointWithMeasurement(points[0].DeviceName).
		SetTimestamp(points[0].Timestamp)
	fields := 0
	for _, dp := range points {
		if w.writable(dp) {
			row.SetField(dp.PointName, dp.Value)
			fields++
		}
	}
	if fields == 0 {
		return nil
	}
	return row
}

// writable reports whether the value of dp is written. A value line protocol cannot
// encode, or one whose type differs from the type its column was first written with,
// is skipped: InfluxDB would reject the line, and with it the whole poll.
func (w *Writer) writable(dp datasource.DataPoint) bool {
	if dp.Value == nil {
		return false // no reading: the column stays empty in this row
	}
	typ, ok := fieldType(dp.Value)
	if !ok {
		w.logger.Warn().Str("device", dp.DeviceName).Str("point", dp.PointName).
			Str("type", fmt.Sprintf("%T", dp.Value)).
			Msg("Skipping point with a value InfluxDB cannot store")
		return false
	}
	return w.checkType(column{device: dp.DeviceName, point: dp.PointName}, typ)
}

// fieldType returns the InfluxDB field type of a value line protocol can encode.
func fieldType(v any) (string, bool) {
	switch v.(type) {
	case float64:
		return "float", true
	case int64:
		return "integer", true
	case uint64:
		return "uinteger", true
	case bool:
		return "boolean", true
	case string:
		return "string", true
	default:
		return "", false
	}
}

// checkType records the first type of a column and reports whether typ matches it. A
// mismatch is warned about once, then logged at debug, and at info when it matches again.
// After a restart the column may already have another type on the server; that line is
// rejected by the server and logged as a partial write.
func (w *Writer) checkType(c column, typ string) bool {
	col, ok := w.columns[c]
	if !ok {
		w.columns[c] = &columnType{name: typ}
		return true
	}
	if typ == col.name {
		if col.mismatch {
			col.mismatch = false
			w.logger.Info().Str("device", c.device).Str("point", c.point).
				Msg("Point has its column type again")
		}
		return true
	}
	level := zerolog.WarnLevel
	if col.mismatch {
		level = zerolog.DebugLevel
	}
	col.mismatch = true
	w.logger.WithLevel(level).Str("device", c.device).Str("point", c.point).
		Str("column_type", col.name).Str("value_type", typ).
		Msg("Skipping value whose type differs from its column")
	return false
}

// flush writes the batch with retries; after the last retry the batch is dropped. A
// partial write counts as written: the server stored every line it did not reject.
func (w *Writer) flush(ctx context.Context) {
	if len(w.batch) == 0 {
		return
	}
	defer func() { w.batch = w.batch[:0] }()

	log := w.logger.With().Int("lines", len(w.batch)).Logger()
	err := w.writeWithRetry(ctx)
	var partialErr *influxdb3.PartialWriteError
	if errors.As(err, &partialErr) {
		w.logRejected(log, partialErr)
		err = nil
	} else if err == nil && w.partial {
		log.Info().Msg("InfluxDB accepts every line again")
		w.partial = false
	}

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

// logRejected logs the lines of a partial write: a warning with the first rejected line,
// then at debug while every batch has rejected lines, so a lasting cause (e.g. a column
// type conflict) does not log once per batch.
func (w *Writer) logRejected(log zerolog.Logger, err *influxdb3.PartialWriteError) {
	level := zerolog.WarnLevel
	if w.partial {
		level = zerolog.DebugLevel
	}
	w.partial = true
	first := err.LineErrors[0] // the client only returns a PartialWriteError with lines
	log.WithLevel(level).Int("rejected", len(err.LineErrors)).Int("line", first.LineNumber).
		Str("error", first.ErrorMessage).Str("original_line", first.OriginalLine).
		Msg("InfluxDB rejected lines of the batch, stored the rest")
}

// writeWithRetry tries the write 1 + MaxRetries times, RetryDelay apart (longer when
// the server asks for it with Retry-After).
func (w *Writer) writeWithRetry(ctx context.Context) error {
	var err error
	for attempt := range w.settings.MaxRetries + 1 {
		if attempt > 0 {
			delay := w.retryDelay(err)
			w.logger.Warn().Int("attempt", attempt+1).Stringer("retry_in", delay).Err(err).
				Msg("Retrying batch write")
			if !clock.Sleep(ctx, w.clock, delay) {
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

// retryDelay returns RetryDelay, or the Retry-After of a 429 or 503 answer (at most
// maxRetryAfter) if that is longer: retrying earlier only adds load to a server that
// is busy. The webhook writer waits the same way.
func (w *Writer) retryDelay(err error) time.Duration {
	var serverErr *influxdb3.ServerError
	if !errors.As(err, &serverErr) {
		return w.settings.RetryDelay
	}
	// Capped before the conversion, which would overflow for huge values.
	seconds := min(max(serverErr.RetryAfter, 0), int(maxRetryAfter/time.Second))
	return max(w.settings.RetryDelay, time.Duration(seconds)*time.Second)
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
