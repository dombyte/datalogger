// Package influxdb provides InfluxDB3 output functionality.
package influxdb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
)

// Default batch configuration
const (
	DefaultBatchSize    = 10000
	DefaultBatchTimeout = 1 * time.Second
	DefaultMaxRetries   = 3
	DefaultRetryDelay   = 1 * time.Second
)

const (
	// shutdownTimeout bounds the final writes after the context is cancelled; it matches
	// the drain time main gives outputs.
	shutdownTimeout = 10 * time.Second

	// gzipThreshold enables gzip for write bodies above this size (library default).
	gzipThreshold = 1000
)

// Writer writes DataPoints to an InfluxDB3 database.
type Writer struct {
	config       config.Output
	logger       zerolog.Logger
	client       *influxdb3.Client
	devices      []string
	batchSize    int
	batchTimeout time.Duration
	maxRetries   int
	retryDelay   time.Duration
	lastWriteErr error // Track if previous write failed
}

// New creates a new Writer.
func New(outputConfig config.Output, logger *zerolog.Logger) (*Writer, error) {
	// Get batch configuration from output config, with defaults
	batchSize := DefaultBatchSize
	batchTimeout := DefaultBatchTimeout
	maxRetries := DefaultMaxRetries
	retryDelay := DefaultRetryDelay

	if outputConfig.BatchSize > 0 {
		batchSize = outputConfig.BatchSize
	}
	if outputConfig.BatchTimeout > 0 {
		batchTimeout = outputConfig.BatchTimeout
	}
	if outputConfig.MaxRetries > 0 {
		maxRetries = outputConfig.MaxRetries
	}
	if outputConfig.RetryDelay > 0 {
		retryDelay = outputConfig.RetryDelay
	}

	w := &Writer{
		config: outputConfig,
		logger: logger.With().
			Str("output", "influxdb").
			Str("name", outputConfig.Name).
			Logger(),
		devices:      outputConfig.Devices,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		maxRetries:   maxRetries,
		retryDelay:   retryDelay,
	}

	return w, w.createClient()
}

// createClient creates and configures the InfluxDB3 client.
func (w *Writer) createClient() error {
	influxConfig := w.config.OutputSpecific.Influxdb

	// Custom HTTP client only for the documented insecure option.
	var httpClient *http.Client
	if influxConfig.Insecure {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: influxConfig.Insecure,
			},
		}
		httpClient = &http.Client{Transport: transport}
	}

	// Create client configuration
	clientConfig := influxdb3.ClientConfig{
		Host:     influxConfig.Address,
		Token:    influxConfig.Token,
		Database: influxConfig.Database,
		WriteOptions: &influxdb3.WriteOptions{
			UseV2Api:      false, // v3 endpoint /api/v3/write_lp
			GzipThreshold: gzipThreshold,
		},
	}

	if httpClient != nil {
		clientConfig.HTTPClient = httpClient
	}

	client, err := influxdb3.New(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create InfluxDB3 client: %w", err)
	}

	w.client = client
	w.logger.Info().
		Str("host", influxConfig.Address).
		Str("database", influxConfig.Database).
		Msg("Connected to InfluxDB")
	return nil
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.config.Name
}

// Devices returns the list of device names this output accepts.
func (w *Writer) Devices() []string {
	return w.devices
}

// Validate validates the InfluxDB3 writer configuration.
func (w *Writer) Validate() error {
	// Configuration was already validated when creating the writer
	return nil
}

// Start starts the InfluxDB3 writer goroutine.
func (w *Writer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		batch := make([]*influxdb3.Point, 0, w.batchSize)
		batchTimer := time.NewTimer(w.batchTimeout)
		defer batchTimer.Stop()

		w.runWriterLoop(ctx, input, batch, batchTimer)
	}()

	return errCh
}

// runWriterLoop is the main loop for the InfluxDB writer.
func (w *Writer) runWriterLoop(
	ctx context.Context,
	input <-chan datasource.DataPoint,
	batch []*influxdb3.Point,
	batchTimer *time.Timer,
) {
	for {
		select {
		case <-ctx.Done():
			w.handleShutdown(ctx, input, batch, batchTimer)
			return

		case dp, ok := <-input:
			if !ok {
				w.flushOnClose(batch)
				return
			}
			w.handleInputPoint(ctx, dp, &batch, batchTimer)

		case <-batchTimer.C:
			w.handleBatchTimeout(ctx, &batch, batchTimer)
		}
	}
}

// handleShutdown handles shutdown signal by flushing remaining batch and draining input.
func (w *Writer) handleShutdown(
	ctx context.Context,
	input <-chan datasource.DataPoint,
	batch []*influxdb3.Point,
	batchTimer *time.Timer,
) {
	w.logger.Info().Msg("InfluxDB writer: shutdown started, flushing final batch")

	// A fresh context, so the final writes can complete after ctx is cancelled.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if len(batch) > 0 {
		w.logger.Info().Int("points", len(batch)).Msg("InfluxDB writer: writing final batch")
		if err := w.writeBatchWithRetry(shutdownCtx, batch); err != nil {
			w.logger.Error().Err(err).Int("points", len(batch)).Msg("Failed to write final batch")
		}
		w.logger.Info().Msg("InfluxDB writer: final batch written")
	}

	w.drainRemainingPoints(shutdownCtx, input, batchTimer)
}

// drainRemainingPoints drains any remaining points from the input channel after shutdown.
func (w *Writer) drainRemainingPoints(
	ctx context.Context,
	input <-chan datasource.DataPoint,
	batchTimer *time.Timer,
) {
	// Reset timer to avoid firing during drain
	if !batchTimer.Stop() {
		<-batchTimer.C
	}

	for {
		select {
		case dp, ok := <-input:
			if !ok {
				return
			}
			point := w.createPoint(dp)
			batch := []*influxdb3.Point{point}
			if err := w.writeBatchWithRetry(ctx, batch); err != nil {
				w.logger.Error().Err(err).Int("points", len(batch)).
					Msg("Failed to write batch during drain")
			}

		default:
			return
		}
	}
}

// flushOnClose writes the remaining batch after the input channel was closed.
func (w *Writer) flushOnClose(batch []*influxdb3.Point) {
	if len(batch) == 0 {
		return
	}
	// A fresh context: the writer context may already be cancelled.
	writeCtx, writeCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer writeCancel()
	if err := w.writeBatchWithRetry(writeCtx, batch); err != nil {
		w.logger.Error().Err(err).Int("points", len(batch)).Msg("Failed to write final batch")
	}
}

// handleInputPoint adds a point to the batch and writes the batch when it is full.
func (w *Writer) handleInputPoint(
	ctx context.Context,
	dp datasource.DataPoint,
	batch *[]*influxdb3.Point,
	batchTimer *time.Timer,
) {
	w.logger.Debug().
		Str("device", dp.DeviceName).
		Str("point", dp.PointName).
		Msg("InfluxDB writer: received point")

	*batch = append(*batch, w.createPoint(dp))

	if len(*batch) >= w.batchSize {
		w.logger.Debug().Int("count", len(*batch)).Msg("Batch full, writing to InfluxDB")
		if err := w.writeBatchWithRetry(ctx, *batch); err != nil {
			w.logger.Error().Err(err).Int("points", len(*batch)).Msg("Failed to write batch")
		}
		*batch = (*batch)[:0]
		batchTimer.Reset(w.batchTimeout)
	}
}

// handleBatchTimeout handles batch timeout by writing the current batch.
func (w *Writer) handleBatchTimeout(
	ctx context.Context,
	batch *[]*influxdb3.Point,
	batchTimer *time.Timer,
) {
	if len(*batch) > 0 {
		w.logger.Debug().Int("count", len(*batch)).Msg("Batch timeout, writing to InfluxDB")
		if err := w.writeBatchWithRetry(ctx, *batch); err != nil {
			w.logger.Error().Err(err).Int("points", len(*batch)).Msg("Failed to write batch")
		}
		*batch = (*batch)[:0]
	}
	batchTimer.Reset(w.batchTimeout)
}

// createPoint creates an InfluxDB point from a DataPoint
// Schema design:
// - measurement: DeviceName (e.g., "spine")
// - tags: point (e.g., "a_voltage"), unit (e.g., "V")
// - field: value (the measured value)
// This creates a homogeneous schema with consistent columns across all rows.
func (w *Writer) createPoint(dp datasource.DataPoint) *influxdb3.Point {
	// Build tags for metadata
	tags := map[string]string{
		"point": dp.PointName,
	}
	if dp.Unit != "" {
		tags["unit"] = dp.Unit
	}

	// Single field for the measured value
	fields := map[string]any{
		"value": dp.Value,
	}

	return influxdb3.NewPoint(
		dp.DeviceName,
		tags,
		fields,
		dp.Timestamp,
	)
}

// writeBatchWithRetry writes a batch of points with retry logic
func (w *Writer) writeBatchWithRetry(ctx context.Context, batch []*influxdb3.Point) error {
	var lastErr error

	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		if err := w.attemptWrite(ctx, batch, attempt, lastErr); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return fmt.Errorf("after %d retries, last error: %w", w.maxRetries, lastErr)
}

// attemptWrite attempts to write a batch with context checking and retry delay.
func (w *Writer) attemptWrite(
	ctx context.Context,
	batch []*influxdb3.Point,
	attempt int,
	lastErr error,
) error {
	// Check context before attempt
	select {
	case <-ctx.Done():
		if lastErr != nil {
			return fmt.Errorf("context cancelled during write retry: %w", lastErr)
		}
		return errors.New("context cancelled")
	default:
	}

	// Wait before retry (except on first attempt)
	if attempt > 0 {
		if err := w.waitForRetry(ctx, attempt, lastErr); err != nil {
			return err
		}
	}

	// Attempt the write
	err := w.client.WritePoints(ctx, batch)
	if err == nil {
		// Check if this is recovery from a previous error
		if w.lastWriteErr != nil {
			w.logger.Info().Int("points", len(batch)).
				Msg("Wrote batch to InfluxDB (recovered from previous error)")
		} else {
			w.logger.Debug().Int("points", len(batch)).Msg("Successfully wrote batch to InfluxDB")
		}
		w.lastWriteErr = nil
		return nil
	}

	w.logger.Error().Err(err).
		Int("attempt", attempt+1).
		Int("points", len(batch)).
		Msg("Batch write failed")
	w.lastWriteErr = err
	return err
}

// waitForRetry waits for the retry delay or context cancellation.
func (w *Writer) waitForRetry(
	ctx context.Context,
	attempt int,
	lastErr error,
) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled during retry delay: %w", lastErr)
	case <-time.After(w.retryDelay):
		w.logger.Warn().Int("attempt", attempt+1).Err(lastErr).Msg("Retrying batch write")
		return nil
	}
}
