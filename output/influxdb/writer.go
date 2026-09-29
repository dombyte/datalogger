// Package influxdb provides InfluxDB3 output functionality.
package influxdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
	"github.com/rs/zerolog"
)

// Default batch configuration
const (
	DefaultBatchSize    = 10000
	DefaultBatchTimeout = 1 * time.Second
	DefaultMaxRetries   = 3
	DefaultRetryDelay   = 1 * time.Second
)

// InfluxDBWriter writes DataPoints to an InfluxDB3 database.
type InfluxDBWriter struct {
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

// NewInfluxDBWriter creates a new InfluxDBWriter.
func NewInfluxDBWriter(outputConfig config.Output, logger *zerolog.Logger) (*InfluxDBWriter, error) {
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

	w := &InfluxDBWriter{
		config:       outputConfig,
		logger:       logger.With().Str("output", "influxdb").Str("name", outputConfig.Name).Logger(),
		devices:      outputConfig.Devices,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		maxRetries:   maxRetries,
		retryDelay:   retryDelay,
	}

	return w, w.createClient()
}

// createClient creates and configures the InfluxDB3 client.
func (w *InfluxDBWriter) createClient() error {
	influxConfig := w.config.OutputSpecific.Influxdb

	// Create HTTP client for insecure connections
	var httpClient *http.Client
	if influxConfig.Insecure {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
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
			UseV2Api:      false, // Use v3 API endpoint (/api/v3/write_lp)
			GzipThreshold: 1000,  // Enable gzip compression for writes > 1000 bytes (default in library)
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
func (w *InfluxDBWriter) Name() string {
	return w.config.Name
}

// Devices returns the list of device names this output accepts.
func (w *InfluxDBWriter) Devices() []string {
	return w.devices
}

// Validate validates the InfluxDB3 writer configuration.
func (w *InfluxDBWriter) Validate() error {
	// Configuration was already validated when creating the writer
	return nil
}

// Start starts the InfluxDB3 writer goroutine.
func (w *InfluxDBWriter) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
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
func (w *InfluxDBWriter) runWriterLoop(
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
			if !w.handleInputPoint(ctx, dp, ok, input, &batch, batchTimer) {
				return
			}

		case <-batchTimer.C:
			w.handleBatchTimeout(ctx, &batch, batchTimer)
		}
	}
}

// handleShutdown handles shutdown signal by flushing remaining batch and draining input.
func (w *InfluxDBWriter) handleShutdown(
	ctx context.Context,
	input <-chan datasource.DataPoint,
	batch []*influxdb3.Point,
	batchTimer *time.Timer,
) {
	w.logger.Info().Msg("InfluxDB writer: shutdown started, flushing final batch")

	// Create a fresh context with timeout for final writes
	// This ensures writes can complete even after the main context is cancelled
	// Use 10s to match the main shutdown timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
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
func (w *InfluxDBWriter) drainRemainingPoints(
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
				w.logger.Error().Err(err).Int("points", len(batch)).Msg("Failed to write batch during drain")
			}

		default:
			return
		}
	}
}

// handleInputPoint handles a single input data point.
func (w *InfluxDBWriter) handleInputPoint(
	ctx context.Context,
	dp datasource.DataPoint,
	ok bool,
	input <-chan datasource.DataPoint,
	batch *[]*influxdb3.Point,
	batchTimer *time.Timer,
) bool {
	if !ok {
		// Input channel closed, flush remaining batch
		if len(*batch) > 0 {
			// Use a fresh context with timeout for final write
			// Use 10s to match the main shutdown timeout
			writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := w.writeBatchWithRetry(writeCtx, *batch); err != nil {
				w.logger.Error().Err(err).Int("points", len(*batch)).Msg("Failed to write final batch")
			}
			writeCancel()
		}
		return false
	}

	w.logger.Debug().Str("device", dp.DeviceName).Str("point", dp.PointName).Msg("InfluxDB writer: received point")

	point := w.createPoint(dp)
	*batch = append(*batch, point)

	if len(*batch) >= w.batchSize {
		w.logger.Debug().Int("count", len(*batch)).Msg("Batch full, writing to InfluxDB")
		if err := w.writeBatchWithRetry(ctx, *batch); err != nil {
			w.logger.Error().Err(err).Int("points", len(*batch)).Msg("Failed to write batch")
		}
		*batch = (*batch)[:0]
		batchTimer.Reset(w.batchTimeout)
	}

	return true
}

// handleBatchTimeout handles batch timeout by writing the current batch.
func (w *InfluxDBWriter) handleBatchTimeout(
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
func (w *InfluxDBWriter) createPoint(dp datasource.DataPoint) *influxdb3.Point {
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
func (w *InfluxDBWriter) writeBatchWithRetry(ctx context.Context, batch []*influxdb3.Point) error {
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
func (w *InfluxDBWriter) attemptWrite(
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
		return fmt.Errorf("context cancelled")
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
			w.logger.Info().Int("points", len(batch)).Msg("Successfully wrote batch to InfluxDB (recovered from previous error)")
		} else {
			w.logger.Debug().Int("points", len(batch)).Msg("Successfully wrote batch to InfluxDB")
		}
		w.lastWriteErr = nil
		return nil
	}

	w.logger.Error().Err(err).Int("attempt", attempt+1).Int("points", len(batch)).Msg("Batch write failed")
	w.lastWriteErr = err
	return err
}

// waitForRetry waits for the retry delay or context cancellation.
func (w *InfluxDBWriter) waitForRetry(
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
