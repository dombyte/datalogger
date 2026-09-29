// Package app is the composition root: it creates readers and writers from the
// config, routes data points between them and runs the two-phase shutdown.
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/config"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output"
	"github.com/dombyte/datalogger/internal/output/csv"
	"github.com/dombyte/datalogger/internal/output/influxdb"
	"github.com/dombyte/datalogger/internal/output/mqtt"
)

const (
	// drainTimeout is how long outputs may drain after the sources stopped.
	drainTimeout = 10 * time.Second

	// defaultBufferSize is the output channel size when buffer_size is not set.
	defaultBufferSize = 1000

	// monitorInterval is how often the component monitor checks for exited components.
	monitorInterval = 1 * time.Second

	// signalBuffer holds a second signal that arrives during the drain phase.
	signalBuffer = 2
)

// Run starts all readers and writers, routes the data and blocks until the two-phase
// shutdown triggered by SIGINT/SIGTERM has finished.
func Run(cfg *config.Config, logger *zerolog.Logger) {
	// Separate contexts for two-phase shutdown: stop sources first, then outputs
	sourceCtx, sourceCancel := context.WithCancel(context.Background())
	outputCtx, outputCancel := context.WithCancel(context.Background())
	defer outputCancel()
	defer sourceCancel()

	sigChan := make(chan os.Signal, signalBuffer)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go handleSignals(sigChan, sourceCancel, outputCancel, logger)

	deviceChannels, deviceDoneChannels := startDeviceReaders(sourceCtx, cfg, logger)
	outputWriters, outputChannels, outputDoneChannels := startOutputWriters(outputCtx, cfg, logger)

	// The monitor ends the process if a component exits outside of a shutdown.
	go monitorComponents(deviceDoneChannels, outputDoneChannels, sourceCtx, outputCtx, logger)

	startRouting(deviceChannels, outputWriters, outputChannels, logger)
	logger.Info().Msg("Datalogger started successfully")

	// Wait for the output context to be cancelled (by the signal handler)
	<-outputCtx.Done()
	waitForOutputs(outputDoneChannels, logger)
}

// waitForOutputs blocks until every output writer has finished its cleanup.
func waitForOutputs(outputDoneChannels map[string]<-chan struct{}, logger *zerolog.Logger) {
	logger.Info().Msg("Waiting for output writers to finish cleanup...")
	for name, doneCh := range outputDoneChannels {
		<-doneCh
		logger.Info().Str("output", name).Msg("Output writer cleanup complete")
	}
}

// handleSignals runs the two-phase shutdown: the first signal stops the sources, then
// the outputs are cancelled after drainTimeout or on a second signal.
func handleSignals(
	sigChan <-chan os.Signal,
	sourceCancel, outputCancel context.CancelFunc,
	logger *zerolog.Logger,
) {
	<-sigChan
	logger.Info().Msg("Shutdown signal received")

	// Phase 1: Stop data sources (no new data will be polled)
	logger.Info().Msg("Stopping data sources...")
	sourceCancel()
	logger.Info().Msg("Data sources stopped")

	// Phase 2: Give outputs up to drainTimeout to drain their buffers
	logger.Info().Dur("timeout", drainTimeout).Msg("Waiting for outputs to drain...")
	select {
	case <-time.After(drainTimeout):
		logger.Info().Msg("Shutdown timeout reached, cancelling outputs")
	case <-sigChan:
		logger.Info().Msg("Second shutdown signal received, forcing immediate exit")
	}
	outputCancel()
	logger.Info().Msg("Outputs cancelled, waiting for cleanup...")
}

// startDeviceReaders starts all device readers and returns their data and done channels.
func startDeviceReaders(
	ctx context.Context,
	cfg *config.Config,
	logger *zerolog.Logger,
) (map[string]<-chan datasource.DataPoint, map[string]<-chan struct{}) {
	deviceReaders := createDeviceReaders(cfg, logger)
	deviceChannels := make(map[string]<-chan datasource.DataPoint)
	deviceDoneChannels := make(map[string]<-chan struct{})

	for _, reader := range deviceReaders {
		dataCh, doneCh, errCh := reader.Start(ctx)
		deviceChannels[reader.Name()] = dataCh
		deviceDoneChannels[reader.Name()] = doneCh
		go monitorDevice(reader.Name(), doneCh, errCh, logger)
	}

	return deviceChannels, deviceDoneChannels
}

// startOutputWriters starts all output writers and returns the writers, their input
// channels and their done channels.
func startOutputWriters(
	ctx context.Context,
	cfg *config.Config,
	logger *zerolog.Logger,
) ([]output.Writer, map[string]chan<- datasource.DataPoint, map[string]<-chan struct{}) {
	outputWriters := createOutputWriters(cfg, logger)
	outputChannels := make(map[string]chan<- datasource.DataPoint)
	outputDoneChannels := make(map[string]<-chan struct{})

	for _, writer := range outputWriters {
		bufferSize := writerBufferSize(writer, cfg)
		ch := make(chan datasource.DataPoint, bufferSize)
		outputChannels[writer.Name()] = ch

		// Create a done channel for this writer
		doneCh := make(chan struct{})
		outputDoneChannels[writer.Name()] = doneCh

		go startWriterGoroutineWithDone(ctx, writer, ch, doneCh, logger)
	}

	return outputWriters, outputChannels, outputDoneChannels
}

// startWriterGoroutineWithDone starts a single output writer goroutine with a done channel.
func startWriterGoroutineWithDone(
	ctx context.Context,
	writer output.Writer,
	ch <-chan datasource.DataPoint,
	doneCh chan<- struct{},
	logger *zerolog.Logger,
) {
	var once sync.Once
	closeDone := func() {
		once.Do(func() { close(doneCh) })
	}

	// Start the writer in a goroutine
	go func() {
		if err := <-writer.Start(ctx, ch); err != nil {
			logger.Error().Err(err).Str("output", writer.Name()).Msg("Output writer failed")
		}
		closeDone()
	}()

	// Also close doneCh if context is cancelled (in case writer.Start doesn't return)
	go func() {
		<-ctx.Done()
		closeDone()
	}()
}

// startRouting sets up routing from device channels to output channels.
func startRouting(
	deviceChannels map[string]<-chan datasource.DataPoint,
	outputWriters []output.Writer,
	outputChannels map[string]chan<- datasource.DataPoint,
	logger *zerolog.Logger,
) {
	for deviceName, deviceCh := range deviceChannels {
		go routeDeviceToOutputs(deviceName, deviceCh, outputWriters, outputChannels, logger)
	}
}

// createDeviceReaders creates device readers based on the configuration.
func createDeviceReaders(cfg *config.Config, logger *zerolog.Logger) []datasource.DeviceReader {
	var readers []datasource.DeviceReader

	for _, deviceConfig := range cfg.Devices {
		reader := createSingleDeviceReader(&deviceConfig, logger)
		if reader != nil {
			readers = append(readers, reader)
		}
	}

	return readers
}

// createSingleDeviceReader creates a single device reader from a config.
func createSingleDeviceReader(
	deviceConfig *config.Device,
	logger *zerolog.Logger,
) datasource.DeviceReader {
	var reader datasource.DeviceReader
	var err error

	switch deviceConfig.Type {
	case "modbus":
		reader, err = createModbusReader(*deviceConfig, *logger, clock.Real{})
	case "http":
		reader, err = createHTTPReader(*deviceConfig, *logger, clock.Real{})
	default:
		logger.Error().
			Str("device", deviceConfig.Name).
			Str("type", deviceConfig.Type).
			Msg("Unknown device type")
		return nil
	}

	if err != nil {
		logger.Error().Err(err).
			Str("device", deviceConfig.Name).
			Msg("Failed to create device reader")
		return nil
	}

	return reader
}

// createOutputWriters creates output writers based on the configuration.
func createOutputWriters(cfg *config.Config, logger *zerolog.Logger) []output.Writer {
	var writers []output.Writer

	for _, outputConfig := range cfg.Outputs {
		writer := createSingleOutputWriter(&outputConfig, logger)
		if writer != nil {
			writers = append(writers, writer)
		}
	}

	return writers
}

// createSingleOutputWriter creates a single output writer from a config.
func createSingleOutputWriter(
	outputConfig *config.Output,
	logger *zerolog.Logger,
) output.Writer {
	writer, err := createOutputWriterByType(outputConfig, logger)
	if err != nil {
		logger.Error().Err(err).
			Str("output", outputConfig.Name).
			Msg("Failed to create output writer")
		return nil
	}

	return writer
}

// createOutputWriterByType creates an output writer based on its type.
func createOutputWriterByType(
	outputConfig *config.Output,
	logger *zerolog.Logger,
) (output.Writer, error) {
	switch outputConfig.Type {
	case "influxdb":
		return influxdb.New(*outputConfig, logger)
	case "mqtt":
		return mqtt.New(*outputConfig, logger)
	case "csv":
		return csv.New(*outputConfig, logger)
	default:
		return nil, fmt.Errorf("unknown output type: %s", outputConfig.Type)
	}
}

// writerBufferSize returns the buffer size for an output writer.
func writerBufferSize(w output.Writer, cfg *config.Config) int {
	// Check if the output has a specific buffer size
	for _, o := range cfg.Outputs {
		if o.Name == w.Name() {
			if o.BufferSize > 0 {
				return o.BufferSize
			}
		}
	}
	return defaultBufferSize
}

// monitorDevice monitors a device reader for completion or errors.
func monitorDevice(
	deviceName string,
	doneCh <-chan struct{},
	errCh <-chan error,
	logger *zerolog.Logger,
) {
	select {
	case <-doneCh:
		logger.Debug().Str("device", deviceName).Msg("Device reader completed")
	case err, ok := <-errCh:
		if ok {
			logger.Error().Err(err).Str("device", deviceName).Msg("Device reader error")
		}
	}
}

// monitorComponents monitors all device and output done channels.
// If a component exits unexpectedly (outside of shutdown), it logs a fatal error.
// During shutdown (when sourceCtx or outputCtx is cancelled), component exits are expected.
func monitorComponents(
	deviceDoneChannels map[string]<-chan struct{},
	outputDoneChannels map[string]<-chan struct{},
	sourceCtx context.Context,
	outputCtx context.Context,
	logger *zerolog.Logger,
) {
	for {
		select {
		case <-sourceCtx.Done():
			// Shutdown in progress, device exits are expected
			return
		case <-outputCtx.Done():
			// Shutdown in progress, output exits are expected
			return
		default:
			exitIfAnyDone(deviceDoneChannels, "device", "Device reader exited unexpectedly", logger)
			exitIfAnyDone(outputDoneChannels, "output", "Output writer exited unexpectedly", logger)
			time.Sleep(monitorInterval)
		}
	}
}

// exitIfAnyDone ends the process if one of the done channels is closed.
func exitIfAnyDone(
	doneChannels map[string]<-chan struct{},
	kind, msg string,
	logger *zerolog.Logger,
) {
	for name, doneCh := range doneChannels {
		select {
		case <-doneCh:
			logger.Fatal().Str(kind, name).Msg(msg)
		default:
		}
	}
}

// routeDeviceToOutputs routes all DataPoints from a device to all relevant output channels.
// This ensures each output receives a copy of every DataPoint, preventing data loss
// when multiple outputs are configured.
func routeDeviceToOutputs(
	deviceName string,
	deviceCh <-chan datasource.DataPoint,
	outputWriters []output.Writer,
	outputChannels map[string]chan<- datasource.DataPoint,
	logger *zerolog.Logger,
) {
	deviceOutputs := buildDeviceOutputMap(deviceName, outputWriters, outputChannels)
	if len(deviceOutputs) == 0 {
		return
	}

	processDataPoints(deviceCh, deviceOutputs, logger)
}

// buildDeviceOutputMap builds a map of output channels that accept data from a specific device.
func buildDeviceOutputMap(
	deviceName string,
	outputWriters []output.Writer,
	outputChannels map[string]chan<- datasource.DataPoint,
) map[string]chan<- datasource.DataPoint {
	deviceOutputs := make(map[string]chan<- datasource.DataPoint)
	for _, writer := range outputWriters {
		for _, acceptedDevice := range writer.Devices() {
			if acceptedDevice == deviceName {
				deviceOutputs[writer.Name()] = outputChannels[writer.Name()]
				break
			}
		}
	}
	return deviceOutputs
}

// processDataPoints processes data points and sends them to the appropriate output channels.
func processDataPoints(
	deviceCh <-chan datasource.DataPoint,
	deviceOutputs map[string]chan<- datasource.DataPoint,
	logger *zerolog.Logger,
) {
	for dp := range deviceCh {
		sendDataPointToOutputs(dp, deviceOutputs, logger)
	}
}

// sendDataPointToOutputs sends a data point to all configured outputs.
func sendDataPointToOutputs(
	dp datasource.DataPoint,
	deviceOutputs map[string]chan<- datasource.DataPoint,
	logger *zerolog.Logger,
) {
	for outputName, outputCh := range deviceOutputs {
		select {
		case outputCh <- dp:
			// Data sent successfully
		default:
			logChannelFullWarning(dp, outputName, logger)
		}
	}
}

// logChannelFullWarning logs a warning when an output channel is full.
func logChannelFullWarning(dp datasource.DataPoint, outputName string, logger *zerolog.Logger) {
	logger.Warn().
		Str("device", dp.DeviceName).
		Str("point", dp.PointName).
		Str("output", outputName).
		Msg("Output channel full, dropping data point")
	if logger.Debug().Enabled() {
		logger.Debug().
			Interface("data", dp).
			Msg("Dropped data point details")
	}
}
