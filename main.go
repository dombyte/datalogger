// Datalogger is a robust, configurable datalogger that reads data from multiple sources
// (Modbus TCP/RTU, HTTP APIs) and writes to multiple outputs (InfluxDB3, MQTT, CSV) with proper
// timestamp preservation, error handling, and concurrent execution.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
	"github.com/dombyte/datalogger/datasource/http"
	"github.com/dombyte/datalogger/datasource/modbus"
	"github.com/dombyte/datalogger/output"
	"github.com/dombyte/datalogger/output/csv"
	"github.com/dombyte/datalogger/output/influxdb"
	"github.com/dombyte/datalogger/output/mqtt"
)

// Build-time variables set via -ldflags
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
	GoVersion = "unknown"

	configPath  string
	debugFlag   bool
	versionFlag bool
)

func parseFlags() {
	// Custom usage to show just the binary name
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	flag.StringVar(&configPath, "config", "", "Path to configuration file (required)")
	flag.BoolVar(&debugFlag, "debug", false, "Enable debug logging")
	flag.BoolVar(&versionFlag, "version", false, "Show version and exit")
	flag.Parse()

	if versionFlag {
		fmt.Printf("Version:    %s\n", Version)
		fmt.Printf("Git Commit: %s\n", Commit)
		fmt.Printf("Build Date: %s\n", BuildDate)
		fmt.Printf("Go Version: %s\n", GoVersion)
		fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	if configPath == "" {
		fmt.Fprintf(os.Stderr, "Error: -config flag is required\n\n")
		flag.Usage()
		fmt.Fprintf(os.Stderr, "\nRun with -config <path> to specify a configuration file\n")
		os.Exit(1)
	}
}

func main() {
	// Parse command line flags
	parseFlags()

	// 1. Setup logging
	logger := setupLogger(debugFlag)

	// 2. Load configuration
	cfg := loadAndValidateConfig(configPath, logger)

	// 3. Create separate contexts for data sources and outputs
	// This allows two-phase shutdown: stop sources first, then outputs
	sourceCtx, sourceCancel := context.WithCancel(context.Background())
	outputCtx, outputCancel := context.WithCancel(context.Background())
	defer outputCancel()
	defer sourceCancel()

	// 4. Setup signal handling for graceful/hard shutdown
	sigChan := make(chan os.Signal, 2) // Buffer of 2 to catch second signal
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Track if we're already in shutdown
	shutdownInProgress := false
	var gracefulShutdownDone chan struct{}

	go func() {
		for range sigChan {
			if !shutdownInProgress {
				// First signal: start graceful shutdown
				shutdownInProgress = true
				logger.Info().Msg("Shutdown signal received")

				// Phase 1: Stop data sources (no new data will be polled)
				logger.Info().Msg("Stopping data sources...")
				sourceCancel()
				logger.Info().Msg("Data sources stopped")

				// Phase 2: Give outputs up to 10 seconds to drain their buffers
				logger.Info().Msg("Waiting for outputs to drain (10s timeout)...")
				gracefulShutdownDone = make(chan struct{})
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer shutdownCancel()

				// Wait for shutdown timeout or second signal
				select {
				case <-shutdownCtx.Done():
					logger.Info().Msg("Shutdown timeout reached, cancelling outputs")
					outputCancel()
					logger.Info().Msg("Outputs cancelled, waiting for cleanup...")
				case <-sigChan:
					// Second signal received - hard shutdown
					logger.Info().Msg("Second shutdown signal received, forcing immediate exit")
					outputCancel()
					logger.Info().Msg("Outputs cancelled, waiting for cleanup...")
				}
				close(gracefulShutdownDone)
			} else {
				// Already in shutdown, second signal: hard shutdown
				logger.Info().Msg("Second shutdown signal received, forcing immediate exit")
				outputCancel()
				if gracefulShutdownDone != nil {
					// Wait for graceful shutdown goroutine to finish
					<-gracefulShutdownDone
				}
				return
			}
		}
	}()

	// 5. Start all device readers (with monitoring)
	deviceChannels, deviceDoneChannels := startDeviceReaders(cfg, sourceCtx, logger)

	// 6. Start all output writers and get their channels (with monitoring)
	outputWriters, outputChannels, outputDoneChannels := startOutputWriters(cfg, outputCtx, logger)

	// 7. Start component health monitor in background
	// This monitors done channels and logs if components exit unexpectedly
	go monitorComponents(deviceDoneChannels, outputDoneChannels, sourceCtx, outputCtx, logger)

	// 8. Route data from devices to outputs
	startRouting(deviceChannels, outputWriters, outputChannels, logger)

	// System is running
	logger.Info().Msg("Datalogger started successfully")

	// 9. Wait for output context to be cancelled (by signal handler)
	<-outputCtx.Done()

	// 10. Wait for all output writers to finish cleanup
	logger.Info().Msg("Waiting for output writers to finish cleanup...")
	for name, doneCh := range outputDoneChannels {
		<-doneCh
		logger.Info().Str("output", name).Msg("Output writer cleanup complete")
	}
	logger.Info().Msg("Shutdown complete")
}

// loadAndValidateConfig loads and validates the configuration file.
func loadAndValidateConfig(configPath string, logger *zerolog.Logger) *config.Config {
	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}

	if err := cfg.Validate(); err != nil {
		logger.Fatal().Err(err).Msg("Invalid configuration")
	}

	return cfg
}

// startDeviceReaders starts all device readers and returns a map of device channels and done channels.
func startDeviceReaders(
	cfg *config.Config,
	ctx context.Context,
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

// startOutputWriters starts all output writers and returns the writers, their channels, and done channels.
func startOutputWriters(
	cfg *config.Config,
	ctx context.Context,
	logger *zerolog.Logger,
) ([]output.OutputWriter, map[string]chan<- datasource.DataPoint, map[string]<-chan struct{}) {
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
	writer output.OutputWriter,
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
	outputWriters []output.OutputWriter,
	outputChannels map[string]chan<- datasource.DataPoint,
	logger *zerolog.Logger,
) {
	for deviceName, deviceCh := range deviceChannels {
		go routeDeviceToOutputs(deviceName, deviceCh, outputWriters, outputChannels, logger)
	}
}

// setupLogger sets up the zerolog logger.
func setupLogger(debug bool) *zerolog.Logger {
	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(level).
		With().
		Timestamp().
		Logger()

	return &logger
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
func createSingleDeviceReader(deviceConfig *config.Device, logger *zerolog.Logger) datasource.DeviceReader {
	var reader datasource.DeviceReader
	var err error

	switch deviceConfig.Type {
	case "modbus":
		reader, err = modbus.NewModbusReader(*deviceConfig, logger)
	case "http":
		reader, err = http.NewHttpReader(*deviceConfig, logger)
	default:
		logger.Error().Str("device", deviceConfig.Name).Str("type", deviceConfig.Type).Msg("Unknown device type")
		return nil
	}

	if err != nil {
		logger.Error().Err(err).Str("device", deviceConfig.Name).Msg("Failed to create device reader")
		return nil
	}

	if err := reader.Validate(); err != nil {
		logger.Error().Err(err).Str("device", deviceConfig.Name).Msg("Device validation failed")
		return nil
	}

	return reader
}

// createOutputWriters creates output writers based on the configuration.
func createOutputWriters(cfg *config.Config, logger *zerolog.Logger) []output.OutputWriter {
	var writers []output.OutputWriter

	for _, outputConfig := range cfg.Outputs {
		writer := createSingleOutputWriter(&outputConfig, logger)
		if writer != nil {
			writers = append(writers, writer)
		}
	}

	return writers
}

// createSingleOutputWriter creates a single output writer from a config.
func createSingleOutputWriter(outputConfig *config.Output, logger *zerolog.Logger) output.OutputWriter {
	writer, err := createOutputWriterByType(outputConfig, logger)
	if err != nil {
		return nil
	}

	if err := writer.Validate(); err != nil {
		logger.Error().Err(err).Str("output", outputConfig.Name).Msg("Output validation failed")
		return nil
	}

	return writer
}

// createOutputWriterByType creates an output writer based on its type.
func createOutputWriterByType(outputConfig *config.Output, logger *zerolog.Logger) (output.OutputWriter, error) {
	switch outputConfig.Type {
	case "influxdb":
		return influxdb.NewInfluxDBWriter(*outputConfig, logger)
	case "mqtt":
		return mqtt.NewMQTTWriter(*outputConfig, logger)
	case "csv":
		return csv.NewCSVWriter(*outputConfig, logger)
	default:
		logger.Error().Str("output", outputConfig.Name).Str("type", outputConfig.Type).Msg("Unknown output type")
		return nil, fmt.Errorf("unknown output type: %s", outputConfig.Type)
	}
}

// writerBufferSize returns the buffer size for an output writer.
func writerBufferSize(w output.OutputWriter, cfg *config.Config) int {
	// Check if the output has a specific buffer size
	for _, o := range cfg.Outputs {
		if o.Name == w.Name() {
			if o.BufferSize > 0 {
				return o.BufferSize
			}
		}
	}
	// Default buffer size
	return 1000
}

// monitorDevice monitors a device reader for completion or errors.
func monitorDevice(deviceName string, doneCh <-chan struct{}, errCh <-chan error, logger *zerolog.Logger) {
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
	// Merge all done channels into a single select
	for {
		select {
		case <-sourceCtx.Done():
			// Shutdown in progress, device exits are expected
			return
		case <-outputCtx.Done():
			// Shutdown in progress, output exits are expected
			return
		default:
			// Check all channels non-blockingly
			for name, doneCh := range deviceDoneChannels {
				select {
				case <-doneCh:
					logger.Fatal().Str("device", name).Msg("Device reader exited unexpectedly")
				default:
				}
			}
			for name, doneCh := range outputDoneChannels {
				select {
				case <-doneCh:
					logger.Fatal().Str("output", name).Msg("Output writer exited unexpectedly")
				default:
				}
			}
			// Sleep briefly to avoid busy waiting
			time.Sleep(1 * time.Second)
		}
	}
}

// routeDeviceToOutputs routes all DataPoints from a device to all relevant output channels.
// This ensures each output receives a copy of every DataPoint, preventing data loss
// when multiple outputs are configured.
func routeDeviceToOutputs(
	deviceName string,
	deviceCh <-chan datasource.DataPoint,
	outputWriters []output.OutputWriter,
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
	outputWriters []output.OutputWriter,
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
