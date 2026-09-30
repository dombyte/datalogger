// Datalogger polls Modbus and HTTP devices and forwards every reading to InfluxDB 3,
// MQTT and CSV outputs. This package only parses flags, builds the logger and loads the
// config; the wiring lives in internal/app.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/app"
	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/config"
)

// Build info, set via -ldflags.
//
//nolint:gochecknoglobals // written by the linker at build time
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
	GoVersion = "unknown"
)

// shutdownTimeout is the hard deadline for the whole shutdown; it stays below the
// 10 s Docker waits before it kills the container.
const shutdownTimeout = 8 * time.Second

// errConfigRequired is returned when -config is missing.
var errConfigRequired = errors.New("-config flag is required")

// cliFlags holds the parsed command line flags.
type cliFlags struct {
	configPath  string
	debug       bool
	showVersion bool
}

// parseFlags parses args (without the program name); usage and errors go to output.
func parseFlags(name string, args []string, output io.Writer) (cliFlags, error) {
	var f cliFlags
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&f.configPath, "config", "", "Path to configuration file (required)")
	fs.BoolVar(&f.debug, "debug", false, "Enable debug logging")
	fs.BoolVar(&f.showVersion, "version", false, "Show version and exit")

	if err := fs.Parse(args); err != nil {
		return f, err
	}
	if f.configPath == "" && !f.showVersion {
		fs.Usage()
		return f, errConfigRequired
	}
	return f, nil
}

// flagsOrExit parses args (with the program name first). ok is false when the process
// should end with code (after -help, -version or a flag error).
func flagsOrExit(args []string, stdout, stderr io.Writer) (f cliFlags, code int, ok bool) {
	f, err := parseFlags(filepath.Base(args[0]), args[1:], stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return f, 0, false
	case err != nil:
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return f, 1, false
	case f.showVersion:
		printVersion(stdout)
		return f, 0, false
	}
	return f, 0, true
}

// printVersion prints the build info to w.
func printVersion(w io.Writer) {
	_, _ = fmt.Fprintf(w, "Version:    %s\n", Version)
	_, _ = fmt.Fprintf(w, "Git Commit: %s\n", Commit)
	_, _ = fmt.Fprintf(w, "Build Date: %s\n", BuildDate)
	_, _ = fmt.Fprintf(w, "Go Version: %s\n", GoVersion)
	_, _ = fmt.Fprintf(w, "OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func main() {
	os.Exit(run(os.Args, os.Stdout, os.Stderr))
}

// run starts the datalogger with args (program name first) and returns the exit code:
// 0 after a clean shutdown, 1 if startup fails, a component stops on its own, a writer
// fails or the shutdown deadline passes. It returns after SIGINT or SIGTERM.
func run(args []string, stdout, stderr io.Writer) int {
	f, code, ok := flagsOrExit(args, stdout, stderr)
	if !ok {
		return code
	}

	logger := setupLogger(f.debug, stderr)
	logger.Info().
		Str("version", Version).
		Str("commit", Commit).
		Str("build_date", BuildDate).
		Str("go_version", GoVersion).
		Msg("Starting datalogger")

	cfg, err := config.Load(f.configPath)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to load config")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.New(cfg, logger, clock.Real{})
	if err != nil {
		logger.Error().Err(err).Msg("Startup failed")
		return 1
	}
	runErr := a.Run(ctx)
	if runErr != nil {
		logger.Error().Err(runErr).Msg("Stopping after a component failure")
	}
	return shutdown(a, runErr, logger)
}

// shutdown runs a.Shutdown with the hard deadline and returns the exit code.
func shutdown(a *app.App, runErr error, logger zerolog.Logger) int {
	done := make(chan error, 1)
	go func() { done <- a.Shutdown() }()

	select {
	case err := <-done:
		if err != nil || runErr != nil {
			return 1
		}
		logger.Info().Msg("Shutdown complete")
		return 0
	case <-time.After(shutdownTimeout):
		logger.Error().Dur("timeout", shutdownTimeout).Msg("Shutdown deadline exceeded")
		return 1
	}
}

// setupLogger builds the console logger on out (debug level with -debug).
func setupLogger(debug bool, out io.Writer) zerolog.Logger {
	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}
	return zerolog.New(zerolog.ConsoleWriter{Out: out}).
		Level(level).
		With().
		Timestamp().
		Logger()
}
