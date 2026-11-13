// Datalogger polls Modbus and HTTP devices and forwards every reading to its outputs:
// InfluxDB 3, MQTT, CSV files, a read-only HTTP API and webhooks. This package only
// parses flags, builds the logger and loads the config; the wiring lives in
// internal/app.
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

const (
	// shutdownTimeout is the hard deadline for the whole shutdown; it stays below the
	// 10 s Docker waits before it kills the container.
	shutdownTimeout = 8 * time.Second

	// exitConfig is the exit code of a configuration that can never run (bad flags, a
	// config that does not load or a setting the libraries reject): the systemd unit
	// does not restart on it (RestartPreventExitStatus).
	exitConfig = 2

	// drainTimeout is how long the outputs may drain; then they are cancelled and get
	// the rest of shutdownTimeout to close their files and connections.
	drainTimeout = 7 * time.Second
)

// errConfigRequired is returned when -config is missing.
var errConfigRequired = errors.New("-config flag is required")

// cliFlags holds the parsed command line flags.
type cliFlags struct {
	configPath  string
	debug       bool
	showVersion bool
	check       bool
}

// parseFlags parses args (without the program name); usage and errors go to output.
func parseFlags(name string, args []string, output io.Writer) (cliFlags, error) {
	var f cliFlags
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&f.configPath, "config", "", "Path to configuration file (required)")
	fs.BoolVar(&f.debug, "debug", false, "Enable debug logging")
	fs.BoolVar(&f.showVersion, "version", false, "Show version and exit")
	fs.BoolVar(&f.check, "check", false,
		"Check the configuration (without connecting) and exit: 0 if it is valid, 2 if not")

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
		return f, exitConfig, false
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
// 0 after a clean shutdown, 2 for a configuration that can never run, 1 if startup
// fails otherwise, a component stops on its own, a writer fails or the shutdown
// deadline passes. It returns after SIGINT or SIGTERM.
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
		return exitConfig
	}
	if f.check {
		return check(cfg, logger, stdout)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.New(cfg, logger, clock.Real{})
	if err != nil {
		logger.Error().Err(err).Msg("Startup failed")
		if errors.Is(err, app.ErrInvalidConfig) {
			return exitConfig
		}
		return 1
	}
	runErr := a.Run(ctx)
	if runErr != nil {
		logger.Error().Err(runErr).Msg("Stopping after a component failure")
	}
	return shutdown(a, runErr, logger)
}

// check checks cfg with app.Check and returns the exit code: 0 if it is valid, else 2.
func check(cfg *config.Config, logger zerolog.Logger, stdout io.Writer) int {
	if err := app.Check(cfg, logger); err != nil {
		logger.Error().Err(err).Msg("Invalid configuration")
		return exitConfig
	}
	_, _ = fmt.Fprintln(stdout, "Configuration OK")
	return 0
}

// shutdown runs a.Shutdown with the hard deadline and returns the exit code.
func shutdown(a *app.App, runErr error, logger zerolog.Logger) int {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Shutdown(ctx) }()

	select {
	case err := <-done:
		if err != nil || runErr != nil { // logged by the app
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
