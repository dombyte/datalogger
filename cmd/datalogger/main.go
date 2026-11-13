// Datalogger polls Modbus and HTTP devices and forwards every reading to its outputs:
// InfluxDB 3, MQTT, CSV files, a read-only HTTP API and webhooks. This package only
// parses flags, builds the logger and loads the config; the wiring lives in
// internal/app.
package main

import (
	"context"
	"encoding/json"
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
	"github.com/dombyte/datalogger/internal/status"
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

	// defaultStatusSocket is where the status socket is by default; the systemd unit
	// sets /run/datalogger/datalogger.sock (its /tmp is private).
	defaultStatusSocket = "/tmp/datalogger.sock"

	// statusTimeout is how long -status and -healthcheck wait for the answer: a
	// datalogger that does not answer in time counts as hung.
	statusTimeout = 2 * time.Second

	// drainTimeout is how long the outputs may drain; then they are cancelled and get
	// the rest of shutdownTimeout to close their files and connections.
	drainTimeout = 7 * time.Second
)

var (
	// errConfigRequired is returned when -config is missing.
	errConfigRequired = errors.New("-config flag is required")

	// errLogFormat is returned for a -log-format other than console or json.
	errLogFormat = errors.New("-log-format must be console or json")
)

// Log formats: console for people, json (one object per line) for journald and log
// shippers.
const (
	logConsole = "console"
	logJSON    = "json"
)

// cliFlags holds the parsed command line flags.
type cliFlags struct {
	configPath  string
	debug       bool
	showVersion bool
	check       bool
	logFormat   string

	statusSocket string // "" turns the status socket off
	showStatus   bool
	healthcheck  bool
}

// parseFlags parses args (without the program name); usage and errors go to output.
func parseFlags(name string, args []string, output io.Writer) (cliFlags, error) {
	var f cliFlags
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&f.configPath, "config", "", "Path to configuration file (required)")
	fs.BoolVar(&f.debug, "debug", false, "Enable debug logging")
	fs.BoolVar(&f.showVersion, "version", false, "Show version and exit")
	fs.StringVar(&f.logFormat, "log-format", logConsole, "Log format: console or json")
	fs.StringVar(&f.statusSocket, "status-socket", defaultStatusSocket,
		"Unix socket of the status (empty: off)")
	fs.BoolVar(&f.showStatus, "status", false,
		"Print the status of the running datalogger (from -status-socket) and exit")
	fs.BoolVar(&f.healthcheck, "healthcheck", false,
		"Exit 0 if the running datalogger is healthy, 1 if not (for container healthchecks)")
	fs.BoolVar(&f.check, "check", false,
		"Check the configuration (without connecting) and exit: 0 if it is valid, 2 if not")

	if err := fs.Parse(args); err != nil {
		return f, err
	}
	if err := f.validate(); err != nil {
		if errors.Is(err, errConfigRequired) {
			fs.Usage()
		}
		return f, err
	}
	return f, nil
}

// validate checks that -config is set where it is needed and the log format is known.
func (f cliFlags) validate() error {
	queriesOnly := f.showVersion || f.showStatus || f.healthcheck
	if f.configPath == "" && !queriesOnly {
		return errConfigRequired
	}
	if f.logFormat != logConsole && f.logFormat != logJSON {
		return errLogFormat
	}
	return nil
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
	case f.showStatus:
		return f, printStatus(f.statusSocket, stdout, stderr), false
	case f.healthcheck:
		return f, healthcheck(f.statusSocket, stdout, stderr), false
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

	logger := setupLogger(f.debug, f.logFormat, stderr)
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

	return start(cfg, f.statusSocket, logger)
}

// start runs the app of cfg with its status socket until SIGINT or SIGTERM and returns
// the exit code.
func start(cfg *config.Config, statusSocket string, logger zerolog.Logger) int {
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
	stopStatus, err := serveStatus(statusSocket, a, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Startup failed")
		return 1
	}
	defer stopStatus()
	runErr := a.Run(ctx)
	if runErr != nil {
		logger.Error().Err(runErr).Msg("Stopping after a component failure")
	}
	return shutdown(a, runErr, logger)
}

// serveStatus serves the status of a on the socket at path (nothing if path is empty)
// and returns the function that stops it and removes the socket.
func serveStatus(path string, a *app.App, logger zerolog.Logger) (func(), error) {
	if path == "" {
		return func() {}, nil
	}
	ln, err := status.Listen(path)
	if err != nil {
		return nil, err
	}
	server := status.NewServer(ln, a.Status, logger)
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.Serve()
	}()
	return func() {
		if err := server.Close(); err != nil {
			logger.Debug().Err(err).Msg("Closing status socket")
		}
		<-done
	}, nil
}

// printStatus prints the status of the running datalogger as JSON and returns the exit
// code: 0, or 1 if it does not answer.
func printStatus(path string, stdout, stderr io.Writer) int {
	s, err := status.Query(path, statusTimeout)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s); err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

// healthcheck returns 0 if the running datalogger answers within statusTimeout and
// reports no problem, else 1 with the reason on stderr.
func healthcheck(path string, stdout, stderr io.Writer) int {
	s, err := status.Query(path, statusTimeout)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "unhealthy: %v\n", err)
		return 1
	}
	if problems := s.Problems(); len(problems) > 0 {
		_, _ = fmt.Fprintf(stderr, "unhealthy: %s\n", status.Describe(problems))
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "healthy")
	return 0
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

// setupLogger builds the logger on out: console or JSON lines, debug level with -debug.
func setupLogger(debug bool, format string, out io.Writer) zerolog.Logger {
	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}
	if format == logConsole {
		out = zerolog.ConsoleWriter{Out: out}
	}
	return zerolog.New(out).
		Level(level).
		With().
		Timestamp().
		Logger()
}
