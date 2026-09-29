// Datalogger polls Modbus and HTTP devices and forwards every reading to InfluxDB 3,
// MQTT and CSV outputs. This package only parses flags, builds the logger and loads the
// config; the wiring lives in internal/app.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/app"
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

// mustParseFlags parses os.Args; -version, -help and flag errors end the process.
func mustParseFlags() cliFlags {
	f, err := parseFlags(filepath.Base(os.Args[0]), os.Args[1:], os.Stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		os.Exit(0)
	case err != nil:
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	case f.showVersion:
		printVersion()
		os.Exit(0)
	}
	return f
}

// printVersion prints the build info.
func printVersion() {
	fmt.Printf("Version:    %s\n", Version)
	fmt.Printf("Git Commit: %s\n", Commit)
	fmt.Printf("Build Date: %s\n", BuildDate)
	fmt.Printf("Go Version: %s\n", GoVersion)
	fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func main() {
	f := mustParseFlags()
	logger := setupLogger(f.debug)
	logger.Info().
		Str("version", Version).
		Str("commit", Commit).
		Str("build_date", BuildDate).
		Str("go_version", GoVersion).
		Msg("Starting datalogger")

	cfg := loadConfig(f.configPath, logger)
	app.Run(cfg, logger)
	logger.Info().Msg("Shutdown complete")
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

// loadConfig loads, completes and validates the configuration file.
func loadConfig(configPath string, logger *zerolog.Logger) *config.Config {
	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}
	return cfg
}
