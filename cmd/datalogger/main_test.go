package main

import (
	"flag"
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseFlags tests the parseFlags function
// Note: This test is tricky because parseFlags uses global variables and flag.CommandLine
// TestParseFlags tests the parseFlags function
func TestParseFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		want    cliFlags
		wantErr error
	}{
		{
			name: "config and debug",
			args: []string{"-config", "test.yaml", "-debug"},
			want: cliFlags{configPath: "test.yaml", debug: true},
		},
		{
			name: "version without config",
			args: []string{"-version"},
			want: cliFlags{showVersion: true},
		},
		{
			name:    "missing config",
			args:    []string{"-debug"},
			wantErr: errConfigRequired,
		},
		{
			name:    "help",
			args:    []string{"-help"},
			wantErr: flag.ErrHelp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseFlags("datalogger", tt.args, io.Discard)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestSetupLogger tests the setupLogger function
func TestSetupLogger(t *testing.T) {
	tests := []struct {
		name     string
		debug    bool
		logLevel zerolog.Level
	}{
		{
			name:     "debug enabled",
			debug:    true,
			logLevel: zerolog.DebugLevel,
		},
		{
			name:     "debug disabled",
			debug:    false,
			logLevel: zerolog.InfoLevel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := setupLogger(tt.debug, io.Discard)

			// Check log level
			if logger.GetLevel() != tt.logLevel {
				t.Errorf("Log level = %v, want %v", logger.GetLevel(), tt.logLevel)
			}
		})
	}
}
