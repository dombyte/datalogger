package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockedBuffer is a bytes.Buffer that the logger may write to from several goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startupConfig is a config with one HTTP device at deviceURL and one output; output is
// the YAML of the output's output_specific block.
const startupConfig = `devices:
  - name: meter
    type: http
    poll_interval: 20ms
    parallelism: 1
    device_specific:
      http:
        address: %q
    points:
      - name: power
        json_path: "power"
        unit: "W"
        expr: "-value"
outputs:
  - name: out
    type: %s
    output_specific:
%s
    devices: [meter]
`

// writeConfig writes a config file into a temporary directory and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// csvConfig returns a config that polls deviceURL and writes to the CSV file csvPath.
func csvConfig(deviceURL, csvPath string) string {
	output := fmt.Sprintf("      csv:\n        file_path: %q", csvPath)
	return fmt.Sprintf(startupConfig, deviceURL, "csv", output)
}

// readRows returns the rows of the CSV file at path, or nil if it cannot be read yet.
func readRows(path string) [][]string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil
	}
	return rows
}

// TestRunCleanShutdown starts the datalogger with an HTTP device (whose point negates
// its value with an expression) and a CSV output, waits for rows, sends SIGINT and
// expects exit code 0. It sends a real signal to the test process, so it must not run
// in parallel.
func TestRunCleanShutdown(t *testing.T) {
	device := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"power": 42.5}`)
	}))
	defer device.Close()

	csvPath := filepath.Join(t.TempDir(), "data", "data.csv")
	configPath := writeConfig(t, csvConfig(device.URL, csvPath))

	var stderr lockedBuffer
	exit := make(chan int, 1)
	go func() {
		exit <- run([]string{"datalogger", "-config", configPath}, io.Discard, &stderr)
	}()

	// Rows are only written after the signal handler is installed, so SIGINT is safe now.
	require.Eventually(t, func() bool { return len(readRows(csvPath)) >= 3 },
		5*time.Second, 10*time.Millisecond, "no rows written; log:\n%s", stderr.String())
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))

	select {
	case code := <-exit:
		assert.Equal(t, 0, code, "log:\n%s", stderr.String())
	case <-time.After(shutdownTimeout + time.Second):
		t.Fatal("run did not return after SIGINT")
	}

	rows := readRows(csvPath)
	require.GreaterOrEqual(t, len(rows), 3)
	assert.Equal(t, []string{"timestamp", "device", "point", "value", "unit"}, rows[0])
	for _, row := range rows[1:] {
		_, err := time.Parse(time.RFC3339Nano, row[0])
		require.NoError(t, err)
		assert.Equal(t, []string{"meter", "power", "-42.5", "W"}, row[1:], "expr applied")
	}
	assert.Contains(t, stderr.String(), "Shutdown complete")
}

// TestRunStartupErrors checks the exit codes of runs that end before the app starts.
func TestRunStartupErrors(t *testing.T) {
	t.Parallel()

	mqttOutput := "      mqtt:\n        address: \"http://broker.invalid:1883\""
	tests := []struct {
		name    string
		config  string // written to a file and passed as -config; empty: no -config
		args    []string
		want    int
		wantLog string
	}{
		{
			name:    "missing config flag",
			want:    1,
			wantLog: "-config flag is required",
		},
		{
			name: "version",
			args: []string{"-version"},
			want: 0,
		},
		{
			name:    "config file missing",
			args:    []string{"-config", filepath.Join(t.TempDir(), "missing.yaml")},
			want:    1,
			wantLog: "Failed to load config",
		},
		{
			name:    "invalid config",
			config:  fmt.Sprintf(startupConfig, "http://192.0.2.1", "csv", "      csv: {}"),
			want:    1,
			wantLog: "Failed to load config",
		},
		{
			name:    "setting rejected by the client library",
			config:  fmt.Sprintf(startupConfig, "http://192.0.2.1", "mqtt", mqttOutput),
			want:    1,
			wantLog: "Startup failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := append([]string{"datalogger"}, tt.args...)
			if tt.config != "" {
				args = append(args, "-config", writeConfig(t, tt.config))
			}
			var stderr lockedBuffer
			code := run(args, io.Discard, &stderr)
			assert.Equal(t, tt.want, code)
			assert.Contains(t, stderr.String(), tt.wantLog)
		})
	}
}
