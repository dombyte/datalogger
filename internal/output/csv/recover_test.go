package csv

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
)

func row(value float64) datasource.DataPoint {
	return datasource.DataPoint{DeviceName: "meter", PointName: "power", Value: value, Timestamp: start}
}

// levels returns the log levels written to logs, in order.
func levels(logs *bytes.Buffer) []string {
	var out []string
	for line := range strings.Lines(logs.String()) {
		for _, level := range []string{"error", "warn", "info"} {
			if strings.Contains(line, `"level":"`+level+`"`) {
				out = append(out, level)
			}
		}
	}
	return out
}

func TestRecoversAfterFailedWrite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "data.csv")
	var logs bytes.Buffer
	w, err := New(Deps{
		Settings: Settings{Name: "file", FilePath: path},
		Clock:    clocktest.NewFake(start),
		Log:      zerolog.New(&logs).Level(zerolog.InfoLevel),
	})
	require.NoError(t, err)

	require.NoError(t, w.file.Close()) // the next write fails
	w.handlePoint(row(1))
	w.handlePoint(row(2))
	w.close()

	assert.Equal(t, header+"2026-09-29T12:00:00Z,meter,power,2,\n", read(t, path),
		"the row after the failure is written to the reopened file")
	assert.Equal(t, []string{"error", "info"}, levels(&logs))
}

func TestRecoversAfterFailedRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	clk := clocktest.NewFake(start)
	w, err := New(Deps{
		Settings: Settings{FilePath: path, MaxAge: time.Hour, MaxBackups: -1},
		Clock:    clk,
		Log:      zerolog.Nop(),
	})
	require.NoError(t, err)

	require.NoError(t, os.Remove(path)) // the rename of the rotation fails
	clk.Advance(time.Hour)
	require.ErrorContains(t, w.write(row(1)), "csv: rotate")
	w.reopen() // what handlePoint does after the error

	w.handlePoint(row(2))
	w.close()

	assert.Equal(t, header+"2026-09-29T12:00:00Z,meter,power,2,\n", read(t, path))
}

func TestOldBackupThatCannotBeDeletedIsSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A non-empty directory with a backup name cannot be removed with os.Remove.
	stuck := filepath.Join(dir, "data.csv.20200101-000000")
	require.NoError(t, os.MkdirAll(filepath.Join(stuck, "x"), 0o755))

	writeEvery(t, Settings{FilePath: filepath.Join(dir, "data.csv"), MaxAge: time.Minute},
		1, time.Minute)

	assert.Equal(t, []string{"data.csv", "data.csv.20200101-000000"}, files(t, dir),
		"the new backup is deleted (max_backups 0), the stuck one is kept")
}

// read returns the content of path.
func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

const header = "timestamp,device,point,value,unit\n"

func TestReopenFailureIsLoggedAndRetried(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "data.csv")
	var logs bytes.Buffer
	w, err := New(Deps{
		Settings: Settings{FilePath: path},
		Clock:    clocktest.NewFake(start),
		Log:      zerolog.New(&logs).Level(zerolog.InfoLevel),
	})
	require.NoError(t, err)

	// A directory in place of the file: writes and reopening fail.
	require.NoError(t, w.file.Close())
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o755))
	w.handlePoint(row(1))
	assert.Equal(t, []string{"error", "error"}, levels(&logs), "write and reopen failed")

	require.NoError(t, os.Remove(path))
	w.handlePoint(row(2)) // fails on the old file, reopens
	w.handlePoint(row(3))
	assert.Equal(t, header+"2026-09-29T12:00:00Z,meter,power,3,\n", read(t, path))
}

func TestCloseErrorIsLogged(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	w, err := New(Deps{
		Settings: Settings{FilePath: filepath.Join(t.TempDir(), "data.csv")},
		Clock:    clocktest.NewFake(start),
		Log:      zerolog.New(&logs),
	})
	require.NoError(t, err)

	w.close()
	w.close() // the file is already closed

	assert.Equal(t, []string{"error"}, levels(&logs))
}
