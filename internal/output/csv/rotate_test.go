package csv

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
)

var start = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

// writeEvery writes n points, moving the clock by step before each.
func writeEvery(t *testing.T, s Settings, n int, step time.Duration) {
	t.Helper()
	clk := clocktest.NewFake(start)
	w, err := New(Deps{Settings: s, Clock: clk, Log: zerolog.Nop()})
	require.NoError(t, err)
	for i := range n {
		clk.Advance(step)
		require.NoError(t, w.write(datasource.DataPoint{
			DeviceName: "meter", PointName: "power", Value: float64(i), Timestamp: start,
		}))
	}
	w.close()
}

func TestRotatesByAgeAndKeepsMaxBackups(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	unrelated := filepath.Join(dir, "data.csv.keep")
	require.NoError(t, os.WriteFile(unrelated, []byte("mine"), 0o644))

	// Every write is an hour after the file was opened, so every write rotates.
	writeEvery(t, Settings{FilePath: path, MaxAge: time.Hour, MaxBackups: 2}, 4, time.Hour)

	assert.Equal(t, []string{
		"data.csv",
		"data.csv.20260929-150000",
		"data.csv.20260929-160000",
		"data.csv.keep",
	}, files(t, dir))

	backup, err := os.ReadFile(filepath.Join(dir, "data.csv.20260929-160000"))
	require.NoError(t, err)
	assert.Equal(t, "timestamp,device,point,value,unit\n"+
		"2026-09-29T12:00:00Z,meter,power,3,\n", string(backup))
	current, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "timestamp,device,point,value,unit\n", string(current))

	mine, err := os.ReadFile(unrelated)
	require.NoError(t, err)
	assert.Equal(t, "mine", string(mine))
}

func TestNoRotationBeforeMaxAge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeEvery(t, Settings{FilePath: filepath.Join(dir, "data.csv"), MaxAge: time.Hour},
		3, 10*time.Minute)
	assert.Equal(t, []string{"data.csv"}, files(t, dir))
}

func TestMaxBackupsZeroAndNegative(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		maxBackups int
		wantFiles  int
	}{{maxBackups: 0, wantFiles: 1}, {maxBackups: -1, wantFiles: 4}} {
		dir := t.TempDir()
		writeEvery(t, Settings{
			FilePath: filepath.Join(dir, "data.csv"), MaxAge: time.Minute, MaxBackups: tt.maxBackups,
		}, 3, time.Minute)
		assert.Len(t, files(t, dir), tt.wantFiles, "max_backups %d", tt.maxBackups)
	}
}
