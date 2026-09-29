package csv_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output/csv"
)

const header = "timestamp,device,point,value,unit\n"

var start = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

func point(value any) datasource.DataPoint {
	return datasource.DataPoint{
		DeviceName: "meter", PointName: "power", Value: value, Timestamp: start, Unit: "W",
	}
}

// run starts a writer, sends the points, closes the input and waits until it finished.
func run(t *testing.T, s csv.Settings, clk *clocktest.Fake, points ...datasource.DataPoint) {
	t.Helper()
	w, err := csv.New(csv.Deps{Settings: s, Clock: clk, Log: zerolog.Nop()})
	require.NoError(t, err)

	in := make(chan datasource.DataPoint)
	done := w.Start(context.Background(), in)
	for _, dp := range points {
		in <- dp
	}
	close(in)
	for err := range done {
		require.NoError(t, err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func TestWritesHeaderAndRowsAndCreatesDirectory(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "sub", "data.csv")
	run(t, csv.Settings{Name: "file", FilePath: path}, clocktest.NewFake(start),
		point(1.5), point(true))

	assert.Equal(t, header+
		"2026-09-29T12:00:00Z,meter,power,1.5,W\n"+
		"2026-09-29T12:00:00Z,meter,power,true,W\n", read(t, path))
}

func TestAppendsToExistingFileWithoutSecondHeader(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "data.csv")
	existing := header + "2026-01-01T00:00:00Z,meter,power,1,W\n"
	require.NoError(t, os.WriteFile(path, []byte(existing), 0o644))

	run(t, csv.Settings{Name: "file", FilePath: path}, clocktest.NewFake(start), point(2.0))

	assert.Equal(t, existing+"2026-09-29T12:00:00Z,meter,power,2,W\n", read(t, path))
}

func TestNewFailsForUnusablePath(t *testing.T) {
	t.Parallel()
	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o644))

	_, err := csv.New(csv.Deps{
		Settings: csv.Settings{FilePath: filepath.Join(blocker, "data.csv")},
		Clock:    clocktest.NewFake(start),
	})
	assert.Error(t, err)

	_, err = csv.New(csv.Deps{Settings: csv.Settings{FilePath: filepath.Join(t.TempDir(), "x")}})
	assert.ErrorIs(t, err, csv.ErrMissingDependency)
}
