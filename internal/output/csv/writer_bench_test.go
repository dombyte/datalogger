package csv_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output/csv"
)

// BenchmarkWritePolls writes polls of 20 points through a buffered input, as the
// router hands them over.
func BenchmarkWritePolls(b *testing.B) {
	w, err := csv.New(csv.Deps{
		Settings: csv.Settings{Name: "file", FilePath: filepath.Join(b.TempDir(), "data.csv")},
		Clock:    clocktest.NewFake(start),
		Log:      zerolog.Nop(),
	})
	require.NoError(b, err)
	poll := make([]datasource.DataPoint, 20)
	for i := range poll {
		poll[i] = datasource.DataPoint{
			DeviceName: "meter", PointName: "p" + strconv.Itoa(i), Value: 230.5 + float64(i),
			Timestamp: start, Unit: "V",
		}
	}

	in := make(chan []datasource.DataPoint, 1000)
	done := w.Start(context.Background(), in)
	for b.Loop() {
		in <- poll
	}
	close(in)
	for err := range done {
		require.NoError(b, err)
	}
}
