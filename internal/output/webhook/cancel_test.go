package webhook

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
)

// noClient is never called: the test only builds a batch.
type noClient struct{ Client }

// Regression: a batch still open when the writer was cancelled at the drain deadline
// was dropped without a log line.
func TestCancelledBatchIsLogged(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	w, err := New(Deps{
		Settings: Settings{URL: "http://hook"}, Client: noClient{},
		Clock: clocktest.NewFake(time.Time{}), Log: zerolog.New(&logs),
	})
	require.NoError(t, err)

	w.dropOpenBatch()
	assert.Empty(t, logs.String(), "nothing open, nothing dropped")
	w.add(datasource.DataPoint{DeviceName: "meter", PointName: "a", Value: 1.0})
	w.add(datasource.DataPoint{DeviceName: "meter", PointName: "b", Value: 2.0})
	w.dropOpenBatch()
	assert.Contains(t, logs.String(), `"points":2`)
	assert.Contains(t, logs.String(), "Dropping batch, output cancelled")
}
