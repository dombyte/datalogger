package modbus

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPollFailuresWarnUntilTheyRepeat(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	r := &Reader{logger: zerolog.New(&logs).Level(zerolog.InfoLevel)}

	for range errorAfterFailures {
		r.handlePollError(errors.New("timeout"))
	}
	r.handlePollSuccess(8)

	var lines []map[string]any
	for line := range strings.Lines(logs.String()) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		lines = append(lines, entry)
	}
	require.Len(t, lines, errorAfterFailures+1)

	tests := []struct {
		level   string
		retryIn string
	}{
		{"warn", "100ms"},
		{"warn", "200ms"},
		{"error", "400ms"},
	}
	for i, tt := range tests {
		assert.Equal(t, tt.level, lines[i]["level"], "failure %d", i+1)
		assert.Equal(t, tt.retryIn, lines[i]["retry_in"], "failure %d", i+1)
		assert.InDelta(t, i+1, lines[i]["failure_count"], 0)
	}
	recovered := lines[errorAfterFailures]
	assert.Equal(t, "info", recovered["level"])
	assert.InDelta(t, errorAfterFailures, recovered["failures"], 0)
	assert.InDelta(t, 8, recovered["points"], 0)
}
