package http

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestMissingPointWarnsOnceUntilItIsBack(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	r := &Reader{
		settings: Settings{
			Name:         "spine",
			ResponseType: "json",
			Points:       []Point{{Name: "freq", JSONPath: "freq", Scale: 1}},
		},
		logger:       zerolog.New(&logs).Level(zerolog.InfoLevel),
		failedPoints: make(map[string]bool),
	}
	levels := func() []string {
		var out []string
		for line := range strings.Lines(logs.String()) {
			for _, level := range []string{"warn", "info"} {
				if strings.Contains(line, `"level":"`+level+`"`) {
					out = append(out, level)
				}
			}
		}
		return out
	}

	for range 3 {
		assert.Empty(t, r.parsePoints([]byte(`{"a_freq": 50}`), time.Time{}))
	}
	assert.Equal(t, []string{"warn"}, levels(), "one warning for three failed polls")

	assert.Len(t, r.parsePoints([]byte(`{"freq": 50}`), time.Time{}), 1)
	r.parsePoints([]byte(`{}`), time.Time{})
	assert.Equal(t, []string{"warn", "info", "warn"}, levels(),
		"back at info, then a new warning when it fails again")
}
