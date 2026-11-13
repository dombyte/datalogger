package modbus

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"
	"github.com/stretchr/testify/assert"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
)

// stubClient answers every read with err, or with one register if err is nil.
type stubClient struct{ err error }

func (c *stubClient) Read(context.Context, Table, uint16, uint16) ([]uint16, error) {
	if c.err != nil {
		return nil, c.err
	}
	return []uint16{1}, nil
}

func (*stubClient) Close() error { return nil }

// Regression: a point that kept failing in direct mode logged a warning per poll.
func TestFailingPointWarnsOnceUntilItIsBack(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	client := &stubClient{err: modbus.ErrIllegalDataAddress}
	r := &Reader{
		settings: Settings{Name: "meter", Points: []Point{
			{Name: "p", Register: 1, Type: "uint16", Scale: 1},
		}},
		clock:        clocktest.NewFake(time.Time{}),
		logger:       zerolog.New(&logs).Level(zerolog.InfoLevel),
		client:       client,
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
		_, err := r.readDirectMode(context.Background())
		assert.True(t, errors.Is(err, modbus.ErrIllegalDataAddress))
	}
	assert.Equal(t, []string{"warn"}, levels(), "one warning for three failed polls")

	client.err = nil
	points, err := r.readDirectMode(context.Background())
	assert.NoError(t, err)
	assert.Len(t, points, 1)
	client.err = modbus.ErrIllegalDataAddress
	_, _ = r.readDirectMode(context.Background())
	assert.Equal(t, []string{"warn", "info", "warn"}, levels(),
		"back at info, then a new warning when it fails again")
}
