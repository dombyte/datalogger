package transform

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/datasource"
)

var ts = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// solis is a device with a status code, a fault register, a battery current direction
// and a battery power.
func solis(exprs map[string]string) Settings {
	return Settings{
		Device: "solis",
		Points: []string{"status", "faults", "batt_cur_dir", "batt_power"},
		Exprs:  exprs,
		Lookups: map[string]map[int]string{
			"status": {0x0003: "Generating", 0x1004: "Grid Off"},
		},
	}
}

// poll returns the points of one poll with the given values.
func poll(values map[string]any) []datasource.DataPoint {
	var points []datasource.DataPoint
	for _, name := range []string{"status", "faults", "batt_cur_dir", "batt_power"} {
		if v, ok := values[name]; ok {
			points = append(points, datasource.DataPoint{
				DeviceName: "solis", PointName: name, Value: v, Timestamp: ts, Unit: "u",
			})
		}
	}
	return points
}

// byName returns the values of points by point name.
func byName(points []datasource.DataPoint) map[string]any {
	out := make(map[string]any, len(points))
	for _, dp := range points {
		out[dp.PointName] = dp.Value
	}
	return out
}

func TestApply(t *testing.T) {
	t.Parallel()
	full := map[string]any{
		"status": 4100.0, "faults": 5.0, "batt_cur_dir": 0.0, "batt_power": 1200.0,
	}
	tests := []struct {
		name  string
		exprs map[string]string
		in    map[string]any
		want  map[string]any
	}{
		{
			name: "no expressions keeps the poll",
			in:   full,
			want: full,
		},
		{
			name:  "negate",
			exprs: map[string]string{"batt_power": "-value"},
			in:    map[string]any{"batt_power": 1200.0},
			want:  map[string]any{"batt_power": -1200.0},
		},
		{
			name:  "lookup",
			exprs: map[string]string{"status": `lookups.status[int(value)] ?? "unknown"`},
			in:    map[string]any{"status": 4100.0},
			want:  map[string]any{"status": "Grid Off"},
		},
		{
			name:  "lookup with unknown code",
			exprs: map[string]string{"status": `lookups.status[int(value)] ?? "unknown"`},
			in:    map[string]any{"status": 7.0},
			want:  map[string]any{"status": "unknown"},
		},
		{
			name:  "bit",
			exprs: map[string]string{"faults": "bitand(int(value), 0x0004) != 0"},
			in:    map[string]any{"faults": 5.0},
			want:  map[string]any{"faults": true},
		},
		{
			name:  "integer result becomes float64",
			exprs: map[string]string{"faults": "bitand(int(value), 0x0004)"},
			in:    map[string]any{"faults": 5.0},
			want:  map[string]any{"faults": 4.0},
		},
		{
			name: "other point of the poll, before its own expression",
			exprs: map[string]string{
				"batt_power":   "points.batt_cur_dir == 1 ? value : -value",
				"batt_cur_dir": "1 - value", // must not change what batt_power sees
			},
			in:   full,
			want: map[string]any{"status": 4100.0, "faults": 5.0, "batt_cur_dir": 1.0, "batt_power": -1200.0},
		},
		{
			name:  "nil drops the point",
			exprs: map[string]string{"batt_power": "points.batt_cur_dir == nil ? nil : value"},
			in:    map[string]any{"batt_power": 1200.0},
			want:  map[string]any{},
		},
		{
			name:  "runtime error drops only that point",
			exprs: map[string]string{"status": "bitand(value, 1)"}, // float64 is no int
			in:    map[string]any{"status": 3.0, "faults": 0.0},
			want:  map[string]any{"faults": 0.0},
		},
		{
			name:  "unsupported result type drops the point",
			exprs: map[string]string{"status": "[value]"},
			in:    map[string]any{"status": 3.0},
			want:  map[string]any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr, err := New(solis(tt.exprs), zerolog.Nop())
			require.NoError(t, err)

			got := tr.Apply(poll(tt.in))

			assert.Equal(t, tt.want, byName(got))
			for _, dp := range got {
				assert.Equal(t, ts, dp.Timestamp, "timestamp is kept")
				assert.Equal(t, "u", dp.Unit, "unit is kept")
			}
		})
	}
}

func TestNewRejectsInvalidExpressions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "syntax", expr: "value +", want: "unexpected token"},
		{name: "unknown variable", expr: "volts * 2", want: "unknown name volts"},
		{name: "unknown point", expr: "points.batt_dir == 1", want: `unknown point "batt_dir"`},
		{name: "unknown point by index", expr: `points["nope"]`, want: `unknown point "nope"`},
		{name: "unknown lookup", expr: "lookups.mode[1]", want: `unknown lookup "mode"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(solis(map[string]string{"status": tt.expr}), zerolog.Nop())
			require.ErrorIs(t, err, ErrInvalidExpression)
			assert.ErrorContains(t, err, "point status")
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestFailingExpressionWarnsOnceUntilItWorks(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	tr, err := New(solis(map[string]string{"status": "bitand(value, 1)"}),
		zerolog.New(&logs).Level(zerolog.InfoLevel))
	require.NoError(t, err)
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
		assert.Empty(t, tr.Apply(poll(map[string]any{"status": 1.0})))
	}
	assert.Equal(t, []string{"warn"}, levels(), "one warning for three failed polls")

	assert.Len(t, tr.Apply(poll(map[string]any{"status": 1})), 1, "an int works")
	tr.Apply(poll(map[string]any{"status": 1.0}))
	assert.Equal(t, []string{"warn", "info", "warn"}, levels(),
		"back at info, then a new warning when it fails again")
}

func TestUnsignedResultAndDynamicPointName(t *testing.T) {
	t.Parallel()
	tr, err := New(solis(map[string]string{
		"faults": "value",
		// A key that is not a constant cannot be checked at startup; nil when unknown.
		"status": `points[string(value)] ?? "none"`,
	}), zerolog.Nop())
	require.NoError(t, err)

	got := byName(tr.Apply(poll(map[string]any{"faults": uint64(3), "status": "faults"})))

	assert.Equal(t, map[string]any{"faults": 3.0, "status": 3.0}, got, "both normalised to float64")
}
