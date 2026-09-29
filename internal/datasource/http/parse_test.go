package http

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractJSONValue(t *testing.T) {
	t.Parallel()

	body := []byte(`{"temp": 23.5, "sensor": {"power": 1500}, "list": [1, 2, 3],
		"on": true, "name": "meter", "big": 18446744073709551615}`)
	tests := []struct {
		name    string
		point   Point
		want    any
		wantErr string
	}{
		{name: "number without type", point: Point{JSONPath: "temp"}, want: 23.5},
		{
			name: "nested int", point: Point{JSONPath: "sensor.power", Type: "int64"},
			want: int64(1500),
		},
		{name: "array element", point: Point{JSONPath: "list.1", Type: "uint32"}, want: uint64(2)},
		{
			name: "big uint", point: Point{JSONPath: "big", Type: "uint64"},
			want: uint64(18446744073709551615),
		},
		{name: "bool", point: Point{JSONPath: "on"}, want: true},
		{name: "string", point: Point{JSONPath: "name", Type: "string"}, want: "meter"},
		{name: "scale and offset", point: Point{
			JSONPath: "sensor.power", Type: "int64",
			Scale: 0.001, Offset: 1,
		}, want: 2.5},
		{name: "scale leaves bools", point: Point{JSONPath: "on", Scale: 10}, want: true},
		{name: "missing path", point: Point{JSONPath: "nope"}, wantErr: "not found"},
		{name: "empty path", point: Point{}, wantErr: "json_path required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.point.Scale == 0 {
				tt.point.Scale = 1 // the config default
			}
			got, err := extractJSONValue(body, tt.point)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
