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

func TestConvertAndScaleAllTypes(t *testing.T) {
	t.Parallel()
	body := []byte(`{"n": 2, "on": "true", "obj": {"a": 1}, "u": 4}`)
	tests := []struct {
		name  string
		point Point
		want  any
	}{
		{name: "float type", point: Point{JSONPath: "n", Type: "float32", Scale: 1}, want: 2.0},
		{name: "bool type from string", point: Point{JSONPath: "on", Type: "bool", Scale: 1}, want: true},
		{
			name:  "object without type keeps the JSON value",
			point: Point{JSONPath: "obj", Scale: 1},
			want:  map[string]any{"a": 1.0},
		},
		{
			name:  "scaled uint",
			point: Point{JSONPath: "u", Type: "uint16", Scale: 0.5, Offset: 1},
			want:  3.0,
		},
		{name: "scaled float", point: Point{JSONPath: "n", Scale: 10}, want: 20.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := extractJSONValue(body, tt.point)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractValueUnknownResponseType(t *testing.T) {
	t.Parallel()
	r := &Reader{settings: Settings{ResponseType: "xml"}}
	_, err := r.extractValue([]byte("<a/>"), Point{})
	assert.ErrorContains(t, err, "unknown response type: xml")
}
