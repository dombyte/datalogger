package http

import (
	"strings"
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
			name:  "object as string keeps the raw JSON",
			point: Point{JSONPath: "obj", Type: "string", Scale: 1},
			want:  `{"a": 1}`,
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

func TestConvertedNumbersAndBools(t *testing.T) {
	t.Parallel()
	body := []byte(`{"f": "23.5", "i": " -7 ", "u": "42", "n": 1, "z": 0, "neg": -3,
		"t": "true", "zero": "0", "s": "text"}`)
	tests := []struct {
		name  string
		point Point
		want  any
	}{
		{name: "float from string", point: Point{JSONPath: "f", Type: "float64"}, want: 23.5},
		{name: "int from string", point: Point{JSONPath: "i", Type: "int32"}, want: int64(-7)},
		{name: "uint from string", point: Point{JSONPath: "u", Type: "uint16"}, want: uint64(42)},
		{name: "uint from number", point: Point{JSONPath: "n", Type: "uint64"}, want: uint64(1)},
		{name: "bool from number", point: Point{JSONPath: "n", Type: "bool"}, want: true},
		{name: "bool from zero", point: Point{JSONPath: "z", Type: "bool"}, want: false},
		{name: "bool from string", point: Point{JSONPath: "t", Type: "bool"}, want: true},
		{name: "bool from string zero", point: Point{JSONPath: "zero", Type: "bool"}, want: false},
		{name: "string without type", point: Point{JSONPath: "s"}, want: "text"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.point.Scale = 1
			got, err := extractJSONValue(body, tt.point)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Regression: gjson turns null, "N/A" and objects into 0, false or a map; the first
// two looked like readings and a map made InfluxDB reject the whole batch.
func TestValuesThatAreNoReadingAreErrors(t *testing.T) {
	t.Parallel()
	body := []byte(`{"null": null, "na": "N/A", "obj": {"a": 1}, "list": [1, 2],
		"neg": -3, "frac": "2.5", "on": true, "num": 3.7, "big": 70000, "huge": 3000000000}`)
	tests := []struct {
		name    string
		point   Point
		wantErr string
	}{
		{name: "null without type", point: Point{JSONPath: "null"}, wantErr: "null"},
		{name: "null as float", point: Point{JSONPath: "null", Type: "float64"}, wantErr: "null"},
		{name: "null as string", point: Point{JSONPath: "null", Type: "string"}, wantErr: "null"},
		{name: "text as int", point: Point{JSONPath: "na", Type: "int64"}, wantErr: "not a valid int"},
		{name: "text as float", point: Point{JSONPath: "na", Type: "float32"}, wantErr: "not a valid"},
		{name: "fraction as uint", point: Point{JSONPath: "frac", Type: "uint32"}, wantErr: "uint"},
		{name: "text as bool", point: Point{JSONPath: "na", Type: "bool"}, wantErr: "not a bool"},
		{name: "object as bool", point: Point{JSONPath: "obj", Type: "bool"}, wantErr: "not a bool"},
		{name: "bool as number", point: Point{JSONPath: "on", Type: "int16"}, wantErr: "not a number"},
		{name: "negative as uint", point: Point{JSONPath: "neg", Type: "uint64"}, wantErr: "not a valid uint64"},
		// Regression: 3.7 as int64 was cut to 3, and 70000 as int16 was delivered.
		{name: "fraction as int", point: Point{JSONPath: "num", Type: "int64"}, wantErr: "not a valid int64"},
		{name: "int16 overflow", point: Point{JSONPath: "big", Type: "int16"}, wantErr: "not a valid int16"},
		{name: "uint16 overflow", point: Point{JSONPath: "big", Type: "uint16"}, wantErr: "not a valid uint16"},
		{name: "int32 overflow", point: Point{JSONPath: "huge", Type: "int32"}, wantErr: "not a valid int32"},
		{name: "unknown type", point: Point{JSONPath: "neg", Type: "flaot64"}, wantErr: "unknown type"},
		{name: "object without type", point: Point{JSONPath: "obj"}, wantErr: "object or array"},
		{name: "array without type", point: Point{JSONPath: "list"}, wantErr: "object or array"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.point.Scale = 1
			_, err := extractJSONValue(body, tt.point)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// Regression: a text response ignored type, scale and offset and kept surrounding white
// space ("23.5\n" was a string), and device text reached the outputs unchecked.
func TestTextValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body string
		point      Point
		want       any
		wantErr    string
	}{
		{name: "string", body: "OK 42", point: Point{}, want: "OK 42"},
		{name: "trimmed", body: " on \r\n", point: Point{}, want: "on"},
		{name: "float", body: "23.5\n", point: Point{Type: "float64"}, want: 23.5},
		{name: "scaled", body: "235", point: Point{Type: "int16", Scale: 0.1}, want: 23.5},
		{name: "bool", body: "1", point: Point{Type: "bool"}, want: true},
		{name: "not a number", body: "N/A", point: Point{Type: "uint16"}, wantErr: "not a valid"},
		{name: "not a bool", body: "maybe", point: Point{Type: "bool"}, wantErr: "not a bool"},
		{
			name: "control characters", body: "on\x1b[31m\rred\x00\tok\nline",
			point: Point{}, want: "on[31mred\tok\nline",
		},
		{name: "invalid utf-8", body: "a\xffb", point: Point{}, want: "a\uFFFDb"},
		{
			name: "too long", body: strings.Repeat("x", maxStringSize+1), point: Point{},
			wantErr: "longer than",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.point.Scale == 0 {
				tt.point.Scale = 1
			}
			got, err := extractTextValue([]byte(tt.body), tt.point)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// JSON strings are cleaned like text: no control characters, at most maxStringSize.
func TestJSONStringsAreCleaned(t *testing.T) {
	t.Parallel()
	got, err := extractJSONValue([]byte(`{"s": "a\u001b[2Jb\r"}`), Point{JSONPath: "s", Scale: 1})
	require.NoError(t, err)
	assert.Equal(t, "a[2Jb", got)

	long := `{"s": "` + strings.Repeat("x", maxStringSize+1) + `"}`
	_, err = extractJSONValue([]byte(long), Point{JSONPath: "s", Type: "string", Scale: 1})
	assert.ErrorContains(t, err, "longer than")
}
