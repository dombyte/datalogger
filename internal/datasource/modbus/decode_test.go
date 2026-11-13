package modbus

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		registers []uint16
		dataType  string
		order     string
		want      any
		wantErr   string
	}{
		{name: "int16 positive", registers: []uint16{0x1234}, dataType: "int16", want: 4660.0},
		{name: "int16 negative", registers: []uint16{0xFFFF}, dataType: "int16", want: -1.0},
		{name: "uint16", registers: []uint16{0xFFFF}, dataType: "uint16", want: 65535.0},
		{name: "bool true", registers: []uint16{5}, dataType: "bool", want: 1.0},
		{name: "bool false", registers: []uint16{0}, dataType: "bool", want: 0.0},
		{name: "int32", registers: []uint16{0xFFFF, 0xFFFE}, dataType: "int32", want: -2.0},
		{
			name: "uint32", registers: []uint16{0x1234, 0x5678}, dataType: "uint32",
			want: float64(0x12345678),
		},
		{
			name: "float32", registers: []uint16{0x4049, 0x0fdb}, dataType: "float32",
			want: float64(float32(3.1415927)),
		},
		// The same value 0x12345678 in every byte order.
		{
			name: "uint32 CDAB", registers: []uint16{0x5678, 0x1234}, dataType: "uint32",
			order: "CDAB", want: float64(0x12345678),
		},
		{
			name: "uint32 BADC", registers: []uint16{0x3412, 0x7856}, dataType: "uint32",
			order: "BADC", want: float64(0x12345678),
		},
		{
			name: "uint32 DCBA", registers: []uint16{0x7856, 0x3412}, dataType: "uint32",
			order: "DCBA", want: float64(0x12345678),
		},
		{
			name: "int16 byte swapped", registers: []uint16{0x3412}, dataType: "int16",
			order: "BADC", want: 4660.0,
		},
		{
			name: "int16 word order has no effect", registers: []uint16{0x1234},
			dataType: "int16", order: "CDAB", want: 4660.0,
		},
		{
			name: "uint64", registers: []uint16{0, 0, 0x0001, 0x0002}, dataType: "uint64",
			want: float64(0x10002),
		},
		{
			name: "int64", registers: []uint16{0xFFFF, 0xFFFF, 0xFFFF, 0xFFFD}, dataType: "int64",
			want: -3.0,
		},
		{
			name: "float64", registers: f64Registers(math.Pi), dataType: "float64",
			want: math.Pi,
		},
		{
			name: "int64 CDAB", registers: []uint16{0x0002, 0x0001, 0, 0}, dataType: "int64",
			order: "CDAB", want: float64(0x10002),
		},
		{
			name: "string", registers: []uint16{0x4142, 0x4320, 0x0000, 0x4545},
			dataType: "string", want: "ABC",
		},
		{
			name: "string byte swapped", registers: []uint16{0x4241, 0x2043},
			dataType: "string", order: "BADC", want: "ABC",
		},
		{
			name: "string words keep their order", registers: []uint16{0x4142, 0x4344},
			dataType: "string", order: "CDAB", want: "ABCD",
		},
		{
			name: "string without control characters", registers: []uint16{0x411B, 0x0D42},
			dataType: "string", want: "AB",
		},
		{
			name: "32-bit with one register", registers: []uint16{1}, dataType: "int32",
			wantErr: "int32 needs 2 registers",
		},
		{
			name: "64-bit with two registers", registers: []uint16{1, 2}, dataType: "float64",
			wantErr: "float64 needs 4 registers",
		},
		{name: "no registers", registers: nil, dataType: "uint16", wantErr: "no registers"},
		{name: "unknown type", registers: []uint16{1}, dataType: "int8", wantErr: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeValue(tt.registers, tt.dataType, tt.order)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// f64Registers returns the four registers of v, high word first.
func f64Registers(v float64) []uint16 {
	bits := math.Float64bits(v)
	return []uint16{uint16(bits >> 48), uint16(bits >> 32), uint16(bits >> 16), uint16(bits)}
}

func TestReorderKeepsInput(t *testing.T) {
	t.Parallel()
	in := []uint16{1, 2}
	_ = reorder(in, "DCBA")
	assert.Equal(t, []uint16{1, 2}, in, "the register buffer of a range is not changed")
}
