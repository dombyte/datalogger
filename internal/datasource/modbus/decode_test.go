package modbus

import (
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
		want      float64
		wantErr   string
	}{
		{name: "int16 positive", registers: []uint16{0x1234}, dataType: "int16", want: 0x1234},
		{name: "int16 negative", registers: []uint16{0xFFFF}, dataType: "int16", want: -1},
		{name: "uint16", registers: []uint16{0xFFFF}, dataType: "uint16", want: 65535},
		{name: "bool true", registers: []uint16{5}, dataType: "bool", want: 1},
		{name: "bool false", registers: []uint16{0}, dataType: "bool", want: 0},
		{name: "int32", registers: []uint16{0xFFFF, 0xFFFE}, dataType: "int32", want: -2},
		{name: "uint32", registers: []uint16{0x1234, 0x5678}, dataType: "uint32", want: 0x12345678},
		{
			name:      "float32",
			registers: []uint16{0x4049, 0x0fdb},
			dataType:  "float32",
			want:      float64(float32(3.1415927)),
		},
		{
			name:      "32-bit with one register",
			registers: []uint16{1},
			dataType:  "int32",
			wantErr:   "int32 needs 2 registers",
		},
		{name: "no registers", registers: nil, dataType: "uint16", wantErr: "no registers"},
		{name: "unknown type", registers: []uint16{1}, dataType: "string", wantErr: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeValue(tt.registers, tt.dataType)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tt.want, got, 0)
		})
	}
}
