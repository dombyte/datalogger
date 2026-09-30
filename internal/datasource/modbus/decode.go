package modbus

import (
	"errors"
	"fmt"
	"math"
)

const (
	// 32-bit types span two 16-bit registers, high word first.
	registersPer32Bit = 2
	bitsPerRegister   = 16
)

// decodeValue decodes raw registers as dataType and returns the value as float64
// (bool: 1 or 0).
func decodeValue(registers []uint16, dataType string) (float64, error) {
	if len(registers) == 0 {
		return 0, errors.New("no registers to decode")
	}

	switch dataType {
	case "int16":
		return float64(int16(registers[0])), nil
	case "uint16":
		return float64(registers[0]), nil
	case "bool":
		if registers[0] != 0 {
			return 1, nil
		}
		return 0, nil
	case "int32", "uint32", "float32":
		return decode32(registers, dataType)
	default:
		return 0, fmt.Errorf("unknown data type: %s", dataType)
	}
}

// decode32 decodes a 32-bit type from two registers, high word first.
func decode32(registers []uint16, dataType string) (float64, error) {
	if len(registers) < registersPer32Bit {
		return 0, fmt.Errorf("%s needs 2 registers (count: 2), got %d", dataType, len(registers))
	}

	bits := uint32(registers[0])<<bitsPerRegister | uint32(registers[1])
	switch dataType {
	case "int32":
		return float64(int32(bits)), nil
	case "float32":
		return float64(math.Float32frombits(bits)), nil
	default:
		return float64(bits), nil
	}
}
