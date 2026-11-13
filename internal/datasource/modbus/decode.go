package modbus

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
)

const (
	// Registers of the multi-register types; the first one is the high word (ABCD).
	registersPer32Bit = 2
	registersPer64Bit = 4
	bitsPerRegister   = 16
	bitsPerByte       = 8
	bytesPerRegister  = 2

	// Byte orders of a value, as the letters of a 32-bit value: ABCD is big-endian with
	// the high word first.
	orderABCD = "ABCD" // the default
	orderCDAB = "CDAB" // words swapped: low word first
	orderBADC = "BADC" // bytes swapped in every word
	orderDCBA = "DCBA" // both: little-endian
)

// decodeValue decodes raw registers as dataType in the byte order and returns float64
// (bool: 1 or 0) or, for string, the text.
func decodeValue(registers []uint16, dataType, order string) (any, error) {
	if len(registers) == 0 {
		return nil, errors.New("no registers to decode")
	}
	if dataType == "string" {
		return decodeString(swapBytes(registers, order)), nil
	}
	registers = reorder(registers, order)
	switch dataType {
	case "int32", "uint32", "float32":
		return decode32(registers, dataType)
	case "int64", "uint64", "float64":
		return decode64(registers, dataType)
	default:
		return decode16(registers[0], dataType)
	}
}

// decode16 decodes a 16-bit type from one register.
func decode16(register uint16, dataType string) (any, error) {
	switch dataType {
	case "int16":
		return float64(int16(register)), nil
	case "uint16":
		return float64(register), nil
	case "bool":
		if register != 0 {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return nil, fmt.Errorf("unknown data type: %s", dataType)
	}
}

// reorder returns the registers in ABCD order: CDAB and DCBA reverse the words, BADC
// and DCBA swap the bytes of every word. ABCD returns them unchanged.
func reorder(registers []uint16, order string) []uint16 {
	if order == "" || order == orderABCD {
		return registers
	}
	words := swapBytes(registers, order)
	if order == orderCDAB || order == orderDCBA {
		reversed := make([]uint16, len(words))
		for i, w := range words {
			reversed[len(words)-1-i] = w
		}
		return reversed
	}
	return words
}

// swapBytes returns the registers with their two bytes swapped for BADC and DCBA.
func swapBytes(registers []uint16, order string) []uint16 {
	if order != orderBADC && order != orderDCBA {
		return registers
	}
	swapped := make([]uint16, len(registers))
	for i, r := range registers {
		swapped[i] = r>>bitsPerByte | r<<bitsPerByte
	}
	return swapped
}

// decodeString decodes two characters per register, high byte first, up to the first
// NUL; surrounding spaces and control characters are removed.
func decodeString(registers []uint16) string {
	b := make([]byte, 0, len(registers)*bytesPerRegister)
	for _, r := range registers {
		b = append(b, byte(r>>bitsPerByte), byte(r))
	}
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		b = b[:i]
	}
	text := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(string(b), string(unicode.ReplacementChar)))
	return strings.TrimSpace(text)
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

// decode64 decodes a 64-bit type from four registers, high word first. The value is a
// float64 like every reading: integers above 2^53 lose their last digits.
func decode64(registers []uint16, dataType string) (float64, error) {
	if len(registers) < registersPer64Bit {
		return 0, fmt.Errorf("%s needs 4 registers (count: 4), got %d", dataType, len(registers))
	}
	var bits uint64
	for _, r := range registers[:registersPer64Bit] {
		bits = bits<<bitsPerRegister | uint64(r)
	}
	switch dataType {
	case "int64":
		return float64(int64(bits)), nil
	case "float64":
		return math.Float64frombits(bits), nil
	default:
		return float64(bits), nil
	}
}
