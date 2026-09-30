// Package modbus provides Modbus TCP/RTU reading functionality.
package modbus

import (
	"fmt"
	"strconv"
	"strings"
)

// Range represents a range of Modbus addresses.
type Range struct {
	Start uint16
	End   uint16
}

// parseRanges parses range specifications "start-end" (inclusive) into Ranges.
func parseRanges(rangeSpecs []string) ([]Range, error) {
	var ranges []Range
	for _, spec := range rangeSpecs {
		rng, err := parseRange(spec)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, rng)
	}
	return ranges, nil
}

// parseRange parses one "start-end" specification.
func parseRange(spec string) (Range, error) {
	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return Range{}, fmt.Errorf("invalid range format '%s': expected 'start-end'", spec)
	}
	start, err := parseAddress(startStr)
	if err != nil {
		return Range{}, fmt.Errorf("invalid start address in range '%s': %w", spec, err)
	}
	end, err := parseAddress(endStr)
	if err != nil {
		return Range{}, fmt.Errorf("invalid end address in range '%s': %w", spec, err)
	}
	if start > end {
		return Range{}, fmt.Errorf("invalid range '%s': start > end", spec)
	}
	return Range{Start: start, End: end}, nil
}

// parseAddress parses a register address (0-65535).
func parseAddress(s string) (uint16, error) {
	v, err := strconv.ParseUint(s, 10, 16)
	return uint16(v), err
}
