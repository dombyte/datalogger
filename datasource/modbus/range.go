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

// parseRanges parses range specifications in the format "start-end" and returns a slice of Ranges.
func parseRanges(rangeSpecs []string) ([]Range, error) {
	var ranges []Range
	for _, spec := range rangeSpecs {
		parts := strings.Split(spec, "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid range format '%s': expected 'start-end'", spec)
		}
		start, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid start address in range '%s': %w", spec, err)
		}
		end, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid end address in range '%s': %w", spec, err)
		}
		if start > end {
			return nil, fmt.Errorf("invalid range '%s': start > end", spec)
		}
		ranges = append(ranges, Range{Start: uint16(start), End: uint16(end)})
	}
	return ranges, nil
}
