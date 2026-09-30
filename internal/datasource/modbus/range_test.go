package modbus

import (
	"fmt"
	"testing"
)

// TestParseRanges tests the parseRanges function
func TestParseRanges(t *testing.T) {
	tests := []struct {
		name       string
		rangeSpecs []string
		want       []Range
		wantErr    bool
	}{
		{
			name:       "valid single range",
			rangeSpecs: []string{"100-200"},
			want:       []Range{{Start: 100, End: 200}},
			wantErr:    false,
		},
		{
			name:       "valid multiple ranges",
			rangeSpecs: []string{"100-200", "300-400"},
			want:       []Range{{Start: 100, End: 200}, {Start: 300, End: 400}},
			wantErr:    false,
		},
		{
			name:       "empty range specs",
			rangeSpecs: []string{},
			want:       []Range{},
			wantErr:    false,
		},
		{
			name:       "same start and end",
			rangeSpecs: []string{"100-100"},
			want:       []Range{{Start: 100, End: 100}},
			wantErr:    false,
		},
		{
			name:       "zero range",
			rangeSpecs: []string{"0-100"},
			want:       []Range{{Start: 0, End: 100}},
			wantErr:    false,
		},
		{
			name:       "large numbers",
			rangeSpecs: []string{"65535-65535"},
			want:       []Range{{Start: 65535, End: 65535}},
			wantErr:    false,
		},
		// Error cases
		{
			name:       "invalid format - no dash",
			rangeSpecs: []string{"100200"},
			want:       nil,
			wantErr:    true,
		},
		{
			name:       "invalid start - non numeric",
			rangeSpecs: []string{"abc-200"},
			want:       nil,
			wantErr:    true,
		},
		{
			name:       "invalid end - non numeric",
			rangeSpecs: []string{"100-xyz"},
			want:       nil,
			wantErr:    true,
		},
		{
			name:       "start greater than end",
			rangeSpecs: []string{"200-100"},
			want:       nil,
			wantErr:    true,
		},
		{
			name:       "negative start",
			rangeSpecs: []string{"-100-200"},
			want:       nil,
			wantErr:    true,
		},
		{
			name:       "leading zeros",
			rangeSpecs: []string{"00100-00200"},
			want:       []Range{{Start: 100, End: 200}},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRanges(tt.rangeSpecs)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRanges() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("parseRanges() got %d ranges, want %d", len(got), len(tt.want))
				return
			}

			for i, rng := range got {
				if rng.Start != tt.want[i].Start {
					t.Errorf("Range %d Start = %v, want %v", i, rng.Start, tt.want[i].Start)
				}
				if rng.End != tt.want[i].End {
					t.Errorf("Range %d End = %v, want %v", i, rng.End, tt.want[i].End)
				}
			}
		})
	}
}

// TestRangeStruct tests the Range struct
func TestRangeStruct(t *testing.T) {
	r := Range{Start: 100, End: 200}

	if r.Start != 100 {
		t.Errorf("Start = %v, want %v", r.Start, 100)
	}

	if r.End != 200 {
		t.Errorf("End = %v, want %v", r.End, 200)
	}
}

// TestRangeOverlap tests that ranges can overlap (this is allowed by the parser)
func TestRangeOverlap(t *testing.T) {
	rangeSpecs := []string{"100-200", "150-250", "100-250"}
	got, err := parseRanges(rangeSpecs)
	if err != nil {
		t.Fatalf("parseRanges() unexpected error: %v", err)
	}

	if len(got) != len(rangeSpecs) {
		t.Errorf("parseRanges() returned %d ranges, want %d", len(got), len(rangeSpecs))
	}

	expected := []Range{
		{Start: 100, End: 200},
		{Start: 150, End: 250},
		{Start: 100, End: 250},
	}

	for i, rng := range got {
		if rng.Start != expected[i].Start || rng.End != expected[i].End {
			t.Errorf("Range %d: got (%d-%d), want (%d-%d)", i, rng.Start, rng.End, expected[i].Start, expected[i].End)
		}
	}
}

// TestRangeAdjacent tests adjacent ranges
func TestRangeAdjacent(t *testing.T) {
	rangeSpecs := []string{"100-199", "200-299"}
	got, err := parseRanges(rangeSpecs)
	if err != nil {
		t.Fatalf("parseRanges() unexpected error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("parseRanges() returned %d ranges, want 2", len(got))
	}

	if got[0].End+1 != got[1].Start {
		t.Errorf("Adjacent ranges should have end+1 == next start")
	}
}

// TestParseRangesEdgeCases tests edge cases for parseRanges
func TestParseRangesEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		rangeSpecs []string
		wantLen    int
		wantErr    bool
	}{
		{
			name:       "nil input",
			rangeSpecs: nil,
			wantLen:    0,
			wantErr:    false,
		},
		{
			name:       "empty string range",
			rangeSpecs: []string{"", "100-200"},
			wantLen:    0,
			wantErr:    true,
		},
		{
			name:       "dash only",
			rangeSpecs: []string{"-"},
			wantLen:    0,
			wantErr:    true,
		},
		{
			name:       "very large range",
			rangeSpecs: []string{"0-65535"},
			wantLen:    1,
			wantErr:    false,
		},
		{
			name:       "whitespace in range",
			rangeSpecs: []string{" 100 - 200 "},
			wantLen:    0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRanges(tt.rangeSpecs)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseRanges() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(got) != tt.wantLen {
				t.Errorf("parseRanges() returned %d ranges, want %d", len(got), tt.wantLen)
			}
		})
	}
}

// TestParseRangesPerformance tests performance with many ranges
func TestParseRangesPerformance(t *testing.T) {
	rangeSpecs := make([]string, 1000)
	for i := range 1000 {
		rangeSpecs[i] = fmt.Sprintf("%d-%d", i*10, i*10+100)
	}

	got, err := parseRanges(rangeSpecs)
	if err != nil {
		t.Fatalf("parseRanges() unexpected error: %v", err)
	}

	if len(got) != 1000 {
		t.Errorf("parseRanges() returned %d ranges, want 1000", len(got))
	}
}
