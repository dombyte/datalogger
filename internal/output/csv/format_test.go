package csv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value any
		want  string
	}{
		{23.5, "23.5"},
		{1e21, "1000000000000000000000"},
		{int64(-42), "-42"},
		{uint64(42), "42"},
		{true, "true"},
		{"text, with comma", "text, with comma"},
		{nil, "<nil>"},
		// Regression: device text that starts like a formula ran in a spreadsheet.
		{"=HYPERLINK(\"http://x\")", "'=HYPERLINK(\"http://x\")"},
		{"+cmd|' /C calc'!A0", "'+cmd|' /C calc'!A0"},
		{"-1+1", "'-1+1"},
		{"@SUM(A1)", "'@SUM(A1)"},
		{"\tx", "'\tx"},
		{"a=b", "a=b"},
		{"", ""},
		{-1.5, "-1.5"}, // numbers stay numbers
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatValue(tt.value))
	}
}
