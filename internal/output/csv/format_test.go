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
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatValue(tt.value))
	}
}
