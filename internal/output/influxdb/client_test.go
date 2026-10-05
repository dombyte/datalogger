package influxdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/output/influxdb"
)

func TestNewClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		s       influxdb.ConnSettings
		wantErr bool
	}{
		{name: "plain", s: influxdb.ConnSettings{Address: "http://db:8181", Token: "t", Database: "d"}},
		{
			name: "insecure",
			s:    influxdb.ConnSettings{Address: "https://db:8181", Token: "t", Database: "d", Insecure: true},
		},
		{name: "no address", s: influxdb.ConnSettings{Token: "t", Database: "d"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, err := influxdb.NewClient(tt.s)
			if tt.wantErr {
				assert.ErrorContains(t, err, "influxdb:")
				return
			}
			require.NoError(t, err)
			assert.NoError(t, client.Close())
		})
	}
}
