package influxdb_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// Regression: with Insecure the client used its own http.Client without a timeout, so
// a server that never answered blocked the writer forever.
func TestInsecureClientTimesOut(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release // does not answer before the test ends
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // runs first: lets Close return

	client, err := influxdb.NewClient(influxdb.ConnSettings{
		Address: srv.URL, Token: "t", Database: "d", Insecure: true,
		Timeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Write(ctx, []byte("m value=1 1\n"))
	require.Error(t, err)
	assert.NoError(t, ctx.Err(), "the client timeout ended the request, not the test")
}
