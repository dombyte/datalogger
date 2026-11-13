package status_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/status"
)

func TestProblems(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		snap status.Snapshot
		want []string
	}{
		{name: "healthy", snap: status.Snapshot{
			Devices: []status.Device{{Name: "meter"}}, // no data yet: no problem
			Outputs: []status.Output{{Name: "db", Queued: 9, StalledSeconds: 299}},
		}},
		{name: "stopped components", snap: status.Snapshot{
			Devices: []status.Device{{Name: "meter", Stopped: true}},
			Outputs: []status.Output{{Name: "db", Stopped: true}},
		}, want: []string{"device meter stopped", "output db stopped"}},
		{name: "stopping", snap: status.Snapshot{
			Stopping: true,
			Devices:  []status.Device{{Name: "meter", Stopped: true}},
		}},
		{name: "stuck output", snap: status.Snapshot{
			Outputs: []status.Output{{Name: "db", Queued: 9, StalledSeconds: 300}},
		}, want: []string{"output db took no data for 300s, 9 polls queued"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.snap.Problems())
		})
	}
	assert.Equal(t, "a; b", status.Describe([]string{"a", "b"}))
}

func TestServeAndQuery(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "s.sock")
	// A socket left by a process that crashed is replaced.
	old, err := net.Listen("unix", path)
	require.NoError(t, err)
	old.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, old.Close())

	ln, err := status.Listen(path)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o660), info.Mode().Perm())

	want := status.Snapshot{
		Time:    time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC),
		Devices: []status.Device{{Name: "meter", Polls: 3}},
	}
	server := status.NewServer(ln, func() status.Snapshot { return want }, zerolog.Nop())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.Serve()
	}()

	got, err := status.Query(path, time.Second)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	require.NoError(t, server.Close())
	<-done
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist, "the socket is removed")
}

func TestListenRefusesOtherFiles(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	_, err := status.Listen(path)
	assert.ErrorContains(t, err, "is no socket")
	_, err = status.Listen(filepath.Join(t.TempDir(), "missing", "s.sock"))
	assert.ErrorContains(t, err, "status: listen")
}

// A process that accepts but does not answer counts as hung.
func TestQueryTimesOut(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "s.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	_, err = status.Query(path, 50*time.Millisecond)
	assert.ErrorContains(t, err, "status: read answer")
}
