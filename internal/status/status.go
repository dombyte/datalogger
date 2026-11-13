// Package status serves the state of a running datalogger on a Unix socket and reads it
// back for -status and -healthcheck: the process answers itself, so a check sees more
// than a file age and finds a component that stopped or an output that is stuck.
package status

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// StallAfter is how long an output may hold queued polls without taking one before
	// it counts as stuck.
	StallAfter = 5 * time.Minute

	// socketPerm lets the owner and its group connect (the healthcheck runs as the
	// same user, an admin may be in the group).
	socketPerm = 0o660

	// writeTimeout bounds the answer to one client.
	writeTimeout = 2 * time.Second
)

// Snapshot is the state of the components at one time.
type Snapshot struct {
	Time     time.Time `json:"time"`
	Stopping bool      `json:"stopping"`
	Devices  []Device  `json:"devices"`
	Outputs  []Output  `json:"outputs"`
}

// Device is the state of one device: its polls that delivered points.
type Device struct {
	Name     string     `json:"name"`
	Polls    int64      `json:"polls"`
	Points   int64      `json:"points"`    // in the last poll
	LastPoll *time.Time `json:"last_poll"` // null before the first poll with points
	Stopped  bool       `json:"stopped"`
}

// Output is the state of one output and its queue.
type Output struct {
	Name     string `json:"name"`
	Queued   int    `json:"queued"`   // polls waiting in the queue
	Capacity int    `json:"capacity"` // buffer_size
	Sent     int64  `json:"sent"`     // polls handed over
	Dropped  int64  `json:"dropped"`  // points dropped because the queue was full
	// StalledSeconds is how long the queue has held polls without the output taking one
	// (0 while it takes data or the queue is empty).
	StalledSeconds int64 `json:"stalled_seconds"`
	Stopped        bool  `json:"stopped"`
}

// Problems returns why the snapshot is unhealthy: a component that stopped while the
// datalogger is not shutting down, or an output stuck for StallAfter. Devices without
// data are not a problem: a restart does not bring back a device that is switched off.
func (s Snapshot) Problems() []string {
	var problems []string
	for _, d := range s.Devices {
		if d.Stopped && !s.Stopping {
			problems = append(problems, "device "+d.Name+" stopped")
		}
	}
	for _, o := range s.Outputs {
		if problem := o.problem(s.Stopping); problem != "" {
			problems = append(problems, problem)
		}
	}
	return problems
}

// problem returns why the output is unhealthy, or "".
func (o Output) problem(stopping bool) string {
	switch {
	case o.Stopped && !stopping:
		return "output " + o.Name + " stopped"
	case time.Duration(o.StalledSeconds)*time.Second >= StallAfter:
		return fmt.Sprintf("output %s took no data for %ds, %d polls queued",
			o.Name, o.StalledSeconds, o.Queued)
	default:
		return ""
	}
}

// Listen binds the Unix socket at path. A socket left by a process that crashed is
// removed first; any other file at path is an error.
func Listen(path string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("status: %s exists and is no socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("status: remove old socket: %w", err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("status: listen %s: %w", path, err)
	}
	if err := os.Chmod(path, socketPerm); err != nil {
		return nil, errors.Join(fmt.Errorf("status: chmod %s: %w", path, err), ln.Close())
	}
	return ln, nil
}

// Server answers every connection on its listener with the current snapshot as JSON.
type Server struct {
	listener net.Listener
	snapshot func() Snapshot
	logger   zerolog.Logger
	wg       sync.WaitGroup
}

// NewServer creates a Server; Serve starts it.
func NewServer(ln net.Listener, snapshot func() Snapshot, log zerolog.Logger) *Server {
	return &Server{
		listener: ln,
		snapshot: snapshot,
		logger:   log.With().Str("component", "status").Logger(),
	}
}

// Serve accepts connections until Close; it returns once every answer is written.
func (s *Server) Serve() {
	s.logger.Info().Str("socket", s.listener.Addr().String()).Msg("Status socket listening")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				s.logger.Error().Err(err).Msg("Status socket failed")
			}
			s.wg.Wait()
			return
		}
		s.wg.Go(func() { s.answer(conn) })
	}
}

// answer writes the snapshot to conn and closes it.
func (s *Server) answer(conn net.Conn) {
	defer conn.Close()
	// A client that went away only costs its answer.
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		s.logger.Debug().Err(err).Msg("Status client went away")
		return
	}
	if err := json.NewEncoder(conn).Encode(s.snapshot()); err != nil {
		s.logger.Debug().Err(err).Msg("Status client went away")
	}
}

// Close stops Serve and removes the socket file.
func (s *Server) Close() error {
	return s.listener.Close() // a Unix listener removes its file
}

// Query reads the snapshot from the socket at path, waiting at most timeout: a process
// that does not answer in time counts as hung.
func Query(path string, timeout time.Duration) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return Snapshot{}, fmt.Errorf("status: connect %s: %w", path, err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		return Snapshot{}, fmt.Errorf("status: %w", err)
	}
	var s Snapshot
	if err := json.NewDecoder(conn).Decode(&s); err != nil {
		return Snapshot{}, fmt.Errorf("status: read answer: %w", err)
	}
	return s, nil
}

// Describe returns the problems of s in one line, for the healthcheck output.
func Describe(problems []string) string {
	return strings.Join(problems, "; ")
}
