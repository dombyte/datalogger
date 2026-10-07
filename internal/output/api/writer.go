// Package api serves the latest value of every point over a read-only HTTP JSON API.
package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// Server timeouts: requests are small GETs, so a slow client is cut off early. They stay
// far below the 8 s shutdown deadline.
const (
	readHeaderTimeout = 5 * time.Second
	writeTimeout      = 5 * time.Second
	idleTimeout       = 60 * time.Second

	// shutdownGrace is how long running requests may take after the input is closed
	// before their connections are closed.
	shutdownGrace = 2 * time.Second
)

// ErrMissingDependency is returned by New when a required dependency is missing.
var ErrMissingDependency = errors.New("api: missing dependency")

// Settings configures a Writer.
type Settings struct {
	Name    string
	Devices []string // devices served; others are 404
	Token   string   // if set, /api requires "Authorization: Bearer <token>"
}

// Deps are the dependencies of a Writer; all are required.
type Deps struct {
	Settings Settings
	Listener net.Listener // bound by the caller, see Listen
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Writer keeps the latest point of every device and serves it over HTTP until its
// input is closed.
type Writer struct {
	settings Settings
	listener net.Listener
	clock    clock.Clock
	logger   zerolog.Logger
	store    *store
	server   *http.Server
}

// Listen binds the address the API is served on (host:port, ":8080" for all
// interfaces). A port that is in use or not allowed fails here, at startup.
func Listen(address string) (net.Listener, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("api: listen %s: %w", address, err)
	}
	return ln, nil
}

// New creates a Writer; it does not accept connections before Start.
func New(d Deps) (*Writer, error) {
	var missing []string
	if d.Listener == nil {
		missing = append(missing, "Listener")
	}
	if d.Clock == nil {
		missing = append(missing, "Clock")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingDependency, strings.Join(missing, ", "))
	}
	w := &Writer{
		settings: d.Settings,
		listener: d.Listener,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "api").Str("output", d.Settings.Name).Logger(),
		store:    newStore(d.Settings.Devices),
	}
	w.server = &http.Server{
		Handler:           w.routes(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readHeaderTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	return w, nil
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.settings.Name
}

// Start serves the API and stores every point from input until input is closed (or
// ctx is cancelled), then stops the server and closes the returned channel. If the
// server stops on its own, its error is sent first.
func (w *Writer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		if err := w.run(ctx, input); err != nil {
			done <- err
		}
	}()
	return done
}

// run serves until input is closed, ctx is cancelled or the server fails.
func (w *Writer) run(ctx context.Context, input <-chan datasource.DataPoint) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- w.server.Serve(w.listener) }()
	w.logger.Info().Str("address", w.listener.Addr().String()).Msg("API listening")

	for {
		select {
		case <-ctx.Done():
			return w.stop(ctx, serveErr)
		case err := <-serveErr:
			w.closeServer()
			return fmt.Errorf("api: serve: %w", err)
		case dp, ok := <-input:
			if !ok {
				return w.stop(ctx, serveErr)
			}
			w.store.set(dp)
		}
	}
}

// stop shuts the server down: running requests get shutdownGrace to finish (none if
// ctx is cancelled), then their connections are closed. It waits until Serve returned.
func (w *Writer) stop(ctx context.Context, serveErr <-chan error) error {
	shutdown := make(chan error, 1)
	// Shutdown gets its own context: the grace period is waited on the injected clock.
	go func() { shutdown <- w.server.Shutdown(context.WithoutCancel(ctx)) }()

	select {
	case err := <-shutdown:
		if err != nil {
			w.logger.Warn().Err(err).Msg("API shutdown")
		}
	case <-ctx.Done():
		w.closeServer()
		<-shutdown // returns once Close removed the connections
	case <-w.clock.After(shutdownGrace):
		w.logger.Warn().Stringer("grace", shutdownGrace).Msg("API requests still running, closing")
		w.closeServer()
		<-shutdown
	}

	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("api: serve: %w", err)
	}
	return nil
}

// closeServer closes the listener and all connections at once.
func (w *Writer) closeServer() {
	// Close only reports errors of the listener, which Serve has already closed or is
	// about to close; nothing is left to clean up.
	if err := w.server.Close(); err != nil {
		w.logger.Debug().Err(err).Msg("Closing API server")
	}
}
