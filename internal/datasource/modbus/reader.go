// Package modbus reads Modbus TCP/RTU devices.
package modbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

const (
	// Modbus function codes that select the register type.
	fcHoldingRegisters = 3
	fcInputRegisters   = 4

	// Backoff after failed polls: starts at initialBackoff and doubles up to maxBackoff.
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

var (
	// ErrMissingDependency is returned by New when a required dependency is missing.
	ErrMissingDependency = errors.New("modbus: missing dependency")

	// ErrInvalidSettings is returned by New for settings the reader cannot work with.
	ErrInvalidSettings = errors.New("modbus: invalid settings")
)

// Settings configures a Reader.
type Settings struct {
	Name         string
	PollInterval time.Duration
	Parallelism  int
	RangeMode    bool     // read Ranges and decode points from them
	Ranges       []string // "start-end", inclusive; range mode only
	Points       []Point
}

// Point is one value read from the device.
type Point struct {
	Name         string
	Register     uint16
	Count        uint16 // registers to read; 0 means 1
	Type         string // int16, uint16, int32, uint32, float32, bool
	FunctionCode uint8  // 3 holding, 4 input; the first 3/4 decides for the device
	Scale        float64
	Offset       float64
	Unit         string
}

// Deps are the dependencies of a Reader; all are required.
type Deps struct {
	Settings Settings
	Dialer   Dialer
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Reader polls one Modbus device. It connects on the first poll and reconnects after
// connection errors, so an unreachable device recovers without a restart.
type Reader struct {
	settings Settings
	dialer   Dialer
	clock    clock.Clock
	logger   zerolog.Logger
	ranges   []Range
	regType  modbus.RegType

	client    Client // nil while disconnected
	failCount int
	backoff   time.Duration
}

// New creates a Reader; it does not connect.
func New(d Deps) (*Reader, error) {
	if err := checkDeps(d); err != nil {
		return nil, err
	}

	r := &Reader{
		settings: d.Settings,
		dialer:   d.Dialer,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "modbus").Str("device", d.Settings.Name).Logger(),
		regType:  regType(d.Settings.Points),
	}

	if d.Settings.RangeMode {
		ranges, err := parseRanges(d.Settings.Ranges)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidSettings, err)
		}
		r.ranges = ranges
		if err := r.checkCoverage(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// checkDeps reports missing dependencies and unusable settings.
func checkDeps(d Deps) error {
	var missing []string
	if d.Dialer == nil {
		missing = append(missing, "Dialer")
	}
	if d.Clock == nil {
		missing = append(missing, "Clock")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", ErrMissingDependency, strings.Join(missing, ", "))
	}
	if d.Settings.PollInterval <= 0 || d.Settings.Parallelism < 1 || len(d.Settings.Points) == 0 {
		return fmt.Errorf("%w: poll interval, parallelism and points are required",
			ErrInvalidSettings)
	}
	return nil
}

// regType returns the register type of the first point with function code 3 or 4
// (holding registers by default).
func regType(points []Point) modbus.RegType {
	for _, p := range points {
		switch p.FunctionCode {
		case fcHoldingRegisters:
			return modbus.HOLDING_REGISTER
		case fcInputRegisters:
			return modbus.INPUT_REGISTER
		}
	}
	return modbus.HOLDING_REGISTER
}

// checkCoverage checks that one configured range holds all registers of every point.
func (r *Reader) checkCoverage() error {
	for _, p := range r.settings.Points {
		if !r.isCovered(p) {
			return fmt.Errorf("%w: point '%s' register range %d-%d not covered by any range",
				ErrInvalidSettings, p.Name, p.Register, pointEnd(p))
		}
	}
	return nil
}

// isCovered reports whether one configured range holds all registers of the point.
func (r *Reader) isCovered(p Point) bool {
	end := pointEnd(p)
	for _, rng := range r.ranges {
		if p.Register >= rng.Start && end <= rng.End {
			return true
		}
	}
	return false
}

// pointEnd returns the last register of a point.
func pointEnd(p Point) uint16 {
	return p.Register + registerCount(p) - 1
}

// registerCount returns the number of registers of a point (count 0 means 1).
func registerCount(p Point) uint16 {
	return max(p.Count, 1)
}

// Name returns the device name.
func (r *Reader) Name() string {
	return r.settings.Name
}

// Start starts the poll loop; the data channel is closed when the loop ends after ctx
// is cancelled.
func (r *Reader) Start(ctx context.Context) <-chan []datasource.DataPoint {
	dataCh := make(chan []datasource.DataPoint)
	go r.pollLoop(ctx, dataCh)
	return dataCh
}

// pollLoop polls on every tick until ctx is cancelled, then disconnects.
func (r *Reader) pollLoop(ctx context.Context, dataCh chan<- []datasource.DataPoint) {
	defer close(dataCh)
	defer r.disconnect()

	ticker := r.clock.NewTicker(r.settings.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			r.pollOnce(ctx, dataCh)
		}
	}
}

// pollOnce waits out the backoff, reads all points and sends what was read.
func (r *Reader) pollOnce(ctx context.Context, dataCh chan<- []datasource.DataPoint) {
	if r.backoff > 0 {
		r.logger.Warn().Dur("wait", r.backoff).Msg("Waiting before retry after a failure")
		if !clock.Sleep(ctx, r.clock, r.backoff) {
			return
		}
	}

	r.logger.Debug().Msg("Starting Modbus poll")
	points, err := r.poll(ctx)
	switch {
	case err == nil:
		r.handlePollSuccess(len(points))
	case ctx.Err() != nil:
		// Points read before the shutdown are still delivered below.
		r.logger.Debug().Err(err).Msg("Modbus poll cancelled during shutdown")
	default:
		r.handlePollError(err)
	}
	send(dataCh, points)
}

// send delivers the points of a finished poll as one batch, also during shutdown: the
// router reads until the reader closes the channel, so data that was already read is
// never lost. A poll without points sends nothing.
func send(dataCh chan<- []datasource.DataPoint, points []datasource.DataPoint) {
	if len(points) > 0 {
		dataCh <- points
	}
}

// poll connects if needed and reads all points in the configured mode. Points that
// were read are returned even when err is set (direct mode).
func (r *Reader) poll(ctx context.Context) ([]datasource.DataPoint, error) {
	if r.client == nil {
		client, err := r.dialer.Dial()
		if err != nil {
			return nil, fmt.Errorf("connect: %w", err)
		}
		r.client = client
		r.logger.Info().Msg("Connected to Modbus device")
	}

	if r.settings.RangeMode {
		return r.readRangeMode(ctx)
	}
	return r.readDirectMode(ctx)
}

// handlePollError logs the error, grows the backoff and drops a broken connection, so
// the next poll dials again.
func (r *Reader) handlePollError(err error) {
	r.failCount++
	r.logger.Error().Err(err).Int("failure_count", r.failCount).Msg("Modbus poll failed")

	r.backoff = min(max(r.backoff*backoffFactor, initialBackoff), maxBackoff)

	if r.client != nil && isConnectionError(err) {
		r.logger.Warn().Msg("Connection lost, reconnecting on the next poll")
		r.disconnect()
	}
}

// handlePollSuccess resets the backoff and logs a recovery at info.
func (r *Reader) handlePollSuccess(count int) {
	if r.failCount > 0 {
		r.logger.Info().Int("count", count).Msg("Modbus poll recovered from previous error")
	} else {
		r.logger.Debug().Int("count", count).Msg("Modbus poll completed")
	}
	r.failCount = 0
	r.backoff = 0
}

// disconnect closes the current connection, if any.
func (r *Reader) disconnect() {
	if r.client == nil {
		return
	}
	if err := r.client.Close(); err != nil {
		r.logger.Debug().Err(err).Msg("Closing Modbus connection failed")
	}
	r.client = nil
}

// isConnectionError reports whether err means the connection is broken. Timeouts
// (modbus.ErrRequestTimedOut) and Modbus exceptions keep the connection.
func isConnectionError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}
