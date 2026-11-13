package modbus

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"
)

// timeoutsBeforeRedial is the number of TCP requests in a row that time out after which
// the connection is closed and opened again: a device or gateway that went away without
// a reset (power loss, a NAT entry that expired) leaves a connection that the kernel
// keeps for many minutes, and only a new one can reach it when it is back.
const timeoutsBeforeRedial = 3

var (
	// ErrBusStopped is returned by the clients of a Bus that has stopped.
	ErrBusStopped = errors.New("modbus: bus stopped")

	// errOpen marks a connection that could not be opened; it is a connection error, so
	// a poll does not try to open it again for every point.
	errOpen = errors.New("open connection")
)

// BusClient is an open connection that reads from any slave on it.
type BusClient interface {
	ReadSlave(slaveID uint8, addr, quantity uint16, regType modbus.RegType) ([]uint16, error)
	Close() error
}

// BusOpener opens the connection of a Bus; *LibDialer implements it.
type BusOpener interface {
	Open() (BusClient, error)
}

// BusDeps are the dependencies of a Bus; all are required.
type BusDeps struct {
	Address string // the connection address, for logs
	// Serial is set for RTU frames (a serial port or an RTU-over-TCP gateway): they carry
	// no transaction ID, so the connection is reopened after a request that may leave a
	// late answer on the line.
	Serial bool
	Opener BusOpener
	Log    zerolog.Logger
}

// Bus owns one Modbus connection: a serial port shared by the devices on it (an RS-485
// bus), or the TCP connection of one device. Its goroutine is the only code that uses
// the connection: it runs the requests one after the other, so frames never mix, and a
// device whose context ends stops waiting for its request at once (the library cannot
// cancel a request in flight; the bus finishes it alone). It opens the connection on the
// first request and reopens it after a connection error; a serial port also after a
// timeout or an answer of another slave, which discards what arrives late. Each device
// reads through Dialer(slaveID).
type Bus struct {
	opener   BusOpener
	serial   bool
	logger   zerolog.Logger
	requests chan request
	done     chan struct{}

	// Used only by the run goroutine.
	client   BusClient // nil while closed
	resync   bool      // the port was closed to discard a late answer, not because it broke
	timeouts int       // TCP requests in a row that timed out
}

// request is one call to the bus: open the connection (open) or read registers of a
// slave. ctx is the caller's: a request whose caller has gone is skipped.
type request struct {
	ctx      context.Context // the caller's, so the bus can skip a request nobody waits for
	open     bool
	slaveID  uint8
	addr     uint16
	quantity uint16
	regType  modbus.RegType
	reply    chan<- response // buffered, so the bus never waits for the caller
}

// response is the answer to a request.
type response struct {
	values []uint16
	err    error
}

// NewBus creates a Bus; it does not connect.
func NewBus(d BusDeps) (*Bus, error) {
	if d.Opener == nil {
		return nil, fmt.Errorf("%w: Opener", ErrMissingDependency)
	}
	return &Bus{
		opener:   d.Opener,
		serial:   d.Serial,
		logger:   d.Log.With().Str("component", "modbus").Str("address", d.Address).Logger(),
		requests: make(chan request),
		done:     make(chan struct{}),
	}, nil
}

// Start runs the bus until ctx is cancelled; then it finishes the request in flight,
// closes the connection and the returned channel. Start it before the readers that use
// it and stop it after them.
func (b *Bus) Start(ctx context.Context) <-chan struct{} {
	go b.run(ctx)
	return b.done
}

// run serves the requests one at a time.
func (b *Bus) run(ctx context.Context) {
	defer close(b.done)
	defer b.disconnect()
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-b.requests:
			req.reply <- b.handle(req)
		}
	}
}

// handle opens the connection if needed and runs the request. A connection error
// closes the connection, so the next request opens it again.
func (b *Bus) handle(req request) response {
	if err := req.ctx.Err(); err != nil {
		return response{err: err} // nobody waits for the answer
	}
	if err := b.connect(); err != nil {
		return response{err: err}
	}
	if req.open {
		return response{}
	}
	values, err := b.client.ReadSlave(req.slaveID, req.addr, req.quantity, req.regType)
	b.afterRead(err)
	return response{values: values, err: err}
}

// connect opens the connection, if it is closed.
func (b *Bus) connect() error {
	if b.client != nil {
		return nil
	}
	resync := b.resync
	b.resync = false
	client, err := b.opener.Open()
	if err != nil {
		return fmt.Errorf("%w: %w", errOpen, err)
	}
	b.client = client
	level := zerolog.InfoLevel
	if resync {
		level = zerolog.DebugLevel // a reopen per timeout is no news
	}
	b.logger.WithLevel(level).Msg("Opened Modbus connection")
	return nil
}

// afterRead closes the connection after a read error that makes it unusable: a
// connection error, and on a serial port an error that can leave an answer on the line.
func (b *Bus) afterRead(err error) {
	if !errors.Is(err, modbus.ErrRequestTimedOut) {
		b.timeouts = 0
	}
	switch {
	case err == nil:
	case isConnectionError(err):
		b.logger.Warn().Err(err).Msg("Modbus connection lost, reopening on the next request")
		b.disconnect()
	case b.serial && leavesAnswerOnLine(err):
		b.logger.Debug().Err(err).Msg("Reopening serial port to discard a late answer")
		b.disconnect()
		b.resync = true
	case errors.Is(err, modbus.ErrRequestTimedOut):
		b.tcpTimeout()
	}
}

// tcpTimeout counts a timeout on a TCP connection and redials after
// timeoutsBeforeRedial in a row.
func (b *Bus) tcpTimeout() {
	b.timeouts++
	if b.timeouts < timeoutsBeforeRedial {
		return
	}
	b.logger.Warn().Int("timeouts", b.timeouts).
		Msg("No answer to Modbus requests in a row, reconnecting on the next request")
	b.timeouts = 0
	b.disconnect()
}

// leavesAnswerOnLine reports whether the answer to a failed request may still arrive:
// after a timeout the slave may answer late, and after an answer of another slave
// (the late answer to an earlier request) the one asked for is still to come. A serial
// frame has no transaction ID, so the next request with the same slave, function and
// length would take that answer for its own, and every later one the answer before it.
// A timeout is also all a port that went away (an unplugged USB adapter) may report.
func leavesAnswerOnLine(err error) bool {
	return errors.Is(err, modbus.ErrRequestTimedOut) || errors.Is(err, modbus.ErrBadUnitId)
}

// disconnect closes the connection, if it is open.
func (b *Bus) disconnect() {
	if b.client == nil {
		return
	}
	if err := b.client.Close(); err != nil {
		b.logger.Debug().Err(err).Msg("Closing Modbus connection failed")
	}
	b.client = nil
}

// call sends a request to the bus goroutine and waits for its answer, or until ctx
// ends: then the bus skips the request or finishes it without the caller.
func (b *Bus) call(ctx context.Context, req request) ([]uint16, error) {
	reply := make(chan response, 1)
	req.ctx, req.reply = ctx, reply
	select {
	case b.requests <- req:
	case <-b.done:
		return nil, ErrBusStopped
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for bus: %w", ctx.Err())
	}
	select {
	case resp := <-reply:
		return resp.values, resp.err
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for reply: %w", ctx.Err())
	}
}

// Dialer returns the Dialer of one slave on this bus, for its Reader.
func (b *Bus) Dialer(slaveID uint8) *BusDialer {
	return &BusDialer{bus: b, slaveID: slaveID}
}

// BusDialer connects a Reader to its slave on a Bus.
type BusDialer struct {
	bus     *Bus
	slaveID uint8
}

// Dial opens the connection if it is not open yet; the connection stays with the bus.
func (d *BusDialer) Dial(ctx context.Context) (Client, error) {
	if _, err := d.bus.call(ctx, request{open: true}); err != nil {
		return nil, err
	}
	return slaveClient{bus: d.bus, slaveID: d.slaveID}, nil
}

// slaveClient reads the registers of one slave through the bus.
type slaveClient struct {
	bus     *Bus
	slaveID uint8
}

// ReadRegisters reads registers of the slave, after the requests queued before it.
func (c slaveClient) ReadRegisters(
	ctx context.Context,
	addr, quantity uint16,
	regType modbus.RegType,
) ([]uint16, error) {
	return c.bus.call(ctx, request{
		slaveID: c.slaveID, addr: addr, quantity: quantity, regType: regType,
	})
}

// Close does nothing: the bus owns the connection and reopens it after a connection
// error.
func (c slaveClient) Close() error {
	return nil
}
