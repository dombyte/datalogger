package modbus

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"
)

// ErrBusStopped is returned by the clients of a Bus that has stopped.
var ErrBusStopped = errors.New("modbus: bus stopped")

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
	Opener  BusOpener
	Log     zerolog.Logger
}

// Bus owns one Modbus connection: a serial port shared by the devices on it (an RS-485
// bus), or the TCP connection of one device. Its goroutine is the only code that uses
// the connection: it runs the requests one after the other, so frames never mix, and a
// device whose context ends stops waiting for its request at once (the library cannot
// cancel a request in flight; the bus finishes it alone). It opens the connection on the
// first request and reopens it after a connection error. Each device reads through
// Dialer(slaveID).
type Bus struct {
	opener   BusOpener
	logger   zerolog.Logger
	requests chan request
	done     chan struct{}
	client   BusClient // nil while closed; used only by the run goroutine
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
	if b.client == nil {
		client, err := b.opener.Open()
		if err != nil {
			return response{err: fmt.Errorf("open connection: %w", err)}
		}
		b.client = client
		b.logger.Info().Msg("Opened Modbus connection")
	}
	if req.open {
		return response{}
	}
	values, err := b.client.ReadSlave(req.slaveID, req.addr, req.quantity, req.regType)
	if err != nil && isConnectionError(err) {
		b.logger.Warn().Err(err).Msg("Modbus connection lost, reopening on the next request")
		b.disconnect()
	}
	return response{values: values, err: err}
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
