package modbus

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"
)

// ErrBusStopped is returned by the clients of a Bus that has stopped.
var ErrBusStopped = errors.New("modbus: serial bus stopped")

// BusClient is an open serial port that reads from any slave on it.
type BusClient interface {
	ReadSlave(slaveID uint8, addr, quantity uint16, regType modbus.RegType) ([]uint16, error)
	Close() error
}

// BusOpener opens the serial port of a Bus; *LibDialer implements it.
type BusOpener interface {
	Open() (BusClient, error)
}

// BusDeps are the dependencies of a Bus; all are required.
type BusDeps struct {
	Port   string // the serial port address, for logs
	Opener BusOpener
	Log    zerolog.Logger
}

// Bus shares one serial port between the devices on it (an RS-485 bus). Its goroutine
// is the only code that uses the port: it runs the requests of every device one after
// the other, so their frames never mix. It opens the port on the first request and
// reopens it after a connection error. Each device reads through Dialer(slaveID).
type Bus struct {
	opener   BusOpener
	logger   zerolog.Logger
	requests chan request
	done     chan struct{}
	client   BusClient // nil while closed; used only by the run goroutine
}

// request is one call to the bus: open the port (open) or read registers of a slave.
type request struct {
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

// NewBus creates a Bus; it does not open the port.
func NewBus(d BusDeps) (*Bus, error) {
	if d.Opener == nil {
		return nil, fmt.Errorf("%w: Opener", ErrMissingDependency)
	}
	return &Bus{
		opener:   d.Opener,
		logger:   d.Log.With().Str("component", "modbus").Str("port", d.Port).Logger(),
		requests: make(chan request),
		done:     make(chan struct{}),
	}, nil
}

// Start runs the bus until ctx is cancelled; then it closes the port and the returned
// channel. Start it before the readers that use it and stop it after them.
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

// handle opens the port if needed and runs the request. A connection error closes the
// port, so the next request opens it again.
func (b *Bus) handle(req request) response {
	if b.client == nil {
		client, err := b.opener.Open()
		if err != nil {
			return response{err: fmt.Errorf("open port: %w", err)}
		}
		b.client = client
		b.logger.Info().Msg("Opened serial port")
	}
	if req.open {
		return response{}
	}
	values, err := b.client.ReadSlave(req.slaveID, req.addr, req.quantity, req.regType)
	if err != nil && isConnectionError(err) {
		b.logger.Warn().Err(err).Msg("Serial port lost, reopening on the next request")
		b.disconnect()
	}
	return response{values: values, err: err}
}

// disconnect closes the port, if it is open.
func (b *Bus) disconnect() {
	if b.client == nil {
		return
	}
	if err := b.client.Close(); err != nil {
		b.logger.Debug().Err(err).Msg("Closing serial port failed")
	}
	b.client = nil
}

// call sends a request to the bus goroutine and waits for its answer.
func (b *Bus) call(req request) ([]uint16, error) {
	reply := make(chan response, 1)
	req.reply = reply
	select {
	case b.requests <- req:
	case <-b.done:
		return nil, ErrBusStopped
	}
	resp := <-reply // the bus answers every request it took
	return resp.values, resp.err
}

// Dialer returns the Dialer of one slave on this bus, for its Reader.
func (b *Bus) Dialer(slaveID uint8) *BusDialer {
	return &BusDialer{bus: b, slaveID: slaveID}
}

// BusDialer connects a Reader to its slave on a shared Bus.
type BusDialer struct {
	bus     *Bus
	slaveID uint8
}

// Dial opens the port if it is not open yet; the connection stays shared.
func (d *BusDialer) Dial() (Client, error) {
	if _, err := d.bus.call(request{open: true}); err != nil {
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
	addr, quantity uint16,
	regType modbus.RegType,
) ([]uint16, error) {
	return c.bus.call(request{
		slaveID: c.slaveID, addr: addr, quantity: quantity, regType: regType,
	})
}

// Close does nothing: the bus owns the port and reopens it after a connection error.
func (c slaveClient) Close() error {
	return nil
}
