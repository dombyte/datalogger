package modbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/simonvetter/modbus"

	"github.com/dombyte/datalogger/internal/datasource"
)

// maxRegistersPerRead is the Modbus protocol limit for one read request.
const maxRegistersPerRead = 125

// registerValue is a register with the time its chunk was received.
type registerValue struct {
	value    uint16
	received time.Time
}

// readDirectMode reads every point with its own request, up to Parallelism at a time.
// A failed point is logged and skipped. Reads not yet started are skipped on shutdown,
// after a connection error and after maxTimeoutsInRow timeouts in a row, so a device
// that stopped answering does not hold the poll for points × timeout. The returned
// error is the first connection error, or the first error if no point could be read.
func (r *Reader) readDirectMode(ctx context.Context) ([]datasource.DataPoint, error) {
	var (
		wg   sync.WaitGroup
		poll directPoll
	)
	sem := make(chan struct{}, r.settings.Parallelism)

	for _, p := range r.settings.Points {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			if poll.skip(ctx) {
				return
			}
			dp, err := r.readPoint(p)
			if err != nil {
				r.logger.Warn().Str("point", p.Name).Err(err).Msg("Failed to read point")
			}
			poll.add(dp, err)
		})
	}
	wg.Wait()
	return poll.result()
}

// maxTimeoutsInRow is the number of timeouts in a row after which a direct-mode poll
// skips its remaining reads: one timeout can be a register the device ignores, two
// mean the device does not answer.
const maxTimeoutsInRow = 2

// directPoll collects the results of one direct-mode poll; its methods are safe for
// the concurrent reads.
type directPoll struct {
	mu            sync.Mutex
	results       []datasource.DataPoint
	firstErr      error
	connErr       error
	timeoutsInRow int
}

// skip reports whether the remaining reads are skipped.
func (d *directPoll) skip(ctx context.Context) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return ctx.Err() != nil || d.connErr != nil || d.timeoutsInRow >= maxTimeoutsInRow
}

// add records the outcome of one read.
func (d *directPoll) add(dp datasource.DataPoint, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case err == nil:
		d.results = append(d.results, dp)
		d.timeoutsInRow = 0
		return
	case errors.Is(err, modbus.ErrRequestTimedOut):
		d.timeoutsInRow++
	case isConnectionError(err):
		d.connErr = cmp(d.connErr, err)
	}
	d.firstErr = cmp(d.firstErr, err)
}

// result returns the points read and the error of the poll.
func (d *directPoll) result() ([]datasource.DataPoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connErr != nil {
		return d.results, d.connErr
	}
	if len(d.results) == 0 {
		return nil, d.firstErr
	}
	return d.results, nil
}

// cmp returns current if it is set, else err.
func cmp(current, err error) error {
	if current != nil {
		return current
	}
	return err
}

// readPoint reads, decodes and scales one point, stamped with the receive time.
func (r *Reader) readPoint(p Point) (datasource.DataPoint, error) {
	values, err := r.client.ReadRegisters(p.Register, registerCount(p), r.regType)
	if err != nil {
		return datasource.DataPoint{}, fmt.Errorf("register %d, count %d: %w",
			p.Register, registerCount(p), err)
	}
	return r.dataPoint(p, values, r.clock.Now().UTC())
}

// readRangeMode reads all ranges, then decodes every point from the collected
// registers. Any failed chunk fails the whole poll.
func (r *Reader) readRangeMode(ctx context.Context) ([]datasource.DataPoint, error) {
	registers, err := r.readAllRanges(ctx)
	if err != nil {
		return nil, err
	}

	var results []datasource.DataPoint
	for _, p := range r.settings.Points {
		values, received, err := pointRegisters(p, registers)
		if err == nil {
			var dp datasource.DataPoint
			if dp, err = r.dataPoint(p, values, received); err == nil {
				results = append(results, dp)
				continue
			}
		}
		r.logger.Warn().Str("point", p.Name).Err(err).Msg("Failed to decode point")
	}
	return results, nil
}

// readAllRanges reads the ranges, up to Parallelism at a time, and returns the first
// error of any chunk. After a failed chunk, or on shutdown, no further chunk is read:
// the poll fails as a whole anyway.
func (r *Reader) readAllRanges(ctx context.Context) (map[uint16]registerValue, error) {
	var wg sync.WaitGroup
	rp := rangePoll{registers: make(map[uint16]registerValue)}
	sem := make(chan struct{}, r.settings.Parallelism)

	for _, rng := range r.ranges {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			rp.fail(r.readRange(ctx, rng, &rp))
		})
	}
	wg.Wait()
	return rp.registers, rp.err
}

// rangePoll collects the registers of one range-mode poll; its methods are safe for
// the concurrent reads.
type rangePoll struct {
	mu        sync.Mutex
	registers map[uint16]registerValue
	err       error
}

// failed reports whether a chunk failed or ctx ended, so no further chunk is read.
func (rp *rangePoll) failed(ctx context.Context) error {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.err == nil && ctx.Err() != nil {
		rp.err = fmt.Errorf("poll cancelled: %w", ctx.Err())
	}
	return rp.err
}

// fail records err (the first one wins); nil is ignored.
func (rp *rangePoll) fail(err error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.err = cmp(rp.err, err)
}

// store records the registers of one chunk.
func (rp *rangePoll) store(start int, values []uint16, received time.Time) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	for i, v := range values {
		rp.registers[uint16(start+i)] = registerValue{value: v, received: received}
	}
}

// readRange reads one range in chunks of at most maxRegistersPerRead and stores the
// registers; it stops at the first failed chunk, or when another one failed.
func (r *Reader) readRange(ctx context.Context, rng Range, rp *rangePoll) error {
	// int arithmetic: a range ending at 65535 would overflow uint16.
	for start := int(rng.Start); start <= int(rng.End); start += maxRegistersPerRead {
		if rp.failed(ctx) != nil {
			return nil // already recorded
		}
		count := min(int(rng.End)-start+1, maxRegistersPerRead)

		values, err := r.client.ReadRegisters(uint16(start), uint16(count), r.regType)
		if err != nil {
			return fmt.Errorf("range %d-%d, chunk at %d: %w", rng.Start, rng.End, start, err)
		}
		rp.store(start, values, r.clock.Now().UTC())
	}
	return nil
}

// pointRegisters collects the registers of a point; the timestamp is the receive time
// of its first register.
func pointRegisters(
	p Point,
	registers map[uint16]registerValue,
) ([]uint16, time.Time, error) {
	values := make([]uint16, registerCount(p))
	var received time.Time
	for i := range registerCount(p) {
		rv, ok := registers[p.Register+i]
		if !ok {
			return nil, time.Time{}, fmt.Errorf("register %d not in range data", p.Register+i)
		}
		if i == 0 {
			received = rv.received
		}
		values[i] = rv.value
	}
	return values, received, nil
}

// dataPoint decodes and scales the registers of a point.
func (r *Reader) dataPoint(
	p Point,
	values []uint16,
	timestamp time.Time,
) (datasource.DataPoint, error) {
	raw, err := decodeValue(values, p.Type)
	if err != nil {
		return datasource.DataPoint{}, fmt.Errorf("decode: %w", err)
	}
	return datasource.DataPoint{
		DeviceName: r.settings.Name,
		PointName:  p.Name,
		Value:      raw*p.Scale + p.Offset,
		Timestamp:  timestamp,
		Unit:       p.Unit,
	}, nil
}
