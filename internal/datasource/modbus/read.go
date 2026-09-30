package modbus

import (
	"fmt"
	"sync"
	"time"

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
// A failed point is logged and skipped. The returned error is the first connection
// error, or the first error if no point could be read at all.
func (r *Reader) readDirectMode() ([]datasource.DataPoint, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		results  []datasource.DataPoint
		firstErr error
		connErr  error
	)
	sem := make(chan struct{}, r.settings.Parallelism)

	for _, p := range r.settings.Points {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			dp, err := r.readPoint(p)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				r.logger.Warn().Str("point", p.Name).Err(err).Msg("Failed to read point")
				firstErr = cmp(firstErr, err)
				if isConnectionError(err) {
					connErr = cmp(connErr, err)
				}
				return
			}
			results = append(results, dp)
		})
	}
	wg.Wait()

	if connErr != nil {
		return results, connErr
	}
	if len(results) == 0 {
		return nil, firstErr
	}
	return results, nil
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
func (r *Reader) readRangeMode() ([]datasource.DataPoint, error) {
	registers, err := r.readAllRanges()
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
// error of any chunk.
func (r *Reader) readAllRanges() (map[uint16]registerValue, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
	)
	registers := make(map[uint16]registerValue)
	sem := make(chan struct{}, r.settings.Parallelism)

	for _, rng := range r.ranges {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := r.readRange(rng, registers, &mu); err != nil {
				mu.Lock()
				firstErr = cmp(firstErr, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return registers, firstErr
}

// readRange reads one range in chunks of at most maxRegistersPerRead and stores the
// registers; it stops at the first failed chunk.
func (r *Reader) readRange(rng Range, registers map[uint16]registerValue, mu *sync.Mutex) error {
	// int arithmetic: a range ending at 65535 would overflow uint16.
	for start := int(rng.Start); start <= int(rng.End); start += maxRegistersPerRead {
		count := min(int(rng.End)-start+1, maxRegistersPerRead)

		values, err := r.client.ReadRegisters(uint16(start), uint16(count), r.regType)
		if err != nil {
			return fmt.Errorf("range %d-%d, chunk at %d: %w", rng.Start, rng.End, start, err)
		}
		received := r.clock.Now().UTC()

		mu.Lock()
		for i, v := range values {
			registers[uint16(start+i)] = registerValue{value: v, received: received}
		}
		mu.Unlock()
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
