package modbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simonvetter/modbus"

	"github.com/dombyte/datalogger/internal/datasource"
)

// maxRegistersPerRead is the Modbus protocol limit for one read request.
const maxRegistersPerRead = 125

// readDirectMode reads every point with its own request, one after the other. A
// failed point is logged and skipped. On shutdown the read in flight is abandoned and
// the rest skipped; after a connection error and after maxTimeoutsInRow timeouts in a
// row the rest is skipped too, so a device that stopped answering does not hold the
// poll for points × timeout. The returned error is the first connection error, or the
// first error if no point could be read. All points carry the receive time of the last
// successful read.
func (r *Reader) readDirectMode(ctx context.Context) ([]datasource.DataPoint, error) {
	var poll directPoll
	for _, p := range r.settings.Points {
		if poll.skip(ctx) {
			break
		}
		dp, err := r.readPoint(ctx, p)
		if err != nil && ctx.Err() == nil {
			r.logger.Warn().Str("point", p.Name).Err(err).Msg("Failed to read point")
		}
		poll.add(dp, err)
	}
	return poll.result()
}

// maxTimeoutsInRow is the number of timeouts in a row after which a direct-mode poll
// skips its remaining reads: one timeout can be a register the device ignores, two
// mean the device does not answer.
const maxTimeoutsInRow = 2

// directPoll collects the results of one direct-mode poll.
type directPoll struct {
	results       []datasource.DataPoint
	firstErr      error
	connErr       error
	timeoutsInRow int
}

// skip reports whether the remaining reads are skipped.
func (d *directPoll) skip(ctx context.Context) bool {
	return ctx.Err() != nil || d.connErr != nil || d.timeoutsInRow >= maxTimeoutsInRow
}

// add records the outcome of one read.
func (d *directPoll) add(dp datasource.DataPoint, err error) {
	switch {
	case err == nil:
		d.results = append(d.results, dp)
		d.timeoutsInRow = 0
		return
	case errors.Is(err, modbus.ErrRequestTimedOut):
		d.timeoutsInRow++
	case isConnectionError(err) && d.connErr == nil:
		d.connErr = err
	}
	if d.firstErr == nil {
		d.firstErr = err
	}
}

// result returns the points read, stamped with one time, and the error of the poll.
func (d *directPoll) result() ([]datasource.DataPoint, error) {
	if len(d.results) > 0 {
		// The reads ran in order, so the last result holds the latest receive time.
		stamp(d.results, d.results[len(d.results)-1].Timestamp)
	}
	if d.connErr != nil {
		return d.results, d.connErr
	}
	if len(d.results) == 0 {
		return nil, d.firstErr
	}
	return d.results, nil
}

// readPoint reads, decodes and scales one point, stamped with the receive time.
func (r *Reader) readPoint(ctx context.Context, p Point) (datasource.DataPoint, error) {
	values, err := r.client.ReadRegisters(ctx, p.Register, registerCount(p), r.regType)
	if err != nil {
		return datasource.DataPoint{}, fmt.Errorf("register %d, count %d: %w",
			p.Register, registerCount(p), err)
	}
	return r.dataPoint(p, values, r.clock.Now().UTC())
}

// readRangeMode reads all ranges, then decodes every point from the collected
// registers, stamped with the receive time of the last chunk. Any failed chunk fails
// the whole poll.
func (r *Reader) readRangeMode(ctx context.Context) ([]datasource.DataPoint, error) {
	for i, rng := range r.ranges {
		// After a failed chunk, or on shutdown, no further chunk is read: the poll fails
		// as a whole anyway.
		if err := r.readRange(ctx, rng, r.registers[i]); err != nil {
			return nil, err
		}
	}
	received := r.clock.Now().UTC()

	results := make([]datasource.DataPoint, 0, len(r.settings.Points))
	for i, p := range r.settings.Points {
		s := r.slots[i]
		values := r.registers[s.rng][s.offset : s.offset+int(registerCount(p))]
		dp, err := r.dataPoint(p, values, received)
		if err != nil {
			r.logger.Warn().Str("point", p.Name).Err(err).Msg("Failed to decode point")
			continue
		}
		results = append(results, dp)
	}
	return results, nil
}

// readRange reads one range in chunks of at most maxRegistersPerRead into registers
// (the buffer of the range); it stops at the first failed chunk or when ctx ends.
func (r *Reader) readRange(ctx context.Context, rng Range, registers []uint16) error {
	// int arithmetic: a range ending at 65535 would overflow uint16.
	for start := int(rng.Start); start <= int(rng.End); start += maxRegistersPerRead {
		if ctx.Err() != nil {
			return fmt.Errorf("poll cancelled: %w", ctx.Err())
		}
		count := min(int(rng.End)-start+1, maxRegistersPerRead)

		values, err := r.client.ReadRegisters(ctx, uint16(start), uint16(count), r.regType)
		if err != nil {
			return fmt.Errorf("range %d-%d, chunk at %d: %w", rng.Start, rng.End, start, err)
		}
		if len(values) != count {
			// The poll would decode registers of the previous poll as new readings.
			return fmt.Errorf("range %d-%d, chunk at %d: got %d registers, want %d",
				rng.Start, rng.End, start, len(values), count)
		}
		copy(registers[start-int(rng.Start):], values)
	}
	return nil
}

// stamp gives every point of a poll the same timestamp, so the poll is one snapshot
// (one row in InfluxDB).
func stamp(points []datasource.DataPoint, timestamp time.Time) {
	for i := range points {
		points[i].Timestamp = timestamp
	}
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
