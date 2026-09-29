// Package modbus provides Modbus TCP/RTU reading functionality.
package modbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
)

const (
	// Modbus function codes that select the register type.
	fcHoldingRegisters = 3
	fcInputRegisters   = 4

	// maxRegistersPerRead is the Modbus protocol limit for one read request.
	maxRegistersPerRead = 125

	// 32-bit types span two 16-bit registers, high word first.
	registersPer32Bit = 2
	bitsPerRegister   = 16

	// Backoff after failed polls: starts at initialBackoff and doubles up to maxBackoff.
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

// errNotConnected is returned when a poll runs without an open client.
var errNotConnected = errors.New("modbus client is not connected")

// modbusLoggerAdapter adapts zerolog.Logger to the standard log.Logger interface
// by wrapping it in a custom writer
type modbusLoggerAdapter struct {
	logger *zerolog.Logger
}

func (a *modbusLoggerAdapter) Write(p []byte) (n int, err error) {
	// Trim newline that log.Logger adds
	msg := string(p)
	if len(msg) > 0 && msg[len(msg)-1] == '\n' {
		msg = msg[:len(msg)-1]
	}
	a.logger.Debug().Msg(msg)
	return len(p), nil
}

// newModbusLogger creates a *log.Logger that writes to zerolog
func newModbusLogger(zl *zerolog.Logger) *log.Logger {
	return log.New(&modbusLoggerAdapter{logger: zl}, "", 0)
}

// Reader reads data from a Modbus device (TCP or RTU).
type Reader struct {
	config      config.Device
	logger      zerolog.Logger
	client      *modbus.ModbusClient
	points      []config.Point
	ranges      []Range
	regType     modbus.RegType
	failCount   int           // Track consecutive failures for backoff
	lastError   error         // Last error encountered
	backoffWait time.Duration // Current backoff duration
}

// New creates a new Reader.
func New(deviceConfig config.Device, logger *zerolog.Logger) (*Reader, error) {
	r := &Reader{
		config: deviceConfig,
		logger: logger.With().
			Str("datasource", "modbus").
			Str("device", deviceConfig.Name).
			Logger(),
		points:  deviceConfig.Points,
		regType: getRegType(deviceConfig.Points),
	}

	// Parse ranges from config
	var err error
	r.ranges, err = parseRanges(deviceConfig.DeviceSpecific.Modbus.Ranges)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ranges: %w", err)
	}

	// Validate all point addresses are covered
	if err := r.validateAddresses(); err != nil {
		return nil, err
	}

	// Create Modbus client and set slave ID
	if err := r.createClient(); err != nil {
		return nil, err
	}
	return r, nil
}

// getRegType determines the Modbus register type from the function code.
// Function code 3 = Holding Registers, 4 = Input Registers.
func getRegType(points []config.Point) modbus.RegType {
	for _, point := range points {
		if point.FunctionCode == fcHoldingRegisters {
			return modbus.HOLDING_REGISTER
		}
		if point.FunctionCode == fcInputRegisters {
			return modbus.INPUT_REGISTER
		}
	}
	// Default to Holding Registers (function code 3)
	return modbus.HOLDING_REGISTER
}

// createClient creates and opens the Modbus client.
func (r *Reader) createClient() error {
	client, err := r.newClient()
	if err != nil {
		return err
	}
	r.client = client
	return nil
}

// newClient creates a Modbus client, opens it and sets the slave ID.
func (r *Reader) newClient() (*modbus.ModbusClient, error) {
	modbusConfig := r.config.DeviceSpecific.Modbus

	clientConfig := &modbus.ClientConfiguration{
		URL:      modbusConfig.Address,
		Speed:    uint(modbusConfig.Speed),
		DataBits: uint(modbusConfig.DataBits),
		Parity:   parseParity(modbusConfig.Parity),
		StopBits: uint(modbusConfig.StopBits),
		Timeout:  r.config.Timeout,
		Logger:   newModbusLogger(&r.logger),
	}

	client, err := modbus.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Modbus client: %w", err)
	}

	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open Modbus connection: %w", err)
	}

	if err := client.SetUnitId(modbusConfig.SlaveID); err != nil {
		r.closeClient(client)
		return nil, fmt.Errorf("failed to set unit ID: %w", err)
	}

	return client, nil
}

// closeClient closes a client; a close error is only logged because the client is
// discarded either way.
func (r *Reader) closeClient(client *modbus.ModbusClient) {
	if err := client.Close(); err != nil {
		r.logger.Debug().Err(err).Msg("Closing Modbus client failed")
	}
}

// shouldReconnect reports whether err means the connection is broken. Timeouts
// (modbus.ErrRequestTimedOut) and Modbus exceptions keep the connection.
func (r *Reader) shouldReconnect(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// reconnect opens a new connection and replaces the current one.
func (r *Reader) reconnect() error {
	r.logger.Warn().Msg("Attempting to reconnect Modbus client")

	// Open the new client first, so r.client is never nil during normal operation.
	client, err := r.newClient()
	if err != nil {
		return err
	}

	if r.client != nil {
		r.closeClient(r.client)
	}

	r.client = client
	return nil
}

// parseParity converts the parity string to modbus parity constant.
func parseParity(parity string) uint {
	switch parity {
	case "N", "":
		return modbus.PARITY_NONE
	case "E":
		return modbus.PARITY_EVEN
	case "O":
		return modbus.PARITY_ODD
	default:
		return modbus.PARITY_NONE
	}
}

// validateAddresses checks that all point registers are covered by the configured
// ranges (range mode only; direct mode needs no ranges).
func (r *Reader) validateAddresses() error {
	if r.config.DeviceSpecific.Modbus.RegisterMode != "range" {
		return nil
	}

	for _, point := range r.points {
		if !r.isCovered(point) {
			return fmt.Errorf("point '%s' register range %d-%d not covered by any configured range",
				point.Name, point.Register, pointEnd(point))
		}
	}
	return nil
}

// isCovered reports whether one configured range holds all registers of the point.
func (r *Reader) isCovered(point config.Point) bool {
	end := pointEnd(point)
	for _, rng := range r.ranges {
		if point.Register >= rng.Start && end <= rng.End {
			return true
		}
	}
	return false
}

// pointEnd returns the last register of a point (count 0 means one register).
func pointEnd(point config.Point) uint16 {
	if point.Count == 0 {
		return point.Register
	}
	return point.Register + point.Count - 1
}

// Name returns the device name.
func (r *Reader) Name() string {
	return r.config.Name
}

// Validate validates the Modbus reader configuration.
func (r *Reader) Validate() error {
	// Configuration was already validated when creating the reader
	return nil
}

// Start starts the polling loop and returns channels for data, done, and errors.
func (r *Reader) Start(
	ctx context.Context,
) (<-chan datasource.DataPoint, <-chan struct{}, <-chan error) {
	dataCh := make(chan datasource.DataPoint)
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)

	go r.pollLoop(ctx, dataCh, doneCh, errCh)

	return dataCh, doneCh, errCh
}

// pollLoop runs the main polling loop for the Modbus device.
func (r *Reader) pollLoop(
	ctx context.Context,
	dataCh chan<- datasource.DataPoint,
	doneCh chan<- struct{},
	errCh chan<- error,
) {
	defer close(doneCh)

	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.handlePollTick(ctx, dataCh)
		}
	}
}

// handlePollTick handles a single poll tick event.
func (r *Reader) handlePollTick(ctx context.Context, dataCh chan<- datasource.DataPoint) {
	// Apply backoff wait if we had previous failures
	if r.backoffWait > 0 {
		r.logger.Warn().Dur("wait", r.backoffWait).Msg("Waiting before retry after a failure")
		time.Sleep(r.backoffWait)
	}

	r.logger.Debug().Msg("Starting Modbus poll")

	// Read all points (timestamps captured at data reception)
	points, err := r.readAllPoints(ctx)
	if err != nil {
		// Check if this is a context cancellation error (expected during shutdown)
		if ctx.Err() != nil {
			r.logger.Debug().Err(err).Msg("Modbus poll cancelled during shutdown")
		} else {
			r.handlePollError(err)
		}
		return
	}

	// Poll succeeded
	r.handlePollSuccess(points, dataCh)
}

// handlePollError handles an error from a poll attempt.
func (r *Reader) handlePollError(err error) {
	r.failCount++
	r.lastError = err

	// Log error with failure count
	r.logger.Error().
		Err(err).
		Int("failure_count", r.failCount).
		Msg("Failed to read Modbus points")

	// Apply exponential backoff: min(2^failCount * 100ms, 30s)
	r.applyBackoff()

	// Try to reconnect if connection-related error
	if r.shouldReconnect(err) {
		r.handleReconnect()
	}
}

// handlePollSuccess handles a successful poll attempt.
func (r *Reader) handlePollSuccess(
	points []datasource.DataPoint,
	dataCh chan<- datasource.DataPoint,
) {
	// Check if this is recovery from a previous error
	recoveredFromError := r.lastError != nil || r.failCount > 0

	// Reset failure tracking on success
	r.failCount = 0
	r.backoffWait = 0
	r.lastError = nil

	// Log at INFO level if recovering from an error, DEBUG otherwise
	if recoveredFromError {
		r.logger.Info().Int("count", len(points)).Msg("Modbus poll recovered from previous error")
	} else {
		r.logger.Debug().Int("count", len(points)).Msg("Modbus poll completed")
	}

	// Send each point to channel
	for _, dp := range points {
		dataCh <- dp
	}
}

// applyBackoff applies exponential backoff to the wait duration.
func (r *Reader) applyBackoff() {
	r.backoffWait *= backoffFactor
	if r.backoffWait == 0 {
		r.backoffWait = initialBackoff
	}
	if r.backoffWait > maxBackoff {
		r.backoffWait = maxBackoff
	}
}

// handleReconnect attempts to reconnect the Modbus client.
func (r *Reader) handleReconnect() {
	if reconnectErr := r.reconnect(); reconnectErr != nil {
		r.logger.Error().Err(reconnectErr).Msg("Modbus reconnection failed")
	} else {
		r.failCount = 0 // Reset on successful reconnect
		r.backoffWait = 0
		r.lastError = nil // Reset last error on successful reconnect
		r.logger.Info().Msg("Modbus reconnected successfully")
	}
}

// readAllPoints reads all configured points using the appropriate mode (direct or range).
// Timestamps are captured at data reception time within each mode.
func (r *Reader) readAllPoints(ctx context.Context) ([]datasource.DataPoint, error) {
	// Check if client is connected before attempting to read
	if r.client == nil {
		return nil, errNotConnected
	}

	modbusConfig := r.config.DeviceSpecific.Modbus

	if modbusConfig.RegisterMode == "range" {
		return r.readRangeMode(ctx)
	}
	return r.readDirectMode(ctx)
}

// readDirectMode reads each point directly (not using range reads).
// Each point captures its own timestamp when data is received.
func (r *Reader) readDirectMode(ctx context.Context) ([]datasource.DataPoint, error) {
	var results []datasource.DataPoint
	var mu sync.Mutex
	sem := make(chan struct{}, r.config.Parallelism) // Semaphore for concurrency limit

	var wg sync.WaitGroup

	for _, point := range r.points {
		wg.Add(1)
		go func(p config.Point) {
			defer wg.Done()

			// Acquire semaphore slot (blocks if at parallelism limit)
			sem <- struct{}{}
			defer func() { <-sem }()

			dp, err := r.readSinglePoint(p)
			if err != nil {
				r.logger.Warn().
					Str("point", p.Name).
					Err(err).
					Msg("Failed to read point")
				return
			}
			if dp != nil {
				mu.Lock()
				results = append(results, *dp)
				mu.Unlock()
			}
		}(point)
	}

	wg.Wait()
	return results, nil
}

// readSinglePoint reads a single point and returns a DataPoint or nil on error.
// Timestamp is captured when data is received from the device.
func (r *Reader) readSinglePoint(point config.Point) (*datasource.DataPoint, error) {
	// Get register values - support both Addresses array and Address+Count
	values, err := r.readRegistersForPoint(point)
	if err != nil {
		return nil, err
	}

	// Capture accurate timestamp when data was received from device
	// Use this instead of the poll start time for better accuracy
	// Use UTC to ensure consistency with InfluxDB expectations
	actualTimestamp := time.Now().UTC()

	return r.createDataPointFromValues(point, values, actualTimestamp)
}

// createDataPointFromValues creates a DataPoint from register values.
func (r *Reader) createDataPointFromValues(
	point config.Point,
	values []uint16,
	timestamp time.Time,
) (*datasource.DataPoint, error) {
	// Decode and scale the value
	value, err := decodeValue(values, point.Type)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	// Apply scale and offset
	scaled := r.applyScaleAndOffset(value, point)

	return &datasource.DataPoint{
		DeviceName: r.config.Name,
		PointName:  point.Name,
		Value:      scaled,
		Timestamp:  timestamp,
		Unit:       point.Unit,
	}, nil
}

// readRegistersForPoint reads registers for a point using Register+Count.
func (r *Reader) readRegistersForPoint(point config.Point) ([]uint16, error) {
	// Read registers using Register and Count
	if point.Count == 0 {
		point.Count = 1 // Default to reading 1 register
	}

	result, err := r.client.ReadRegisters(
		point.Register,
		point.Count,
		r.regType,
	)
	if err != nil {
		return nil, fmt.Errorf("register %d, count %d: %w", point.Register, point.Count, err)
	}
	return result, nil
}

// extractValuesFromRangeData extracts values and timestamp from range data for a point.
// Returns the timestamp of the first register in the point's range.
func (r *Reader) extractValuesFromRangeData(
	point config.Point,
	rangeData map[uint16]RangeValue,
) ([]uint16, time.Time, error) {
	// Extract values using Register and Count
	if point.Count == 0 {
		point.Count = 1
	}

	values := make([]uint16, point.Count)
	var pointTimestamp time.Time
	for i := range point.Count {
		addr := point.Register + i
		if val, ok := rangeData[addr]; ok {
			values[i] = val.Value
			// Use timestamp from first register for the entire point
			if i == 0 {
				pointTimestamp = val.Timestamp
			}
		} else {
			return nil, time.Time{}, fmt.Errorf("register %d not in range data", addr)
		}
	}
	return values, pointTimestamp, nil
}

// RangeValue stores a register value along with its reception timestamp
type RangeValue struct {
	Value     uint16
	Timestamp time.Time
}

// readRangeMode reads all ranges first, then decodes points from the range data.
// Each range captures its timestamp when data is received.
func (r *Reader) readRangeMode(ctx context.Context) ([]datasource.DataPoint, error) {
	// Read all ranges first (with parallelism)
	rangeData, err := r.readAllRanges(ctx)
	if err != nil {
		return nil, err
	}

	// Now decode points from the range data
	return r.decodePointsFromRangeData(rangeData)
}

// readAllRanges reads all configured ranges with parallelism and chunking.
func (r *Reader) readAllRanges(ctx context.Context) (map[uint16]RangeValue, error) {
	var mu sync.Mutex
	sem := make(chan struct{}, r.config.Parallelism)
	// Store both value and timestamp for each address
	rangeData := make(map[uint16]RangeValue)

	var wg sync.WaitGroup
	var readErr error

	for _, rng := range r.ranges {
		wg.Add(1)
		go func(rng Range) {
			defer wg.Done()

			// Acquire semaphore slot
			sem <- struct{}{}
			defer func() { <-sem }()

			r.readRangeChunked(rng, rangeData, &mu, &readErr)
		}(rng)
	}

	wg.Wait()

	return rangeData, readErr
}

// readRangeChunked reads a single range in chunks of at most maxRegistersPerRead.
// The first failed chunk is logged and recorded in readErr; the rest of the range is
// skipped.
func (r *Reader) readRangeChunked(
	rng Range,
	rangeData map[uint16]RangeValue,
	mu *sync.Mutex,
	readErr *error,
) {
	// int arithmetic: a range ending at 65535 would overflow uint16.
	for start := int(rng.Start); start <= int(rng.End); start += maxRegistersPerRead {
		chunkSize := min(int(rng.End)-start+1, maxRegistersPerRead)

		result, err := r.client.ReadRegisters(uint16(start), uint16(chunkSize), r.regType)
		if err != nil {
			r.logger.Warn().
				Int("start", start).
				Int("count", chunkSize).
				Err(err).
				Msg("Failed to read range")
			mu.Lock()
			if *readErr == nil {
				*readErr = err
			}
			mu.Unlock()
			return
		}

		storeChunk(uint16(start), result, time.Now().UTC(), rangeData, mu)
	}
}

// storeChunk stores the registers of one chunk with the time the chunk was received.
func storeChunk(
	start uint16,
	registers []uint16,
	receiveTime time.Time,
	rangeData map[uint16]RangeValue,
	mu *sync.Mutex,
) {
	mu.Lock()
	defer mu.Unlock()
	for i, val := range registers {
		rangeData[start+uint16(i)] = RangeValue{Value: val, Timestamp: receiveTime}
	}
}

// decodePointsFromRangeData decodes all points from the range data.
func (r *Reader) decodePointsFromRangeData(
	rangeData map[uint16]RangeValue,
) ([]datasource.DataPoint, error) {
	var results []datasource.DataPoint

	for _, point := range r.points {
		values, rangeTimestamp, err := r.extractValuesFromRangeData(point, rangeData)
		if err != nil {
			r.logger.Warn().
				Str("point", point.Name).
				Err(err).
				Msg("Failed to extract values from range data")
			continue
		}

		dp := r.decodeAndScalePoint(point, values, rangeTimestamp)
		if dp != nil {
			results = append(results, *dp)
		}
	}

	return results, nil
}

// decodeAndScalePoint decodes a single point value and applies scale/offset.
// Uses the common createDataPointFromValues and applyScaleAndOffset functions.
func (r *Reader) decodeAndScalePoint(
	point config.Point,
	values []uint16,
	timestamp time.Time,
) *datasource.DataPoint {
	// Use the common function to create DataPoint
	dp, err := r.createDataPointFromValues(point, values, timestamp)
	if err != nil {
		r.logger.Warn().
			Str("point", point.Name).
			Err(err).
			Msg("Failed to decode value")
		return nil
	}
	return dp
}

// applyScaleAndOffset returns value × scale + offset; unsupported types yield 0.
func (r *Reader) applyScaleAndOffset(value interface{}, point config.Point) float64 {
	raw, ok := toFloat64(value)
	if !ok {
		r.logger.Warn().
			Str("point", point.Name).
			Str("type", fmt.Sprintf("%T", value)).
			Msg("Unsupported type for scaling, skipping")
		return 0
	}
	return raw*point.Scale + point.Offset
}

// toFloat64 converts a decoded register value to float64 (bool: 1 or 0).
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int16:
		return float64(v), true
	case uint16:
		return float64(v), true
	case int32:
		return float64(v), true
	case uint32:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return boolToFloat64(value)
	}
}

// boolToFloat64 converts a bool to 1 or 0; ok is false if value is not a bool.
func boolToFloat64(value interface{}) (float64, bool) {
	b, ok := value.(bool)
	if !ok || !b {
		return 0, ok
	}
	return 1, true
}

// decodeValue decodes raw Modbus register values into the appropriate type.
func decodeValue(registers []uint16, dataType string) (interface{}, error) {
	if len(registers) == 0 {
		return nil, errors.New("no registers to decode")
	}

	switch dataType {
	case "int16":
		return int16(registers[0]), nil
	case "uint16":
		return registers[0], nil
	case "bool":
		return registers[0] != 0, nil
	case "int32", "uint32", "float32":
		return decode32(registers, dataType)
	default:
		return nil, fmt.Errorf("unknown data type: %s", dataType)
	}
}

// decode32 decodes a 32-bit type from two registers, high word first.
func decode32(registers []uint16, dataType string) (interface{}, error) {
	if len(registers) < registersPer32Bit {
		return nil, fmt.Errorf("%s needs 2 registers (count: 2), got %d", dataType, len(registers))
	}

	bits := uint32(registers[0])<<bitsPerRegister | uint32(registers[1])
	switch dataType {
	case "int32":
		return int32(bits), nil
	case "float32":
		return math.Float32frombits(bits), nil
	default:
		return bits, nil
	}
}
