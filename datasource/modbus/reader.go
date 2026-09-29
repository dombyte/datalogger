// Package modbus provides Modbus TCP/RTU reading functionality.
package modbus

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/simonvetter/modbus"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
)

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
		config:  deviceConfig,
		logger:  logger.With().Str("datasource", "modbus").Str("device", deviceConfig.Name).Logger(),
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
		if point.FunctionCode == 3 {
			return modbus.HOLDING_REGISTER
		}
		if point.FunctionCode == 4 {
			return modbus.INPUT_REGISTER
		}
	}
	// Default to Holding Registers (function code 3)
	return modbus.HOLDING_REGISTER
}

// createClient creates and opens the Modbus client.
func (r *Reader) createClient() error {
	modbusConfig := r.config.DeviceSpecific.Modbus

	// Create a standard logger from zerolog for the modbus library
	// The r.logger already has datasource=modbus and device=name from New
	modbusStdLogger := newModbusLogger(&r.logger)

	clientConfig := &modbus.ClientConfiguration{
		URL:      modbusConfig.Address,
		Speed:    uint(modbusConfig.Speed),
		DataBits: uint(modbusConfig.DataBits),
		Parity:   parseParity(modbusConfig.Parity),
		StopBits: uint(modbusConfig.StopBits),
		Timeout:  r.config.Timeout,
		Logger:   modbusStdLogger,
	}

	client, err := modbus.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create Modbus client: %w", err)
	}

	if err := client.Open(); err != nil {
		return fmt.Errorf("failed to open Modbus connection: %w", err)
	}

	// Set the slave ID for the client
	if err := client.SetUnitId(modbusConfig.SlaveID); err != nil {
		client.Close()
		return fmt.Errorf("failed to set unit ID: %w", err)
	}

	r.client = client
	return nil
}

// shouldReconnect determines if an error indicates a connection issue that warrants reconnection.
func (r *Reader) shouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	// Check for common connection errors
	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "refused") ||
		strings.Contains(errStr, "unreachable") ||
		strings.Contains(errStr, "reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "EOF")
}

// reconnect closes the current connection and opens a new one.
func (r *Reader) reconnect() error {
	r.logger.Warn().Msg("Attempting to reconnect Modbus client")

	// Create new client first, so we always have a valid client or error
	modbusConfig := r.config.DeviceSpecific.Modbus
	modbusStdLogger := newModbusLogger(&r.logger)

	clientConfig := &modbus.ClientConfiguration{
		URL:      modbusConfig.Address,
		Speed:    uint(modbusConfig.Speed),
		DataBits: uint(modbusConfig.DataBits),
		Parity:   parseParity(modbusConfig.Parity),
		StopBits: uint(modbusConfig.StopBits),
		Timeout:  r.config.Timeout,
		Logger:   modbusStdLogger,
	}

	client, err := modbus.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create Modbus client: %w", err)
	}

	if err := client.Open(); err != nil {
		client.Close()
		return fmt.Errorf("failed to open Modbus connection: %w", err)
	}

	if err := client.SetUnitId(modbusConfig.SlaveID); err != nil {
		client.Close()
		return fmt.Errorf("failed to set unit ID: %w", err)
	}

	// Only close old client after new one is successfully created
	// This prevents r.client from ever being nil during normal operation
	if r.client != nil {
		r.client.Close()
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

// validateAddresses checks that all point registers are covered by the configured ranges (for range mode).
// For direct mode, ranges are not required.
func (r *Reader) validateAddresses() error {
	modbusConfig := r.config.DeviceSpecific.Modbus

	// Only validate if using range mode
	if modbusConfig.RegisterMode != "range" {
		return nil
	}

	// For range mode, check that all point registers are covered
	for _, point := range r.points {
		// Check if the point's register range is covered by any configured range
		covered := false
		for _, rng := range r.ranges {
			// Check if point.Register to point.Register+point.Count-1 is within rng.Start to rng.End
			pointEnd := point.Register
			if point.Count > 0 {
				pointEnd = point.Register + point.Count - 1
			}
			if point.Register >= rng.Start && pointEnd <= rng.End {
				covered = true
				break
			}
		}
		if !covered {
			return fmt.Errorf("point '%s' register range %d-%d not covered by any configured range",
				point.Name, point.Register, point.Register+point.Count-1)
		}
	}
	return nil
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
func (r *Reader) Start(ctx context.Context) (<-chan datasource.DataPoint, <-chan struct{}, <-chan error) {
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
		r.logger.Warn().Dur("wait", r.backoffWait).Msg("Waiting before retry due to previous failure")
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
func (r *Reader) handlePollSuccess(points []datasource.DataPoint, dataCh chan<- datasource.DataPoint) {
	// Check if this is recovery from a previous error
	recoveredFromError := r.lastError != nil || r.failCount > 0

	// Reset failure tracking on success
	r.failCount = 0
	r.backoffWait = 0
	r.lastError = nil

	// Log at INFO level if recovering from an error, DEBUG otherwise
	if recoveredFromError {
		r.logger.Info().Int("count", len(points)).Msg("Modbus poll completed successfully (recovered from previous error)")
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
	const maxBackoff = 30 * time.Second
	r.backoffWait = r.backoffWait * 2
	if r.backoffWait == 0 {
		r.backoffWait = 100 * time.Millisecond
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
		return nil, fmt.Errorf("modbus client is not connected")
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

			// Read all addresses for this point sequentially (within the point)
			// Pass nil for the timestamp parameter - it will be captured inside readSinglePoint
			dp, err := r.readSinglePoint(ctx, p, time.Time{})
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
func (r *Reader) readSinglePoint(ctx context.Context, point config.Point, _ time.Time) (*datasource.DataPoint, error) {
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
func (r *Reader) extractValuesFromRangeData(point config.Point, rangeData map[uint16]RangeValue) ([]uint16, time.Time, error) {
	// Extract values using Register and Count
	if point.Count == 0 {
		point.Count = 1
	}

	values := make([]uint16, point.Count)
	var pointTimestamp time.Time
	for i := uint16(0); i < point.Count; i++ {
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

	// Modbus protocol hard limit: 125 registers per request
	const maxRangeSize uint16 = 125

	for _, rng := range r.ranges {
		wg.Add(1)
		go func(rng Range) {
			defer wg.Done()

			// Acquire semaphore slot
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := r.readRangeChunked(rng, maxRangeSize, rangeData, &mu, &readErr); err != nil {
				// Error already logged in readRangeChunked
				return
			}
		}(rng)
	}

	wg.Wait()

	return rangeData, readErr
}

// readRangeChunked reads a single range with chunking to respect Modbus protocol limits.
func (r *Reader) readRangeChunked(
	rng Range,
	maxRangeSize uint16,
	rangeData map[uint16]RangeValue,
	mu *sync.Mutex,
	readErr *error,
) error {
	count := rng.End - rng.Start + 1
	start := rng.Start

	for start <= rng.End {
		chunkSize := count
		if chunkSize > maxRangeSize {
			chunkSize = maxRangeSize
		}

		result, err := r.client.ReadRegisters(
			start,
			chunkSize,
			r.regType,
		)
		if err != nil {
			r.logger.Warn().
				Uint16("start", start).
				Uint16("count", chunkSize).
				Err(err).
				Msg("Failed to read range")
			mu.Lock()
			if *readErr == nil {
				*readErr = err
			}
			mu.Unlock()
			return err
		}

		// Capture timestamp when this chunk was received
		// Use UTC to ensure consistency with InfluxDB expectations
		receiveTime := time.Now().UTC()

		mu.Lock()
		for i, val := range result {
			rangeData[start+uint16(i)] = RangeValue{
				Value:     val,
				Timestamp: receiveTime,
			}
		}
		mu.Unlock()

		start += chunkSize
		count -= chunkSize
	}

	return nil
}

// decodePointsFromRangeData decodes all points from the range data.
func (r *Reader) decodePointsFromRangeData(rangeData map[uint16]RangeValue) ([]datasource.DataPoint, error) {
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

// applyScaleAndOffset applies scale and offset to a decoded value.
func (r *Reader) applyScaleAndOffset(value interface{}, point config.Point) float64 {
	// Convert value to float64 for scaling
	var scaled float64
	switch v := value.(type) {
	case int16:
		scaled = float64(v)*point.Scale + point.Offset
	case uint16:
		scaled = float64(v)*point.Scale + point.Offset
	case int32:
		scaled = float64(v)*point.Scale + point.Offset
	case uint32:
		scaled = float64(v)*point.Scale + point.Offset
	case float32:
		scaled = float64(v)*point.Scale + point.Offset
	case float64:
		scaled = v*point.Scale + point.Offset
	case bool:
		if v {
			scaled = 1.0*point.Scale + point.Offset
		} else {
			scaled = 0.0*point.Scale + point.Offset
		}
	default:
		r.logger.Warn().
			Str("point", point.Name).
			Str("type", fmt.Sprintf("%T", value)).
			Msg("Unsupported type for scaling, skipping")
	}
	return scaled
}

// decodeValue decodes raw Modbus register values into the appropriate type.
func decodeValue(registers []uint16, dataType string) (interface{}, error) {
	switch dataType {
	case "int16":
		return int16(registers[0]), nil
	case "uint16":
		return uint16(registers[0]), nil
	case "int32":
		return int32(binary.BigEndian.Uint32([]byte{byte(registers[0] >> 8), byte(registers[0]), byte(registers[1] >> 8), byte(registers[1])})), nil
	case "uint32":
		return uint32(binary.BigEndian.Uint32([]byte{byte(registers[0] >> 8), byte(registers[0]), byte(registers[1] >> 8), byte(registers[1])})), nil
	case "float32":
		bits := binary.BigEndian.Uint32([]byte{byte(registers[0] >> 8), byte(registers[0]), byte(registers[1] >> 8), byte(registers[1])})
		return math.Float32frombits(bits), nil
	case "bool":
		return registers[0] != 0, nil
	case "string":
		// Convert registers to string
		// Implementation depends on encoding
		return "", fmt.Errorf("string type not implemented for modbus")
	default:
		return nil, fmt.Errorf("unknown data type: %s", dataType)
	}
}
