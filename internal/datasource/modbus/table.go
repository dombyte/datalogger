package modbus

const (
	// Modbus function codes that select the data table of a device.
	fcCoils            = 1
	fcDiscreteInputs   = 2
	fcHoldingRegisters = 3
	fcInputRegisters   = 4

	// maxRegistersPerRead and maxBitsPerRead are the protocol limits of one read
	// request: 125 registers, or 2000 coils or discrete inputs.
	maxRegistersPerRead = 125
	maxBitsPerRead      = 2000
)

// Table is the Modbus data table a device reads; one per device.
type Table uint8

// The data tables: 16-bit registers, or single bits (coils and discrete inputs), which
// are read as one value of 0 or 1 per address.
const (
	HoldingRegisters Table = iota
	InputRegisters
	Coils
	DiscreteInputs
)

// maxPerRead returns how many addresses of the table one request reads at most.
func (t Table) maxPerRead() int {
	if t == Coils || t == DiscreteInputs {
		return maxBitsPerRead
	}
	return maxRegistersPerRead
}

// tableOf returns the table of the first point with a function code (holding registers
// by default); validation allows only one per device.
func tableOf(points []Point) Table {
	for _, p := range points {
		switch p.FunctionCode {
		case fcCoils:
			return Coils
		case fcDiscreteInputs:
			return DiscreteInputs
		case fcHoldingRegisters:
			return HoldingRegisters
		case fcInputRegisters:
			return InputRegisters
		}
	}
	return HoldingRegisters
}
