package config

import (
	"errors"
	"fmt"
)

// Modbus function codes: 3 reads holding registers, 4 reads input registers.
const (
	functionCodeHolding = 3
	functionCodeInput   = 4

	// registersPer32Bit is the register count of int32, uint32 and float32.
	registersPer32Bit = 2
)

// validatePoints checks the points against the rules of the device type.
func (d *Device) validatePoints() error {
	switch d.Type {
	case "modbus":
		return validateModbusPoints(d.Points)
	case "http":
		return validateHTTPPoints(d.Points, d.DeviceSpecific.HTTP.ResponseType)
	default:
		return nil
	}
}

// validateModbusPoints checks each point and that all points use one register type.
func validateModbusPoints(points []Point) error {
	var functionCode uint8
	for i, p := range points {
		if err := validateModbusPoint(p); err != nil {
			return fmt.Errorf("points[%d] %s: %w", i, p.Name, err)
		}
		if p.FunctionCode == 0 {
			continue
		}
		if functionCode != 0 && p.FunctionCode != functionCode {
			return fmt.Errorf("points[%d] %s: function_code %d mixed with %d; "+
				"use one device per register type", i, p.Name, p.FunctionCode, functionCode)
		}
		functionCode = p.FunctionCode
	}
	return nil
}

// validateModbusPoint checks the type, the register count and the function code.
func validateModbusPoint(p Point) error {
	need, ok := modbusRegisterCount(p.Type)
	if !ok {
		return fmt.Errorf("type must be int16, uint16, int32, uint32, float32 or bool, got %q",
			p.Type)
	}
	if max(p.Count, 1) < need {
		return fmt.Errorf("type %s needs count: %d", p.Type, need)
	}
	if p.FunctionCode != 0 && p.FunctionCode != functionCodeHolding &&
		p.FunctionCode != functionCodeInput {
		return fmt.Errorf("function_code must be 3 or 4, got %d", p.FunctionCode)
	}
	return nil
}

// modbusRegisterCount returns how many registers a Modbus data type spans.
func modbusRegisterCount(dataType string) (uint16, bool) {
	switch dataType {
	case "int16", "uint16", "bool":
		return 1, true
	case "int32", "uint32", "float32":
		return registersPer32Bit, true
	default:
		return 0, false
	}
}

// validateHTTPPoints checks that JSON points have a json_path.
func validateHTTPPoints(points []Point, responseType string) error {
	if responseType != "json" {
		return nil
	}
	for i, p := range points {
		if p.JSONPath == "" {
			return fmt.Errorf("points[%d] %s: %w", i, p.Name, errJSONPathRequired)
		}
	}
	return nil
}

// errJSONPathRequired is returned for a JSON point without json_path.
var errJSONPathRequired = errors.New("json_path required for response_type json")
