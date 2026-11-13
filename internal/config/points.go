package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Modbus function codes: 3 reads holding registers, 4 reads input registers.
const (
	functionCodeHolding = 3
	functionCodeInput   = 4

	// registersPer32Bit is the register count of int32, uint32 and float32.
	registersPer32Bit = 2

	// maxRegistersPerRead is the Modbus protocol limit for one read request.
	maxRegistersPerRead = 125

	// lastRegister is the highest Modbus register address.
	lastRegister = 65535
)

// validatePointNames checks that every point has a unique name without "/", so
// expressions and exclude_points can name it.
func validatePointNames(points []Point) error {
	seen := make(map[string]bool, len(points))
	for i, p := range points {
		switch {
		case p.Name == "":
			return fmt.Errorf("points[%d]: name required", i)
		case strings.Contains(p.Name, pathSeparator):
			return fmt.Errorf("points[%d] %s: name must not contain %q", i, p.Name, pathSeparator)
		case seen[p.Name]:
			return fmt.Errorf("points[%d]: duplicate point name: %s", i, p.Name)
		}
		seen[p.Name] = true
	}
	return nil
}

// validatePoints checks the point names and the points against the rules of the device
// type.
func (d *Device) validatePoints() error {
	if err := validatePointNames(d.Points); err != nil {
		return err
	}
	switch d.Type {
	case "modbus":
		return validateModbusPoints(d.Points)
	case "http":
		return validateHTTPPoints(d.Points, d.HTTP.ResponseType)
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
	if p.JSONPath != "" {
		return errors.New("json_path is an HTTP setting")
	}
	need, ok := modbusRegisterCount(p.Type)
	if !ok {
		return fmt.Errorf("type must be int16, uint16, int32, uint32, float32 or bool, got %q",
			p.Type)
	}
	if err := validateRegisters(p, need); err != nil {
		return err
	}
	if p.FunctionCode != 0 && p.FunctionCode != functionCodeHolding &&
		p.FunctionCode != functionCodeInput {
		return fmt.Errorf("function_code must be 3 or 4, got %d", p.FunctionCode)
	}
	return nil
}

// validateRegisters checks that the count fits the type and one request, and that the
// registers end at the last register address at the latest.
func validateRegisters(p Point, need uint16) error {
	count := max(p.Count, 1)
	if count < need {
		return fmt.Errorf("type %s needs count: %d", p.Type, need)
	}
	if count > maxRegistersPerRead {
		return fmt.Errorf("count must be at most %d, got %d", maxRegistersPerRead, count)
	}
	// int arithmetic: in uint16 the end would wrap around to register 0.
	if end := int(p.Register) + int(count) - 1; end > lastRegister {
		return fmt.Errorf("registers %d-%d exceed the last register %d",
			p.Register, end, lastRegister)
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

// httpTypes are the point types of an HTTP device: the value is converted to it.
func httpTypes() []string {
	return []string{
		"", "float32", "float64", "int16", "int32", "int64",
		"uint16", "uint32", "uint64", "bool", "string",
	}
}

// validateHTTPPoints checks every point of an HTTP device: a known type, json_path for
// JSON (and only for JSON) and no Modbus settings, which would be ignored silently.
func validateHTTPPoints(points []Point, responseType string) error {
	for i, p := range points {
		if err := validateHTTPPoint(p, responseType); err != nil {
			return fmt.Errorf("points[%d] %s: %w", i, p.Name, err)
		}
	}
	return nil
}

// validateHTTPPoint checks one point of an HTTP device.
func validateHTTPPoint(p Point, responseType string) error {
	if !slices.Contains(httpTypes(), p.Type) {
		return fmt.Errorf("type must be float32, float64, int16, int32, int64, uint16, "+
			"uint32, uint64, bool or string, got %q", p.Type)
	}
	if p.Register != 0 || p.Count != 0 || p.FunctionCode != 0 {
		return errors.New("register, count and function_code are Modbus settings")
	}
	return validateHTTPSource(p, responseType)
}

// validateHTTPSource checks where the value comes from: json_path for JSON, the whole
// body for text.
func validateHTTPSource(p Point, responseType string) error {
	switch {
	case responseType == "json" && p.JSONPath == "":
		return errJSONPathRequired
	case responseType == "json":
		return nil
	case p.JSONPath != "":
		return errors.New("json_path is only used with response_type json")
	}
	return nil
}

// errJSONPathRequired is returned for a JSON point without json_path.
var errJSONPathRequired = errors.New("json_path required for response_type json")
