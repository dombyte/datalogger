package http

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// maxStringSize bounds a string value: InfluxDB stores at most 64 KiB in a string field,
// and no reading of a device is longer.
const maxStringSize = 64 << 10

// Number kinds of the configured point types.
const (
	kindFloat = "float"
	kindInt   = "int"
	kindUint  = "uint"
)

// numberType is the kind and bit size of a numeric point type.
type numberType struct {
	kind string
	bits int
}

// numberTypeOf returns the kind and bit size of a numeric point type. Values are
// delivered as float64, int64 or uint64; the bit size only bounds the range, so a value
// that does not fit the type (70000 for int16) is an error instead of a wrapped number.
// float32 is parsed as float64: float32 would round the value.
func numberTypeOf(targetType string) (numberType, bool) {
	const bits16, bits32, bits64 = 16, 32, 64
	types := [...]struct {
		name string
		typ  numberType
	}{
		{"float32", numberType{kindFloat, bits64}},
		{"float64", numberType{kindFloat, bits64}},
		{"int16", numberType{kindInt, bits16}},
		{"int32", numberType{kindInt, bits32}},
		{"int64", numberType{kindInt, bits64}},
		{"uint16", numberType{kindUint, bits16}},
		{"uint32", numberType{kindUint, bits32}},
		{"uint64", numberType{kindUint, bits64}},
	}
	for _, t := range types {
		if t.name == targetType {
			return t.typ, true
		}
	}
	return numberType{}, false
}

// validType reports whether a point type is known: "" (detect), bool, string or a
// number type.
func validType(targetType string) bool {
	_, number := numberTypeOf(targetType)
	return number || targetType == "" || targetType == "bool" || targetType == "string"
}

// extractValue returns the value of a point from the response body.
func (r *Reader) extractValue(body []byte, p Point) (any, error) {
	switch r.settings.ResponseType {
	case "json":
		return extractJSONValue(body, p)
	case "text":
		return extractTextValue(body, p)
	default:
		return nil, fmt.Errorf("unknown response type: %s", r.settings.ResponseType)
	}
}

// extractJSONValue reads the point's json_path, converts it and applies scale/offset.
func extractJSONValue(body []byte, p Point) (any, error) {
	if p.JSONPath == "" {
		return nil, errors.New("json_path required for JSON response")
	}

	result := gjson.GetBytes(body, p.JSONPath)
	if !result.Exists() {
		return nil, fmt.Errorf("JSONPath %s not found", p.JSONPath)
	}

	value, err := convertJSONResult(result, p.Type)
	if err == nil {
		value, err = cleanValue(value)
	}
	if err != nil {
		return nil, fmt.Errorf("JSONPath %s: %w", p.JSONPath, err)
	}
	return applyScaleAndOffset(value, p), nil
}

// extractTextValue converts the whole body, without surrounding white space, like a
// JSON string: to the point type, a string without one; scale and offset apply to
// numbers.
func extractTextValue(body []byte, p Point) (any, error) {
	text := strings.TrimSpace(string(body))
	var value any = text
	var err error
	switch p.Type {
	case "", "string":
	case "bool":
		value, err = parseBool(text)
	default:
		value, err = convertNumber(gjson.Result{Type: gjson.String, Str: text}, p.Type)
	}
	if err == nil {
		value, err = cleanValue(value)
	}
	if err != nil {
		return nil, fmt.Errorf("text: %w", err)
	}
	return applyScaleAndOffset(value, p), nil
}

// cleanValue checks a string from the device before it reaches the outputs: at most
// maxStringSize bytes, invalid UTF-8 replaced, and control characters other than tab
// and newline removed, so no device text can carry terminal escapes or carriage returns
// into a log, a CSV file or a dashboard. Other values are returned unchanged.
func cleanValue(value any) (any, error) {
	s, ok := value.(string)
	if !ok {
		return value, nil
	}
	if len(s) > maxStringSize {
		return nil, fmt.Errorf("text of %d bytes is longer than %d", len(s), maxStringSize)
	}
	return strings.Map(func(r rune) rune {
		if r != '\t' && r != '\n' && unicode.IsControl(r) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, string(utf8.RuneError))), nil
}

// convertJSONResult converts a gjson result to the point type; without a type numbers
// become float64 and bools and strings stay. null and values that do not fit the type
// are an error: gjson would turn them into 0 or false, which looks like a reading.
func convertJSONResult(result gjson.Result, targetType string) (any, error) {
	if result.Type == gjson.Null {
		return nil, errors.New("value is null")
	}
	switch targetType {
	case "":
		return autoDetect(result)
	case "bool":
		return convertBool(result)
	case "string":
		return result.String(), nil
	}
	return convertNumber(result, targetType)
}

// convertNumber converts a JSON number, or a string holding one, to a number type.
func convertNumber(result gjson.Result, targetType string) (any, error) {
	typ, ok := numberTypeOf(targetType)
	if !ok {
		return nil, fmt.Errorf("unknown type %q", targetType) // rejected by New
	}
	switch result.Type {
	case gjson.String:
		return parseNumber(strings.TrimSpace(result.Str), targetType, typ)
	case gjson.Number:
		// The raw text, not gjson's float: an int64 above 2^53 keeps its digits, and
		// 2.5 is no integer instead of being cut to 2.
		return parseNumber(result.Raw, targetType, typ)
	default:
		return nil, fmt.Errorf("%s is not a number", result.Raw)
	}
}

// parseNumber parses a number of the type; a fraction for an integer type, a negative
// number for an unsigned one and a value outside the type's range are errors.
func parseNumber(s, name string, typ numberType) (any, error) {
	var (
		value any
		err   error
	)
	switch typ.kind {
	case kindFloat:
		value, err = strconv.ParseFloat(s, typ.bits)
	case kindInt:
		value, err = strconv.ParseInt(s, 10, typ.bits)
	default:
		value, err = strconv.ParseUint(s, 10, typ.bits)
	}
	if err != nil {
		return nil, fmt.Errorf("%q is not a valid %s number", s, name)
	}
	return value, nil
}

// convertBool converts a JSON bool, a number (non-zero is true) or a string such as
// "true" or "0".
func convertBool(result gjson.Result) (any, error) {
	switch result.Type {
	case gjson.True, gjson.False, gjson.Number:
		return result.Bool(), nil
	case gjson.String:
		return parseBool(strings.TrimSpace(result.Str))
	default:
		return nil, fmt.Errorf("%s is not a bool", result.Raw)
	}
}

// parseBool parses a bool sent as text ("true", "0").
func parseBool(s string) (any, error) {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return nil, fmt.Errorf("%q is not a bool", s)
	}
	return b, nil
}

// autoDetect converts a JSON value without a configured type: numbers become float64,
// bools and strings stay. Objects and arrays are an error, because no output can store
// them as one value (InfluxDB would reject the whole batch).
func autoDetect(result gjson.Result) (any, error) {
	switch result.Type {
	case gjson.True, gjson.False:
		return result.Bool(), nil
	case gjson.Number:
		return result.Float(), nil
	case gjson.String:
		return result.Str, nil
	default:
		return nil, errors.New("value is a JSON object or array; point json_path at a " +
			"single value or set type: string")
	}
}

// applyScaleAndOffset returns value × scale + offset as float64 for numeric values.
// With scale 1 and offset 0 (the defaults) the value keeps its type; bools and strings
// are never changed.
func applyScaleAndOffset(value any, p Point) any {
	if p.Scale == 1 && p.Offset == 0 {
		return value
	}
	switch v := value.(type) {
	case float64:
		return v*p.Scale + p.Offset
	case int64:
		return float64(v)*p.Scale + p.Offset
	case uint64:
		return float64(v)*p.Scale + p.Offset
	default:
		return value
	}
}
