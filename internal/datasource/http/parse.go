package http

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// Number kinds of the configured point types.
const (
	kindFloat = "float"
	kindInt   = "int"
	kindUint  = "uint"
)

// extractValue returns the value of a point from the response body.
func (r *Reader) extractValue(body []byte, p Point) (any, error) {
	switch r.settings.ResponseType {
	case "json":
		return extractJSONValue(body, p)
	case "text":
		return string(body), nil
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
	if err != nil {
		return nil, fmt.Errorf("JSONPath %s: %w", p.JSONPath, err)
	}
	return applyScaleAndOffset(value, p), nil
}

// convertJSONResult converts a gjson result to the point type; without a type numbers
// become float64 and bools and strings stay. null and values that do not fit the type
// are an error: gjson would turn them into 0 or false, which looks like a reading.
func convertJSONResult(result gjson.Result, targetType string) (any, error) {
	if result.Type == gjson.Null {
		return nil, errors.New("value is null")
	}
	switch targetType {
	case "bool":
		return convertBool(result)
	case "string":
		return result.String(), nil
	}
	if kind := numberKind(targetType); kind != "" {
		return convertNumber(result, kind)
	}
	return autoDetect(result)
}

// numberKind returns the number kind of a point type, "" for other types.
func numberKind(targetType string) string {
	switch targetType {
	case "float32", "float64":
		return kindFloat
	case "int16", "int32", "int64":
		return kindInt
	case "uint16", "uint32", "uint64":
		return kindUint
	default:
		return ""
	}
}

// convertNumber converts a JSON number, or a string holding one, to the number kind.
func convertNumber(result gjson.Result, kind string) (any, error) {
	switch {
	case result.Type == gjson.String:
		return parseNumber(strings.TrimSpace(result.Str), kind)
	case result.Type != gjson.Number:
		return nil, fmt.Errorf("%s is not a number", result.Raw)
	case kind == kindFloat:
		return result.Float(), nil
	case kind == kindInt:
		return result.Int(), nil
	case result.Num < 0:
		return nil, fmt.Errorf("%s is negative, type is unsigned", result.Raw)
	default:
		return result.Uint(), nil
	}
}

// parseNumber parses a number sent as a JSON string.
func parseNumber(s, kind string) (any, error) {
	var (
		value any
		err   error
	)
	switch kind {
	case kindFloat:
		value, err = strconv.ParseFloat(s, 64)
	case kindInt:
		value, err = strconv.ParseInt(s, 10, 64)
	default:
		value, err = strconv.ParseUint(s, 10, 64)
	}
	if err != nil {
		return nil, fmt.Errorf("%q is not a valid %s number", s, kind)
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
		b, err := strconv.ParseBool(strings.TrimSpace(result.Str))
		if err != nil {
			return nil, fmt.Errorf("%q is not a bool", result.Str)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("%s is not a bool", result.Raw)
	}
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
