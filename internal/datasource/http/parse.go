package http

import (
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
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

	return applyScaleAndOffset(convertJSONResult(result, p.Type), p), nil
}

// convertJSONResult converts a gjson result to the point type; without a type numbers
// become float64, bools stay bool and anything else keeps its JSON value.
func convertJSONResult(result gjson.Result, targetType string) any {
	switch targetType {
	case "float32", "float64":
		return result.Float()
	case "int16", "int32", "int64":
		return result.Int()
	case "uint16", "uint32", "uint64":
		return result.Uint()
	case "bool":
		return result.Bool()
	case "string":
		return result.String()
	default:
		return autoDetect(result)
	}
}

// autoDetect converts a JSON value without a configured type.
func autoDetect(result gjson.Result) any {
	switch {
	case result.IsBool():
		return result.Bool()
	case result.Type == gjson.Number:
		return result.Float()
	default:
		return result.Value()
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
