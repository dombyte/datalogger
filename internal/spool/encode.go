package spool

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/dombyte/datalogger/internal/datasource"
)

// maxLine bounds one poll in the file when it is counted at startup: 500 points of
// 64 KiB text each fit.
const maxLine = 64 << 20

// Value kinds in the file: the value is kept as text with its Go type, so an int64
// stays an int64 (JSON would make it a float64) and NaN or ±Inf survive.
const (
	kindFloat  = "f"
	kindInt    = "i"
	kindUint   = "u"
	kindBool   = "b"
	kindString = "s"
)

// record is one point in the file; a poll is one line, a JSON array of records.
type record struct {
	Device string `json:"d"`
	Point  string `json:"p"`
	Time   string `json:"t"`
	Unit   string `json:"u,omitempty"`
	Kind   string `json:"k"`
	Value  string `json:"v"`
}

// encodePoll returns the line of a poll. A value of another type than the readers and
// expressions deliver is an error.
func encodePoll(points []datasource.DataPoint) ([]byte, error) {
	records := make([]record, len(points))
	for i, dp := range points {
		kind, value, err := encodeValue(dp.Value)
		if err != nil {
			return nil, fmt.Errorf("point %s/%s: %w", dp.DeviceName, dp.PointName, err)
		}
		records[i] = record{
			Device: dp.DeviceName, Point: dp.PointName, Unit: dp.Unit,
			Time: dp.Timestamp.Format(time.RFC3339Nano), Kind: kind, Value: value,
		}
	}
	line, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("encode poll: %w", err)
	}
	return append(line, '\n'), nil
}

// encodeValue returns the kind and text of a value.
func encodeValue(v any) (kind, value string, err error) {
	switch val := v.(type) {
	case float64:
		return kindFloat, strconv.FormatFloat(val, 'g', -1, 64), nil
	case int64:
		return kindInt, strconv.FormatInt(val, 10), nil
	case uint64:
		return kindUint, strconv.FormatUint(val, 10), nil
	case bool:
		return kindBool, strconv.FormatBool(val), nil
	case string:
		return kindString, val, nil
	default:
		return "", "", fmt.Errorf("cannot spool a value of type %T", v)
	}
}

// decodePoll returns the points of a line.
func decodePoll(line []byte) ([]datasource.DataPoint, error) {
	var records []record
	if err := json.Unmarshal(line, &records); err != nil {
		return nil, fmt.Errorf("decode poll: %w", err)
	}
	points := make([]datasource.DataPoint, len(records))
	for i, r := range records {
		t, err := time.Parse(time.RFC3339Nano, r.Time)
		if err != nil {
			return nil, fmt.Errorf("decode time: %w", err)
		}
		value, err := decodeValue(r.Kind, r.Value)
		if err != nil {
			return nil, err
		}
		points[i] = datasource.DataPoint{
			DeviceName: r.Device, PointName: r.Point, Value: value, Timestamp: t, Unit: r.Unit,
		}
	}
	return points, nil
}

// decodeValue returns the value of a kind and text.
func decodeValue(kind, value string) (any, error) {
	var (
		v   any
		err error
	)
	switch kind {
	case kindFloat:
		v, err = strconv.ParseFloat(value, 64)
	case kindInt:
		v, err = strconv.ParseInt(value, 10, 64)
	case kindUint:
		v, err = strconv.ParseUint(value, 10, 64)
	case kindBool:
		v, err = strconv.ParseBool(value)
	case kindString:
		v = value
	default:
		return nil, fmt.Errorf("unknown value kind %q", kind)
	}
	if err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}
	return v, nil
}
