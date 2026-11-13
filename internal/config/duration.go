package config

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
)

// durationPattern is a duration with units: one or more <number><unit>, the units ms, s,
// min (or m) and h, e.g. 500ms, 30s, 5min, 1h30m. Smaller units (us, ns) are rejected:
// no setting needs them, and a value in nanoseconds is almost always a mistake.
const durationPattern = `^(\d+(\.\d+)?(ms|s|min|m|h))+$`

// secondsPattern is a number without a unit: seconds. Only digits, so NaN, Inf or 1e9
// are no durations.
const secondsPattern = `^\d+(\.\d+)?$`

// durationHook decodes every time.Duration setting: a number without a unit (5, 1.5,
// "5") is seconds, a string with units is parsed by parseDuration. Without it, YAML
// numbers became nanoseconds: poll_interval: 5 polled every 5 ns.
func durationHook(_ reflect.Type, to reflect.Type, data any) (any, error) {
	if to != reflect.TypeFor[time.Duration]() {
		return data, nil
	}
	switch v := data.(type) {
	case int:
		return time.Duration(v) * time.Second, nil
	case float64:
		return time.Duration(v * float64(time.Second)), nil
	case string:
		return parseDuration(v)
	default:
		return data, nil
	}
}

// parseDuration parses a duration setting: a number without a unit is seconds,
// otherwise the units ms, s, min (or m) and h are allowed.
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	// Compiled per call: Load parses a few durations once, at startup.
	if regexp.MustCompile(secondsPattern).MatchString(s) {
		seconds, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		return time.Duration(seconds * float64(time.Second)), nil
	}
	if !regexp.MustCompile(durationPattern).MatchString(s) {
		return 0, fmt.Errorf("invalid duration %q: use a number of seconds or the units "+
			"ms, s, min and h (e.g. 500ms, 30s, 5min, 1h)", s)
	}
	d, err := time.ParseDuration(strings.ReplaceAll(s, "min", "m"))
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return d, nil
}

// decodeHook is the decode hook of Load: durations as above, and comma-separated
// strings into lists, as viper does by default.
func decodeHook() mapstructure.DecodeHookFunc {
	return mapstructure.ComposeDecodeHookFunc(
		durationHook,
		mapstructure.StringToSliceHookFunc(","),
	)
}
