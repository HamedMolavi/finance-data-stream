package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ToFloat64 converts typical JSON-decoded values (string, float64, json.Number) to float64.
func ToFloat64(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return 0, fmt.Errorf("invalid numeric value: %v", t)
		}
		return t, nil
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0, fmt.Errorf("json.Number->float64: %w", err)
		}
		return f, nil
	case string:
		// trim spaces, sometimes API returns " 123.45 "
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, errors.New("empty string where number expected")
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("parse float from string %q: %w", s, err)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("invalid numeric value parsed from %q", s)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported type for numeric conversion: %T", v)
	}
}

// ToInt64 converts JSON-decoded values to int64. Accepts float64, json.Number, string.
func ToInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case float64:
		// JSON numbers decode to float64; ensure they are integral (no fractional part).
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return 0, fmt.Errorf("invalid numeric value: %v", t)
		}
		i := int64(t)
		// sanity check: if float had fractional, i would differ
		if float64(i) != t {
			return 0, fmt.Errorf("non-integer numeric value: %v", t)
		}
		return i, nil
	case json.Number:
		// try parse as int first then fallback to float if needed
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, err := t.Float64()
		if err != nil {
			return 0, fmt.Errorf("json.Number->float64: %w", err)
		}
		return int64(f), nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, errors.New("empty string where integer expected")
		}
		// try integer parse
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, nil
		}
		// fallback to float parse then cast
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot parse int from string %q", s)
	default:
		return 0, fmt.Errorf("unsupported type for int conversion: %T", v)
	}
}
