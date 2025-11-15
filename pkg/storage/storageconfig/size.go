// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import (
	"regexp"
	"strconv"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"go.yaml.in/yaml/v4"
)

// Size is used to specify an on-disk size in bytes, either as an absolute value
// or as a percentage of the total disk capacity. The zero value indicates an
// unset size (different from a size of 0 bytes).
type Size struct {
	kind sizeKind
	// bytes is an absolute value in bytes. Can only be set with kind ==
	// bytesSize. At most one of Bytes and Percent can be non-zero.
	bytes int64
	// percent is a relative value as a percentage of the total disk capacity. Can
	// only be set with kind == percentSize.
	percent float64
}

type sizeKind int8

const (
	bytesSize sizeKind = 1 + iota
	percentSize
)

// BytesSize returns a Size that is set to an absolute value in bytes.
func BytesSize(bytes int64) Size {
	return Size{kind: bytesSize, bytes: bytes}
}

// PercentSize returns a Size that is set to a percentage value.
func PercentSize(percent float64) Size {
	return Size{kind: percentSize, percent: percent}
}

// IsSet returns true if the size is set.
func (s Size) IsSet() bool {
	return s.kind != 0
}

// IsBytes returns true if the size is set to an absolute bytes value.
func (s Size) IsBytes() bool {
	return s.kind == bytesSize
}

// Bytes returns the absolute bytes value; can only be used if IsBytes() is
// true.
func (s Size) Bytes() int64 {
	if !s.IsBytes() {
		panic("size is not absolute")
	}
	return s.bytes
}

// IsPercent returns true if the size is set to a percentage value.
func (s Size) IsPercent() bool {
	return s.kind == percentSize
}

// Percent returns the percentage value; can only be used if IsPercent() is
// true.
func (s Size) Percent() float64 {
	if !s.IsPercent() {
		panic("size is not a percent")
	}
	return s.percent
}

// Calculate returns the size in bytes. The total value is used if the size is
// set as a percentage. If the size is not set, 0 is returned.
func (s Size) Calculate(total int64) int64 {
	switch {
	case s.IsBytes():
		return s.Bytes()
	case s.IsPercent():
		return int64(float64(total) * s.Percent() * 0.01)
	default:
		return 0
	}
}

func (s Size) SafeFormat(p redact.SafePrinter, verb rune) {
	switch {
	case s.IsBytes():
		p.SafeString(redact.SafeString(crhumanize.Bytes(s.Bytes(), crhumanize.Exact)))
	case s.IsPercent():
		p.SafeString(redact.SafeString(humanize.Ftoa(s.Percent()) + "%"))
	default:
		p.SafeString("n/a")
	}
}

func (s Size) String() string {
	return redact.StringWithoutMarkers(s)
}

var _ yaml.IsZeroer = Size{}
var _ yaml.Marshaler = Size{}
var _ yaml.Unmarshaler = (*Size)(nil)

// IsZero implements yaml.IsZeroer. It returns true if s is the zero value, i.e.
// the size is not set. This should not be used directly, as it is confusing (in
// that it returns false for sizes set to 0B or 0%).
func (s Size) IsZero() bool {
	return !s.IsSet()
}

func (s Size) MarshalYAML() (interface{}, error) {
	return s.String(), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (s *Size) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := ParseSizeSpec(value.Value)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

// fractionRegex is the regular expression that recognizes whether
// the specified size is a fraction of the total available space.
// Proportional sizes can be expressed as fractional numbers, either
// in absolute value or with a trailing "%" sign. A fractional number
// without a trailing "%" must be recognized by the presence of a
// decimal separator; numbers without decimal separators are plain
// sizes in bytes (separate case in the parsing).
// The first part of the regexp matches NNN.[MMM]; the second part
// [NNN].MMM, and the last part matches explicit percentages with or
// without a decimal separator.
// Values smaller than 1% and 100% are rejected after parsing using
// a separate check.
var fractionRegex = regexp.MustCompile(`^([-]?([0-9]+\.[0-9]*|[0-9]*\.[0-9]+|[0-9]+(\.[0-9]*)?%))$`)

// ParseSizeSpec parses a string into a Size. The string contains one of:
//   - an absolute bytes value, possibly humanized; e.g. "10000", "100MiB".
//   - a percentage value (relative to the total disk capacity); e.g. "10%".
//   - a fractional value which is converted to a percentage; e.g. "0.1" is
//     equivalent to "10%".
//   - if the string is "n/a", the returned Size is unset.
func ParseSizeSpec(value string) (Size, error) {
	if fractionRegex.MatchString(value) {
		percentFactor := 100.0
		factorValue := value
		if value[len(value)-1] == '%' {
			percentFactor = 1.0
			factorValue = value[:len(value)-1]
		}
		percent, err := strconv.ParseFloat(factorValue, 64)
		percent *= percentFactor
		if err != nil {
			return Size{}, errors.Wrapf(err, "could not parse size (%s)", redact.Safe(value))
		}
		if percent < 0 || percent > 100 {
			return Size{}, errors.Newf("size (%s) must be between 0%% and 100%%", redact.Safe(value))
		}
		return PercentSize(percent), nil
	}

	bytes, err := crhumanize.ParseBytes[int64](value)
	if err != nil {
		return Size{}, errors.Wrapf(err, "could not parse size (%s)", redact.Safe(value))
	}
	return BytesSize(bytes), nil
}
