// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import (
	"regexp"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// Size is used to specify an on-disk size in bytes, either as an absolute
// value or as a percentage of the total disk capacity.
type Size struct {
	// Bytes is an absolute value in bytes.
	// At most one of Bytes and Percent can be non-zero.
	Bytes int64
	// Percent is a relative value as a percentage of the total disk capacity.
	// At most one of Bytes and Percent can be non-zero.
	Percent float64
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

// SizeSpecConstraints describes acceptable values when parsing a Size
// value. The zero struct value is valid and means that there are no
// constraints.
type SizeSpecConstraints struct {
	// MinBytes is the minimum acceptable size in bytes when an absolute bytes
	// value is used.
	MinBytes int64
	// MinPercent is the minimum acceptable relative size when a percentage value
	// is used.
	MinPercent float64
	// MaxPercent is the maximum acceptable relative size when a percentage value
	// is used. If 0, there is no limit (up to 100%).
	MaxPercent float64
}

// ParseSizeSpec parses a string into a Size. The string contains one of:
//   - an absolute bytes value, possibly humanized; e.g. "10000", "100MiB".
//   - a percentage value (relative to the total disk capacity); e.g. "10%".
//   - a fractional value which is converted to a percentage; e.g. "0.1" is
//     equivalent to "10%".
func ParseSizeSpec(value string, constraints SizeSpecConstraints) (Size, error) {
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
		minPercent := constraints.MinPercent
		maxPercent := constraints.MaxPercent
		if maxPercent == 0 {
			maxPercent = 100.0
		}
		if percent < minPercent || percent > maxPercent {
			return Size{}, errors.Newf(
				"size (%s) must be between %s%% and %s%%",
				redact.Safe(value), humanize.Ftoa(minPercent), humanize.Ftoa(maxPercent),
			)
		}
		return Size{Percent: percent}, nil
	}

	bytes, err := humanizeutil.ParseBytes(value)
	if err != nil {
		return Size{}, errors.Wrapf(err, "could not parse size (%s)", redact.Safe(value))
	}
	if bytes < constraints.MinBytes {
		return Size{}, errors.Newf("size (%s) must be at least %s",
			redact.Safe(value), humanizeutil.IBytes(constraints.MinBytes))
	}
	return Size{Bytes: bytes}, nil
}
