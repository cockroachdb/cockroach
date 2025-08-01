// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storagepb

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/pflag"
)

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

type IntInterval struct {
	Min *int64
	Max *int64
}

type FloatInterval struct {
	Min *float64
	Max *float64
}

// NewSizeSpec parses the string passed into a --size flag and returns a
// SizeSpec if it is correctly parsed.
func NewSizeSpec(
	field redact.SafeString, value string, bytesRange *IntInterval, percentRange *FloatInterval,
) (SizeSpec, error) {
	var size SizeSpec
	if fractionRegex.MatchString(value) {
		percentFactor := 100.0
		factorValue := value
		if value[len(value)-1] == '%' {
			percentFactor = 1.0
			factorValue = value[:len(value)-1]
		}
		var err error
		size.Percent, err = strconv.ParseFloat(factorValue, 64)
		size.Percent *= percentFactor
		if err != nil {
			return SizeSpec{}, errors.Wrapf(err, "could not parse %s size (%s)", field, value)
		}
		if percentRange != nil {
			if (percentRange.Min != nil && size.Percent < *percentRange.Min) ||
				(percentRange.Max != nil && size.Percent > *percentRange.Max) {
				return SizeSpec{}, errors.Newf(
					"%s size (%s) must be between %f%% and %f%%",
					field,
					value,
					*percentRange.Min,
					*percentRange.Max,
				)
			}
		}
	} else {
		var err error
		size.Capacity, err = humanizeutil.ParseBytes(value)
		if err != nil {
			return SizeSpec{}, errors.Wrapf(err, "could not parse %s size (%s)", field, value)
		}
		if bytesRange != nil {
			if bytesRange.Min != nil && size.Capacity < *bytesRange.Min {
				return SizeSpec{}, errors.Newf("%s size (%s) must be larger than %s",
					field, value, humanizeutil.IBytes(*bytesRange.Min))
			}
			if bytesRange.Max != nil && size.Capacity > *bytesRange.Max {
				return SizeSpec{}, errors.Newf("%s size (%s) must be smaller than %s",
					field, value, humanizeutil.IBytes(*bytesRange.Max))
			}
		}
	}
	return size, nil
}

// String returns a string representation of the SizeSpec. This is part
// of pflag's value interface.
func (ss *SizeSpec) String() string {
	var buffer bytes.Buffer
	if ss.Capacity != 0 {
		fmt.Fprintf(&buffer, "--size=%s,", humanizeutil.IBytes(ss.Capacity))
	}
	if ss.Percent != 0 {
		fmt.Fprintf(&buffer, "--size=%s%%,", humanize.Ftoa(ss.Percent))
	}
	return buffer.String()
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (ss *SizeSpec) Type() string {
	return "SizeSpec"
}

var _ pflag.Value = &SizeSpec{}

// Set adds a new value to the StoreSpecValue. It is the important part of
// pflag's value interface.
func (ss *SizeSpec) Set(value string) error {
	spec, err := NewSizeSpec("specified", value, nil, nil)
	if err != nil {
		return err
	}
	ss.Capacity = spec.Capacity
	ss.Percent = spec.Percent
	return nil
}
