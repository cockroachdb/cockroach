// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding

import (
	"math"

	"github.com/cockroachdb/errors"
)

// EncodeFloatAscending returns the resulting byte slice with the encoded float64
// appended to b. The encoded format for a float64 value f is, for positive f, the
// encoding of the 64 bits (in IEEE 754 format) re-interpreted as an int64 and
// encoded using EncodeUint64Ascending. For negative f, we keep the sign bit and
// invert all other bits, encoding this value using EncodeUint64Descending. This
// approach was inspired by in github.com/google/orderedcode/orderedcode.go.
//
// One of five single-byte prefix tags are appended to the front of the encoding.
// These tags enforce logical ordering of keys for both ascending and descending
// encoding directions. The tags split the encoded floats into five categories:
// - NaN for an ascending encoding direction
// - Negative valued floats
// - Zero (positive and negative)
// - Positive valued floats
// - NaN for a descending encoding direction
// This ordering ensures that NaNs are always sorted first in either encoding
// direction, and that after them a logical ordering is followed.
func EncodeFloatAscending(b []byte, f float64) []byte {
	// Handle the simplistic cases first.
	switch {
	case math.IsNaN(f):
		return append(b, floatNaN)
	case f == 0:
		// This encodes both positive and negative zero the same. Negative zero uses
		// composite indexes to decode itself correctly.
		return append(b, floatZero)
	}
	u := math.Float64bits(f)
	if u&(1<<63) != 0 {
		u = ^u
		b = append(b, floatNeg)
	} else {
		b = append(b, floatPos)
	}
	return EncodeUint64Ascending(b, u)
}

// EncodeFloatDescending is the descending version of EncodeFloatAscending.
func EncodeFloatDescending(b []byte, f float64) []byte {
	if math.IsNaN(f) {
		return append(b, floatNaNDesc)
	}
	return EncodeFloatAscending(b, -f)
}

// DecodeFloatAscending returns the remaining byte slice after decoding and the decoded
// float64 from buf.
func DecodeFloatAscending(buf []byte) ([]byte, float64, error) {
	if PeekType(buf) != Float {
		return buf, 0, errors.Errorf("did not find marker")
	}
	switch buf[0] {
	case floatNaN, floatNaNDesc:
		return buf[1:], math.NaN(), nil
	case floatNeg:
		b, u, err := DecodeUint64Ascending(buf[1:])
		if err != nil {
			return b, 0, err
		}
		u = ^u
		return b, math.Float64frombits(u), nil
	case floatZero:
		return buf[1:], 0, nil
	case floatPos:
		b, u, err := DecodeUint64Ascending(buf[1:])
		if err != nil {
			return b, 0, err
		}
		return b, math.Float64frombits(u), nil
	default:
		return nil, 0, errors.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

// DecodeFloatDescending decodes floats encoded with EncodeFloatDescending.
func DecodeFloatDescending(buf []byte) ([]byte, float64, error) {
	b, r, err := DecodeFloatAscending(buf)
	return b, -r, err
}
