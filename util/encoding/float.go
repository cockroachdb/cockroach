// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

package encoding

import (
	"math"

	"github.com/cockroachdb/cockroach/util"
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
	u := math.Float64bits(f)
	switch {
	case math.IsNaN(f):
		return append(b, floatNaN)
	case u == 0:
		// Only special-case positive zero.
		return append(b, floatZero)
	}
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
		return buf, 0, util.Errorf("did not find marker")
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
		return nil, 0, util.Errorf("unknown prefix of the encoded byte slice: %q", buf)
	}
}

// DecodeFloatDescending decodes floats encoded with EncodeFloatDescending.
func DecodeFloatDescending(buf []byte) ([]byte, float64, error) {
	b, r, err := DecodeFloatAscending(buf)
	return b, -r, err
}
