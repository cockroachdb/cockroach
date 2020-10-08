// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package arith

import "math"

// AddWithOverflow returns a+b. If ok is false, a+b overflowed.
func AddWithOverflow(a, b int64) (r int64, ok bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

// Add32to64WithOverflow returns a+b. If ok is false, b was outside the
// int32 range or a+b overflowed.
func Add32to64WithOverflow(a int32, b int64) (r int32, ok bool) {
	if b > math.MaxInt32 || b < math.MinInt32 {
		return 0, false
	}
	return Add32WithOverflow(a, int32(b))
}

// Add32WithOverflow returns a+b. If ok is false, a+b overflowed.
func Add32WithOverflow(a, b int32) (r int32, ok bool) {
	if b > 0 && a > math.MaxInt32-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt32-b {
		return 0, false
	}
	return a + b, true
}

// SubWithOverflow returns a-b. If ok is false, a-b overflowed.
func SubWithOverflow(a, b int64) (r int64, ok bool) {
	if b < 0 && a > math.MaxInt64+b {
		return 0, false
	}
	if b > 0 && a < math.MinInt64+b {
		return 0, false
	}
	return a - b, true
}

// Sub32to64WithOverflow returns a-b. If ok is false, b was outside the
// int32 range or a-b overflowed.
func Sub32to64WithOverflow(a int32, b int64) (r int32, ok bool) {
	if b > math.MaxInt32 || b < math.MinInt32 {
		return 0, false
	}
	return Sub32WithOverflow(a, int32(b))
}

// Sub32WithOverflow returns a-b. If ok is false, a-b overflowed.
func Sub32WithOverflow(a, b int32) (r int32, ok bool) {
	if b < 0 && a > math.MaxInt32+b {
		return 0, false
	}
	if b > 0 && a < math.MinInt32+b {
		return 0, false
	}
	return a - b, true
}

// MulHalfPositiveWithOverflow returns a*b. b must be positive. If ok
// is false, a*b overflowed.
func MulHalfPositiveWithOverflow(a, b int64) (r int64, ok bool) {
	if a >= 0 {
		if a > math.MaxInt64/b {
			return 0, false
		}
	} else {
		if a < math.MinInt64/b {
			return 0, false
		}
	}
	return a * b, true
}
