// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

// Miscellaneous helper algorithms

import (
	"fmt"
)

func maxint(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minint(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sumint(xs []int) int {
	sum := 0
	for _, x := range xs {
		sum += x
	}
	return sum
}

// bisect returns an x in [low, high] such that |f(x)| <= tolerance
// using the bisection method.
//
// f(low) and f(high) must have opposite signs.
//
// If f does not have a root in this interval (e.g., it is
// discontiguous), this returns the X of the apparent discontinuity
// and false.
func bisect(f func(float64) float64, low, high, tolerance float64) (float64, bool) {
	flow, fhigh := f(low), f(high)
	if -tolerance <= flow && flow <= tolerance {
		return low, true
	}
	if -tolerance <= fhigh && fhigh <= tolerance {
		return high, true
	}
	if mathSign(flow) == mathSign(fhigh) {
		panic(fmt.Sprintf("root of f is not bracketed by [low, high]; f(%g)=%g f(%g)=%g", low, flow, high, fhigh))
	}
	for {
		mid := (high + low) / 2
		fmid := f(mid)
		if -tolerance <= fmid && fmid <= tolerance {
			return mid, true
		}
		if mid == high || mid == low {
			return mid, false
		}
		if mathSign(fmid) == mathSign(flow) {
			low = mid
			flow = fmid
		} else {
			high = mid
			fhigh = fmid
		}
	}
}

// bisectBool implements the bisection method on a boolean function.
// It returns x1, x2 ∈ [low, high], x1 < x2 such that f(x1) != f(x2)
// and x2 - x1 <= xtol.
//
// If f(low) == f(high), it panics.
func bisectBool(f func(float64) bool, low, high, xtol float64) (x1, x2 float64) {
	flow, fhigh := f(low), f(high)
	if flow == fhigh {
		panic(fmt.Sprintf("root of f is not bracketed by [low, high]; f(%g)=%v f(%g)=%v", low, flow, high, fhigh))
	}
	for {
		if high-low <= xtol {
			return low, high
		}
		mid := (high + low) / 2
		if mid == high || mid == low {
			return low, high
		}
		fmid := f(mid)
		if fmid == flow {
			low = mid
			flow = fmid
		} else {
			high = mid
			fhigh = fmid
		}
	}
}

// series returns the sum of the series f(0), f(1), ...
//
// This implementation is fast, but subject to round-off error.
func series(f func(float64) float64) float64 {
	y, yp := 0.0, 1.0
	for n := 0.0; y != yp; n++ {
		yp = y
		y += f(n)
	}
	return y
}
