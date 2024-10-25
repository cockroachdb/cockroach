// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import "math"

// Abs returns the absolute value of x.
//
// Special cases are:
//
//	Abs(±Inf) = +Inf
//	Abs(NaN) = NaN
func Abs(x float32) float32 {
	return math.Float32frombits(math.Float32bits(x) &^ (1 << 31))
}

// Sqrt returns the square root of x.
//
// Special cases are:
//
//	Sqrt(+Inf) = +Inf
//	Sqrt(±0) = ±0
//	Sqrt(x < 0) = NaN
//	Sqrt(NaN) = NaN
func Sqrt(x float32) float32 {
	// For now, use the float64 Sqrt operation.
	// TODO(andyk): Consider using the SQRTSS instruction like gonum does
	// internally.
	return float32(math.Sqrt(float64(x)))
}

// IsNaN reports whether f is an IEEE 754 “not-a-number” value.
func IsNaN(v float32) bool {
	// IEEE 754 says that only NaNs satisfy f != f.
	return v != v
}
