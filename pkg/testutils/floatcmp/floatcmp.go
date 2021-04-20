// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package floatcmp provides functions for determining float values to be equal
// if they are within a tolerance. It is designed to be used in tests.
package floatcmp

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	// CloseFraction can be used to set a "close" tolerance for the fraction
	// argument of functions in this package. It should typically be used with
	// the CloseMargin constant for the margin argument. Its value is taken from
	// the close tolerances in go's math package.
	CloseFraction float64 = 1e-14

	// CloseMargin can be used to set a "close" tolerance for the margin
	// argument of functions in this package. It should typically be used with
	// the CloseFraction constant for the fraction argument.
	//
	// It is set to the square of CloseFraction so it is only used when the
	// smaller of the absolute expected and actual values is in the range:
	//
	//   -CloseFraction <= 0 <= CloseFraction
	//
	// CloseMargin is greater than 0 otherwise if either expected or actual were
	// 0 the calculated tolerance from the fraction would be 0.
	CloseMargin float64 = CloseFraction * CloseFraction
)

// EqualApprox reports whether expected and actual are deeply equal with the
// following modifications for float64 and float32 types:
//
// • If both expected and actual are not NaN or infinate, they are equal within
// the larger of the relative fraction or absolute margin calculated from the
// fraction and margin arguments.
//
// • If both expected and actual are NaN, they are equal.
//
// Both fraction and margin must be non-negative.
//
// fraction is used to calculate the tolerance as a relative fraction of the
// smaller of expected and actual:
//
//   tolerance_frac = (fraction * min(|expected|, |actual|))
//
// margin specifies the tolerance as an absolute value:
//
//   tolerance_marg = margin
//
// The tolerance used to determine approximate equality is:
//
//   tolerance = max(tolerance_frac, tolerance_marg)
//
// To use only one of fraction or margin, set the other to 0.
//
// For comparing expected and actual values in tests, typically the fraction
// should be set to the smallest relative fraction to tolerate. The margin
// should be set to a much smaller value so that it is only used when:
//
//   (fraction * min(|expected|, |actual|)) < margin
//
// which allows expected and actual to be approximately equal within margin when
// either is 0.
func EqualApprox(expected interface{}, actual interface{}, fraction float64, margin float64) bool {
	return cmp.Equal(expected, actual, cmpopts.EquateApprox(fraction, margin), cmpopts.EquateNaNs())
}
