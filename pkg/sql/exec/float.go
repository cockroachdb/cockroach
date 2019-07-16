// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import "math"

// compareFloats compares two float values. This function is necessary for NaN
// handling. In SQL, NaN is treated as less than all other float values. In Go,
// any comparison with NaN returns false.
func compareFloats(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	// Compare bits so that NaN == NaN.
	if math.Float64bits(a) == math.Float64bits(b) {
		return 0
	}
	// Either a or b is NaN.
	if math.IsNaN(a) {
		return -1
	}
	return 1
}
