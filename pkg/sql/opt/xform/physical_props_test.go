// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import "testing"

// TestDistinctOnLimitHint verifies that distinctOnLimitHint never returns a
// negative result.
func TestDistinctOnLimitHint(t *testing.T) {
	// Generate a list of test values.
	var values []float64
	for _, v := range []float64{0, 1e-10, 1e-5, 0.1, 1, 10, 1e5, 1e10} {
		for _, n := range []float64{-1e10, +1e10, -1e7, +1e7} {
			if v+n >= 0 {
				values = append(values, v+n)
			}
		}
	}

	for _, distinctCount := range values {
		for _, neededRows := range values {
			if hint := distinctOnLimitHint(distinctCount, neededRows); hint < 0 {
				t.Fatalf("distinctOnLimitHint(%g,%g)=%g", distinctCount, neededRows, hint)
			}
		}
	}
}
