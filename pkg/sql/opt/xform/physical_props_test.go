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

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestDistinctOnLimitHint verifies that distinctOnLimitHint never returns a
// negative result.
func TestDistinctOnLimitHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Generate a list of test values. We want to try the 0 value, very small
	// values, very large values, and cases where the two values are ~1 off from
	// each other, or very close to each other.
	var values []float64
	for _, v := range []float64{0, 1e-10, 1e-5, 0.1, 1, 2, 3, 10, 1e5, 1e10} {
		values = append(values, v)
		for _, noise := range []float64{1e-10, 1e-7, 0.1, 1} {
			values = append(values, v+noise)
			if v-noise >= 0 {
				values = append(values, v-noise)
			}
		}
	}

	for _, distinctCount := range values {
		for _, neededRows := range values {
			hint := distinctOnLimitHint(distinctCount, neededRows)
			if hint < 0 || math.IsNaN(hint) || math.IsInf(hint, +1) {
				t.Fatalf("distinctOnLimitHint(%g,%g)=%g", distinctCount, neededRows, hint)
			}
		}
	}
}
