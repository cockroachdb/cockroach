// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistinctCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	test := func(rowCount, maxDistinctCount uint64) {
		n, count := float64(maxDistinctCount), float64(0)
		var expected uint64
		// This calculation should produce the same result as the calculation
		// in DistinctCount, but it's easier to see how this is correct (it's also
		// much less efficient). For each row, we select a new value. The
		// probability that it hasn't been seen before is (n-count)/n, where count
		// is the total number of values seen so far, and n is the number of
		// possible values. This probability is also equivalent to the expected
		// value of the increase in distinct values seen so far, so we calculate
		// the expected total number of distinct values by summing this probability
		// over all rows.
		for i := uint64(0); i < rowCount && expected < maxDistinctCount; i++ {
			count += (n - count) / n
			expected = uint64(int64(math.Round(count)))
		}

		actual := DistinctCount(rowCount, maxDistinctCount)
		if expected != actual {
			t.Fatalf("For row count %d and max distinct count %d, expected distinct"+
				" count %d but found %d", rowCount, maxDistinctCount, expected, actual)
		}
	}

	for _, rowCount := range []uint64{0, 1, 10, 100, 1000} {
		for _, maxDistinctCount := range []uint64{1, 10, 100, 1000} {
			test(rowCount, maxDistinctCount)
		}
	}
}
