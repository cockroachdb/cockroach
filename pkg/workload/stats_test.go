// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package workload

import (
	"math"
	"testing"
)

func TestDistinctCount(t *testing.T) {
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

	test(1, 1)
	test(1, 10)
	test(1, 100)
	test(1, 1000)
	test(10, 1)
	test(10, 10)
	test(10, 100)
	test(10, 1000)
	test(100, 1)
	test(100, 10)
	test(100, 100)
	test(100, 1000)
	test(1000, 1)
	test(1000, 10)
	test(1000, 100)
	test(1000, 1000)
}
