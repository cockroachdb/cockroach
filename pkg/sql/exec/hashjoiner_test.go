// Copyright 2018 The Cockroach Authors.
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

package exec

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestHashJoinerInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		leftTypes  []types.T
		rightTypes []types.T

		leftTuples  tuples
		rightTuples tuples

		leftEqCol  int
		rightEqCol int

		leftOutCols  []int
		rightOutCols []int

		expectedTuples tuples
	}{
		{
			// test handling of various output column types.
			leftTypes:  []types.T{types.Bool, types.Int64, types.Bytes, types.Int64},
			rightTypes: []types.T{types.Int64, types.Float64, types.Int32},

			leftTuples: tuples{
				{false, 5, "a", 10},
				{true, 3, "b", 30},
				{false, 2, "foo", 20},
				{false, 6, "bar", 50},
			},
			rightTuples: tuples{
				{1, 1.1, int32(1)},
				{2, 2.2, int32(2)},
				{3, 3.3, int32(4)},
				{4, 4.4, int32(8)},
				{5, 5.5, int32(16)},
			},

			leftEqCol:    1,
			rightEqCol:   0,
			leftOutCols:  []int{1, 2},
			rightOutCols: []int{0, 2},

			expectedTuples: tuples{
				{2, "foo", 2, int32(2)},
				{3, "b", 3, int32(4)},
				{5, "a", 5, int32(16)},
			},
		},
		{
			leftTypes:  []types.T{types.Int64},
			rightTypes: []types.T{types.Int64},

			// reverse engineering hash table hash heuristic to find key values that
			// hash to the same bucket.
			leftTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 2},
				{hashTableBucketSize * 3},
			},
			rightTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 3},
			},

			leftEqCol:    0,
			rightEqCol:   0,
			leftOutCols:  []int{0},
			rightOutCols: []int{},

			expectedTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 3},
			},
		},
		{
			leftTypes:  []types.T{types.Int64},
			rightTypes: []types.T{types.Int64},

			// test a N:1 inner join where the right side key has duplicate values.
			leftTuples: tuples{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			rightTuples: tuples{
				{1},
				{1},
				{1},
				{2},
				{2},
			},

			leftEqCol:    0,
			rightEqCol:   0,
			leftOutCols:  []int{0},
			rightOutCols: []int{0},

			expectedTuples: tuples{
				{1, 1},
				{1, 1},
				{1, 1},
				{2, 2},
				{2, 2},
			},
		},
	}

	for _, tc := range tcs {
		spec := &hashJoinerSpec{
			leftSourceTypes:  tc.leftTypes,
			rightSourceTypes: tc.rightTypes,

			leftEqCol:  tc.leftEqCol,
			rightEqCol: tc.rightEqCol,

			leftOutCols:  tc.leftOutCols,
			rightOutCols: tc.rightOutCols,
		}

		for _, batchSize := range []uint16{1, 2, 3, 4, 16, 1024} {
			t.Run(fmt.Sprintf("batchSize=%d", batchSize), func(t *testing.T) {

				leftSource := newOpTestInput(tc.leftTuples)
				rightSource := newOpTestInput(tc.rightTuples)

				hj := &hashJoinerInt64Op{
					leftSource:  leftSource,
					rightSource: rightSource,
					spec:        spec,
				}

				nOutCols := len(tc.leftOutCols) + len(tc.rightOutCols)
				cols := make([]int, nOutCols)
				for i := 0; i < nOutCols; i++ {
					cols[i] = i
				}

				out := newOpTestOutput(hj, cols, tc.expectedTuples)

				if err := out.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func BenchmarkHashJoiner(b *testing.B) {

}
