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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestSortedDistinct(t *testing.T) {
	tcs := []struct {
		distinctCols []uint32
		colTypes     []types.T
		numCols      int
		tuples       []tuple
		expected     []tuple
	}{
		{
			distinctCols: []uint32{0, 1, 2},
			colTypes:     []types.T{types.Int64, types.Int64, types.Int64},
			numCols:      4,
			tuples: tuples{
				{1, 2, 3, 4},
				{1, 2, 3, 5},
				{2, 2, 3, 4},
				{2, 3, 3, 4},
				{2, 3, 4, 4},
				{2, 3, 4, 4},
			},
			expected: tuples{
				{1, 2, 3, 4},
				{2, 2, 3, 4},
				{2, 3, 3, 4},
				{2, 3, 4, 4},
			},
		},
	}

	for _, tc := range tcs {
		runTests(t, tc.tuples, []types.T{}, func(t *testing.T, input Operator) {
			distinct, err := NewOrderedDistinct(input, tc.distinctCols, tc.colTypes)
			if err != nil {
				t.Fatal(err)
			}
			out := newOpTestOutput(distinct, []int{0, 1, 2, 3}, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkSortedDistinct(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	batch := NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	aCol := batch.ColVec(1).Int64()
	bCol := batch.ColVec(2).Int64()
	lastA := int64(0)
	lastB := int64(0)
	for i := 0; i < ColBatchSize; i++ {
		// 1/4 chance of changing each distinct col.
		if rng.Float64() > 0.75 {
			lastA++
		}
		if rng.Float64() > 0.75 {
			lastB++
		}
		aCol[i] = lastA
		bCol[i] = lastB
	}
	batch.SetLength(ColBatchSize)
	source := newRepeatableBatchSource(batch)
	source.Init()

	distinct, err := NewOrderedDistinct(source, []uint32{1, 2}, []types.T{types.Int64, types.Int64})
	if err != nil {
		b.Fatal(err)
	}

	// don't count the artificial zeroOp'd column in the throughput
	b.SetBytes(int64(8 * ColBatchSize * 3))
	for i := 0; i < b.N; i++ {
		distinct.Next()
	}
}
