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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSumInt(t *testing.T) {
	testCases := []struct {
		input    []tuple
		expected []tuple
		// batchSize if not 0 is passed in to newOpTestInput to divide the input.
		batchSize       uint16
		outputBatchSize uint16
		name            string
	}{
		{
			input: tuples{
				{true, 1},
			},
			expected: tuples{
				{1},
			},
			batchSize:       1,
			outputBatchSize: 1,
			name:            "OneTuple",
		},
		{
			input: tuples{
				{true, 1},
				{false, 2},
			},
			expected: tuples{
				{3},
			},
			name: "OneBatchOneGroup",
		},
		{
			input: tuples{
				{true, 1},
				{false, 0},
				{true, 3},
				{true, 1},
			},
			expected: tuples{
				{1},
				{3},
				{1},
			},
			name: "OneBatchMultiGroup",
		},
		{
			input: tuples{
				{true, 1},
				{false, 2},
				{false, 3},
				{true, 4},
				{false, 5},
			},
			expected: tuples{
				{6},
				{9},
			},
			batchSize: 1,
			name:      "CarryBetweenInputBatches",
		},
		{
			input: tuples{
				{true, 1},
				{false, 2},
				{false, 3},
				{false, 4},
				{true, 5},
				{true, 6},
			},
			expected: tuples{
				{10},
				{5},
				{6},
			},
			batchSize:       3,
			outputBatchSize: 1,
			name:            "CarryBetweenOutputBatches",
		},
	}

	// Run tests with deliberate batch sizes and no selection vectors.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batchSize := tc.batchSize
			if batchSize == 0 {
				batchSize = ColBatchSize
			}

			outputBatchSize := tc.outputBatchSize
			if outputBatchSize == 0 {
				outputBatchSize = ColBatchSize
			}

			tupleSource := newOpTestInput(batchSize, tc.input)
			a := &sumInt64Agg{
				input: tupleSource,
			}
			a.initWithBatchSize(outputBatchSize)

			out := newOpTestOutput(a, []int{0}, tc.expected)
			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}

	// Run test with random selection vectors and different batch sizes.
	for _, tc := range testCases {
		tc.name = fmt.Sprintf("%s/Random", tc.name)
		t.Run(tc.name, func(t *testing.T) {
			runTests(t, []tuples{tc.input}, nil, func(t *testing.T, input []Operator) {
				a := &sumInt64Agg{
					input: input[0],
				}
				outputBatchSize := tc.outputBatchSize
				if outputBatchSize == 0 {
					outputBatchSize = ColBatchSize
				}
				a.initWithBatchSize(outputBatchSize)

				out := newOpTestOutput(a, []int{0}, tc.expected)
				if err := out.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func BenchmarkSumInt(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	for _, groupSize := range []int{1, 2, ColBatchSize / 2, ColBatchSize} {
		for _, numInputBatches := range []int{1, 2, 32, 64} {
			batch := NewMemBatch([]types.T{types.Bool, types.Int64})
			groups, ints := batch.ColVec(0).Bool(), batch.ColVec(1).Int64()
			for i := 0; i < ColBatchSize; i++ {
				ints[i] = rng.Int63()
				groups[i] = false
				if groupSize == 1 || i%groupSize == 0 {
					groups[i] = true
				}
			}
			batch.SetLength(ColBatchSize)
			source := newRepeatableBatchSource(batch)

			a := &sumInt64Agg{
				input: source,
			}
			a.Init()

			b.Run(
				fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(b *testing.B) {
					// Only count the int64 column.
					b.SetBytes(int64(8 * ColBatchSize * numInputBatches))
					for i := 0; i < b.N; i++ {
						source.resetBatchesToReturn(numInputBatches)
						// Exhaust aggregator until all batches have been read.
						for b := a.Next(); b.Length() != 0; b = a.Next() {
						}
					}
				})
		}
	}
}
