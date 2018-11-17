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

	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	defaultGroupCols  = []uint32{0}
	defaultGroupTypes = []types.T{types.Int64}
	defaultAggCols    = [][]uint32{{1}}
	defaultAggTypes   = [][]types.T{{types.Int64}}
)

type aggregatorTestCase struct {
	// groupCols, groupTypes, and aggCols will be set to their default values
	// before running a test if nil.
	groupCols  []uint32
	groupTypes []types.T
	aggCols    [][]uint32
	aggTypes   [][]types.T
	input      tuples
	expected   tuples
	// {output}BatchSize if not 0 are passed in to NewOrderedAggregator to
	// divide input/output batches.
	batchSize       int
	outputBatchSize int
	name            string

	// convToDecimal will convert any float64s to apd.Decimal{}s.
	convToDecimal bool
}

func (tc *aggregatorTestCase) init() error {
	if tc.convToDecimal {
		for _, tuples := range []tuples{tc.input, tc.expected} {
			for _, tuple := range tuples {
				for i, e := range tuple {
					switch v := e.(type) {
					case float64:
						d := &apd.Decimal{}
						d, err := d.SetFloat64(v)
						if err != nil {
							return err
						}
						tuple[i] = *d
					}
				}
			}
		}
	}
	groupCols := defaultGroupCols
	if tc.groupCols == nil {
		tc.groupCols = groupCols
	}
	groupTypes := defaultGroupTypes
	if tc.groupTypes == nil {
		tc.groupTypes = groupTypes
	}
	aggCols := defaultAggCols
	if tc.aggCols == nil {
		tc.aggCols = aggCols
	}
	aggTypes := defaultAggTypes
	if tc.aggTypes == nil {
		tc.aggTypes = aggTypes
	}
	batchSize := ColBatchSize
	if tc.batchSize == 0 {
		tc.batchSize = batchSize
	}
	outputBatchSize := ColBatchSize
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = outputBatchSize
	}
	return nil
}

func TestAggregatorOneFunc(t *testing.T) {
	testCases := []aggregatorTestCase{
		{
			input: tuples{
				{0, 1},
			},
			expected: tuples{
				{1},
			},
			name:            "OneTuple",
			outputBatchSize: 4,
		},
		{
			input: tuples{
				{0, 1},
				{0, 1},
			},
			expected: tuples{
				{2},
			},
			name: "OneGroup",
		},
		{
			input: tuples{
				{0, 1},
				{0, 0},
				{0, 1},
				{1, 4},
				{2, 5},
			},
			expected: tuples{
				{2},
				{4},
				{5},
			},
			batchSize: 2,
			name:      "MultiGroup",
		},
		{
			input: tuples{
				{0, 1},
				{0, 2},
				{0, 3},
				{1, 4},
				{1, 5},
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
				{0, 1},
				{0, 2},
				{0, 3},
				{0, 4},
				{1, 5},
				{2, 6},
			},
			expected: tuples{
				{10},
				{5},
				{6},
			},
			batchSize:       2,
			outputBatchSize: 1,
			name:            "CarryBetweenOutputBatches",
		},
		{
			input: tuples{
				{0, 1},
				{0, 1},
				{1, 2},
				{2, 3},
				{2, 3},
				{3, 4},
				{3, 4},
				{4, 5},
				{5, 6},
				{6, 7},
				{7, 8},
			},
			expected: tuples{
				{2},
				{2},
				{6},
				{8},
				{5},
				{6},
				{7},
				{8},
			},
			batchSize:       4,
			outputBatchSize: 1,
			name:            "CarryBetweenInputAndOutputBatches",
		},
		{
			input: tuples{
				{0, 1},
				{0, 2},
				{0, 3},
				{0, 4},
			},
			expected: tuples{
				{10},
			},
			batchSize:       1,
			outputBatchSize: 1,
			name:            "NoGroupingCols",
			groupCols:       []uint32{},
			groupTypes:      []types.T{},
		},
	}

	// Run tests with deliberate batch sizes and no selection vectors.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.init(); err != nil {
				t.Fatal(err)
			}

			tupleSource := newOpTestInput(uint16(tc.batchSize), tc.input)
			a, err := NewOrderedAggregator(tupleSource, tc.groupCols, tc.groupTypes, tc.aggCols, tc.aggTypes)
			if err != nil {
				t.Fatal(err)
			}

			out := newOpTestOutput(a, []int{0}, tc.expected)
			// Explicitly reinitialize the aggregator with the given output batch
			// size.
			a.(*orderedAggregator).initWithBatchSize(tc.batchSize, tc.outputBatchSize)
			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}

			// Run randomized tests on this test case.
			t.Run(fmt.Sprintf("Randomized"), func(t *testing.T) {
				runTests(t, []tuples{tc.input}, nil, func(t *testing.T, input []Operator) {
					a, err := NewOrderedAggregator(input[0], tc.groupCols, tc.groupTypes, tc.aggCols, tc.aggTypes)
					if err != nil {
						t.Fatal(err)
					}
					out := newOpTestOutput(a, []int{0}, tc.expected)
					if err := out.Verify(); err != nil {
						t.Fatal(err)
					}
				})
			})
		})
	}
}

func TestAggregatorMultiFunc(t *testing.T) {
	testCases := []aggregatorTestCase{
		{
			aggCols: [][]uint32{
				{2}, {1},
			},
			aggTypes: [][]types.T{
				{types.Int64}, {types.Int64},
			},
			input: tuples{
				{0, 1, 2},
				{0, 1, 2},
			},
			expected: tuples{
				{4, 2},
			},
			name: "OutputOrder",
		},
		{
			aggCols: [][]uint32{
				{2}, {1},
			},
			aggTypes: [][]types.T{
				{types.Decimal}, {types.Int64},
			},
			input: tuples{
				{0, 1, 1.3},
				{0, 1, 1.6},
				{0, 1, 0.5},
				{1, 1, 1.2},
			},
			expected: tuples{
				{3.4, 3},
				{1.2, 1},
			},
			name:          "SumMultiType",
			convToDecimal: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/Randomized", tc.name), func(t *testing.T) {
			if err := tc.init(); err != nil {
				t.Fatal(err)
			}
			runTests(t, []tuples{tc.input}, nil, func(t *testing.T, input []Operator) {
				a, err := NewOrderedAggregator(input[0], tc.groupCols, tc.groupTypes, tc.aggCols, tc.aggTypes)
				if err != nil {
					t.Fatal(err)
				}
				out := newOpTestOutput(a, []int{0, 1}, tc.expected)
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
			batch := NewMemBatch([]types.T{types.Int64, types.Int64})
			groups, ints := batch.ColVec(0).Int64(), batch.ColVec(1).Int64()
			curGroup := 0
			for i := 0; i < ColBatchSize; i++ {
				ints[i] = rng.Int63()
				groups[i] = int64(curGroup)
				if groupSize == 1 || i%groupSize == 0 {
					curGroup++
				}
			}
			batch.SetLength(ColBatchSize)
			source := newRepeatableBatchSource(batch)

			a, err := NewOrderedAggregator(source, []uint32{0}, []types.T{types.Int64}, [][]uint32{{1}}, [][]types.T{{types.Int64}})
			if err != nil {
				b.Fatal(err)
			}
			a.Init()

			b.Run(
				fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(b *testing.B) {
					// Only count the int64 column.
					b.SetBytes(int64(8 * ColBatchSize * numInputBatches))
					for i := 0; i < b.N; i++ {
						a.(*orderedAggregator).Reset()
						source.resetBatchesToReturn(numInputBatches)
						// Exhaust aggregator until all batches have been read.
						for b := a.Next(); b.Length() != 0; b = a.Next() {
						}
					}
				})
		}
	}
}
