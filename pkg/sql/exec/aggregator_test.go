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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// Mirrors AggregatorSpec_Func until we can import without a distsqlrun
// dependency.
const (
	AVG = 1
	SUM = 10
)

var (
	defaultGroupCols  = []uint32{0}
	defaultGroupTypes = []types.T{types.Int64}
	defaultAggCols    = [][]uint32{{1}}
	defaultAggTypes   = [][]types.T{{types.Int64}}
	defaultAggFns     = []int{SUM}
)

type aggregatorTestCase struct {
	// groupCols, groupTypes, and aggCols will be set to their default values
	// before running a test if nil.
	groupCols  []uint32
	groupTypes []types.T
	aggFns     []int
	aggCols    [][]uint32
	aggTypes   [][]types.T
	input      tuples
	expected   tuples
	// {output}BatchSize if not 0 are passed in to NewOrderedAggregator to
	// divide input/output batches.
	batchSize       int
	outputBatchSize int
	name            string

	// convToDecimal will convert any float64s to apd.Decimals. If a string is
	// encountered, a best effort is made to convert that string to an
	// apd.Decimal.
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
					case string:
						d := &apd.Decimal{}
						d, _, err := d.SetString(v)
						if err != nil {
							// If there was an error converting the string to decimal, just
							// leave the datum as is.
							continue
						}
						tuple[i] = *d
					}
				}
			}
		}
	}
	if tc.groupCols == nil {
		tc.groupCols = defaultGroupCols
	}
	if tc.groupTypes == nil {
		tc.groupTypes = defaultGroupTypes
	}
	if tc.aggFns == nil {
		tc.aggFns = defaultAggFns
	}
	if tc.aggCols == nil {
		tc.aggCols = defaultAggCols
	}
	if tc.aggTypes == nil {
		tc.aggTypes = defaultAggTypes
	}
	if tc.batchSize == 0 {
		tc.batchSize = ColBatchSize
	}
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = ColBatchSize
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
			a, err := NewOrderedAggregator(
				tupleSource, tc.groupCols, tc.groupTypes, tc.aggFns, tc.aggCols, tc.aggTypes,
			)
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
					a, err := NewOrderedAggregator(
						input[0], tc.groupCols, tc.groupTypes, tc.aggFns, tc.aggCols, tc.aggTypes,
					)
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
			aggFns: []int{SUM, SUM},
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
			aggFns: []int{SUM, SUM},
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
		{
			aggFns: []int{AVG, SUM},
			aggCols: [][]uint32{
				{1}, {1},
			},
			aggTypes: [][]types.T{
				{types.Decimal}, {types.Decimal},
			},
			input: tuples{
				{0, 1.1},
				{0, 1.2},
				{0, 2.3},
				{1, 6.21},
				{1, 2.43},
			},
			expected: tuples{
				{"1.5333333333333333333", 4.6},
				{4.32, 8.64},
			},
			name:          "AvgSumSingleInputBatch",
			convToDecimal: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/Randomized", tc.name), func(t *testing.T) {
			if err := tc.init(); err != nil {
				t.Fatal(err)
			}
			runTests(t, []tuples{tc.input}, nil, func(t *testing.T, input []Operator) {
				a, err := NewOrderedAggregator(
					input[0], tc.groupCols, tc.groupTypes, tc.aggFns, tc.aggCols, tc.aggTypes,
				)
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

func BenchmarkAggregator(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	for _, aggFn := range []int{SUM, AVG} {
		fName := ""
		switch aggFn {
		case AVG:
			fName = "AVG"
		case SUM:
			fName = "SUM"
		}
		b.Run(fName, func(b *testing.B) {
			for _, groupSize := range []int{1, 2, ColBatchSize / 2, ColBatchSize} {
				for _, numInputBatches := range []int{1, 2, 32, 64} {
					batch := NewMemBatch([]types.T{types.Int64, types.Decimal})
					groups, decimals := batch.ColVec(0).Int64(), batch.ColVec(1).Decimal()
					curGroup := 0
					for i := 0; i < ColBatchSize; i++ {
						decimals[i].SetInt64(rng.Int63())
						groups[i] = int64(curGroup)
						if groupSize == 1 || i%groupSize == 0 {
							curGroup++
						}
					}
					batch.SetLength(ColBatchSize)
					source := newRepeatableBatchSource(batch)

					a, err := NewOrderedAggregator(
						source,
						[]uint32{0},
						[]types.T{types.Int64},
						[]int{aggFn},
						[][]uint32{{1}},
						[][]types.T{{types.Decimal}},
					)
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
						},
					)
				}
			}
		})
	}
}

func TestHashAggregator(t *testing.T) {
	// Set up the apd.Decimal values used in tests.
	floats := []float64{1.3, 1.6, 0.5, 1.2, 3.4}
	decs := make([]apd.Decimal, len(floats))
	for i, f := range floats {
		_, err := decs[i].SetFloat64(f)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}

	tcs := []struct {
		input      tuples
		inputTypes []types.T

		groupCols []uint32
		aggFns    []int
		aggCols   [][]uint32

		expected tuples
	}{
		{
			// Test carry between output batches.
			input: tuples{
				{0, 1},
				{1, 5},
				{0, 4},
				{0, 2},
				{2, 6},
				{0, 3},
				{0, 7},
			},
			inputTypes: []types.T{types.Int64, types.Int64},

			aggFns:    []int{SUM},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{1}},

			expected: tuples{
				{5},
				{6},
				{17},
			},
		},
		{
			// Test a single row input source.
			input: tuples{
				{5},
			},
			inputTypes: []types.T{types.Int64},

			aggFns:    []int{SUM},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}},

			expected: tuples{
				{5},
			},
		},
		{
			// Test bucket collisions.
			input: tuples{
				{0, 3},
				{0, 4},
				{hashTableBucketSize, 6},
				{0, 5},
				{hashTableBucketSize, 7},
			},
			inputTypes: []types.T{types.Int64, types.Int64},

			aggFns:    []int{SUM},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{1}},

			expected: tuples{
				{12},
				{13},
			},
		},
		{
			input: tuples{
				{0, 1, decs[0]},
				{0, 1, decs[1]},
				{0, 1, decs[2]},
				{1, 1, decs[3]},
			},
			inputTypes: []types.T{types.Int64, types.Int64, types.Decimal},

			aggFns:    []int{SUM, SUM},
			groupCols: []uint32{0, 1},
			aggCols: [][]uint32{
				{2}, {1},
			},

			expected: tuples{
				{decs[4], 3},
				{decs[3], 1},
			},
		},
		{
			// Test unused input columns.
			input: tuples{
				{0, 1, 2, 3},
				{0, 1, 4, 5},
				{1, 1, 3, 7},
				{1, 2, 4, 9},
				{0, 1, 6, 11},
				{1, 2, 6, 13},
			},
			inputTypes: []types.T{types.Int64, types.Int64, types.Int64, types.Int64},

			aggFns:    []int{SUM},
			groupCols: []uint32{0, 1},
			aggCols:   [][]uint32{{3}},

			expected: tuples{
				{7},
				{19},
				{22},
			},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.input}, []types.T{types.Bool}, func(t *testing.T, sources []Operator) {
			ag, err := NewHashedAggregator(sources[0], tc.aggFns, tc.groupCols, tc.aggCols, tc.inputTypes)

			if err != nil {
				t.Fatal(err)
			}

			nOutput := len(tc.aggCols)
			cols := make([]int, nOutput)
			for i := 0; i < nOutput; i++ {
				cols[i] = i
			}

			out := newOpTestOutput(ag, cols, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkHashAggregator(b *testing.B) {
	nCols := 4
	sourceTypes := make([]types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int64
	}

	batch := NewMemBatch(sourceTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < ColBatchSize; i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(ColBatchSize)

	for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", nBatches*ColBatchSize), func(b *testing.B) {
			b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source := newFiniteBatchSource(batch, nBatches)
				agg, err := NewHashedAggregator(source, []int{SUM, SUM}, []uint32{0, 2}, [][]uint32{{1}, {3}}, sourceTypes)

				if err != nil {
					b.Fatal(err)
				}

				agg.Init()

				for i := 0; i < nBatches; i++ {
					agg.Next()
				}
			}
		})
	}
}
