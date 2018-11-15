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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	defaultGroupCols = []uint32{0}
	defaultAggCols   = [][]uint32{{1}}
	defaultAggFns    = []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_SUM}
	defaultColTyps   = []types.T{types.Int64, types.Int64}
)

type aggregatorTestCase struct {
	// groupCols, groupTypes, and aggCols will be set to their default values
	// before running a test if nil.
	groupCols []uint32
	colTypes  []types.T
	aggFns    []distsqlpb.AggregatorSpec_Func
	aggCols   [][]uint32
	input     tuples
	expected  tuples
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
	if tc.aggFns == nil {
		tc.aggFns = defaultAggFns
	}
	if tc.aggCols == nil {
		tc.aggCols = defaultAggCols
	}
	if tc.colTypes == nil {
		tc.colTypes = defaultColTyps
	}
	if tc.batchSize == 0 {
		tc.batchSize = coldata.BatchSize
	}
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = coldata.BatchSize
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
				tupleSource,
				tc.colTypes,
				tc.aggFns,
				tc.groupCols,
				tc.aggCols,
			)
			if err != nil {
				t.Fatal(err)
			}

			out := newOpTestOutput(a, []int{0}, tc.expected)
			// Explicitly reinitialize the aggregator with the given output batch
			// size.
			a.(*orderedAggregator).initWithBatchSize(tc.batchSize, tc.outputBatchSize)
			if err := out.VerifyAnyOrder(); err != nil {
				t.Fatal(err)
			}

			// Run randomized tests on this test case.
			t.Run(fmt.Sprintf("Randomized"), func(t *testing.T) {
				runTests(t, []tuples{tc.input}, func(t *testing.T, input []Operator) {
					a, err := NewOrderedAggregator(
						input[0],
						tc.colTypes,
						tc.aggFns,
						tc.groupCols,
						tc.aggCols,
					)
					if err != nil {
						t.Fatal(err)
					}
					out := newOpTestOutput(a, []int{0}, tc.expected)
					if err := out.VerifyAnyOrder(); err != nil {
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
			aggFns: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_SUM, distsqlpb.AggregatorSpec_SUM},
			aggCols: [][]uint32{
				{2}, {1},
			},
			input: tuples{
				{0, 1, 2},
				{0, 1, 2},
			},
			colTypes: []types.T{types.Int64, types.Int64, types.Int64},
			expected: tuples{
				{4, 2},
			},
			name: "OutputOrder",
		},
		{
			aggFns: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_SUM, distsqlpb.AggregatorSpec_SUM},
			aggCols: [][]uint32{
				{2}, {1},
			},
			input: tuples{
				{0, 1, 1.3},
				{0, 1, 1.6},
				{0, 1, 0.5},
				{1, 1, 1.2},
			},
			colTypes: []types.T{types.Int64, types.Int64, types.Decimal},
			expected: tuples{
				{3.4, 3},
				{1.2, 1},
			},
			name:          "SumMultiType",
			convToDecimal: true,
		},
		{
			aggFns: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_AVG, distsqlpb.AggregatorSpec_SUM},
			aggCols: [][]uint32{
				{1}, {1},
			},
			input: tuples{
				{0, 1.1},
				{0, 1.2},
				{0, 2.3},
				{1, 6.21},
				{1, 2.43},
			},
			colTypes: []types.T{types.Int64, types.Decimal},
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
			runTests(t, []tuples{tc.input}, func(t *testing.T, input []Operator) {
				a, err := NewOrderedAggregator(
					input[0],
					tc.colTypes,
					tc.aggFns,
					tc.groupCols,
					tc.aggCols,
				)
				if err != nil {
					t.Fatal(err)
				}
				out := newOpTestOutput(a, []int{0, 1}, tc.expected)
				if err := out.VerifyAnyOrder(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestAggregatorKitchenSink(t *testing.T) {
	testCases := []aggregatorTestCase{
		{
			aggFns: []distsqlpb.AggregatorSpec_Func{
				distsqlpb.AggregatorSpec_ANY_NOT_NULL,
				distsqlpb.AggregatorSpec_AVG,
				distsqlpb.AggregatorSpec_COUNT_ROWS,
				distsqlpb.AggregatorSpec_SUM,
			},
			aggCols:  [][]uint32{{0}, {1}, {}, {2}},
			colTypes: []types.T{types.Int64, types.Decimal, types.Int64},
			input: tuples{
				{0, 3.1, 2},
				{0, 1.1, 3},
				{1, 1.1, 1},
				{1, 4.1, 0},
				{2, 1.1, 1},
				{3, 4.1, 0},
				{3, 5.1, 0},
			},
			expected: tuples{
				{0, 2.1, 2, 5},
				{1, 2.6, 2, 1},
				{2, 1.1, 1, 1},
				{3, 4.6, 2, 0},
			},
			convToDecimal: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			if err := tc.init(); err != nil {
				t.Fatal(err)
			}
			runTests(t, []tuples{tc.input}, func(t *testing.T, input []Operator) {
				a, err := NewOrderedAggregator(
					input[0], tc.colTypes, tc.aggFns, tc.groupCols, tc.aggCols,
				)
				if err != nil {
					t.Fatal(err)
				}
				out := newOpTestOutput(a, []int{0, 1, 2, 3}, tc.expected)
				if err := out.VerifyAnyOrder(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestAggregatorRandomCountSum(t *testing.T) {
	// This test sums and counts random inputs, keeping track of the expected
	// results to make sure the aggregations are correct.
	rng, _ := randutil.NewPseudoRand()
	for _, groupSize := range []int{1, 2, coldata.BatchSize / 4, coldata.BatchSize / 2} {
		for _, numInputBatches := range []int{1, 2, 64} {
			t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(t *testing.T) {
					batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64})
					groups, col := batch.ColVec(0).Int64(), batch.ColVec(1).Int64()
					var expCounts, expSums []int64
					curGroup := -1
					for i := 0; i < coldata.BatchSize; i++ {
						if i%groupSize == 0 {
							expCounts = append(expCounts, int64(groupSize))
							expSums = append(expSums, 0)
							curGroup++
						}
						col[i] = rng.Int63() % 1024
						expSums[len(expSums)-1] += col[i]
						groups[i] = int64(curGroup)
					}
					batch.SetLength(coldata.BatchSize)
					source := newRepeatableBatchSource(batch)
					source.resetBatchesToReturn(numInputBatches)
					a, err := NewOrderedAggregator(
						source,
						[]types.T{types.Int64, types.Int64},
						[]distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_COUNT_ROWS, distsqlpb.AggregatorSpec_SUM_INT},
						[]uint32{0},
						[][]uint32{{}, {1}},
					)
					if err != nil {
						t.Fatal(err)
					}
					a.Init()

					// Exhaust aggregator until all batches have been read.
					i := 0
					for b := a.Next(); b.Length() != 0; b = a.Next() {
						countCol := b.ColVec(0).Int64()
						sumCol := b.ColVec(1).Int64()
						for j := uint16(0); j < b.Length(); j++ {
							count := countCol[j]
							sum := sumCol[j]
							expCount := expCounts[int(j)%len(expCounts)]
							if count != expCount {
								t.Fatalf("Found count %d, expected %d, idx %d of batch %d", count, expCount, j, i)
							}
							expSum := expSums[int(j)%len(expSums)]
							if sum != expSum {
								t.Fatalf("Found sum %d, expected %d, idx %d of batch %d", sum, expSum, j, i)
							}
						}
						i++
					}
					totalInputRows := numInputBatches * coldata.BatchSize
					nOutputRows := totalInputRows / groupSize
					expBatches := (nOutputRows / coldata.BatchSize)
					if nOutputRows%coldata.BatchSize != 0 {
						expBatches++
					}
					if i != expBatches {
						t.Fatalf("expected %d batches, found %d", expBatches, i)
					}
				})
		}
	}
}

func BenchmarkAggregator(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	for _, aggFn := range []distsqlpb.AggregatorSpec_Func{
		distsqlpb.AggregatorSpec_ANY_NOT_NULL,
		distsqlpb.AggregatorSpec_AVG,
		distsqlpb.AggregatorSpec_COUNT_ROWS,
		distsqlpb.AggregatorSpec_SUM,
	} {
		fName := distsqlpb.AggregatorSpec_Func_name[int32(aggFn)]
		b.Run(fName, func(b *testing.B) {
			for _, groupSize := range []int{1, 2, coldata.BatchSize / 2, coldata.BatchSize} {
				for _, numInputBatches := range []int{1, 2, 64} {
					colTypes := []types.T{types.Int64, types.Decimal}
					batch := coldata.NewMemBatch(colTypes)
					groups, decimals := batch.ColVec(0).Int64(), batch.ColVec(1).Decimal()
					curGroup := 0
					for i := 0; i < coldata.BatchSize; i++ {
						decimals[i].SetInt64(rng.Int63())
						groups[i] = int64(curGroup)
						if groupSize == 1 || i%groupSize == 0 {
							curGroup++
						}
					}
					batch.SetLength(coldata.BatchSize)
					source := newRepeatableBatchSource(batch)

					nCols := 1
					if aggFn == distsqlpb.AggregatorSpec_COUNT_ROWS {
						nCols = 0
					}
					a, err := NewOrderedAggregator(
						source,
						colTypes,
						[]distsqlpb.AggregatorSpec_Func{aggFn},
						[]uint32{0},
						[][]uint32{[]uint32{1}[:nCols]},
					)
					if err != nil {
						b.Fatal(err)
					}
					a.Init()

					b.Run(
						fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
						func(b *testing.B) {
							// Only count the int64 column.
							b.SetBytes(int64(8 * coldata.BatchSize * numInputBatches))
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
	tcs := []aggregatorTestCase{
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
			colTypes:  []types.T{types.Int64, types.Int64},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{1}},

			expected: tuples{
				{5},
				{6},
				{17},
			},

			name: "carryBetweenBatches",
		},
		{
			// Test a single row input source.
			input: tuples{
				{5},
			},
			colTypes:  []types.T{types.Int64},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}},

			expected: tuples{
				{5},
			},

			name: "singleRowInput",
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
			colTypes:  []types.T{types.Int64, types.Int64},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{1}},

			expected: tuples{
				{12},
				{13},
			},

			name: "bucketCollision",
		},
		{
			input: tuples{
				{0, 1, 1.3},
				{0, 1, 1.6},
				{0, 1, 0.5},
				{1, 1, 1.2},
			},
			colTypes:      []types.T{types.Int64, types.Int64, types.Decimal},
			convToDecimal: true,

			aggFns:    []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_SUM, distsqlpb.AggregatorSpec_SUM},
			groupCols: []uint32{0, 1},
			aggCols: [][]uint32{
				{2}, {1},
			},

			expected: tuples{
				{3.4, 3},
				{1.2, 1},
			},

			name: "decimalSums",
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
			colTypes:  []types.T{types.Int64, types.Int64, types.Int64, types.Int64},
			groupCols: []uint32{0, 1},
			aggCols:   [][]uint32{{3}},

			expected: tuples{
				{7},
				{19},
				{22},
			},

			name: "unusedInputCol",
		},
	}

	for _, tc := range tcs {
		if err := tc.init(); err != nil {
			t.Fatal(err)
		}
		runTests(t, []tuples{tc.input}, func(t *testing.T, sources []Operator) {
			ag, err := NewHashAggregator(sources[0], tc.colTypes, tc.aggFns, tc.groupCols, tc.aggCols)

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
	rng, _ := randutil.NewPseudoRand()

	nCols := 2
	sourceTypes := []types.T{types.Int64, types.Decimal}

	batch := coldata.NewMemBatch(sourceTypes)
	groups, decimals := batch.ColVec(0).Int64(), batch.ColVec(1).Decimal()
	for i := 0; i < coldata.BatchSize; i++ {
		decimals[i].SetInt64(rng.Int63())
		groups[i] = int64(i)
	}

	batch.SetLength(coldata.BatchSize)
	source := newRepeatableBatchSource(batch)

	for _, aggFn := range []distsqlpb.AggregatorSpec_Func{
		distsqlpb.AggregatorSpec_ANY_NOT_NULL,
		distsqlpb.AggregatorSpec_AVG,
		distsqlpb.AggregatorSpec_COUNT_ROWS,
		distsqlpb.AggregatorSpec_SUM,
	} {
		fName := distsqlpb.AggregatorSpec_Func_name[int32(aggFn)]
		b.Run(fName, func(b *testing.B) {
			for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8} {
				b.Run(fmt.Sprintf("rows=%d", nBatches*coldata.BatchSize), func(b *testing.B) {
					b.SetBytes(int64(8 * nBatches * coldata.BatchSize * nCols))
					b.ResetTimer()

					agg, err := NewHashAggregator(source, sourceTypes, []distsqlpb.AggregatorSpec_Func{aggFn}, []uint32{0}, [][]uint32{{1}})
					if err != nil {
						b.Fatal(err)
					}
					agg.Init()

					for i := 0; i < b.N; i++ {
						agg.(*hashAggregator).Reset()
						source.resetBatchesToReturn(nBatches)

						for b := agg.Next(); b.Length() != 0; b = agg.Next() {
						}
					}
				})
			}
		})
	}
}
