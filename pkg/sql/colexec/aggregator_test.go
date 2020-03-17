// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	defaultGroupCols = []uint32{0}
	defaultAggCols   = [][]uint32{{1}}
	defaultAggFns    = []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM}
	defaultColTyps   = []coltypes.T{coltypes.Int64, coltypes.Int64}
)

type aggregatorTestCase struct {
	// colTypes, aggFns, groupCols, and aggCols will be set to their default
	// values before running a test if nil.
	colTypes       []coltypes.T
	aggFns         []execinfrapb.AggregatorSpec_Func
	groupCols      []uint32
	aggCols        [][]uint32
	input          tuples
	unorderedInput bool
	expected       tuples
	// {output}BatchSize() if not 0 are passed in to NewOrderedAggregator to
	// divide input/output batches.
	batchSize       int
	outputBatchSize int
	name            string

	// convToDecimal will convert any float64s to apd.Decimals. If a string is
	// encountered, a best effort is made to convert that string to an
	// apd.Decimal.
	convToDecimal bool
}

// aggType is a helper struct that allows tests to test both the ordered and
// hash aggregators at the same time.
type aggType struct {
	new func(
		allocator *Allocator,
		input Operator,
		colTypes []coltypes.T,
		aggFns []execinfrapb.AggregatorSpec_Func,
		groupCols []uint32,
		aggCols [][]uint32,
		isScalar bool,
	) (Operator, error)
	name string
}

var aggTypes = []aggType{
	{
		// This is a wrapper around NewHashAggregator so its signature is compatible
		// with orderedAggregator.
		new: func(
			allocator *Allocator,
			input Operator,
			colTypes []coltypes.T,
			aggFns []execinfrapb.AggregatorSpec_Func,
			groupCols []uint32,
			aggCols [][]uint32,
			_ bool,
		) (Operator, error) {
			return NewHashAggregator(
				allocator, input, colTypes, aggFns, groupCols, aggCols)
		},
		name: "hash",
	},
	{
		new:  NewOrderedAggregator,
		name: "ordered",
	},
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
		tc.batchSize = coldata.BatchSize()
	}
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = coldata.BatchSize()
	}
	return nil
}

func TestAggregatorOneFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			batchSize:       3,
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
		{
			input: tuples{
				{1, 0, 0},
				{2, 0, 0},
				{3, 0, 0},
				{4, 0, 0},
			},
			expected: tuples{
				{10},
			},
			batchSize:       1,
			outputBatchSize: 1,
			name:            "UnusedInputColumns",
			colTypes:        []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			groupCols:       []uint32{1, 2},
			aggCols:         [][]uint32{{0}},
		},
		{
			input: tuples{
				{nil, 1},
				{4, 42},
				{nil, 2},
			},
			expected: tuples{
				{3},
				{42},
			},
			name:           "UnorderedWithNullsInGroupingCol",
			unorderedInput: true,
		},
	}

	// Run tests with deliberate batch sizes and no selection vectors.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.init(); err != nil {
				t.Fatal(err)
			}

			if !tc.unorderedInput {
				tupleSource := newOpTestInput(tc.batchSize, tc.input, nil /* typs */)
				a, err := NewOrderedAggregator(
					testAllocator,
					tupleSource,
					tc.colTypes,
					tc.aggFns,
					tc.groupCols,
					tc.aggCols,
					false, /* isScalar */
				)
				if err != nil {
					t.Fatal(err)
				}

				out := newOpTestOutput(a, tc.expected)
				// Explicitly reinitialize the aggregator with the given output batch
				// size.
				a.(*orderedAggregator).initWithInputAndOutputBatchSize(tc.batchSize, tc.outputBatchSize)
				if err := out.VerifyAnyOrder(); err != nil {
					t.Fatal(err)
				}
			}

			// Run randomized tests on this test case.
			t.Run(fmt.Sprintf("Randomized"), func(t *testing.T) {
				for _, agg := range aggTypes {
					if tc.unorderedInput && agg.name == "ordered" {
						// This test case has unordered input, so we skip ordered
						// aggregator.
						continue
					}
					t.Run(agg.name, func(t *testing.T) {
						runTests(t, []tuples{tc.input}, tc.expected, unorderedVerifier,
							func(input []Operator) (Operator, error) {
								return agg.new(
									testAllocator,
									input[0],
									tc.colTypes,
									tc.aggFns,
									tc.groupCols,
									tc.aggCols,
									false, /* isScalar */
								)
							})
					})
				}
			})
		})
	}
}

func TestAggregatorMultiFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []aggregatorTestCase{
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM},
			aggCols: [][]uint32{
				{2}, {1},
			},
			input: tuples{
				{0, 1, 2},
				{0, 1, 2},
			},
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			expected: tuples{
				{4, 2},
			},
			name: "OutputOrder",
		},
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM},
			aggCols: [][]uint32{
				{2}, {1},
			},
			input: tuples{
				{0, 1, 1.3},
				{0, 1, 1.6},
				{0, 1, 0.5},
				{1, 1, 1.2},
			},
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Decimal},
			expected: tuples{
				{3.4, 3},
				{1.2, 1},
			},
			name:          "SumMultiType",
			convToDecimal: true,
		},
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_SUM},
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
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Decimal},
			expected: tuples{
				{"1.5333333333333333333", 4.6},
				{4.32, 8.64},
			},
			name:          "AvgSumSingleInputBatch",
			convToDecimal: true,
		},
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_BOOL_AND,
				execinfrapb.AggregatorSpec_BOOL_OR,
			},
			aggCols: [][]uint32{
				{1}, {1},
			},
			input: tuples{
				{0, true},
				{1, false},
				{2, true},
				{2, false},
				{3, true},
				{3, true},
				{4, false},
				{4, false},
				{5, false},
				{5, nil},
				{6, nil},
				{6, true},
				{7, nil},
				{7, false},
				{7, true},
				{8, nil},
				{8, nil},
			},
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Bool},
			expected: tuples{
				{true, true},
				{false, false},
				{false, true},
				{true, true},
				{false, false},
				{false, false},
				{true, true},
				{false, true},
				{nil, nil},
			},
			name: "BoolAndOrBatch",
		},
	}

	for _, agg := range aggTypes {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/%s/Randomized", agg.name, tc.name), func(t *testing.T) {
				if err := tc.init(); err != nil {
					t.Fatal(err)
				}
				runTests(t, []tuples{tc.input}, tc.expected, unorderedVerifier,
					func(input []Operator) (Operator, error) {
						return agg.new(testAllocator, input[0], tc.colTypes, tc.aggFns, tc.groupCols, tc.aggCols, false /* isScalar */)
					})
			})
		}
	}
}

func TestAggregatorAllFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []aggregatorTestCase{
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_AVG,
				execinfrapb.AggregatorSpec_COUNT_ROWS,
				execinfrapb.AggregatorSpec_COUNT,
				execinfrapb.AggregatorSpec_SUM,
				execinfrapb.AggregatorSpec_MIN,
				execinfrapb.AggregatorSpec_MAX,
				execinfrapb.AggregatorSpec_BOOL_AND,
				execinfrapb.AggregatorSpec_BOOL_OR,
			},
			aggCols:  [][]uint32{{0}, {4}, {1}, {}, {1}, {2}, {2}, {2}, {3}, {3}},
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Decimal, coltypes.Int64, coltypes.Bool, coltypes.Bytes},
			input: tuples{
				{0, 3.1, 2, true, "zero"},
				{0, 1.1, 3, false, "zero"},
				{1, 1.1, 1, false, "one"},
				{1, 4.1, 0, false, "one"},
				{2, 1.1, 1, true, "two"},
				{3, 4.1, 0, false, "three"},
				{3, 5.1, 0, true, "three"},
			},
			expected: tuples{
				{0, "zero", 2.1, 2, 2, 5, 2, 3, false, true},
				{1, "one", 2.6, 2, 2, 1, 0, 1, false, false},
				{2, "two", 1.1, 1, 1, 1, 1, 1, true, true},
				{3, "three", 4.6, 2, 2, 0, 0, 0, false, true},
			},
			convToDecimal: true,
		},

		// Test case for null handling.
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_COUNT_ROWS,
				execinfrapb.AggregatorSpec_COUNT,
				execinfrapb.AggregatorSpec_SUM,
				execinfrapb.AggregatorSpec_SUM_INT,
				execinfrapb.AggregatorSpec_MIN,
				execinfrapb.AggregatorSpec_MAX,
				execinfrapb.AggregatorSpec_AVG,
				execinfrapb.AggregatorSpec_BOOL_AND,
				execinfrapb.AggregatorSpec_BOOL_OR,
			},
			aggCols:  [][]uint32{{0}, {1}, {}, {1}, {1}, {2}, {2}, {2}, {1}, {3}, {3}},
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Decimal, coltypes.Int64, coltypes.Bool, coltypes.Bool},
			input: tuples{
				{nil, 1.1, 4, true},
				{0, nil, nil, nil},
				{0, 3.1, 5, nil},
				{1, nil, nil, nil},
				{1, nil, nil, false},
			},
			expected: tuples{
				{nil, 1.1, 1, 1, 1.1, 4, 4, 4, 1.1, true, true},
				{0, 3.1, 2, 1, 3.1, 5, 5, 5, 3.1, nil, nil},
				{1, nil, 2, 0, nil, nil, nil, nil, nil, false, false},
			},
			convToDecimal: true,
		},
	}

	for _, agg := range aggTypes {
		for i, tc := range testCases {
			t.Run(fmt.Sprintf("%s/%d", agg.name, i), func(t *testing.T) {
				if err := tc.init(); err != nil {
					t.Fatal(err)
				}
				verifier := orderedVerifier
				if strings.Contains(agg.name, "hash") {
					verifier = unorderedVerifier
				}
				runTests(
					t,
					[]tuples{tc.input},
					tc.expected,
					verifier,
					func(input []Operator) (Operator, error) {
						return agg.new(testAllocator, input[0], tc.colTypes, tc.aggFns, tc.groupCols, tc.aggCols, false /* isScalar */)
					})
			})
		}
	}
}

func TestAggregatorRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test aggregates random inputs, keeping track of the expected results
	// to make sure the aggregations are correct.
	rng, _ := randutil.NewPseudoRand()
	for _, groupSize := range []int{1, 2, coldata.BatchSize() / 4, coldata.BatchSize() / 2} {
		if groupSize == 0 {
			// We might be varying coldata.BatchSize() so that when it is divided by
			// 4, groupSize is 0. We want to skip such configuration.
			continue
		}
		for _, numInputBatches := range []int{1, 2, 64} {
			for _, hasNulls := range []bool{true, false} {
				for _, agg := range aggTypes {
					t.Run(fmt.Sprintf("%s/groupSize=%d/numInputBatches=%d/hasNulls=%t", agg.name, groupSize, numInputBatches, hasNulls),
						func(t *testing.T) {
							nTuples := coldata.BatchSize() * numInputBatches
							typs := []coltypes.T{coltypes.Int64, coltypes.Float64}
							cols := []coldata.Vec{
								testAllocator.NewMemColumn(typs[0], nTuples),
								testAllocator.NewMemColumn(typs[1], nTuples),
							}
							groups, aggCol, aggColNulls := cols[0].Int64(), cols[1].Float64(), cols[1].Nulls()
							expectedTuples := tuples{}

							var expRowCounts, expCounts []int64
							var expSums, expMins, expMaxs []float64
							// SUM, MIN, MAX, and AVG aggregators can output null.
							var expNulls []bool
							curGroup := -1
							for i := range groups {
								if i%groupSize == 0 {
									if curGroup != -1 {
										if expNulls[curGroup] {
											expectedTuples = append(expectedTuples, tuple{
												expRowCounts[curGroup], expCounts[curGroup], nil, nil, nil, nil,
											})
										} else {
											expectedTuples = append(expectedTuples, tuple{
												expRowCounts[curGroup], expCounts[curGroup], expSums[curGroup], expMins[curGroup], expMaxs[curGroup], expSums[curGroup] / float64(expCounts[curGroup]),
											})
										}
									}
									expRowCounts = append(expRowCounts, 0)
									expCounts = append(expCounts, 0)
									expSums = append(expSums, 0)
									expMins = append(expMins, 2048)
									expMaxs = append(expMaxs, -2048)
									expNulls = append(expNulls, true)
									curGroup++
								}
								// Keep the inputs small so they are a realistic size. Using a
								// large range is not realistic and makes decimal operations
								// slower.
								aggCol[i] = 2048 * (rng.Float64() - 0.5)

								// NULL values contribute to the row count, so we're updating
								// the row counts outside of the if block.
								expRowCounts[curGroup]++
								if hasNulls && rng.Float64() < nullProbability {
									aggColNulls.SetNull(i)
								} else {
									expNulls[curGroup] = false
									expCounts[curGroup]++
									expSums[curGroup] += aggCol[i]
									expMins[curGroup] = min64(aggCol[i], expMins[curGroup])
									expMaxs[curGroup] = max64(aggCol[i], expMaxs[curGroup])
								}
								groups[i] = int64(curGroup)
							}
							// Add result for last group.
							if expNulls[curGroup] {
								expectedTuples = append(expectedTuples, tuple{
									expRowCounts[curGroup], expCounts[curGroup], nil, nil, nil, nil,
								})
							} else {
								expectedTuples = append(expectedTuples, tuple{
									expRowCounts[curGroup], expCounts[curGroup], expSums[curGroup], expMins[curGroup], expMaxs[curGroup], expSums[curGroup] / float64(expCounts[curGroup]),
								})
							}

							source := newChunkingBatchSource(typs, cols, nTuples)
							a, err := agg.new(
								testAllocator,
								source,
								typs,
								[]execinfrapb.AggregatorSpec_Func{
									execinfrapb.AggregatorSpec_COUNT_ROWS,
									execinfrapb.AggregatorSpec_COUNT,
									execinfrapb.AggregatorSpec_SUM_INT,
									execinfrapb.AggregatorSpec_MIN,
									execinfrapb.AggregatorSpec_MAX,
									execinfrapb.AggregatorSpec_AVG},
								[]uint32{0},
								[][]uint32{{}, {1}, {1}, {1}, {1}, {1}},
								false, /* isScalar */
							)
							if err != nil {
								t.Fatal(err)
							}
							a.Init()

							testOutput := newOpTestOutput(a, expectedTuples)
							if strings.Contains(agg.name, "hash") {
								err = testOutput.VerifyAnyOrder()
							} else {
								err = testOutput.Verify()
							}

							if err != nil {
								t.Fatal(err)
							}
						})
				}
			}
		}
	}
}

func BenchmarkAggregator(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	for _, aggFn := range []execinfrapb.AggregatorSpec_Func{
		execinfrapb.AggregatorSpec_ANY_NOT_NULL,
		execinfrapb.AggregatorSpec_AVG,
		execinfrapb.AggregatorSpec_COUNT_ROWS,
		execinfrapb.AggregatorSpec_COUNT,
		execinfrapb.AggregatorSpec_SUM,
		execinfrapb.AggregatorSpec_MIN,
		execinfrapb.AggregatorSpec_MAX,
		execinfrapb.AggregatorSpec_BOOL_AND,
		execinfrapb.AggregatorSpec_BOOL_OR,
	} {
		fName := execinfrapb.AggregatorSpec_Func_name[int32(aggFn)]
		b.Run(fName, func(b *testing.B) {
			for _, agg := range aggTypes {
				for _, typ := range []coltypes.T{coltypes.Int64, coltypes.Decimal} {
					for _, groupSize := range []int{1, 2, coldata.BatchSize() / 2, coldata.BatchSize()} {
						for _, hasNulls := range []bool{false, true} {
							for _, numInputBatches := range []int{64} {
								if aggFn == execinfrapb.AggregatorSpec_BOOL_AND || aggFn == execinfrapb.AggregatorSpec_BOOL_OR {
									typ = coltypes.Bool
								}
								b.Run(fmt.Sprintf("%s/%s/groupSize=%d/hasNulls=%t/numInputBatches=%d", agg.name, typ.String(),
									groupSize, hasNulls, numInputBatches),
									func(b *testing.B) {
										colTypes := []coltypes.T{coltypes.Int64, typ}
										nTuples := numInputBatches * coldata.BatchSize()
										cols := []coldata.Vec{
											testAllocator.NewMemColumn(coltypes.Int64, nTuples),
											testAllocator.NewMemColumn(typ, nTuples),
										}
										groups := cols[0].Int64()
										curGroup := -1
										for i := 0; i < nTuples; i++ {
											if groupSize == 1 || i%groupSize == 0 {
												curGroup++
											}
											groups[i] = int64(curGroup)
										}
										if hasNulls {
											nulls := cols[1].Nulls()
											for i := 0; i < nTuples; i++ {
												if rng.Float64() < nullProbability {
													nulls.SetNull(i)
												}
											}
										}
										switch typ {
										case coltypes.Int64:
											vals := cols[1].Int64()
											for i := range vals {
												vals[i] = rng.Int63() % 1024
											}
										case coltypes.Decimal:
											vals := cols[1].Decimal()
											for i := range vals {
												vals[i].SetInt64(rng.Int63() % 1024)
											}
										case coltypes.Bool:
											vals := cols[1].Bool()
											for i := range vals {
												vals[i] = rng.Float64() < 0.5
											}
										}
										source := newChunkingBatchSource(colTypes, cols, nTuples)

										nCols := 1
										if aggFn == execinfrapb.AggregatorSpec_COUNT_ROWS {
											nCols = 0
										}
										a, err := agg.new(
											testAllocator,
											source,
											colTypes,
											[]execinfrapb.AggregatorSpec_Func{aggFn},
											[]uint32{0},
											[][]uint32{[]uint32{1}[:nCols]},
											false, /* isScalar */
										)
										if err != nil {
											b.Skip()
										}
										a.Init()

										b.ResetTimer()

										// Only count the int64 column.
										b.SetBytes(int64(8 * nTuples))
										for i := 0; i < b.N; i++ {
											a.(resetter).reset(ctx)
											source.reset()
											// Exhaust aggregator until all batches have been read.
											for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
											}
										}
									},
								)
							}
						}
					}
				}
			}
		})
	}
}

func TestHashAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			colTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64},
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
			colTypes:  []coltypes.T{coltypes.Int64},
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
				{hashTableNumBuckets, 6},
				{0, 5},
				{hashTableNumBuckets, 7},
			},
			colTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64},
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
			colTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Decimal},
			convToDecimal: true,

			aggFns:    []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM},
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
			colTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64, coltypes.Int64},
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

	for _, numOfHashBuckets := range []int{0 /* no limit */, 1, coldata.BatchSize()} {
		for _, tc := range tcs {
			if err := tc.init(); err != nil {
				t.Fatal(err)
			}
			t.Run(fmt.Sprintf("numOfHashBuckets=%d", numOfHashBuckets), func(t *testing.T) {
				runTests(t, []tuples{tc.input}, tc.expected, unorderedVerifier, func(sources []Operator) (Operator, error) {
					a, err := NewHashAggregator(testAllocator, sources[0], tc.colTypes, tc.aggFns, tc.groupCols, tc.aggCols)
					a.(*hashAggregator).testingKnobs.numOfHashBuckets = uint64(numOfHashBuckets)
					return a, err
				})
			})
		}
	}
}

func min64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
