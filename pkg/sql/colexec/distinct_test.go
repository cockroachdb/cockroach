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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

type distinctTestCase struct {
	distinctCols            []uint32
	typs                    []*types.T
	tuples                  []colexectestutils.Tuple
	expected                []colexectestutils.Tuple
	isOrderedOnDistinctCols bool
}

var distinctTestCases = []distinctTestCase{
	{
		distinctCols: []uint32{0, 1, 2},
		typs:         []*types.T{types.Float, types.Int, types.String, types.Int},
		tuples: colexectestutils.Tuples{
			{nil, nil, nil, nil},
			{nil, nil, nil, nil},
			{nil, nil, "30", nil},
			{1.0, 2, "30", 4},
			{1.0, 2, "30", 4},
			{2.0, 2, "30", 4},
			{2.0, 3, "30", 4},
			{2.0, 3, "40", 4},
			{2.0, 3, "40", 4},
		},
		expected: colexectestutils.Tuples{
			{nil, nil, nil, nil},
			{nil, nil, "30", nil},
			{1.0, 2, "30", 4},
			{2.0, 2, "30", 4},
			{2.0, 3, "30", 4},
			{2.0, 3, "40", 4},
		},
		isOrderedOnDistinctCols: true,
	},
	{
		distinctCols: []uint32{1, 0, 2},
		typs:         []*types.T{types.Float, types.Int, types.Bytes, types.Int},
		tuples: colexectestutils.Tuples{
			{nil, nil, nil, nil},
			{nil, nil, nil, nil},
			{nil, nil, "30", nil},
			{1.0, 2, "30", 4},
			{1.0, 2, "30", 4},
			{2.0, 2, "30", 4},
			{2.0, 3, "30", 4},
			{2.0, 3, "40", 4},
			{2.0, 3, "40", 4},
		},
		expected: colexectestutils.Tuples{
			{nil, nil, nil, nil},
			{nil, nil, "30", nil},
			{1.0, 2, "30", 4},
			{2.0, 2, "30", 4},
			{2.0, 3, "30", 4},
			{2.0, 3, "40", 4},
		},
		isOrderedOnDistinctCols: true,
	},
	{
		distinctCols: []uint32{0, 1, 2},
		typs:         []*types.T{types.Float, types.Int, types.String, types.Int},
		tuples: colexectestutils.Tuples{
			{1.0, 2, "30", 4},
			{1.0, 2, "30", 4},
			{nil, nil, nil, nil},
			{nil, nil, nil, nil},
			{2.0, 2, "30", 4},
			{2.0, 3, "30", 4},
			{nil, nil, "30", nil},
			{2.0, 3, "40", 4},
			{2.0, 3, "40", 4},
		},
		expected: colexectestutils.Tuples{
			{1.0, 2, "30", 4},
			{nil, nil, nil, nil},
			{2.0, 2, "30", 4},
			{2.0, 3, "30", 4},
			{nil, nil, "30", nil},
			{2.0, 3, "40", 4},
		},
	},
	{
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Int, types.Bytes},
		tuples: colexectestutils.Tuples{
			{1, "a"},
			{2, "b"},
			{3, "c"},
			{nil, "d"},
			{5, "e"},
			{6, "f"},
			{1, "1"},
			{2, "2"},
			{3, "3"},
		},
		expected: colexectestutils.Tuples{
			{1, "a"},
			{2, "b"},
			{3, "c"},
			{nil, "d"},
			{5, "e"},
			{6, "f"},
		},
	},
	{
		// This is to test HashTable deduplication with various batch size
		// boundaries and ensure it always emits the first tuple it encountered.
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Int, types.String},
		tuples: colexectestutils.Tuples{
			{1, "1"},
			{1, "2"},
			{1, "3"},
			{1, "4"},
			{1, "5"},
			{2, "6"},
			{2, "7"},
			{2, "8"},
			{2, "9"},
			{2, "10"},
			{0, "11"},
			{0, "12"},
			{0, "13"},
			{1, "14"},
			{1, "15"},
			{1, "16"},
		},
		expected: colexectestutils.Tuples{
			{1, "1"},
			{2, "6"},
			{0, "11"},
		},
	},
	{
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Jsonb, types.String},
		tuples: colexectestutils.Tuples{
			{`'{"id": 1}'`, "a"},
			{`'{"id": 2}'`, "b"},
			{`'{"id": 3}'`, "c"},
			{`'{"id": 1}'`, "1"},
			{`'{"id": null}'`, "d"},
			{`'{"id": 2}'`, "2"},
			{`'{"id": 5}'`, "e"},
			{`'{"id": 6}'`, "f"},
			{`'{"id": 3}'`, "3"},
		},
		expected: colexectestutils.Tuples{
			{`'{"id": 1}'`, "a"},
			{`'{"id": 2}'`, "b"},
			{`'{"id": 3}'`, "c"},
			{`'{"id": null}'`, "d"},
			{`'{"id": 5}'`, "e"},
			{`'{"id": 6}'`, "f"},
		},
	},
}

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()
	for _, tc := range distinctTestCases {
		log.Infof(context.Background(), "unordered")
		colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.typs}, tc.expected, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return NewUnorderedDistinct(
					testAllocator, input[0], tc.distinctCols, tc.typs,
				), nil
			})
		if tc.isOrderedOnDistinctCols {
			for numOrderedCols := 1; numOrderedCols < len(tc.distinctCols); numOrderedCols++ {
				log.Infof(context.Background(), "partiallyOrdered/ordCols=%d", numOrderedCols)
				orderedCols := make([]uint32, numOrderedCols)
				for i, j := range rng.Perm(len(tc.distinctCols))[:numOrderedCols] {
					orderedCols[i] = tc.distinctCols[j]
				}
				colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.typs}, tc.expected, colexectestutils.OrderedVerifier,
					func(input []colexecop.Operator) (colexecop.Operator, error) {
						return newPartiallyOrderedDistinct(
							testAllocator, testAllocator, input[0], tc.distinctCols, orderedCols, tc.typs,
						)
					})
			}
			log.Info(context.Background(), "ordered")
			colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.typs}, tc.expected, colexectestutils.OrderedVerifier,
				func(input []colexecop.Operator) (colexecop.Operator, error) {
					return colexecbase.NewOrderedDistinct(input[0], tc.distinctCols, tc.typs)
				})
		}
	}
}

func TestUnorderedDistinctRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()
	nCols := 1 + rng.Intn(3)
	typs := make([]*types.T, nCols)
	distinctCols := make([]uint32, nCols)
	for i := range typs {
		typs[i] = types.Int
		distinctCols[i] = uint32(i)
	}
	nDistinctBatches := 2 + rng.Intn(2)
	newTupleProbability := rng.Float64()
	nTuples := int(float64(nDistinctBatches*coldata.BatchSize()) / newTupleProbability)
	const maxNumTuples = 25000
	if nTuples > maxNumTuples {
		// If we happen to set a large value for coldata.BatchSize() and a small
		// value for newTupleProbability, we might end up with huge number of
		// tuples. Then, when runTests test harness uses small batch size, the
		// test might take a while, so we'll limit the number of tuples.
		nTuples = maxNumTuples
	}
	tups, expected := generateRandomDataForUnorderedDistinct(rng, nTuples, nCols, newTupleProbability)
	colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tups}, [][]*types.T{typs}, expected, colexectestutils.UnorderedVerifier,
		func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewUnorderedDistinct(testAllocator, input[0], distinctCols, typs), nil
		},
	)
}

// getNewValueProbabilityForDistinct returns the probability that we need to use
// a new value for a single element in a tuple when overall the tuples need to
// be distinct with newTupleProbability and they consists of nCols columns.
func getNewValueProbabilityForDistinct(newTupleProbability float64, nCols int) float64 {
	// We have the following equation:
	//   newTupleProbability = 1 - (1 - newValueProbability) ^ nCols,
	// so applying some manipulations we get:
	//   newValueProbability = 1 - (1 - newTupleProbability) ^ (1 / nCols).
	return 1.0 - math.Pow(1-newTupleProbability, 1.0/float64(nCols))
}

// runDistinctBenchmarks runs the benchmarks of a distinct operator variant on
// multiple configurations.
func runDistinctBenchmarks(
	ctx context.Context,
	b *testing.B,
	distinctConstructor func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error),
	getNumOrderedCols func(nCols int) int,
	namePrefix string,
	isExternal bool,
) {
	rng, _ := randutil.NewPseudoRand()
	const nCols = 2
	const bytesValueLength = 8
	distinctCols := []uint32{0, 1}
	nullsOptions := []bool{false, true}
	nRowsOptions := []int{1, 64, 4 * coldata.BatchSize(), 256 * coldata.BatchSize()}
	if isExternal {
		nullsOptions = []bool{false}
		nRowsOptions = []int{coldata.BatchSize(), 64 * coldata.BatchSize(), 4096 * coldata.BatchSize()}
	}
	if testing.Short() {
		nRowsOptions = []int{coldata.BatchSize()}
	}
	setFirstValue := func(vec coldata.Vec) {
		if typ := vec.Type(); typ == types.Int {
			vec.Int64()[0] = 0
		} else if typ == types.Bytes {
			vec.Bytes().Set(0, make([]byte, bytesValueLength))
		} else {
			colexecerror.InternalError(errors.AssertionFailedf("unsupported type %s", typ))
		}
	}
	setIthValue := func(vec coldata.Vec, i int, newValueProbability float64) {
		if i == 0 {
			colexecerror.InternalError(errors.New("setIthValue called with i == 0"))
		}
		if typ := vec.Type(); typ == types.Int {
			col := vec.Int64()
			col[i] = col[i-1]
			if rng.Float64() < newValueProbability {
				col[i]++
			}
		} else if typ == types.Bytes {
			v := make([]byte, bytesValueLength)
			copy(v, vec.Bytes().Get(i-1))
			if rng.Float64() < newValueProbability {
				for pos := 0; pos < bytesValueLength; pos++ {
					v[pos]++
					// If we have overflowed our current byte, we need to
					// increment the next one; otherwise, we have a new distinct
					// value.
					if v[pos] != 0 {
						break
					}
				}
			}
			vec.Bytes().Set(i, v)
		} else {
			colexecerror.InternalError(errors.AssertionFailedf("unsupported type %s", typ))
		}
	}
	for _, hasNulls := range nullsOptions {
		for _, newTupleProbability := range []float64{0.001, 0.1} {
			for _, nRows := range nRowsOptions {
				for _, typ := range []*types.T{types.Int, types.Bytes} {
					typs := make([]*types.T, nCols)
					cols := make([]coldata.Vec, nCols)
					for i := range typs {
						typs[i] = typ
						cols[i] = testAllocator.NewMemColumn(typs[i], nRows)
					}
					numOrderedCols := getNumOrderedCols(nCols)
					newValueProbability := getNewValueProbabilityForDistinct(newTupleProbability, nCols)
					for i := range distinctCols {
						setFirstValue(cols[i])
						for j := 1; j < nRows; j++ {
							setIthValue(cols[i], j, newValueProbability)
						}
						if hasNulls {
							cols[i].Nulls().SetNull(0)
						}
					}
					nullsPrefix := ""
					if len(nullsOptions) > 1 {
						nullsPrefix = fmt.Sprintf("/hasNulls=%t", hasNulls)
					}
					b.Run(
						fmt.Sprintf("%s%s/newTupleProbability=%.3f/rows=%d/ordCols=%d/type=%s",
							namePrefix, nullsPrefix, newTupleProbability,
							nRows, numOrderedCols, typ.Name(),
						),
						func(b *testing.B) {
							b.SetBytes(int64(8 * nRows * nCols))
							b.ResetTimer()
							for n := 0; n < b.N; n++ {
								// Note that the source will be ordered on all nCols so that the
								// number of distinct tuples doesn't vary between different
								// distinct operator variations.
								source := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nRows)
								distinct, err := distinctConstructor(testAllocator, source, distinctCols, numOrderedCols, typs)
								if err != nil {
									b.Fatal(err)
								}
								distinct.Init()
								for b := distinct.Next(ctx); b.Length() > 0; b = distinct.Next(ctx) {
								}
							}
							b.StopTimer()
						})
				}
			}
		}
	}
}

func BenchmarkDistinct(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	distinctConstructors := []func(*colmem.Allocator, colexecop.Operator, []uint32, int, []*types.T) (colexecop.Operator, error){
		func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
			return NewUnorderedDistinct(allocator, input, distinctCols, typs), nil
		},
		func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
			return newPartiallyOrderedDistinct(allocator, allocator, input, distinctCols, distinctCols[:numOrderedCols], typs)
		},
		func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
			return colexecbase.NewOrderedDistinct(input, distinctCols, typs)
		},
	}
	distinctNames := []string{"Unordered", "PartiallyOrdered", "Ordered"}
	orderedColsFraction := []float64{0, 0.5, 1.0}
	for distinctIdx, distinctConstructor := range distinctConstructors {
		runDistinctBenchmarks(
			ctx,
			b,
			distinctConstructor,
			func(nCols int) int {
				return int(float64(nCols) * orderedColsFraction[distinctIdx])
			},
			distinctNames[distinctIdx],
			false, /* isExternal */
		)
	}
}
