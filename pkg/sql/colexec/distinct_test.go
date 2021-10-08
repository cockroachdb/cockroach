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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type distinctTestCase struct {
	distinctCols            []uint32
	typs                    []*types.T
	tuples                  []colexectestutils.Tuple
	expected                []colexectestutils.Tuple
	isOrderedOnDistinctCols bool
	nullsAreDistinct        bool
	// errorOnDup indicates the message that should be emitted if any duplicates
	// are observed by the distinct operator.
	errorOnDup string
	// noError indicates whether no error should actually occur. It should only
	// be used in conjunction with errorOnDup.
	noError bool
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
			{`{"id": 1}`, "a"},
			{`{"id": 2}`, "b"},
			{`{"id": 3}`, "c"},
			{`{"id": 1}`, "1"},
			{`{"id": null}`, "d"},
			{`{"id": 2}`, "2"},
			{`{"id": 5}`, "e"},
			{`{"id": 6}`, "f"},
			{`{"id": 3}`, "3"},
		},
		// We need to pass in "actual JSON" to our expected output tuples, or else
		// the tests will fail because the sort order of stringified JSON is not the
		// same as the sort order of JSON. Specifically, NULL sorts before integers
		// in JSON, but after integers in strings.
		expected: colexectestutils.Tuples{
			{mustParseJSON(`{"id": 1}`), "a"},
			{mustParseJSON(`{"id": 2}`), "b"},
			{mustParseJSON(`{"id": 3}`), "c"},
			{mustParseJSON(`{"id": null}`), "d"},
			{mustParseJSON(`{"id": 5}`), "e"},
			{mustParseJSON(`{"id": 6}`), "f"},
		},
	},
	{
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Int},
		tuples: colexectestutils.Tuples{
			{nil},
			{nil},
			{nil},
			{1},
			{1},
			{2},
			{2},
			{2},
		},
		expected: colexectestutils.Tuples{
			{nil},
			{nil},
			{nil},
			{1},
			{2},
		},
		isOrderedOnDistinctCols: true,
		nullsAreDistinct:        true,
	},
	{
		distinctCols: []uint32{0, 1},
		typs:         []*types.T{types.Int, types.Int},
		tuples: colexectestutils.Tuples{
			{nil, nil},
			{nil, nil},
			{1, nil},
			{1, nil},
			{1, 1},
			{1, 1},
			{1, 1},
			{2, nil},
			{2, 2},
			{2, 2},
		},
		expected: colexectestutils.Tuples{
			{nil, nil},
			{nil, nil},
			{1, nil},
			{1, nil},
			{1, 1},
			{2, nil},
			{2, 2},
		},
		isOrderedOnDistinctCols: true,
		nullsAreDistinct:        true,
	},
	{
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Int},
		tuples: colexectestutils.Tuples{
			{1},
			{2},
			{2},
			{3},
		},
		isOrderedOnDistinctCols: true,
		errorOnDup:              "duplicate twos",
	},
	{
		distinctCols: []uint32{0, 1},
		typs:         []*types.T{types.Int, types.Int},
		tuples: colexectestutils.Tuples{
			{1, 1},
			{1, 2},
			{1, 2},
			{1, 3},
		},
		isOrderedOnDistinctCols: true,
		errorOnDup:              "duplicates in the second column",
	},
	{
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Int},
		tuples: colexectestutils.Tuples{
			{nil},
			{nil},
		},
		isOrderedOnDistinctCols: true,
		errorOnDup:              "duplicate nulls",
	},
	{
		distinctCols: []uint32{0},
		typs:         []*types.T{types.Int},
		tuples: colexectestutils.Tuples{
			{nil},
			{nil},
		},
		expected: colexectestutils.Tuples{
			{nil},
			{nil},
		},
		isOrderedOnDistinctCols: true,
		nullsAreDistinct:        true,
		errorOnDup:              "\"duplicate\" distinct nulls",
		noError:                 true,
	},
}

func mustParseJSON(s string) json.JSON {
	j, err := json.ParseJSON(s)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	return j
}

func (tc *distinctTestCase) runTests(
	t *testing.T,
	verifier colexectestutils.VerifierType,
	constructor func(inputs []colexecop.Operator) (colexecop.Operator, error),
) {
	if tc.errorOnDup == "" {
		colexectestutils.RunTestsWithTyps(
			t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.typs},
			tc.expected, verifier, constructor,
		)
	} else {
		var numErrorRuns int
		errorHandler := func(err error) {
			if strings.Contains(err.Error(), tc.errorOnDup) {
				numErrorRuns++
				return
			}
			t.Fatal(err)
		}
		// numConstructorCalls is incremented every time the operator to test is
		// constructed and is a lower bound on the number of test runs. It is a
		// lower bound because in some cases we will reset the operator chain
		// for a second run, and the constructor won't get called then.
		var numConstructorCalls int
		instrumentedConstructor := func(inputs []colexecop.Operator) (colexecop.Operator, error) {
			numConstructorCalls++
			return constructor(inputs)
		}
		colexectestutils.RunTestsWithoutAllNullsInjectionWithErrorHandler(
			t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.typs},
			tc.expected, verifier, instrumentedConstructor, errorHandler,
		)
		if tc.noError {
			require.Zero(t, numErrorRuns)
		} else {
			// RunTests test harness runs two scenarios in which the error might
			// not be encountered:
			// 1) verifySelAndNullResets - because it exits once two batches are
			//    returned. Note that in this scenario the constructor is called
			//    up to two times;
			// 2) randomNullsInjection - because the input data set is modified;
			// so we subtract three runs from the lower bound on the number of
			// expected errors.
			numConstructorCalls -= 3
			// Because numConstructorCalls is a lower bound on the number of
			// the test runs, we expect numErrorRuns to be no less than
			// numConstructorCalls.
			require.GreaterOrEqual(
				t, numErrorRuns, numConstructorCalls,
				"expected to have no less erroneous runs than the total number of constructor calls",
			)
		}
	}
}

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()
	for _, tc := range distinctTestCases {
		log.Infof(context.Background(), "unordered")
		tc.runTests(t, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewUnorderedDistinct(
				testAllocator, input[0], tc.distinctCols, tc.typs, tc.nullsAreDistinct, tc.errorOnDup,
			), nil
		})
		if tc.isOrderedOnDistinctCols {
			for numOrderedCols := 1; numOrderedCols < len(tc.distinctCols); numOrderedCols++ {
				log.Infof(context.Background(), "partiallyOrdered/ordCols=%d", numOrderedCols)
				orderedCols := make([]uint32, numOrderedCols)
				for i, j := range rng.Perm(len(tc.distinctCols))[:numOrderedCols] {
					orderedCols[i] = tc.distinctCols[j]
				}
				tc.runTests(t, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
					return newPartiallyOrderedDistinct(
						testAllocator, input[0], tc.distinctCols, orderedCols, tc.typs, tc.nullsAreDistinct, tc.errorOnDup,
					)
				})
			}
			log.Info(context.Background(), "ordered")
			tc.runTests(t, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexecbase.NewOrderedDistinct(input[0], tc.distinctCols, tc.typs, tc.nullsAreDistinct, tc.errorOnDup)
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
			return NewUnorderedDistinct(testAllocator, input[0], distinctCols, typs, false /* nullsAreDistinct */, "" /* errorOnDup */), nil
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
								distinct.Init(ctx)
								for b := distinct.Next(); b.Length() > 0; b = distinct.Next() {
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
			return NewUnorderedDistinct(allocator, input, distinctCols, typs, false /* nullsAreDistinct */, "" /* errorOnDup */), nil
		},
		func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
			return newPartiallyOrderedDistinct(allocator, input, distinctCols, distinctCols[:numOrderedCols], typs, false /* nullsAreDistinct */, "" /* errorOnDup */)
		},
		func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
			return colexecbase.NewOrderedDistinct(input, distinctCols, typs, false /* nullsAreDistinct */, "" /* errorOnDup */)
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
