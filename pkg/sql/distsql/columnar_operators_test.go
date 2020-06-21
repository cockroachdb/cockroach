// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsql

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

const nullProbability = 0.2
const randTypesProbability = 0.5

func TestAggregatorAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	rng, seed := randutil.NewPseudoRand()
	nRuns := 20
	nRows := 100
	const (
		maxNumGroupingCols = 3
		nextGroupProb      = 0.2
	)
	groupingCols := make([]uint32, maxNumGroupingCols)
	orderingCols := make([]execinfrapb.Ordering_Column, maxNumGroupingCols)
	for i := uint32(0); i < maxNumGroupingCols; i++ {
		groupingCols[i] = i
		orderingCols[i].ColIdx = i
	}
	var da sqlbase.DatumAlloc

	deterministicAggFns := make([]execinfrapb.AggregatorSpec_Func, 0, len(colexec.SupportedAggFns)-1)
	for _, aggFn := range colexec.SupportedAggFns {
		if aggFn == execinfrapb.AggregatorSpec_ANY_NOT_NULL {
			// We skip ANY_NOT_NULL aggregate function because it returns
			// non-deterministic results.
			continue
		}
		deterministicAggFns = append(deterministicAggFns, aggFn)
	}
	aggregations := make([]execinfrapb.AggregatorSpec_Aggregation, len(deterministicAggFns))
	for _, hashAgg := range []bool{false, true} {
		for numGroupingCols := 1; numGroupingCols <= maxNumGroupingCols; numGroupingCols++ {
			for i, aggFn := range deterministicAggFns {
				aggregations[i].Func = aggFn
				aggregations[i].ColIdx = []uint32{uint32(i + numGroupingCols)}
			}
			inputTypes := make([]*types.T, len(aggregations)+numGroupingCols)
			for i := 0; i < numGroupingCols; i++ {
				inputTypes[i] = types.Int
			}
			outputTypes := make([]*types.T, len(aggregations))

			for run := 0; run < nRuns; run++ {
				var rows sqlbase.EncDatumRows
				// We will be grouping based on the first numGroupingCols columns (which
				// we already set to be of INT types) with the values for the column set
				// manually below.
				for i := range aggregations {
					aggFn := aggregations[i].Func
					var aggTyp *types.T
					for {
						aggTyp = sqlbase.RandType(rng)
						aggInputTypes := []*types.T{aggTyp}
						if aggFn == execinfrapb.AggregatorSpec_COUNT_ROWS {
							// Count rows takes no arguments.
							aggregations[i].ColIdx = []uint32{}
							aggInputTypes = aggInputTypes[:0]
						}
						if _, outputType, err := execinfrapb.GetAggregateInfo(aggFn, aggInputTypes...); err == nil {
							outputTypes[i] = outputType
							break
						}
					}
					inputTypes[i+numGroupingCols] = aggTyp
				}
				rows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
				groupIdx := 0
				for _, row := range rows {
					for i := 0; i < numGroupingCols; i++ {
						if rng.Float64() < nullProbability {
							row[i] = sqlbase.EncDatum{Datum: tree.DNull}
						} else {
							row[i] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(groupIdx))}
							if rng.Float64() < nextGroupProb {
								groupIdx++
							}
						}
					}
				}

				aggregatorSpec := &execinfrapb.AggregatorSpec{
					Type:         execinfrapb.AggregatorSpec_NON_SCALAR,
					GroupCols:    groupingCols[:numGroupingCols],
					Aggregations: aggregations,
				}
				if hashAgg {
					// Let's shuffle the rows for the hash aggregator.
					rand.Shuffle(nRows, func(i, j int) {
						rows[i], rows[j] = rows[j], rows[i]
					})
				} else {
					aggregatorSpec.OrderedGroupCols = groupingCols[:numGroupingCols]
					orderedCols := execinfrapb.ConvertToColumnOrdering(
						execinfrapb.Ordering{Columns: orderingCols[:numGroupingCols]},
					)
					// Although we build the input rows in "non-decreasing" order, it is
					// possible that some NULL values are present here and there, so we
					// need to sort the rows to satisfy the ordering conditions.
					sort.Slice(rows, func(i, j int) bool {
						cmp, err := rows[i].Compare(inputTypes, &da, orderedCols, &evalCtx, rows[j])
						if err != nil {
							t.Fatal(err)
						}
						return cmp < 0
					})
				}
				pspec := &execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
					Core:  execinfrapb.ProcessorCoreUnion{Aggregator: aggregatorSpec},
				}
				args := verifyColOperatorArgs{
					anyOrder:    hashAgg,
					inputTypes:  [][]*types.T{inputTypes},
					inputs:      []sqlbase.EncDatumRows{rows},
					outputTypes: outputTypes,
					pspec:       pspec,
				}
				if err := verifyColOperator(args); err != nil {
					// Columnar aggregators check whether an overflow occurs whereas
					// processors don't, so we simply swallow the integer out of range
					// error if such occurs and move on.
					if strings.Contains(err.Error(), tree.ErrIntOutOfRange.Error()) {
						continue
					}
					fmt.Printf("--- seed = %d run = %d hash = %t ---\n",
						seed, run, hashAgg)
					prettyPrintTypes(inputTypes, "t" /* tableName */)
					prettyPrintInput(rows, inputTypes, "t" /* tableName */)
					t.Fatal(err)
				}
			}
		}
	}
}

func TestDistinctAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	rng, seed := randutil.NewPseudoRand()
	nRuns := 10
	nRows := 10
	maxCols := 3
	maxNum := 3
	intTyps := make([]*types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = types.Int
	}

	for run := 0; run < nRuns; run++ {
		for nCols := 1; nCols <= maxCols; nCols++ {
			for nDistinctCols := 1; nDistinctCols <= nCols; nDistinctCols++ {
				for nOrderedCols := 0; nOrderedCols <= nDistinctCols; nOrderedCols++ {
					var (
						rows       sqlbase.EncDatumRows
						inputTypes []*types.T
						ordCols    []execinfrapb.Ordering_Column
					)
					if rng.Float64() < randTypesProbability {
						inputTypes = generateRandomSupportedTypes(rng, nCols)
						rows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
					} else {
						inputTypes = intTyps[:nCols]
						rows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
					}
					distinctCols := make([]uint32, nDistinctCols)
					for i, distinctCol := range rng.Perm(nCols)[:nDistinctCols] {
						distinctCols[i] = uint32(distinctCol)
					}
					orderedCols := make([]uint32, nOrderedCols)
					for i, orderedColIdx := range rng.Perm(nDistinctCols)[:nOrderedCols] {
						// From the set of distinct columns we need to choose nOrderedCols
						// to be in the ordered columns set.
						orderedCols[i] = distinctCols[orderedColIdx]
					}
					ordCols = make([]execinfrapb.Ordering_Column, nOrderedCols)
					for i, col := range orderedCols {
						ordCols[i] = execinfrapb.Ordering_Column{
							ColIdx: col,
						}
					}
					sort.Slice(rows, func(i, j int) bool {
						cmp, err := rows[i].Compare(
							inputTypes, &da,
							execinfrapb.ConvertToColumnOrdering(execinfrapb.Ordering{Columns: ordCols}),
							&evalCtx, rows[j],
						)
						if err != nil {
							t.Fatal(err)
						}
						return cmp < 0
					})

					spec := &execinfrapb.DistinctSpec{
						DistinctColumns: distinctCols,
						OrderedColumns:  orderedCols,
					}
					pspec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
						Core:  execinfrapb.ProcessorCoreUnion{Distinct: spec},
					}
					args := verifyColOperatorArgs{
						anyOrder:    false,
						inputTypes:  [][]*types.T{inputTypes},
						inputs:      []sqlbase.EncDatumRows{rows},
						outputTypes: inputTypes,
						pspec:       pspec,
					}
					if err := verifyColOperator(args); err != nil {
						fmt.Printf("--- seed = %d run = %d nCols = %d distinct cols = %v ordered cols = %v ---\n",
							seed, run, nCols, distinctCols, orderedCols)
						prettyPrintTypes(inputTypes, "t" /* tableName */)
						prettyPrintInput(rows, inputTypes, "t" /* tableName */)
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func TestSorterAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	rng, seed := randutil.NewPseudoRand()
	nRuns := 5
	nRows := 8 * coldata.BatchSize()
	maxCols := 5
	maxNum := 10
	intTyps := make([]*types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = types.Int
	}

	for _, spillForced := range []bool{false, true} {
		for run := 0; run < nRuns; run++ {
			for nCols := 1; nCols <= maxCols; nCols++ {
				// We will try both general sort and top K sort.
				for _, topK := range []uint64{0, uint64(1 + rng.Intn(64))} {
					var (
						rows       sqlbase.EncDatumRows
						inputTypes []*types.T
					)
					if rng.Float64() < randTypesProbability {
						inputTypes = generateRandomSupportedTypes(rng, nCols)
						rows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
					} else {
						inputTypes = intTyps[:nCols]
						rows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
					}

					// Note: we're only generating column orderings on all nCols columns since
					// if there are columns not in the ordering, the results are not fully
					// deterministic.
					orderingCols := generateColumnOrdering(rng, nCols, nCols)
					sorterSpec := &execinfrapb.SorterSpec{
						OutputOrdering: execinfrapb.Ordering{Columns: orderingCols},
					}
					var limit, offset uint64
					if topK > 0 {
						offset = uint64(rng.Intn(int(topK)))
						limit = topK - offset
					}
					pspec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
						Core:  execinfrapb.ProcessorCoreUnion{Sorter: sorterSpec},
						Post:  execinfrapb.PostProcessSpec{Limit: limit, Offset: offset},
					}
					args := verifyColOperatorArgs{
						inputTypes:     [][]*types.T{inputTypes},
						inputs:         []sqlbase.EncDatumRows{rows},
						outputTypes:    inputTypes,
						pspec:          pspec,
						forceDiskSpill: spillForced,
					}
					if spillForced {
						args.numForcedRepartitions = 2 + rng.Intn(3)
					}
					if err := verifyColOperator(args); err != nil {
						fmt.Printf("--- seed = %d spillForced = %t nCols = %d K = %d ---\n",
							seed, spillForced, nCols, topK)
						prettyPrintTypes(inputTypes, "t" /* tableName */)
						prettyPrintInput(rows, inputTypes, "t" /* tableName */)
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func TestSortChunksAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	rng, seed := randutil.NewPseudoRand()
	nRuns := 5
	nRows := 5 * coldata.BatchSize() / 4
	maxCols := 3
	maxNum := 10
	intTyps := make([]*types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = types.Int
	}

	for _, spillForced := range []bool{false, true} {
		for run := 0; run < nRuns; run++ {
			for nCols := 2; nCols <= maxCols; nCols++ {
				for matchLen := 1; matchLen < nCols; matchLen++ {
					var (
						rows       sqlbase.EncDatumRows
						inputTypes []*types.T
					)
					if rng.Float64() < randTypesProbability {
						inputTypes = generateRandomSupportedTypes(rng, nCols)
						rows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
					} else {
						inputTypes = intTyps[:nCols]
						rows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
					}

					// Note: we're only generating column orderings on all nCols columns since
					// if there are columns not in the ordering, the results are not fully
					// deterministic.
					orderingCols := generateColumnOrdering(rng, nCols, nCols)
					matchedCols := execinfrapb.ConvertToColumnOrdering(execinfrapb.Ordering{Columns: orderingCols[:matchLen]})
					// Presort the input on first matchLen columns.
					sort.Slice(rows, func(i, j int) bool {
						cmp, err := rows[i].Compare(inputTypes, &da, matchedCols, &evalCtx, rows[j])
						if err != nil {
							t.Fatal(err)
						}
						return cmp < 0
					})

					sorterSpec := &execinfrapb.SorterSpec{
						OutputOrdering:   execinfrapb.Ordering{Columns: orderingCols},
						OrderingMatchLen: uint32(matchLen),
					}
					pspec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
						Core:  execinfrapb.ProcessorCoreUnion{Sorter: sorterSpec},
					}
					args := verifyColOperatorArgs{
						inputTypes:     [][]*types.T{inputTypes},
						inputs:         []sqlbase.EncDatumRows{rows},
						outputTypes:    inputTypes,
						pspec:          pspec,
						forceDiskSpill: spillForced,
					}
					if err := verifyColOperator(args); err != nil {
						fmt.Printf("--- seed = %d spillForced = %t orderingCols = %v matchLen = %d run = %d ---\n",
							seed, spillForced, orderingCols, matchLen, run)
						prettyPrintTypes(inputTypes, "t" /* tableName */)
						prettyPrintInput(rows, inputTypes, "t" /* tableName */)
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func TestHashJoinerAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	type hjTestSpec struct {
		joinType        sqlbase.JoinType
		onExprSupported bool
	}
	testSpecs := []hjTestSpec{
		{
			joinType:        sqlbase.InnerJoin,
			onExprSupported: true,
		},
		{
			joinType: sqlbase.LeftOuterJoin,
		},
		{
			joinType: sqlbase.RightOuterJoin,
		},
		{
			joinType: sqlbase.FullOuterJoin,
		},
		{
			joinType: sqlbase.LeftSemiJoin,
		},
		{
			joinType: sqlbase.LeftAntiJoin,
		},
		{
			joinType: sqlbase.IntersectAllJoin,
		},
		{
			joinType: sqlbase.ExceptAllJoin,
		},
	}

	rng, seed := randutil.NewPseudoRand()
	nRuns := 3
	nRows := 10
	maxCols := 3
	maxNum := 5
	intTyps := make([]*types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = types.Int
	}

	for _, spillForced := range []bool{false, true} {
		for run := 0; run < nRuns; run++ {
			for _, testSpec := range testSpecs {
				for nCols := 1; nCols <= maxCols; nCols++ {
					for nEqCols := 1; nEqCols <= nCols; nEqCols++ {
						for _, addFilter := range getAddFilterOptions(testSpec.joinType, nEqCols < nCols) {
							triedWithoutOnExpr, triedWithOnExpr := false, false
							if !testSpec.onExprSupported {
								triedWithOnExpr = true
							}
							for !triedWithoutOnExpr || !triedWithOnExpr {
								var (
									lRows, rRows             sqlbase.EncDatumRows
									lEqCols, rEqCols         []uint32
									lInputTypes, rInputTypes []*types.T
									usingRandomTypes         bool
								)
								if rng.Float64() < randTypesProbability {
									lInputTypes = generateRandomSupportedTypes(rng, nCols)
									lEqCols = generateEqualityColumns(rng, nCols, nEqCols)
									rInputTypes = append(rInputTypes[:0], lInputTypes...)
									rEqCols = append(rEqCols[:0], lEqCols...)
									rng.Shuffle(nEqCols, func(i, j int) {
										iColIdx, jColIdx := rEqCols[i], rEqCols[j]
										rInputTypes[iColIdx], rInputTypes[jColIdx] = rInputTypes[jColIdx], rInputTypes[iColIdx]
										rEqCols[i], rEqCols[j] = rEqCols[j], rEqCols[i]
									})
									rInputTypes = generateRandomComparableTypes(rng, rInputTypes)
									lRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, lInputTypes)
									rRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, rInputTypes)
									usingRandomTypes = true
								} else {
									lInputTypes = intTyps[:nCols]
									rInputTypes = lInputTypes
									lRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
									rRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
									lEqCols = generateEqualityColumns(rng, nCols, nEqCols)
									rEqCols = generateEqualityColumns(rng, nCols, nEqCols)
								}

								var outputTypes []*types.T
								if testSpec.joinType.ShouldIncludeRightColsInOutput() {
									outputTypes = append(lInputTypes, rInputTypes...)
								} else {
									outputTypes = lInputTypes
								}
								outputColumns := make([]uint32, len(outputTypes))
								for i := range outputColumns {
									outputColumns[i] = uint32(i)
								}

								var filter, onExpr execinfrapb.Expression
								if addFilter {
									colTypes := append(lInputTypes, rInputTypes...)
									filter = generateFilterExpr(
										rng, nCols, nEqCols, colTypes, usingRandomTypes,
										!testSpec.joinType.ShouldIncludeRightColsInOutput(),
									)
								}
								if triedWithoutOnExpr {
									colTypes := append(lInputTypes, rInputTypes...)
									onExpr = generateFilterExpr(
										rng, nCols, nEqCols, colTypes, usingRandomTypes, false, /* forceLeftSide */
									)
								}
								hjSpec := &execinfrapb.HashJoinerSpec{
									LeftEqColumns:  lEqCols,
									RightEqColumns: rEqCols,
									OnExpr:         onExpr,
									Type:           testSpec.joinType,
								}
								pspec := &execinfrapb.ProcessorSpec{
									Input: []execinfrapb.InputSyncSpec{
										{ColumnTypes: lInputTypes},
										{ColumnTypes: rInputTypes},
									},
									Core: execinfrapb.ProcessorCoreUnion{HashJoiner: hjSpec},
									Post: execinfrapb.PostProcessSpec{
										Projection:    true,
										OutputColumns: outputColumns,
										Filter:        filter,
									},
								}
								args := verifyColOperatorArgs{
									anyOrder:       true,
									inputTypes:     [][]*types.T{lInputTypes, rInputTypes},
									inputs:         []sqlbase.EncDatumRows{lRows, rRows},
									outputTypes:    outputTypes,
									pspec:          pspec,
									forceDiskSpill: spillForced,
									// It is possible that we have a filter that is always false, and this
									// will allow us to plan a zero operator which always returns a zero
									// batch. In such case, the spilling might not occur and that's ok.
									forcedDiskSpillMightNotOccur: !filter.Empty() || !onExpr.Empty(),
									numForcedRepartitions:        2,
									rng:                          rng,
								}
								if testSpec.joinType.IsSetOpJoin() && nEqCols < nCols {
									// The output of set operation joins is not fully
									// deterministic when there are non-equality
									// columns, however, the rows must match on the
									// equality columns between vectorized and row
									// executions.
									args.colIdxsToCheckForEquality = make([]int, nEqCols)
									for i := range args.colIdxsToCheckForEquality {
										args.colIdxsToCheckForEquality[i] = int(lEqCols[i])
									}
								}

								if err := verifyColOperator(args); err != nil {
									fmt.Printf("--- spillForced = %t join type = %s onExpr = %q"+
										" filter = %q seed = %d run = %d ---\n",
										spillForced, testSpec.joinType.String(), onExpr.Expr, filter.Expr, seed, run)
									fmt.Printf("--- lEqCols = %v rEqCols = %v ---\n", lEqCols, rEqCols)
									prettyPrintTypes(lInputTypes, "left_table" /* tableName */)
									prettyPrintTypes(rInputTypes, "right_table" /* tableName */)
									prettyPrintInput(lRows, lInputTypes, "left_table" /* tableName */)
									prettyPrintInput(rRows, rInputTypes, "right_table" /* tableName */)
									t.Fatal(err)
								}
								if onExpr.Expr == "" {
									triedWithoutOnExpr = true
								} else {
									triedWithOnExpr = true
								}
							}
						}
					}
				}
			}
		}
	}
}

// generateEqualityColumns produces a random permutation of nEqCols random
// columns on a table with nCols columns, so nEqCols must be not greater than
// nCols.
func generateEqualityColumns(rng *rand.Rand, nCols int, nEqCols int) []uint32 {
	if nEqCols > nCols {
		panic("nEqCols > nCols in generateEqualityColumns")
	}
	eqCols := make([]uint32, 0, nEqCols)
	for _, eqCol := range rng.Perm(nCols)[:nEqCols] {
		eqCols = append(eqCols, uint32(eqCol))
	}
	return eqCols
}

func TestMergeJoinerAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	type mjTestSpec struct {
		joinType        sqlbase.JoinType
		anyOrder        bool
		onExprSupported bool
	}
	testSpecs := []mjTestSpec{
		{
			joinType:        sqlbase.InnerJoin,
			onExprSupported: true,
		},
		{
			joinType: sqlbase.LeftOuterJoin,
		},
		{
			joinType: sqlbase.RightOuterJoin,
		},
		{
			joinType: sqlbase.FullOuterJoin,
			// FULL OUTER JOIN doesn't guarantee any ordering on its output (since it
			// is ambiguous), so we're comparing the outputs as sets.
			anyOrder: true,
		},
		{
			joinType: sqlbase.LeftSemiJoin,
		},
		{
			joinType: sqlbase.LeftAntiJoin,
		},
		{
			joinType: sqlbase.IntersectAllJoin,
		},
		{
			joinType: sqlbase.ExceptAllJoin,
		},
	}

	rng, seed := randutil.NewPseudoRand()
	nRuns := 3
	nRows := 10
	maxCols := 3
	maxNum := 5
	intTyps := make([]*types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = types.Int
	}

	for run := 0; run < nRuns; run++ {
		for _, testSpec := range testSpecs {
			for nCols := 1; nCols <= maxCols; nCols++ {
				for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
					for _, addFilter := range getAddFilterOptions(testSpec.joinType, nOrderingCols < nCols) {
						triedWithoutOnExpr, triedWithOnExpr := false, false
						if !testSpec.onExprSupported {
							triedWithOnExpr = true
						}
						for !triedWithoutOnExpr || !triedWithOnExpr {
							var (
								lRows, rRows                 sqlbase.EncDatumRows
								lInputTypes, rInputTypes     []*types.T
								lOrderingCols, rOrderingCols []execinfrapb.Ordering_Column
								usingRandomTypes             bool
							)
							if rng.Float64() < randTypesProbability {
								lInputTypes = generateRandomSupportedTypes(rng, nCols)
								lOrderingCols = generateColumnOrdering(rng, nCols, nOrderingCols)
								rInputTypes = append(rInputTypes[:0], lInputTypes...)
								rOrderingCols = append(rOrderingCols[:0], lOrderingCols...)
								rng.Shuffle(nOrderingCols, func(i, j int) {
									iColIdx, jColIdx := rOrderingCols[i].ColIdx, rOrderingCols[j].ColIdx
									rInputTypes[iColIdx], rInputTypes[jColIdx] = rInputTypes[jColIdx], rInputTypes[iColIdx]
									rOrderingCols[i], rOrderingCols[j] = rOrderingCols[j], rOrderingCols[i]
								})
								rInputTypes = generateRandomComparableTypes(rng, rInputTypes)
								lRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, lInputTypes)
								rRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, rInputTypes)
								usingRandomTypes = true
							} else {
								lInputTypes = intTyps[:nCols]
								rInputTypes = lInputTypes
								lRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
								rRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
								lOrderingCols = generateColumnOrdering(rng, nCols, nOrderingCols)
								rOrderingCols = generateColumnOrdering(rng, nCols, nOrderingCols)
							}
							// Set the directions of both columns to be the same.
							for i, lCol := range lOrderingCols {
								rOrderingCols[i].Direction = lCol.Direction
							}

							lMatchedCols := execinfrapb.ConvertToColumnOrdering(execinfrapb.Ordering{Columns: lOrderingCols})
							rMatchedCols := execinfrapb.ConvertToColumnOrdering(execinfrapb.Ordering{Columns: rOrderingCols})
							sort.Slice(lRows, func(i, j int) bool {
								cmp, err := lRows[i].Compare(lInputTypes, &da, lMatchedCols, &evalCtx, lRows[j])
								if err != nil {
									t.Fatal(err)
								}
								return cmp < 0
							})
							sort.Slice(rRows, func(i, j int) bool {
								cmp, err := rRows[i].Compare(rInputTypes, &da, rMatchedCols, &evalCtx, rRows[j])
								if err != nil {
									t.Fatal(err)
								}
								return cmp < 0
							})
							var outputTypes []*types.T
							if testSpec.joinType.ShouldIncludeRightColsInOutput() {
								outputTypes = append(lInputTypes, rInputTypes...)
							} else {
								outputTypes = lInputTypes
							}
							outputColumns := make([]uint32, len(outputTypes))
							for i := range outputColumns {
								outputColumns[i] = uint32(i)
							}

							var filter, onExpr execinfrapb.Expression
							if addFilter {
								colTypes := append(lInputTypes, rInputTypes...)
								filter = generateFilterExpr(
									rng, nCols, nOrderingCols, colTypes, usingRandomTypes,
									!testSpec.joinType.ShouldIncludeRightColsInOutput(),
								)
							}
							if triedWithoutOnExpr {
								colTypes := append(lInputTypes, rInputTypes...)
								onExpr = generateFilterExpr(
									rng, nCols, nOrderingCols, colTypes, usingRandomTypes, false, /* forceLeftSide */
								)
							}
							mjSpec := &execinfrapb.MergeJoinerSpec{
								OnExpr:        onExpr,
								LeftOrdering:  execinfrapb.Ordering{Columns: lOrderingCols},
								RightOrdering: execinfrapb.Ordering{Columns: rOrderingCols},
								Type:          testSpec.joinType,
								NullEquality:  testSpec.joinType.IsSetOpJoin(),
							}
							pspec := &execinfrapb.ProcessorSpec{
								Input: []execinfrapb.InputSyncSpec{{ColumnTypes: lInputTypes}, {ColumnTypes: rInputTypes}},
								Core:  execinfrapb.ProcessorCoreUnion{MergeJoiner: mjSpec},
								Post:  execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outputColumns, Filter: filter},
							}
							args := verifyColOperatorArgs{
								anyOrder:    testSpec.anyOrder,
								inputTypes:  [][]*types.T{lInputTypes, rInputTypes},
								inputs:      []sqlbase.EncDatumRows{lRows, rRows},
								outputTypes: outputTypes,
								pspec:       pspec,
								rng:         rng,
							}
							if testSpec.joinType.IsSetOpJoin() && nOrderingCols < nCols {
								// The output of set operation joins is not fully
								// deterministic when there are non-equality
								// columns, however, the rows must match on the
								// equality columns between vectorized and row
								// executions.
								args.colIdxsToCheckForEquality = make([]int, nOrderingCols)
								for i := range args.colIdxsToCheckForEquality {
									args.colIdxsToCheckForEquality[i] = int(lOrderingCols[i].ColIdx)
								}
							}
							if err := verifyColOperator(args); err != nil {
								fmt.Printf("--- join type = %s onExpr = %q filter = %q seed = %d run = %d ---\n",
									testSpec.joinType.String(), onExpr.Expr, filter.Expr, seed, run)
								fmt.Printf("--- left ordering = %v right ordering = %v ---\n", lOrderingCols, rOrderingCols)
								prettyPrintTypes(lInputTypes, "left_table" /* tableName */)
								prettyPrintTypes(rInputTypes, "right_table" /* tableName */)
								prettyPrintInput(lRows, lInputTypes, "left_table" /* tableName */)
								prettyPrintInput(rRows, rInputTypes, "right_table" /* tableName */)
								t.Fatal(err)
							}
							if onExpr.Expr == "" {
								triedWithoutOnExpr = true
							} else {
								triedWithOnExpr = true
							}
						}
					}
				}
			}
		}
	}
}

// generateColumnOrdering produces a random ordering of nOrderingCols columns
// on a table with nCols columns, so nOrderingCols must be not greater than
// nCols.
func generateColumnOrdering(
	rng *rand.Rand, nCols int, nOrderingCols int,
) []execinfrapb.Ordering_Column {
	if nOrderingCols > nCols {
		panic("nOrderingCols > nCols in generateColumnOrdering")
	}

	orderingCols := make([]execinfrapb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(nCols)[:nOrderingCols] {
		orderingCols[i] = execinfrapb.Ordering_Column{
			ColIdx:    uint32(col),
			Direction: execinfrapb.Ordering_Column_Direction(rng.Intn(2)),
		}
	}
	return orderingCols
}

func getAddFilterOptions(joinType sqlbase.JoinType, nonEqualityColsPresent bool) []bool {
	if joinType.IsSetOpJoin() && nonEqualityColsPresent {
		// Output of set operation join when rows have non equality columns is
		// not deterministic, so applying a filter on top of it can produce
		// arbitrary results, and we skip such configuration.
		return []bool{false}
	}
	return []bool{false, true}
}

// generateFilterExpr populates an execinfrapb.Expression that contains a
// single comparison which can be either comparing a column from the left
// against a column from the right or comparing a column from either side
// against a constant.
// If forceConstComparison is true, then the comparison against the constant
// will be used.
// If forceLeftSide is true, then the comparison of a column from the left
// against a constant will be used.
func generateFilterExpr(
	rng *rand.Rand,
	nCols int,
	nEqCols int,
	colTypes []*types.T,
	forceConstComparison bool,
	forceLeftSide bool,
) execinfrapb.Expression {
	var comparison string
	r := rng.Float64()
	if r < 0.25 {
		comparison = "<"
	} else if r < 0.5 {
		comparison = ">"
	} else if r < 0.75 {
		comparison = "="
	} else {
		comparison = "<>"
	}
	// When all columns are used in equality comparison between inputs, there is
	// only one interesting case when a column from either side is compared
	// against a constant. The second conditional is us choosing to compare
	// against a constant.
	if nCols == nEqCols || rng.Float64() < 0.33 || forceConstComparison || forceLeftSide {
		colIdx := rng.Intn(nCols)
		if !forceLeftSide && rng.Float64() >= 0.5 {
			// Use right side.
			colIdx += nCols
		}
		constDatum := sqlbase.RandDatum(rng, colTypes[colIdx], true /* nullOk */)
		constDatumString := constDatum.String()
		switch colTypes[colIdx].Family() {
		case types.FloatFamily, types.DecimalFamily:
			if strings.Contains(strings.ToLower(constDatumString), "nan") ||
				strings.Contains(strings.ToLower(constDatumString), "inf") {
				// We need to surround special numerical values with quotes.
				constDatumString = fmt.Sprintf("'%s'", constDatumString)
			}
		}
		return execinfrapb.Expression{Expr: fmt.Sprintf("@%d %s %s", colIdx+1, comparison, constDatumString)}
	}
	// We will compare a column from the left against a column from the right.
	leftColIdx := rng.Intn(nCols) + 1
	rightColIdx := rng.Intn(nCols) + nCols + 1
	return execinfrapb.Expression{Expr: fmt.Sprintf("@%d %s @%d", leftColIdx, comparison, rightColIdx)}
}

func TestWindowFunctionsAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, seed := randutil.NewPseudoRand()
	nRows := 2 * coldata.BatchSize()
	maxCols := 4
	maxNum := 10
	typs := make([]*types.T, maxCols)
	for i := range typs {
		// TODO(yuzefovich): randomize the types of the columns once we support
		// window functions that take in arguments.
		typs[i] = types.Int
	}
	for windowFn := range colbuilder.SupportedWindowFns {
		for _, partitionBy := range [][]uint32{
			{},     // No PARTITION BY clause.
			{0},    // Partitioning on the first input column.
			{0, 1}, // Partitioning on the first and second input columns.
		} {
			for _, nOrderingCols := range []int{
				0, // No ORDER BY clause.
				1, // ORDER BY on at most one column.
				2, // ORDER BY on at most two columns.
			} {
				for nCols := 1; nCols <= maxCols; nCols++ {
					if len(partitionBy) > nCols || nOrderingCols > nCols {
						continue
					}
					inputTypes := typs[:nCols:nCols]
					rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)

					windowerSpec := &execinfrapb.WindowerSpec{
						PartitionBy: partitionBy,
						WindowFns: []execinfrapb.WindowerSpec_WindowFn{
							{
								Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &windowFn},
								Ordering:     generateOrderingGivenPartitionBy(rng, nCols, nOrderingCols, partitionBy),
								OutputColIdx: uint32(nCols),
								FilterColIdx: tree.NoColumnIdx,
							},
						},
					}
					if windowFn == execinfrapb.WindowerSpec_ROW_NUMBER &&
						len(partitionBy)+len(windowerSpec.WindowFns[0].Ordering.Columns) < nCols {
						// The output of row_number is not deterministic if there are
						// columns that are not present in either PARTITION BY or ORDER BY
						// clauses, so we skip such a configuration.
						continue
					}

					pspec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
						Core:  execinfrapb.ProcessorCoreUnion{Windower: windowerSpec},
					}
					// Currently, we only support window functions that take no
					// arguments, so we leave the second argument empty.
					_, outputType, err := execinfrapb.GetWindowFunctionInfo(execinfrapb.WindowerSpec_Func{WindowFunc: &windowFn})
					require.NoError(t, err)
					args := verifyColOperatorArgs{
						anyOrder:    true,
						inputTypes:  [][]*types.T{inputTypes},
						inputs:      []sqlbase.EncDatumRows{rows},
						outputTypes: append(inputTypes, outputType),
						pspec:       pspec,
					}
					if err := verifyColOperator(args); err != nil {
						fmt.Printf("seed = %d\n", seed)
						prettyPrintTypes(inputTypes, "t" /* tableName */)
						prettyPrintInput(rows, inputTypes, "t" /* tableName */)
						t.Fatal(err)
					}
				}
			}
		}
	}
}

// generateRandomSupportedTypes generates nCols random types that are supported
// by the vectorized engine.
func generateRandomSupportedTypes(rng *rand.Rand, nCols int) []*types.T {
	typs := make([]*types.T, 0, nCols)
	for len(typs) < nCols {
		typ := sqlbase.RandType(rng)
		if typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) == typeconv.DatumVecCanonicalTypeFamily {
			// At the moment, we disallow datum-backed types.
			// TODO(yuzefovich): remove this.
			continue
		}
		typs = append(typs, typ)
	}
	return typs
}

// generateRandomComparableTypes generates random types that are supported by
// the vectorized engine and are such that they are comparable to the
// corresponding types in inputTypes.
func generateRandomComparableTypes(rng *rand.Rand, inputTypes []*types.T) []*types.T {
	typs := make([]*types.T, len(inputTypes))
	for i, inputType := range inputTypes {
		for {
			typ := sqlbase.RandType(rng)
			if typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) == typeconv.DatumVecCanonicalTypeFamily {
				// At the moment, we disallow datum-backed types.
				// TODO(yuzefovich): remove this.
				continue
			}
			comparable := false
			for _, cmpOverloads := range tree.CmpOps[tree.LT] {
				o := cmpOverloads.(*tree.CmpOp)
				if inputType.Equivalent(o.LeftType) && typ.Equivalent(o.RightType) {
					if (typ.Family() == types.DateFamily && inputType.Family() != types.DateFamily) ||
						(typ.Family() != types.DateFamily && inputType.Family() == types.DateFamily) {
						// We map Dates to int64 and don't have casts from int64 to
						// timestamps (and there is a comparison between dates and
						// timestamps).
						continue
					}
					comparable = true
					break
				}
			}
			if comparable {
				typs[i] = typ
				break
			}
		}
	}
	return typs
}

// generateOrderingGivenPartitionBy produces a random ordering of up to
// nOrderingCols columns on a table with nCols columns such that only columns
// not present in partitionBy are used. This is useful to simulate how
// optimizer plans window functions - for example, with an OVER clause as
// (PARTITION BY a ORDER BY a DESC), the optimizer will omit the ORDER BY
// clause entirely.
func generateOrderingGivenPartitionBy(
	rng *rand.Rand, nCols int, nOrderingCols int, partitionBy []uint32,
) execinfrapb.Ordering {
	var ordering execinfrapb.Ordering
	if nOrderingCols == 0 || len(partitionBy) == nCols {
		return ordering
	}
	ordering = execinfrapb.Ordering{Columns: make([]execinfrapb.Ordering_Column, 0, nOrderingCols)}
	for len(ordering.Columns) == 0 {
		for _, ordCol := range generateColumnOrdering(rng, nCols, nOrderingCols) {
			usedInPartitionBy := false
			for _, p := range partitionBy {
				if p == ordCol.ColIdx {
					usedInPartitionBy = true
					break
				}
			}
			if !usedInPartitionBy {
				ordering.Columns = append(ordering.Columns, ordCol)
			}
		}
	}
	return ordering
}

// prettyPrintTypes prints out typs as a CREATE TABLE statement.
func prettyPrintTypes(typs []*types.T, tableName string) {
	fmt.Printf("CREATE TABLE %s(", tableName)
	colName := byte('a')
	for typIdx, typ := range typs {
		if typIdx < len(typs)-1 {
			fmt.Printf("%c %s, ", colName, typ.SQLStandardName())
		} else {
			fmt.Printf("%c %s);\n", colName, typ.SQLStandardName())
		}
		colName++
	}
}

// prettyPrintInput prints out rows as INSERT INTO tableName VALUES statement.
func prettyPrintInput(rows sqlbase.EncDatumRows, inputTypes []*types.T, tableName string) {
	fmt.Printf("INSERT INTO %s VALUES\n", tableName)
	for rowIdx, row := range rows {
		fmt.Printf("(%s", row[0].String(inputTypes[0]))
		for i := range row[1:] {
			fmt.Printf(", %s", row[i+1].String(inputTypes[i+1]))
		}
		if rowIdx < len(rows)-1 {
			fmt.Printf("),\n")
		} else {
			fmt.Printf(");\n")
		}
	}
}
