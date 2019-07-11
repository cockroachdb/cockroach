// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSorterAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()

	nRows := 100
	maxCols := 5
	maxNum := 10
	// TODO (yuzefovich): change nullProbability to non 0 value.
	nullProbability := 0.0
	typs := make([]types.T, maxCols)
	for i := range typs {
		typs[i] = *types.Int
	}
	for nCols := 1; nCols <= maxCols; nCols++ {
		inputTypes := typs[:nCols]

		rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
		// Note: we're only generating column orderings on all nCols columns since
		// if there are columns not in the ordering, the results are not fully
		// deterministic.
		orderingCols := generateColumnOrdering(rng, nCols, nCols)
		sorterSpec := &distsqlpb.SorterSpec{
			OutputOrdering: distsqlpb.Ordering{Columns: orderingCols},
		}
		pspec := &distsqlpb.ProcessorSpec{
			Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}},
			Core:  distsqlpb.ProcessorCoreUnion{Sorter: sorterSpec},
		}
		if err := verifyColOperator(false /* anyOrder */, [][]types.T{inputTypes}, []sqlbase.EncDatumRows{rows}, inputTypes, pspec); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSortChunksAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()

	nRows := 100
	maxCols := 5
	maxNum := 10
	// TODO (yuzefovich): change nullProbability to non 0 value.
	nullProbability := 0.0
	typs := make([]types.T, maxCols)
	for i := range typs {
		typs[i] = *types.Int
	}
	for nCols := 1; nCols <= maxCols; nCols++ {
		inputTypes := typs[:nCols]
		// Note: we're only generating column orderings on all nCols columns since
		// if there are columns not in the ordering, the results are not fully
		// deterministic.
		orderingCols := generateColumnOrdering(rng, nCols, nCols)
		for matchLen := 1; matchLen <= nCols; matchLen++ {
			rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
			matchedCols := distsqlpb.ConvertToColumnOrdering(distsqlpb.Ordering{Columns: orderingCols[:matchLen]})
			// Presort the input on first matchLen columns.
			sort.Slice(rows, func(i, j int) bool {
				cmp, err := rows[i].Compare(inputTypes, &da, matchedCols, &evalCtx, rows[j])
				if err != nil {
					t.Fatal(err)
				}
				return cmp < 0
			})

			sorterSpec := &distsqlpb.SorterSpec{
				OutputOrdering:   distsqlpb.Ordering{Columns: orderingCols},
				OrderingMatchLen: uint32(matchLen),
			}
			pspec := &distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}},
				Core:  distsqlpb.ProcessorCoreUnion{Sorter: sorterSpec},
			}
			if err := verifyColOperator(false /* anyOrder */, [][]types.T{inputTypes}, []sqlbase.EncDatumRows{rows}, inputTypes, pspec); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMergeJoinerAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	seed := rand.Int()
	rng := rand.New(rand.NewSource(int64(seed)))

	nRuns := 10
	nRows := 10
	maxCols := 3
	maxNum := 5
	nullProbability := 0.1
	typs := make([]types.T, maxCols)
	for i := range typs {
		// TODO (georgeutsin): Randomize the types of the columns.
		typs[i] = *types.Int
	}
	for run := 1; run < nRuns; run++ {
		for _, joinType := range []sqlbase.JoinType{
			sqlbase.JoinType_INNER,
			sqlbase.JoinType_LEFT_OUTER,
			sqlbase.JoinType_RIGHT_OUTER,
		} {
			for nCols := 1; nCols <= maxCols; nCols++ {
				for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
					inputTypes := typs[:nCols]
					lOrderingCols := generateColumnOrdering(rng, nCols, nOrderingCols)
					rOrderingCols := generateColumnOrdering(rng, nCols, nOrderingCols)
					// Set the directions of both columns to be the same.
					for i, lCol := range lOrderingCols {
						rOrderingCols[i].Direction = lCol.Direction
					}

					lRows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
					rRows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
					lMatchedCols := distsqlpb.ConvertToColumnOrdering(distsqlpb.Ordering{Columns: lOrderingCols})
					rMatchedCols := distsqlpb.ConvertToColumnOrdering(distsqlpb.Ordering{Columns: rOrderingCols})
					sort.Slice(lRows, func(i, j int) bool {
						cmp, err := lRows[i].Compare(inputTypes, &da, lMatchedCols, &evalCtx, lRows[j])
						if err != nil {
							t.Fatal(err)
						}
						return cmp < 0
					})
					sort.Slice(rRows, func(i, j int) bool {
						cmp, err := rRows[i].Compare(inputTypes, &da, rMatchedCols, &evalCtx, rRows[j])
						if err != nil {
							t.Fatal(err)
						}
						return cmp < 0
					})

					mjSpec := &distsqlpb.MergeJoinerSpec{
						LeftOrdering:  distsqlpb.Ordering{Columns: lOrderingCols},
						RightOrdering: distsqlpb.Ordering{Columns: rOrderingCols},
						Type:          joinType,
					}
					pspec := &distsqlpb.ProcessorSpec{
						Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}, {ColumnTypes: inputTypes}},
						Core:  distsqlpb.ProcessorCoreUnion{MergeJoiner: mjSpec},
					}
					if err := verifyColOperator(false /* anyOrder */, [][]types.T{inputTypes, inputTypes}, []sqlbase.EncDatumRows{lRows, rRows}, append(inputTypes, inputTypes...), pspec); err != nil {
						fmt.Printf("--- seed = %d run = %d ---\n", seed, run)
						t.Fatal(err)
					}
				}
			}
		}
	}
}

// generateColumnOrdering produces a random ordering of nOrderingCols columns
// on a table with nCols columns, so nOrderingCols must be not greater than
// nCols
func generateColumnOrdering(
	rng *rand.Rand, nCols int, nOrderingCols int,
) []distsqlpb.Ordering_Column {
	if nOrderingCols > nCols {
		panic("nOrderingCols > nCols in generateColumnOrdering")
	}

	orderingCols := make([]distsqlpb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(nCols)[:nOrderingCols] {
		orderingCols[i] = distsqlpb.Ordering_Column{
			ColIdx:    uint32(col),
			Direction: distsqlpb.Ordering_Column_Direction(rng.Intn(2)),
		}
	}
	return orderingCols
}

func TestWindowFunctionsAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	nRows := 10
	maxCols := 4
	maxNum := 5
	// TODO(yuzefovich): use non-zero null probability once sorter handles nulls.
	nullProbability := 0.0
	typs := make([]types.T, maxCols)
	for i := range typs {
		// TODO(yuzefovich): randomize the types of the columns once we support
		// window functions that take in arguments.
		typs[i] = *types.Int
	}
	for _, windowFn := range []distsqlpb.WindowerSpec_WindowFunc{
		distsqlpb.WindowerSpec_ROW_NUMBER,
		distsqlpb.WindowerSpec_RANK,
		distsqlpb.WindowerSpec_DENSE_RANK,
	} {
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
					inputTypes := typs[:nCols]
					rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)

					windowerSpec := &distsqlpb.WindowerSpec{
						PartitionBy: partitionBy,
						WindowFns: []distsqlpb.WindowerSpec_WindowFn{
							{
								Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &windowFn},
								Ordering:     generateOrderingGivenPartitionBy(rng, nCols, nOrderingCols, partitionBy),
								OutputColIdx: uint32(nCols),
							},
						},
					}
					if windowFn == distsqlpb.WindowerSpec_ROW_NUMBER &&
						len(partitionBy)+len(windowerSpec.WindowFns[0].Ordering.Columns) < nCols {
						// The output of row_number is not deterministic if there are
						// columns that are not present in either PARTITION BY or ORDER BY
						// clauses, so we skip such a configuration.
						continue
					}

					pspec := &distsqlpb.ProcessorSpec{
						Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}},
						Core:  distsqlpb.ProcessorCoreUnion{Windower: windowerSpec},
					}
					if err := verifyColOperator(true /* anyOrder */, [][]types.T{inputTypes}, []sqlbase.EncDatumRows{rows}, append(inputTypes, *types.Int), pspec); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

// generateOrderingGivenPartitionBy produces a random ordering of up to
// nOrderingCols columns on a table with nCols columns such that only columns
// not present in partitionBy are used. This is useful to simulate how
// optimizer plans window functions - for example, with an OVER clause as
// (PARTITION BY a ORDER BY a DESC), the optimizer will omit the ORDER BY
// clause entirely.
func generateOrderingGivenPartitionBy(
	rng *rand.Rand, nCols int, nOrderingCols int, partitionBy []uint32,
) distsqlpb.Ordering {
	var ordering distsqlpb.Ordering
	if nOrderingCols == 0 || len(partitionBy) == nCols {
		return ordering
	}
	ordering = distsqlpb.Ordering{Columns: make([]distsqlpb.Ordering_Column, 0, nOrderingCols)}
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
