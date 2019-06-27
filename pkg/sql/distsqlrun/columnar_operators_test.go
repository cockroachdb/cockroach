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
	rng, _ := randutil.NewPseudoRand()

	nRows := 100
	maxCols := 5
	maxNum := 10
	nullProbability := 0.1
	typs := make([]types.T, maxCols)
	for i := range typs {
		// TODO (georgeutsin): Randomize the types of the columns.
		typs[i] = *types.Int
	}
	for _, joinType := range []sqlbase.JoinType{
		sqlbase.JoinType_INNER,
		// TODO(yuzefovich): uncomment once ordered distinct handles nulls.
		//sqlbase.JoinType_LEFT_OUTER,
	} {
		for nCols := 1; nCols <= maxCols; nCols++ {
			inputTypes := typs[:nCols]
			// Note: we're only generating column orderings on all nCols columns since
			// if there are columns not in the ordering, the results are not fully
			// deterministic.
			lOrderingCols := generateColumnOrdering(rng, nCols, nCols)
			rOrderingCols := generateColumnOrdering(rng, nCols, nCols)
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
				t.Fatal(err)
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
