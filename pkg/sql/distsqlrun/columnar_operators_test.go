// Copyright 2019 The Cockroach Authors.
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

package distsqlrun

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
	typs := make([]sqlbase.ColumnType, maxCols)
	for i := range typs {
		typs[i] = sqlbase.IntType
	}
	for nCols := 1; nCols <= maxCols; nCols++ {
		inputTypes := typs[:nCols]
		rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum)
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
		if err := verifyColOperator(false /* anyOrder */, [][]sqlbase.ColumnType{inputTypes}, []sqlbase.EncDatumRows{rows}, inputTypes, pspec); err != nil {
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
	typs := make([]sqlbase.ColumnType, maxCols)
	for i := range typs {
		typs[i] = sqlbase.IntType
	}
	for nCols := 1; nCols <= maxCols; nCols++ {
		inputTypes := typs[:nCols]
		// Note: we're only generating column orderings on all nCols columns since
		// if there are columns not in the ordering, the results are not fully
		// deterministic.
		orderingCols := generateColumnOrdering(rng, nCols, nCols)
		for matchLen := 1; matchLen <= nCols; matchLen++ {
			rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum)
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
			if err := verifyColOperator(false /* anyOrder */, [][]sqlbase.ColumnType{inputTypes}, []sqlbase.EncDatumRows{rows}, inputTypes, pspec); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMergeJoinerAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()

	nRows := 100
	maxCols := 5
	maxNum := 10
	typs := make([]sqlbase.ColumnType, maxCols)
	for i := range typs {
		typs[i] = sqlbase.IntType
	}
	for nCols := 1; nCols <= maxCols; nCols++ {
		inputTypes := typs[:nCols]
		// Note: we're only generating column orderings on all nCols columns since
		// if there are columns not in the ordering, the results are not fully
		// deterministic.
		directions := generateColumnDirections(rng, nCols)
		lOrderingCols := generateColumnOrderingWithDirections(rng, nCols, nCols, directions)
		rOrderingCols := generateColumnOrderingWithDirections(rng, nCols, nCols, directions)

		lRows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum)
		rRows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum)
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
		}
		pspec := &distsqlpb.ProcessorSpec{
			Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}, {ColumnTypes: inputTypes}},
			Core:  distsqlpb.ProcessorCoreUnion{MergeJoiner: mjSpec},
		}
		if err := verifyColOperator(false /* anyOrder */, [][]sqlbase.ColumnType{inputTypes, inputTypes}, []sqlbase.EncDatumRows{lRows, rRows}, append(inputTypes, inputTypes...), pspec); err != nil {
			t.Fatal(err)
		}
	}
}

// generateColumnOrdering produces a random ordering of nOrderingCols columns
// on a table with nCols columns, so nOrderingCols must be not greater than
// nCols, given a slice of column directions.
func generateColumnOrderingWithDirections(
	rng *rand.Rand, nCols int, nOrderingCols int, directions []distsqlpb.Ordering_Column_Direction,
) []distsqlpb.Ordering_Column {
	if nOrderingCols > nCols {
		panic("nOrderingCols > nCols in generateColumnOrdering")
	}
	orderingCols := make([]distsqlpb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(nCols)[:nOrderingCols] {
		orderingCols[i] = distsqlpb.Ordering_Column{ColIdx: uint32(col), Direction: directions[i]}
	}
	return orderingCols
}

// generateColumnDirections produces a slice of random direction given the number
// of columns in the ordering.
func generateColumnDirections(
	rng *rand.Rand, nOrderingCols int,
) []distsqlpb.Ordering_Column_Direction {
	directions := make([]distsqlpb.Ordering_Column_Direction, nOrderingCols)
	for i := 0; i < nOrderingCols; i++ {
		directions[i] = distsqlpb.Ordering_Column_Direction(rng.Intn(2))
	}

	return directions
}

// generateColumnOrdering is a wrapper for generateColumnOrderingWithDirections
// that also includes generating random directions.
func generateColumnOrdering(
	rng *rand.Rand, nCols int, nOrderingCols int,
) []distsqlpb.Ordering_Column {
	directions := generateColumnDirections(rng, nOrderingCols)
	return generateColumnOrderingWithDirections(rng, nCols, nOrderingCols, directions)
}
