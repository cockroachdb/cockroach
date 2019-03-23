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
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestSortChunksAgainstProcessor(t *testing.T) {
	var da sqlbase.DatumAlloc
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	nRows := 100
	maxCols := 5
	modulus := 10
	for nCols := 1; nCols <= maxCols; nCols++ {
		types := make([]sqlbase.ColumnType, nCols)
		for i := range types {
			types[i] = sqlbase.IntType
		}
		orderingCols := generateColumnOrdering(rng, nCols)

		for matchLen := 1; matchLen <= nCols; matchLen++ {
			rows := sqlbase.MakeRandIntRowsModulus(rng, nRows, nCols, modulus)
			matchedCols := make(sqlbase.ColumnOrdering, matchLen)
			for i := range matchedCols {
				matchedCols[i] = sqlbase.ColumnOrderInfo{ColIdx: int(orderingCols[i].ColIdx), Direction: encoding.Direction(orderingCols[i].Direction + 1)}
			}
			// Presort the input on first matchLen columns.
			sort.Slice(rows, func(i, j int) bool {
				cmp, err := rows[i].Compare(
					types,
					&da,
					matchedCols,
					&evalCtx,
					rows[j],
				)
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
				Input: []distsqlpb.InputSyncSpec{{ColumnTypes: types}},
				Core:  distsqlpb.ProcessorCoreUnion{Sorter: sorterSpec},
			}
			if err := verifyColOperator(false, [][]sqlbase.ColumnType{types}, []sqlbase.EncDatumRows{rows}, types, pspec); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSorterAgainstProcessor(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	nRows := 100
	maxCols := 5
	modulus := 10
	for nCols := 1; nCols <= maxCols; nCols++ {
		types := make([]sqlbase.ColumnType, nCols)
		for i := range types {
			types[i] = sqlbase.IntType
		}

		rows := sqlbase.MakeRandIntRowsModulus(rng, nRows, nCols, modulus)
		orderingCols := generateColumnOrdering(rng, nCols)
		sorterSpec := &distsqlpb.SorterSpec{
			OutputOrdering: distsqlpb.Ordering{Columns: orderingCols},
		}
		pspec := &distsqlpb.ProcessorSpec{
			Input: []distsqlpb.InputSyncSpec{{ColumnTypes: types}},
			Core:  distsqlpb.ProcessorCoreUnion{Sorter: sorterSpec},
		}
		if err := verifyColOperator(false, [][]sqlbase.ColumnType{types}, []sqlbase.EncDatumRows{rows}, types, pspec); err != nil {
			t.Fatal(err)
		}
	}
}

func generateColumnOrdering(rng *rand.Rand, nCols int) []distsqlpb.Ordering_Column {
	orderingCols := make([]distsqlpb.Ordering_Column, nCols)
	usedCol := make([]bool, nCols)
	for i := range orderingCols {
		var col int
		for {
			col = rng.Intn(nCols)
			if !usedCol[col] {
				usedCol[col] = true
				break
			}
		}
		orderingCols[i] = distsqlpb.Ordering_Column{ColIdx: uint32(col), Direction: distsqlpb.Ordering_Column_Direction(rng.Intn(2))}
	}
	return orderingCols
}
