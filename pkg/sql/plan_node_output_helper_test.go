// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMutationOutputHelper(t *testing.T) {
	rnd, _ := randutil.NewTestRand()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	makeRow := func(colTypes []*types.T) tree.Datums {
		row := make(tree.Datums, len(colTypes))
		for i, typ := range colTypes {
			row[i] = randgen.RandDatum(rnd, typ, true /* nullOk */)
		}
		return row
	}

	t.Run("rows not needed", func(t *testing.T) {
		helper := mutationOutputHelper{}
		modifiedCount := rnd.Intn(100)
		for range modifiedCount {
			helper.onModifiedRow()
		}
		require.Equal(t, modifiedCount, int(helper.modifiedRowCount()))

		// addRow is a no-op if rowsNeeded is false.
		colTypes := randgen.RandSortingTypes(rnd, rnd.Intn(10)+1)
		require.NoError(t, helper.addRow(ctx, makeRow(colTypes)))

		require.True(t, helper.next())
		res := helper.values()
		require.Equal(t, len(res), 1)
		rowCount, ok := res[0].(*tree.DInt)
		require.True(t, ok)
		require.Equal(t, modifiedCount, int(*rowCount))
		require.False(t, helper.next())
	})

	t.Run("rows needed", func(t *testing.T) {
		helper := mutationOutputHelper{rowsNeeded: true}
		colTypes := randgen.RandSortingTypes(rnd, rnd.Intn(10)+1)
		memAcc := evalCtx.TestingMon.MakeBoundAccount()
		helper.rows = rowcontainer.NewRowContainer(memAcc, colinfo.ColTypeInfoFromColTypes(colTypes))

		// 50% chance of using one or zero rows.
		var numRows int
		if rnd.Float64() < 0.5 {
			numRows = rnd.Intn(100) + 2
		} else {
			numRows = rnd.Intn(1)
		}
		for range numRows {
			helper.onModifiedRow()
			require.NoError(t, helper.addRow(ctx, makeRow(colTypes)))
		}
		require.Equal(t, numRows, int(helper.modifiedRowCount()))
		require.Equal(t, numRows, helper.rows.Len())

		var nextCount int
		for helper.next() {
			nextCount++
			res := helper.values()
			require.Equal(t, len(res), len(colTypes))
			for i := range res {
				require.NotNil(t, res[i])
			}
		}
		require.Equal(t, numRows, nextCount)
		require.NotPanics(t, func() { helper.values() })
		require.NotPanics(t, func() { helper.close(ctx) })
	})
}

func TestRowsAffectedOutputHelper(t *testing.T) {
	rnd, _ := randutil.NewPseudoRand()

	makeHelper := func() *rowsAffectedOutputHelper {
		return &rowsAffectedOutputHelper{rowCount: rnd.Intn(20)}
	}

	t.Run("multiple next calls", func(t *testing.T) {
		helper := makeHelper()
		require.True(t, helper.next())
		require.False(t, helper.next())
		require.False(t, helper.next())
	})

	t.Run("output row count", func(t *testing.T) {
		helper := makeHelper()
		require.True(t, helper.next())
		res := helper.values()
		require.Equal(t, len(res), 1)
		rowCount, ok := res[0].(*tree.DInt)
		require.True(t, ok)
		require.Equal(t, helper.rowCount, int(*rowCount))
		require.False(t, helper.next())
	})
}
