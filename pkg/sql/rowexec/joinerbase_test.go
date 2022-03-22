// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestAddColumnsNeededByOnExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	checkOneSide := func(jb *joinerBase, startIdx, endIdx int, needed []int) {
		var neededCols util.FastIntSet
		var expected util.FastIntSet
		jb.addColumnsNeededByOnExpr(&neededCols, startIdx, endIdx)
		for _, col := range needed {
			expected.Add(col)
		}
		require.Equal(t, expected, neededCols)
	}

	leftTypes := types.ThreeIntCols
	rightTypes := types.FourIntCols
	for _, tc := range []struct {
		onExpr         execinfrapb.Expression
		neededFromLeft []int
		// Note that onExpr refers to columns from right with len(leftTypes)
		// offset, but neededFromRight without it.
		neededFromRight []int
	}{
		{
			onExpr:          execinfrapb.Expression{Expr: "@1 > 1 AND @6 > 1"},
			neededFromLeft:  []int{0},
			neededFromRight: []int{2},
		},
		{
			onExpr:         execinfrapb.Expression{Expr: "@2 > 1 AND @3 > 1"},
			neededFromLeft: []int{1, 2},
		},
		{
			onExpr:          execinfrapb.Expression{Expr: "@5 > 1 AND @7 > 1 OR @4 < 1"},
			neededFromRight: []int{0, 1, 3},
		},
	} {
		var jb joinerBase
		require.NoError(t, jb.init(
			nil, /* self */
			flowCtx,
			0, /* processorID */
			leftTypes,
			rightTypes,
			descpb.InnerJoin,
			tc.onExpr,
			false, /* outputContinuationColumn */
			&execinfrapb.PostProcessSpec{},
			&distsqlutils.RowBuffer{},
			execinfra.ProcStateOpts{},
		))
		checkOneSide(&jb, 0 /* startIdx */, len(leftTypes), tc.neededFromLeft)
		checkOneSide(&jb, len(leftTypes), len(leftTypes)+len(rightTypes), tc.neededFromRight)
	}
}
