// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type mergeJoinerTestCase struct {
	spec          execinfrapb.MergeJoinerSpec
	outCols       []uint32
	leftTypes     []*types.T
	leftInput     rowenc.EncDatumRows
	rightTypes    []*types.T
	rightInput    rowenc.EncDatumRows
	expectedTypes []*types.T
	expected      rowenc.EncDatumRows
}

func TestMergeJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	null := rowenc.EncDatum{Datum: tree.DNull}

	testCases := []mergeJoinerTestCase{
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.InnerJoin,
				// Implicit @1 = @3 constraint.
			},
			outCols:   []uint32{0, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.InnerJoin,
				// Implicit @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1, 3},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[4]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[1]},
				{v[0], v[0], v[0]},
				{v[0], v[0], v[5]},
				{v[0], v[0], v[4]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[1]},
				{v[0], v[1], v[0]},
				{v[0], v[1], v[5]},
				{v[0], v[1], v[4]},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   descpb.InnerJoin,
				OnExpr: execinfrapb.Expression{Expr: "@4 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1, 3},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[4]},
				{v[1], v[4]},
				{v[1], v[1]},
				{v[1], v[0]},
				{v[1], v[5]},
				{v[1], v[4]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[5]},
				{v[0], v[0], v[4]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[5]},
				{v[0], v[1], v[4]},
				{v[1], v[0], v[4]},
				{v[1], v[0], v[5]},
				{v[1], v[0], v[4]},
				{v[1], v[1], v[4]},
				{v[1], v[1], v[5]},
				{v[1], v[1], v[4]},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   descpb.FullOuterJoin,
				OnExpr: execinfrapb.Expression{Expr: "@2 >= @4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1, 3},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},

				{v[1], v[5]},

				{v[2], v[0]},
				{v[2], v[8]},

				{v[3], v[5]},

				{v[6], v[0]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[5]},
				{v[0], v[5]},

				{v[1], v[0]},
				{v[1], v[8]},

				{v[2], v[5]},

				{v[3], v[0]},
				{v[3], v[0]},

				{v[5], v[0]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], v[0], null},
				{v[0], v[0], null},
				{null, null, v[5]},
				{null, null, v[5]},

				{v[1], v[5], v[0]},
				{null, null, v[8]},

				{v[2], v[0], null},
				{v[2], v[8], v[5]},

				{v[3], v[5], v[0]},
				{v[3], v[5], v[0]},

				{null, null, v[0]},

				{v[6], v[0], null},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.LeftOuterJoin,
				// Implicit @1 = @3 constraint.
			},
			outCols:   []uint32{0, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], null, null},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.RightOuterJoin,
				// Implicit @1 = @3 constraint.
			},
			outCols:   []uint32{3, 1, 2},
			leftTypes: types.ThreeIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], null, null},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.FullOuterJoin,
				// Implicit @1 = @3 constraint.
			},
			outCols:   []uint32{0, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], v[5], v[1]},
			},
			expectedTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{null, v[5], v[1]},
			},
		},
		{
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
						{ColIdx: 1, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
						{ColIdx: 1, Direction: encoding.Ascending},
					}),
				Type: descpb.FullOuterJoin,
			},
			outCols:   []uint32{0, 1, 2, 3},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, v[4]},
				{v[0], null},
				{v[0], v[1]},
				{v[2], v[4]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{null, v[4]},
				{v[0], null},
				{v[0], v[1]},
				{v[2], v[4]},
			},
			expectedTypes: []*types.T{types.Int, types.Int, types.Int, types.Int},
			expected: rowenc.EncDatumRows{
				{null, v[4], null, null},
				{null, null, null, v[4]},
				{v[0], null, null, null},
				{null, null, v[0], null},
				{v[0], v[1], v[0], v[1]},
				{v[2], v[4], v[2], v[4]},
			},
		},
		{
			// Ensure that NULL = NULL is not matched.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.InnerJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.OneIntCol,
			leftInput: rowenc.EncDatumRows{
				{null},
				{v[0]},
			},
			rightTypes: types.OneIntCol,
			rightInput: rowenc.EncDatumRows{
				{null},
				{v[1]},
			},
			expectedTypes: types.TwoIntCols,
			expected:      rowenc.EncDatumRows{},
		},
		{
			// Ensure that left semi join doesn't output duplicates from
			// the right side.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.LeftSemiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[1], v[2]},
				{v[2], v[3]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[2], v[2]},
				{v[2], v[2]},
				{v[3], v[3]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{v[2], v[3]},
			},
		},
		{
			// Ensure that duplicate rows in the left are matched
			// in the output in left semi-joins.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.LeftSemiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[2]},
				{v[2], v[3]},
				{v[3], v[4]},
				{v[3], v[5]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[2], v[2]},
				{v[3], v[3]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[4]},
				{v[3], v[5]},
			},
		},
		{
			// Ensure that NULL == NULL doesn't match in left semi-join.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.LeftSemiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, v[2]},
				{v[2], v[3]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{null, v[3]},
				{v[2], v[4]},
				{v[2], v[5]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{v[2], v[3]},
			},
		},
		{
			// Ensure that OnExprs are satisfied for left semi-joins.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   descpb.LeftSemiJoin,
				OnExpr: execinfrapb.Expression{Expr: "@1 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
				{v[6], v[0]},
				{v[6], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[4]},
				{v[5], v[4]},
				{v[5], v[1]},
				{v[5], v[0]},
				{v[5], v[5]},
				{v[5], v[4]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{v[5], v[0]},
				{v[5], v[1]},
			},
		},
		{
			// Ensure that duplicate rows in the left are matched
			// in the output in left anti-joins.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.LeftAntiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[3]},
				{v[3], v[4]},
				{v[3], v[5]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[2], v[2]},
				{v[3], v[3]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
			},
		},
		{
			// Ensure that NULL == NULL doesn't match in left anti-join.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: descpb.LeftAntiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, v[2]},
				{v[2], v[3]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{null, v[3]},
				{v[2], v[4]},
				{v[2], v[5]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{null, v[2]},
			},
		},
		{
			// Ensure that OnExprs are satisfied for left anti-joins.
			spec: execinfrapb.MergeJoinerSpec{
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					colinfo.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   descpb.LeftAntiJoin,
				OnExpr: execinfrapb.Expression{Expr: "@1 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
				{v[6], v[0]},
				{v[6], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[4]},
				{v[5], v[4]},
				{v[5], v[1]},
				{v[5], v[0]},
				{v[5], v[5]},
				{v[5], v[4]},
			},
			expectedTypes: types.TwoIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
				{v[6], v[0]},
				{v[6], v[1]},
			},
		},
	}

	for _, c := range testCases {
		if c.spec.Type == descpb.LeftSemiJoin || c.spec.Type == descpb.LeftAntiJoin {
			// For every left semi and left anti join, we will automatically
			// populate a "mirroring" test case with right semi or right anti
			// join - all we need to do is to switch the inputs and change the
			// join type accordingly.
			mirroringCase := c
			mirroringCase.spec.Type, mirroringCase.spec.OnExpr = mirrorJoinTypeAndOnExpr(c.spec.Type, c.spec.OnExpr)
			mirroringCase.spec.LeftOrdering, mirroringCase.spec.RightOrdering = mirroringCase.spec.RightOrdering, mirroringCase.spec.LeftOrdering
			mirroringCase.spec.LeftEqColumnsAreKey, mirroringCase.spec.RightEqColumnsAreKey = mirroringCase.spec.RightEqColumnsAreKey, mirroringCase.spec.LeftEqColumnsAreKey
			mirroringCase.leftTypes, mirroringCase.rightTypes = mirroringCase.rightTypes, mirroringCase.leftTypes
			mirroringCase.leftInput, mirroringCase.rightInput = mirroringCase.rightInput, mirroringCase.leftInput
			testCases = append(testCases, mirroringCase)
		}
	}

	// Add INTERSECT ALL cases with MergeJoinerSpecs.
	for _, tc := range intersectAllTestCases() {
		testCases = append(testCases, setOpTestCaseToMergeJoinerTestCase(tc))
	}

	// Add EXCEPT ALL cases with MergeJoinerSpecs.
	for _, tc := range exceptAllTestCases() {
		testCases = append(testCases, setOpTestCaseToMergeJoinerTestCase(tc))
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ms := c.spec
			leftInput := distsqlutils.NewRowBuffer(c.leftTypes, c.leftInput, distsqlutils.RowBufferArgs{})
			rightInput := distsqlutils.NewRowBuffer(c.rightTypes, c.rightInput, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}
			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			m, err := newMergeJoiner(&flowCtx, 0 /* processorID */, &ms, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(context.Background())

			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}

			var retRows rowenc.EncDatumRows
			for {
				row := out.NextNoMeta(t)
				if row == nil {
					break
				}
				retRows = append(retRows, row)
			}
			expStr := c.expected.String(c.expectedTypes)
			retStr := retRows.String(c.expectedTypes)
			if expStr != retStr {
				t.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
					expStr, retStr)
			}
		})
	}
}

// Test that the joiner shuts down fine if the consumer is closed prematurely.
func TestConsumerClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	spec := execinfrapb.MergeJoinerSpec{
		LeftOrdering: execinfrapb.ConvertToSpecOrdering(
			colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightOrdering: execinfrapb.ConvertToSpecOrdering(
			colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: descpb.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	leftTypes := types.OneIntCol
	rightTypes := types.OneIntCol

	testCases := []struct {
		typ       descpb.JoinType
		leftRows  rowenc.EncDatumRows
		rightRows rowenc.EncDatumRows
	}{
		{
			typ: descpb.InnerJoin,
			// Implicit @1 = @2 constraint.
			leftRows: rowenc.EncDatumRows{
				{v[0]},
			},
			rightRows: rowenc.EncDatumRows{
				{v[0]},
			},
		},
		{
			typ: descpb.LeftOuterJoin,
			// Implicit @1 = @2 constraint.
			leftRows: rowenc.EncDatumRows{
				{v[0]},
			},
			rightRows: rowenc.EncDatumRows{},
		},
		{
			typ: descpb.RightOuterJoin,
			// Implicit @1 = @2 constraint.
			leftRows: rowenc.EncDatumRows{},
			rightRows: rowenc.EncDatumRows{
				{v[0]},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.typ.String() /* name */, func(t *testing.T) {
			leftInput := distsqlutils.NewRowBuffer(leftTypes, tc.leftRows, distsqlutils.RowBufferArgs{})
			rightInput := distsqlutils.NewRowBuffer(rightTypes, tc.rightRows, distsqlutils.RowBufferArgs{})

			// Create a consumer and close it immediately. The mergeJoiner should find out
			// about this closer the first time it attempts to push a row.
			out := &distsqlutils.RowBuffer{}
			out.ConsumerDone()

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}
			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outCols}
			m, err := newMergeJoiner(&flowCtx, 0 /* processorID */, &spec, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(context.Background())

			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
		})
	}
}

func BenchmarkMergeJoiner(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	spec := &execinfrapb.MergeJoinerSpec{
		LeftOrdering: execinfrapb.ConvertToSpecOrdering(
			colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightOrdering: execinfrapb.ConvertToSpecOrdering(
			colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: descpb.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &rowDisposer{}

	const numCols = 1
	for _, inputSize := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("InputSize=%d", inputSize), func(b *testing.B) {
			rows := randgen.MakeIntRows(inputSize, numCols)
			leftInput := execinfra.NewRepeatableRowSource(types.OneIntCol, rows)
			rightInput := execinfra.NewRepeatableRowSource(types.OneIntCol, rows)
			b.SetBytes(int64(8 * inputSize * numCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := newMergeJoiner(flowCtx, 0 /* processorID */, spec, leftInput, rightInput, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				m.Run(context.Background())
				leftInput.Reset()
				rightInput.Reset()
			}
		})
	}

	for _, inputSize := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		numRepeats := inputSize
		b.Run(fmt.Sprintf("OneSideRepeatInputSize=%d", inputSize), func(b *testing.B) {
			leftInput := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(inputSize, numCols))
			rightInput := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeRepeatedIntRows(numRepeats, inputSize, numCols))
			b.SetBytes(int64(8 * inputSize * numCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := newMergeJoiner(flowCtx, 0 /* processorID */, spec, leftInput, rightInput, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				m.Run(context.Background())
				leftInput.Reset()
				rightInput.Reset()
			}
		})
	}

	for _, inputSize := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		numRepeats := int(math.Sqrt(float64(inputSize)))
		b.Run(fmt.Sprintf("BothSidesRepeatInputSize=%d", inputSize), func(b *testing.B) {
			row := randgen.MakeRepeatedIntRows(numRepeats, inputSize, numCols)
			leftInput := execinfra.NewRepeatableRowSource(types.OneIntCol, row)
			rightInput := execinfra.NewRepeatableRowSource(types.OneIntCol, row)
			b.SetBytes(int64(8 * inputSize * numCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := newMergeJoiner(flowCtx, 0 /* processorID */, spec, leftInput, rightInput, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				m.Run(context.Background())
				leftInput.Reset()
				rightInput.Reset()
			}
		})
	}
}
