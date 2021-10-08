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
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type hashJoinerTestCase struct {
	leftEqCols  []uint32
	rightEqCols []uint32
	joinType    descpb.JoinType
	onExpr      execinfrapb.Expression
	outCols     []uint32
	leftTypes   []*types.T
	leftInput   rowenc.EncDatumRows
	rightTypes  []*types.T
	rightInput  rowenc.EncDatumRows
	expected    rowenc.EncDatumRows
}

func hashJoinerTestCases() []hashJoinerTestCase {
	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	null := rowenc.EncDatum{Datum: tree.DNull}

	testCases := []hashJoinerTestCase{
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.InnerJoin,
			// Implicit @1 = @3 constraint.
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
			expected: rowenc.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.InnerJoin,
			// Implicit @1 = @3 constraint.
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
		// Test that inner joins work with filter expressions.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.InnerJoin,
			onExpr:      execinfrapb.Expression{Expr: "@4 >= 4"},
			// Implicit AND @1 = @3 constraint.
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
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftOuterJoin,
			// Implicit @1 = @3 constraint.
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
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.RightOuterJoin,
			// Implicit @1 = @4 constraint.
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
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.FullOuterJoin,
			// Implicit @1 = @3 constraint.
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
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.InnerJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
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
			expected: rowenc.EncDatumRows{
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		// Test that left outer joins work with filters as expected.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftOuterJoin,
			onExpr:      execinfrapb.Expression{Expr: "@3 = 9"},
			outCols:     []uint32{0, 1},
			leftTypes:   types.OneIntCol,
			leftInput: rowenc.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
				{v[5]},
				{v[6]},
				{v[7]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[2], v[8]},
				{v[3], v[9]},
				{v[4], v[9]},

				// Rows that match v[5].
				{v[5], v[9]},
				{v[5], v[9]},

				// Rows that match v[6] but the ON condition fails.
				{v[6], v[8]},
				{v[6], v[8]},

				// Rows that match v[7], ON condition fails for one.
				{v[7], v[8]},
				{v[7], v[9]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], null},
				{v[2], null},
				{v[3], v[3]},
				{v[5], v[5]},
				{v[5], v[5]},
				{v[6], null},
				{v[7], v[7]},
			},
		},
		// Test that right outer joins work with filters as expected.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.RightOuterJoin,
			onExpr:      execinfrapb.Expression{Expr: "@2 > 1"},
			outCols:     []uint32{0, 1},
			leftTypes:   types.OneIntCol,
			leftInput: rowenc.EncDatumRows{
				{v[0]},
				{v[1]},
				{v[2]},
			},
			rightTypes: types.OneIntCol,
			rightInput: rowenc.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
			},
			expected: rowenc.EncDatumRows{
				{null, v[1]},
				{v[2], v[2]},
				{null, v[3]},
			},
		},
		// Test that full outer joins work with filters as expected.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.FullOuterJoin,
			onExpr:      execinfrapb.Expression{Expr: "@2 > 1"},
			outCols:     []uint32{0, 1},
			leftTypes:   types.OneIntCol,
			leftInput: rowenc.EncDatumRows{
				{v[0]},
				{v[1]},
				{v[2]},
			},
			rightTypes: types.OneIntCol,
			rightInput: rowenc.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[0], null},
				{null, v[1]},
				{v[1], null},
				{v[2], v[2]},
				{null, v[3]},
			},
		},

		// Tests for behavior when input contains NULLs.
		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    descpb.InnerJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
			},
		},

		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    descpb.LeftOuterJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{v[1], null, null, null, null},
				{null, v[2], null, null, null},
				{null, null, null, null, null},
			},
		},

		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    descpb.RightOuterJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{null, null, v[1], null, v[5]},
				{null, null, null, v[2], v[6]},
				{null, null, null, null, v[7]},
			},
		},

		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    descpb.FullOuterJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: types.ThreeIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{null, null, v[1], null, v[5]},
				{null, null, null, v[2], v[6]},
				{null, null, null, null, v[7]},
				{v[1], null, null, null, null},
				{null, v[2], null, null, null},
				{null, null, null, null, null},
			},
		},
		{
			// Ensure semi join doesn't emit extra rows when
			// there are multiple matching rows in the
			// rightInput and the rightInput is smaller.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[0]},
				{v[2], v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{v[0]},
				{v[2]},
				{v[2]},
			},
		},
		{
			// Ensure semi join doesn't emit extra rows when
			// there are multiple matching rows in the
			// rightInput and the leftInput is smaller
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[0]},
				{v[2], v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[0]},
				{v[0]},
				{v[2]},
			},
		},
		{
			// Ensure nulls don't match with any value
			// for semi joins.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{null, v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[0]},
				{v[0]},
			},
		},
		{
			// Ensure that nulls don't match
			// with nulls for semiJoins
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{null, v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{null, v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[0]},
			},
		},
		{
			// Ensure that semi joins respect OnExprs.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			onExpr:      execinfrapb.Expression{Expr: "@1 > 1"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[1]},
				{v[2], v[2]},
			},
		},
		{
			// Ensure that semi joins respect OnExprs on both inputs.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			onExpr:      execinfrapb.Expression{Expr: "@4 > 4 and @2 + @4 = 8"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[2]},
			},
		},
		{
			// Ensure that anti-joins don't produce duplicates when left
			// side is smaller.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[1]},
			},
		},
		{
			// Ensure that anti-joins don't produce duplicates when right
			// side is smaller.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[1], v[1]},
				{v[1], v[2]},
				{v[2], v[1]},
				{v[3], v[4]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[1]},
				{v[1], v[2]},
			},
		},
		{
			// Ensure nulls aren't equal in anti-joins.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[1], v[1]},
				{null, v[2]},
				{v[2], v[1]},
				{v[3], v[4]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{null, v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[1]},
				{null, v[2]},
			},
		},
		{
			// Ensure nulls don't match to anything in anti-joins.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[1], v[1]},
				{null, v[2]},
				{v[2], v[1]},
				{v[3], v[4]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{null, v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[1]},
				{null, v[2]},
			},
		},
		{
			// Ensure anti-joins obey onExpr constraints on columns
			// from both inputs.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			onExpr:      execinfrapb.Expression{Expr: "(@2 + @4) % 2 = 0"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
				{v[2], v[3]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[2]},
				{v[2], v[1]},
				{v[3], v[3]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
			},
		},
		{
			// Ensure anti-joins obey onExpr constraints on columns
			// from both inputs when left input is smaller.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			onExpr:      execinfrapb.Expression{Expr: "(@2 + @4) % 2 = 0"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
				{v[2], v[3]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[2]},
				{v[2], v[1]},
				{v[3], v[3]},
				{v[4], v[1]},
				{v[4], v[2]},
				{v[4], v[3]},
				{v[4], v[4]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
			},
		},
	}

	return testCases
}

// hashJoinerErrorTestCase specifies a test case where an error is expected.
type hashJoinerErrorTestCase struct {
	description string
	leftEqCols  []uint32
	rightEqCols []uint32
	joinType    descpb.JoinType
	onExpr      execinfrapb.Expression
	outCols     []uint32
	leftTypes   []*types.T
	leftInput   rowenc.EncDatumRows
	rightTypes  []*types.T
	rightInput  rowenc.EncDatumRows
	expectedErr error
}

func hashJoinerErrorTestCases() []hashJoinerErrorTestCase {
	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []hashJoinerErrorTestCase{
		{
			description: "Ensure that columns from the right input cannot be in left semi-join output.",
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 2},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expectedErr: errors.Errorf("invalid output column %d (only %d available)", 2, 2),
		},
		{
			description: "Ensure that columns from the right input cannot be in left anti-join output.",
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 2},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expectedErr: errors.Errorf("invalid output column %d (only %d available)", 2, 2),
		},
		{
			description: "Ensure that columns from the left input cannot be in right semi-join output.",
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.RightSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 2},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expectedErr: errors.Errorf("invalid output column %d (only %d available)", 2, 2),
		},
		{
			description: "Ensure that columns from the left input cannot be in right anti-join output.",
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    descpb.RightAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 2},
			leftTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: types.TwoIntCols,
			rightInput: rowenc.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expectedErr: errors.Errorf("invalid output column %d (only %d available)", 2, 2),
		},
	}
	return testCases
}

// mirrorJoinTypeAndOnExpr returns a "mirror" of the join type and the ON
// expression. Only left semi/anti joins are expected.
func mirrorJoinTypeAndOnExpr(
	t descpb.JoinType, onExpr execinfrapb.Expression,
) (descpb.JoinType, execinfrapb.Expression) {
	switch t {
	case descpb.LeftSemiJoin, descpb.LeftAntiJoin:
	default:
		panic(errors.AssertionFailedf("only left semi/anti joins can be mirrored, %s requested", t))
	}
	var mirrorJoinType descpb.JoinType
	if t == descpb.LeftSemiJoin {
		mirrorJoinType = descpb.RightSemiJoin
	} else {
		mirrorJoinType = descpb.RightAntiJoin
	}
	var mirrorOnExpr execinfrapb.Expression
	if onExpr.Expr != "" {
		// We update the ON expression by switching the index variables
		// (namely, if we have both inputs with two columns, we need to do the
		// following replacement: @1 -> @3, @2 -> @4, @3 -> @1, and @4 -> @2).
		onExpr := []byte(onExpr.Expr)
		// Make a copy of the original.
		mirroringOnExpr := []byte(string(onExpr))
		for i := range onExpr {
			if onExpr[i] == '@' {
				oldIdxVar := onExpr[i+1] - '0'
				newIdxVar := (oldIdxVar+1)%4 + 1
				mirroringOnExpr[i+1] = newIdxVar + '0'
			}
		}
		mirrorOnExpr.Expr = string(mirroringOnExpr)
	}
	return mirrorJoinType, mirrorOnExpr
}

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := hashJoinerTestCases()

	for _, c := range testCases {
		if c.joinType == descpb.LeftSemiJoin || c.joinType == descpb.LeftAntiJoin {
			// For every left semi and left anti join, we will automatically
			// populate a "mirroring" test case with right semi or right anti
			// join - all we need to do is to switch the inputs and change the
			// join type accordingly.
			mirroringCase := c
			mirroringCase.joinType, mirroringCase.onExpr = mirrorJoinTypeAndOnExpr(c.joinType, c.onExpr)
			mirroringCase.leftEqCols, mirroringCase.rightEqCols = mirroringCase.rightEqCols, mirroringCase.leftEqCols
			mirroringCase.leftTypes, mirroringCase.rightTypes = mirroringCase.rightTypes, mirroringCase.leftTypes
			mirroringCase.leftInput, mirroringCase.rightInput = mirroringCase.rightInput, mirroringCase.leftInput
			testCases = append(testCases, mirroringCase)
		}
	}

	// Add INTERSECT ALL cases with HashJoinerSpecs.
	for _, tc := range intersectAllTestCases() {
		testCases = append(testCases, setOpTestCaseToHashJoinerTestCase(tc))
	}

	// Add EXCEPT ALL cases with HashJoinerSpecs.
	for _, tc := range exceptAllTestCases() {
		testCases = append(testCases, setOpTestCaseToHashJoinerTestCase(tc))
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case.
		// flowCtxSetup can optionally be provided to set up additional testing
		// knobs in the flowCtx before instantiating a hashJoiner and hjSetup can
		// optionally be provided to modify the hashJoiner after instantiation but
		// before Run().
		testFunc := func(t *testing.T, flowCtxSetup func(f *execinfra.FlowCtx), hjSetup func(h *hashJoiner)) error {
			for i := 0; i < 2; i++ {
				leftInput := distsqlutils.NewRowBuffer(c.leftTypes, c.leftInput, distsqlutils.RowBufferArgs{})
				rightInput := distsqlutils.NewRowBuffer(c.rightTypes, c.rightInput, distsqlutils.RowBufferArgs{})
				out := &distsqlutils.RowBuffer{}
				flowCtx := execinfra.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg: &execinfra.ServerConfig{
						Settings:    st,
						TempStorage: tempEngine,
					},
					DiskMonitor: diskMonitor,
				}
				if flowCtxSetup != nil {
					flowCtxSetup(&flowCtx)
				}
				post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
				spec := &execinfrapb.HashJoinerSpec{
					LeftEqColumns:  c.leftEqCols,
					RightEqColumns: c.rightEqCols,
					Type:           c.joinType,
					OnExpr:         c.onExpr,
				}
				h, err := newHashJoiner(
					&flowCtx, 0 /* processorID */, spec, leftInput, rightInput, &post, out,
				)
				if err != nil {
					return err
				}
				outTypes := h.OutputTypes()
				if hjSetup != nil {
					hjSetup(h)
				}
				h.Run(ctx)

				if !out.ProducerClosed() {
					return errors.New("output RowReceiver not closed")
				}

				if err := checkExpectedRows(outTypes, c.expected, out); err != nil {
					return errors.Wrapf(err, "join type %s", c.joinType)
				}
			}
			return nil
		}

		// Run test with a variety of memory limits.
		for _, memLimit := range []int64{1, 256, 512, 1024, 2048} {
			t.Run(fmt.Sprintf("MemLimit=%d", memLimit), func(t *testing.T) {
				if err := testFunc(t, func(f *execinfra.FlowCtx) {
					f.Cfg.TestingKnobs.MemoryLimitBytes = memLimit
				}, nil); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestHashJoinerError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := hashJoinerErrorTestCases()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case after running the provided setup function.
		testFunc := func(t *testing.T) error {
			leftInput := distsqlutils.NewRowBuffer(c.leftTypes, c.leftInput, distsqlutils.RowBufferArgs{})
			rightInput := distsqlutils.NewRowBuffer(c.rightTypes, c.rightInput, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg: &execinfra.ServerConfig{
					Settings:    st,
					TempStorage: tempEngine,
				},
				DiskMonitor: diskMonitor,
			}

			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			spec := &execinfrapb.HashJoinerSpec{
				LeftEqColumns:  c.leftEqCols,
				RightEqColumns: c.rightEqCols,
				Type:           c.joinType,
				OnExpr:         c.onExpr,
			}
			h, err := newHashJoiner(
				&flowCtx, 0 /* processorID */, spec, leftInput, rightInput, &post, out,
			)
			if err != nil {
				return err
			}
			outTypes := h.OutputTypes()
			h.Run(ctx)

			if !out.ProducerClosed() {
				return errors.New("output RowReceiver not closed")
			}

			return checkExpectedRows(outTypes, nil, out)
		}

		t.Run(c.description, func(t *testing.T) {
			if err := testFunc(t); err == nil {
				t.Errorf("Expected an error:%s, but found nil", c.expectedErr)
			} else if err.Error() != c.expectedErr.Error() {
				t.Errorf("HashJoinerErrorTest: expected\n%s, but found\n%v", c.expectedErr, err)
			}
		})
	}
}

func checkExpectedRows(
	types []*types.T, expectedRows rowenc.EncDatumRows, results *distsqlutils.RowBuffer,
) error {
	var expected []string
	for _, row := range expectedRows {
		expected = append(expected, row.String(types))
	}
	sort.Strings(expected)
	expStr := strings.Join(expected, "")

	var rets []string
	for {
		row, meta := results.Next()
		if meta != nil {
			return errors.Errorf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		rets = append(rets, row.String(types))
	}
	sort.Strings(rets)
	retStr := strings.Join(rets, "")

	if expStr != retStr {
		return errors.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
			expStr, retStr)
	}
	return nil
}

// TestHashJoinerDrain tests that, if the consumer starts draining, the
// hashJoiner informs the producers and drains them.
//
// Concretely, the HashJoiner is set up to read the right input fully before
// starting to produce rows, so only the left input will be asked to drain if
// the consumer is draining.
func TestHashJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	spec := execinfrapb.HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           descpb.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	inputs := []rowenc.EncDatumRows{
		{
			{v[0]},
			{v[1]},
		},
		{
			{v[0]},
			{v[1]},
		},
	}
	expected := rowenc.EncDatumRows{
		{v[0]},
	}
	leftInputDrainNotification := make(chan error, 1)
	leftInputConsumerDone := func(rb *distsqlutils.RowBuffer) {
		// Check that draining occurs before the left input has been consumed,
		// not at the end.
		// The left input started with 2 rows and 1 was consumed to find out
		// that we need to drain. So we expect 1 to be left.
		rb.Mu.Lock()
		defer rb.Mu.Unlock()
		if len(rb.Mu.Records) != 1 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 1 row left, got: %d", len(rb.Mu.Records))
			return
		}
		leftInputDrainNotification <- nil
	}
	leftInput := distsqlutils.NewRowBuffer(
		types.OneIntCol,
		inputs[0],
		distsqlutils.RowBufferArgs{OnConsumerDone: leftInputConsumerDone},
	)
	rightInput := distsqlutils.NewRowBuffer(types.OneIntCol, inputs[1], distsqlutils.RowBufferArgs{})
	out := distsqlutils.NewRowBuffer(
		types.OneIntCol,
		nil, /* rows */
		distsqlutils.RowBufferArgs{AccumulateRowsWhileDraining: true},
	)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
		},
		DiskMonitor: diskMonitor,
		EvalCtx:     &evalCtx,
	}

	post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(
		&flowCtx, 0 /* processorID */, &spec, leftInput, rightInput, &post, out,
	)
	if err != nil {
		t.Fatal(err)
	}

	out.ConsumerDone()
	h.Run(ctx)

	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.Mu.Lock()
	defer leftInput.Mu.Unlock()
	if len(leftInput.Mu.Records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.Mu.Records))
	}

	if err := checkExpectedRows(types.OneIntCol, expected, out); err != nil {
		t.Fatal(err)
	}
}

// TestHashJoinerDrainAfterBuildPhaseError tests that, if the HashJoiner
// encounters an error in the "build phase" (reading of the right input), the
// joiner will drain both inputs.
func TestHashJoinerDrainAfterBuildPhaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	spec := execinfrapb.HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           descpb.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	inputs := []rowenc.EncDatumRows{
		{
			{v[0]},
			{v[1]},
		},
		{
			{v[0]},
			{v[1]},
		},
	}
	leftInputDrainNotification := make(chan error, 1)
	leftInputConsumerDone := func(rb *distsqlutils.RowBuffer) {
		// Check that draining occurs before the left input has been consumed, not
		// at the end.
		rb.Mu.Lock()
		defer rb.Mu.Unlock()
		if len(rb.Mu.Records) != 2 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the left input, got: %d", len(rb.Mu.Records))
			return
		}
		leftInputDrainNotification <- nil
	}
	rightInputDrainNotification := make(chan error, 1)
	rightInputConsumerDone := func(rb *distsqlutils.RowBuffer) {
		// Check that draining occurs before the right input has been consumed, not
		// at the end.
		rb.Mu.Lock()
		defer rb.Mu.Unlock()
		if len(rb.Mu.Records) != 2 {
			rightInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the right input, got: %d", len(rb.Mu.Records))
			return
		}
		rightInputDrainNotification <- nil
	}
	rightErrorReturned := false
	rightInputNext := func(rb *distsqlutils.RowBuffer) (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
		if !rightErrorReturned {
			rightErrorReturned = true
			// The right input is going to return an error as the first thing.
			return nil, &execinfrapb.ProducerMetadata{Err: errors.Errorf("Test error. Please drain.")}
		}
		// Let RowBuffer.Next() do its usual thing.
		return nil, nil
	}
	leftInput := distsqlutils.NewRowBuffer(
		types.OneIntCol,
		inputs[0],
		distsqlutils.RowBufferArgs{OnConsumerDone: leftInputConsumerDone},
	)
	rightInput := distsqlutils.NewRowBuffer(
		types.OneIntCol,
		inputs[1],
		distsqlutils.RowBufferArgs{
			OnConsumerDone: rightInputConsumerDone,
			OnNext:         rightInputNext,
		},
	)
	out := distsqlutils.NewRowBuffer(
		types.OneIntCol,
		nil, /* rows */
		distsqlutils.RowBufferArgs{},
	)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
		},
		DiskMonitor: diskMonitor,
		EvalCtx:     &evalCtx,
	}

	post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(
		&flowCtx, 0 /* processorID */, &spec, leftInput, rightInput, &post, out,
	)
	if err != nil {
		t.Fatal(err)
	}

	h.Run(ctx)

	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.Mu.Lock()
	defer leftInput.Mu.Unlock()
	if len(leftInput.Mu.Records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.Mu.Records))
	}

	out.Mu.Lock()
	defer out.Mu.Unlock()
	if len(out.Mu.Records) != 1 {
		t.Fatalf("expected 1 record, got: %d", len(out.Mu.Records))
	}
	if !testutils.IsError(out.Mu.Records[0].Meta.Err, "Test error. Please drain.") {
		t.Fatalf("expected %q, got: %v", "Test error", out.Mu.Records[0].Meta.Err)
	}
}

// BenchmarkHashJoiner times how long it takes to join two tables of the same
// variable size. There is a 1:1 relationship between the rows of each table.
// TODO(asubiotto): More complex benchmarks.
func BenchmarkHashJoiner(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: diskMonitor,
	}
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()
	flowCtx.Cfg.TempStorage = tempEngine

	spec := &execinfrapb.HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           descpb.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	post := &execinfrapb.PostProcessSpec{}

	const numCols = 1
	for _, spill := range []bool{true, false} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spill
		b.Run(fmt.Sprintf("spill=%t", spill), func(b *testing.B) {
			for _, numRows := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
				if spill && numRows < 1<<8 {
					// The benchmark takes a long time with a small number of rows and
					// spilling, since the times change wildly. Disable for now.
					continue
				}
				b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
					rows := randgen.MakeIntRows(numRows, numCols)
					leftInput := execinfra.NewRepeatableRowSource(types.OneIntCol, rows)
					rightInput := execinfra.NewRepeatableRowSource(types.OneIntCol, rows)
					b.SetBytes(int64(8 * numRows * numCols * 2))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						// TODO(asubiotto): Get rid of uncleared state between
						// hashJoiner Run()s to omit instantiation time from benchmarks.
						h, err := newHashJoiner(
							flowCtx, 0 /* processorID */, spec, leftInput, rightInput, post, &rowDisposer{},
						)
						if err != nil {
							b.Fatal(err)
						}
						h.Run(ctx)
						leftInput.Reset()
						rightInput.Reset()
					}
				})
			}
		})
	}
}
