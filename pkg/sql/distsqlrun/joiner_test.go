// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type joinerTestCase struct {
	leftEqCols  []uint32
	rightEqCols []uint32
	joinType    sqlbase.JoinType
	onExpr      distsqlpb.Expression
	outCols     []uint32
	leftTypes   []sqlbase.ColumnType
	leftInput   sqlbase.EncDatumRows
	rightTypes  []sqlbase.ColumnType
	rightInput  sqlbase.EncDatumRows
	expected    sqlbase.EncDatumRows
}

func joinerTestCases() []joinerTestCase {
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: tree.DNull}

	testCases := []joinerTestCase{
		// Originally from HashJoiner tests.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.InnerJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.InnerJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 3},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[4]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.InnerJoin,
			onExpr:      distsqlpb.Expression{Expr: "@4 >= 4"},
			// Implicit AND @1 = @3 constraint.
			outCols:   []uint32{0, 1, 3},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.LeftOuterJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.RightOuterJoin,
			// Implicit @1 = @4 constraint.
			outCols:   []uint32{3, 1, 2},
			leftTypes: threeIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.FullOuterJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[4]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.InnerJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
			expected: sqlbase.EncDatumRows{
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		// Test that left outer joins work with filters as expected.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftOuterJoin,
			onExpr:      distsqlpb.Expression{Expr: "@3 = 9"},
			outCols:     []uint32{0, 1},
			leftTypes:   oneIntCol,
			leftInput: sqlbase.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
				{v[5]},
				{v[6]},
				{v[7]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.RightOuterJoin,
			onExpr:      distsqlpb.Expression{Expr: "@2 > 1"},
			outCols:     []uint32{0, 1},
			leftTypes:   oneIntCol,
			leftInput: sqlbase.EncDatumRows{
				{v[0]},
				{v[1]},
				{v[2]},
			},
			rightTypes: oneIntCol,
			rightInput: sqlbase.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{null, v[1]},
				{v[2], v[2]},
				{null, v[3]},
			},
		},
		// Test that full outer joins work with filters as expected.
		{
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.FullOuterJoin,
			onExpr:      distsqlpb.Expression{Expr: "@2 > 1"},
			outCols:     []uint32{0, 1},
			leftTypes:   oneIntCol,
			leftInput: sqlbase.EncDatumRows{
				{v[0]},
				{v[1]},
				{v[2]},
			},
			rightTypes: oneIntCol,
			rightInput: sqlbase.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.InnerJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
			},
		},

		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{v[1], null, null, null, null},
				{null, v[2], null, null, null},
				{null, null, null, null, null},
			},
		},

		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    sqlbase.RightOuterJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{null, null, v[1], null, v[5]},
				{null, null, null, v[2], v[6]},
				{null, null, null, null, v[7]},
			},
		},

		{
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			joinType:    sqlbase.FullOuterJoin,
			// Implicit @1,@2 = @3,@4 constraint.
			outCols:   []uint32{0, 1, 2, 3, 4},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], null},
				{null, v[2]},
				{null, null},
			},
			rightTypes: threeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[1], null, v[5]},
				{null, v[2], v[6]},
				{null, null, v[7]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[0]},
				{v[2], v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[0]},
				{v[2], v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{null, v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0]},
				{v[0]},
			},
		},
		{
			// Ensure that nulls don't match
			// with nulls for semiJoins
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{null, v[1]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{null, v[1]},
				{v[3], v[5]},
				{v[3], v[4]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0]},
			},
		},
		{
			// Ensure that semi joins respect OnExprs.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			onExpr:      distsqlpb.Expression{Expr: "@1 > 1"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[1]},
				{v[2], v[2]},
			},
		},
		{
			// Ensure that semi joins respect OnExprs on both inputs.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			onExpr:      distsqlpb.Expression{Expr: "@4 > 4 and @2 + @4 = 8"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[2]},
			},
		},
		{
			// Ensure that anti-joins don't produce duplicates when left
			// side is smaller.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[1]},
			},
		},
		{
			// Ensure that anti-joins don't produce duplicates when right
			// side is smaller.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[1], v[1]},
				{v[1], v[2]},
				{v[2], v[1]},
				{v[3], v[4]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[1]},
				{v[1], v[2]},
			},
		},
		{
			// Ensure nulls aren't equal in anti-joins.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[1], v[1]},
				{null, v[2]},
				{v[2], v[1]},
				{v[3], v[4]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{null, v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[1]},
				{null, v[2]},
			},
		},
		{
			// Ensure nulls don't match to anything in anti-joins.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[1], v[1]},
				{null, v[2]},
				{v[2], v[1]},
				{v[3], v[4]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{null, v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[1]},
				{null, v[2]},
			},
		},
		{
			// Ensure anti-joins obey onExpr constraints on columns
			// from both inputs.
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			onExpr:      distsqlpb.Expression{Expr: "(@2 + @4) % 2 = 0"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
				{v[2], v[3]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[2]},
				{v[2], v[1]},
				{v[3], v[3]},
			},
			expected: sqlbase.EncDatumRows{
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
			joinType:    sqlbase.LeftAntiJoin,
			onExpr:      distsqlpb.Expression{Expr: "(@2 + @4) % 2 = 0"},
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
				{v[2], v[3]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[2]},
				{v[2], v[1]},
				{v[3], v[3]},
				{v[4], v[1]},
				{v[4], v[2]},
				{v[4], v[3]},
				{v[4], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[2]},
			},
		},
	}

	return testCases
}

// joinerErrorTestCase specifies a test case where an error is expected.
type joinerErrorTestCase struct {
	description string
	leftEqCols  []uint32
	rightEqCols []uint32
	joinType    sqlbase.JoinType
	onExpr      distsqlpb.Expression
	outCols     []uint32
	leftTypes   []sqlbase.ColumnType
	leftInput   sqlbase.EncDatumRows
	rightTypes  []sqlbase.ColumnType
	rightInput  sqlbase.EncDatumRows
	expectedErr error
}

func joinerErrorTestCases() []joinerErrorTestCase {
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []joinerErrorTestCase{
		// Originally from HashJoiner tests.
		{
			description: "Ensure that columns from the right input cannot be in semi-join output.",
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 2},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[4]},
				{v[2], v[5]},
				{v[2], v[6]},
				{v[3], v[3]},
			},
			expectedErr: errors.Errorf("invalid output column %d (only %d available)", 2, 2),
		},
		{
			description: "Ensure that columns from the right input cannot be in anti-join output.",
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			// Implicit @1 = @3 constraint.
			outCols:   []uint32{0, 1, 2},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[1]},
				{v[2], v[1]},
				{v[2], v[2]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
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
