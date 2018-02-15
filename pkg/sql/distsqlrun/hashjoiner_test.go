// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	math "math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/pkg/errors"
)

type hashJoinerTestCase struct {
	spec       HashJoinerSpec
	outCols    []uint32
	leftTypes  []sqlbase.ColumnType
	leftInput  sqlbase.EncDatumRows
	rightTypes []sqlbase.ColumnType
	rightInput sqlbase.EncDatumRows
	expected   sqlbase.EncDatumRows
}

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: tree.DNull}

	testCases := []hashJoinerTestCase{
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.InnerJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.InnerJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.InnerJoin,
				OnExpr:         Expression{Expr: "@4 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftOuterJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.RightOuterJoin,
				// Implicit @1 = @4 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.FullOuterJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.InnerJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftOuterJoin,
				OnExpr:         Expression{Expr: "@3 = 9"},
			},
			outCols:   []uint32{0, 1},
			leftTypes: oneIntCol,
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.RightOuterJoin,
				OnExpr:         Expression{Expr: "@2 > 1"},
			},
			outCols:   []uint32{0, 1},
			leftTypes: oneIntCol,
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.FullOuterJoin,
				OnExpr:         Expression{Expr: "@2 > 1"},
			},
			outCols:   []uint32{0, 1},
			leftTypes: oneIntCol,
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           sqlbase.InnerJoin,
				// Implicit @1,@2 = @3,@4 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           sqlbase.LeftOuterJoin,
				// Implicit @1,@2 = @3,@4 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           sqlbase.RightOuterJoin,
				// Implicit @1,@2 = @3,@4 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           sqlbase.FullOuterJoin,
				// Implicit @1,@2 = @3,@4 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				OnExpr:         Expression{Expr: "@1 > 1"},
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				OnExpr:         Expression{Expr: "@4 > 4 and @2 + @4 = 8"},
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			// Ensure anti-joins obey OnExpr constraints on columns
			// from both inputs.
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				OnExpr:         Expression{Expr: "(@2 + @4) % 2 = 0"},
				// Implicit @1 = @3 constraint.
			},
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
			// Ensure anti-joins obey OnExpr constraints on columns
			// from both inputs when left input is smaller.
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				OnExpr:         Expression{Expr: "(@2 + @4) % 2 = 0"},
				// Implicit @1 = @3 constraint.
			},
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
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st))
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case after running the provided setup function.
		testFunc := func(t *testing.T, setup func(h *hashJoiner)) error {
			for _, side := range [2]joinSide{leftSide, rightSide} {
				leftInput := NewRowBuffer(c.leftTypes, c.leftInput, RowBufferArgs{})
				rightInput := NewRowBuffer(c.rightTypes, c.rightInput, RowBufferArgs{})
				out := &RowBuffer{}
				flowCtx := FlowCtx{
					Ctx:         ctx,
					Settings:    st,
					EvalCtx:     evalCtx,
					TempStorage: tempEngine,
					diskMonitor: &diskMonitor,
				}
				post := PostProcessSpec{Projection: true, OutputColumns: c.outCols}
				h, err := newHashJoiner(&flowCtx, &c.spec, leftInput, rightInput, &post, out)
				if err != nil {
					return err
				}
				outTypes := h.OutputTypes()
				setup(h)
				h.forcedStoredSide = &side
				h.Run(nil)

				if !out.ProducerClosed {
					return errors.New("output RowReceiver not closed")
				}

				if err := checkExpectedRows(outTypes, c.expected, out); err != nil {
					return err
				}
			}
			return nil
		}

		// Run test with a variety of initial buffer sizes.
		for _, initialBuffer := range []int64{0, 32, 64, 128, 1024 * 1024} {
			t.Run(fmt.Sprintf("InitialBuffer=%d", initialBuffer), func(t *testing.T) {
				if err := testFunc(t, func(h *hashJoiner) {
					h.initialBufferSize = initialBuffer
				}); err != nil {
					t.Fatal(err)
				}
			})
		}

		// Run tests with a probability of the run failing with a memory error.
		// These verify that the hashJoiner falls back to disk correctly in all
		// cases.
		for _, memFailPoint := range []hashJoinPhase{buffer, build} {
			for i := 0; i < 5; i++ {
				t.Run(fmt.Sprintf("MemFailPoint=%s", memFailPoint), func(t *testing.T) {
					if err := testFunc(t, func(h *hashJoiner) {
						h.testingKnobMemFailPoint = memFailPoint
						h.testingKnobFailProbability = 0.5
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		}

		// Run test with a variety of memory limits.
		for _, memLimit := range []int64{1, 256, 512, 1024, 2048} {
			t.Run(fmt.Sprintf("MemLimit=%d", memLimit), func(t *testing.T) {
				if err := testFunc(t, func(h *hashJoiner) {
					h.flowCtx.testingKnobs.MemoryLimitBytes = memLimit
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestHashJoinerError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		description string
		spec        HashJoinerSpec
		outCols     []uint32
		leftTypes   []sqlbase.ColumnType
		leftInput   sqlbase.EncDatumRows
		rightTypes  []sqlbase.ColumnType
		rightInput  sqlbase.EncDatumRows
		expectedErr error
	}{
		{
			description: "Ensure that columns from the right input cannot be in semi-join output.",
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftSemiJoin,
				// Implicit @1 = @3 constraint.
			},
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
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           sqlbase.LeftAntiJoin,
				// Implicit @1 = @3 constraint.
			},
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

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st))
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case after running the provided setup function.
		testFunc := func(t *testing.T, setup func(h *hashJoiner)) error {
			leftInput := NewRowBuffer(c.leftTypes, c.leftInput, RowBufferArgs{})
			rightInput := NewRowBuffer(c.rightTypes, c.rightInput, RowBufferArgs{})
			out := &RowBuffer{}
			flowCtx := FlowCtx{
				Ctx:         ctx,
				Settings:    st,
				EvalCtx:     evalCtx,
				TempStorage: tempEngine,
				diskMonitor: &diskMonitor,
			}

			post := PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			h, err := newHashJoiner(&flowCtx, &c.spec, leftInput, rightInput, &post, out)
			if err != nil {
				return err
			}
			outTypes := h.OutputTypes()
			setup(h)
			h.Run(nil)

			if !out.ProducerClosed {
				return errors.New("output RowReceiver not closed")
			}

			return checkExpectedRows(outTypes, nil, out)
		}

		t.Run(c.description, func(t *testing.T) {
			if err := testFunc(t, func(h *hashJoiner) {
				h.initialBufferSize = 1024 * 32
			}); err == nil {
				t.Errorf("Expected an error:%s, but found nil", c.expectedErr)
			} else if err.Error() != c.expectedErr.Error() {
				t.Errorf("HashJoinerErrorTest: expected\n%s, but found\n%v", c.expectedErr, err)
			}
		})
	}
}

func checkExpectedRows(
	types []sqlbase.ColumnType, expectedRows sqlbase.EncDatumRows, results *RowBuffer,
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

// TestDrain tests that, if the consumer starts draining, the hashJoiner informs
// the producers and drains them.
//
// Concretely, the HashJoiner is set up to read the right input fully before
// starting to produce rows, so only the left input will be asked to drain if
// the consumer is draining.
func TestHashJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}
	spec := HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           sqlbase.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	inputs := []sqlbase.EncDatumRows{
		{
			{v[0]},
			{v[1]},
		},
		{
			{v[0]},
			{v[1]},
		},
	}
	expected := sqlbase.EncDatumRows{
		{v[0]},
	}
	leftInputDrainNotification := make(chan error, 1)
	leftInputConsumerDone := func(rb *RowBuffer) {
		// Check that draining occurs before the left input has been consumed,
		// not at the end.
		// The left input started with 2 rows and 1 was consumed to find out
		// that we need to drain. So we expect 1 to be left.
		rb.mu.Lock()
		defer rb.mu.Unlock()
		if len(rb.mu.records) != 1 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 1 row left, got: %d", len(rb.mu.records))
			return
		}
		leftInputDrainNotification <- nil
	}
	leftInput := NewRowBuffer(
		oneIntCol,
		inputs[0],
		RowBufferArgs{OnConsumerDone: leftInputConsumerDone},
	)
	rightInput := NewRowBuffer(oneIntCol, inputs[1], RowBufferArgs{})
	out := NewRowBuffer(
		oneIntCol,
		nil, /* rows */
		RowBufferArgs{AccumulateRowsWhileDraining: true},
	)

	// Since the use of external storage overrides h.initialBufferSize, disable
	// it for this test.
	settings := cluster.MakeTestingClusterSettings()
	settingUseTempStorageJoins.Override(&settings.SV, false)

	evalCtx := tree.MakeTestingEvalContext(settings)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	flowCtx := FlowCtx{
		Ctx:      ctx,
		Settings: settings,
		EvalCtx:  evalCtx,
	}

	post := PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
	if err != nil {
		t.Fatal(err)
	}
	// Disable initial buffering. We always store the right stream in this case.
	// If not disabled, both streams will be fully consumed before outputting
	// any rows.
	h.initialBufferSize = 0

	out.ConsumerDone()
	h.Run(nil)

	if !out.ProducerClosed {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.mu.Lock()
	defer leftInput.mu.Unlock()
	if len(leftInput.mu.records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.mu.records))
	}

	if err := checkExpectedRows(oneIntCol, expected, out); err != nil {
		t.Fatal(err)
	}
}

// TestHashJoinerDrainAfterBuildPhaseError tests that, if the HashJoiner
// encounters an error in the "build phase" (reading of the right input), the
// joiner will drain both inputs.
func TestHashJoinerDrainAfterBuildPhaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}
	spec := HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           sqlbase.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	inputs := []sqlbase.EncDatumRows{
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
	leftInputConsumerDone := func(rb *RowBuffer) {
		// Check that draining occurs before the left input has been consumed, not
		// at the end.
		rb.mu.Lock()
		defer rb.mu.Unlock()
		if len(rb.mu.records) != 2 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the left input, got: %d", len(rb.mu.records))
			return
		}
		leftInputDrainNotification <- nil
	}
	rightInputDrainNotification := make(chan error, 1)
	rightInputConsumerDone := func(rb *RowBuffer) {
		// Check that draining occurs before the right input has been consumed, not
		// at the end.
		rb.mu.Lock()
		defer rb.mu.Unlock()
		if len(rb.mu.records) != 2 {
			rightInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the right input, got: %d", len(rb.mu.records))
			return
		}
		rightInputDrainNotification <- nil
	}
	rightErrorReturned := false
	rightInputNext := func(rb *RowBuffer) (sqlbase.EncDatumRow, *ProducerMetadata) {
		if !rightErrorReturned {
			rightErrorReturned = true
			// The right input is going to return an error as the first thing.
			return nil, &ProducerMetadata{Err: errors.Errorf("Test error. Please drain.")}
		}
		// Let RowBuffer.Next() do its usual thing.
		return nil, nil
	}
	leftInput := NewRowBuffer(
		oneIntCol,
		inputs[0],
		RowBufferArgs{OnConsumerDone: leftInputConsumerDone},
	)
	rightInput := NewRowBuffer(
		oneIntCol,
		inputs[1],
		RowBufferArgs{
			OnConsumerDone: rightInputConsumerDone,
			OnNext:         rightInputNext,
		},
	)
	out := NewRowBuffer(
		oneIntCol,
		nil, /* rows */
		RowBufferArgs{},
	)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Ctx:      context.Background(),
		Settings: st,
		EvalCtx:  evalCtx,
	}

	post := PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
	if err != nil {
		t.Fatal(err)
	}
	// Disable initial buffering. We always store the right stream in this case.
	h.initialBufferSize = 0

	h.Run(nil)

	if !out.ProducerClosed {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.mu.Lock()
	defer leftInput.mu.Unlock()
	if len(leftInput.mu.records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.mu.records))
	}

	out.mu.Lock()
	defer out.mu.Unlock()
	if len(out.mu.records) != 1 {
		t.Fatalf("expected 1 record, got: %d", len(out.mu.records))
	}
	if !testutils.IsError(out.mu.records[0].Meta.Err, "Test error. Please drain.") {
		t.Fatalf("expected %q, got: %v", "Test error", out.mu.records[0].Meta.Err)
	}
}

// BenchmarkHashJoiner times how long it takes to join two tables of the same
// variable size. There is a 1:1 relationship between the rows of each table.
// TODO(asubiotto): More complex benchmarks.
func BenchmarkHashJoiner(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Ctx:      ctx,
		Settings: st,
		EvalCtx:  evalCtx,
	}

	spec := &HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           sqlbase.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	post := &PostProcessSpec{}

	const numCols = 1
	for _, numRows := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			rows := makeIntRows(numRows, numCols)
			leftInput := NewRepeatableRowSource(oneIntCol, rows)
			rightInput := NewRepeatableRowSource(oneIntCol, rows)
			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// TODO(asubiotto): Get rid of uncleared state between
				// hashJoiner Run()s to omit instantiation time from benchmarks.
				h, err := newHashJoiner(flowCtx, spec, leftInput, rightInput, post, &RowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				h.Run(nil)
				leftInput.Reset()
				rightInput.Reset()
			}
		})
	}
}
