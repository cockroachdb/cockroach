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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type mergeJoinerTestCase struct {
	spec          MergeJoinerSpec
	outCols       []uint32
	leftTypes     []sqlbase.ColumnType
	leftInput     sqlbase.EncDatumRows
	rightTypes    []sqlbase.ColumnType
	rightInput    sqlbase.EncDatumRows
	expectedTypes []sqlbase.ColumnType
	expected      sqlbase.EncDatumRows
}

func TestMergeJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: tree.DNull}

	testCases := []mergeJoinerTestCase{
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.InnerJoin,
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
			expectedTypes: threeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.InnerJoin,
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
			expectedTypes: threeIntCols,
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
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   sqlbase.InnerJoin,
				OnExpr: Expression{Expr: "@4 >= 4"},
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
			expectedTypes: threeIntCols,
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   sqlbase.FullOuterJoin,
				OnExpr: Expression{Expr: "@2 >= @4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1, 3},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},

				{v[1], v[5]},

				{v[2], v[0]},
				{v[2], v[8]},

				{v[3], v[5]},

				{v[6], v[0]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[5]},
				{v[0], v[5]},

				{v[1], v[0]},
				{v[1], v[8]},

				{v[2], v[5]},

				{v[3], v[0]},
				{v[3], v[0]},

				{v[5], v[0]},
			},
			expectedTypes: threeIntCols,
			expected: sqlbase.EncDatumRows{
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.LeftOuterJoin,
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
			expectedTypes: threeIntCols,
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.RightOuterJoin,
				// Implicit @1 = @3 constraint.
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
			expectedTypes: threeIntCols,
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.FullOuterJoin,
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
			expectedTypes: threeIntCols,
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
						{ColIdx: 1, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
						{ColIdx: 1, Direction: encoding.Ascending},
					}),
				Type: sqlbase.FullOuterJoin,
			},
			outCols:   []uint32{0, 1, 2, 3},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{null, v[4]},
				{v[0], null},
				{v[0], v[1]},
				{v[2], v[4]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{null, v[4]},
				{v[0], null},
				{v[0], v[1]},
				{v[2], v[4]},
			},
			expectedTypes: []sqlbase.ColumnType{intType, intType, intType, intType},
			expected: sqlbase.EncDatumRows{
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.InnerJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: oneIntCol,
			leftInput: sqlbase.EncDatumRows{
				{null},
				{v[0]},
			},
			rightTypes: oneIntCol,
			rightInput: sqlbase.EncDatumRows{
				{null},
				{v[1]},
			},
			expectedTypes: twoIntCols,
			expected:      sqlbase.EncDatumRows{},
		},
		{
			// Ensure that semi joins doesn't output duplicates from
			// the right side.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.LeftSemiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[2], v[3]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[2], v[2]},
				{v[2], v[2]},
				{v[3], v[3]},
			},
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
			},
		},
		{
			// Ensure that duplicate rows in the left are matched
			// in the output in semi-joins.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.LeftSemiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[2]},
				{v[2], v[3]},
				{v[3], v[4]},
				{v[3], v[5]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[2], v[2]},
				{v[3], v[3]},
			},
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[4]},
				{v[3], v[5]},
			},
		},
		{
			// Ensure that NULL == NULL doesn't match in semi-join.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.LeftSemiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{null, v[2]},
				{v[2], v[3]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{null, v[3]},
				{v[2], v[4]},
				{v[2], v[5]},
			},
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
			},
		},
		{
			// Ensure that OnExprs are satisfied for semi-joins.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   sqlbase.LeftSemiJoin,
				OnExpr: Expression{Expr: "@1 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
				{v[6], v[0]},
				{v[6], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
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
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{v[5], v[0]},
				{v[5], v[1]},
			},
		},
		{
			// Ensure that duplicate rows in the left are matched
			// in the output in anti-joins.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.LeftAntiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
				{v[2], v[3]},
				{v[3], v[4]},
				{v[3], v[5]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[2], v[2]},
				{v[3], v[3]},
			},
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[3]},
			},
		},
		{
			// Ensure that NULL == NULL doesn't match in anti-join.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.LeftAntiJoin,
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{null, v[2]},
				{v[2], v[3]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{null, v[3]},
				{v[2], v[4]},
				{v[2], v[5]},
			},
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{null, v[2]},
			},
		},
		{
			// Ensure that OnExprs are satisfied for semi-joins.
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   sqlbase.LeftAntiJoin,
				OnExpr: Expression{Expr: "@1 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols:   []uint32{0, 1},
			leftTypes: twoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
				{v[6], v[0]},
				{v[6], v[1]},
			},
			rightTypes: twoIntCols,
			rightInput: sqlbase.EncDatumRows{
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
			expectedTypes: twoIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], v[0]},
				{v[1], v[1]},
				{v[6], v[0]},
				{v[6], v[1]},
			},
		},
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
			leftInput := NewRowBuffer(c.leftTypes, c.leftInput, RowBufferArgs{})
			rightInput := NewRowBuffer(c.rightTypes, c.rightInput, RowBufferArgs{})
			out := &RowBuffer{}
			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Ctx:      context.Background(),
				Settings: st,
				EvalCtx:  evalCtx,
			}

			post := PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			m, err := newMergeJoiner(&flowCtx, &ms, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(nil /* wg */)

			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}

			var retRows sqlbase.EncDatumRows
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

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(i)))
	}

	spec := MergeJoinerSpec{
		LeftOrdering: convertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightOrdering: convertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: sqlbase.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	leftTypes := oneIntCol
	rightTypes := oneIntCol

	testCases := []struct {
		typ       sqlbase.JoinType
		leftRows  sqlbase.EncDatumRows
		rightRows sqlbase.EncDatumRows
	}{
		{
			typ: sqlbase.InnerJoin,
			// Implicit @1 = @2 constraint.
			leftRows: sqlbase.EncDatumRows{
				{v[0]},
			},
			rightRows: sqlbase.EncDatumRows{
				{v[0]},
			},
		},
		{
			typ: sqlbase.LeftOuterJoin,
			// Implicit @1 = @2 constraint.
			leftRows: sqlbase.EncDatumRows{
				{v[0]},
			},
			rightRows: sqlbase.EncDatumRows{},
		},
		{
			typ: sqlbase.RightOuterJoin,
			// Implicit @1 = @2 constraint.
			leftRows: sqlbase.EncDatumRows{},
			rightRows: sqlbase.EncDatumRows{
				{v[0]},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.typ.String() /* name */, func(t *testing.T) {
			leftInput := NewRowBuffer(leftTypes, tc.leftRows, RowBufferArgs{})
			rightInput := NewRowBuffer(rightTypes, tc.rightRows, RowBufferArgs{})

			// Create a consumer and close it immediately. The mergeJoiner should find out
			// about this closer the first time it attempts to push a row.
			out := &RowBuffer{}
			out.ConsumerDone()

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Ctx:      context.Background(),
				Settings: st,
				EvalCtx:  evalCtx,
			}
			post := PostProcessSpec{Projection: true, OutputColumns: outCols}
			m, err := newMergeJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(nil /* wg */)

			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}
		})
	}
}

func BenchmarkMergeJoiner(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Ctx:      ctx,
		Settings: st,
		EvalCtx:  evalCtx,
	}

	spec := &MergeJoinerSpec{
		LeftOrdering: convertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightOrdering: convertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: sqlbase.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	post := &PostProcessSpec{}
	disposer := &RowDisposer{}

	const numCols = 1
	for _, inputSize := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("InputSize=%d", inputSize), func(b *testing.B) {
			rows := makeIntRows(inputSize, numCols)
			leftInput := NewRepeatableRowSource(oneIntCol, rows)
			rightInput := NewRepeatableRowSource(oneIntCol, rows)
			b.SetBytes(int64(8 * inputSize * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := newMergeJoiner(flowCtx, spec, leftInput, rightInput, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				m.Run(nil)
				leftInput.Reset()
				rightInput.Reset()
			}
		})
	}
}
