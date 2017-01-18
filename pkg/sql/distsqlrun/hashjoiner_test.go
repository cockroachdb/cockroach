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
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"sort"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: parser.DNull}

	testCases := []struct {
		spec     HashJoinerSpec
		inputs   []sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_INNER,
				OutputColumns:  []uint32{0, 3, 4},
				// Implicit @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
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
				Type:           JoinType_INNER,
				OutputColumns:  []uint32{0, 1, 3},
				// Implicit @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[1]},
				},
				{
					{v[0], v[4]},
					{v[0], v[1]},
					{v[0], v[0]},
					{v[0], v[5]},
					{v[0], v[4]},
				},
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
				Type:           JoinType_INNER,
				OutputColumns:  []uint32{0, 1, 3},
				Expr:           Expression{Expr: "@4 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[1]},
					{v[1], v[0]},
					{v[1], v[1]},
				},
				{
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
				Type:           JoinType_LEFT_OUTER,
				OutputColumns:  []uint32{0, 3, 4},
				// Implicit @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
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
				Type:           JoinType_RIGHT_OUTER,
				OutputColumns:  []uint32{3, 1, 2},
				// Implicit @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
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
				Type:           JoinType_FULL_OUTER,
				OutputColumns:  []uint32{0, 3, 4},
				// Implicit @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
					{v[5], v[5], v[1]},
				},
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
				Type:           JoinType_INNER,
				OutputColumns:  []uint32{0, 3, 4},
				// Implicit @1 = @3 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
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
				Type:           JoinType_LEFT_OUTER,
				OutputColumns:  []uint32{0, 1},
				Expr:           Expression{Expr: "@2 > 1"},
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0]},
					{v[1]},
					{v[2]},
				},
				{
					{v[1]},
					{v[2]},
					{v[3]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null},
				{v[1], null},
				{v[2], v[2]},
			},
		},
		// Test that right outer joins work with filters as expected.
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_RIGHT_OUTER,
				OutputColumns:  []uint32{0, 1},
				Expr:           Expression{Expr: "@2 > 1"},
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0]},
					{v[1]},
					{v[2]},
				},
				{
					{v[1]},
					{v[2]},
					{v[3]},
				},
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
				Type:           JoinType_FULL_OUTER,
				OutputColumns:  []uint32{0, 1},
				Expr:           Expression{Expr: "@2 > 1"},
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0]},
					{v[1]},
					{v[2]},
				},
				{
					{v[1]},
					{v[2]},
					{v[3]},
				},
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
				Type:           JoinType_INNER,
				OutputColumns:  []uint32{0, 1, 2, 3, 4},
				// Implicit @1,@2 = @3,@4 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
			},
		},

		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           JoinType_LEFT_OUTER,
				OutputColumns:  []uint32{0, 1, 2, 3, 4},
				// Implicit @1,@2 = @3,@4 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
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
				Type:           JoinType_RIGHT_OUTER,
				OutputColumns:  []uint32{0, 1, 2, 3, 4},
				// Implicit @1,@2 = @3,@4 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
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
				Type:           JoinType_FULL_OUTER,
				OutputColumns:  []uint32{0, 1, 2, 3, 4},
				// Implicit @1,@2 = @3,@4 constraint.
			},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
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
	}

	for _, c := range testCases {
		hs := c.spec
		leftInput := NewRowBuffer(nil, c.inputs[0])
		rightInput := NewRowBuffer(nil, c.inputs[1])
		out := &RowBuffer{}
		flowCtx := FlowCtx{Context: context.Background(), evalCtx: &parser.EvalContext{}}

		h, err := newHashJoiner(&flowCtx, &hs, leftInput, rightInput, out)
		if err != nil {
			t.Fatal(err)
		}

		h.Run(nil)

		if out.Err != nil {
			t.Fatal(out.Err)
		}
		if !out.Closed {
			t.Fatalf("output RowReceiver not closed")
		}

		var expected []string
		for _, row := range c.expected {
			expected = append(expected, row.String())
		}
		sort.Strings(expected)
		expStr := strings.Join(expected, "")

		var rets []string
		for {
			row, err := out.NextRow()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}
			rets = append(rets, row.String())
		}
		sort.Strings(rets)
		retStr := strings.Join(rets, "")

		if expStr != retStr {
			t.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
				expStr, retStr)
		}
	}
}
