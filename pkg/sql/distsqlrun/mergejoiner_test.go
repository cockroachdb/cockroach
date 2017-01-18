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
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMergeJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [6]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: parser.DNull}

	testCases := []struct {
		spec     MergeJoinerSpec
		inputs   []sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
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
				Type:          JoinType_INNER,
				OutputColumns: []uint32{0, 3, 4},
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:          JoinType_INNER,
				OutputColumns: []uint32{0, 1, 3},
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
				Type:          JoinType_INNER,
				OutputColumns: []uint32{0, 1, 3},
				Expr:          Expression{Expr: "@4 >= 4"},
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:          JoinType_LEFT_OUTER,
				OutputColumns: []uint32{0, 3, 4},
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:          JoinType_RIGHT_OUTER,
				OutputColumns: []uint32{3, 1, 2},
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
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:          JoinType_FULL_OUTER,
				OutputColumns: []uint32{0, 3, 4},
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
	}

	for _, c := range testCases {
		ms := c.spec
		leftInput := NewRowBuffer(nil, c.inputs[0])
		rightInput := NewRowBuffer(nil, c.inputs[1])
		out := &RowBuffer{}
		flowCtx := FlowCtx{Context: context.Background(), evalCtx: &parser.EvalContext{}}

		m, err := newMergeJoiner(&flowCtx, &ms, leftInput, rightInput, out)
		if err != nil {
			t.Fatal(err)
		}

		m.Run(nil)

		if out.Err != nil {
			t.Fatal(out.Err)
		}
		if !out.Closed {
			t.Fatalf("output RowReceiver not closed")
		}

		var retRows sqlbase.EncDatumRows
		for {
			row, err := out.NextRow()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}
			retRows = append(retRows, row)
		}
		expStr := c.expected.String()
		retStr := retRows.String()
		if expStr != retStr {
			t.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
				expStr, retStr)
		}
	}
}
