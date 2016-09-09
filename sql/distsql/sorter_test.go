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

package distsql

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"

	"golang.org/x/net/context"
)

func TestSorter(t *testing.T) {
	v := [6]sqlbase.EncDatum{}
	for i := range v {
		v[i].SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		spec     SorterSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: SorterSpec{
				Ordering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[1], v[4]},
				{v[0], v[1], v[2]},
				{v[0], v[2], v[3]},
				{v[1], v[1], v[3]},
				{v[1], v[0], v[4]},
				{v[0], v[0], v[0]},
				{v[4], v[4], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[2]},
				{v[0], v[2], v[3]},
				{v[1], v[0], v[4]},
				{v[1], v[1], v[3]},
				{v[4], v[4], v[4]},
			},
		}, {
			spec: SorterSpec{
				Ordering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: desc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[4]},
				{v[3], v[2], v[0]},
				{v[4], v[4], v[5]},
				{v[3], v[3], v[0]},
				{v[0], v[0], v[0]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[3], v[3], v[0]},
				{v[3], v[2], v[0]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
			},
		},
	}

	for _, c := range testCases {
		ss := c.spec
		in := &RowBuffer{rows: c.input}
		out := &RowBuffer{}
		flowCtx := FlowCtx{Context: context.Background()}

		s := newSorter(&flowCtx, &ss, in, out)
		s.Run(nil)

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
