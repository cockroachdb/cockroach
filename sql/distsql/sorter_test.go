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
			// No specified input ordering and unspecified limit.
			spec: SorterSpec{
				OutputOrdering: convertToSpecOrdering(
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
		}, {
			// No specified input ordering but specified limit.
			spec: SorterSpec{
				Limit: 4,
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[3], v[3], v[0]},
				{v[3], v[4], v[1]},
				{v[1], v[0], v[4]},
				{v[0], v[0], v[0]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
				{v[3], v[2], v[0]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[1], v[0], v[4]},
				{v[3], v[2], v[0]},
				{v[3], v[3], v[0]},
			},
		}, {
			// Specified input ordering but no specified limit.
			spec: SorterSpec{
				InputOrderingInfo: SorterSpec_OrderingInfo{
					Ordering: convertToSpecOrdering(
						sqlbase.ColumnOrdering{
							{ColIdx: 0, Direction: desc},
						}),
				},
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[4], v[4], v[4]},
				{v[3], v[4], v[1]},
				{v[1], v[0], v[5]},
				{v[0], v[1], v[0]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[1], v[0]},
				{v[1], v[0], v[5]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[4]},
			},
		}, {
			// Specified input ordering but no specified limit.
			spec: SorterSpec{
				InputOrderingInfo: SorterSpec_OrderingInfo{
					Ordering: convertToSpecOrdering(
						sqlbase.ColumnOrdering{
							{ColIdx: 0, Direction: asc},
							{ColIdx: 1, Direction: asc},
						}),
				},
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[1], v[2]},
				{v[0], v[1], v[0]},
				{v[1], v[0], v[5]},
				{v[1], v[1], v[5]},
				{v[1], v[1], v[4]},
				{v[3], v[4], v[3]},
				{v[3], v[4], v[2]},
				{v[3], v[5], v[1]},
				{v[4], v[4], v[5]},
				{v[4], v[4], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[1], v[0]},
				{v[0], v[1], v[2]},
				{v[1], v[0], v[5]},
				{v[1], v[1], v[4]},
				{v[1], v[1], v[5]},
				{v[3], v[4], v[2]},
				{v[3], v[4], v[3]},
				{v[3], v[5], v[1]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
			},
		}, {
			// Specified input ordering but no specified limit, larger input
			// column ordering specified.
			spec: SorterSpec{
				InputOrderingInfo: SorterSpec_OrderingInfo{
					Ordering: convertToSpecOrdering(
						sqlbase.ColumnOrdering{
							{ColIdx: 0, Direction: asc},
							{ColIdx: 1, Direction: asc},
							{ColIdx: 2, Direction: asc},
						}),
				},
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
						{ColIdx: 3, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[1], v[2], v[4]},
				{v[0], v[1], v[2], v[3]},
				{v[0], v[1], v[3], v[4]},
				{v[0], v[1], v[3], v[3]},
				{v[0], v[2], v[2], v[4]},
				{v[0], v[2], v[2], v[3]},
				{v[0], v[2], v[3], v[4]},
				{v[0], v[2], v[3], v[3]},
				{v[1], v[1], v[2], v[4]},
				{v[1], v[1], v[2], v[3]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[1], v[2], v[3]},
				{v[0], v[1], v[2], v[4]},
				{v[0], v[1], v[3], v[3]},
				{v[0], v[1], v[3], v[4]},
				{v[0], v[2], v[2], v[3]},
				{v[0], v[2], v[2], v[4]},
				{v[0], v[2], v[3], v[3]},
				{v[0], v[2], v[3], v[4]},
				{v[1], v[1], v[2], v[3]},
				{v[1], v[1], v[2], v[4]},
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

func TestColumnMatching(t *testing.T) {
	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		desired  sqlbase.ColumnOrdering
		existing sqlbase.ColumnOrdering
		unique   bool
		expected int
	}{
		{
			desired: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: desc},
				{ColIdx: 1, Direction: desc},
			},
			existing: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: desc},
			},
			expected: 0,
		}, {
			desired: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: desc},
				{ColIdx: 2, Direction: asc},
			},
			existing: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: desc},
				{ColIdx: 2, Direction: asc},
			},
			expected: 3,
		},
	}

	for _, c := range testCases {
		res := computeOrderingMatch(
			c.desired,
			c.existing,
			c.unique)

		if res != c.expected {
			t.Errorf("expected:%d got:%d",
				c.expected, res)
		}
	}
}
