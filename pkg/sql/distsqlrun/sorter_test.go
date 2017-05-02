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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"

	"golang.org/x/net/context"
)

func TestSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [6]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		spec     SorterSpec
		post     PostProcessSpec
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
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			post: PostProcessSpec{Limit: 4},
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
			// Specified match ordering length but no specified limit.
			spec: SorterSpec{
				OrderingMatchLen: 2,
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
			// Specified input ordering but no specified limit.
			spec: SorterSpec{
				OrderingMatchLen: 2,
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
						{ColIdx: 3, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[1], v[2], v[5]},
				{v[0], v[1], v[2], v[4]},
				{v[0], v[1], v[2], v[3]},
				{v[1], v[1], v[2], v[2]},
				{v[1], v[2], v[2], v[5]},
				{v[0], v[2], v[2], v[4]},
				{v[0], v[2], v[2], v[3]},
				{v[1], v[2], v[2], v[2]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[1], v[2], v[2]},
				{v[0], v[1], v[2], v[3]},
				{v[0], v[1], v[2], v[4]},
				{v[1], v[1], v[2], v[5]},
				{v[1], v[2], v[2], v[2]},
				{v[0], v[2], v[2], v[3]},
				{v[0], v[2], v[2], v[4]},
				{v[1], v[2], v[2], v[5]},
			},
		},
	}

	monitor := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, math.MaxInt64)
	defer monitor.Stop(context.Background())
	for _, c := range testCases {
		ss := c.spec
		types := make([]sqlbase.ColumnType, len(c.input[0]))
		for i := range types {
			types[i] = columnTypeInt
		}
		in := NewRowBuffer(types, c.input, RowBufferArgs{})
		out := &RowBuffer{}
		flowCtx := FlowCtx{
			evalCtx: parser.EvalContext{Mon: &monitor},
		}

		s, err := newSorter(&flowCtx, &ss, in, &c.post, out)
		if err != nil {
			t.Fatal(err)
		}
		s.Run(context.Background(), nil)
		if !out.ProducerClosed {
			t.Fatalf("output RowReceiver not closed")
		}

		var retRows sqlbase.EncDatumRows
		for {
			row, meta := out.Next()
			if !meta.Empty() {
				t.Fatalf("unexpected metadata: %v", meta)
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
