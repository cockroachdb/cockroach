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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"

	"golang.org/x/net/context"
)

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
			parser.NewDInt(parser.DInt(i)))
	}

	testCases := []struct {
		spec     DistinctSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: DistinctSpec{
				DistinctColumns: []uint32{0, 1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
			},
		},
		{
			spec: DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{0, 1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
			},
		},
		{
			spec: DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
			},
		},
		{
			spec: DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[6], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[6], v[6]},
			},
		},
	}

	for _, c := range testCases {
		ds := c.spec

		in := NewRowBuffer(nil /* types */, c.input, RowBufferArgs{})
		out := &RowBuffer{}

		flowCtx := FlowCtx{}

		d, err := newDistinct(&flowCtx, &ds, in, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}

		d.Run(context.Background(), nil)
		if !out.ProducerClosed {
			t.Fatalf("output RowReceiver not closed")
		}
		var res sqlbase.EncDatumRows
		for {
			row, meta := out.Next()
			if !meta.Empty() {
				t.Fatalf("unexpected metadata: %v", meta)
			}
			if row == nil {
				break
			}
			res = append(res, row)
		}

		if result := res.String(); result != c.expected.String() {
			t.Errorf("invalid results: %s, expected %s'", result, c.expected.String())
		}
	}
}
