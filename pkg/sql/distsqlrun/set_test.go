// Copyright 2017 The Cockroach Authors.
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
// Author: Arjun Narayan (arjun@cockroachlabs.com)

package distsqlrun

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestUnionAll(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
			parser.NewDInt(parser.DInt(i)))
	}

	testCases := []struct {
		spec       SetSpec
		inputLeft  sqlbase.EncDatumRows
		inputRight sqlbase.EncDatumRows
		expected   sqlbase.EncDatumRows
	}{
		{
			spec: SetSpec{
				Type: SetSpec_UnionAll,
			},
			inputLeft: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
			},
			inputRight: sqlbase.EncDatumRows{
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
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
			},
		},
		{
			spec: SetSpec{
				Ordering: Ordering{
					Columns: []Ordering_Column{
						Ordering_Column{
							ColIdx:    0,
							Direction: Ordering_Column_ASC,
						},
					},
				},
				Type: SetSpec_UnionAll,
			},
			inputLeft: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[6]},
				{v[4], v[3]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[5]},
				{v[8], v[9]},
			},
			inputRight: sqlbase.EncDatumRows{
				{v[3], v[3]},
				{v[5], v[6]},
				{v[7], v[3]},
				{v[9], v[6]},
				{v[11], v[6]},
				{v[13], v[5]},
				{v[14], v[9]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[3]},
				{v[3], v[6]},
				{v[4], v[3]},
				{v[5], v[6]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[3]},
				{v[7], v[5]},
				{v[8], v[9]},
				{v[9], v[6]},
				{v[11], v[6]},
				{v[13], v[5]},
				{v[14], v[9]},
			},
		},
	}

	for _, tc := range testCases {
		inL := NewRowBuffer(nil, tc.inputLeft)
		inR := NewRowBuffer(nil, tc.inputRight)
		out := &RowBuffer{}

		flowCtx := FlowCtx{}

		s, err := newSet(&flowCtx, &tc.spec, inL, inR, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}

		s.Run(context.Background(), nil)
		if out.Err != nil {
			t.Fatal(out.Err)
		}
		if !out.Closed {
			t.Fatalf("output RowReceiver not closed")
		}

		if result := out.Rows.String(); result != tc.expected.String() {
			t.Errorf("invalid results: %s, expected %s'", result, tc.expected.String())
		}
	}
}
