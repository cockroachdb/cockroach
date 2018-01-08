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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
			tree.NewDInt(tree.DInt(i)))
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
		t.Run("", func(t *testing.T) {
			ds := c.spec

			in := NewRowBuffer(twoIntCols, c.input, RowBufferArgs{})
			out := &RowBuffer{}

			evalCtx := tree.MakeTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Ctx:      context.Background(),
				Settings: cluster.MakeTestingClusterSettings(),
				EvalCtx:  evalCtx,
			}

			d, err := newDistinct(&flowCtx, &ds, in, &PostProcessSpec{}, out)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(nil)
			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t)
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(twoIntCols); result != c.expected.String(twoIntCols) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(twoIntCols))
			}
		})
	}
}

func BenchmarkDistinct(b *testing.B) {
	const numCols = 1
	const numRows = 1000

	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(ctx)

	flowCtx := &FlowCtx{
		Ctx:      ctx,
		Settings: cluster.MakeTestingClusterSettings(),
		EvalCtx:  evalCtx,
	}
	spec := &DistinctSpec{
		DistinctColumns: []uint32{0},
	}
	post := &PostProcessSpec{}
	input := NewRepeatableRowSource(oneIntCol, makeIntRows(numRows, numCols))

	b.SetBytes(8 * numRows * numCols)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := newDistinct(flowCtx, spec, input, post, &RowDisposer{})
		if err != nil {
			b.Fatal(err)
		}
		d.Run(nil)
		input.Reset()
	}
	b.StopTimer()
}
