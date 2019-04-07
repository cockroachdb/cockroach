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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		spec     distsqlpb.DistinctSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: distsqlpb.DistinctSpec{
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
			spec: distsqlpb.DistinctSpec{
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
			spec: distsqlpb.DistinctSpec{
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
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ds := c.spec

			in := NewRowBuffer(sqlbase.TwoIntCols, c.input, RowBufferArgs{})
			out := &RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Settings: st,
				EvalCtx:  &evalCtx,
			}

			d, err := NewDistinct(&flowCtx, 0 /* processorID */, &ds, in, &distsqlpb.PostProcessSpec{}, out)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background())
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t).Copy()
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(sqlbase.TwoIntCols); result != c.expected.String(sqlbase.TwoIntCols) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(sqlbase.TwoIntCols))
			}
		})
	}
}

func benchmarkDistinct(b *testing.B, orderedColumns []uint32) {
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	spec := &distsqlpb.DistinctSpec{
		DistinctColumns: []uint32{0, 1},
	}
	spec.OrderedColumns = orderedColumns

	post := &distsqlpb.PostProcessSpec{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			input := NewRepeatableRowSource(sqlbase.TwoIntCols, sqlbase.MakeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := NewDistinct(flowCtx, 0 /* processorID */, spec, input, post, &RowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
		})
	}
}

func BenchmarkOrderedDistinct(b *testing.B) {
	benchmarkDistinct(b, []uint32{0, 1})
}

func BenchmarkPartiallyOrderedDistinct(b *testing.B) {
	benchmarkDistinct(b, []uint32{0})
}

func BenchmarkUnorderedDistinct(b *testing.B) {
	benchmarkDistinct(b, []uint32{})
}
