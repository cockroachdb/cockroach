// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestFilterer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	// We run the same input rows through various PostProcessSpecs.
	input := rowenc.EncDatumRows{
		{v[0], v[1], v[2]},
		{v[0], v[1], v[3]},
		{v[0], v[1], v[4]},
		{v[0], v[2], v[3]},
		{v[0], v[2], v[4]},
		{v[0], v[3], v[4]},
		{v[1], v[2], v[3]},
		{v[1], v[2], v[4]},
		{v[1], v[3], v[4]},
		{v[2], v[3], v[4]},
	}

	testCases := []struct {
		filter   string
		post     execinfrapb.PostProcessSpec
		expected string
	}{
		{
			filter:   "@1 = 1",
			expected: "[[1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			filter:   "(@1 + @2) % 2 = 0",
			expected: "[[0 2 3] [0 2 4] [1 3 4]]",
		},
		{
			filter: "@2 % 2 <> @3 % 2",
			post: execinfrapb.PostProcessSpec{
				Limit:  4,
				Offset: 1,
			},
			expected: "[[0 1 4] [0 2 3] [0 3 4] [1 2 3]]",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {

			in := distsqlutils.NewRowBuffer(types.ThreeIntCols, input, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}
			spec := execinfrapb.FiltererSpec{
				Filter: execinfrapb.Expression{Expr: c.filter},
			}

			d, err := newFiltererProcessor(&flowCtx, 0 /* processorID */, &spec, in, &c.post, out)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background())
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res rowenc.EncDatumRows
			for {
				row := out.NextNoMeta(t).Copy()
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(types.ThreeIntCols); result != c.expected {
				t.Errorf("invalid results: %s, expected %s", result, c.expected)
			}
		})
	}
}

func BenchmarkFilterer(b *testing.B) {
	defer log.Scope(b).Close(b)
	const numRows = 1 << 16

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &rowDisposer{}
	for _, numCols := range []int{1, 1 << 1, 1 << 2, 1 << 4, 1 << 8} {
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			cols := make([]*types.T, numCols)
			for i := range cols {
				cols[i] = types.Int
			}
			input := execinfra.NewRepeatableRowSource(cols, randgen.MakeIntRows(numRows, numCols))

			var spec execinfrapb.FiltererSpec
			if numCols == 1 {
				spec.Filter.Expr = "@1 % 2 = 0"
			} else {
				spec.Filter.Expr = "@1 % 2 = @2 % 3"
			}
			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newFiltererProcessor(flowCtx, 0 /* processorID */, &spec, input, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
		})
	}
}
