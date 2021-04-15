// Copyright 2019 The Cockroach Authors.
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

func TestOrdinality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		spec     execinfrapb.OrdinalitySpec
		input    rowenc.EncDatumRows
		expected rowenc.EncDatumRows
	}{
		{
			input: rowenc.EncDatumRows{
				{v[2]},
				{v[5]},
				{v[2]},
				{v[5]},
				{v[2]},
				{v[3]},
				{v[2]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[1]},
				{v[5], v[2]},
				{v[2], v[3]},
				{v[5], v[4]},
				{v[2], v[5]},
				{v[3], v[6]},
				{v[2], v[7]},
			},
		},
		{
			input: rowenc.EncDatumRows{
				{},
				{},
				{},
				{},
				{},
				{},
				{},
			},
			expected: rowenc.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
				{v[4]},
				{v[5]},
				{v[6]},
				{v[7]},
			},
		},
		{
			input: rowenc.EncDatumRows{
				{v[2], v[1]},
				{v[5], v[2]},
				{v[2], v[3]},
				{v[5], v[4]},
				{v[2], v[5]},
				{v[3], v[6]},
				{v[2], v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[1], v[1]},
				{v[5], v[2], v[2]},
				{v[2], v[3], v[3]},
				{v[5], v[4], v[4]},
				{v[2], v[5], v[5]},
				{v[3], v[6], v[6]},
				{v[2], v[7], v[7]},
			},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			os := c.spec

			in := distsqlutils.NewRowBuffer(types.TwoIntCols, c.input, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			d, err := newOrdinalityProcessor(&flowCtx, 0 /* processorID */, &os, in, &execinfrapb.PostProcessSpec{}, out)
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

			var typs []*types.T
			switch len(res[0]) {
			case 1:
				typs = types.OneIntCol
			case 2:
				typs = types.TwoIntCols
			case 3:
				typs = types.ThreeIntCols
			}
			if result := res.String(typs); result != c.expected.String(typs) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(types.TwoIntCols))
			}
		})
	}
}

func BenchmarkOrdinality(b *testing.B) {
	defer log.Scope(b).Close(b)
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	spec := &execinfrapb.OrdinalitySpec{}

	post := &execinfrapb.PostProcessSpec{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		input := execinfra.NewRepeatableRowSource(types.TwoIntCols, randgen.MakeIntRows(numRows, numCols))
		b.SetBytes(int64(8 * numRows * numCols))
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				o, err := newOrdinalityProcessor(flowCtx, 0 /* processorID */, spec, input, post, &rowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				o.Run(ctx)
				input.Reset()
			}
		})
	}
}
