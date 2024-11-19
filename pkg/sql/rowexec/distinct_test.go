// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	v := [15]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	vNull := rowenc.DatumToEncDatum(types.Unknown, tree.DNull)

	testCases := []struct {
		spec     execinfrapb.DistinctSpec
		input    rowenc.EncDatumRows
		expected rowenc.EncDatumRows
		error    string
	}{
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns: []uint32{0, 1},
			},
			input: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[5], v[6], v[2]},
				{v[2], v[3], v[3]},
				{v[5], v[6], v[4]},
				{v[2], v[6], v[5]},
				{v[3], v[5], v[6]},
				{v[2], v[9], v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[5], v[6], v[2]},
				{v[2], v[6], v[5]},
				{v[3], v[5], v[6]},
				{v[2], v[9], v[7]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{0, 1},
			},
			input: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
				{v[5], v[6], v[7]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{1},
			},
			input: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
				{v[6], v[6], v[7]},
				{v[7], v[6], v[8]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{1},
			},
			input: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
				{v[6], v[6], v[7]},
				{v[7], v[6], v[8]},
			},
			expected: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
			},
		},

		// Test NullsAreDistinct flag (not ordered).
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: false,
			},
			input: rowenc.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
				{vNull, v[2], v[7]},
				{v[1], vNull, v[8]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: true,
			},
			input: rowenc.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
				{vNull, v[2], v[7]},
				{v[1], vNull, v[8]},
			},
			expected: rowenc.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
				{vNull, v[2], v[7]},
				{v[1], vNull, v[8]},
			},
		},

		// Test NullsAreDistinct flag (ordered).
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:   []uint32{0},
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: false,
			},
			input: rowenc.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
				{v[1], vNull, v[7]},
				{v[1], v[2], v[8]},
			},
			expected: rowenc.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:   []uint32{0},
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: true,
			},
			input: rowenc.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
				{v[1], vNull, v[7]},
				{v[1], v[2], v[8]},
			},
			expected: rowenc.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
				{v[1], vNull, v[7]},
			},
		},

		// Test ErrorOnDup flag (ordered).
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{0},
				DistinctColumns: []uint32{0, 1},
				ErrorOnDup:      "duplicate rows",
			},
			input: rowenc.EncDatumRows{
				{v[1], v[2], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[3], v[3]},
				{v[3], v[4], v[4]},
			},
			error: "duplicate rows",
		},

		// Test ErrorOnDup flag (unordered).
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns: []uint32{0, 1},
				ErrorOnDup:      "duplicate rows",
			},
			input: rowenc.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[1], v[2], v[2]},
				{v[3], v[4], v[3]},
				{v[2], v[3], v[4]},
			},
			error: "duplicate rows",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ds := c.spec

			in := distsqlutils.NewRowBuffer(types.ThreeIntCols, c.input, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := eval.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
				Mon:     evalCtx.TestingMon,
			}

			d, err := newDistinct(context.Background(), &flowCtx, 0 /* processorID */, &ds, in, &execinfrapb.PostProcessSpec{})
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background(), out)
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res rowenc.EncDatumRows
			for {
				row, meta := out.Next()
				if meta != nil {
					err = meta.Err
					break
				}
				if row == nil {
					break
				}
				res = append(res, row.Copy())
			}

			if c.error != "" {
				if err == nil || err.Error() != c.error {
					t.Errorf("expected error: %v, got %v", c.error, err)
				}
			} else {
				if result := res.String(types.ThreeIntCols); result != c.expected.String(types.ThreeIntCols) {
					t.Errorf("invalid results: %v, expected %v'", result, c.expected.String(types.ThreeIntCols))
				}
			}
		})
	}
}

func benchmarkDistinct(b *testing.B, orderedColumns []uint32) {
	defer log.Scope(b).Close(b)
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}
	spec := &execinfrapb.DistinctSpec{
		DistinctColumns: []uint32{0, 1},
	}
	spec.OrderedColumns = orderedColumns

	post := &execinfrapb.PostProcessSpec{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			input := execinfra.NewRepeatableRowSource(types.TwoIntCols, randgen.MakeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newDistinct(ctx, flowCtx, 0 /* processorID */, spec, input, post)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background(), &rowDisposer{})
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
