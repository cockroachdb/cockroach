// Copyright 2018 The Cockroach Authors.
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

func TestProjectSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = randgen.IntEncDatum(i)
	}
	null := randgen.NullEncDatum()

	testCases := []struct {
		description string
		spec        execinfrapb.ProjectSetSpec
		input       rowenc.EncDatumRows
		inputTypes  []*types.T
		expected    rowenc.EncDatumRows
	}{
		{
			description: "scalar function",
			spec: execinfrapb.ProjectSetSpec{
				Exprs: []execinfrapb.Expression{
					{Expr: "@1 + 1"},
				},
				GeneratedColumns: types.OneIntCol,
				NumColsPerGen:    []uint32{1},
			},
			input: rowenc.EncDatumRows{
				{v[2]},
			},
			inputTypes: types.OneIntCol,
			expected: rowenc.EncDatumRows{
				{v[2], v[3]},
			},
		},
		{
			description: "set-returning function",
			spec: execinfrapb.ProjectSetSpec{
				Exprs: []execinfrapb.Expression{
					{Expr: "generate_series(@1, 2)"},
				},
				GeneratedColumns: types.OneIntCol,
				NumColsPerGen:    []uint32{1},
			},
			input: rowenc.EncDatumRows{
				{v[0]},
				{v[1]},
			},
			inputTypes: types.OneIntCol,
			expected: rowenc.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[2]},
				{v[1], v[1]},
				{v[1], v[2]},
			},
		},
		{
			description: "multiple exprs with different lengths",
			spec: execinfrapb.ProjectSetSpec{
				Exprs: []execinfrapb.Expression{
					{Expr: "0"},
					{Expr: "generate_series(0, 0)"},
					{Expr: "generate_series(0, 1)"},
					{Expr: "generate_series(0, 2)"},
				},
				GeneratedColumns: intCols(4),
				NumColsPerGen:    []uint32{1, 1, 1, 1},
			},
			input: rowenc.EncDatumRows{
				{v[0]},
			},
			inputTypes: types.OneIntCol,
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[0]},
				{v[0], null, null, v[1], v[1]},
				{v[0], null, null, null, v[2]},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			runProcessorTest(
				t,
				execinfrapb.ProcessorCoreUnion{ProjectSet: &c.spec},
				execinfrapb.PostProcessSpec{},
				c.inputTypes,
				c.input,
				append(c.inputTypes, c.spec.GeneratedColumns...), /* outputTypes */
				c.expected,
				nil,
			)
		})
	}
}

func BenchmarkProjectSet(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = randgen.IntEncDatum(i)
	}

	benchCases := []struct {
		description string
		spec        execinfrapb.ProjectSetSpec
		input       rowenc.EncDatumRows
		inputTypes  []*types.T
	}{
		{
			description: "generate_series",
			spec: execinfrapb.ProjectSetSpec{
				Exprs: []execinfrapb.Expression{
					{Expr: "generate_series(1, 100000)"},
				},
				GeneratedColumns: types.OneIntCol,
				NumColsPerGen:    []uint32{1},
			},
			input: rowenc.EncDatumRows{
				{v[0]},
			},
			inputTypes: types.OneIntCol,
		},
	}

	for _, c := range benchCases {
		b.Run(c.description, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				flowCtx := execinfra.FlowCtx{
					Cfg:     &execinfra.ServerConfig{Settings: st},
					EvalCtx: &evalCtx,
				}

				in := distsqlutils.NewRowBuffer(c.inputTypes, c.input, distsqlutils.RowBufferArgs{})
				out := &distsqlutils.RowBuffer{}
				p, err := NewProcessor(
					context.Background(), &flowCtx, 0, /* processorID */
					&execinfrapb.ProcessorCoreUnion{ProjectSet: &c.spec}, &execinfrapb.PostProcessSpec{},
					[]execinfra.RowSource{in}, []execinfra.RowReceiver{out}, []execinfra.LocalProcessor{})
				if err != nil {
					b.Fatal(err)
				}

				p.Run(context.Background())
			}
		})
	}

}
