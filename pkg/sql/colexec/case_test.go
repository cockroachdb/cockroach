// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCaseOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{}},
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post: execinfrapb.PostProcessSpec{
			RenderExprs: []execinfrapb.Expression{{}},
		},
	}

	decs := make([]apd.Decimal, 2)
	decs[0].SetInt64(0)
	decs[1].SetInt64(1)
	zero := decs[0]
	one := decs[1]

	for _, tc := range []struct {
		tuples     tuples
		renderExpr string
		expected   tuples
		inputTypes []types.T
	}{
		{
			// Basic test.
			tuples:     tuples{{1}, {2}, {nil}, {3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 1 ELSE 0 END",
			expected:   tuples{{0}, {1}, {0}, {0}},
			inputTypes: []types.T{*types.Int},
		},
		{
			// Test "reordered when's."
			tuples:     tuples{{1, 1}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 + @2 > 3 THEN 0 WHEN @1 = 2 THEN 1 ELSE 2 END",
			expected:   tuples{{2}, {1}, {2}, {0}},
			inputTypes: []types.T{*types.Int, *types.Int},
		},
		{
			// Test the short-circuiting behavior.
			tuples:     tuples{{1, 2}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 0::DECIMAL WHEN @1 / @2 = 1 THEN 1::DECIMAL END",
			expected:   tuples{{nil}, {zero}, {nil}, {one}},
			inputTypes: []types.T{*types.Int, *types.Int},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(inputs []Operator) (Operator, error) {
			spec.Input[0].ColumnTypes = tc.inputTypes
			spec.Post.RenderExprs[0].Expr = tc.renderExpr
			result, err := NewColOperator(
				ctx, flowCtx, spec, inputs, testMemAcc,
				true, /* useStreamingMemAccountForBuffering */
			)
			if err != nil {
				return nil, err
			}
			return result.Op, nil
		})
	}
}
