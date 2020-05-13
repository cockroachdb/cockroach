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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestIsNullProjOp(t *testing.T) {
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

	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		projExpr     string
	}{
		{
			desc:         "SELECT c, c IS NULL FROM t -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0, false}, {nil, true}, {1, false}, {2, false}, {nil, true}},
			projExpr:     "IS NULL",
		},
		{
			desc:         "SELECT c, c IS NULL FROM t -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0, false}, {1, false}, {2, false}},
			projExpr:     "IS NULL",
		},
		{
			desc:         "SELECT c, c IS NULL FROM t -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil, true}, {nil, true}},
			projExpr:     "IS NULL",
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0, true}, {nil, false}, {1, true}, {2, true}, {nil, false}},
			projExpr:     "IS NOT NULL",
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0, true}, {1, true}, {2, true}},
			projExpr:     "IS NOT NULL",
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil, false}, {nil, false}},
			projExpr:     "IS NOT NULL",
		},
		{
			desc:         "SELECT c, c IS NOT DISTINCT FROM NULL FROM t -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0, false}, {nil, true}, {1, false}, {2, false}, {nil, true}},
			projExpr:     "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS NOT DISTINCT FROM NULL FROM t -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0, false}, {1, false}, {2, false}},
			projExpr:     "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS NOT DISTINCT FROM NULL FROM t -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil, true}, {nil, true}},
			projExpr:     "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS DISTINCT FROM NULL FROM t -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0, true}, {nil, false}, {1, true}, {2, true}, {nil, false}},
			projExpr:     "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS DISTINCT FROM NULL FROM t -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0, true}, {1, true}, {2, true}},
			projExpr:     "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS DISTINCT FROM NULL FROM t -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil, false}, {nil, false}},
			projExpr:     "IS DISTINCT FROM NULL",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			opConstructor := func(input []colexecbase.Operator) (colexecbase.Operator, error) {
				return createTestProjectingOperator(
					ctx, flowCtx, input[0], []*types.T{types.Int},
					fmt.Sprintf("@1 %s", c.projExpr), false, /* canFallbackToRowexec */
				)
			}
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier, opConstructor)
		})
	}
}

func TestIsNullSelOp(t *testing.T) {
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

	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		selExpr      string
	}{
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{nil}, {nil}},
			selExpr:      "IS NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{},
			selExpr:      "IS NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil}, {nil}},
			selExpr:      "IS NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0}, {1}, {2}},
			selExpr:      "IS NOT NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0}, {1}, {2}},
			selExpr:      "IS NOT NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{},
			selExpr:      "IS NOT NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT DISTINCT FROM NULL -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{nil}, {nil}},
			selExpr:      "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT DISTINCT FROM NULL -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{},
			selExpr:      "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT DISTINCT FROM NULL -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil}, {nil}},
			selExpr:      "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS DISTINCT FROM NULL -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0}, {1}, {2}},
			selExpr:      "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS DISTINCT FROM NULL -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0}, {1}, {2}},
			selExpr:      "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS DISTINCT FROM NULL -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{},
			selExpr:      "IS DISTINCT FROM NULL",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			opConstructor := func(input []colexecbase.Operator) (colexecbase.Operator, error) {
				spec := &execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{ColumnTypes: []*types.T{types.Int}}},
					Core: execinfrapb.ProcessorCoreUnion{
						Noop: &execinfrapb.NoopCoreSpec{},
					},
					Post: execinfrapb.PostProcessSpec{
						Filter: execinfrapb.Expression{Expr: fmt.Sprintf("@1 %s", c.selExpr)},
					},
				}
				args := NewColOperatorArgs{
					Spec:                spec,
					Inputs:              input,
					StreamingMemAccount: testMemAcc,
				}
				args.TestingKnobs.UseStreamingMemAccountForBuffering = true
				result, err := NewColOperator(ctx, flowCtx, args)
				if err != nil {
					return nil, err
				}
				return result.Op, nil
			}
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier, opConstructor)
		})
	}
}
