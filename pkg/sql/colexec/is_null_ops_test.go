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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestIsNullProjOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		projExpr     string
	}{
		{
			desc:         "SELECT c, c IS NULL FROM t -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{0, false}, {nil, true}, {1, false}, {2, false}, {nil, true}},
			projExpr:     "IS NULL",
		},
		{
			desc:         "SELECT c, c IS NULL FROM t -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0, false}, {1, false}, {2, false}},
			projExpr:     "IS NULL",
		},
		{
			desc:         "SELECT c, c IS NULL FROM t -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil, true}, {nil, true}},
			projExpr:     "IS NULL",
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{0, true}, {nil, false}, {1, true}, {2, true}, {nil, false}},
			projExpr:     "IS NOT NULL",
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0, true}, {1, true}, {2, true}},
			projExpr:     "IS NOT NULL",
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil, false}, {nil, false}},
			projExpr:     "IS NOT NULL",
		},
		{
			desc:         "SELECT c, c IS NOT DISTINCT FROM NULL FROM t -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{0, false}, {nil, true}, {1, false}, {2, false}, {nil, true}},
			projExpr:     "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS NOT DISTINCT FROM NULL FROM t -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0, false}, {1, false}, {2, false}},
			projExpr:     "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS NOT DISTINCT FROM NULL FROM t -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil, true}, {nil, true}},
			projExpr:     "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS DISTINCT FROM NULL FROM t -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{0, true}, {nil, false}, {1, true}, {2, true}, {nil, false}},
			projExpr:     "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS DISTINCT FROM NULL FROM t -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0, true}, {1, true}, {2, true}},
			projExpr:     "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c, c IS DISTINCT FROM NULL FROM t -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil, false}, {nil, false}},
			projExpr:     "IS DISTINCT FROM NULL",
		},
	}

	for _, c := range testCases {
		log.Infof(ctx, "%s", c.desc)
		opConstructor := func(input []colexecop.Operator) (colexecop.Operator, error) {
			return colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, input[0], []*types.T{types.Int},
				fmt.Sprintf("@1 %s", c.projExpr), false /* canFallbackToRowexec */, testMemAcc,
			)
		}
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
	}
}

func TestIsNullSelOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		selExpr      string
	}{
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil}, {nil}},
			selExpr:      "IS NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{},
			selExpr:      "IS NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil}, {nil}},
			selExpr:      "IS NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{0}, {1}, {2}},
			selExpr:      "IS NOT NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0}, {1}, {2}},
			selExpr:      "IS NOT NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{},
			selExpr:      "IS NOT NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT DISTINCT FROM NULL -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil}, {nil}},
			selExpr:      "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT DISTINCT FROM NULL -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{},
			selExpr:      "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT DISTINCT FROM NULL -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil}, {nil}},
			selExpr:      "IS NOT DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS DISTINCT FROM NULL -- both",
			inputTuples:  colexectestutils.Tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{0}, {1}, {2}},
			selExpr:      "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS DISTINCT FROM NULL -- no NULLs",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0}, {1}, {2}},
			selExpr:      "IS DISTINCT FROM NULL",
		},
		{
			desc:         "SELECT c FROM t WHERE c IS DISTINCT FROM NULL -- only NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}},
			outputTuples: colexectestutils.Tuples{},
			selExpr:      "IS DISTINCT FROM NULL",
		},
	}

	for _, c := range testCases {
		log.Infof(ctx, "%s", c.desc)
		opConstructor := func(sources []colexecop.Operator) (colexecop.Operator, error) {
			typs := []*types.T{types.Int}
			spec := &execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{ColumnTypes: typs}},
				Core: execinfrapb.ProcessorCoreUnion{
					Filterer: &execinfrapb.FiltererSpec{
						Filter: execinfrapb.Expression{Expr: fmt.Sprintf("@1 %s", c.selExpr)},
					},
				},
				ResultTypes: typs,
			}
			args := &colexecargs.NewColOperatorArgs{
				Spec:                spec,
				Inputs:              colexectestutils.MakeInputs(sources),
				StreamingMemAccount: testMemAcc,
			}
			result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
			if err != nil {
				return nil, err
			}
			return result.Root, nil
		}
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
	}
}
