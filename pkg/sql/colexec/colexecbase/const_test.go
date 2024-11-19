// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbase_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestConst(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	tcs := []struct {
		tuples   colexectestutils.Tuples
		expected colexectestutils.Tuples
	}{
		{
			tuples:   colexectestutils.Tuples{{1}, {1}},
			expected: colexectestutils.Tuples{{1, 9}, {1, 9}},
		},
		{
			tuples:   colexectestutils.Tuples{},
			expected: colexectestutils.Tuples{},
		},
	}
	for _, tc := range tcs {
		colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{{types.Int}}, tc.expected, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, input[0], []*types.T{types.Int},
					"9" /* projectingExpr */, testMemAcc,
				)
			})
	}
}

func TestConstNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	tcs := []struct {
		tuples   colexectestutils.Tuples
		expected colexectestutils.Tuples
	}{
		{
			tuples:   colexectestutils.Tuples{{1}, {1}},
			expected: colexectestutils.Tuples{{1, nil}, {1, nil}},
		},
		{
			tuples:   colexectestutils.Tuples{},
			expected: colexectestutils.Tuples{},
		},
	}
	for _, tc := range tcs {
		colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{{types.Int}}, tc.expected, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, input[0], []*types.T{types.Int},
					"NULL::INT" /* projectingExpr */, testMemAcc,
				)
			})
	}
}
