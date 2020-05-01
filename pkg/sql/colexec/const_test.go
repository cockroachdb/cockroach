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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestConst(t *testing.T) {
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
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, 9}, {1, 9}},
		},
		{
			tuples:   tuples{},
			expected: tuples{},
		},
	}
	for _, tc := range tcs {
		runTestsWithTyps(t, []tuples{tc.tuples}, [][]*types.T{{types.Int}}, tc.expected, orderedVerifier,
			func(input []colexecbase.Operator) (colexecbase.Operator, error) {
				return createTestProjectingOperator(
					ctx, flowCtx, input[0], []*types.T{types.Int},
					"9" /* projectingExpr */, false, /* canFallbackToRowexec */
				)
			})
	}
}

func TestConstNull(t *testing.T) {
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
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, nil}, {1, nil}},
		},
		{
			tuples:   tuples{},
			expected: tuples{},
		},
	}
	for _, tc := range tcs {
		runTestsWithTyps(t, []tuples{tc.tuples}, [][]*types.T{{types.Int}}, tc.expected, orderedVerifier,
			func(input []colexecbase.Operator) (colexecbase.Operator, error) {
				return createTestProjectingOperator(
					ctx, flowCtx, input[0], []*types.T{types.Int},
					"NULL::INT" /* projectingExpr */, false, /* canFallbackToRowexec */
				)
			})
	}
}
