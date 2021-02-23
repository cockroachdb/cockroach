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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCaseOp(t *testing.T) {
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

	for _, tc := range []struct {
		tuples     colexectestutils.Tuples
		renderExpr string
		expected   colexectestutils.Tuples
		inputTypes []*types.T
	}{
		{
			// Basic test.
			tuples:     colexectestutils.Tuples{{1}, {2}, {nil}, {3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 1 ELSE 0 END",
			expected:   colexectestutils.Tuples{{0}, {1}, {0}, {0}},
			inputTypes: []*types.T{types.Int},
		},
		{
			// Test "reordered when's."
			tuples:     colexectestutils.Tuples{{1, 1}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 + @2 > 3 THEN 0 WHEN @1 = 2 THEN 1 ELSE 2 END",
			expected:   colexectestutils.Tuples{{2}, {1}, {2}, {0}},
			inputTypes: []*types.T{types.Int, types.Int},
		},
		{
			// Test the short-circuiting behavior.
			tuples:     colexectestutils.Tuples{{1, 2}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 0::FLOAT WHEN @1 / @2 = 1 THEN 1::FLOAT END",
			expected:   colexectestutils.Tuples{{nil}, {0.0}, {nil}, {1.0}},
			inputTypes: []*types.T{types.Int, types.Int},
		},
	} {
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(inputs []colexecop.Operator) (colexecop.Operator, error) {
			caseOp, err := colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, inputs[0], tc.inputTypes, tc.renderExpr,
				false /* canFallbackToRowexec */, testMemAcc,
			)
			if err != nil {
				return nil, err
			}
			// We will project out the input columns in order to have test
			// cases be less verbose.
			return colexecbase.NewSimpleProjectOp(caseOp, len(tc.inputTypes)+1, []uint32{uint32(len(tc.inputTypes))}), nil
		})
	}
}
