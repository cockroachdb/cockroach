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

	for _, tc := range []struct {
		tuples     tuples
		renderExpr string
		expected   tuples
		inputTypes []*types.T
	}{
		{
			// Basic test.
			tuples:     tuples{{1}, {2}, {nil}, {3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 1 ELSE 0 END",
			expected:   tuples{{0}, {1}, {0}, {0}},
			inputTypes: []*types.T{types.Int},
		},
		{
			// Test "reordered when's."
			tuples:     tuples{{1, 1}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 + @2 > 3 THEN 0 WHEN @1 = 2 THEN 1 ELSE 2 END",
			expected:   tuples{{2}, {1}, {2}, {0}},
			inputTypes: []*types.T{types.Int, types.Int},
		},
		{
			// Test the short-circuiting behavior.
			tuples:     tuples{{1, 2}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 0::FLOAT WHEN @1 / @2 = 1 THEN 1::FLOAT END",
			expected:   tuples{{nil}, {0.0}, {nil}, {1.0}},
			inputTypes: []*types.T{types.Int, types.Int},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(inputs []colexecbase.Operator) (colexecbase.Operator, error) {
			caseOp, err := createTestProjectingOperator(
				ctx, flowCtx, inputs[0], tc.inputTypes, tc.renderExpr,
				false, /* canFallbackToRowexec */
			)
			if err != nil {
				return nil, err
			}
			// We will project out the input columns in order to have test
			// cases be less verbose.
			return NewSimpleProjectOp(caseOp, len(tc.inputTypes)+1, []uint32{uint32(len(tc.inputTypes))}), nil
		})
	}
}
