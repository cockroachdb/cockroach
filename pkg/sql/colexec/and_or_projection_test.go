// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type andOrTestCase struct {
	tuples                []colexectestutils.Tuple
	expected              []colexectestutils.Tuple
	skipAllNullsInjection bool
}

var (
	andTestCases []andOrTestCase
	orTestCases  []andOrTestCase
)

func init() {
	andTestCases = []andOrTestCase{
		// All variations of pairs separately first.
		{
			tuples:   colexectestutils.Tuples{{false, true}},
			expected: colexectestutils.Tuples{{false}},
		},
		{
			tuples:   colexectestutils.Tuples{{false, nil}},
			expected: colexectestutils.Tuples{{false}},
		},
		{
			tuples:   colexectestutils.Tuples{{false, false}},
			expected: colexectestutils.Tuples{{false}},
		},
		{
			tuples:   colexectestutils.Tuples{{true, true}},
			expected: colexectestutils.Tuples{{true}},
		},
		{
			tuples:   colexectestutils.Tuples{{true, false}},
			expected: colexectestutils.Tuples{{false}},
		},
		{
			tuples:   colexectestutils.Tuples{{true, nil}},
			expected: colexectestutils.Tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   colexectestutils.Tuples{{nil, true}},
			expected: colexectestutils.Tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   colexectestutils.Tuples{{nil, false}},
			expected: colexectestutils.Tuples{{false}},
		},
		{
			tuples:   colexectestutils.Tuples{{nil, nil}},
			expected: colexectestutils.Tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: colexectestutils.Tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: colexectestutils.Tuples{
				{false}, {false}, {false},
				{true}, {false}, {nil},
				{nil}, {false}, {nil},
			},
		},
	}

	orTestCases = []andOrTestCase{
		// All variations of pairs separately first.
		{
			tuples:   colexectestutils.Tuples{{false, true}},
			expected: colexectestutils.Tuples{{true}},
		},
		{
			tuples:   colexectestutils.Tuples{{false, nil}},
			expected: colexectestutils.Tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   colexectestutils.Tuples{{false, false}},
			expected: colexectestutils.Tuples{{false}},
		},
		{
			tuples:   colexectestutils.Tuples{{true, true}},
			expected: colexectestutils.Tuples{{true}},
		},
		{
			tuples:   colexectestutils.Tuples{{true, false}},
			expected: colexectestutils.Tuples{{true}},
		},
		{
			tuples:   colexectestutils.Tuples{{true, nil}},
			expected: colexectestutils.Tuples{{true}},
		},
		{
			tuples:   colexectestutils.Tuples{{nil, true}},
			expected: colexectestutils.Tuples{{true}},
		},
		{
			tuples:   colexectestutils.Tuples{{nil, false}},
			expected: colexectestutils.Tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   colexectestutils.Tuples{{nil, nil}},
			expected: colexectestutils.Tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: colexectestutils.Tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: colexectestutils.Tuples{
				{true}, {nil}, {false},
				{true}, {true}, {true},
				{true}, {nil}, {nil},
			},
		},
	}
}

func TestAndOrOps(t *testing.T) {
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

	for _, test := range []struct {
		operation string
		cases     []andOrTestCase
	}{
		{
			operation: "AND",
			cases:     andTestCases,
		},
		{
			operation: "OR",
			cases:     orTestCases,
		},
	} {
		t.Run(test.operation, func(t *testing.T) {
			for _, tc := range test.cases {
				var runner colexectestutils.TestRunner
				if tc.skipAllNullsInjection {
					// We're omitting all nulls injection test. See comments for each such
					// test case.
					runner = colexectestutils.RunTestsWithoutAllNullsInjection
				} else {
					runner = colexectestutils.RunTestsWithTyps
				}
				runner(
					t,
					testAllocator,
					[]colexectestutils.Tuples{tc.tuples},
					[][]*types.T{{types.Bool, types.Bool}},
					tc.expected,
					colexectestutils.OrderedVerifier,
					func(input []colexecop.Operator) (colexecop.Operator, error) {
						projOp, err := colexectestutils.CreateTestProjectingOperator(
							ctx, flowCtx, input[0], []*types.T{types.Bool, types.Bool},
							fmt.Sprintf("@1 %s @2", test.operation), testMemAcc,
						)
						if err != nil {
							return nil, err
						}
						// We will project out the first two columns in order
						// to have test cases be less verbose.
						return colexecbase.NewSimpleProjectOp(projOp, 3 /* numInputCols */, []uint32{2}), nil
					})
			}
		})
	}
}
