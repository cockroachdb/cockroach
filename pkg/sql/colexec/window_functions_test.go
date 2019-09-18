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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type windowFnTestCase struct {
	tuples       []tuple
	expected     []tuple
	windowerSpec execinfrapb.WindowerSpec
}

func TestRank(t *testing.T) {
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

	rankFn := execinfrapb.WindowerSpec_RANK
	denseRankFn := execinfrapb.WindowerSpec_DENSE_RANK
	for _, tc := range []windowFnTestCase{
		// With PARTITION BY, no ORDER BY.
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
			windowerSpec: execinfrapb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
						OutputColIdx: 1,
					},
				},
			},
		},
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
			windowerSpec: execinfrapb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
						OutputColIdx: 1,
					},
				},
			},
		},
		// No PARTITION BY, with ORDER BY.
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 3}, {1, 3}, {2, 5}, {3, 6}, {3, 6}},
			windowerSpec: execinfrapb.WindowerSpec{
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
						Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
						OutputColIdx: 1,
					},
				},
			},
		},
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 4}},
			windowerSpec: execinfrapb.WindowerSpec{
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
						Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
						OutputColIdx: 1,
					},
				},
			},
		},
		// With both PARTITION BY and ORDER BY.
		{
			tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
			expected: tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
			windowerSpec: execinfrapb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
						Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
						OutputColIdx: 2,
					},
				},
			},
		},
		{
			tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
			expected: tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 2}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
			windowerSpec: execinfrapb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
						Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
						OutputColIdx: 2,
					},
				},
			},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier, func(inputs []Operator) (Operator, error) {
			ct := make([]types.T, len(tc.tuples[0]))
			for i := range ct {
				ct[i] = *types.Int
			}
			spec := &execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{ColumnTypes: ct}},
				Core: execinfrapb.ProcessorCoreUnion{
					Windower: &tc.windowerSpec,
				},
			}
			result, err := NewColOperator(ctx, flowCtx, spec, inputs)
			if err != nil {
				return nil, err
			}
			return result.Op, nil
		})
	}
}

func TestRowNumber(t *testing.T) {
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

	rowNumberFn := execinfrapb.WindowerSpec_ROW_NUMBER
	for _, tc := range []windowFnTestCase{
		// Without ORDER BY, the output of row_number is non-deterministic, so we
		// skip such a case.
		//
		// No PARTITION BY, with ORDER BY.
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 2}, {1, 3}, {1, 4}, {2, 5}, {3, 6}, {3, 7}},
			windowerSpec: execinfrapb.WindowerSpec{
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
						Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
						OutputColIdx: 1,
					},
				},
			},
		},
		// With both PARTITION BY and ORDER BY.
		{
			tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
			expected: tuples{{nil, nil, 1}, {nil, nil, 2}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
			windowerSpec: execinfrapb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []execinfrapb.WindowerSpec_WindowFn{
					{
						Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
						Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
						OutputColIdx: 2,
					},
				},
			},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier, func(inputs []Operator) (Operator, error) {
			ct := make([]types.T, len(tc.tuples[0]))
			for i := range ct {
				ct[i] = *types.Int
			}
			spec := &execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{ColumnTypes: ct}},
				Core: execinfrapb.ProcessorCoreUnion{
					Windower: &tc.windowerSpec,
				},
			}
			result, err := NewColOperator(ctx, flowCtx, spec, inputs)
			if err != nil {
				return nil, err
			}
			return result.Op, nil
		})
	}
}
