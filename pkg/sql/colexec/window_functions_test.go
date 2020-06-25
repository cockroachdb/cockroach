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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

type windowFnTestCase struct {
	tuples       []tuple
	expected     []tuple
	windowerSpec execinfrapb.WindowerSpec
}

func (tc *windowFnTestCase) init() {
	for i := range tc.windowerSpec.WindowFns {
		tc.windowerSpec.WindowFns[i].FilterColIdx = tree.NoColumnIdx
	}
}

func TestWindowFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	evalCtx.SessionData.VectorizeMode = sessiondata.VectorizeOn
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
	}
	// All supported window function operators will use from 0 to 3 disk queues
	// with each using a single FD at any point in time. Additionally, the
	// disk-backed sorter (that will be planned depending on PARTITION BY and
	// ORDER BY combinations) will be limited to this number using a testing
	// knob, so 3 is necessary and sufficient.
	const maxNumberFDs = 3
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rowNumberFn := execinfrapb.WindowerSpec_ROW_NUMBER
	rankFn := execinfrapb.WindowerSpec_RANK
	denseRankFn := execinfrapb.WindowerSpec_DENSE_RANK
	percentRankFn := execinfrapb.WindowerSpec_PERCENT_RANK
	cumeDistFn := execinfrapb.WindowerSpec_CUME_DIST
	accounts := make([]*mon.BoundAccount, 0)
	monitors := make([]*mon.BytesMonitor, 0)
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, tc := range []windowFnTestCase{
			// With PARTITION BY, no ORDER BY.
			{
				tuples:   tuples{{1}, {1}, {1}, {2}, {2}, {3}},
				expected: tuples{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {3, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
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
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 0}, {nil, 0}, {1, 0}, {1, 0}, {2, 0}, {3, 0}, {3, 0}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1.0}, {nil, 1.0}, {1, 1.0}, {1, 1.0}, {2, 1.0}, {3, 1.0}, {3, 1.0}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
							OutputColIdx: 1,
						},
					},
				},
			},

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
			{
				tuples:   tuples{{3}, {1}, {2}, {1}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 0}, {nil, 0}, {1, 2.0 / 7}, {1, 2.0 / 7}, {1, 2.0 / 7}, {2, 5.0 / 7}, {3, 6.0 / 7}, {3, 6.0 / 7}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {1}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 2.0 / 8}, {nil, 2.0 / 8}, {1, 5.0 / 8}, {1, 5.0 / 8}, {1, 5.0 / 8}, {2, 6.0 / 8}, {3, 1.0}, {3, 1.0}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
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
			{
				tuples:   tuples{{nil, 2}, {3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {1, 3}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 0}, {nil, nil, 0}, {nil, 1, 2.0 / 3}, {nil, 2, 1}, {1, nil, 0}, {1, 2, 1.0 / 2}, {1, 3, 1}, {2, 1, 0}, {3, 1, 0}, {3, 2, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   tuples{{nil, 2}, {3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {1, 3}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 2.0 / 4}, {nil, nil, 2.0 / 4}, {nil, 1, 3.0 / 4}, {nil, 2, 1}, {1, nil, 1.0 / 3}, {1, 2, 2.0 / 3}, {1, 3, 1}, {2, 1, 1}, {3, 1, 1.0 / 2}, {3, 2, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
		} {
			t.Run(fmt.Sprintf("spillForced=%t/%s", spillForced, tc.windowerSpec.WindowFns[0].Func.String()), func(t *testing.T) {
				var semsToCheck []semaphore.Semaphore
				runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier, func(inputs []colexecbase.Operator) (colexecbase.Operator, error) {
					tc.init()
					ct := make([]*types.T, len(tc.tuples[0]))
					for i := range ct {
						ct[i] = types.Int
					}
					spec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: ct}},
						Core: execinfrapb.ProcessorCoreUnion{
							Windower: &tc.windowerSpec,
						},
					}
					sem := colexecbase.NewTestingSemaphore(maxNumberFDs)
					args := NewColOperatorArgs{
						Spec:                spec,
						Inputs:              inputs,
						StreamingMemAccount: testMemAcc,
						DiskQueueCfg:        queueCfg,
						FDSemaphore:         sem,
					}
					semsToCheck = append(semsToCheck, sem)
					args.TestingKnobs.UseStreamingMemAccountForBuffering = true
					args.TestingKnobs.NumForcedRepartitions = maxNumberFDs
					result, err := TestNewColOperator(ctx, flowCtx, args)
					accounts = append(accounts, result.OpAccounts...)
					monitors = append(monitors, result.OpMonitors...)
					return result.Op, err
				})
				for i, sem := range semsToCheck {
					require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
				}
			})
		}
	}

	for _, acc := range accounts {
		acc.Close(ctx)
	}

	for _, m := range monitors {
		m.Stop(ctx)
	}
}
