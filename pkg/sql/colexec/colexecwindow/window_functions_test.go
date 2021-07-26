// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

type windowFnTestCase struct {
	tuples       []colexectestutils.Tuple
	expected     []colexectestutils.Tuple
	windowerSpec execinfrapb.WindowerSpec
}

func (tc *windowFnTestCase) init() {
	for i := range tc.windowerSpec.WindowFns {
		tc.windowerSpec.WindowFns[i].FilterColIdx = tree.NoColumnIdx
	}
}

func TestWindowFunctions(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rowNumberFn := execinfrapb.WindowerSpec_ROW_NUMBER
	rankFn := execinfrapb.WindowerSpec_RANK
	denseRankFn := execinfrapb.WindowerSpec_DENSE_RANK
	percentRankFn := execinfrapb.WindowerSpec_PERCENT_RANK
	cumeDistFn := execinfrapb.WindowerSpec_CUME_DIST
	nTileFn := execinfrapb.WindowerSpec_NTILE
	lagFn := execinfrapb.WindowerSpec_LAG
	leadFn := execinfrapb.WindowerSpec_LEAD
	firstValueFn := execinfrapb.WindowerSpec_FIRST_VALUE
	lastValueFn := execinfrapb.WindowerSpec_LAST_VALUE
	nthValueFn := execinfrapb.WindowerSpec_NTH_VALUE
	accounts := make([]*mon.BoundAccount, 0)
	monitors := make([]*mon.BytesMonitor, 0)
	for _, spillForced := range []bool{true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, tc := range []windowFnTestCase{
			// With PARTITION BY, no ORDER BY.
			{
				tuples:   colexectestutils.Tuples{{1}, {1}, {1}, {2}, {2}, {3}},
				expected: colexectestutils.Tuples{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {3, 1}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 0}, {nil, 0}, {1, 0}, {1, 0}, {2, 0}, {3, 0}, {3, 0}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1.0}, {nil, 1.0}, {1, 1.0}, {1, 1.0}, {2, 1.0}, {3, 1.0}, {3, 1.0}},
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
			{
				tuples:   colexectestutils.Tuples{{3, 3}, {1, 3}, {2, 3}, {nil, 3}, {1, 3}, {1, 3}, {nil, 3}, {3, 3}},
				expected: colexectestutils.Tuples{{nil, 3, 1}, {nil, 3, 2}, {1, 3, 1}, {1, 3, 2}, {1, 3, 3}, {2, 3, 1}, {3, 3, 1}, {3, 3, 2}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nTileFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 1, -1, 10}, {1, 2, 1, nil}, {2, 3, 0, 10}, {nil, 4, 5, 10},
					{1, 5, 1, 10}, {1, 6, 1, 10}, {nil, 7, nil, 10}, {3, 8, nil, 10},
				},
				expected: colexectestutils.Tuples{
					{3, 1, -1, 10, 8}, {1, 2, 1, nil, nil}, {2, 3, 0, 10, 3}, {nil, 4, 5, 10, 10},
					{1, 5, 1, 10, 2}, {1, 6, 1, 10, 5}, {nil, 7, nil, 10, nil}, {3, 8, nil, 10, nil},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lagFn},
							ArgsIdxs:     []uint32{1, 2, 3},
							OutputColIdx: 4,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 1, 1, 10}, {1, 2, 1, 10}, {2, 3, 0, 10}, {nil, 4, 5, 10},
					{1, 5, 1, 10}, {1, 6, 1, 10}, {nil, 7, nil, 10}, {3, 8, -1, 10},
				},
				expected: colexectestutils.Tuples{
					{3, 1, 1, 10, 8}, {1, 2, 1, 10, 5}, {2, 3, 0, 10, 3}, {nil, 4, 5, 10, 10},
					{1, 5, 1, 10, 6}, {1, 6, 1, 10, 10}, {nil, 7, nil, 10, nil}, {3, 8, -1, 10, 1},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &leadFn},
							ArgsIdxs:     []uint32{1, 2, 3},
							OutputColIdx: 4,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 5}, {3, 6}},
				expected: colexectestutils.Tuples{{1, 1, 1}, {1, 2, 1}, {1, 3, 1}, {2, 4, 4}, {2, 5, 4}, {3, 6, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &firstValueFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 5}, {3, 6}},
				expected: colexectestutils.Tuples{{1, 1, 3}, {1, 2, 3}, {1, 3, 3}, {2, 4, 5}, {2, 5, 5}, {3, 6, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lastValueFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1, 2, 4}, {1, 2, 1}, {1, 2, 2}, {2, 4, 1}, {2, 5, 1}, {3, 6, 1}},
				expected: colexectestutils.Tuples{{1, 2, 4, nil}, {1, 2, 1, 2}, {1, 2, 2, 2}, {2, 4, 1, 4}, {2, 5, 1, 4}, {3, 6, 1, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nthValueFn},
							ArgsIdxs:     []uint32{1, 2},
							OutputColIdx: 3,
						},
					},
				},
			},

			// No PARTITION BY, with ORDER BY.
			{
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 2}, {1, 3}, {1, 4}, {2, 5}, {3, 6}, {3, 7}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 1}, {1, 3}, {1, 3}, {2, 5}, {3, 6}, {3, 6}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 1}, {1, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 4}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {1}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 0}, {nil, 0}, {1, 2.0 / 7}, {1, 2.0 / 7}, {1, 2.0 / 7}, {2, 5.0 / 7}, {3, 6.0 / 7}, {3, 6.0 / 7}},
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
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {1}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 2.0 / 8}, {nil, 2.0 / 8}, {1, 5.0 / 8}, {1, 5.0 / 8}, {1, 5.0 / 8}, {2, 6.0 / 8}, {3, 1.0}, {3, 1.0}},
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
			{
				tuples:   colexectestutils.Tuples{{3, 3}, {1, 3}, {2, 3}, {1, 3}, {nil, 3}, {1, 3}, {nil, 3}, {3, 3}},
				expected: colexectestutils.Tuples{{nil, 3, 1}, {nil, 3, 1}, {1, 3, 1}, {1, 3, 2}, {1, 3, 2}, {2, 3, 2}, {3, 3, 3}, {3, 3, 3}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nTileFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 1, 5, 10}, {1, 2, 1, 10}, {2, 3, 0, 10}, {nil, 4, 5, 10},
					{1, 5, 1, 10}, {1, 6, nil, 10}, {nil, 7, -3, 10}, {3, 8, 1, 10},
				},
				expected: colexectestutils.Tuples{
					{3, 1, 5, 10, 7}, {1, 2, 1, 10, 7}, {2, 3, 0, 10, 3}, {nil, 4, 5, 10, 10},
					{1, 5, 1, 10, 2}, {1, 6, nil, 10, nil}, {nil, 7, -3, 10, 6}, {3, 8, 1, 10, 1},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lagFn},
							ArgsIdxs:     []uint32{1, 2, 3},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}}},
							OutputColIdx: 4,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 1, -5, 10}, {1, 2, 1, 10}, {2, 3, 0, 10}, {nil, 4, 5, 10},
					{1, 5, 1, 10}, {1, 6, nil, 10}, {nil, 7, -3, 10}, {3, 8, -1, 10},
				},
				expected: colexectestutils.Tuples{
					{3, 1, -5, 10, 7}, {1, 2, 1, 10, 5}, {2, 3, 0, 10, 3}, {nil, 4, 5, 10, 3},
					{1, 5, 1, 10, 6}, {1, 6, nil, 10, nil}, {nil, 7, -3, 10, 10}, {3, 8, -1, 10, 1},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &leadFn},
							ArgsIdxs:     []uint32{1, 2, 3},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}}},
							OutputColIdx: 4,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3, 1}, {1, 2}, {2, 3}, {nil, 4}, {1, 5}, {nil, 6}, {3, 7}},
				expected: colexectestutils.Tuples{{nil, 4, 4}, {nil, 6, 4}, {1, 2, 4}, {1, 5, 4}, {2, 3, 4}, {3, 1, 4}, {3, 7, 4}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &firstValueFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3, 1}, {1, 2}, {2, 3}, {nil, 4}, {1, 5}, {nil, 6}, {3, 7}},
				expected: colexectestutils.Tuples{{nil, 4, 6}, {nil, 6, 6}, {1, 2, 5}, {1, 5, 5}, {2, 3, 3}, {3, 1, 7}, {3, 7, 7}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lastValueFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{nil, 4, 1}, {nil, 6, 2}, {1, 2, 2}, {1, 5, 1}, {2, 3, 1}, {3, 1, 8}, {3, 7, 4}},
				expected: colexectestutils.Tuples{{nil, 4, 1, 4}, {nil, 6, 2, 6}, {1, 2, 2, 6}, {1, 5, 1, 4}, {2, 3, 1, 4}, {3, 1, 8, nil}, {3, 7, 4, 5}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nthValueFn},
							ArgsIdxs:     []uint32{1, 2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 3,
						},
					},
				},
			},

			// With both PARTITION BY and ORDER BY.
			{
				tuples:   colexectestutils.Tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
				expected: colexectestutils.Tuples{{nil, nil, 1}, {nil, nil, 2}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
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
				tuples:   colexectestutils.Tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
				expected: colexectestutils.Tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
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
				tuples:   colexectestutils.Tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
				expected: colexectestutils.Tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 2}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
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
				tuples:   colexectestutils.Tuples{{nil, 2}, {3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {1, 3}, {nil, nil}, {3, 1}},
				expected: colexectestutils.Tuples{{nil, nil, 0}, {nil, nil, 0}, {nil, 1, 2.0 / 3}, {nil, 2, 1}, {1, nil, 0}, {1, 2, 1.0 / 2}, {1, 3, 1}, {2, 1, 0}, {3, 1, 0}, {3, 2, 1}},
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
				tuples:   colexectestutils.Tuples{{nil, 2}, {3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {1, 3}, {nil, nil}, {3, 1}},
				expected: colexectestutils.Tuples{{nil, nil, 2.0 / 4}, {nil, nil, 2.0 / 4}, {nil, 1, 3.0 / 4}, {nil, 2, 1}, {1, nil, 1.0 / 3}, {1, 2, 2.0 / 3}, {1, 3, 1}, {2, 1, 1}, {3, 1, 1.0 / 2}, {3, 2, 1}},
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
			{
				tuples:   colexectestutils.Tuples{{nil, 2, 5}, {3, 2, 2}, {1, nil, 2}, {2, 1, nil}, {nil, nil, 5}, {1, 2, 5}, {nil, 1, 5}, {1, 3, 5}, {nil, nil, 5}, {3, 1, 1}},
				expected: colexectestutils.Tuples{{nil, nil, 5, 1}, {nil, nil, 5, 2}, {nil, 1, 5, 3}, {nil, 2, 5, 4}, {1, nil, 2, 1}, {1, 2, 5, 1}, {1, 3, 5, 2}, {2, 1, nil, nil}, {3, 1, 1, 1}, {3, 2, 2, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nTileFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 1, 1, 10}, {1, 2, 1, 10}, {2, 3, 0, 10}, {nil, 4, -1, 10},
					{1, 5, 1, 10}, {1, 6, nil, 10}, {nil, 7, 3, 10}, {3, 8, 1, 10},
				},
				expected: colexectestutils.Tuples{
					{3, 1, 1, 10, 10}, {1, 2, 1, 10, 10}, {2, 3, 0, 10, 3}, {nil, 4, -1, 10, 7},
					{1, 5, 1, 10, 2}, {1, 6, nil, 10, nil}, {nil, 7, 3, 10, 10}, {3, 8, 1, 10, 1},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lagFn},
							ArgsIdxs:     []uint32{1, 2, 3},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 4,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 1, 1, 10}, {1, 2, 1, 10}, {2, 3, 0, 10}, {nil, 4, 3, 10},
					{1, 5, 1, 10}, {1, 6, nil, 10}, {nil, 7, -1, 10}, {3, 8, 1, 10},
				},
				expected: colexectestutils.Tuples{
					{3, 1, 1, 10, 8}, {1, 2, 1, 10, 5}, {2, 3, 0, 10, 3}, {nil, 4, 3, 10, 10},
					{1, 5, 1, 10, 6}, {1, 6, nil, 10, nil}, {nil, 7, -1, 10, 4}, {3, 8, 1, 10, 10},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &leadFn},
							ArgsIdxs:     []uint32{1, 2, 3},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 4,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 2, 1}, {1, nil, 2}, {2, 1, 3}, {nil, nil, 4},
					{1, 2, 5}, {nil, 1, 6}, {nil, nil, 4}, {3, 1, 8},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, 4, 4}, {nil, nil, 4, 4}, {nil, 1, 6, 4}, {1, nil, 2, 2},
					{1, 2, 5, 2}, {2, 1, 3, 3}, {3, 1, 8, 8}, {3, 2, 1, 8},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &firstValueFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{3, 2, 1}, {1, nil, 2}, {2, 1, 3}, {nil, nil, 4},
					{1, 2, 5}, {nil, 1, 6}, {nil, nil, 4}, {3, 1, 8},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, 4, 4}, {nil, nil, 4, 4}, {nil, 1, 6, 6}, {1, nil, 2, 2},
					{1, 2, 5, 5}, {2, 1, 3, 3}, {3, 1, 8, 8}, {3, 2, 1, 1},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lastValueFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{nil, nil, 4, 2}, {nil, nil, 4, 5}, {nil, 1, 6, 1}, {1, nil, 2, 2},
					{1, 2, 5, 1}, {2, 1, 3, 2}, {3, 1, 8, 1}, {3, 2, 1, 2},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, 4, 2, 4}, {nil, nil, 4, 5, nil}, {nil, 1, 6, 1, 4}, {1, nil, 2, 2, nil},
					{1, 2, 5, 1, 2}, {2, 1, 3, 2, nil}, {3, 1, 8, 1, 8}, {3, 2, 1, 2, 1},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nthValueFn},
							ArgsIdxs:     []uint32{2, 3},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 4,
						},
					},
				},
			},

			// With neither PARTITION BY nor ORDER BY.
			{
				tuples:   colexectestutils.Tuples{{1}, {1}, {1}, {2}, {2}, {3}},
				expected: colexectestutils.Tuples{{1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 5}, {3, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 0}, {nil, 0}, {1, 0}, {1, 0}, {2, 0}, {3, 0}, {3, 0}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: colexectestutils.Tuples{{nil, 1.0}, {nil, 1.0}, {1, 1.0}, {1, 1.0}, {2, 1.0}, {3, 1.0}, {3, 1.0}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{3, 3}, {1, 3}, {2, 3}, {nil, 3}, {1, 3}, {1, 3}, {nil, 3}, {3, 3}},
				expected: colexectestutils.Tuples{{3, 3, 1}, {1, 3, 1}, {2, 3, 1}, {nil, 3, 2}, {1, 3, 2}, {1, 3, 2}, {nil, 3, 3}, {3, 3, 3}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nTileFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{1, 5, 10}, {2, 1, 10}, {3, 0, 10}, {4, 3, 10},
					{5, -1, 10}, {nil, nil, 10}, {7, 3, 10}, {8, 1, 10},
				},
				expected: colexectestutils.Tuples{
					{1, 5, 10, 10}, {2, 1, 10, 1}, {3, 0, 10, 3}, {4, 3, 10, 1},
					{5, -1, 10, nil}, {nil, nil, 10, nil}, {7, 3, 10, 4}, {8, 1, 10, 7},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lagFn},
							ArgsIdxs:     []uint32{0, 1, 2},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{1, 5, 10}, {2, 1, 10}, {3, 0, 10}, {4, 3, 10},
					{5, -1, 10}, {nil, nil, 10}, {7, -1, 10}, {8, 1, 10},
				},
				expected: colexectestutils.Tuples{
					{1, 5, 10, nil}, {2, 1, 10, 3}, {3, 0, 10, 3}, {4, 3, 10, 7},
					{5, -1, 10, 4}, {nil, nil, 10, nil}, {7, -1, 10, nil}, {8, 1, 10, 10},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &leadFn},
							ArgsIdxs:     []uint32{0, 1, 2},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}},
				expected: colexectestutils.Tuples{{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &firstValueFn},
							ArgsIdxs:     []uint32{0},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}},
				expected: colexectestutils.Tuples{{1, 6}, {2, 6}, {3, 6}, {4, 6}, {5, 6}, {6, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &lastValueFn},
							ArgsIdxs:     []uint32{0},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1, 1}, {2, 1}, {3, 5}, {4, 7}, {5, 3}, {6, 2}},
				expected: colexectestutils.Tuples{{1, 1, 1}, {2, 1, 1}, {3, 5, 5}, {4, 7, nil}, {5, 3, 3}, {6, 2, 2}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &nthValueFn},
							ArgsIdxs:     []uint32{0, 1},
							OutputColIdx: 2,
						},
					},
				},
			},
		} {
			log.Infof(ctx, "spillForced=%t/%s", spillForced, tc.windowerSpec.WindowFns[0].Func.String())
			var semsToCheck []semaphore.Semaphore
			colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.UnorderedVerifier, func(sources []colexecop.Operator) (colexecop.Operator, error) {
				tc.init()
				ct := make([]*types.T, len(tc.tuples[0]))
				for i := range ct {
					ct[i] = types.Int
				}
				resultType := types.Int
				wf := tc.windowerSpec.WindowFns[0].Func.WindowFunc
				if wf == &percentRankFn || wf == &cumeDistFn {
					resultType = types.Float
				}
				spec := &execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{ColumnTypes: ct}},
					Core: execinfrapb.ProcessorCoreUnion{
						Windower: &tc.windowerSpec,
					},
					ResultTypes: append(ct, resultType),
				}
				// Relative rank operators currently require the most number of
				// FDs.
				sem := colexecop.NewTestingSemaphore(relativeRankNumRequiredFDs)
				args := &colexecargs.NewColOperatorArgs{
					Spec:                spec,
					Inputs:              colexectestutils.MakeInputs(sources),
					StreamingMemAccount: testMemAcc,
					DiskQueueCfg:        queueCfg,
					FDSemaphore:         sem,
				}
				semsToCheck = append(semsToCheck, sem)
				args.TestingKnobs.UseStreamingMemAccountForBuffering = true
				result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
				accounts = append(accounts, result.OpAccounts...)
				monitors = append(monitors, result.OpMonitors...)
				return result.Root, err
			})
			for i, sem := range semsToCheck {
				require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
			}
		}
	}

	for _, acc := range accounts {
		acc.Close(ctx)
	}

	for _, m := range monitors {
		m.Stop(ctx)
	}
}

func BenchmarkWindowFunctions(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	const (
		memLimit        = 64 << 20
		fdLimit         = 3
		partitionSize   = 5
		peerGroupSize   = 3
		arg1ColIdx      = 0
		arg2ColIdx      = 1
		arg3ColIdx      = 2
		partitionColIdx = 3
		orderColIdx     = 4
		peersColIdx     = 5
		numIntCols      = 4
		numBoolCols     = 2
	)

	sourceTypes := []*types.T{
		types.Int, types.Int, types.Int, // Window function arguments
		types.Bool, // Partition column
		types.Int,  // Ordering column
		types.Bool, // Peer groups column
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	benchMemAccount := testMemMonitor.MakeBoundAccount()
	defer benchMemAccount.Close(ctx)

	getWindowFn := func(
		windowFn execinfrapb.WindowerSpec_WindowFunc, source colexecop.Operator, partition, order bool,
	) (op colexecop.Operator) {
		var err error
		outputIdx := len(sourceTypes)
		benchMemAccount.Clear(ctx)
		mainAllocator := colmem.NewAllocator(ctx, &benchMemAccount, testColumnFactory)
		bufferAllocator := colmem.NewAllocator(ctx, &benchMemAccount, testColumnFactory)

		var orderingCols []execinfrapb.Ordering_Column
		partitionCol, peersCol := tree.NoColumnIdx, tree.NoColumnIdx
		if partition {
			partitionCol = partitionColIdx
		}
		if order {
			peersCol = peersColIdx
			orderingCols = append(orderingCols, execinfrapb.Ordering_Column{
				ColIdx:    uint32(orderColIdx),
				Direction: execinfrapb.Ordering_Column_ASC,
			})
		}

		args := &WindowArgs{
			EvalCtx:         &evalCtx,
			MainAllocator:   mainAllocator,
			BufferAllocator: bufferAllocator,
			MemoryLimit:     memLimit,
			QueueCfg:        queueCfg,
			FdSemaphore:     colexecop.NewTestingSemaphore(fdLimit),
			DiskAcc:         testDiskAcc,
			Input:           source,
			InputTypes:      sourceTypes,
			OutputColIdx:    outputIdx,
			PartitionColIdx: partitionCol,
			PeersColIdx:     peersCol,
		}

		switch windowFn {
		case execinfrapb.WindowerSpec_ROW_NUMBER:
			op = NewRowNumberOperator(args)
		case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
			op, err = NewRankOperator(args, windowFn, orderingCols)
		case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
			op, err = NewRelativeRankOperator(args, windowFn, orderingCols)
		case execinfrapb.WindowerSpec_NTILE:
			op = NewNTileOperator(args, arg1ColIdx)
		case execinfrapb.WindowerSpec_LAG:
			op, err = NewLagOperator(args, arg1ColIdx, arg2ColIdx, arg3ColIdx)
		case execinfrapb.WindowerSpec_LEAD:
			op, err = NewLeadOperator(args, arg1ColIdx, arg2ColIdx, arg3ColIdx)
		case execinfrapb.WindowerSpec_FIRST_VALUE:
			op, err = NewFirstValueOperator(args, NormalizeWindowFrame(nil),
				&execinfrapb.Ordering{Columns: orderingCols}, []int{arg1ColIdx})
		case execinfrapb.WindowerSpec_LAST_VALUE:
			op, err = NewLastValueOperator(args, NormalizeWindowFrame(nil),
				&execinfrapb.Ordering{Columns: orderingCols}, []int{arg1ColIdx})
		case execinfrapb.WindowerSpec_NTH_VALUE:
			op, err = NewNthValueOperator(args, NormalizeWindowFrame(nil),
				&execinfrapb.Ordering{Columns: orderingCols}, []int{arg1ColIdx, arg2ColIdx})
		}
		require.NoError(b, err)
		return op
	}

	var batch coldata.Batch
	batchCreator := func(batchLength int) coldata.Batch {
		const arg1Offset = 5
		batch, _ = testAllocator.ResetMaybeReallocate(sourceTypes, batch, batchLength, math.MaxInt64)
		argCol1 := batch.ColVec(arg1ColIdx).Int64()
		argCol2 := batch.ColVec(arg2ColIdx).Int64()
		partitionCol := batch.ColVec(partitionColIdx).Bool()
		orderCol := batch.ColVec(orderColIdx).Int64()
		peersCol := batch.ColVec(peersColIdx).Bool()
		for i := 0; i < batchLength; i++ {
			argCol1[i] = int64(i + arg1Offset)
			argCol2[i] = 1
			partitionCol[i] = i%partitionSize == 0
			orderCol[i] = int64(i / peerGroupSize)
			peersCol[i] = i%peerGroupSize == 0
		}
		batch.ColVec(arg1ColIdx).Nulls().UnsetNulls()
		batch.ColVec(arg2ColIdx).Nulls().UnsetNulls()
		batch.ColVec(arg3ColIdx).Nulls().SetNulls()
		batch.ColVec(partitionColIdx).Nulls().UnsetNulls()
		batch.ColVec(orderColIdx).Nulls().UnsetNulls()
		batch.ColVec(peersColIdx).Nulls().UnsetNulls()
		batch.SetLength(batchLength)
		return batch
	}

	// The number of rows should be a multiple of coldata.BatchSize().
	rowsOptions := []int{4 * coldata.BatchSize(), 32 * coldata.BatchSize()}

	for windowFnIdx := 0; windowFnIdx < len(execinfrapb.WindowerSpec_WindowFunc_name); windowFnIdx++ {
		windowFn := execinfrapb.WindowerSpec_WindowFunc(windowFnIdx)
		b.Run(fmt.Sprintf("%v", windowFn), func(b *testing.B) {
			for _, nRows := range rowsOptions {
				b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
					nBatches := nRows / coldata.BatchSize()
					batch := batchCreator(coldata.BatchSize())
					for _, partitionInput := range []bool{true, false} {
						b.Run(fmt.Sprintf("partition=%v", partitionInput), func(b *testing.B) {
							for _, orderInput := range []bool{true, false} {
								b.Run(fmt.Sprintf("order=%v", orderInput), func(b *testing.B) {
									// Account only for the argument columns as
									// well as the output column. All other
									// columns are internal and should be
									// ignored.
									numArgs := windowFnMaxNumArgs[windowFn]
									b.SetBytes(int64(nRows * 8 * (numArgs + 1)))
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										source := colexectestutils.NewFiniteChunksSource(
											testAllocator, batch, sourceTypes, nBatches, 1,
										)
										s := getWindowFn(windowFn, source, partitionInput, orderInput)
										s.Init(ctx)
										b.StartTimer()
										for b := s.Next(); b.Length() != 0; b = s.Next() {
										}
										b.StopTimer()
									}
								})
							}
						})
					}
				})
			}
		})
	}
}
