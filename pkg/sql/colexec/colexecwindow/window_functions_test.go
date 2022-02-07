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
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
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
	semaCtx := tree.MakeSemaContext()
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)

	dec := func(val string) apd.Decimal {
		res, _, err := apd.NewFromString(val)
		require.NoError(t, err)
		return *res
	}

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

	// Because aggregate window functions are all executed by the same operator,
	// we only test a few representative aggregates.
	sumFn := execinfrapb.AggregatorSpec_SUM
	countFn := execinfrapb.AggregatorSpec_COUNT
	avgFn := execinfrapb.AggregatorSpec_AVG
	maxFn := execinfrapb.AggregatorSpec_MAX

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
			{
				tuples: colexectestutils.Tuples{{1, 7}, {1, 3}, {1, -5}, {2, nil}, {2, 0}, {3, 6}},
				expected: colexectestutils.Tuples{
					{1, 7, dec("5")}, {1, 3, dec("5")}, {1, -5, dec("5")},
					{2, nil, dec("0")}, {2, 0, dec("0")}, {3, 6, dec("6")},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &sumFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1, 7}, {1, 3}, {1, -5}, {2, nil}, {2, 0}, {3, 6}},
				expected: colexectestutils.Tuples{{1, 7, 3}, {1, 3, 3}, {1, -5, 3}, {2, nil, 1}, {2, 0, 1}, {3, 6, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &countFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{{1, 7}, {1, 3}, {1, -5}, {2, nil}, {2, 0}, {3, 6}},
				expected: colexectestutils.Tuples{
					{1, 7, dec("1.6666666666666666667")}, {1, 3, dec("1.6666666666666666667")},
					{1, -5, dec("1.6666666666666666667")}, {2, nil, dec("0")},
					{2, 0, dec("0")}, {3, 6, dec("6.0000000000000000000")},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &avgFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1, 7}, {1, 3}, {1, -5}, {2, nil}, {2, 0}, {3, 6}},
				expected: colexectestutils.Tuples{{1, 7, 7}, {1, 3, 7}, {1, -5, 7}, {2, nil, 0}, {2, 0, 0}, {3, 6, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &maxFn},
							ArgsIdxs:     []uint32{1},
							OutputColIdx: 2,
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
			{
				tuples: colexectestutils.Tuples{{nil, 4}, {nil, -6}, {1, 2}, {1, nil}, {2, -3}, {3, 1}, {3, 7}},
				expected: colexectestutils.Tuples{
					{nil, 4, dec("-2")}, {nil, -6, dec("-2")}, {1, 2, dec("0")},
					{1, nil, dec("0")}, {2, -3, dec("-3")}, {3, 1, dec("5")}, {3, 7, dec("5")}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &sumFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{nil, 4}, {nil, -6}, {1, 2}, {1, nil}, {2, -3}, {3, 1}, {3, 7}},
				expected: colexectestutils.Tuples{{nil, 4, 2}, {nil, -6, 2}, {1, 2, 3}, {1, nil, 3}, {2, -3, 4}, {3, 1, 6}, {3, 7, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &countFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{{nil, 4}, {nil, -6}, {1, 2}, {1, nil}, {2, -3}, {3, 1}, {3, 7}},
				expected: colexectestutils.Tuples{
					{nil, 4, dec("-1.0000000000000000000")}, {nil, -6, dec("-1.0000000000000000000")},
					{1, 2, dec("0")}, {1, nil, dec("0")}, {2, -3, dec("-0.75000000000000000000")},
					{3, 1, dec("0.83333333333333333333")}, {3, 7, dec("0.83333333333333333333")}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &avgFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{nil, 4}, {nil, -6}, {1, 2}, {1, nil}, {2, -3}, {3, 1}, {3, 7}},
				expected: colexectestutils.Tuples{{nil, 4, 4}, {nil, -6, 4}, {1, 2, 4}, {1, nil, 4}, {2, -3, 4}, {3, 1, 7}, {3, 7, 7}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &maxFn},
							ArgsIdxs:     []uint32{1},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 2,
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
			{
				tuples: colexectestutils.Tuples{
					{nil, nil, -10}, {nil, nil, 4}, {nil, 1, 6}, {1, nil, 2},
					{1, 2, 5}, {2, 1, nil}, {3, 1, 8}, {3, 2, 1},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, -10, dec("-6")}, {nil, nil, 4, dec("-6")}, {nil, 1, 6, dec("0")},
					{1, nil, 2, dec("2")}, {1, 2, 5, dec("7")}, {2, 1, nil, nil},
					{3, 1, 8, dec("8")}, {3, 2, 1, dec("9")},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &sumFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{nil, nil, -10}, {nil, nil, 4}, {nil, 1, 6}, {1, nil, 2},
					{1, 2, 5}, {2, 1, nil}, {3, 1, 8}, {3, 2, 1},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, -10, 2}, {nil, nil, 4, 2}, {nil, 1, 6, 3}, {1, nil, 2, 1},
					{1, 2, 5, 2}, {2, 1, nil, 0}, {3, 1, 8, 1}, {3, 2, 1, 2},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &countFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{nil, nil, -10}, {nil, nil, 4}, {nil, 1, 6}, {1, nil, 2},
					{1, 2, 5}, {2, 1, nil}, {3, 1, 8}, {3, 2, 1},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, -10, dec("-3.0000000000000000000")}, {nil, nil, 4, dec("-3.0000000000000000000")},
					{nil, 1, 6, dec("0")}, {1, nil, 2, dec("2.0000000000000000000")},
					{1, 2, 5, dec("3.5000000000000000000")}, {2, 1, nil, nil},
					{3, 1, 8, dec("8.0000000000000000000")}, {3, 2, 1, dec("4.5000000000000000000")},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &avgFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{
					{nil, nil, -10}, {nil, nil, 4}, {nil, 1, 6}, {1, nil, 2},
					{1, 2, 5}, {2, 1, nil}, {3, 1, 8}, {3, 2, 1},
				},
				expected: colexectestutils.Tuples{
					{nil, nil, -10, 4}, {nil, nil, 4, 4}, {nil, 1, 6, 6}, {1, nil, 2, 2},
					{1, 2, 5, 5}, {2, 1, nil, nil}, {3, 1, 8, 8}, {3, 2, 1, 8},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &maxFn},
							ArgsIdxs:     []uint32{2},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 3,
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
			{
				tuples: colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}},
				expected: colexectestutils.Tuples{
					{1, dec("21")}, {2, dec("21")}, {3, dec("21")},
					{4, dec("21")}, {5, dec("21")}, {6, dec("21")},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &sumFn},
							ArgsIdxs:     []uint32{0},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1}, {2}, {nil}, {4}, {nil}, {6}},
				expected: colexectestutils.Tuples{{1, 4}, {2, 4}, {nil, 4}, {4, 4}, {nil, 4}, {6, 4}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &countFn},
							ArgsIdxs:     []uint32{0},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples: colexectestutils.Tuples{{1}, {2}, {nil}, {4}, {nil}, {6}},
				expected: colexectestutils.Tuples{
					{1, dec("3.2500000000000000000")}, {2, dec("3.2500000000000000000")},
					{nil, dec("3.2500000000000000000")}, {4, dec("3.2500000000000000000")},
					{nil, dec("3.2500000000000000000")}, {6, dec("3.2500000000000000000")},
				},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &avgFn},
							ArgsIdxs:     []uint32{0},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   colexectestutils.Tuples{{1}, {2}, {nil}, {4}, {nil}, {6}},
				expected: colexectestutils.Tuples{{1, 6}, {2, 6}, {nil, 6}, {4, 6}, {nil, 6}, {6, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &maxFn},
							ArgsIdxs:     []uint32{0},
							OutputColIdx: 1,
						},
					},
				},
			},
		} {
			log.Infof(ctx, "spillForced=%t/%s", spillForced, tc.windowerSpec.WindowFns[0].Func.String())
			var toClose []colexecop.Closers
			var semsToCheck []semaphore.Semaphore
			colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.UnorderedVerifier, func(sources []colexecop.Operator) (colexecop.Operator, error) {
				tc.init()
				ct := make([]*types.T, len(tc.tuples[0]))
				for i := range ct {
					ct[i] = types.Int
				}
				resultType := types.Int
				fun := tc.windowerSpec.WindowFns[0].Func
				if fun.WindowFunc != nil {
					if fun.WindowFunc == &percentRankFn || fun.WindowFunc == &cumeDistFn {
						resultType = types.Float
					}
				} else {
					argIdxs := tc.windowerSpec.WindowFns[0].ArgsIdxs
					aggregations := []execinfrapb.AggregatorSpec_Aggregation{{
						Func:   *fun.AggregateFunc,
						ColIdx: argIdxs,
					}}
					_, _, outputTypes, err :=
						colexecagg.ProcessAggregations(&evalCtx, &semaCtx, aggregations, ct)
					require.NoError(t, err)
					resultType = outputTypes[0]
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
					MonitorRegistry:     &monitorRegistry,
				}
				semsToCheck = append(semsToCheck, sem)
				result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
				toClose = append(toClose, result.ToClose)
				return result.Root, err
			})
			// Close all closers manually (in production this is done on the
			// flow cleanup).
			for _, c := range toClose {
				require.NoError(t, c.Close(ctx))
			}
			for i, sem := range semsToCheck {
				require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
			}
		}
	}
}

func BenchmarkWindowFunctions(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	semaCtx := tree.MakeSemaContext()

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
		fun execinfrapb.WindowerSpec_Func, source colexecop.Operator, partition, order bool,
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

		if fun.WindowFunc != nil {
			switch *fun.WindowFunc {
			case execinfrapb.WindowerSpec_ROW_NUMBER:
				op = NewRowNumberOperator(args)
			case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
				op, err = NewRankOperator(args, *fun.WindowFunc, orderingCols)
			case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
				op, err = NewRelativeRankOperator(args, *fun.WindowFunc, orderingCols)
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
		} else if fun.AggregateFunc != nil {
			var argIdxs []int
			switch *fun.AggregateFunc {
			case execinfrapb.CountRows:
				// CountRows has a specialized implementation.
				return NewCountRowsOperator(
					args,
					NormalizeWindowFrame(nil),
					&execinfrapb.Ordering{Columns: orderingCols},
				)
			default:
				// Supported aggregate functions other than CountRows take one argument.
				argIdxs = []int{arg1ColIdx}
			}
			colIdxs := make([]uint32, len(argIdxs))
			for i, idx := range argIdxs {
				colIdxs[i] = uint32(idx)
			}
			aggArgs := colexecagg.NewAggregatorArgs{
				Allocator:  mainAllocator,
				InputTypes: sourceTypes,
				EvalCtx:    &evalCtx,
			}
			aggregations := []execinfrapb.AggregatorSpec_Aggregation{{
				Func:   *fun.AggregateFunc,
				ColIdx: colIdxs,
			}}
			aggArgs.Constructors, aggArgs.ConstArguments, aggArgs.OutputTypes, err =
				colexecagg.ProcessAggregations(&evalCtx, &semaCtx, aggregations, sourceTypes)
			require.NoError(b, err)
			aggFnsAlloc, _, toClose, err := colexecagg.NewAggregateFuncsAlloc(
				&aggArgs, aggregations, 1 /* allocSize */, colexecagg.WindowAggKind,
			)
			require.NoError(b, err)
			op = NewWindowAggregatorOperator(
				args, *fun.AggregateFunc, NormalizeWindowFrame(nil),
				&execinfrapb.Ordering{Columns: orderingCols}, []int{arg1ColIdx},
				aggArgs.OutputTypes[0], aggFnsAlloc, toClose)
		} else {
			require.Fail(b, "expected non-nil window function")
		}
		require.NoError(b, err)
		return op
	}

	inputCreator := func(length int) []coldata.Vec {
		const arg1Offset, arg1Range = 5, 10
		vecs := make([]coldata.Vec, len(sourceTypes))
		for i := range vecs {
			vecs[i] = testAllocator.NewMemColumn(sourceTypes[i], length)
		}
		argCol1 := vecs[arg1ColIdx].Int64()
		argCol2 := vecs[arg2ColIdx].Int64()
		partitionCol := vecs[partitionColIdx].Bool()
		orderCol := vecs[orderColIdx].Int64()
		peersCol := vecs[peersColIdx].Bool()
		for i := 0; i < length; i++ {
			argCol1[i] = int64(1 + (i+arg1Offset)%arg1Range)
			argCol2[i] = 1
			partitionCol[i] = i%partitionSize == 0
			orderCol[i] = int64(i / peerGroupSize)
			peersCol[i] = i%peerGroupSize == 0
		}
		vecs[arg3ColIdx].Nulls().SetNulls()
		return vecs
	}

	// The number of rows should be a multiple of coldata.BatchSize().
	rowsOptions := []int{4 * coldata.BatchSize(), 32 * coldata.BatchSize()}
	if testing.Short() {
		rowsOptions = []int{4 * coldata.BatchSize()}
	}

	runBench := func(fun execinfrapb.WindowerSpec_Func, fnName string, numArgs int) {
		b.Run(fnName, func(b *testing.B) {
			for _, nRows := range rowsOptions {
				if !isWindowFnLinear(fun) && nRows == 32*coldata.BatchSize() {
					// Skip functions that scale poorly for the larger row size.
					continue
				}
				b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
					vecs := inputCreator(nRows)
					for _, partitionInput := range []bool{true, false} {
						b.Run(fmt.Sprintf("partition=%v", partitionInput), func(b *testing.B) {
							for _, orderInput := range []bool{true, false} {
								b.Run(fmt.Sprintf("order=%v", orderInput), func(b *testing.B) {
									// Account only for the argument columns as
									// well as the output column. All other
									// columns are internal and should be
									// ignored.
									// TODO(drewk): the type isn't always int.
									b.SetBytes(int64(nRows * 8 * (numArgs + 1)))
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										source := colexectestutils.NewChunkingBatchSource(
											testAllocator, sourceTypes, vecs, nRows,
										)
										s := getWindowFn(fun, source, partitionInput, orderInput)
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

	for windowFnIdx := 0; windowFnIdx < len(execinfrapb.WindowerSpec_WindowFunc_name); windowFnIdx++ {
		windowFn := execinfrapb.WindowerSpec_WindowFunc(windowFnIdx)
		numArgs := windowFnMaxNumArgs[windowFn]
		runBench(execinfrapb.WindowerSpec_Func{WindowFunc: &windowFn}, windowFn.String(), numArgs)
	}

	// We need <= because an entry for index=6 was omitted by mistake.
	for aggFnIdx := 0; aggFnIdx <= len(execinfrapb.AggregatorSpec_Func_name); aggFnIdx++ {
		aggFn := execinfrapb.AggregatorSpec_Func(aggFnIdx)
		if !colexecagg.IsAggOptimized(aggFn) {
			continue
		}
		switch aggFn {
		case execinfrapb.AnyNotNull, execinfrapb.BoolAnd, execinfrapb.BoolOr, execinfrapb.ConcatAgg:
			// Skip AnyNotNull because it is not a valid window function, and the
			// other three in order to avoid handling non-integer arguments.
			continue
		}
		// Of the supported aggregate functions, only count_rows has zero arguments.
		// The rest take one argument.
		var numArgs int
		if aggFn != execinfrapb.CountRows {
			numArgs = 1
		}
		runBench(execinfrapb.WindowerSpec_Func{AggregateFunc: &aggFn}, aggFn.String(), numArgs)
	}
}
