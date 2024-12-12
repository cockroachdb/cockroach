// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type groupJoinTestCase struct {
	description       string
	jtc               joinTestCase
	joinOutProjection []uint32
	atc               aggregatorTestCase
	outputTypes       []*types.T
}

func TestHashGroupJoiner(t *testing.T) {
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
		DiskMonitor: testDiskMonitor,
	}
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)
	var closerRegistry colexecargs.CloserRegistry
	defer closerRegistry.Close(ctx)

	tcs := []groupJoinTestCase{
		{
			description: "inner join without projection",
			jtc: joinTestCase{
				joinType:     descpb.InnerJoin,
				leftTuples:   colexectestutils.Tuples{{1, 1}, {2, 3}, {4, 4}, {1, 2}},
				rightTuples:  colexectestutils.Tuples{{1, -1}, {3, -6}, {2, -5}, {2, nil}, {1, -2}, {2, -3}},
				leftTypes:    types.TwoIntCols,
				rightTypes:   types.TwoIntCols,
				leftOutCols:  []uint32{0, 1},
				rightOutCols: []uint32{0, 1},
				leftEqCols:   []uint32{0},
				rightEqCols:  []uint32{0},
			},
			atc: aggregatorTestCase{
				typs: []*types.T{types.Int, types.Int, types.Int, types.Int},
				aggFns: []execinfrapb.AggregatorSpec_Func{
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_SUM_INT,
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_MIN,
				},
				groupCols: []uint32{0},
				aggCols:   [][]uint32{{0}, {1}, {2}, {3}},
				expected:  colexectestutils.Tuples{{1, 6, 1, -2}, {2, 9, 2, -5}},
			},
			outputTypes: []*types.T{types.Int, types.Int, types.Int, types.Int},
		},
		{
			description: "inner join with projection",
			jtc: joinTestCase{
				joinType:     descpb.InnerJoin,
				leftTuples:   colexectestutils.Tuples{{2, 3}, {1, 1}, {4, 4}, {1, 2}},
				rightTuples:  colexectestutils.Tuples{{1, -2}, {2, -3}, {1, -1}, {3, -6}, {2, -5}, {2, nil}},
				leftTypes:    types.TwoIntCols,
				rightTypes:   types.TwoIntCols,
				leftOutCols:  []uint32{0, 1},
				rightOutCols: []uint32{1},
				leftEqCols:   []uint32{0},
				rightEqCols:  []uint32{0},
			},
			joinOutProjection: []uint32{0, 1, 3},
			atc: aggregatorTestCase{
				typs: types.ThreeIntCols,
				aggFns: []execinfrapb.AggregatorSpec_Func{
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_MAX,
					execinfrapb.AggregatorSpec_COUNT,
				},
				groupCols: []uint32{0},
				aggCols:   [][]uint32{{0}, {1}, {2}},
				expected:  colexectestutils.Tuples{{1, 2, 4}, {2, 3, 2}},
			},
			outputTypes: types.ThreeIntCols,
		},
		{
			description: "right outer join",
			jtc: joinTestCase{
				joinType:     descpb.RightOuterJoin,
				leftTuples:   colexectestutils.Tuples{{2, 3}, {1, 1}, {4, 4}, {1, 2}},
				rightTuples:  colexectestutils.Tuples{{1, -2}, {3, -7}, {2, -3}, {1, -1}, {3, -6}, {0, nil}, {2, -5}, {2, nil}},
				leftTypes:    types.TwoIntCols,
				rightTypes:   types.TwoIntCols,
				leftOutCols:  []uint32{1},
				rightOutCols: []uint32{0, 1},
				leftEqCols:   []uint32{0},
				rightEqCols:  []uint32{0},
			},
			joinOutProjection: []uint32{2, 1, 3},
			atc: aggregatorTestCase{
				typs: types.ThreeIntCols,
				aggFns: []execinfrapb.AggregatorSpec_Func{
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_MAX,
					execinfrapb.AggregatorSpec_COUNT,
				},
				groupCols: []uint32{0},
				aggCols:   [][]uint32{{0}, {1}, {2}},
				expected:  colexectestutils.Tuples{{0, nil, 0}, {1, 2, 4}, {2, 3, 2}, {3, nil, 2}},
			},
			outputTypes: types.ThreeIntCols,
		},
	}

	for _, spillForced := range []bool{false, true} {
		// TODO(yuzefovich): consider adding one run with random low workmem
		// limit so that OOM is triggered not on the first allocation.
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, tc := range tcs {
			var suffix string
			if spillForced {
				suffix = ", spill forced"
			}
			log.Infof(ctx, "%s%s", tc.description, suffix)
			var spilled bool
			var numRuns int
			colexectestutils.RunTests(
				t, testAllocator, []colexectestutils.Tuples{tc.jtc.leftTuples, tc.jtc.rightTuples}, tc.atc.expected, colexectestutils.UnorderedVerifier,
				func(inputs []colexecop.Operator) (colexecop.Operator, error) {
					numRuns++
					return createDiskBackedHashGroupJoiner(
						ctx, flowCtx, tc, inputs, func() { spilled = true },
						queueCfg, &monitorRegistry, &closerRegistry,
					)
				},
			)
			require.Equal(t, spillForced, spilled)
			// We always add the in-memory hash group joiner and the disk
			// spiller for the hash group join into the closers.
			numExpectedClosers := 2
			if spilled {
				// If we spill, then we also add the following five closers:
				// - the hash-based partitioner (1) for the external hash join
				// - the disk spiller (2), (3) for each disk-backed sort on top
				// of the inputs to the merge join used in the fallback strategy
				// in the external hash join
				// - the hash-based partitioner (4) for the external hash
				// aggregator
				// - the disk spiller (5) for the disk-backed sort used in the
				// external hash aggregator.
				numExpectedClosers += 5
			}
			require.Equal(t, numExpectedClosers*numRuns, closerRegistry.NumClosers())
			closerRegistry.Close(ctx)
			closerRegistry.Reset()
		}
	}
}

func createDiskBackedHashGroupJoiner(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	tc groupJoinTestCase,
	inputs []colexecop.Operator,
	spillingCallbackFn func(),
	diskQueueCfg colcontainer.DiskQueueCfg,
	monitorRegistry *colexecargs.MonitorRegistry,
	closerRegistry *colexecargs.CloserRegistry,
) (colexecop.Operator, error) {
	tc.jtc.init()
	hjSpec := createSpecForHashJoiner(&tc.jtc)
	tc.atc.unorderedInput = true
	if err := tc.atc.init(); err != nil {
		colexecerror.InternalError(err)
	}
	hgjSpec := execinfrapb.HashGroupJoinerSpec{
		HashJoinerSpec:    *hjSpec.Core.HashJoiner,
		JoinOutputColumns: tc.joinOutProjection,
		AggregatorSpec:    *tc.atc.spec,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec: &execinfrapb.ProcessorSpec{
			Input:       hjSpec.Input,
			Core:        execinfrapb.ProcessorCoreUnion{HashGroupJoiner: &hgjSpec},
			ResultTypes: tc.outputTypes,
		},
		Inputs:          colexectestutils.MakeInputs(inputs),
		DiskQueueCfg:    diskQueueCfg,
		FDSemaphore:     &colexecop.TestingSemaphore{},
		MonitorRegistry: monitorRegistry,
		CloserRegistry:  closerRegistry,
	}
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, err
}
