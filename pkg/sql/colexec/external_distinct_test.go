// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		accounts []*mon.BoundAccount
		monitors []*mon.BytesMonitor
	)
	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for tcIdx, tc := range distinctTestCases {
			log.Infof(context.Background(), "spillForced=%t/%d", spillForced, tcIdx)
			var semsToCheck []semaphore.Semaphore
			runTestsWithTyps(
				t,
				[]tuples{tc.tuples},
				[][]*types.T{tc.typs},
				tc.expected,
				// We're using an unordered verifier because the in-memory
				// unordered distinct is free to change the order of the tuples
				// when exporting them into an external distinct.
				unorderedVerifier,
				func(input []colexecbase.Operator) (colexecbase.Operator, error) {
					// A sorter should never exceed ExternalSorterMinPartitions, even
					// during repartitioning. A panic will happen if a sorter requests
					// more than this number of file descriptors.
					sem := colexecbase.NewTestingSemaphore(ExternalSorterMinPartitions)
					semsToCheck = append(semsToCheck, sem)
					var outputOrdering execinfrapb.Ordering
					if tc.isOrderedOnDistinctCols {
						outputOrdering = convertDistinctColsToOrdering(tc.distinctCols)
					}
					distinct, newAccounts, newMonitors, closers, err := createExternalDistinct(
						ctx, flowCtx, input, tc.typs, tc.distinctCols, outputOrdering, queueCfg, sem,
					)
					// Check that the external distinct and the disk-backed sort
					// were added as Closers.
					numExpectedClosers := 2
					if len(outputOrdering.Columns) > 0 {
						// The final disk-backed sort must also be added as a
						// Closer.
						numExpectedClosers++
					}
					require.Equal(t, numExpectedClosers, len(closers))
					accounts = append(accounts, newAccounts...)
					monitors = append(monitors, newMonitors...)
					return distinct, err
				},
			)
			for i, sem := range semsToCheck {
				require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
			}
		}
	}
	for _, acc := range accounts {
		acc.Close(ctx)
	}
	for _, mon := range monitors {
		mon.Stop(ctx)
	}
}

func convertDistinctColsToOrdering(distinctCols []uint32) execinfrapb.Ordering {
	var ordering execinfrapb.Ordering
	for _, colIdx := range distinctCols {
		ordering.Columns = append(ordering.Columns, execinfrapb.Ordering_Column{ColIdx: colIdx})
	}
	return ordering
}

func BenchmarkExternalDistinct(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
	}
	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()

	for _, spillForced := range []bool{false, true} {
		for _, maintainOrdering := range []bool{false, true} {
			if !spillForced && maintainOrdering {
				// The in-memory unordered distinct maintains the input ordering
				// by design, so it's not an interesting case to test it with
				// both options for 'maintainOrdering' parameter, and we skip
				// one.
				continue
			}
			flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
			name := fmt.Sprintf("spilled=%t/ordering=%t", spillForced, maintainOrdering)
			runDistinctBenchmarks(
				ctx,
				b,
				func(allocator *colmem.Allocator, input colexecbase.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecbase.Operator, error) {
					var outputOrdering execinfrapb.Ordering
					if maintainOrdering {
						outputOrdering = convertDistinctColsToOrdering(distinctCols)
					}
					op, accs, mons, _, err := createExternalDistinct(
						ctx, flowCtx, []colexecbase.Operator{input}, typs,
						distinctCols, outputOrdering, queueCfg, &colexecbase.TestingSemaphore{},
					)
					memAccounts = append(memAccounts, accs...)
					memMonitors = append(memMonitors, mons...)
					return op, err
				},
				func(nCols int) int {
					return 0
				},
				name,
				true, /* isExternal */
			)
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

// createExternalDistinct is a helper function that instantiates a disk-backed
// distinct operator. It returns an operator and an error as well as memory
// monitors and memory accounts that will need to be closed once the caller is
// done with the operator.
func createExternalDistinct(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input []colexecbase.Operator,
	typs []*types.T,
	distinctCols []uint32,
	outputOrdering execinfrapb.Ordering,
	diskQueueCfg colcontainer.DiskQueueCfg,
	testingSemaphore semaphore.Semaphore,
) (colexecbase.Operator, []*mon.BoundAccount, []*mon.BytesMonitor, []colexecbase.Closer, error) {
	distinctSpec := &execinfrapb.DistinctSpec{
		DistinctColumns: distinctCols,
		OutputOrdering:  outputOrdering,
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: typs}},
		Core: execinfrapb.ProcessorCoreUnion{
			Distinct: distinctSpec,
		},
		Post:        execinfrapb.PostProcessSpec{},
		ResultTypes: typs,
	}
	args := &NewColOperatorArgs{
		Spec:                spec,
		Inputs:              input,
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
	}
	// External sorter relies on different memory accounts to
	// understand when to start a new partition, so we will not use
	// the streaming memory account.
	result, err := TestNewColOperator(ctx, flowCtx, args)
	return result.Op, result.OpAccounts, result.OpMonitors, result.ToClose, err
}
