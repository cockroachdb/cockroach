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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalSort(t *testing.T) {
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

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		if spillForced {
			// In order to increase test coverage of recursive merging, we have the
			// lowest possible memory limit - this will force creating partitions
			// consisting of a single batch.
			flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 1
		} else {
			flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 0
		}
		for _, tcs := range [][]sortTestCase{sortAllTestCases, topKSortTestCases, sortChunksTestCases} {
			for _, tc := range tcs {
				t.Run(fmt.Sprintf("spillForced=%t/%s", spillForced, tc.description), func(t *testing.T) {
					// Unfortunately, there is currently no better way to check that a
					// sorter does not have leftover file descriptors other than appending
					// each semaphore used to this slice on construction. This is because
					// some tests don't fully drain the input, making intercepting the
					// sorter.Close() method not a useful option, since it is impossible
					// to check between an expected case where more than 0 FDs are open
					// (e.g. in verifySelAndNullResets, where the sorter is not fully
					// drained so Close must be called explicitly) and an unexpected one.
					// These cases happen during normal execution when a limit is
					// satisfied, but flows will call Close explicitly on Cleanup.
					// TODO(asubiotto): Not implemented yet, currently we rely on the
					//  flow tracking open FDs and releasing any leftovers.
					var semsToCheck []semaphore.Semaphore
					runTests(
						t,
						[]tuples{tc.tuples},
						tc.expected,
						orderedVerifier,
						func(input []Operator) (Operator, error) {
							// A sorter should never exceed externalSorterMinPartitions, even
							// during repartitioning. A panic will happen if a sorter requests
							// more than this number of file descriptors.
							sem := NewTestingSemaphore(externalSorterMinPartitions)
							// If a limit is satisfied before the sorter is drained of all its
							// tuples, the sorter will not close its partitioner. During a
							// flow this will happen in Cleanup, since there is no way to tell
							// an operator that Next won't be called again.
							if tc.k == 0 || int(tc.k) >= len(tc.tuples) {
								semsToCheck = append(semsToCheck, sem)
							}
							sorter, accounts, monitors, err := createDiskBackedSorter(
								ctx, flowCtx, input, tc.logTypes, tc.ordCols, tc.matchLen, tc.k, func() {},
								externalSorterMinPartitions, false /* delegateFDAcquisition */, queueCfg, sem,
							)
							memAccounts = append(memAccounts, accounts...)
							memMonitors = append(memMonitors, monitors...)
							return sorter, err
						})
					for i, sem := range semsToCheck {
						require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
					}
				})
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

func TestExternalSortRandomized(t *testing.T) {
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
	rng, _ := randutil.NewPseudoRand()
	nTups := coldata.BatchSize()*4 + 1
	maxCols := 2
	// TODO(yuzefovich): randomize types as well.
	logTypes := make([]types.T, maxCols)
	for i := range logTypes {
		logTypes[i] = *types.Int
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	// Interesting disk spilling scenarios:
	// 1) The sorter is forced to spill to disk as soon as possible.
	// 2) The memory limit is dynamically set to repartition twice, this will also
	//    allow the in-memory sorter to spool several batches before hitting the
	//    memory limit.
	colTyps, err := typeconv.FromColumnTypes(logTypes)
	require.NoError(t, err)
	// memoryToSort is the total amount of memory that will be sorted in this
	// test.
	memoryToSort := (nTups / coldata.BatchSize()) * estimateBatchSizeBytes(colTyps, coldata.BatchSize())
	// partitionSize will be the memory limit passed in to tests with a memory
	// limit. With a maximum number of partitions of 2 this will result in
	// repartitioning twice. To make this a total amount of memory, we also need
	// to add the cache sizes of the queues.
	partitionSize := int64(memoryToSort/4) + int64(externalSorterMinPartitions*queueCfg.BufferSizeBytes)
	for _, tk := range []execinfra.TestingKnobs{{ForceDiskSpill: true}, {MemoryLimitBytes: partitionSize}} {
		flowCtx.Cfg.TestingKnobs = tk
		for nCols := 1; nCols <= maxCols; nCols++ {
			for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
				namePrefix := "MemoryLimit=" + humanizeutil.IBytes(tk.MemoryLimitBytes)
				if tk.ForceDiskSpill {
					namePrefix = "ForceDiskSpill=true"
				}
				delegateFDAcquisition := rng.Float64() < 0.5
				name := fmt.Sprintf("%s/nCols=%d/nOrderingCols=%d/delegateFDAcquisition=%t", namePrefix, nCols, nOrderingCols, delegateFDAcquisition)
				t.Run(name, func(t *testing.T) {
					// Unfortunately, there is currently no better way to check that a
					// sorter does not have leftover file descriptors other than appending
					// each semaphore used to this slice on construction. This is because
					// some tests don't fully drain the input, making intercepting the
					// sorter.Close() method not a useful option, since it is impossible
					// to check between an expected case where more than 0 FDs are open
					// (e.g. in verifySelAndNullResets, where the sorter is not fully
					// drained so Close must be called explicitly) and an unexpected one.
					// These cases happen during normal execution when a limit is
					// satisfied, but flows will call Close explicitly on Cleanup.
					// TODO(asubiotto): Not implemented yet, currently we rely on the
					//  flow tracking open FDs and releasing any leftovers.
					var semsToCheck []semaphore.Semaphore
					tups, expected, ordCols := generateRandomDataForTestSort(rng, nTups, nCols, nOrderingCols)
					runTests(
						t,
						[]tuples{tups},
						expected,
						orderedVerifier,
						func(input []Operator) (Operator, error) {
							sem := NewTestingSemaphore(externalSorterMinPartitions)
							semsToCheck = append(semsToCheck, sem)
							sorter, accounts, monitors, err := createDiskBackedSorter(
								ctx, flowCtx, input, logTypes[:nCols], ordCols,
								0 /* matchLen */, 0 /* k */, func() {},
								externalSorterMinPartitions, delegateFDAcquisition, queueCfg, sem)
							memAccounts = append(memAccounts, accounts...)
							memMonitors = append(memMonitors, monitors...)
							return sorter, err
						})
					for i, sem := range semsToCheck {
						require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
					}
				})
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

func BenchmarkExternalSort(b *testing.B) {
	defer leaktest.AfterTest(b)()
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
	rng, _ := randutil.NewPseudoRand()
	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			for _, spillForced := range []bool{false, true} {
				flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
				name := fmt.Sprintf("rows=%d/cols=%d/spilled=%t", nBatches*coldata.BatchSize(), nCols, spillForced)
				b.Run(name, func(b *testing.B) {
					// 8 (bytes / int64) * nBatches (number of batches) * coldata.BatchSize() (rows /
					// batch) * nCols (number of columns / row).
					b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
					logTypes := make([]types.T, nCols)
					for i := range logTypes {
						logTypes[i] = *types.Int
					}
					physTypes, err := typeconv.FromColumnTypes(logTypes)
					require.NoError(b, err)
					batch := testAllocator.NewMemBatch(physTypes)
					batch.SetLength(coldata.BatchSize())
					ordCols := make([]execinfrapb.Ordering_Column, nCols)
					for i := range ordCols {
						ordCols[i].ColIdx = uint32(i)
						ordCols[i].Direction = execinfrapb.Ordering_Column_Direction(rng.Int() % 2)
						col := batch.ColVec(i).Int64()
						for j := 0; j < coldata.BatchSize(); j++ {
							col[j] = rng.Int63() % int64((i*1024)+1)
						}
					}
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						source := newFiniteBatchSource(batch, nBatches)
						var spilled bool
						// TODO(yuzefovich): do not specify maxNumberPartitions (let the
						// external sorter figure out that number itself) once we pass in
						// filled-in disk queue config.
						sorter, accounts, monitors, err := createDiskBackedSorter(
							ctx, flowCtx, []Operator{source}, logTypes, ordCols,
							0 /* matchLen */, 0 /* k */, func() { spilled = true },
							64 /* maxNumberPartitions */, false /* delegateFDAcquisitions */, queueCfg, &TestingSemaphore{},
						)
						memAccounts = append(memAccounts, accounts...)
						memMonitors = append(memMonitors, monitors...)
						if err != nil {
							b.Fatal(err)
						}
						sorter.Init()
						for out := sorter.Next(ctx); out.Length() != 0; out = sorter.Next(ctx) {
						}
						require.Equal(b, spillForced, spilled, fmt.Sprintf(
							"expected: spilled=%t\tactual: spilled=%t", spillForced, spilled,
						))
					}
				})
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

// createDiskBackedSorter is a helper function that instantiates a disk-backed
// sort operator. The desired memory limit must have been already set on
// flowCtx. It returns an operator and an error as well as memory monitors and
// memory accounts that will need to be closed once the caller is done with the
// operator.
func createDiskBackedSorter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input []Operator,
	logTypes []types.T,
	ordCols []execinfrapb.Ordering_Column,
	matchLen int,
	k uint16,
	spillingCallbackFn func(),
	maxNumberPartitions int,
	delegateFDAcquisitions bool,
	diskQueueCfg colcontainer.DiskQueueCfg,
	testingSemaphore semaphore.Semaphore,
) (Operator, []*mon.BoundAccount, []*mon.BytesMonitor, error) {
	sorterSpec := &execinfrapb.SorterSpec{
		OutputOrdering:   execinfrapb.Ordering{Columns: ordCols},
		OrderingMatchLen: uint32(matchLen),
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: logTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Sorter: sorterSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Limit: uint64(k),
		},
	}
	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              input,
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
	}
	// External sorter relies on different memory accounts to
	// understand when to start a new partition, so we will not use
	// the streaming memory account.
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.NumForcedRepartitions = maxNumberPartitions
	args.TestingKnobs.DelegateFDAcquisitions = delegateFDAcquisitions
	result, err := NewColOperator(ctx, flowCtx, args)
	return result.Op, result.BufferingOpMemAccounts, result.BufferingOpMemMonitors, err
}
