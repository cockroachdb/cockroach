// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalSort(t *testing.T) {
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

	rng, _ := randutil.NewTestRand()
	numForcedRepartitions := rng.Intn(5)
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)
	var closerRegistry colexecargs.CloserRegistry
	defer closerRegistry.Close(ctx)

	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		if spillForced {
			// In order to increase test coverage of recursive merging, we have
			// the lowest possible memory limit (that will not be overridden by
			// the external sorter) - this will force creating partitions
			// consisting of a single batch.
			flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 2
		} else {
			flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 0
		}
		for _, tcs := range [][]sortTestCase{sortAllTestCases, topKSortTestCases, sortChunksTestCases} {
			for _, tc := range tcs {
				log.Infof(context.Background(), "spillForced=%t/numRepartitions=%d/%s", spillForced, numForcedRepartitions, tc.description)
				var semsToCheck []semaphore.Semaphore
				var numRuns, numSpills int
				colexectestutils.RunTestsWithTyps(
					t,
					testAllocator,
					[]colexectestutils.Tuples{tc.tuples},
					[][]*types.T{tc.typs},
					tc.expected,
					colexectestutils.OrderedVerifier,
					func(input []colexecop.Operator) (colexecop.Operator, error) {
						// A sorter should never exceed ExternalSorterMinPartitions, even
						// during repartitioning. A panic will happen if a sorter requests
						// more than this number of file descriptors.
						sem := colexecop.NewTestingSemaphore(colexecop.ExternalSorterMinPartitions)
						// If a limit is satisfied before the sorter is drained of all its
						// tuples, the sorter will not close its partitioner. During a
						// flow this will happen in a downstream materializer/outbox,
						// since there is no way to tell an operator that Next won't be
						// called again.
						if tc.k == 0 || tc.k >= uint64(len(tc.tuples)) {
							semsToCheck = append(semsToCheck, sem)
						}
						numRuns++
						return createDiskBackedSorter(
							ctx, flowCtx, input, tc.typs, tc.ordCols, tc.matchLen, tc.k, func() { numSpills++ },
							numForcedRepartitions, false /* delegateFDAcquisition */, queueCfg, sem,
							&monitorRegistry, &closerRegistry,
						)
					})
				// We expect that each run resulted in the disk spiller being
				// included into the closer registry. Additionally, every time
				// we spilled, we should've included the external sort in there
				// too.
				require.Equal(t, numRuns+numSpills, closerRegistry.NumClosers())
				// Close all closers manually (in production this is done on the
				// flow cleanup).
				closerRegistry.Close(ctx)
				closerRegistry.Reset()
				for i, sem := range semsToCheck {
					require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
				}
			}
		}
	}
}

func TestExternalSortRandomized(t *testing.T) {
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
	rng, _ := randutil.NewTestRand()
	nTups := coldata.BatchSize()*4 + 1
	maxCols := 2
	// TODO(yuzefovich): randomize types as well.
	typs := make([]*types.T, maxCols)
	for i := range typs {
		typs[i] = types.Int
	}

	const numForcedRepartitions = 3
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)
	var closerRegistry colexecargs.CloserRegistry
	defer closerRegistry.Close(ctx)

	// Interesting disk spilling scenarios:
	// 1) The sorter is forced to spill to disk as soon as possible.
	// 2) The memory limit is dynamically set to repartition twice, this will also
	//    allow the in-memory sorter to spool several batches before hitting the
	//    memory limit.
	// memoryToSort is the total amount of memory that will be sorted in this
	// test.
	memoryToSort := (nTups / coldata.BatchSize()) * int(colmem.EstimateBatchSizeBytes(typs, coldata.BatchSize()))
	// partitionSize will be the memory limit passed in to tests with a memory
	// limit. With a maximum number of partitions of 2 this will result in
	// repartitioning twice. To make this a total amount of memory, we also need
	// to add the cache sizes of the queues.
	partitionSize := int64(memoryToSort/4) + int64(colexecop.ExternalSorterMinPartitions*queueCfg.BufferSizeBytes)
	for _, tk := range []execinfra.TestingKnobs{{ForceDiskSpill: true}, {MemoryLimitBytes: partitionSize}} {
		flowCtx.Cfg.TestingKnobs = tk
		for nCols := 1; nCols <= maxCols; nCols++ {
			for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
				for _, k := range []int{0, rng.Intn(nTups) + 1} {
					namePrefix := "MemoryLimit=" + humanizeutil.IBytes(tk.MemoryLimitBytes)
					if tk.ForceDiskSpill {
						namePrefix = "ForceDiskSpill=true"
					}
					delegateFDAcquisition := rng.Float64() < 0.5
					name := fmt.Sprintf("%s/nCols=%d/nOrderingCols=%d/delegateFDAcquisition=%t/k=%d", namePrefix, nCols, nOrderingCols, delegateFDAcquisition, k)
					log.Infof(ctx, "%s", name)
					var semsToCheck []semaphore.Semaphore
					tups, expected, ordCols := generateRandomDataForTestSort(rng, nTups, nCols, nOrderingCols, 0 /* matchLen */)
					if k > 0 {
						expected = expected[:k]
					}
					colexectestutils.RunTests(
						t,
						testAllocator,
						[]colexectestutils.Tuples{tups},
						expected,
						colexectestutils.OrderedVerifier,
						func(input []colexecop.Operator) (colexecop.Operator, error) {
							sem := colexecop.NewTestingSemaphore(colexecop.ExternalSorterMinPartitions)
							semsToCheck = append(semsToCheck, sem)
							return createDiskBackedSorter(
								ctx, flowCtx, input, typs[:nCols], ordCols,
								0 /* matchLen */, uint64(k), func() {},
								numForcedRepartitions, delegateFDAcquisition, queueCfg, sem,
								&monitorRegistry, &closerRegistry,
							)
						})
					// Close all closers manually (in production this is done on
					// the flow cleanup).
					closerRegistry.Close(ctx)
					closerRegistry.Reset()
					for i, sem := range semsToCheck {
						require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
					}
				}
			}
		}
	}
}

func BenchmarkExternalSort(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
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
	rng, _ := randutil.NewTestRand()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	var closerRegistry colexecargs.CloserRegistry
	afterEachRun := func() {
		closerRegistry.BenchmarkReset(ctx)
		monitorRegistry.BenchmarkReset(ctx)
	}

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			for _, topK := range []bool{false, true} {
				for _, spillForced := range []bool{false, true} {
					flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
					var topKSubstring string
					if topK {
						topKSubstring = "topK/"
					}
					name := fmt.Sprintf("rows=%d/cols=%d/%sspilled=%t", nBatches*coldata.BatchSize(), nCols, topKSubstring, spillForced)
					b.Run(name, func(b *testing.B) {
						// 8 (bytes / int64) * nBatches (number of batches) * coldata.BatchSize() (rows /
						// batch) * nCols (number of columns / row).
						b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
						typs := make([]*types.T, nCols)
						for i := range typs {
							typs[i] = types.Int
						}
						batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
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
							source := colexectestutils.NewFiniteBatchSource(testAllocator, batch, typs, nBatches)
							var spilled bool
							k := uint64(0)
							if topK {
								// Pick the same value for K as we do in the
								// in-memory top K sort benchmark.
								k = 128
							}
							sorter, err := createDiskBackedSorter(
								ctx, flowCtx, []colexecop.Operator{source}, typs, ordCols,
								0 /* matchLen */, k, func() { spilled = true },
								0 /* numForcedRepartitions */, false /* delegateFDAcquisitions */, queueCfg, &colexecop.TestingSemaphore{},
								&monitorRegistry, &closerRegistry,
							)
							require.NoError(b, err)
							sorter.Init(ctx)
							for out := sorter.Next(); out.Length() != 0; out = sorter.Next() {
							}
							afterEachRun()
							require.Equal(b, spillForced, spilled, fmt.Sprintf(
								"expected: spilled=%t\tactual: spilled=%t", spillForced, spilled,
							))
						}
					})
				}
			}
		}
	}
}

// createDiskBackedSorter is a helper function that instantiates a disk-backed
// sort operator. The desired memory limit must have been already set on
// flowCtx.
func createDiskBackedSorter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	sources []colexecop.Operator,
	typs []*types.T,
	ordCols []execinfrapb.Ordering_Column,
	matchLen int,
	k uint64,
	spillingCallbackFn func(),
	numForcedRepartitions int,
	delegateFDAcquisitions bool,
	diskQueueCfg colcontainer.DiskQueueCfg,
	testingSemaphore semaphore.Semaphore,
	monitorRegistry *colexecargs.MonitorRegistry,
	closerRegistry *colexecargs.CloserRegistry,
) (colexecop.Operator, error) {
	sorterSpec := &execinfrapb.SorterSpec{
		OutputOrdering:   execinfrapb.Ordering{Columns: ordCols},
		OrderingMatchLen: uint32(matchLen),
		Limit:            int64(k),
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: typs}},
		Core: execinfrapb.ProcessorCoreUnion{
			Sorter: sorterSpec,
		},
		ResultTypes: typs,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:            spec,
		Inputs:          colexectestutils.MakeInputs(sources),
		DiskQueueCfg:    diskQueueCfg,
		FDSemaphore:     testingSemaphore,
		MonitorRegistry: monitorRegistry,
		CloserRegistry:  closerRegistry,
	}
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	args.TestingKnobs.DelegateFDAcquisitions = delegateFDAcquisitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, err
}
