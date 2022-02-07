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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

	rng, _ := randutil.NewTestRand()
	numForcedRepartitions := rng.Intn(5)
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)

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
						// TODO(asubiotto): Pass in the testing.T of the caller to this
						//  function and do substring matching on the test name to
						//  conditionally explicitly call Close() on the sorter (through
						//  result.ToClose) in cases where it is know the sorter will not
						//  be drained.
						sorter, closers, err := createDiskBackedSorter(
							ctx, flowCtx, input, tc.typs, tc.ordCols, tc.matchLen, tc.k, func() {},
							numForcedRepartitions, false /* delegateFDAcquisition */, queueCfg, sem,
							&monitorRegistry,
						)
						// Check that the sort was added as a Closer.
						// TODO(asubiotto): Explicitly Close when testing.T is passed into
						//  this constructor and we do a substring match.
						require.Equal(t, 1, len(closers))
						return sorter, err
					})
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
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
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
							sorter, closers, err := createDiskBackedSorter(
								ctx, flowCtx, input, typs[:nCols], ordCols,
								0 /* matchLen */, uint64(k), func() {},
								numForcedRepartitions, delegateFDAcquisition, queueCfg, sem,
								&monitorRegistry,
							)
							// TODO(asubiotto): Explicitly Close when testing.T is passed into
							//  this constructor and we do a substring match.
							require.Equal(t, 1, len(closers))
							return sorter, err
						})
					for i, sem := range semsToCheck {
						require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
					}
				}
			}
		}
	}
}

// TestExternalSortMemoryAccounting is a sanity check for the memory accounting
// done throughout the external sort operation. At the moment there are a lot of
// known problems with the memory accounting, so the test is not very strict.
// The goal of the test is to make sure that the total maximum reported memory
// usage (as would have been collected as stats) is within reasonable range. It
// additionally checks that the number of partitions created is as expected too.
//
// It is impossible to come up with the exact numbers here due to the randomness
// of appends (which happen when setting values on Bytes vectors) and due to the
// randomization of coldata.BatchSize() value.
func TestExternalSortMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "the test is very memory-intensive and is likely to OOM under stress")
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
	rng, _ := randutil.NewTestRand()

	// Use the Bytes type because we can control the size of values with it
	// easily.
	typs := []*types.T{types.Bytes}
	ordCols := []execinfrapb.Ordering_Column{{ColIdx: 0}}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)

	numInMemoryBufferedBatches := 8 + rng.Intn(4)
	// numNewPartitions determines the expected number of partitions created as
	// a result of consuming the input (i.e. not as a result of the repeated
	// merging).
	numNewPartitions := 4 + rng.Intn(3)
	numTotalBatches := numInMemoryBufferedBatches * numNewPartitions
	batchLength := coldata.BatchSize()
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, batchLength)
	// Use such a size for a single value that the memory footprint of a single
	// batch is relatively large.
	singleTupleSize := mon.DefaultPoolAllocationSize
	singleTupleValue := make([]byte, singleTupleSize)
	for i := 0; i < batchLength; i++ {
		batch.ColVec(0).Bytes().Set(i, singleTupleValue)
	}
	batch.SetLength(batchLength)
	numFDs := colexecop.ExternalSorterMinPartitions + rng.Intn(3)
	// The memory limit in the external sorter is divided as follows:
	// - BufferSizeBytes for each of the disk queues is subtracted right away
	// - the remaining part is divided evenly between the sorter and the merger
	// - the sorter gives 80% of its half to the buffer.
	bufferMemoryLimit := colmem.GetBatchMemSize(batch) * int64(numInMemoryBufferedBatches)
	memoryLimit := int64(queueCfg.BufferSizeBytes*numFDs) + int64(float64(bufferMemoryLimit)/0.8*2)
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimit
	input := colexectestutils.NewFiniteBatchSource(testAllocator, batch, typs, numTotalBatches)

	var spilled bool
	// We multiply by 16 because the external sorter divides by this number.
	sem := colexecop.NewTestingSemaphore(numFDs * 16)
	sorter, closers, err := createDiskBackedSorter(
		ctx, flowCtx, []colexecop.Operator{input}, typs, ordCols,
		0 /* matchLen */, 0 /* k */, func() { spilled = true },
		0 /* numForcedRepartitions */, false, /* delegateFDAcquisition */
		queueCfg, sem, &monitorRegistry,
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(closers))

	sorter.Init(ctx)
	for b := sorter.Next(); b.Length() > 0; b = sorter.Next() {
	}
	for _, c := range closers {
		require.NoError(t, c.Close(ctx))
	}

	require.True(t, spilled)
	require.Zero(t, sem.GetCount(), "sem still reports open FDs")

	externalSorter := MaybeUnwrapInvariantsChecker(sorter).(*diskSpillerBase).diskBackedOp.(*externalSorter)
	numPartitionsCreated := externalSorter.currentPartitionIdx
	// This maximum can be achieved when we have minimum required number of FDs
	// as follows: we expect that each newly created partition contains about
	// numInMemoryBufferedBatches number of batches with only the partition that
	// is the result of the repeated merge growing with count as a multiple of
	// numInMemoryBufferedBatches (first merge = 2x, second merge = 3x, third
	// merge 4x, etc, so we expect 2*numNewPartitions-1 partitions).
	expMaxTotalPartitionsCreated := 2*numNewPartitions - 1
	// Because of the fact that we are creating partitions slightly larger than
	// memoryLimit in size and because of our "after the fact" memory
	// accounting, we might create less partitions than maximum defined above
	// (e.g., if numNewPartitions is 4, then we will create 3 partitions when
	// batch size is 3).
	expMinTotalPartitionsCreated := numNewPartitions - 1
	require.GreaterOrEqualf(t, numPartitionsCreated, expMinTotalPartitionsCreated,
		"didn't create enough partitions: actual %d, min expected %d",
		numPartitionsCreated, expMinTotalPartitionsCreated,
	)
	require.GreaterOrEqualf(t, expMaxTotalPartitionsCreated, numPartitionsCreated,
		"created too many partitions: actual %d, max expected %d",
		numPartitionsCreated, expMaxTotalPartitionsCreated,
	)

	monitors := monitorRegistry.GetMonitors()

	// Check that the monitor for the in-memory sorter reports lower than
	// memoryLimit max usage (the allocation that would put the monitor over the
	// limit must have been denied with OOM error).
	require.Greater(t, memoryLimit, monitors[0].MaximumBytes())

	// Use the same calculation as we have when computing stats (maximums are
	// summed).
	var totalMaxMemUsage int64
	for i := range monitors {
		if monitors[i].Resource() == mon.MemoryResource {
			totalMaxMemUsage += monitors[i].MaximumBytes()
		}
	}
	// We cannot guarantee a fixed value, so we use an allowed range.
	expMin := memoryLimit
	expMax := int64(float64(memoryLimit) * 1.6)
	require.GreaterOrEqualf(t, totalMaxMemUsage, expMin, "minimum memory bound not satisfied: "+
		"actual %d, expected min %d", totalMaxMemUsage, expMin)
	require.GreaterOrEqualf(t, expMax, totalMaxMemUsage, "maximum memory bound not satisfied: "+
		"actual %d, expected max %d", totalMaxMemUsage, expMax)
}

func BenchmarkExternalSort(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
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
	rng, _ := randutil.NewTestRand()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)

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
							sorter, _, err := createDiskBackedSorter(
								ctx, flowCtx, []colexecop.Operator{source}, typs, ordCols,
								0 /* matchLen */, k, func() { spilled = true },
								0 /* numForcedRepartitions */, false /* delegateFDAcquisitions */, queueCfg, &colexecop.TestingSemaphore{},
								&monitorRegistry,
							)
							require.NoError(b, err)
							sorter.Init(ctx)
							for out := sorter.Next(); out.Length() != 0; out = sorter.Next() {
							}
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
) (colexecop.Operator, []colexecop.Closer, error) {
	sorterSpec := &execinfrapb.SorterSpec{
		OutputOrdering:   execinfrapb.Ordering{Columns: ordCols},
		OrderingMatchLen: uint32(matchLen),
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: typs}},
		Core: execinfrapb.ProcessorCoreUnion{
			Sorter: sorterSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Limit: k,
		},
		ResultTypes: typs,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              colexectestutils.MakeInputs(sources),
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
		MonitorRegistry:     monitorRegistry,
	}
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	args.TestingKnobs.DelegateFDAcquisitions = delegateFDAcquisitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, result.ToClose, err
}
