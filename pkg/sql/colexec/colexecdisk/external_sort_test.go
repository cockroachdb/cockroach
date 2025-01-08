// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecdisk

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

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

	// Ensure that coldata-batch-size is in [MinBatchSize, 1024] range. If the
	// batch size becomes too large, then the test will use multiple GBs of RAM
	// which might lead to OOMs in some environments.
	const maxBatchSize = 1024
	if oldBatchSize := coldata.BatchSize(); oldBatchSize > maxBatchSize {
		defer func() {
			require.NoError(t, coldata.SetBatchSizeForTests(oldBatchSize))
		}()
		newBatchSize := colexectestutils.MinBatchSize + rng.Intn(maxBatchSize-colexectestutils.MinBatchSize+1)
		require.NoError(t, coldata.SetBatchSizeForTests(newBatchSize))
		t.Logf("coldata-batch-size overridden to %d", newBatchSize)
	}

	// Use the Bytes type because we can control the size of values with it
	// easily.
	typs := []*types.T{types.Bytes}
	ordCols := []execinfrapb.Ordering_Column{{ColIdx: 0}}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)
	var closerRegistry colexecargs.CloserRegistry
	defer closerRegistry.Close(ctx)

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
	sorter, err := createDiskBackedSorter(
		ctx, flowCtx, []colexecop.Operator{input}, typs, ordCols,
		0 /* matchLen */, 0 /* k */, func() { spilled = true },
		0 /* numForcedRepartitions */, false, /* delegateFDAcquisition */
		queueCfg, sem, &monitorRegistry, &closerRegistry,
	)
	require.NoError(t, err)

	sorter.Init(ctx)
	for b := sorter.Next(); b.Length() > 0; b = sorter.Next() {
	}

	require.True(t, spilled)
	require.Zero(t, sem.GetCount(), "sem still reports open FDs")

	externalSorter := colexec.MaybeUnwrapInvariantsChecker(sorter).(*oneInputDiskSpiller).diskBackedOp.(*externalSorter)
	numPartitionsCreated := externalSorter.currentPartitionIdx
	// This maximum can be achieved when we have minimum required number of FDs
	// as follows: we expect that each newly created partition contains about
	// numInMemoryBufferedBatches number of batches with only the partition that
	// is the result of the repeated merge growing with count as a multiple of
	// numInMemoryBufferedBatches (first merge = 2x, second merge = 3x, third
	// merge 4x, etc, so we expect 2*numNewPartitions-1 partitions).
	expMaxTotalPartitionsCreated := 2*numNewPartitions - 1
	// Since we are creating partitions slightly larger than memoryLimit in size
	// and because of our "after the fact" memory accounting, we might create
	// fewer partitions than the target (e.g., if numNewPartitions is 4, then we
	// will create 3 partitions when batch size is 3). However, we might not
	// even create numNewPartitions-1 in edge cases (due to how we grow
	// coldata.Bytes.buffer when setting values), so we opt for a sanity check
	// that at least two partitions were created that must always be true.
	expMinTotalPartitionsCreated := 2
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
	// The factor of 2.8 is necessary due to the following setup:
	// - 0.8x is used by the limited monitor of the in-memory sorter
	// - 0.8x is used by the unlimited monitor that "supports" the limited
	//      monitor of the in-memory sorter. This part is needed since we
	//      perform the accounting after the fact. See
	//      colmem.NewLimitedAllocator for details.
	// - 1x is used by the unlimited monitor of the external sort.
	//
	// 0.8 is the fraction of the memory limit that we give to the spooler while
	// keeping the remaining 0.2 for the output batch (which is never utilized).
	// This logic lives in createDiskBackedSorter in execplan.go.
	//
	// To allow some drift (for things like memory used by the disk queue) we
	// add another 0.7.
	expMax := int64(float64(memoryLimit) * 3.3)
	require.GreaterOrEqualf(t, totalMaxMemUsage, expMin, "minimum memory bound not satisfied: "+
		"actual %d, expected min %d", totalMaxMemUsage, expMin)
	require.GreaterOrEqualf(t, expMax, totalMaxMemUsage, "maximum memory bound not satisfied: "+
		"actual %d, expected max %d", totalMaxMemUsage, expMax)
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
