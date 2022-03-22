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
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/marusama/semaphore"
)

// hashBasedPartitionerState indicates the current state of the hash-based
// partitioner.
type hashBasedPartitionerState int

const (
	// hbpInitialPartitioning indicates that the operator is currently reading
	// batches from its inputs and distributing tuples to different partitions
	// based on the hash values. Once all inputs are exhausted, the hash-based
	// partitioner transitions to hbpProcessNewPartitionUsingMain state.
	hbpInitialPartitioning hashBasedPartitionerState = iota
	// hbpRecursivePartitioning indicates that the operator is recursively
	// partitioning one of the existing partitions (that is too big to process
	// using the "main" strategy at once). It will do so using a different hash
	// function and will spill newly created partitions to disk. We also keep
	// track whether repartitioning reduces the size of the partitions in
	// question - if we see that the newly created largest partition is about
	// the same in size as the "parent" partition (the percentage difference is
	// less than hbpRecursivePartitioningSizeDecreaseThreshold), it is likely
	// that the partition consists of the tuples not distinct on the equality
	// columns, so we fall back to processing strategy that is provided as the
	// "fallback". After repartitioning, the operator transitions to
	// hbpProcessNewPartitionUsingMain state.
	hbpRecursivePartitioning
	// hbpProcessNewPartitionUsingMain indicates that the operator should choose
	// a partition index and process the corresponding partitions from all
	// inputs using the "main" operator. We will only process the partitions if
	// the partition fits into memory. If there are no partition indices that
	// the operator can process, it transitions into hbpRecursivePartitioning
	// state. If there are no partition indices to process using the main
	// operator, but there are indices to process using the "fallback" strategy,
	// the operator transitions to hbpProcessNewPartitionUsingFallback state. If
	// there are no partition indices left at all to process, the operator
	// transitions to hbpFinished state.
	hbpProcessNewPartitionUsingMain
	// hbpProcessingUsingMain indicates that the operator is currently
	// processing tuples from the corresponding partitions from all inputs using
	// the "main" operator. Once the "main" operator returns a zero-length batch
	// (indicating that full output for the current partitions has been
	// emitted), the hash-based partitioner transitions to
	// hbpProcessNewPartitionUsingMain state.
	hbpProcessingUsingMain
	// hbpProcessNewPartitionUsingFallback indicates that the operator should
	// choose a partition index to process using the "fallback" strategy. If
	// there are no partition indices for this strategy left, the operator
	// transitions to hbpFinished state.
	hbpProcessNewPartitionUsingFallback
	// hbpProcessingUsingFallback indicates that the operator is currently
	// processing tuples from the corresponding partitions from all inputs using
	// (disk-backed) "fallback" strategy. Once the "fallback" returns a
	// zero-length batch (indicating that full output for the current partitions
	// has been emitted), the hash-based partitioner transitions to
	// hbpProcessNewPartitionUsingFallback state.
	hbpProcessingUsingFallback
	// hbpFinished indicates that the hash-based partitioner has emitted all
	// tuples already and only zero-length batch will be emitted from now on.
	hbpFinished
)

const (
	// hbpRecursivePartitioningSizeDecreaseThreshold determines by how much the
	// newly-created partitions in the recursive partitioning stage should be
	// smaller than the "parent" partition in order to consider the
	// repartitioning "successful". If this threshold is not met, then the newly
	// created partition will be processed by the "fallback" disk-backed
	// operator.
	hbpRecursivePartitioningSizeDecreaseThreshold = 0.05
	// hbpDiskQueuesMemFraction determines the fraction of the available RAM
	// that is allocated for the in-memory cache of disk queues.
	hbpDiskQueuesMemFraction = 0.5
	// hbpMinimalMaxPartitionSizeForMain determines the minimum value for the
	// partition size that can be processed using the "main" in-memory strategy.
	hbpMinimalMaxPartitionSizeForMain = 64 << 10 /* 64 KiB */
)

// hashBasedPartitioner is an operator that extracts the logic of Grace hash
// join (see the comment in external_hash_joiner.go for more details) to be
// reused for other operations.
//
// It works in two phases: partitioning and processing. Every partition is
// processed either using the "main" in-memory strategy (preferable) or the
// "fallback" disk-backed strategy (when the recursive repartitioning doesn't
// seem to make progress in reducing the size of the partitions).
type hashBasedPartitioner struct {
	colexecop.NonExplainable
	colexecop.InitHelper
	colexecop.CloserHelper

	unlimitedAllocator                 *colmem.Allocator
	name                               redact.SafeString
	state                              hashBasedPartitionerState
	inputs                             []colexecop.Operator
	inputTypes                         [][]*types.T
	hashCols                           [][]uint32
	inMemMainOp                        colexecop.ResettableOperator
	diskBackedFallbackOp               colexecop.ResettableOperator
	maxPartitionSizeToProcessUsingMain int64
	// fdState is used to acquire file descriptors up front.
	fdState struct {
		fdSemaphore semaphore.Semaphore
		acquiredFDs int
	}

	partitioners      []*colcontainer.PartitionedDiskQueue
	partitionedInputs []*partitionerToOperator
	tupleDistributor  *colexechash.TupleHashDistributor
	// maxNumberActivePartitions determines the maximum number of active
	// partitions that the operator is allowed to have. This number is computed
	// semi-dynamically and will influence the choice of numBuckets value.
	maxNumberActivePartitions int
	// numBuckets is the number of buckets that a partition is divided into.
	numBuckets int
	// partitionsToProcessUsingMain is a map from partitionIdx to a utility
	// struct. This map contains all partition indices that need to be processed
	// using the in-memory "main" operator. If the partition is too big, it will
	// be tried to be repartitioned; if during repartitioning the size doesn't
	// decrease enough, it will be added to partitionsToProcessUsingFallback.
	partitionsToProcessUsingMain map[int]*hbpPartitionInfo
	// partitionsToProcessUsingFallback contains all partition indices that need
	// to be processed using the "fallback" strategy. Partition indices will be
	// added into this map if recursive partitioning doesn't seem to make
	// progress on partition' size reduction.
	partitionsToProcessUsingFallback []int
	// partitionIdxOffset stores the first "available" partition index to use.
	// During the partitioning step, all tuples will go into one of the buckets
	// in [partitionIdxOffset, partitionIdxOffset + numBuckets) range.
	partitionIdxOffset int
	// numRepartitions tracks the number of times the hash-based partitioner had
	// to recursively repartition another partition because the latter was too
	// big to process using the "main" operator.
	numRepartitions int
	// scratch and recursiveScratch are helper structs.
	scratch, recursiveScratch struct {
		// Input sources can have different schemas, so when distributing tuples
		// (i.e. copying them into scratch batch to be spilled) we might need
		// two different batches.
		batches []coldata.Batch
	}

	testingKnobs struct {
		// numForcedRepartitions is a number of times that the hash-based
		// partitioner is forced to recursively repartition (even if it is
		// otherwise not needed) before it proceeds to actually processing the
		// partitions.
		numForcedRepartitions int
		// delegateFDAcquisitions, if true, means that a test wants to force the
		// PartitionedDiskQueues to track the number of file descriptors the
		// hash-based partitioner will open/close. This disables the default
		// behavior of acquiring all file descriptors up front in Next.
		delegateFDAcquisitions bool
	}
}

var _ colexecop.ClosableOperator = &hashBasedPartitioner{}

// hbpPartitionInfo is a helper struct that tracks the memory usage of a
// partition. Note that if the hash-based partitioner has two inputs, we take
// the right partition size for this since that is the input we're buffering
// from.
type hbpPartitionInfo struct {
	memSize       int64
	parentMemSize int64
}

// DiskBackedSorterConstructor is used by the external operators to instantiate
// a disk-backed sorter used in the fallback strategies.
type DiskBackedSorterConstructor func(input colexecop.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column, maxNumberPartitions int) colexecop.Operator

// newHashBasedPartitioner returns a disk-backed operator that utilizes
// partitioning by hash approach to divide up the input set into separate
// partitions which are then processed using the "main" in-memory operator if
// they fit under the memory limit. If a partition is too big, it is attempted
// to be recursively repartitioned; if that is not successful, the partition in
// question is handled by the "fallback" disk-backed operator.
func newHashBasedPartitioner(
	unlimitedAllocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	name redact.SafeString,
	inputs []colexecop.Operator,
	inputTypes [][]*types.T,
	hashCols [][]uint32,
	inMemMainOpConstructor func([]*partitionerToOperator) colexecop.ResettableOperator,
	diskBackedFallbackOpConstructor func(
		partitionedInputs []*partitionerToOperator,
		maxNumberActivePartitions int,
		fdSemaphore semaphore.Semaphore,
	) colexecop.ResettableOperator,
	diskAcc *mon.BoundAccount,
	numRequiredActivePartitions int,
) *hashBasedPartitioner {
	// Make a copy of the DiskQueueCfg and set defaults for the partitioning
	// operators. The cache mode is chosen to automatically close the cache
	// belonging to partitions at a parent level when repartitioning.
	diskQueueCfg := args.DiskQueueCfg
	diskQueueCfg.SetCacheMode(colcontainer.DiskQueueCacheModeClearAndReuseCache)
	partitionedDiskQueueSemaphore := args.FDSemaphore
	if !args.TestingKnobs.DelegateFDAcquisitions {
		// To avoid deadlocks with other disk queues, we manually attempt to
		// acquire the maximum number of descriptors all at once in Next.
		// Passing in a nil semaphore indicates that the caller will do the
		// acquiring.
		partitionedDiskQueueSemaphore = nil
	}
	numInputs := len(inputs)
	partitioners := make([]*colcontainer.PartitionedDiskQueue, numInputs)
	partitionedInputs := make([]*partitionerToOperator, numInputs)
	for i := range inputs {
		partitioners[i] = colcontainer.NewPartitionedDiskQueue(
			inputTypes[i], diskQueueCfg, partitionedDiskQueueSemaphore, colcontainer.PartitionerStrategyDefault, diskAcc,
		)
		partitionedInputs[i] = newPartitionerToOperator(
			unlimitedAllocator, inputTypes[i], partitioners[i],
		)
	}
	maxNumberActivePartitions := calculateMaxNumberActivePartitions(flowCtx, args, numRequiredActivePartitions)
	diskQueuesMemUsed := maxNumberActivePartitions * diskQueueCfg.BufferSizeBytes
	memoryLimit := execinfra.GetWorkMemLimit(flowCtx)
	if memoryLimit == 1 {
		// If memory limit is 1, we're likely in a "force disk spill"
		// scenario, but we don't want to artificially limit batches when we
		// have already spilled, so we'll use a larger limit.
		memoryLimit = execinfra.DefaultMemoryLimit
	}
	maxPartitionSizeToProcessUsingMain := memoryLimit - int64(diskQueuesMemUsed)
	if maxPartitionSizeToProcessUsingMain < hbpMinimalMaxPartitionSizeForMain {
		maxPartitionSizeToProcessUsingMain = hbpMinimalMaxPartitionSizeForMain
	}
	op := &hashBasedPartitioner{
		unlimitedAllocator: unlimitedAllocator,
		name:               name,
		inputs:             inputs,
		inputTypes:         inputTypes,
		hashCols:           hashCols,
		inMemMainOp:        inMemMainOpConstructor(partitionedInputs),
		diskBackedFallbackOp: diskBackedFallbackOpConstructor(
			partitionedInputs, maxNumberActivePartitions, partitionedDiskQueueSemaphore,
		),
		maxPartitionSizeToProcessUsingMain: maxPartitionSizeToProcessUsingMain,
		partitioners:                       partitioners,
		partitionedInputs:                  partitionedInputs,
		maxNumberActivePartitions:          maxNumberActivePartitions,
		// In the initial partitioning state we will use all available
		// partitions fairly among all inputs.
		// TODO(yuzefovich): figure out whether we should care about
		// op.numBuckets being a power of two (finalizeHash step is faster if
		// so).
		numBuckets: maxNumberActivePartitions / numInputs,
	}
	op.fdState.fdSemaphore = args.FDSemaphore
	op.testingKnobs.numForcedRepartitions = args.TestingKnobs.NumForcedRepartitions
	op.testingKnobs.delegateFDAcquisitions = args.TestingKnobs.DelegateFDAcquisitions
	return op
}

func calculateMaxNumberActivePartitions(
	flowCtx *execinfra.FlowCtx, args *colexecargs.NewColOperatorArgs, numRequiredActivePartitions int,
) int {
	// With the default limit of 256 file descriptors, this results in 16
	// partitions. This is a hard maximum of partitions that will be used by the
	// hash-based partitioner. Below we check whether we have enough RAM to
	// support the caches of this number of partitions.
	// TODO(yuzefovich): this number should be tuned.
	maxNumberActivePartitions := args.FDSemaphore.GetLimit() / 16
	memoryLimit := execinfra.GetWorkMemLimit(flowCtx)
	if args.DiskQueueCfg.BufferSizeBytes > 0 {
		diskQueuesTotalMemLimit := int(float64(memoryLimit) * hbpDiskQueuesMemFraction)
		numDiskQueuesThatFit := diskQueuesTotalMemLimit / args.DiskQueueCfg.BufferSizeBytes
		if numDiskQueuesThatFit < maxNumberActivePartitions {
			maxNumberActivePartitions = numDiskQueuesThatFit
		}
	}
	if maxNumberActivePartitions < numRequiredActivePartitions {
		maxNumberActivePartitions = numRequiredActivePartitions
	}
	return maxNumberActivePartitions
}

func (op *hashBasedPartitioner) Init(ctx context.Context) {
	if !op.InitHelper.Init(ctx) {
		return
	}
	for i := range op.inputs {
		op.inputs[i].Init(op.Ctx)
	}
	op.partitionsToProcessUsingMain = make(map[int]*hbpPartitionInfo)
	// If we are initializing the hash-based partitioner, it means that we had
	// to fallback from the in-memory one since the inputs had more tuples that
	// could fit into the memory, and, therefore, it makes sense to instantiate
	// the batches with maximum capacity.
	op.scratch.batches = append(op.scratch.batches, op.unlimitedAllocator.NewMemBatchWithFixedCapacity(op.inputTypes[0], coldata.BatchSize()))
	op.recursiveScratch.batches = append(op.recursiveScratch.batches, op.unlimitedAllocator.NewMemBatchWithFixedCapacity(op.inputTypes[0], coldata.BatchSize()))
	if len(op.inputs) == 2 {
		sameSourcesSchema := len(op.inputTypes[0]) == len(op.inputTypes[1])
		for i := 0; sameSourcesSchema && i < len(op.inputTypes[0]); i++ {
			if !op.inputTypes[0][i].Identical(op.inputTypes[1][i]) {
				sameSourcesSchema = false
			}
		}
		if sameSourcesSchema {
			// The schemas of both sources are the same, so we can reuse the
			// first scratch batch.
			op.scratch.batches = append(op.scratch.batches, op.scratch.batches[0])
			op.recursiveScratch.batches = append(op.recursiveScratch.batches, op.recursiveScratch.batches[0])
		} else {
			op.scratch.batches = append(op.scratch.batches, op.unlimitedAllocator.NewMemBatchWithFixedCapacity(op.inputTypes[1], coldata.BatchSize()))
			op.recursiveScratch.batches = append(op.recursiveScratch.batches, op.unlimitedAllocator.NewMemBatchWithFixedCapacity(op.inputTypes[1], coldata.BatchSize()))
		}
	}
	// In the processing phase, the in-memory operator will use the default init
	// hash value, so in order to use a "different" hash function in the
	// partitioning phase we use a different init hash value.
	op.tupleDistributor = colexechash.NewTupleHashDistributor(
		colexechash.DefaultInitHashValue+1, op.numBuckets,
	)
	op.tupleDistributor.Init(op.Ctx)
	op.state = hbpInitialPartitioning
}

func (op *hashBasedPartitioner) partitionBatch(
	batch coldata.Batch, inputIdx int, parentMemSize int64,
) {
	batchLen := batch.Length()
	if batchLen == 0 {
		return
	}
	scratchBatch := op.scratch.batches[inputIdx]
	selections := op.tupleDistributor.Distribute(batch, op.hashCols[inputIdx])
	for idx, sel := range selections {
		partitionIdx := op.partitionIdxOffset + idx
		if len(sel) > 0 {
			scratchBatch.ResetInternalBatch()
			// The partitioner expects the batches without a selection vector,
			// so we need to copy the tuples according to the selection vector
			// into a scratch batch.
			colVecs := scratchBatch.ColVecs()
			op.unlimitedAllocator.PerformOperation(colVecs, func() {
				for i, colvec := range colVecs {
					colvec.Copy(coldata.SliceArgs{
						Src:       batch.ColVec(i),
						Sel:       sel,
						SrcEndIdx: len(sel),
					})
				}
				scratchBatch.SetLength(len(sel))
			})
			if err := op.partitioners[inputIdx].Enqueue(op.Ctx, partitionIdx, scratchBatch); err != nil {
				colexecutils.HandleErrorFromDiskQueue(err)
			}
			partitionInfo, ok := op.partitionsToProcessUsingMain[partitionIdx]
			if !ok {
				partitionInfo = &hbpPartitionInfo{}
				op.partitionsToProcessUsingMain[partitionIdx] = partitionInfo
			}
			if inputIdx == len(op.inputs)-1 {
				partitionInfo.parentMemSize = parentMemSize
				// We cannot use allocator's methods directly because those look
				// at the capacities of the vectors, and in our case only first
				// len(sel) tuples belong to the "current" batch.
				partitionInfo.memSize += colmem.GetProportionalBatchMemSize(scratchBatch, int64(len(sel)))
			}
		}
	}
}

func (op *hashBasedPartitioner) Next() coldata.Batch {
	var batches [2]coldata.Batch
StateChanged:
	for {
		switch op.state {
		case hbpInitialPartitioning:
			allZero := true
			for i := range op.inputs {
				batches[i] = op.inputs[i].Next()
				allZero = allZero && batches[i].Length() == 0
			}
			if allZero {
				// All inputs have been partitioned and spilled, so we
				// transition to processing phase. Close all the open write file
				// descriptors.
				//
				// TODO(yuzefovich): this will also clear the cache once the new
				// PR is in. This means we will reallocate a cache whenever
				// reading from the partitions. What I think we might want to do
				// is not close the partitions here. Instead, we move on to
				// joining, which will switch all of these reserved file
				// descriptors to read in the best case (no repartitioning) and
				// reuse the cache. Only if we need to repartition should we
				// CloseAllOpenWriteFileDescriptors of both sides. It might also
				// be more efficient to Dequeue from the partitions you'll read
				// from before doing that to exempt them from releasing their
				// FDs to the semaphore.
				for i := range op.inputs {
					if err := op.partitioners[i].CloseAllOpenWriteFileDescriptors(op.Ctx); err != nil {
						colexecerror.InternalError(err)
					}
				}
				op.inMemMainOp.Init(op.Ctx)
				op.partitionIdxOffset += op.numBuckets
				op.state = hbpProcessNewPartitionUsingMain
				log.VEventf(op.Ctx, 1, "%s consumed its input", op.name)
				continue
			}
			if !op.testingKnobs.delegateFDAcquisitions && op.fdState.acquiredFDs == 0 {
				toAcquire := op.maxNumberActivePartitions
				if err := op.fdState.fdSemaphore.Acquire(op.Ctx, toAcquire); err != nil {
					colexecerror.InternalError(err)
				}
				op.fdState.acquiredFDs = toAcquire
			}
			for i := range op.inputs {
				op.partitionBatch(batches[i], i, math.MaxInt64)
			}

		case hbpRecursivePartitioning:
			op.numRepartitions++
			if op.numRepartitions%10 == 0 {
				log.VEventf(op.Ctx, 2,
					"%s is performing %d'th repartition", op.name, op.numRepartitions,
				)
			}
			// In order to use a different hash function when repartitioning, we
			// need to increase the seed value of the tuple distributor.
			op.tupleDistributor.InitHashValue++
			// We're actively will be using op.numBuckets + 1 partitions
			// (because we're repartitioning one side at a time), so we can set
			// op.numBuckets higher than in the initial partitioning step.
			// TODO(yuzefovich): figure out whether we should care about
			// op.numBuckets being a power of two (finalizeHash step is faster
			// if so).
			op.numBuckets = op.maxNumberActivePartitions - 1
			op.tupleDistributor.ResetNumOutputs(op.numBuckets)
			for parentPartitionIdx, parentPartitionInfo := range op.partitionsToProcessUsingMain {
				for i := range op.inputs {
					batch := op.recursiveScratch.batches[i]
					partitioner := op.partitioners[i]
					for {
						op.unlimitedAllocator.PerformOperation(batch.ColVecs(), func() {
							if err := partitioner.Dequeue(op.Ctx, parentPartitionIdx, batch); err != nil {
								colexecerror.InternalError(err)
							}
						})
						if batch.Length() == 0 {
							break
						}
						op.partitionBatch(batch, i, parentPartitionInfo.memSize)
					}
					// We're done reading from this partition, and it will never
					// be read from again, so we can close it.
					if err := partitioner.CloseInactiveReadPartitions(op.Ctx); err != nil {
						colexecerror.InternalError(err)
					}
					// We're done writing to the newly created partitions.
					// TODO(yuzefovich): we should not release the descriptors
					// here. The invariant should be: we're entering
					// hbpRecursivePartitioning, at that stage we have at
					// most numBuckets*2 file descriptors open. At the top of
					// the state transition, close all open write file
					// descriptors, which should reduce the open descriptors to
					// 0. Now we open the two read' partitions for 2 file
					// descriptors and whatever number of write partitions we
					// want. This will allow us to remove the call to
					// CloseAllOpen... in the first state as well.
					if err := partitioner.CloseAllOpenWriteFileDescriptors(op.Ctx); err != nil {
						colexecerror.InternalError(err)
					}
				}
				for idx := 0; idx < op.numBuckets; idx++ {
					newPartitionIdx := op.partitionIdxOffset + idx
					if partitionInfo, ok := op.partitionsToProcessUsingMain[newPartitionIdx]; ok {
						before, after := partitionInfo.parentMemSize, partitionInfo.memSize
						if before > 0 {
							sizeDecrease := 1.0 - float64(after)/float64(before)
							if sizeDecrease < hbpRecursivePartitioningSizeDecreaseThreshold {
								// We will need to process this partition using
								// the "fallback" strategy.
								op.partitionsToProcessUsingFallback = append(op.partitionsToProcessUsingFallback, newPartitionIdx)
								delete(op.partitionsToProcessUsingMain, newPartitionIdx)
							}
						}
					}
				}
				// We have successfully repartitioned the partitions with index
				// 'parentPartitionIdx' from all inputs, so we delete that index
				// from the map and proceed on processing the newly created
				// partitions.
				delete(op.partitionsToProcessUsingMain, parentPartitionIdx)
				op.partitionIdxOffset += op.numBuckets
				op.state = hbpProcessNewPartitionUsingMain
				continue StateChanged
			}

		case hbpProcessNewPartitionUsingMain:
			if op.testingKnobs.numForcedRepartitions > 0 && len(op.partitionsToProcessUsingMain) > 0 {
				op.testingKnobs.numForcedRepartitions--
				op.state = hbpRecursivePartitioning
				continue
			}
			// Find next partition that we can process without having to
			// recursively repartition.
			for partitionIdx, partitionInfo := range op.partitionsToProcessUsingMain {
				if partitionInfo.memSize <= op.maxPartitionSizeToProcessUsingMain {
					log.VEventf(op.Ctx, 2,
						`%s processes partition with idx %d of size %s using the "main" strategy`,
						op.name, partitionIdx, humanizeutil.IBytes(partitionInfo.memSize),
					)
					for i := range op.partitionedInputs {
						op.partitionedInputs[i].partitionIdx = partitionIdx
					}
					op.inMemMainOp.Reset(op.Ctx)
					delete(op.partitionsToProcessUsingMain, partitionIdx)
					op.state = hbpProcessingUsingMain
					continue StateChanged
				}
			}
			if len(op.partitionsToProcessUsingMain) == 0 {
				// All partitions to process using the in-memory operator have
				// been exhausted.
				if len(op.partitionsToProcessUsingFallback) > 0 {
					// But there are still some partitions to process using the
					// "fallback" strategy.
					op.diskBackedFallbackOp.Init(op.Ctx)
					log.VEventf(op.Ctx, 1,
						`%s will process %d partitions using the "fallback" strategy`,
						op.name, len(op.partitionsToProcessUsingFallback),
					)
					op.state = hbpProcessNewPartitionUsingFallback
					continue
				}
				// All partitions have been processed, so we transition to
				// finished state.
				op.state = hbpFinished
				continue
			}
			// We have partitions that we cannot process without recursively
			// repartitioning first, so we transition to the corresponding state.
			op.state = hbpRecursivePartitioning
			continue

		case hbpProcessingUsingMain:
			b := op.inMemMainOp.Next()
			if b.Length() == 0 {
				// We're done processing these partitions, so we close them and
				// transition to processing new ones.
				for i := range op.inputs {
					if err := op.partitioners[i].CloseInactiveReadPartitions(op.Ctx); err != nil {
						colexecerror.InternalError(err)
					}
				}
				op.state = hbpProcessNewPartitionUsingMain
				continue
			}
			return b

		case hbpProcessNewPartitionUsingFallback:
			if len(op.partitionsToProcessUsingFallback) == 0 {
				// All partitions have been processed, so we transition to
				// finished state.
				op.state = hbpFinished
				continue
			}
			partitionIdx := op.partitionsToProcessUsingFallback[0]
			op.partitionsToProcessUsingFallback = op.partitionsToProcessUsingFallback[1:]
			for i := range op.partitionedInputs {
				op.partitionedInputs[i].partitionIdx = partitionIdx
			}
			op.diskBackedFallbackOp.Reset(op.Ctx)
			op.state = hbpProcessingUsingFallback
			continue

		case hbpProcessingUsingFallback:
			b := op.diskBackedFallbackOp.Next()
			if b.Length() == 0 {
				// We're done processing these partitions, so we close them and
				// transition to processing new ones.
				for i := range op.inputs {
					if err := op.partitioners[i].CloseInactiveReadPartitions(op.Ctx); err != nil {
						colexecerror.InternalError(err)
					}
				}
				op.state = hbpProcessNewPartitionUsingFallback
				continue
			}
			return b

		case hbpFinished:
			if err := op.Close(op.Ctx); err != nil {
				colexecerror.InternalError(err)
			}
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected hashBasedPartitionerState %d", op.state))
		}
	}
}

func (op *hashBasedPartitioner) Close(ctx context.Context) error {
	if !op.CloserHelper.Close() {
		return nil
	}
	log.VEventf(ctx, 1, "%s is closed", op.name)
	var retErr error
	for i := range op.inputs {
		if err := op.partitioners[i].Close(ctx); err != nil {
			retErr = err
		}
	}
	// The in-memory main operator might be a Closer (e.g. the in-memory hash
	// aggregator), and we need to close it if so.
	if c, ok := op.inMemMainOp.(colexecop.Closer); ok {
		if err := c.Close(ctx); err != nil {
			retErr = err
		}
	}
	// Note that it is ok if the disk-backed fallback operator is not a Closer -
	// it will still be closed appropriately because we accumulate all closers
	// in NewColOperatorResult.
	if c, ok := op.diskBackedFallbackOp.(colexecop.Closer); ok {
		if err := c.Close(ctx); err != nil {
			retErr = err
		}
	}
	if !op.testingKnobs.delegateFDAcquisitions && op.fdState.acquiredFDs > 0 {
		op.fdState.fdSemaphore.Release(op.fdState.acquiredFDs)
		op.fdState.acquiredFDs = 0
	}
	return retErr
}

func (op *hashBasedPartitioner) ChildCount(_ bool) int {
	return len(op.inputs)
}

func (op *hashBasedPartitioner) Child(nth int, _ bool) execinfra.OpNode {
	return op.inputs[nth]
}
