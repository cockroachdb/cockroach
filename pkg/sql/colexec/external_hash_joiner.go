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
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// externalHashJoinerState indicates the current state of the external hash
// joiner.
type externalHashJoinerState int

const (
	// externalHJInitialPartitioning indicates that the operator is currently
	// reading batches from both inputs and distributing tuples to different
	// partitions based on the hash values. Once both inputs are exhausted, the
	// external hash joiner transitions to externalHJJoinNewPartition state.
	externalHJInitialPartitioning externalHashJoinerState = iota
	// externalHJRecursivePartitioning indicates that the operator is recursively
	// partitioning one of the existing partitions (that is too big to join at
	// once). It will do so using a different hash function and will spill newly
	// created partitions to disk. We also keep track whether repartitioning
	// reduces the size of the partitions in question - if we see that the newly
	// created largest partition is about the same in size as the "parent"
	// partition (the percentage difference is less than
	// externalHJRecursivePartitioningSizeDecreaseThreshold), it is likely that
	// the partition consists of the tuples not distinct on the equality columns,
	// so we fall back to using a combination of sort and merge join to process
	// such partition. After repartitioning, the operator transitions to
	// externalHJJoinNewPartition state.
	externalHJRecursivePartitioning
	// externalHJJoinNewPartition indicates that the operator should choose a
	// partition index and join the corresponding partitions from both sides
	// using the in-memory hash joiner. We will only join the partitions if the
	// right side partition fits into memory (because in-memory hash joiner will
	// fully buffer the right side but will process left side in the streaming
	// fashion). If there are no partition indices that the operator can join, it
	// transitions into externalHJRecursivePartitioning state. If there are no
	// partition indices to join using in-memory hash joiner, but there are
	// indices to join using sort + merge join strategy, the operator transitions
	// to externalHJSortMergeNewPartition state. If there are no partition
	// indices left at all to join, the operator transitions to
	// externalHJFinished state.
	externalHJJoinNewPartition
	// externalHJJoining indicates that the operator is currently joining tuples
	// from the corresponding partitions from both sides. An in-memory hash join
	// operator is used to perform the join. Once the in-memory operator returns
	// a zero-length batch (indicating that full output for the current
	// partitions has been emitted), the external hash joiner transitions to
	// externalHJJoinNewPartition state.
	externalHJJoining
	// externalHJSortMergeNewPartition indicates that the operator should choose
	// a partition index to join using sort + merge join strategy. If there are
	// no partition indices for this strategy left, the operator transitions to
	// externalHJFinished state.
	externalHJSortMergeNewPartition
	// externalHJSortMergeJoining indicates that the operator is currently
	// joining tuples from the corresponding partitions from both sides using
	// (disk-backed) sort + merge join strategy. Once the in-memory merge joiner
	// returns a zero-length batch (indicating that full output for the current
	// partitions has been emitted), the external hash joiner transitions to
	// externalHJSortMergeNewPartition state.
	externalHJSortMergeJoining
	// externalHJFinished indicates that the external hash joiner has emitted all
	// tuples already and only zero-length batch will be emitted from now on.
	externalHJFinished
)

const (
	// externalHJRecursivePartitioningSizeDecreaseThreshold determines by how
	// much the newly-created partitions in the recursive partitioning stage
	// should be smaller than the "parent" partition in order to consider the
	// repartitioning "successful". If this threshold is not met, then this newly
	// created partition will be added to sort + merge join list (which, in a
	// sense, serves as the base case for "recursion").
	externalHJRecursivePartitioningSizeDecreaseThreshold = 0.05
	// externalHJDiskQueuesMemFraction determines the fraction of the available
	// RAM that is allocated for the in-memory cache of disk queues.
	externalHJDiskQueuesMemFraction = 0.5
	// We need at least two buckets per side to make progress. However, the
	// minimum number of partitions necessary are the partitions in use during a
	// fallback to sort and merge join. We'll be using the minimum necessary per
	// input + 2 (1 for each spilling queue that the merge joiner uses). For
	// clarity this is what happens:
	// - The 2 partitions that need to be sorted + merged will use an FD each: 2
	//   FDs. Meanwhile, each sorter will use up to externalSorterMinPartitions to
	//   sort and partition this input. At this stage 2 + 2 *
	//   externalSorterMinPartitions FDs are used.
	// - Once the inputs (the hash joiner partitions) are finished, both FDs will
	//   be released. The merge joiner will now be in use, which uses two
	//   spillingQueues with 1 FD each for a total of 2. Since each sorter will
	//   use externalSorterMinPartitions, the FDs used at this stage are 2 +
	//   (2 * externalSorterMinPartitions) as well. Note that as soon as the
	//   sorter emits its first batch, it must be the case that the input to it
	//   has returned a zero batch, and thus the FD has been closed.
	sortMergeNonSortMinFDsOpen = 2
	externalHJMinPartitions    = sortMergeNonSortMinFDsOpen + (externalSorterMinPartitions * 2)
	// externalHJMinimalMaxRightPartitionSize determines the minimum value for
	// maxRightPartitionSizeToJoin variable of the external hash joiner.
	externalHJMinimalMaxRightPartitionSize = 64 << 10 /* 64 KiB */
)

// externalHashJoiner is an operator that performs Grace hash join algorithm
// and can spill to disk. The high level view is that it partitions the left
// and right side into large buckets by a hash function A, writes those buckets
// to disk, then iterates through pairs of those buckets and does a normal hash
// join with a different hash function B.
//
// In order to get different hash functions, we're using the same family of
// hash functions that in-memory hash joiner uses, but we will seed it with a
// different initial hash value.
//
// The operator works in two phases.
//
// Phase 1: partitioning
// In this phase, we iterate through both sides of the join, hashing every row
// using a hash function A that produces n partitions. This will produce n
// partitions for each side of the join, which will be persisted to disk
// separately. As memory fills up, each of these partitions is flushed to disk
// repeatedly until the inputs are exhausted.
//
// Phase 2: join
// Now, we retrieve pairs of partitions from disk and join each pair using the
// ordinary hash join algorithm (and a different hash function B). Since we're
// performing an equality join, we can guarantee that each row on the left side
// of the join, if it has a match, will be in the same partition on the right
// side of the join. So, it's safe to do the join in pieces, partition by
// partition.
//
// If one of the partitions itself runs out of memory, we can recursively apply
// this algorithm. The partition will be divided into sub-partitions by a new
// hash function, spilled to disk, and so on. If repartitioning doesn't reduce
// size of the partitions sufficiently, then such partitions will be handled
// using the combination of disk-backed sort and merge join operators.
type externalHashJoiner struct {
	twoInputNode
	NonExplainable
	closerHelper

	// mu is used to protect against concurrent IdempotentClose and Next calls,
	// which are currently allowed.
	// TODO(asubiotto): Explore calling IdempotentClose from the same goroutine as
	//  Next, which will simplify this model.
	mu syncutil.Mutex

	state              externalHashJoinerState
	unlimitedAllocator *colmem.Allocator
	spec               HashJoinerSpec
	diskQueueCfg       colcontainer.DiskQueueCfg

	// fdState is used to acquire file descriptors up front.
	fdState struct {
		fdSemaphore semaphore.Semaphore
		acquiredFDs int
	}

	// Partitioning phase variables.
	leftPartitioner  colcontainer.PartitionedQueue
	rightPartitioner colcontainer.PartitionedQueue
	tupleDistributor *tupleHashDistributor
	// maxNumberActivePartitions determines the maximum number of active
	// partitions that the operator is allowed to have. This number is computed
	// semi-dynamically and will influence the choice of numBuckets value.
	maxNumberActivePartitions int
	// numBuckets is the number of buckets that a partition is divided into.
	numBuckets int
	// partitionsToJoinUsingInMemHash is a map from partitionIdx to a utility
	// struct. This map contains all partition indices that need to be joined
	// using the in-memory hash joiner. If the partition is too big, it will be
	// tried to be repartitioned; if during repartitioning the size doesn't
	// decrease enough, it will be added to partitionsToJoinUsingSortMerge.
	partitionsToJoinUsingInMemHash map[int]*externalHJPartitionInfo
	// partitionsToJoinUsingSortMerge contains all partition indices that need to
	// be joined using sort + merge join strategy. Partition indices will be
	// added into this map if recursive partitioning doesn't seem to make
	// progress on partition' size reduction.
	partitionsToJoinUsingSortMerge []int
	// partitionIdxOffset stores the first "available" partition index to use.
	// During the partitioning step, all tuples will go into one of the buckets
	// in [partitionIdxOffset, partitionIdxOffset + numBuckets) range.
	partitionIdxOffset int
	// numRepartitions tracks the number of times the external hash joiner had to
	// recursively repartition another partition because the latter was too big
	// to join.
	numRepartitions int
	// scratch and recursiveScratch are helper structs.
	scratch, recursiveScratch struct {
		// Input sources can have different schemas, so when distributing tuples
		// (i.e. copying them into scratch batch to be spilled) we might need two
		// different batches.
		leftBatch, rightBatch coldata.Batch
	}

	// Join phase variables.
	leftJoinerInput, rightJoinerInput *partitionerToOperator
	inMemHashJoiner                   *hashJoiner
	// diskBackedSortMerge is a side chain of disk-backed sorters that feed into
	// disk-backed merge joiner which the external hash joiner can fall back to.
	diskBackedSortMerge ResettableOperator

	memState struct {
		// maxRightPartitionSizeToJoin indicates the maximum memory size of a
		// partition on the right side that we're ok with joining without having to
		// repartition it. We pay attention only to the right side because in-memory
		// hash joiner will buffer the whole right input before processing the left
		// input in a "streaming" fashion.
		maxRightPartitionSizeToJoin int64
	}

	testingKnobs struct {
		// numForcedRepartitions is a number of times that the external hash joiner
		// is forced to recursively repartition (even if it is otherwise not
		// needed) before it proceeds to actual join partitions.
		numForcedRepartitions int
		// delegateFDAcquisitions, if true, means that a test wants to force the
		// PartitionedDiskQueues to track the number of file descriptors the hash
		// joiner will open/close. This disables the default behavior of acquiring
		// all file descriptors up front in Next.
		delegateFDAcquisitions bool
	}
}

var _ closableOperator = &externalHashJoiner{}

type externalHJPartitionInfo struct {
	rightMemSize       int64
	rightParentMemSize int64
}

type joinSide int

const (
	leftSide joinSide = iota
	rightSide
)

// NewExternalHashJoiner returns a disk-backed hash joiner.
// - unlimitedAllocator must have been created with a memory account derived
// from an unlimited memory monitor. It will be used by several internal
// components of the external hash joiner which is responsible for making sure
// that the components stay within the memory limit.
// - numForcedRepartitions is a number of times that the external hash joiner
// is forced to recursively repartition (even if it is otherwise not needed).
// This should be non-zero only in tests.
// - delegateFDAcquisitions specifies whether the external hash joiner should
// let the partitioned disk queues acquire file descriptors instead of acquiring
// them up front in Next. Should be true only in tests.
func NewExternalHashJoiner(
	unlimitedAllocator *colmem.Allocator,
	spec HashJoinerSpec,
	leftInput, rightInput colexecbase.Operator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	createReusableDiskBackedSorter func(input colexecbase.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column, maxNumberPartitions int) (colexecbase.Operator, error),
	numForcedRepartitions int,
	delegateFDAcquisitions bool,
	diskAcc *mon.BoundAccount,
) colexecbase.Operator {
	if diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeClearAndReuseCache {
		colexecerror.InternalError(errors.Errorf("external hash joiner instantiated with suboptimal disk queue cache mode: %d", diskQueueCfg.CacheMode))
	}
	partitionedDiskQueueSemaphore := fdSemaphore
	if !delegateFDAcquisitions {
		// To avoid deadlocks with other disk queues, we manually attempt to acquire
		// the maximum number of descriptors all at once in Next. Passing in a nil
		// semaphore indicates that the caller will do the acquiring.
		partitionedDiskQueueSemaphore = nil
	}
	leftPartitioner := colcontainer.NewPartitionedDiskQueue(
		spec.left.sourceTypes, diskQueueCfg, partitionedDiskQueueSemaphore, colcontainer.PartitionerStrategyDefault, diskAcc,
	)
	leftJoinerInput := newPartitionerToOperator(
		unlimitedAllocator, spec.left.sourceTypes, leftPartitioner, 0, /* partitionIdx */
	)
	rightPartitioner := colcontainer.NewPartitionedDiskQueue(
		spec.right.sourceTypes, diskQueueCfg, partitionedDiskQueueSemaphore, colcontainer.PartitionerStrategyDefault, diskAcc,
	)
	rightJoinerInput := newPartitionerToOperator(
		unlimitedAllocator, spec.right.sourceTypes, rightPartitioner, 0, /* partitionIdx */
	)
	// With the default limit of 256 file descriptors, this results in 16
	// partitions. This is a hard maximum of partitions that will be used by the
	// external hash joiner. Below we check whether we have enough RAM to support
	// the caches of this number of partitions.
	// TODO(yuzefovich): this number should be tuned.
	maxNumberActivePartitions := fdSemaphore.GetLimit() / 16
	if diskQueueCfg.BufferSizeBytes > 0 {
		diskQueuesTotalMemLimit := int(float64(memoryLimit) * externalHJDiskQueuesMemFraction)
		numDiskQueuesThatFit := diskQueuesTotalMemLimit / diskQueueCfg.BufferSizeBytes
		if numDiskQueuesThatFit < maxNumberActivePartitions {
			maxNumberActivePartitions = numDiskQueuesThatFit
		}
	}
	if maxNumberActivePartitions < externalHJMinPartitions {
		maxNumberActivePartitions = externalHJMinPartitions
	}
	diskQueuesMemUsed := maxNumberActivePartitions * diskQueueCfg.BufferSizeBytes
	makeOrderingCols := func(eqCols []uint32) []execinfrapb.Ordering_Column {
		res := make([]execinfrapb.Ordering_Column, len(eqCols))
		for i, colIdx := range eqCols {
			res[i].ColIdx = colIdx
		}
		return res
	}
	// We need to allocate 2 FDs for reading the partitions (reused by the merge
	// joiner) that we need to join using sort + merge join strategy, and all
	// others are divided between the two inputs.
	externalSorterMaxNumberPartitions := (maxNumberActivePartitions - sortMergeNonSortMinFDsOpen) / 2
	if externalSorterMaxNumberPartitions < externalSorterMinPartitions {
		// This code gets a maximum number of partitions based on the semaphore
		// limit. In tests, this limit is set artificially low to catch any
		// violations of the limit, resulting in possibly computing a low number of
		// partitions for the sorter, which we overwrite here.
		externalSorterMaxNumberPartitions = externalSorterMinPartitions
	}
	leftOrdering := makeOrderingCols(spec.left.eqCols)
	leftPartitionSorter, err := createReusableDiskBackedSorter(
		leftJoinerInput, spec.left.sourceTypes, leftOrdering, externalSorterMaxNumberPartitions,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
	rightOrdering := makeOrderingCols(spec.right.eqCols)
	rightPartitionSorter, err := createReusableDiskBackedSorter(
		rightJoinerInput, spec.right.sourceTypes, rightOrdering, externalSorterMaxNumberPartitions,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
	diskBackedSortMerge, err := NewMergeJoinOp(
		unlimitedAllocator, memoryLimit, diskQueueCfg,
		partitionedDiskQueueSemaphore, spec.joinType, leftPartitionSorter, rightPartitionSorter,
		spec.left.sourceTypes, spec.right.sourceTypes, leftOrdering, rightOrdering,
		diskAcc,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
	ehj := &externalHashJoiner{
		twoInputNode:              newTwoInputNode(leftInput, rightInput),
		unlimitedAllocator:        unlimitedAllocator,
		spec:                      spec,
		diskQueueCfg:              diskQueueCfg,
		leftPartitioner:           leftPartitioner,
		rightPartitioner:          rightPartitioner,
		maxNumberActivePartitions: maxNumberActivePartitions,
		// In the initial partitioning state we will use half of available
		// partitions to write the partitioned input from the left side and another
		// half for the right side.
		// TODO(yuzefovich): figure out whether we should care about
		// hj.numBuckets being a power of two (finalizeHash step is faster if so).
		numBuckets:                     maxNumberActivePartitions / 2,
		partitionsToJoinUsingInMemHash: make(map[int]*externalHJPartitionInfo),
		partitionsToJoinUsingSortMerge: make([]int, 0),
		leftJoinerInput:                leftJoinerInput,
		rightJoinerInput:               rightJoinerInput,
		inMemHashJoiner: NewHashJoiner(
			unlimitedAllocator, spec, leftJoinerInput, rightJoinerInput,
		).(*hashJoiner),
		diskBackedSortMerge: diskBackedSortMerge,
	}
	ehj.fdState.fdSemaphore = fdSemaphore
	// To simplify the accounting, we will assume that the in-memory hash
	// joiner's memory usage is equal to the size of the right partition to be
	// joined (which will be fully buffered). This is an underestimate because a
	// single batch from the left partition will be read at a time as well as an
	// output batch will be used, but that shouldn't matter in the grand scheme
	// of things.
	ehj.memState.maxRightPartitionSizeToJoin = memoryLimit - int64(diskQueuesMemUsed)
	if ehj.memState.maxRightPartitionSizeToJoin < externalHJMinimalMaxRightPartitionSize {
		ehj.memState.maxRightPartitionSizeToJoin = externalHJMinimalMaxRightPartitionSize
	}
	ehj.scratch.leftBatch = unlimitedAllocator.NewMemBatch(spec.left.sourceTypes)
	ehj.recursiveScratch.leftBatch = unlimitedAllocator.NewMemBatch(spec.left.sourceTypes)
	sameSourcesSchema := len(spec.left.sourceTypes) == len(spec.right.sourceTypes)
	for i, leftType := range spec.left.sourceTypes {
		if i < len(spec.right.sourceTypes) && !leftType.Identical(spec.right.sourceTypes[i]) {
			sameSourcesSchema = false
		}
	}
	if sameSourcesSchema {
		// The schemas of both sources are the same, so we can reuse the left
		// scratch batch.
		ehj.scratch.rightBatch = ehj.scratch.leftBatch
		ehj.recursiveScratch.rightBatch = ehj.recursiveScratch.leftBatch
	} else {
		ehj.scratch.rightBatch = unlimitedAllocator.NewMemBatch(spec.right.sourceTypes)
		ehj.recursiveScratch.rightBatch = unlimitedAllocator.NewMemBatch(spec.right.sourceTypes)
	}
	ehj.testingKnobs.numForcedRepartitions = numForcedRepartitions
	ehj.testingKnobs.delegateFDAcquisitions = delegateFDAcquisitions
	return ehj
}

func (hj *externalHashJoiner) Init() {
	hj.inputOne.Init()
	hj.inputTwo.Init()
	// In the join phase, hash join operator will use the default init hash
	// value, so in order to use a "different" hash function in the partitioning
	// phase we use a different init hash value.
	hj.tupleDistributor = newTupleHashDistributor(
		defaultInitHashValue+1, hj.numBuckets,
	)
	hj.state = externalHJInitialPartitioning
}

func (hj *externalHashJoiner) partitionBatch(
	ctx context.Context, batch coldata.Batch, side joinSide, parentMemSize int64,
) {
	batchLen := batch.Length()
	if batchLen == 0 {
		return
	}
	scratchBatch := hj.scratch.leftBatch
	sourceSpec := hj.spec.left
	partitioner := hj.leftPartitioner
	if side == rightSide {
		scratchBatch = hj.scratch.rightBatch
		sourceSpec = hj.spec.right
		partitioner = hj.rightPartitioner
	}
	selections := hj.tupleDistributor.distribute(
		ctx, batch, sourceSpec.sourceTypes, sourceSpec.eqCols,
	)
	for idx, sel := range selections {
		partitionIdx := hj.partitionIdxOffset + idx
		if len(sel) > 0 {
			scratchBatch.ResetInternalBatch()
			// The partitioner expects the batches without a selection vector, so we
			// need to copy the tuples according to the selection vector into a
			// scratch batch.
			colVecs := scratchBatch.ColVecs()
			hj.unlimitedAllocator.PerformOperation(colVecs, func() {
				for i, colvec := range colVecs {
					colvec.Copy(coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:       batch.ColVec(i),
							Sel:       sel,
							SrcEndIdx: len(sel),
						},
					})
				}
				scratchBatch.SetLength(len(sel))
			})
			if err := partitioner.Enqueue(ctx, partitionIdx, scratchBatch); err != nil {
				colexecerror.InternalError(err)
			}
			partitionInfo, ok := hj.partitionsToJoinUsingInMemHash[partitionIdx]
			if !ok {
				partitionInfo = &externalHJPartitionInfo{}
				hj.partitionsToJoinUsingInMemHash[partitionIdx] = partitionInfo
			}
			if side == rightSide {
				partitionInfo.rightParentMemSize = parentMemSize
				// We cannot use allocator's methods directly because those
				// look at the capacities of the vectors, and in our case only
				// first len(sel) tuples belong to the "current" batch.
				partitionInfo.rightMemSize += colmem.GetProportionalBatchMemSize(scratchBatch, int64(len(sel)))
			}
		}
	}
}

func (hj *externalHashJoiner) Next(ctx context.Context) coldata.Batch {
	hj.mu.Lock()
	defer hj.mu.Unlock()
StateChanged:
	for {
		switch hj.state {
		case externalHJInitialPartitioning:
			leftBatch := hj.inputOne.Next(ctx)
			rightBatch := hj.inputTwo.Next(ctx)
			if leftBatch.Length() == 0 && rightBatch.Length() == 0 {
				// Both inputs have been partitioned and spilled, so we transition to
				// "joining" phase. Close all the open write file descriptors.
				//
				// TODO(yuzefovich): this will also clear the cache once the new PR is
				// in. This means we will reallocate a cache whenever reading from the
				// partitions. What I think we might want to do is not close the
				// partitions here. Instead, we move on to joining, which will switch
				// all of these reserved file descriptors to read in the best case (no
				// repartitioning) and reuse the cache. Only if we need to repartition
				// should we CloseAllOpenWriteFileDescriptors of both sides. It might
				// also be more efficient to Dequeue from the partitions you'll read
				// from before doing that to exempt them from releasing their FDs to
				// the semaphore.
				if err := hj.leftPartitioner.CloseAllOpenWriteFileDescriptors(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				if err := hj.rightPartitioner.CloseAllOpenWriteFileDescriptors(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				hj.inMemHashJoiner.Init()
				hj.partitionIdxOffset += hj.numBuckets
				hj.state = externalHJJoinNewPartition
				continue
			}
			if !hj.testingKnobs.delegateFDAcquisitions && hj.fdState.acquiredFDs == 0 {
				toAcquire := hj.maxNumberActivePartitions
				if err := hj.fdState.fdSemaphore.Acquire(ctx, toAcquire); err != nil {
					colexecerror.InternalError(err)
				}
				hj.fdState.acquiredFDs = toAcquire
			}
			hj.partitionBatch(ctx, leftBatch, leftSide, math.MaxInt64)
			hj.partitionBatch(ctx, rightBatch, rightSide, math.MaxInt64)

		case externalHJRecursivePartitioning:
			hj.numRepartitions++
			if log.V(2) && hj.numRepartitions%10 == 0 {
				log.Infof(ctx,
					"external hash joiner is performing %d'th repartition", hj.numRepartitions,
				)
			}
			// In order to use a different hash function when repartitioning, we need
			// to increase the seed value of the tuple distributor.
			hj.tupleDistributor.initHashValue++
			// We're actively will be using hj.numBuckets + 1 partitions (because
			// we're repartitioning one side at a time), so we can set hj.numBuckets
			// higher than in the initial partitioning step.
			// TODO(yuzefovich): figure out whether we should care about
			// hj.numBuckets being a power of two (finalizeHash step is faster if so).
			hj.numBuckets = hj.maxNumberActivePartitions - 1
			hj.tupleDistributor.resetNumOutputs(hj.numBuckets)
			for parentPartitionIdx, parentPartitionInfo := range hj.partitionsToJoinUsingInMemHash {
				for _, side := range []joinSide{leftSide, rightSide} {
					batch := hj.recursiveScratch.leftBatch
					partitioner := hj.leftPartitioner
					memSize := int64(math.MaxInt64)
					if side == rightSide {
						batch = hj.recursiveScratch.rightBatch
						partitioner = hj.rightPartitioner
						memSize = parentPartitionInfo.rightMemSize
					}
					for {
						if err := partitioner.Dequeue(ctx, parentPartitionIdx, batch); err != nil {
							colexecerror.InternalError(err)
						}
						if batch.Length() == 0 {
							break
						}
						hj.partitionBatch(ctx, batch, side, memSize)
					}
					// We're done reading from this partition, and it will never be read
					// from again, so we can close it.
					if err := partitioner.CloseInactiveReadPartitions(ctx); err != nil {
						colexecerror.InternalError(err)
					}
					// We're done writing to the newly created partitions.
					// TODO(yuzefovich): we should not release the descriptors here. The
					// invariant should be: we're entering
					// externalHJRecursivePartitioning, at that stage we have at most
					// numBuckets*2 file descriptors open. At the top of the state
					// transition, close all open write file descriptors, which should
					// reduce the open descriptors to 0. Now we open the two read'
					// partitions for 2 file descriptors and whatever number of write
					// partitions we want. This'll allow us to remove the call to
					// CloseAllOpen... in the first state as well.
					if err := partitioner.CloseAllOpenWriteFileDescriptors(ctx); err != nil {
						colexecerror.InternalError(err)
					}
				}
				for idx := 0; idx < hj.numBuckets; idx++ {
					newPartitionIdx := hj.partitionIdxOffset + idx
					if partitionInfo, ok := hj.partitionsToJoinUsingInMemHash[newPartitionIdx]; ok {
						before, after := partitionInfo.rightParentMemSize, partitionInfo.rightMemSize
						if before > 0 {
							sizeDecrease := 1.0 - float64(after)/float64(before)
							if sizeDecrease < externalHJRecursivePartitioningSizeDecreaseThreshold {
								// We will need to join this partition using sort + merge
								// join strategy.
								hj.partitionsToJoinUsingSortMerge = append(hj.partitionsToJoinUsingSortMerge, newPartitionIdx)
								delete(hj.partitionsToJoinUsingInMemHash, newPartitionIdx)
							}
						}
					}
				}
				// We have successfully repartitioned the partitions with index
				// 'parentPartitionIdx' on both sides, so we delete that index from the
				// map and proceed on joining the newly created partitions.
				delete(hj.partitionsToJoinUsingInMemHash, parentPartitionIdx)
				hj.partitionIdxOffset += hj.numBuckets
				hj.state = externalHJJoinNewPartition
				continue StateChanged
			}

		case externalHJJoinNewPartition:
			if hj.testingKnobs.numForcedRepartitions > 0 && len(hj.partitionsToJoinUsingInMemHash) > 0 {
				hj.testingKnobs.numForcedRepartitions--
				hj.state = externalHJRecursivePartitioning
				continue
			}
			// Find next partition that we can join without having to recursively
			// repartition.
			for partitionIdx, partitionInfo := range hj.partitionsToJoinUsingInMemHash {
				if partitionInfo.rightMemSize <= hj.memState.maxRightPartitionSizeToJoin {
					// Update the inputs to in-memory hash joiner and reset the latter.
					hj.leftJoinerInput.partitionIdx = partitionIdx
					hj.rightJoinerInput.partitionIdx = partitionIdx
					hj.inMemHashJoiner.reset(ctx)
					delete(hj.partitionsToJoinUsingInMemHash, partitionIdx)
					hj.state = externalHJJoining
					continue StateChanged
				}
			}
			if len(hj.partitionsToJoinUsingInMemHash) == 0 {
				// All partitions to join using the hash joiner have been processed.
				if len(hj.partitionsToJoinUsingSortMerge) > 0 {
					// But there are still some partitions to join using sort + merge
					// join strategy.
					hj.diskBackedSortMerge.Init()
					if log.V(2) {
						log.Infof(ctx,
							"external hash joiner will join %d partitions using sort + merge join",
							len(hj.partitionsToJoinUsingSortMerge),
						)
					}
					hj.state = externalHJSortMergeNewPartition
					continue
				}
				// All partitions have been processed, so we transition to finished
				// state.
				hj.state = externalHJFinished
				continue
			}
			// We have partitions that we cannot join without recursively
			// repartitioning first, so we transition to the corresponding state.
			hj.state = externalHJRecursivePartitioning
			continue

		case externalHJJoining:
			b := hj.inMemHashJoiner.Next(ctx)
			if b.Length() == 0 {
				// We're done joining these partitions, so we close them and transition
				// to joining new ones.
				if err := hj.leftPartitioner.CloseInactiveReadPartitions(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				if err := hj.rightPartitioner.CloseInactiveReadPartitions(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				hj.state = externalHJJoinNewPartition
				continue
			}
			return b

		case externalHJSortMergeNewPartition:
			if len(hj.partitionsToJoinUsingSortMerge) == 0 {
				// All partitions have been processed, so we transition to finished
				// state.
				hj.state = externalHJFinished
				continue
			}
			partitionIdx := hj.partitionsToJoinUsingSortMerge[0]
			hj.partitionsToJoinUsingSortMerge = hj.partitionsToJoinUsingSortMerge[1:]
			// Update the inputs to sort + merge joiner and reset that chain.
			hj.leftJoinerInput.partitionIdx = partitionIdx
			hj.rightJoinerInput.partitionIdx = partitionIdx
			hj.diskBackedSortMerge.reset(ctx)
			hj.state = externalHJSortMergeJoining
			continue

		case externalHJSortMergeJoining:
			b := hj.diskBackedSortMerge.Next(ctx)
			if b.Length() == 0 {
				// We're done joining these partitions, so we close them and transition
				// to joining new ones.
				if err := hj.leftPartitioner.CloseInactiveReadPartitions(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				if err := hj.rightPartitioner.CloseInactiveReadPartitions(ctx); err != nil {
					colexecerror.InternalError(err)
				}
				hj.state = externalHJSortMergeNewPartition
				continue
			}
			return b

		case externalHJFinished:
			if err := hj.idempotentCloseLocked(ctx); err != nil {
				colexecerror.InternalError(err)
			}
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(fmt.Sprintf("unexpected externalHashJoinerState %d", hj.state))
		}
	}
}

func (hj *externalHashJoiner) IdempotentClose(ctx context.Context) error {
	hj.mu.Lock()
	defer hj.mu.Unlock()
	return hj.idempotentCloseLocked(ctx)
}

func (hj *externalHashJoiner) idempotentCloseLocked(ctx context.Context) error {
	if !hj.close() {
		return nil
	}
	var retErr error
	if err := hj.leftPartitioner.Close(ctx); err != nil {
		retErr = err
	}
	if err := hj.rightPartitioner.Close(ctx); err != nil && retErr == nil {
		retErr = err
	}
	if c, ok := hj.diskBackedSortMerge.(IdempotentCloser); ok {
		if err := c.IdempotentClose(ctx); err != nil && retErr == nil {
			retErr = err
		}
	}
	if !hj.testingKnobs.delegateFDAcquisitions && hj.fdState.acquiredFDs > 0 {
		hj.fdState.fdSemaphore.Release(hj.fdState.acquiredFDs)
		hj.fdState.acquiredFDs = 0
	}
	return retErr
}
