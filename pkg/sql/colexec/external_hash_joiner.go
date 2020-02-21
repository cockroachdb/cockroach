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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
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
	// partition (the difference is less than
	// externalHJRecursivePartitioningSizeDecreaseThreshold), it is likely that
	// the partition consists of the tuples not distinct on the equality columns,
	// so we fall back to using a combination of sort and merge join to process
	// such partition. After repartitioning, the operator transitions to
	// externalHJJoinNewPartition state.
	// TODO(yuzefovich): implement the fallback to sort + merge join.
	externalHJRecursivePartitioning
	// externalHJJoinNewPartition indicates that the operator should choose a
	// partition index and join the corresponding partitions from both sides. We
	// will only join the partitions if the right side partition fits into memory
	// (because in-memory hash joiner will fully buffer the right side). If there
	// are no partition indices that the operator can join, it transitions into
	// externalHJRecursivePartitioning state. If there are no partition indices
	// left at all to join, the operator transitions to externalHJFinished state.
	externalHJJoinNewPartition
	// externalHJJoining indicates that the operator is currently joining tuples
	// from the corresponding partitions from both sides. An in-memory hash join
	// operator is used to perform the join. Once the in-memory operator returns
	// a zero-length batch (indicating that full output for the current
	// partitions has been emitted), the external hash joiner transitions to
	// externalHJJoinNewPartition state.
	externalHJJoining
	// externalHJFinished indicates that the external hash joiner has emitted all
	// tuples already and only zero-length batch will be emitted from now on.
	externalHJFinished
)

const (
	// externalHJMaxNumBucketsPerSide is the hard maximum of buckets that the
	// external hash joiner will use for both initial and recursive partitioning.
	externalHJMaxNumBucketsPerSide = 16
	// externalHJRecursivePartitioningSizeDecreaseThreshold determines by how
	// much the newly-created partitions in the recursive partitioning stage
	// should be smaller than the "parent" partition in order to consider the
	// repartitioning "successful". If this threshold is not met, then we fall
	// back to sort + merge join (which, in a sense, serves as the base case
	// for "recursion").
	externalHJRecursivePartitioningSizeDecreaseThreshold = 0.05
	// externalHJDiskQueuesMemFraction determines the fraction of the available
	// RAM that is allocated for the in-memory cache of disk queues.
	externalHJDiskQueuesMemFraction      = 0.5
	externalHJFallbackToSortMergeJoinMsg = "recursive partitioning didn't sufficiently decrease the size of the partition"
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
// hash function, spilled to disk, and so on.
type externalHashJoiner struct {
	twoInputNode
	NonExplainable

	state              externalHashJoinerState
	unlimitedAllocator *Allocator
	spec               hashJoinerSpec

	// Partitioning phase variables.
	leftPartitioner  Partitioner
	rightPartitioner Partitioner
	tupleDistributor *tupleHashDistributor
	// numBuckets is the number of buckets that a partition is divided into.
	numBuckets int
	// partitionsToJoin is a map from partitionIdx to a utility struct. This map
	// contains all partition indices that need to be joined.
	partitionsToJoin map[int]*externalHJPartitionInfo
	// partitionIdxOffset stores the first "available" partition index to use.
	// During the partitioning step, all tuples will go into one of the buckets
	// in [partitionIdxOffset, partitionIdxOffset + numBuckets) range.
	partitionIdxOffset int
	// TODO(yuzefovich): remove recursiveScratch once actual disk queues are
	// used.
	scratch, recursiveScratch struct {
		// Input sources can have different schemas, so when distributing tuples
		// (i.e. copying them into scratch batch to be spilled) we might need two
		// different batches.
		leftBatch, rightBatch coldata.Batch
	}

	// Join phase variables.
	leftInMemHashJoinerInput, rightInMemHashJoinerInput *partitionerToOperator
	inMemHashJoiner                                     *hashJoiner

	memState struct {
		// maxRightPartitionSizeToJoin indicates the maximum memory size of a
		// partition on the right side that we're ok with joining without having to
		// repartition it. We pay attention only to the right side because in-memory
		// hash joiner will buffer the whole right input before processing the left
		// input in a "streaming" fashion.
		maxRightPartitionSizeToJoin int64
		memUsagePerDiskQueue        int
		diskQueuesTotalMemLimit     int
	}
}

var _ Operator = &externalHashJoiner{}

type externalHJPartitionInfo struct {
	rightMemSize       int64
	rightParentMemSize int64
}

type joinSide int

const (
	leftSide joinSide = iota
	rightSide
)

// newExternalHashJoiner returns a disk-backed hash joiner.
// - unlimitedAllocator must have been created with a memory account derived
// from an unlimited memory monitor. It will be used by several internal
// components of the external hash joiner which is responsible for making sure
// that the components stay within the memory limit.
// - diskQueuesUnlimitedAllocator is a (temporary) unlimited allocator that
// will be used to create dummy queues and will be removed once we have actual
// disk-backed queues.
func newExternalHashJoiner(
	unlimitedAllocator *Allocator,
	spec hashJoinerSpec,
	leftInput, rightInput Operator,
	memoryLimit int64,
	memUsagePerDiskQueue int,
	// TODO(yuzefovich): remove this once actual disk queues are in-place.
	diskQueuesUnlimitedAllocator *Allocator,
) Operator {
	leftPartitioner := newDummyPartitioner(diskQueuesUnlimitedAllocator, spec.left.sourceTypes)
	leftInMemHashJoinerInput := newPartitionerToOperator(
		unlimitedAllocator, spec.left.sourceTypes, leftPartitioner, 0, /* partitionIdx */
	)
	rightPartitioner := newDummyPartitioner(diskQueuesUnlimitedAllocator, spec.right.sourceTypes)
	rightInMemHashJoinerInput := newPartitionerToOperator(
		unlimitedAllocator, spec.right.sourceTypes, rightPartitioner, 0, /* partitionIdx */
	)
	diskQueuesTotalMemLimit := int(float64(memoryLimit) * externalHJDiskQueuesMemFraction)
	numBucketsPerSide := externalHJMaxNumBucketsPerSide
	if memUsagePerDiskQueue > 0 {
		numDiskQueuesThatFit := diskQueuesTotalMemLimit / memUsagePerDiskQueue
		if numDiskQueuesThatFit/2 < numBucketsPerSide {
			// We divide by 2 because we might need a queue on both sides for each
			// bucket.
			numBucketsPerSide = numDiskQueuesThatFit / 2
		}
	}
	ehj := &externalHashJoiner{
		twoInputNode:              newTwoInputNode(leftInput, rightInput),
		unlimitedAllocator:        unlimitedAllocator,
		spec:                      spec,
		leftPartitioner:           leftPartitioner,
		rightPartitioner:          rightPartitioner,
		numBuckets:                numBucketsPerSide,
		partitionsToJoin:          make(map[int]*externalHJPartitionInfo),
		leftInMemHashJoinerInput:  leftInMemHashJoinerInput,
		rightInMemHashJoinerInput: rightInMemHashJoinerInput,
		inMemHashJoiner: newHashJoiner(
			unlimitedAllocator, spec, leftInMemHashJoinerInput, rightInMemHashJoinerInput,
		).(*hashJoiner),
	}
	// To simplify the accounting, we will assume that the in-memory hash
	// joiner's memory usage is equal to the size of the right partition to be
	// joined (which will be fully buffered). This is an underestimate because a
	// single batch from the left partition will be read at a time as well as an
	// output batch will be used, but that shouldn't matter in the grand scheme
	// of things.
	ehj.memState.maxRightPartitionSizeToJoin = memoryLimit - int64(diskQueuesTotalMemLimit)
	ehj.memState.memUsagePerDiskQueue = memUsagePerDiskQueue
	ehj.memState.diskQueuesTotalMemLimit = diskQueuesTotalMemLimit
	ehj.scratch.leftBatch = unlimitedAllocator.NewMemBatch(spec.left.sourceTypes)
	ehj.recursiveScratch.leftBatch = unlimitedAllocator.NewMemBatch(spec.left.sourceTypes)
	sameSourcesSchema := len(spec.left.sourceTypes) == len(spec.right.sourceTypes)
	for i, leftType := range spec.left.sourceTypes {
		if i < len(spec.right.sourceTypes) && leftType != spec.right.sourceTypes[i] {
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
	if batch.Length() == 0 {
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
							ColType:   sourceSpec.sourceTypes[i],
							Src:       batch.ColVec(i),
							Sel:       sel,
							SrcEndIdx: uint64(len(sel)),
						},
					})
				}
				scratchBatch.SetLength(uint16(len(sel)))
			})
			if err := partitioner.Enqueue(partitionIdx, scratchBatch); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			partitionInfo, ok := hj.partitionsToJoin[partitionIdx]
			if !ok {
				partitionInfo = &externalHJPartitionInfo{}
				hj.partitionsToJoin[partitionIdx] = partitionInfo
			}
			if side == rightSide {
				partitionInfo.rightParentMemSize = parentMemSize
				// We cannot use allocator's methods directly because those look at the
				// capacities of the vectors, and in our case only first len(sel)
				// tuples belong to the "current" batch. Also, there is no selection
				// vectors on the enqueued batch, so we don't need to worry about that.
				curBatchMemSize := getVecsSize(colVecs) / int64(coldata.BatchSize()) * int64(len(sel))
				partitionInfo.rightMemSize += curBatchMemSize
			}
		}
	}
}

func (hj *externalHashJoiner) Next(ctx context.Context) coldata.Batch {
	for {
		switch hj.state {
		case externalHJInitialPartitioning:
			leftBatch := hj.inputOne.Next(ctx)
			rightBatch := hj.inputTwo.Next(ctx)
			if leftBatch.Length() == 0 && rightBatch.Length() == 0 {
				// Both inputs have been partitioned and spilled, so we transition to
				// "joining" phase.
				hj.inMemHashJoiner.Init()
				hj.partitionIdxOffset += hj.numBuckets
				hj.state = externalHJJoinNewPartition
				continue
			}
			hj.partitionBatch(ctx, leftBatch, leftSide, math.MaxInt64)
			hj.partitionBatch(ctx, rightBatch, rightSide, math.MaxInt64)

		case externalHJRecursivePartitioning:
			// In order to use a different hash function when repartitioning, we need
			// to increase the seed value of the tuple distributor.
			hj.tupleDistributor.initHashValue++
			hj.numBuckets = externalHJMaxNumBucketsPerSide
			if hj.memState.memUsagePerDiskQueue > 0 {
				// At this point we have 2 * len(hj.partitionsToJoin) of active disk
				// queues, so we need to calculate how much RAM is available for the
				// cache of new disk queues we're about to create, so we also need to
				// update how many buckets we're using when repartitioning.
				diskQueuesUsedMem := 2 * len(hj.partitionsToJoin) * hj.memState.memUsagePerDiskQueue
				diskQueuesAvailableMem := hj.memState.diskQueuesTotalMemLimit - diskQueuesUsedMem
				numAvailableDiskQueues := diskQueuesAvailableMem / hj.memState.memUsagePerDiskQueue
				if numAvailableDiskQueues/2 < hj.numBuckets {
					hj.numBuckets = numAvailableDiskQueues / 2
				}
				if hj.numBuckets < 2 {
					// We need at least two buckets to make progress.
					hj.numBuckets = 2
				}
			}
			hj.tupleDistributor.resetNumOutputs(hj.numBuckets)
			for parentPartitionIdx, parentPartitionInfo := range hj.partitionsToJoin {
				for {
					leftBatch := hj.recursiveScratch.leftBatch
					if err := hj.leftPartitioner.Dequeue(parentPartitionIdx, leftBatch); err != nil {
						execerror.VectorizedInternalPanic(err)
					}
					if leftBatch.Length() == 0 {
						break
					}
					hj.partitionBatch(ctx, leftBatch, leftSide, math.MaxInt64)
				}
				for {
					rightBatch := hj.recursiveScratch.rightBatch
					if err := hj.rightPartitioner.Dequeue(parentPartitionIdx, rightBatch); err != nil {
						execerror.VectorizedInternalPanic(err)
					}
					if rightBatch.Length() == 0 {
						break
					}
					hj.partitionBatch(ctx, rightBatch, rightSide, parentPartitionInfo.rightMemSize)
				}
				for idx := 0; idx < hj.numBuckets; idx++ {
					if partitionInfo, ok := hj.partitionsToJoin[hj.partitionIdxOffset+idx]; ok {
						before, after := partitionInfo.rightParentMemSize, partitionInfo.rightMemSize
						if before > 0 {
							sizeDecrease := 1.0 - float64(after)/float64(before)
							if sizeDecrease < externalHJRecursivePartitioningSizeDecreaseThreshold {
								// TODO(yuzefovich): support this case by falling back to
								// sort + merge join.
								execerror.VectorizedInternalPanic(errors.Errorf(
									"%s: before %s, after %s", externalHJFallbackToSortMergeJoinMsg,
									humanize.Bytes(uint64(before)), humanize.Bytes(uint64(after))))
							}
						}
					}
				}
				// We have successfully repartitioned the partitions with index
				// 'parentPartitionIdx' on both sides, so we delete that index from the
				// map and proceed on joining the newly created partitions.
				delete(hj.partitionsToJoin, parentPartitionIdx)
				hj.partitionIdxOffset += hj.numBuckets
				hj.state = externalHJJoinNewPartition
				return hj.Next(ctx)
			}

		case externalHJJoinNewPartition:
			// Find next partition that we can join without having to recursively
			// repartition.
			for partitionIdx, partitionInfo := range hj.partitionsToJoin {
				if partitionInfo.rightMemSize <= hj.memState.maxRightPartitionSizeToJoin {
					// Update the inputs to in-memory hash joiner and reset the latter.
					hj.leftInMemHashJoinerInput.partitionIdx = partitionIdx
					hj.rightInMemHashJoinerInput.partitionIdx = partitionIdx
					hj.inMemHashJoiner.reset()
					hj.state = externalHJJoining
					delete(hj.partitionsToJoin, partitionIdx)
					return hj.Next(ctx)
				}
			}
			if len(hj.partitionsToJoin) == 0 {
				// All partitions have been processed, so we clean up the disk
				// infrastructure and transition to finished state.
				if err := hj.leftPartitioner.Close(); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				if err := hj.rightPartitioner.Close(); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				hj.state = externalHJFinished
				continue
			}
			// We have partitions that we cannot join without recursively
			// repartitioning first, so we transition to the corresponding state.
			hj.state = externalHJRecursivePartitioning

		case externalHJJoining:
			b := hj.inMemHashJoiner.Next(ctx)
			if b.Length() == 0 {
				hj.state = externalHJJoinNewPartition
				continue
			}
			return b
		case externalHJFinished:
			return coldata.ZeroBatch
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected externalHashJoinerState %d", hj.state))
		}
	}
}
