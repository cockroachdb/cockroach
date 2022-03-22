// Copyright 2019 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
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
	"github.com/marusama/semaphore"
)

// externalSorterState indicates the current state of the external sorter.
type externalSorterState int

const (
	// externalSorterNewPartition indicates that the next batch we read should
	// start a new partition. A zero-length batch in this state indicates that
	// the input to the external sorter has been fully consumed and we should
	// proceed to merging the partitions.
	externalSorterNewPartition externalSorterState = iota
	// externalSorterSpillPartition indicates that the next batch we read should
	// be added to the last partition so far. A zero-length batch in this state
	// indicates that the end of the partition has been reached and we should
	// transition to starting a new partition. If maxNumberPartitions is reached
	// in this state, the sorter will transition to
	// externalSorterRepeatedMerging to reduce the number of partitions.
	externalSorterSpillPartition
	// externalSorterRepeatedMerging indicates that we need to merge
	// maxNumberPartitions into one and spill that new partition to disk. When
	// finished, the sorter will transition to externalSorterNewPartition.
	externalSorterRepeatedMerging
	// externalSorterFinalMerging indicates that we have fully consumed the
	// input and can merge all of the partitions in one step. We then transition
	// to externalSorterEmitting state.
	externalSorterFinalMerging
	// externalSorterEmitting indicates that we are ready to emit output. A
	// zero-length batch in this state indicates that we have emitted all
	// tuples and should transition to externalSorterFinished state.
	externalSorterEmitting
	// externalSorterFinished indicates that all tuples from all partitions have
	// been emitted and from now on only a zero-length batch will be emitted by
	// the external sorter. This state is also responsible for closing the
	// partitions.
	externalSorterFinished
)

// externalSorter is an Operator that performs external merge sort. It works in
// two stages:
// 1. it will use a combination of an input partitioner and in-memory sorter to
// divide up all batches from the input into partitions, sort each partition in
// memory, and write sorted partitions to disk
// 2. it will use OrderedSynchronizer to merge the partitions.
//
// The (simplified) diagram of the components involved is as follows:
//
//                      input
//                        |
//                        ↓
//                 input partitioner
//                        |
//                        ↓
//                 in-memory sorter
//                        |
//                        ↓
//    ------------------------------------------
//   |             external sorter              |
//   |             ---------------              |
//   |                                          |
//   | partition1     partition2 ... partitionN |
//   |     |              |              |      |
//   |     ↓              ↓              ↓      |
//   |      merger (ordered synchronizer)       |
//    ------------------------------------------
//                        |
//                        ↓
//                      output
//
// There are a couple of implicit upstream links in the setup:
// - input partitioner checks the allocator used by the in-memory sorter to see
// whether a new partition must be started
// - external sorter resets in-memory sorter (which, in turn, resets input
// partitioner) once the full partition has been spilled to disk.
//
// What is hidden in the diagram is the fact that at some point we might need
// to merge several partitions into a new one that we spill to disk in order to
// reduce the number of "active" partitions. This requirement comes from the
// need to limit the number of "active" partitions because each partition uses
// some amount of RAM for its buffer. This is determined by
// maxNumberPartitions variable.
type externalSorter struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable
	colexecop.CloserHelper

	// mergeUnlimitedAllocator is used to track the memory under the batches
	// dequeued from partitions during the merge operation.
	mergeUnlimitedAllocator *colmem.Allocator
	// outputUnlimitedAllocator is used to track the memory under the output
	// batch in the merge operation.
	outputUnlimitedAllocator *colmem.Allocator
	// mergeMemoryLimit determines the amount of RAM available during the merge
	// operation. This will be roughly a half of the total limit and is used by
	// the dequeued batches and the output batch.
	mergeMemoryLimit int64

	state      externalSorterState
	inputTypes []*types.T
	ordering   execinfrapb.Ordering
	// topK, if non-zero, indicates the number of tuples needed by the output.
	// Each partition will be limited by this number in size.
	topK uint64
	// columnOrdering is the same as ordering used when creating mergers.
	columnOrdering     colinfo.ColumnOrdering
	inMemSorter        colexecop.ResettableOperator
	inMemSorterInput   *inputPartitioningOperator
	partitioner        colcontainer.PartitionedQueue
	partitionerCreator func() colcontainer.PartitionedQueue
	// partitionerToOperators stores all partitionerToOperator instances that we
	// have created when merging partitions. This allows for reusing them in
	// case we need to perform repeated merging (namely, we'll be able to reuse
	// their internal batches from dequeueing from disk).
	partitionerToOperators []*partitionerToOperator
	// numPartitions is the current number of partitions.
	numPartitions int
	// maxNumberPartitions determines the maximum number of active partitions
	// we can have at once. This number can be limited by the number of FDs
	// available as well as by dynamically computed limit when the memory usage
	// of the merge operation exceeds its allowance.
	maxNumberPartitions int
	// maxNumberPartitionsDynamicallyReduced is true when maxNumberPartitions
	// has been reduced based on the memory usage of the merge operation. Once
	// it is true, we won't reduce maxNumberPartitions any further.
	maxNumberPartitionsDynamicallyReduced bool
	numForcedMerges                       int
	// emitted is the number of tuples emitted by the externalSorter so far, and
	// is used if there is a topK limit to only emit topK tuples.
	emitted uint64

	// partitionsInfo tracks some information about all current partitions
	// (those in currentPartitionIdxs).
	partitionsInfo struct {
		// tupleCount stores the number of tuples in each partition.
		tupleCount []uint64
		// totalSize is used for logging purposes.
		totalSize []int64
		// maxBatchMemSize is used when determining how many partitions to have
		// at once, potentially reducing maxNumberPartitions.
		maxBatchMemSize []int64
	}

	// currentPartitionIdx keeps track of the next available partition index.
	currentPartitionIdx int
	// currentPartitionIdxs is a slice of size maxNumberPartitions containing
	// the mapping of all partitions to their corresponding partition indices.
	currentPartitionIdxs []int
	// maxMerged is a slice of size maxNumberPartitions containing
	// the mapping of all partitions to their maximum number of merges.
	maxMerged []int

	// fdState is used to acquire file descriptors up front.
	fdState struct {
		fdSemaphore semaphore.Semaphore
		acquiredFDs int
	}

	emitter colexecop.Operator

	testingKnobs struct {
		// delegateFDAcquisitions if true, means that a test wants to force the
		// PartitionedDiskQueues to track the number of file descriptors the
		// hash joiner will open/close. This disables the default behavior of
		// acquiring all file descriptors up front in Next.
		delegateFDAcquisitions bool
	}
}

var _ colexecop.ResettableOperator = &externalSorter{}
var _ colexecop.ClosableOperator = &externalSorter{}

// NewExternalSorter returns a disk-backed general sort operator.
// - unlimitedAllocators must have been created with a memory account derived
// from an unlimited memory monitor. They will be used by several internal
// components of the external sort which is responsible for making sure that
// the components stay within the memory limit.
// - topK, if non-zero, indicates the number of tuples needed by the output.
// - maxNumberPartitions (when non-zero) overrides the semi-dynamically
// computed maximum number of partitions to have at once.
// - numForcedMerges (when non-zero) specifies the number of times the repeated
// merging occurs even if the maximum number of partitions hasn't been reached.
// It should only be used in tests.
// - delegateFDAcquisitions specifies whether the external sorter should let
// the partitioned disk queue acquire file descriptors instead of acquiring
// them up front in Next. This should only be true in tests.
func NewExternalSorter(
	sortUnlimitedAllocator *colmem.Allocator,
	mergeUnlimitedAllocator *colmem.Allocator,
	outputUnlimitedAllocator *colmem.Allocator,
	input colexecop.Operator,
	inputTypes []*types.T,
	ordering execinfrapb.Ordering,
	topK uint64,
	matchLen int,
	memoryLimit int64,
	maxNumberPartitions int,
	numForcedMerges int,
	delegateFDAcquisitions bool,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAcc *mon.BoundAccount,
) colexecop.Operator {
	if diskQueueCfg.BufferSizeBytes > 0 && maxNumberPartitions == 0 {
		// With the default limit of 256 file descriptors, this results in 16
		// partitions. This is a hard maximum of partitions that will be used by
		// the external sorter.
		// TODO(asubiotto): this number should be tuned.
		maxNumberPartitions = fdSemaphore.GetLimit() / 16
	}
	if maxNumberPartitions < colexecop.ExternalSorterMinPartitions {
		maxNumberPartitions = colexecop.ExternalSorterMinPartitions
	}
	if memoryLimit == 1 {
		// If memory limit is 1, we're likely in a "force disk spill"
		// scenario, but we don't want to artificially limit batches when we
		// have already spilled, so we'll use a larger limit.
		memoryLimit = execinfra.DefaultMemoryLimit
	}
	// Each disk queue will use up to BufferSizeBytes of RAM, so we reduce the
	// memoryLimit of the partitions to sort in memory by those cache sizes.
	memoryLimit -= int64(maxNumberPartitions * diskQueueCfg.BufferSizeBytes)
	// We give half of the available RAM to the in-memory sorter. Note that we
	// will reuse that memory for each partition and will be holding onto it all
	// the time, so we cannot "return" this usage after spilling each partition.
	inMemSortTotalMemoryLimit := memoryLimit / 2
	inMemSortPartitionLimit := inMemSortTotalMemoryLimit * 4 / 5
	inMemSortOutputLimit := inMemSortTotalMemoryLimit / 5
	// We give another half of the available RAM to the merge operation.
	mergeMemoryLimit := memoryLimit / 2
	if inMemSortPartitionLimit < 1 {
		// If the memory limit is 0, the input partitioning operator will return
		// a zero-length batch, so make it at least 1.
		inMemSortPartitionLimit = 1
		inMemSortOutputLimit = 1
		mergeMemoryLimit = 1
	}
	inputPartitioner := newInputPartitioningOperator(sortUnlimitedAllocator, input, inputTypes, inMemSortPartitionLimit)
	var inMemSorter colexecop.ResettableOperator
	if topK > 0 {
		inMemSorter = NewTopKSorter(sortUnlimitedAllocator, inputPartitioner, inputTypes, ordering.Columns, matchLen, topK, inMemSortOutputLimit)
	} else {
		inMemSorter = newSorter(
			sortUnlimitedAllocator, newAllSpooler(sortUnlimitedAllocator, inputPartitioner, inputTypes),
			inputTypes, ordering.Columns, inMemSortOutputLimit,
		)
	}
	partitionedDiskQueueSemaphore := fdSemaphore
	if !delegateFDAcquisitions {
		// To avoid deadlocks with other disk queues, we manually attempt to
		// acquire the maximum number of descriptors all at once in Next.
		// Passing in a nil semaphore indicates that the caller will do the
		// acquiring.
		partitionedDiskQueueSemaphore = nil
	}
	es := &externalSorter{
		OneInputHelper:           colexecop.MakeOneInputHelper(inMemSorter),
		mergeUnlimitedAllocator:  mergeUnlimitedAllocator,
		outputUnlimitedAllocator: outputUnlimitedAllocator,
		mergeMemoryLimit:         mergeMemoryLimit,
		inMemSorter:              inMemSorter,
		inMemSorterInput:         inputPartitioner.(*inputPartitioningOperator),
		partitionerCreator: func() colcontainer.PartitionedQueue {
			return colcontainer.NewPartitionedDiskQueue(inputTypes, diskQueueCfg, partitionedDiskQueueSemaphore, colcontainer.PartitionerStrategyCloseOnNewPartition, diskAcc)
		},
		inputTypes:           inputTypes,
		ordering:             ordering,
		topK:                 topK,
		columnOrdering:       execinfrapb.ConvertToColumnOrdering(ordering),
		maxNumberPartitions:  maxNumberPartitions,
		numForcedMerges:      numForcedMerges,
		currentPartitionIdxs: make([]int, maxNumberPartitions),
		maxMerged:            make([]int, maxNumberPartitions),
	}
	es.partitionsInfo.tupleCount = make([]uint64, maxNumberPartitions)
	es.partitionsInfo.totalSize = make([]int64, maxNumberPartitions)
	es.partitionsInfo.maxBatchMemSize = make([]int64, maxNumberPartitions)
	es.fdState.fdSemaphore = fdSemaphore
	es.testingKnobs.delegateFDAcquisitions = delegateFDAcquisitions
	return es
}

// doneWithCurrentPartition should be called whenever all tuples needed for the
// current partition have been enqueued in order to prepare the external sorter
// for the next partition.
func (s *externalSorter) doneWithCurrentPartition() {
	// The current partition has been fully processed, so we reset the in-memory
	// sorter (which will do the "shallow" reset of inputPartitioningOperator).
	s.inMemSorterInput.interceptReset = true
	s.inMemSorter.Reset(s.Ctx)
	s.currentPartitionIdxs[s.numPartitions] = s.currentPartitionIdx
	s.maxMerged[s.numPartitions] = 0
	s.numPartitions++
	s.currentPartitionIdx++
	if s.shouldMergeSomePartitions() {
		s.state = externalSorterRepeatedMerging
	} else {
		s.state = externalSorterNewPartition
	}
}

func (s *externalSorter) resetPartitionsInfoForCurrentPartition() {
	s.partitionsInfo.tupleCount[s.numPartitions] = 0
	s.partitionsInfo.totalSize[s.numPartitions] = 0
	s.partitionsInfo.maxBatchMemSize[s.numPartitions] = 0
}

func (s *externalSorter) Next() coldata.Batch {
	for {
		switch s.state {
		case externalSorterNewPartition:
			b := s.Input.Next()
			if b.Length() == 0 {
				// The input has been fully exhausted, and it is always the case
				// that the number of partitions is less than the maximum number
				// since externalSorterSpillPartition will check and re-merge if
				// not. Proceed to the final merging state.
				s.state = externalSorterFinalMerging
				log.VEvent(s.Ctx, 1, "external sorter consumed its input")
				continue
			}
			if s.partitioner == nil {
				s.partitioner = s.partitionerCreator()
				if !s.testingKnobs.delegateFDAcquisitions && s.fdState.fdSemaphore != nil {
					toAcquire := s.maxNumberPartitions
					if err := s.fdState.fdSemaphore.Acquire(s.Ctx, toAcquire); err != nil {
						colexecerror.InternalError(err)
					}
					s.fdState.acquiredFDs = toAcquire
				}
			}
			s.resetPartitionsInfoForCurrentPartition()
			partitionDone := s.enqueue(b)
			if partitionDone {
				s.doneWithCurrentPartition()
			} else {
				s.state = externalSorterSpillPartition
			}

		case externalSorterSpillPartition:
			b := s.Input.Next()
			partitionDone := s.enqueue(b)
			if b.Length() == 0 || partitionDone {
				s.doneWithCurrentPartition()
			}

		case externalSorterRepeatedMerging:
			// We will merge at least 2 partitions (the 2 most recently
			// created partitions), along with any partitions that share
			// the same maxMerged value as the second most recently
			// created partition (because the most recently created
			// partition will always have merge value 0), and will spill
			// all the resulting batches into a new partition with the
			// next available index.
			//
			// The merger will be using some amount of RAM for the output batch,
			// will register it with the output unlimited allocator and will
			// *not* release that memory from the allocator, so we have to do it
			// ourselves.
			//
			// Note that the memory used for dequeueing batches from the
			// partitions is retained and is registered with the merge unlimited
			// allocator.
			n := 2
			for i := s.numPartitions - 2; i > 0; i-- {
				if s.maxMerged[i] != s.maxMerged[i-1] {
					break
				}
				n++
			}
			merger := s.createMergerForPartitions(n)
			merger.Init(s.Ctx)
			s.numPartitions -= n
			s.resetPartitionsInfoForCurrentPartition()
			for b := merger.Next(); ; b = merger.Next() {
				partitionDone := s.enqueue(b)
				if b.Length() == 0 || partitionDone {
					if err := merger.Close(s.Ctx); err != nil {
						colexecerror.InternalError(err)
					}
					break
				}
			}
			s.maxMerged[s.numPartitions]++
			s.currentPartitionIdxs[s.numPartitions] = s.currentPartitionIdx
			s.numPartitions++
			s.currentPartitionIdx++
			// We are now done with the merger, so we can release the memory
			// used for the output batches (all of which have been enqueued into
			// the new partition).
			s.outputUnlimitedAllocator.ReleaseMemory(s.outputUnlimitedAllocator.Used())
			// Make sure to close out all partitions we have just read from.
			//
			// Note that this operation is a noop for the general sort and is
			// only needed for the top K sort. In the former case we have fully
			// exhausted all old partitions, and they have been closed for
			// reading automatically; in the latter case we stop reading once we
			// have at least K tuples in the new partition, and we have to
			// manually close all old partitions for reading in order for
			// resources to be properly released in CloseInactiveReadPartitions
			// call below.
			if err := s.partitioner.CloseAllOpenReadFileDescriptors(); err != nil {
				colexecerror.InternalError(err)
			}
			if err := s.partitioner.CloseInactiveReadPartitions(s.Ctx); err != nil {
				colexecerror.InternalError(err)
			}
			s.state = externalSorterNewPartition

		case externalSorterFinalMerging:
			if s.numPartitions == 0 {
				s.state = externalSorterFinished
				continue
			} else if s.numPartitions == 1 {
				s.createPartitionerToOperators(s.numPartitions)
				s.emitter = s.partitionerToOperators[0]
			} else {
				s.emitter = s.createMergerForPartitions(s.numPartitions)
			}
			s.emitter.Init(s.Ctx)
			s.state = externalSorterEmitting

		case externalSorterEmitting:
			b := s.emitter.Next()
			if b.Length() == 0 {
				s.state = externalSorterFinished
				continue
			}
			if s.topK > 0 {
				// If there's a topK limit, only emit the first topK tuples.
				if b.Length() >= int(s.topK-s.emitted) {
					// This batch contains the last of the topK tuples to emit.
					b.SetLength(int(s.topK - s.emitted))
					s.state = externalSorterFinished
				}
				s.emitted += uint64(b.Length())
			}
			return b

		case externalSorterFinished:
			if err := s.Close(s.Ctx); err != nil {
				colexecerror.InternalError(err)
			}
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected externalSorterState %d", s.state))
		}
	}
}

// enqueue enqueues b to the current partition (which has index
// currentPartitionIdx) as well as updates the information about the partition.
//
// If the current partition reaches the desired topK number of tuples, a zero
// batch is enqueued and true is returned indicating that the current partition
// is done.
//
// The following observation is what allows us to stop enqueueing into the
// current partition once topK number of tuples is reached: `b` is not coming
// from the input to the sort operation as a whole (when tuples can be in an
// arbitrary order) - `b` is coming to us either from the in-memory top K sorter
// (which has already performed the sort over a subset of tuples, with
// inputPartitioningOperator defining the boundaries of that subset) or from the
// merger (which performs the merge of N already sorted partitions while
// preserving the order of tuples).
func (s *externalSorter) enqueue(b coldata.Batch) bool {
	if b.Length() > 0 {
		batchMemSize := colmem.GetBatchMemSize(b)
		s.partitionsInfo.tupleCount[s.numPartitions] += uint64(b.Length())
		s.partitionsInfo.totalSize[s.numPartitions] += batchMemSize
		if batchMemSize > s.partitionsInfo.maxBatchMemSize[s.numPartitions] {
			s.partitionsInfo.maxBatchMemSize[s.numPartitions] = batchMemSize
		}
	}
	// Note that b will never have a selection vector set because the allSpooler
	// performs a deselection when buffering up the tuples, and the in-memory
	// sorter has allSpooler as its input.
	if err := s.partitioner.Enqueue(s.Ctx, s.currentPartitionIdx, b); err != nil {
		colexecutils.HandleErrorFromDiskQueue(err)
	}
	if s.topK > 0 && s.topK <= s.partitionsInfo.tupleCount[s.numPartitions] {
		// We have a top K sort and already have at least K tuples in the
		// current partition. Enqueue a zero-length batch and tell the caller
		// that the partition is done.
		if err := s.partitioner.Enqueue(s.Ctx, s.currentPartitionIdx, coldata.ZeroBatch); err != nil {
			colexecutils.HandleErrorFromDiskQueue(err)
		}
		return true
	}
	return false
}

// shouldMergeSomePartitions returns true if we need to merge some current
// partitions into one before proceeding to spilling a new partition.
func (s *externalSorter) shouldMergeSomePartitions() bool {
	if s.numPartitions <= 1 {
		return false
	}
	forceRepeatedMerging := s.numForcedMerges > 0 && s.numPartitions == 2
	if s.numPartitions == s.maxNumberPartitions-1 || forceRepeatedMerging {
		// We either have reached the maximum number of active partitions that
		// we know that we'll be able to merge without exceeding the limit of
		// FDs or we're forced to merge the partitions in tests, so we need to
		// merge all of them and spill the new partition to disk before we can
		// proceed on consuming the input.
		if forceRepeatedMerging {
			s.numForcedMerges--
		}
		return true
	}
	if s.maxNumberPartitionsDynamicallyReduced {
		// We haven't reached the dynamically computed maximum number of
		// partitions yet, so we will wait before performing the merge
		// operation.
		return false
	}
	// Now we check whether already we're likely to exceed the memory limit for
	// the merge operation.
	//
	// Each of the partitions will need to use a single batch at a time, so we
	// count that usage based on the maximum batch mem size of each partition.
	var expectedMergeMemUsage int64
	for i := 0; i < s.numPartitions; i++ {
		expectedMergeMemUsage += s.partitionsInfo.maxBatchMemSize[i]
	}
	// We also need to account for the output batch of the merge operation. We
	// will estimate that it will use the average of max batch mem sizes from
	// each of the partition.
	expectedMergeMemUsage = expectedMergeMemUsage / int64(s.numPartitions) * int64(s.numPartitions+1)
	if expectedMergeMemUsage < s.mergeMemoryLimit {
		return false
	}
	// From now on, we will never be able to create more partitions than we
	// currently have due to the fact that we're keeping the memory used for
	// dequeued batches, so we'll override the maximum number of partitions and
	// release the unused FDs.
	//
	// Note that we need an extra partition to write into.
	newMaxNumberPartitions := s.numPartitions + 1
	if !s.testingKnobs.delegateFDAcquisitions && s.fdState.fdSemaphore != nil {
		toRelease := s.maxNumberPartitions - newMaxNumberPartitions
		s.fdState.fdSemaphore.Release(toRelease)
		s.fdState.acquiredFDs -= toRelease
	}
	s.maxNumberPartitions = newMaxNumberPartitions
	s.maxNumberPartitionsDynamicallyReduced = true
	return true
}

func (s *externalSorter) Reset(ctx context.Context) {
	if r, ok := s.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	s.state = externalSorterNewPartition
	if err := s.Close(ctx); err != nil {
		colexecerror.InternalError(err)
	}
	// Reset the CloserHelper so that the sorter may be closed again.
	s.CloserHelper.Reset()
	s.currentPartitionIdx = 0
	s.numPartitions = 0
	// Note that we consciously do not reset maxNumberPartitions and
	// maxNumberPartitionsDynamicallyReduced (when the latter is true) since we
	// are keeping the memory used for dequeueing batches.
	s.emitted = 0
}

func (s *externalSorter) Close(ctx context.Context) error {
	if !s.CloserHelper.Close() {
		return nil
	}
	log.VEvent(ctx, 1, "external sorter is closed")
	var lastErr error
	if s.partitioner != nil {
		lastErr = s.partitioner.Close(ctx)
		s.partitioner = nil
	}
	if c, ok := s.emitter.(colexecop.Closer); ok {
		if err := c.Close(ctx); err != nil {
			lastErr = err
		}
	}
	s.inMemSorterInput.close()
	if !s.testingKnobs.delegateFDAcquisitions && s.fdState.fdSemaphore != nil && s.fdState.acquiredFDs > 0 {
		s.fdState.fdSemaphore.Release(s.fdState.acquiredFDs)
		s.fdState.acquiredFDs = 0
	}
	return lastErr
}

// createPartitionerToOperators updates s.partitionerToOperators to correspond
// to the last n current partitions to be merged.
func (s *externalSorter) createPartitionerToOperators(n int) {
	oldPartitioners := s.partitionerToOperators
	if len(oldPartitioners) < n {
		s.partitionerToOperators = make([]*partitionerToOperator, n)
		copy(s.partitionerToOperators, oldPartitioners)
		for i := len(oldPartitioners); i < n; i++ {
			s.partitionerToOperators[i] = newPartitionerToOperator(
				s.mergeUnlimitedAllocator, s.inputTypes, s.partitioner,
			)
		}
	}
	for i := 0; i < n; i++ {
		// We only need to set the partitioner and partitionIdx fields because
		// all others will not change when these operators are reused.
		s.partitionerToOperators[i].partitioner = s.partitioner
		s.partitionerToOperators[i].partitionIdx = s.currentPartitionIdxs[s.numPartitions-n+i]
	}
}

// createMergerForPartitions creates an ordered synchronizer that will merge
// the last n current partitions.
func (s *externalSorter) createMergerForPartitions(n int) *OrderedSynchronizer {
	s.createPartitionerToOperators(n)
	syncInputs := make([]colexecargs.OpWithMetaInfo, n)
	for i := range syncInputs {
		syncInputs[i].Root = s.partitionerToOperators[i]
	}
	if log.V(2) {
		var counts, sizes strings.Builder
		for i := 0; i < n; i++ {
			if i > 0 {
				counts.WriteString(", ")
				sizes.WriteString(", ")
			}
			partitionOrdinal := s.numPartitions - n + i
			counts.WriteString(fmt.Sprintf("%d", s.partitionsInfo.tupleCount[partitionOrdinal]))
			sizes.WriteString(string(humanizeutil.IBytes(s.partitionsInfo.totalSize[partitionOrdinal])))
		}
		log.Infof(s.Ctx,
			"external sorter is merging partitions with partition indices %v with counts [%s] and sizes [%s]",
			s.currentPartitionIdxs[s.numPartitions-n:s.numPartitions], counts.String(), sizes.String(),
		)
	}

	// Calculate the limit on the output batch mem size.
	outputBatchMemSize := s.mergeMemoryLimit
	for i := 0; i < s.numPartitions; i++ {
		outputBatchMemSize -= s.partitionsInfo.maxBatchMemSize[i]
	}
	// It is possible that the expected usage of the dequeued batches already
	// exceeds the memory limit (this is likely when the tuples are wide). In
	// such a scenario we want to produce output batches of relatively large
	// memory size too, so we give the output batch at least its fair share of
	// the memory limit.
	minOutputBatchMemSize := s.mergeMemoryLimit / int64(s.numPartitions+1)
	if outputBatchMemSize < minOutputBatchMemSize {
		outputBatchMemSize = minOutputBatchMemSize
	}
	return NewOrderedSynchronizer(
		s.outputUnlimitedAllocator, outputBatchMemSize, syncInputs, s.inputTypes, s.columnOrdering,
	)
}

func newInputPartitioningOperator(
	allocator *colmem.Allocator, input colexecop.Operator, typs []*types.T, memoryLimit int64,
) colexecop.ResettableOperator {
	return &inputPartitioningOperator{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		memoryLimit:    memoryLimit,
		allocator:      allocator,
		typs:           typs,
	}
}

// inputPartitioningOperator is an operator that returns the batches from its
// input until the memory footprint of all emitted batches reaches the memory
// limit. From that point, the operator returns a zero-length batch (until it is
// reset).
type inputPartitioningOperator struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	// memoryLimit determines the size of each partition.
	memoryLimit int64
	// alreadyUsedMemory tracks the size of the current partition so far.
	alreadyUsedMemory int64
	// lastBatchState keeps track of the state around emitting rows from the
	// last batch we read from input. This is needed in case a single batch
	// needs to be divided between different partitions (possibly more than
	// two).
	lastBatchState struct {
		// batch is non-nil only if there are more rows to be emitted from the
		// last read batch.
		batch coldata.Batch
		// avgRowSize is an estimate about the size of each row in batch, in
		// bytes.
		avgRowSize int64
		// emitted is the number of rows already emitted from batch.
		emitted int
	}

	allocator     *colmem.Allocator
	windowedBatch coldata.Batch
	typs          []*types.T

	// interceptReset determines whether the reset method will be called on
	// the input to this operator when the latter is being reset. This field is
	// managed by externalSorter.
	// NOTE: this field itself is set to 'false' when inputPartitioningOperator
	// is being reset, regardless of the original value.
	//
	// The reason for having this knob is that we need two kinds of behaviors
	// when resetting the inputPartitioningOperator:
	// 1. ("shallow" reset) we need to reset alreadyUsedMemory because the
	// external sorter is moving on spilling the data into a new partition.
	// However, we *cannot* propagate the reset further up because it might
	// delete the data that the external sorter has not yet spilled. This
	// behavior is needed in externalSorter when resetting the in-memory sorter
	// when spilling the next "chunk" of data into the new partition.
	// 2. ("deep" reset) we need to do the full reset of the whole chain of
	// operators. This behavior is needed when the whole external sorter is
	// being reset.
	interceptReset bool
}

var _ colexecop.ResettableOperator = &inputPartitioningOperator{}

func (o *inputPartitioningOperator) Next() coldata.Batch {
	if o.alreadyUsedMemory >= o.memoryLimit {
		return coldata.ZeroBatch
	}
	if o.lastBatchState.batch != nil {
		// We still have some rows from the last batch.
		return o.emitWindowIntoLastBatch()
	}
	b := o.Input.Next()
	n := b.Length()
	if n == 0 {
		return b
	}
	// This operator is an input to sortOp which will spool all the tuples and
	// buffer them (by appending into the buffered batch), so we need to account
	// for memory proportionally to the length of the batch. (Note: this is not
	// exactly true for Bytes type, but it's ok if we have some deviation. This
	// numbers matter only to understand when to start a new partition, and the
	// memory will be actually accounted for correctly.)
	proportionalBatchMemSize := colmem.GetProportionalBatchMemSize(b, int64(n))
	if o.alreadyUsedMemory+proportionalBatchMemSize >= o.memoryLimit {
		// Emitting this batch as is will make the current partition exceed the
		// memory limit, so we will actually "split" this batch between multiple
		// partitions.
		o.lastBatchState.batch = b
		o.lastBatchState.emitted = 0
		o.lastBatchState.avgRowSize = proportionalBatchMemSize / int64(n)
		return o.emitWindowIntoLastBatch()
	}
	o.alreadyUsedMemory += proportionalBatchMemSize
	o.lastBatchState.batch = nil
	return b
}

// emitWindowIntoLastBatch returns a window into the last batch such that either
// all remaining rows are emitted or this window batch barely puts the current
// partition above the memory limit. The method assumes that the last batch is
// not nil and not all rows have been emitted from it.
func (o *inputPartitioningOperator) emitWindowIntoLastBatch() coldata.Batch {
	// We use plus one so that this windowed batch reaches the memory limit if
	// possible.
	toEmit := (o.memoryLimit-o.alreadyUsedMemory)/o.lastBatchState.avgRowSize + 1
	// But do not try to emit more than there are rows remaining.
	n := o.lastBatchState.batch.Length()
	if toEmit > int64(n-o.lastBatchState.emitted) {
		toEmit = int64(n - o.lastBatchState.emitted)
	}
	if o.windowedBatch == nil {
		// The columns will be replaced into this windowed batch, but we do need
		// to support a selection vector of an arbitrary length.
		o.windowedBatch = o.allocator.NewMemBatchNoCols(o.typs, coldata.BatchSize())
	}
	colexecutils.MakeWindowIntoBatch(
		o.windowedBatch, o.lastBatchState.batch, o.lastBatchState.emitted,
		o.lastBatchState.emitted+int(toEmit), o.typs,
	)
	o.alreadyUsedMemory += o.lastBatchState.avgRowSize * toEmit
	o.lastBatchState.emitted += int(toEmit)
	if o.lastBatchState.emitted == n {
		// This batch is now fully emitted, so we will need to fetch a new one
		// from the input.
		o.lastBatchState.batch = nil
	}
	return o.windowedBatch
}

func (o *inputPartitioningOperator) Reset(ctx context.Context) {
	if !o.interceptReset {
		if r, ok := o.Input.(colexecop.Resetter); ok {
			r.Reset(ctx)
		}
	}
	o.interceptReset = false
	o.alreadyUsedMemory = 0
}

func (o *inputPartitioningOperator) close() {
	o.alreadyUsedMemory = 0
	o.lastBatchState.batch = nil
	o.lastBatchState.avgRowSize = 0
	o.lastBatchState.emitted = 0
	// Nil out the windowed batch in order to lose the references to the vectors
	// of the last batch. We need to shrink the account accordingly and will
	// allocate a new windowed batch if necessary (which might be the case for
	// the fallback strategy of the users of the hash-based partitioner).
	o.windowedBatch = nil
	o.allocator.ReleaseMemory(colmem.SizeOfBatchSizeSelVector)
}
