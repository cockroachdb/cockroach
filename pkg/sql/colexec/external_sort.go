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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
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
	colexecop.OneInputNode
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

	// partitionsInfo tracks some information about all current partitions
	// (those in currentPartitionIdxs).
	partitionsInfo struct {
		// totalSize is used for logging purposes.
		totalSize []int64
		// maxBatchMemSize decides how many partitions to have at once,
		// potentially reducing maxNumberPartitions
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
	memoryLimit int64,
	maxNumberPartitions int,
	numForcedMerges int,
	delegateFDAcquisitions bool,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAcc *mon.BoundAccount,
) colexecop.Operator {
	// The cache mode is chosen to reuse the cache to have a smaller cache per
	// partition without affecting performance.
	diskQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeReuseCache
	diskQueueCfg.SetDefaultBufferSizeBytesForCacheMode()
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
		memoryLimit = colexecop.DefaultMemoryLimit
	}
	// Each disk queue will use up to BufferSizeBytes of RAM, so we reduce the
	// memoryLimit of the partitions to sort in memory by those cache sizes.
	memoryLimit -= int64(maxNumberPartitions * diskQueueCfg.BufferSizeBytes)
	// We give half of the available RAM to the in-memory sorter. Note that we
	// will reuse that memory for each partition and will be holding onto it all
	// the time, so we cannot "return" this usage after spilling each partition.
	inMemSortMemoryLimit := memoryLimit / 2
	// We give another half of the available RAM to the merge operation.
	mergeMemoryLimit := memoryLimit / 2
	if inMemSortMemoryLimit < 1 {
		// If the memory limit is 0, the input partitioning operator will return
		// a zero-length batch, so make it at least 1.
		inMemSortMemoryLimit = 1
		mergeMemoryLimit = 1
	}
	inputPartitioner := newInputPartitioningOperator(input, inMemSortMemoryLimit)
	inMemSorter, err := newSorter(
		sortUnlimitedAllocator, newAllSpooler(sortUnlimitedAllocator, inputPartitioner, inputTypes),
		inputTypes, ordering.Columns,
	)
	if err != nil {
		colexecerror.InternalError(err)
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
		OneInputNode:             colexecop.NewOneInputNode(inMemSorter),
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
		columnOrdering:       execinfrapb.ConvertToColumnOrdering(ordering),
		maxNumberPartitions:  maxNumberPartitions,
		numForcedMerges:      numForcedMerges,
		currentPartitionIdxs: make([]int, maxNumberPartitions),
		maxMerged:            make([]int, maxNumberPartitions),
	}
	es.partitionsInfo.totalSize = make([]int64, maxNumberPartitions)
	es.partitionsInfo.maxBatchMemSize = make([]int64, maxNumberPartitions)
	es.fdState.fdSemaphore = fdSemaphore
	es.testingKnobs.delegateFDAcquisitions = delegateFDAcquisitions
	return es
}

func (s *externalSorter) Init() {
	s.Input.Init()
	s.state = externalSorterNewPartition
}

func (s *externalSorter) Next(ctx context.Context) coldata.Batch {
	for {
		switch s.state {
		case externalSorterNewPartition:
			b := s.Input.Next(ctx)
			if b.Length() == 0 {
				// The input has been fully exhausted, and it is always the case
				// that the number of partitions is less than the maximum number
				// since externalSorterSpillPartition will check and re-merge if
				// not. Proceed to the final merging state.
				s.state = externalSorterFinalMerging
				log.VEvent(ctx, 1, "external sorter consumed its input")
				continue
			}
			if s.partitioner == nil {
				s.partitioner = s.partitionerCreator()
				if !s.testingKnobs.delegateFDAcquisitions && s.fdState.fdSemaphore != nil {
					toAcquire := s.maxNumberPartitions
					if err := s.fdState.fdSemaphore.Acquire(ctx, toAcquire); err != nil {
						colexecerror.InternalError(err)
					}
					s.fdState.acquiredFDs = toAcquire
				}
			}
			s.partitionsInfo.totalSize[s.numPartitions] = 0
			s.partitionsInfo.maxBatchMemSize[s.numPartitions] = 0
			s.enqueue(ctx, b)
			s.state = externalSorterSpillPartition

		case externalSorterSpillPartition:
			b := s.Input.Next(ctx)
			s.enqueue(ctx, b)
			if b.Length() == 0 {
				// The partition has been fully spilled, so we reset the
				// in-memory sorter (which will do the "shallow" reset of
				// inputPartitioningOperator).
				s.inMemSorterInput.interceptReset = true
				s.inMemSorter.Reset(ctx)
				s.currentPartitionIdxs[s.numPartitions] = s.currentPartitionIdx
				s.maxMerged[s.numPartitions] = 0
				s.numPartitions++
				s.currentPartitionIdx++
				if s.shouldMergeSomePartitions() {
					s.state = externalSorterRepeatedMerging
					continue
				}
				s.state = externalSorterNewPartition
				continue
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
			merger, err := s.createMergerForPartitions(ctx, n)
			if err != nil {
				colexecerror.InternalError(err)
			}
			merger.Init()
			s.numPartitions -= n
			s.partitionsInfo.totalSize[s.numPartitions] = 0
			s.partitionsInfo.maxBatchMemSize[s.numPartitions] = 0
			for b := merger.Next(ctx); ; b = merger.Next(ctx) {
				s.enqueue(ctx, b)
				if b.Length() == 0 {
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
			// Reclaim disk space by closing the inactive read partitions. Since
			// the merger must have exhausted all inputs, this is all the
			// partitions just read from.
			if err := s.partitioner.CloseInactiveReadPartitions(ctx); err != nil {
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
				var err error
				s.emitter, err = s.createMergerForPartitions(ctx, s.numPartitions)
				if err != nil {
					colexecerror.InternalError(err)
				}
			}
			s.emitter.Init()
			s.state = externalSorterEmitting

		case externalSorterEmitting:
			b := s.emitter.Next(ctx)
			if b.Length() == 0 {
				s.state = externalSorterFinished
				continue
			}
			return b

		case externalSorterFinished:
			if err := s.Close(ctx); err != nil {
				colexecerror.InternalError(err)
			}
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected externalSorterState %d", s.state))
		}
	}
}

// enqueue enqueues b to the current partition (which has index
// currentPartitionIdx) as well as updates the information about
// the partition.
func (s *externalSorter) enqueue(ctx context.Context, b coldata.Batch) {
	if b.Length() > 0 {
		batchMemSize := colmem.GetBatchMemSize(b)
		s.partitionsInfo.totalSize[s.numPartitions] += batchMemSize
		if batchMemSize > s.partitionsInfo.maxBatchMemSize[s.numPartitions] {
			s.partitionsInfo.maxBatchMemSize[s.numPartitions] = batchMemSize
		}
	}
	// Note that b will never have a selection vector set because the allSpooler
	// performs a deselection when buffering up the tuples, and the in-memory
	// sorter has allSpooler as its input.
	if err := s.partitioner.Enqueue(ctx, s.currentPartitionIdx, b); err != nil {
		colexecutils.HandleErrorFromDiskQueue(err)
	}
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
	// Reset closed so that the sorter may be closed again.
	s.Closed = false
	s.currentPartitionIdx = 0
	s.numPartitions = 0
	// Note that we consciously do not reset maxNumberPartitions and
	// maxNumberPartitionsDynamicallyReduced (when the latter is true) since we
	// are keeping the memory used for dequeueing batches.
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
	if err := s.inMemSorterInput.Close(ctx); err != nil {
		lastErr = err
	}
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
func (s *externalSorter) createMergerForPartitions(
	ctx context.Context, n int,
) (colexecop.Operator, error) {
	s.createPartitionerToOperators(n)
	syncInputs := make([]SynchronizerInput, n)
	for i := range syncInputs {
		syncInputs[i].Op = s.partitionerToOperators[i]
	}
	if log.V(2) {
		var b strings.Builder
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(humanizeutil.IBytes(s.partitionsInfo.totalSize[s.numPartitions-n+i]))
		}
		log.Infof(ctx,
			"external sorter is merging partitions with partition indices %v with sizes [%s]",
			s.currentPartitionIdxs[s.numPartitions-n:s.numPartitions], b.String(),
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
	input colexecop.Operator, memoryLimit int64,
) colexecop.ResettableOperator {
	return &inputPartitioningOperator{
		OneInputNode: colexecop.NewOneInputNode(input),
		memoryLimit:  memoryLimit,
	}
}

// inputPartitioningOperator is an operator that returns the batches from its
// input until the memory footprint of all emitted batches reaches the memory
// limit. From that point, the operator returns a zero-length batch (until it is
// reset).
type inputPartitioningOperator struct {
	colexecop.OneInputNode
	colexecop.NonExplainable

	// memoryLimit determines the size of each partition.
	memoryLimit int64
	// alreadyUsedMemory tracks the size of the current partition so far.
	alreadyUsedMemory int64
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

func (o *inputPartitioningOperator) Init() {
	o.Input.Init()
}

func (o *inputPartitioningOperator) Next(ctx context.Context) coldata.Batch {
	if o.alreadyUsedMemory >= o.memoryLimit {
		return coldata.ZeroBatch
	}
	b := o.Input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	// This operator is an input to sortOp which will spool all the tuples and
	// buffer them (by appending into the buffered batch), so we need to account
	// for memory proportionally to the length of the batch. (Note: this is not
	// exactly true for Bytes type, but it's ok if we have some deviation. This
	// numbers matter only to understand when to start a new partition, and the
	// memory will be actually accounted for correctly.)
	o.alreadyUsedMemory += colmem.GetProportionalBatchMemSize(b, int64(b.Length()))
	return b
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

func (o *inputPartitioningOperator) Close(ctx context.Context) error {
	o.alreadyUsedMemory = 0
	return nil
}
