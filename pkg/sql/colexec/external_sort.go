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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
	// in this state, the sorter will transition to externalSorterRepeatedMerging
	// to reduce the number of partitions.
	externalSorterSpillPartition
	// externalSorterRepeatedMerging indicates that we need to merge
	// maxNumberPartitions into one and spill that new partition to disk. When
	// finished, the sorter will transition to externalSorterNewPartition.
	externalSorterRepeatedMerging
	// externalSorterFinalMerging indicates that we have fully consumed the input
	// and can merge all of the partitions in one step. We then transition to
	// externalSorterEmitting state.
	externalSorterFinalMerging
	// externalSorterEmitting indicates that we are ready to emit output. A zero-
	// length batch in this state indicates that we have emitted all tuples and
	// should transition to externalSorterFinished state.
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
	OneInputNode
	NonExplainable

	closed             bool
	unlimitedAllocator *Allocator
	memoryLimit        int64
	state              externalSorterState
	inputTypes         []coltypes.T
	ordering           execinfrapb.Ordering
	inMemSorter        resettableOperator
	partitioner        colcontainer.PartitionedQueue
	// numPartitions is the current number of partitions.
	numPartitions int
	// firstPartitionIdx is the index of the first partition to merge next.
	firstPartitionIdx   int
	maxNumberPartitions int

	emitter Operator
}

var _ Operator = &externalSorter{}

// newExternalSorter returns a disk-backed general sort operator.
// - ctx is the same context that standaloneMemAccount was created with.
// - unlimitedAllocator must have been created with a memory account derived
// from an unlimited memory monitor. It will be used by several internal
// components of the external sort which is responsible for making sure that
// the components stay within the memory limit.
// - standaloneMemAccount must be a memory account derived from an unlimited
// memory monitor with a standalone budget. It will be used by
// inputPartitioningOperator to "partition" the input according to memory
// limit. The budget *must* be standalone because we don't want to double
// count the memory (the memory under the batches will be accounted for with
// the unlimitedAllocator).
// - diskQueuesUnlimitedAllocator is a (temporary) unlimited allocator that
// will be used to create dummy queues and will be removed once we have actual
// disk-backed queues.
// - maxNumberPartitions (when non-zero) overrides the semi-dynamically
// computed maximum number of partitions to have at once. It should be
// non-zero only in tests.
func newExternalSorter(
	ctx context.Context,
	unlimitedAllocator *Allocator,
	standaloneMemAccount *mon.BoundAccount,
	input Operator,
	inputTypes []coltypes.T,
	ordering execinfrapb.Ordering,
	memoryLimit int64,
	maxNumberPartitions int,
	cfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
) Operator {
	inputPartitioner := newInputPartitioningOperator(ctx, input, standaloneMemAccount, memoryLimit)
	inMemSorter, err := newSorter(
		unlimitedAllocator, newAllSpooler(unlimitedAllocator, inputPartitioner, inputTypes),
		inputTypes, ordering.Columns,
	)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	if cfg.BufferSizeBytes > 0 && maxNumberPartitions == 0 {
		// Each disk queue will use up to BufferSizeBytes of RAM, so we will give
		// it almost all of the available memory (except for a single output batch
		// that mergers will use).
		batchMemSize := estimateBatchSizeBytes(inputTypes, int(coldata.BatchSize()))
		// TODO(yuzefovich): we currently allocate a full-sized batch in
		// partitionerToOperator, but once we use actual disk-backed queues, we
		// should allocate zero-sized batch in there and all memory will be
		// allocated by the partitioner (and will be included in BufferSizeBytes).
		maxNumberPartitions = (int(memoryLimit) - batchMemSize) / (cfg.BufferSizeBytes + batchMemSize)
	}
	// In order to make progress when merging we have to merge at least two
	// partitions.
	if maxNumberPartitions < 2 {
		maxNumberPartitions = 2
	}
	return &externalSorter{
		OneInputNode:        NewOneInputNode(inMemSorter),
		unlimitedAllocator:  unlimitedAllocator,
		memoryLimit:         memoryLimit,
		inMemSorter:         inMemSorter,
		partitioner:         colcontainer.NewPartitionedDiskQueue(inputTypes, cfg, fdSemaphore, colcontainer.PartitionerStrategyCloseOnNewPartition),
		inputTypes:          inputTypes,
		ordering:            ordering,
		maxNumberPartitions: maxNumberPartitions,
	}
}

func (s *externalSorter) Init() {
	s.input.Init()
	s.state = externalSorterNewPartition
}

func (s *externalSorter) Next(ctx context.Context) coldata.Batch {
	for {
		switch s.state {
		case externalSorterNewPartition:
			b := s.input.Next(ctx)
			if b.Length() == 0 {
				// The input has been fully exhausted, and it is always the case that
				// the number of partitions is less than the maximum number since
				// externalSorterSpillPartition will check and re-merge if not.
				// Proceed to the final merging state.
				s.state = externalSorterFinalMerging
				continue
			}
			newPartitionIdx := s.firstPartitionIdx + s.numPartitions
			if err := s.partitioner.Enqueue(ctx, newPartitionIdx, b); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			s.state = externalSorterSpillPartition
			continue
		case externalSorterSpillPartition:
			curPartitionIdx := s.firstPartitionIdx + s.numPartitions
			b := s.input.Next(ctx)
			if b.Length() == 0 {
				// The partition has been fully spilled, so we reset the in-memory
				// sorter (which will reset inputPartitioningOperator).
				s.inMemSorter.reset()
				s.numPartitions++
				if s.numPartitions == s.maxNumberPartitions {
					// We have reached the maximum number of active partitions, so we
					// need to merge them and spill the new partition to disk before we
					// can proceed on consuming the input.
					s.state = externalSorterRepeatedMerging
					continue
				}
				s.state = externalSorterNewPartition
				continue
			}
			if err := s.partitioner.Enqueue(ctx, curPartitionIdx, b); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			continue
		case externalSorterRepeatedMerging:
			// We will merge all partitions in range [s.firstPartitionIdx,
			// s.firstPartitionIdx+s.numPartitions) and will spill all the resulting
			// batches into a new partition with the next available index.
			merger := s.createMergerForPartitions(s.firstPartitionIdx, s.numPartitions)
			merger.Init()
			newPartitionIdx := s.firstPartitionIdx + s.numPartitions
			for b := merger.Next(ctx); b.Length() > 0; b = merger.Next(ctx) {
				if err := s.partitioner.Enqueue(ctx, newPartitionIdx, b); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
			}
			// Reclaim disk space by closing the inactive read partitions. Since the
			// merger must have exhausted all inputs, this is all the partitions just
			// read from.
			if err := s.partitioner.CloseInactiveReadPartitions(); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			s.firstPartitionIdx += s.numPartitions
			s.numPartitions = 1
			s.state = externalSorterNewPartition
			continue
		case externalSorterFinalMerging:
			if s.numPartitions == 0 {
				s.state = externalSorterFinished
				continue
			} else if s.numPartitions == 1 {
				s.emitter = newPartitionerToOperator(
					s.unlimitedAllocator, s.inputTypes, s.partitioner, s.firstPartitionIdx,
				)
			} else {
				s.emitter = s.createMergerForPartitions(s.firstPartitionIdx, s.numPartitions)
			}
			s.emitter.Init()
			s.state = externalSorterEmitting
			continue
		case externalSorterEmitting:
			b := s.emitter.Next(ctx)
			if b.Length() == 0 {
				s.state = externalSorterFinished
				continue
			}
			return b
		case externalSorterFinished:
			if err := s.Close(); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			return coldata.ZeroBatch
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected externalSorterState %d", s.state))
		}
	}
}

func (s *externalSorter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.partitioner.Close()
}

// createMergerForPartitions creates an ordered synchronizer that will merge
// partitions in [firstIdx, firstIdx+numPartitions) range.
func (s *externalSorter) createMergerForPartitions(firstIdx, numPartitions int) Operator {
	syncInputs := make([]Operator, numPartitions)
	for i := range syncInputs {
		syncInputs[i] = newPartitionerToOperator(
			s.unlimitedAllocator, s.inputTypes, s.partitioner, firstIdx+i,
		)
	}
	return NewOrderedSynchronizer(
		s.unlimitedAllocator,
		syncInputs,
		s.inputTypes,
		execinfrapb.ConvertToColumnOrdering(s.ordering),
	)
}

func newInputPartitioningOperator(
	ctx context.Context, input Operator, standaloneMemAccount *mon.BoundAccount, memoryLimit int64,
) resettableOperator {
	return &inputPartitioningOperator{
		OneInputNode:         NewOneInputNode(input),
		ctx:                  ctx,
		standaloneMemAccount: standaloneMemAccount,
		memoryLimit:          memoryLimit,
	}
}

// inputPartitioningOperator is an operator that returns the batches from its
// input until the standalone allocator reaches the memory limit. From that
// point, the operator returns a zero-length batch (until it is reset).
type inputPartitioningOperator struct {
	OneInputNode
	NonExplainable

	ctx                  context.Context
	standaloneMemAccount *mon.BoundAccount
	memoryLimit          int64
}

var _ resettableOperator = &inputPartitioningOperator{}

func (o *inputPartitioningOperator) Init() {
	o.input.Init()
}

func (o *inputPartitioningOperator) Next(ctx context.Context) coldata.Batch {
	if o.standaloneMemAccount.Used() >= o.memoryLimit {
		return coldata.ZeroBatch
	}
	b := o.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	// We cannot use Allocator.RetainBatch here because that method looks at the
	// capacities of the vectors. However, this operator is an input to sortOp
	// which will spool all the tuples and buffer them (by appending into the
	// buffered batch), so we need to account for memory proportionally to the
	// length of the batch. (Note: this is not exactly true for Bytes type, but
	// it's ok if we have some deviation. This numbers matter only to understand
	// when to start a new partition, and the memory will be actually accounted
	// for correctly.)
	length := int64(b.Length())
	usesSel := b.Selection() != nil
	b.SetSelection(true)
	selCapacity := cap(b.Selection())
	b.SetSelection(usesSel)
	batchMemSize := int64(0)
	if selCapacity > 0 {
		batchMemSize = selVectorSize(selCapacity) * length / int64(selCapacity)
	}
	for _, vec := range b.ColVecs() {
		if vec.Type() == coltypes.Bytes {
			batchMemSize += int64(vec.Bytes().ProportionalSize(length))
		} else {
			batchMemSize += getVecMemoryFootprint(vec) * length / int64(vec.Capacity())
		}
	}
	if err := o.standaloneMemAccount.Grow(o.ctx, batchMemSize); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return b
}

func (o *inputPartitioningOperator) reset() {
	o.standaloneMemAccount.Shrink(o.ctx, o.standaloneMemAccount.Used())
}

func newPartitionerToOperator(
	allocator *Allocator,
	types []coltypes.T,
	partitioner colcontainer.PartitionedQueue,
	partitionIdx int,
) *partitionerToOperator {
	return &partitionerToOperator{
		partitioner:  partitioner,
		partitionIdx: partitionIdx,
		// TODO(yuzefovich): allocate zero-sized batch once the disk-backed
		// partitioner is used.
		batch: allocator.NewMemBatch(types),
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
type partitionerToOperator struct {
	ZeroInputNode
	NonExplainable

	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {}

func (p *partitionerToOperator) Next(ctx context.Context) coldata.Batch {
	if err := p.partitioner.Dequeue(ctx, p.partitionIdx, p.batch); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return p.batch
}
