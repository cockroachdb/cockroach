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
)

// Partitioner is the abstraction for on-disk storage.
type Partitioner interface {
	// Enqueue adds the batch to the end of the partitionIdx'th partition.
	Enqueue(partitionIdx int, batch coldata.Batch) error
	// Dequeue removes and returns the batch from the front of the
	// partitionIdx'th partition. If the partition is empty (either all batches
	// from it have already been dequeued or no batches were ever enqueued), a
	// zero-length batch is returned.
	Dequeue(partitionIdx int, batch coldata.Batch) error
	// Close closes the partitioner.
	Close() error
}

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
	// transition to starting a new partition.
	externalSorterSpillPartition
	// externalSorterRepeatedMerging indicates that we need to merge several
	// partitions into one and spill that new partition to disk. The number of
	// partitions that we can merge at a time is determined by
	// maxNumberPartitions. This procedure will be repeated until only one
	// partition is left, and we transition to externalSorterNewPartition state.
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

	unlimitedAllocator *Allocator
	memoryLimit        int64
	state              externalSorterState
	inputDone          bool
	inputTypes         []coltypes.T
	ordering           execinfrapb.Ordering
	inMemSorter        resettableOperator
	partitioner        Partitioner
	// numPartitions is the current number of partitions.
	numPartitions int
	// firstPartitionIdx is the index of the first partition to merge next.
	firstPartitionIdx   int
	maxNumberPartitions int
	cfg                 colcontainer.DiskQueueCfg

	emitter Operator
}

var _ Operator = &externalSorter{}

// newExternalSorter returns a disk-backed general sort operator.
// - unlimitedAllocator must have been created with a memory account derived
// from an unlimited memory monitor. It will be used by several internal
// components of the external sort which is responsible for making sure that
// the components stay within the memory limit.
// - standaloneAllocator must have been created with a memory account derived
// from an unlimited memory monitor with a standalone budget. It will be used
// by inputPartitioningOperator to "partition" the input according to memory
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
	unlimitedAllocator *Allocator,
	standaloneAllocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	ordering execinfrapb.Ordering,
	memoryLimit int64,
	maxNumberPartitions int,
	// TODO(yuzefovich): remove this once actual disk queues are in-place.
	diskQueuesUnlimitedAllocator *Allocator,
	cfg colcontainer.DiskQueueCfg,
) Operator {
	inputPartitioner := newInputPartitioningOperator(standaloneAllocator, input, memoryLimit)
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
		partitioner:         newDummyPartitioner(diskQueuesUnlimitedAllocator, inputTypes),
		inputTypes:          inputTypes,
		ordering:            ordering,
		maxNumberPartitions: maxNumberPartitions,
		cfg:                 cfg,
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
				// The input has been fully exhausted, so we transition to "merging
				// partitions" state.
				s.inputDone = true
				s.state = externalSorterRepeatedMerging
				continue
			}
			newPartitionIdx := s.firstPartitionIdx + s.numPartitions
			if err := s.partitioner.Enqueue(newPartitionIdx, b); err != nil {
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
			if err := s.partitioner.Enqueue(curPartitionIdx, b); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			continue
		case externalSorterRepeatedMerging:
			if s.numPartitions < 2 {
				if s.inputDone {
					s.state = externalSorterFinalMerging
				} else {
					s.state = externalSorterNewPartition
				}
				continue
			}
			numPartitionsToMerge := s.maxNumberPartitions
			if numPartitionsToMerge > s.numPartitions {
				numPartitionsToMerge = s.numPartitions
			}
			if numPartitionsToMerge == s.numPartitions && s.inputDone {
				// The input has been fully consumed and we can merge all of the
				// remaining partitions, so we transition to "final merging" state.
				s.state = externalSorterFinalMerging
				continue
			}
			// We will merge all partitions in range [s.firstPartitionIdx,
			// s.firstPartitionIdx+numPartitionsToMerge) and will spill all the
			// resulting batches into a new partition with the next available
			// index.
			merger := s.createMergerForPartitions(s.firstPartitionIdx, numPartitionsToMerge)
			merger.Init()
			newPartitionIdx := s.firstPartitionIdx + s.numPartitions
			for b := merger.Next(ctx); b.Length() > 0; b = merger.Next(ctx) {
				if err := s.partitioner.Enqueue(newPartitionIdx, b); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
			}
			s.firstPartitionIdx += numPartitionsToMerge
			s.numPartitions -= numPartitionsToMerge - 1
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
			if err := s.partitioner.Close(); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			return coldata.ZeroBatch
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected externalSorterState %d", s.state))
		}
	}
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
	standaloneAllocator *Allocator, input Operator, memoryLimit int64,
) resettableOperator {
	return &inputPartitioningOperator{
		OneInputNode:        NewOneInputNode(input),
		standaloneAllocator: standaloneAllocator,
		memoryLimit:         memoryLimit,
	}
}

// inputPartitioningOperator is an operator that returns the batches from its
// input until the standalone allocator reaches the memory limit. From that
// point, the operator returns a zero-length batch (until it is reset).
type inputPartitioningOperator struct {
	OneInputNode
	NonExplainable

	standaloneAllocator *Allocator
	memoryLimit         int64
}

var _ resettableOperator = &inputPartitioningOperator{}

func (o *inputPartitioningOperator) Init() {
	o.input.Init()
}

func (o *inputPartitioningOperator) Next(ctx context.Context) coldata.Batch {
	if o.standaloneAllocator.Used() >= o.memoryLimit {
		return coldata.ZeroBatch
	}
	b := o.input.Next(ctx)
	o.standaloneAllocator.RetainBatch(b)
	return b
}

func (o *inputPartitioningOperator) reset() {
	o.standaloneAllocator.Clear()
}

func newPartitionerToOperator(
	allocator *Allocator, types []coltypes.T, partitioner Partitioner, partitionIdx int,
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
// Partitioner to Operator.
type partitionerToOperator struct {
	ZeroInputNode
	NonExplainable

	partitioner  Partitioner
	partitionIdx int
	batch        coldata.Batch
}

var _ Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {}

func (p *partitionerToOperator) Next(context.Context) coldata.Batch {
	if err := p.partitioner.Dequeue(p.partitionIdx, p.batch); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return p.batch
}

func newDummyPartitioner(allocator *Allocator, types []coltypes.T) Partitioner {
	return &dummyPartitioner{allocator: allocator, types: types}
}

// dummyPartitioner is a simple implementation of Partitioner interface that
// uses dummy in-memory queues.
type dummyPartitioner struct {
	allocator  *Allocator
	types      []coltypes.T
	partitions []*dummyQueue
}

var _ Partitioner = &dummyPartitioner{}

func (d *dummyPartitioner) Enqueue(partitionIdx int, batch coldata.Batch) error {
	if len(d.partitions) <= partitionIdx {
		d.partitions = append(d.partitions, make([]*dummyQueue, partitionIdx-len(d.partitions)+1)...)
	}
	if d.partitions[partitionIdx] == nil {
		d.partitions[partitionIdx] = newDummyQueue(d.allocator, d.types)
	}
	return d.partitions[partitionIdx].Enqueue(batch)
}

func (d *dummyPartitioner) Dequeue(partitionIdx int, batch coldata.Batch) error {
	var partition *dummyQueue
	if partitionIdx < len(d.partitions) {
		partition = d.partitions[partitionIdx]
	}
	if partition == nil {
		batch.SetLength(0)
		return nil
	}
	return partition.Dequeue(batch)
}

func (d *dummyPartitioner) Close() error {
	for _, partition := range d.partitions {
		if err := partition.Close(); err != nil {
			return err
		}
	}
	return nil
}

func newDummyQueue(allocator *Allocator, types []coltypes.T) *dummyQueue {
	return &dummyQueue{allocator: allocator, types: types}
}

// dummyQueue is an in-memory queue that simply copies the passed in batches.
type dummyQueue struct {
	allocator *Allocator
	queue     []coldata.Batch
	types     []coltypes.T
}

func (d *dummyQueue) Enqueue(batch coldata.Batch) error {
	if batch.Length() == 0 {
		return nil
	}
	batchCopy := d.allocator.NewMemBatchWithSize(d.types, int(batch.Length()))
	d.allocator.PerformOperation(batchCopy.ColVecs(), func() {
		for i, vec := range batchCopy.ColVecs() {
			vec.Append(
				coldata.SliceArgs{
					ColType:     d.types[i],
					Src:         batch.ColVec(i),
					Sel:         batch.Selection(),
					DestIdx:     0,
					SrcStartIdx: 0,
					SrcEndIdx:   uint64(batch.Length()),
				})
		}
	})
	batchCopy.SetLength(batch.Length())
	d.queue = append(d.queue, batchCopy)
	return nil
}

func (d *dummyQueue) Dequeue(batch coldata.Batch) error {
	if len(d.queue) == 0 {
		batch.SetLength(0)
		return nil
	}
	batch.ResetInternalBatch()
	batchToCopy := d.queue[0]
	d.queue = d.queue[1:]
	d.allocator.PerformOperation(batch.ColVecs(), func() {
		for i, colVecToCopy := range batchToCopy.ColVecs() {
			batch.ColVec(i).Append(coldata.SliceArgs{
				ColType:     d.types[i],
				Src:         colVecToCopy,
				Sel:         batchToCopy.Selection(),
				DestIdx:     0,
				SrcStartIdx: 0,
				SrcEndIdx:   uint64(batchToCopy.Length()),
			})
		}
	})
	batch.SetLength(batchToCopy.Length())
	return nil
}

func (d *dummyQueue) Close() error {
	return nil
}
