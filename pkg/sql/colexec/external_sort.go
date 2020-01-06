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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// Queue is the abstraction for on-disk storage.
type Queue interface {
	// Enqueue adds the batch to the end of the queue.
	Enqueue(coldata.Batch) error
	// Dequeue removes and returns the batch from the front of the queue.
	Dequeue() (coldata.Batch, error)
	// Close closes the queue.
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
	// externalSorterMerging indicates that we have fully consumed the input and
	// should proceed to merging the partitions and emitting the batches.
	externalSorterMerging
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
// TODO(yuzefovich): we probably want to have a maximum number of partitions at
// a time. In that case, we might need several stages of mergers.
//
// The diagram of the components involved is as follows:
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
type externalSorter struct {
	OneInputNode
	NonExplainable

	state                    externalSorterState
	inputTypes               []coltypes.T
	ordering                 execinfrapb.Ordering
	inMemSorter              resettableOperator
	partitions               []Queue
	merger                   Operator
	mergerUnlimitedAllocator *Allocator

	// TODO(yuzefovich): remove this once actual disk queues are in-place.
	diskQueuesUnlimitedAllocator *Allocator
}

var _ Operator = &externalSorter{}

// newExternalSorter returns a disk-backed general sort operator.
// - inMemSorterUnlimitedAllocator must have been created with a memory account
// derived from an unlimited memory monitor. It will be used by input
// partitioner and in-memory sorter.
// - mergerUnlimitedAllocator must have been created with a memory account
// derived from an unlimited memory monitor (different from the monitor
// mentioned above) and will be used by the merger (ordered synchronizer).
// - diskQueuesUnlimitedAllocator is a (temporary) unlimited allocator that
// will be used to create dummy queues and will be removed once we have actual
// disk-backed queues.
func newExternalSorter(
	inMemSorterUnlimitedAllocator *Allocator,
	mergerUnlimitedAllocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	ordering execinfrapb.Ordering,
	memoryLimit int64,
	// TODO(yuzefovich): remove this once actual disk queues are in-place.
	diskQueuesUnlimitedAllocator *Allocator,
) Operator {
	inputPartitioner := newInputPartitioningOperator(inMemSorterUnlimitedAllocator, input, memoryLimit)
	inMemSorter, err := newSorter(
		inMemSorterUnlimitedAllocator, newAllSpooler(inMemSorterUnlimitedAllocator, inputPartitioner, inputTypes),
		inputTypes, ordering.Columns,
	)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return &externalSorter{
		OneInputNode:                 NewOneInputNode(inMemSorter),
		diskQueuesUnlimitedAllocator: diskQueuesUnlimitedAllocator,
		mergerUnlimitedAllocator:     mergerUnlimitedAllocator,
		inMemSorter:                  inMemSorter,
		inputTypes:                   inputTypes,
		ordering:                     ordering,
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
				s.state = externalSorterMerging
				continue
			}
			newPartition := newDummyQueue(s.diskQueuesUnlimitedAllocator, s.inputTypes)
			s.partitions = append(s.partitions, newPartition)
			if err := newPartition.Enqueue(b); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			s.state = externalSorterSpillPartition
			continue
		case externalSorterSpillPartition:
			b := s.input.Next(ctx)
			if b.Length() == 0 {
				// The partition has been fully spilled, so we reset the in-memory
				// sorter (which will reset inputPartitioningOperator) and transition
				// to "new partition" state.
				s.inMemSorter.reset()
				s.state = externalSorterNewPartition
				continue
			}
			if err := s.partitions[len(s.partitions)-1].Enqueue(b); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			continue
		case externalSorterMerging:
			// Ideally, we should not be in such a state that we have zero or one
			// partition (we should have not spilled in such scenario), but we want
			// to be safe and handle those cases anyway.
			if len(s.partitions) == 0 {
				s.state = externalSorterFinished
				continue
			} else if len(s.partitions) == 1 {
				b, err := s.partitions[0].Dequeue()
				if err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				if b.Length() == 0 {
					s.state = externalSorterFinished
					continue
				}
				return b
			} else {
				if s.merger == nil {
					syncInputs := make([]Operator, len(s.partitions))
					for i := range syncInputs {
						syncInputs[i] = newQueueToOperator(s.partitions[i])
					}
					s.merger = NewOrderedSynchronizer(
						s.mergerUnlimitedAllocator,
						syncInputs,
						s.inputTypes,
						execinfrapb.ConvertToColumnOrdering(s.ordering),
					)
					s.merger.Init()
				}
				b := s.merger.Next(ctx)
				if b.Length() == 0 {
					s.state = externalSorterFinished
					continue
				}
				return b
			}
		case externalSorterFinished:
			if len(s.partitions) > 0 {
				for _, p := range s.partitions {
					if err := p.Close(); err != nil {
						execerror.VectorizedInternalPanic(err)
					}
				}
				s.partitions = s.partitions[:0]
			}
			return coldata.ZeroBatch
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected externalSorterState %d", s.state))
		}
	}
}

func newInputPartitioningOperator(
	unlimitedAllocator *Allocator, input Operator, memoryLimit int64,
) resettableOperator {
	return &inputPartitioningOperator{
		OneInputNode:       NewOneInputNode(input),
		unlimitedAllocator: unlimitedAllocator,
		memoryLimit:        memoryLimit,
	}
}

// inputPartitioningOperator is an operator that returns the batches from its
// input until the unlimited allocator (which the output operator must be
// using) reaches the memory limit. From that point, the operator returns a
// zero-length batch (until it is reset).
type inputPartitioningOperator struct {
	OneInputNode
	NonExplainable

	unlimitedAllocator *Allocator
	memoryLimit        int64
}

var _ resettableOperator = &inputPartitioningOperator{}

func (o *inputPartitioningOperator) Init() {
	o.input.Init()
}

func (o *inputPartitioningOperator) Next(ctx context.Context) coldata.Batch {
	if o.unlimitedAllocator.Used() >= o.memoryLimit {
		return coldata.ZeroBatch
	}
	return o.input.Next(ctx)
}

func (o *inputPartitioningOperator) reset() {
	o.unlimitedAllocator.Clear()
}

func newQueueToOperator(queue Queue) Operator {
	return &queueToOperator{queue: queue}
}

// queueToOperator is an Operator that Dequeue's from queue on every call to
// Next. It is a converter from filled in Queue to Operator.
type queueToOperator struct {
	ZeroInputNode
	NonExplainable
	queue Queue
}

var _ Operator = &queueToOperator{}

func (q *queueToOperator) Init() {}

func (q *queueToOperator) Next(context.Context) coldata.Batch {
	b, err := q.queue.Dequeue()
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return b
}

func newDummyQueue(allocator *Allocator, types []coltypes.T) Queue {
	return &dummyQueue{allocator: allocator, types: types}
}

// dummyQueue is an in-memory implementation of Queue that simply copies the
// passed in batches.
type dummyQueue struct {
	allocator *Allocator
	queue     []coldata.Batch
	types     []coltypes.T
}

var _ Queue = &dummyQueue{}

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

func (d *dummyQueue) Dequeue() (coldata.Batch, error) {
	if len(d.queue) == 0 {
		return coldata.ZeroBatch, nil
	}
	batch := d.queue[0]
	d.queue = d.queue[1:]
	return batch, nil
}

func (d *dummyQueue) Close() error {
	return nil
}
