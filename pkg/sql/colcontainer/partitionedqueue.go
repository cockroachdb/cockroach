// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colcontainer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// PartitionedQueue is the abstraction for on-disk storage.
type PartitionedQueue interface {
	// Enqueue adds the batch to the end of the partitionIdx'th partition. If a
	// partition at that index does not exist, a new one is created.
	Enqueue(ctx context.Context, partitionIdx int, batch coldata.Batch) error
	// Dequeue removes and returns the batch from the front of the
	// partitionIdx'th partition. If the partition is empty, or no partition at
	// that index was Enqueued to, a zero-length batch is returned. Note that
	// it is illegal to call Enqueue on a partition after Dequeue.
	// TODO(asubiotto): If we change the DiskQueue to have the same semantics, we
	//  could probably reduce the memory overhead of a DiskQueue.
	Dequeue(partitionIdx int, batch coldata.Batch) error
	// Close closes all partitions created.
	Close() error
}

// partition is a simple wrapper over a Queue used by the PartitionedDiskQueue.
type partition struct {
	Queue
	// reading is set to true when a partition has been Dequeued from. It is
	// illegal to Enqueue to a partition when reading=true, this is specified in
	// the interface contract.
	reading bool
}

type PartitionedDiskQueue struct {
	typs []coltypes.T
	cfg  DiskQueueCfg

	partitionIdxToIndex map[int]int
	partitions          []partition

	fdSemaphore semaphore.Semaphore
}

// NewPartitionedDiskQueue creates a PartitionedDiskQueue whose partitions are
// all on-disk queues. Note that diskQueues will be lazily created when
// enqueueing to a new partition. Each new partition will use
// cfg.BufferSizeBytes, so memory usage may increase in an unbounded fashion.
func NewPartitionedDiskQueue(
	typs []coltypes.T, cfg DiskQueueCfg, fdSemaphore semaphore.Semaphore,
) *PartitionedDiskQueue {
	return &PartitionedDiskQueue{
		typs:                typs,
		cfg:                 cfg,
		partitionIdxToIndex: make(map[int]int),
		partitions:          make([]partition, 0),
		fdSemaphore:         fdSemaphore,
	}
}

func (p *PartitionedDiskQueue) Enqueue(
	ctx context.Context, partitionIdx int, batch coldata.Batch,
) error {
	idx, ok := p.partitionIdxToIndex[partitionIdx]
	if !ok {
		// Acquire only one file descriptor. This will represent the write file
		// descriptor. When we start Dequeueing from this partition, this will
		// represent the read file descriptor.
		if err := p.fdSemaphore.Acquire(ctx, 1); err != nil {
			return err
		}
		// Partition has not been created yet.
		q, err := NewDiskQueue(p.typs, p.cfg)
		if err != nil {
			return err
		}
		idx = len(p.partitions)
		p.partitions = append(p.partitions, partition{Queue: q})
		p.partitionIdxToIndex[partitionIdx] = idx
	}
	if p.partitions[idx].reading {
		return errors.New("Enqueue illegally called after Dequeue")
	}
	return p.partitions[idx].Enqueue(batch)
}

func (p *PartitionedDiskQueue) Dequeue(partitionIdx int, batch coldata.Batch) error {
	idx, ok := p.partitionIdxToIndex[partitionIdx]
	if !ok {
		batch.SetLength(0)
		return nil
	}
	partition := p.partitions[idx]
	if !partition.reading {
		// coldata.ZeroBatch signals to the DiskQueue that no more writes will come
		// in, which allows us to avoid acquiring a new file descriptor for the read
		// file.
		if err := partition.Enqueue(coldata.ZeroBatch); err != nil {
			return err
		}
		p.partitions[idx].reading = true
	}
	notEmpty, err := p.partitions[idx].Dequeue(batch)
	if err != nil {
		return err
	}
	if !notEmpty {
		// The queue was empty.
		batch.SetLength(0)
	}
	return nil
}

// Close closes all the PartitionedDiskQueue's partitions. If an error is
// encountered, the PartitionedDiskQueue will attempt to close all partitions
// anyway and return the last error encountered.
func (p *PartitionedDiskQueue) Close() error {
	var lastErr error
	for _, q := range p.partitions {
		lastErr = q.Close()
	}
	p.fdSemaphore.Release(len(p.partitions))
	return lastErr
}
