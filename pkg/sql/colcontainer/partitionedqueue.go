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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// PartitionedQueue is the abstraction for on-disk storage.
type PartitionedQueue interface {
	// Enqueue adds the batch to the end of the partitionIdx'th partition. If a
	// partition at that index does not exist, a new one is created. Existing
	// partitions may not be Enqueued to after calling
	// CloseAllOpenWriteFileDescriptors. A zero-length batch must be enqueued as
	// the last one.
	// WARNING: Selection vectors are ignored.
	Enqueue(ctx context.Context, partitionIdx int, batch coldata.Batch) error
	// Dequeue removes and returns the batch from the front of the
	// partitionIdx'th partition. If the partition is empty, or no partition at
	// that index was Enqueued to, a zero-length batch is returned. Note that
	// it is illegal to call Enqueue on a partition after Dequeue.
	Dequeue(ctx context.Context, partitionIdx int, batch coldata.Batch) error
	// CloseAllOpenWriteFileDescriptors notifies the PartitionedQueue that it can
	// close all open write file descriptors. After this point, only new
	// partitions may be Enqueued to.
	CloseAllOpenWriteFileDescriptors(ctx context.Context) error
	// CloseAllOpenReadFileDescriptors closes the open read file descriptors
	// belonging to partitions. These partitions may still be Dequeued from,
	// although this will trigger files to be reopened.
	CloseAllOpenReadFileDescriptors() error
	// CloseInactiveReadPartitions closes all partitions that have been Dequeued
	// from and have either been temporarily closed through
	// CloseAllOpenReadFileDescriptors or have returned a coldata.ZeroBatch from
	// Dequeue. This close removes the underlying files.
	CloseInactiveReadPartitions(ctx context.Context) error
	// Close closes all partitions created.
	Close(ctx context.Context) error
}

// partitionState is the state a partition is in.
type partitionState int

const (
	// partitionStateWriting is the initial state of a partition. A partition will
	// always transition to partitionStateClosedForWriting next.
	partitionStateWriting partitionState = iota
	// partitionStateClosedForWriting is the state a partition is in when it is
	// closed for writing. The next possible state is for reads to happen, and
	// thus a transition to partitionStateReading. Note that if a partition is
	// in this state when entering a method of PartitionedDiskQueue, it is always
	// true that the file descriptor for this partition has been released.
	partitionStateClosedForWriting
	// partitionStateReading is the state a partition is in when Dequeue has been
	// called. It is always the case that this partition has been closed for
	// writing and may not transition back to partitionStateWriting or
	// partitionStateClosedForWriting. The read file descriptor for this partition
	// may be closed in this state, although it will be reacquired on the next
	// read.
	partitionStateReading
	// partitionStateClosedForReading is the state a partition is in when a
	// partition was in partitionStateReading and CloseAllOpenReadFileDescriptors
	// was called. If Dequeued, this partition will reacquire a file descriptor
	// and transition back to partitionStateReading.
	partitionStateClosedForReading
	// partitionStatePermanentlyClosed is the state a partition is in when its
	// underlying DiskQueue has been Closed.
	partitionStatePermanentlyClosed
)

// partition is a simple wrapper over a Queue used by the PartitionedDiskQueue.
type partition struct {
	Queue
	state partitionState
}

// PartitionerStrategy describes a strategy used by the PartitionedQueue during
// its operation.
type PartitionerStrategy int

const (
	// PartitionerStrategyDefault is a partitioner strategy in which the
	// PartitionedQueue will keep all partitions open for writing.
	// Note that this uses up as many file descriptors as partitions.
	PartitionerStrategyDefault PartitionerStrategy = iota
	// PartitionerStrategyCloseOnNewPartition is a partitioner strategy that
	// closes an open partition for writing if a new partition is created. This
	// ensures that the total number of file descriptors remains at 1. However,
	// note that closed partitions may never be written to again, only read.
	PartitionerStrategyCloseOnNewPartition
)

// PartitionedDiskQueue is a PartitionedQueue whose partitions are on-disk.
type PartitionedDiskQueue struct {
	typs     []*types.T
	strategy PartitionerStrategy
	cfg      DiskQueueCfg

	partitionIdxToIndex map[int]int
	partitions          []partition

	// lastEnqueuedPartitionIdx is the index of the last enqueued partition. Set
	// to -1 during initialization.
	lastEnqueuedPartitionIdx int

	numOpenFDs  int
	fdSemaphore semaphore.Semaphore
	diskAcc     *mon.BoundAccount
}

var _ PartitionedQueue = &PartitionedDiskQueue{}

// NewPartitionedDiskQueue creates a PartitionedDiskQueue whose partitions are
// all on-disk queues. Note that diskQueues will be lazily created when
// enqueueing to a new partition. Each new partition will use
// cfg.BufferSizeBytes, so memory usage may increase in an unbounded fashion if
// used unmethodically. The file descriptors are acquired through fdSemaphore.
// If fdSemaphore is nil, the partitioned disk queue will not Acquire or Release
// file descriptors. Do this if the caller knows that it will use a constant
// maximum number of file descriptors and wishes to acquire these up front.
// Note that actual file descriptors open may be less than, but never more than
// the number acquired through the semaphore.
func NewPartitionedDiskQueue(
	typs []*types.T,
	cfg DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	partitionerStrategy PartitionerStrategy,
	diskAcc *mon.BoundAccount,
) *PartitionedDiskQueue {
	return &PartitionedDiskQueue{
		typs:                     typs,
		strategy:                 partitionerStrategy,
		cfg:                      cfg,
		partitionIdxToIndex:      make(map[int]int),
		partitions:               make([]partition, 0),
		lastEnqueuedPartitionIdx: -1,
		fdSemaphore:              fdSemaphore,
		diskAcc:                  diskAcc,
	}
}

type closeWritePartitionArgument int

const (
	retainFD closeWritePartitionArgument = iota
	releaseFD
)

// closeWritePartition enqueues a coldata.ZeroBatch to a partition at index
// idx, resulting in it closing its write file descriptor. This partition may
// still be read from, but never written to again. Note that releaseFDOption
// should always be retainFD if a new file is opened in the same scope, to avoid
// having to re-enter the semaphore. The argument should only be releaseFD if
// reopening a different file in a different scope.
func (p *PartitionedDiskQueue) closeWritePartition(
	ctx context.Context, idx int, releaseFDOption closeWritePartitionArgument,
) error {
	if p.partitions[idx].state != partitionStateWriting {
		colexecerror.InternalError(errors.AssertionFailedf("illegal state change from %d to partitionStateClosedForWriting, only partitionStateWriting allowed", p.partitions[idx].state))
	}
	if err := p.partitions[idx].Enqueue(ctx, coldata.ZeroBatch); err != nil {
		return err
	}
	if releaseFDOption == releaseFD && p.fdSemaphore != nil {
		p.fdSemaphore.Release(1)
		p.numOpenFDs--
	}
	p.partitions[idx].state = partitionStateClosedForWriting
	return nil
}

func (p *PartitionedDiskQueue) closeReadPartition(idx int) error {
	if p.partitions[idx].state != partitionStateReading {
		colexecerror.InternalError(errors.AssertionFailedf("illegal state change from %d to partitionStateClosedForReading, only partitionStateReading allowed", p.partitions[idx].state))
	}
	if err := p.partitions[idx].CloseRead(); err != nil {
		return err
	}
	if p.fdSemaphore != nil {
		p.fdSemaphore.Release(1)
		p.numOpenFDs--
	}
	p.partitions[idx].state = partitionStateClosedForReading
	return nil
}

func (p *PartitionedDiskQueue) acquireNewFD(ctx context.Context) error {
	if p.fdSemaphore == nil {
		return nil
	}
	if err := p.fdSemaphore.Acquire(ctx, 1); err != nil {
		return err
	}
	p.numOpenFDs++
	return nil
}

// Enqueue enqueues a batch at partition partitionIdx.
func (p *PartitionedDiskQueue) Enqueue(
	ctx context.Context, partitionIdx int, batch coldata.Batch,
) error {
	idx, ok := p.partitionIdxToIndex[partitionIdx]
	if !ok {
		needToAcquireFD := true
		if p.strategy == PartitionerStrategyCloseOnNewPartition && p.lastEnqueuedPartitionIdx != -1 {
			idxToClose, found := p.partitionIdxToIndex[p.lastEnqueuedPartitionIdx]
			if !found {
				// This would be unexpected.
				return errors.New("PartitionerStrategyCloseOnNewPartition unable to find last Enqueued partition")
			}
			if p.partitions[idxToClose].state == partitionStateWriting {
				// Close the last enqueued partition. No need to release or acquire a new
				// file descriptor, since the acquired FD will represent the new
				// partition's FD opened in Enqueue below.
				if err := p.closeWritePartition(ctx, idxToClose, retainFD); err != nil {
					return err
				}
				needToAcquireFD = false
			} else {
				// The partition that was last enqueued to is not open for writing, so
				// we need to acquire a new FD for this new partition.
				needToAcquireFD = true
			}
		}

		if needToAcquireFD {
			// Acquire only one file descriptor. This will represent the write file
			// descriptor. When we start Dequeueing from this partition, this will
			// represent the read file descriptor.
			if err := p.acquireNewFD(ctx); err != nil {
				return err
			}
		}
		// Partition has not been created yet.
		q, err := NewDiskQueue(ctx, p.typs, p.cfg, p.diskAcc)
		if err != nil {
			return err
		}
		idx = len(p.partitions)
		p.partitions = append(p.partitions, partition{Queue: q})
		p.partitionIdxToIndex[partitionIdx] = idx
	}
	if state := p.partitions[idx].state; state != partitionStateWriting {
		if state == partitionStatePermanentlyClosed {
			return errors.Errorf("partition at index %d permanently closed, cannot Enqueue", partitionIdx)
		}
		return errors.New("Enqueue illegally called after Dequeue or CloseAllOpenWriteFileDescriptors")
	}
	p.lastEnqueuedPartitionIdx = partitionIdx
	return p.partitions[idx].Enqueue(ctx, batch)
}

// Dequeue dequeues a batch from partition partitionIdx, returns a
// coldata.ZeroBatch if that partition does not exist. If the partition exists
// and a coldata.ZeroBatch is returned, that partition is closed.
func (p *PartitionedDiskQueue) Dequeue(
	ctx context.Context, partitionIdx int, batch coldata.Batch,
) error {
	idx, ok := p.partitionIdxToIndex[partitionIdx]
	if !ok {
		batch.SetLength(0)
		return nil
	}
	switch state := p.partitions[idx].state; state {
	case partitionStateWriting:
		// Close this partition for writing. However, we keep a file descriptor
		// acquired for the read file descriptor opened for Dequeue.
		if err := p.closeWritePartition(ctx, idx, retainFD); err != nil {
			return err
		}
		p.partitions[idx].state = partitionStateReading
	case partitionStateClosedForWriting, partitionStateClosedForReading:
		// There will never be a file descriptor acquired for a partition in this
		// state, so acquire it.
		if err := p.acquireNewFD(ctx); err != nil {
			return err
		}
		p.partitions[idx].state = partitionStateReading
	case partitionStateReading:
	// Do nothing.
	case partitionStatePermanentlyClosed:
		return errors.Errorf("partition at index %d permanently closed, cannot Dequeue", partitionIdx)
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unhandled state %d", state))
	}
	notEmpty, err := p.partitions[idx].Dequeue(ctx, batch)
	if err != nil {
		return err
	}
	if batch.Length() == 0 {
		// Queue is finished, release the acquired file descriptor.
		if err := p.closeReadPartition(idx); err != nil {
			return err
		}
	}
	if !notEmpty {
		// This should never happen. It simply means that there was no batch to
		// Dequeue but more batches will be added in the future (i.e. a zero batch
		// was never enqueued). Since we require partitions to be closed for writing
		// before reading, this state is unexpected.
		colexecerror.InternalError(
			errors.AssertionFailedf("DiskQueue unexpectedly returned that more data will be added"))
	}
	return nil
}

// CloseAllOpenWriteFileDescriptors closes all open write file descriptors
// belonging to partitions that are being Enqueued to. Once this method is
// called, existing partitions may not be enqueued to again.
func (p *PartitionedDiskQueue) CloseAllOpenWriteFileDescriptors(ctx context.Context) error {
	for i, q := range p.partitions {
		if q.state != partitionStateWriting {
			continue
		}
		// closeWritePartition will Release the file descriptor.
		if err := p.closeWritePartition(ctx, i, releaseFD); err != nil {
			return err
		}
	}
	return nil
}

// CloseAllOpenReadFileDescriptors closes all open read file descriptors
// belonging to partitions that are being Dequeued from. If Dequeue is called
// on a closed partition, it will be reopened.
func (p *PartitionedDiskQueue) CloseAllOpenReadFileDescriptors() error {
	for i, q := range p.partitions {
		if q.state != partitionStateReading {
			continue
		}
		// closeReadPartition will Release the file descriptor.
		if err := p.closeReadPartition(i); err != nil {
			return err
		}
	}
	return nil
}

// CloseInactiveReadPartitions closes all partitions that were Dequeued from
// and either Dequeued a coldata.ZeroBatch or were closed through
// CloseAllOpenReadFileDescriptors. This method call Closes the underlying
// DiskQueue to remove its files, so a partition may never be used again.
func (p *PartitionedDiskQueue) CloseInactiveReadPartitions(ctx context.Context) error {
	var lastErr error
	for i, q := range p.partitions {
		if q.state != partitionStateClosedForReading {
			continue
		}
		lastErr = q.Close(ctx)
		p.partitions[i].state = partitionStatePermanentlyClosed
	}
	return lastErr
}

// Close closes all the PartitionedDiskQueue's partitions. If an error is
// encountered, the PartitionedDiskQueue will attempt to close all partitions
// anyway and return the last error encountered.
func (p *PartitionedDiskQueue) Close(ctx context.Context) error {
	var lastErr error
	for i, q := range p.partitions {
		if q.state == partitionStatePermanentlyClosed {
			// Already closed.
			continue
		}
		lastErr = q.Close(ctx)
		p.partitions[i].state = partitionStatePermanentlyClosed
	}
	if p.numOpenFDs != 0 {
		// Note that if p.numOpenFDs is non-zero, it must be the case that
		// fdSemaphore is non-nil.
		p.fdSemaphore.Release(p.numOpenFDs)
		p.numOpenFDs = 0
	}
	return lastErr
}
