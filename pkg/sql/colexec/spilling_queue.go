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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// spillingQueue is a Queue that uses a fixed-size in-memory circular buffer
// and spills to disk if spillingQueue.items has no more slots available to hold
// a reference to an enqueued batch or the allocator reports that more memory
// than the caller-provided maxMemoryLimit is in use.
// When spilling to disk, a DiskQueue will be created. When spilling batches to
// disk, their memory will first be released using the allocator. When batches
// are read from disk back into memory, that memory will be reclaimed.
// NOTE: When a batch is returned, that batch's memory will still be tracked
// using the allocator. Since the memory in use is fixed, a previously returned
// batch may be overwritten by a batch read from disk. This new batch's memory
// footprint will replace the footprint of the previously returned batch. Since
// batches are unsafe for reuse, it is assumed that the previously returned
// batch is not kept around and thus its referenced memory will be GCed as soon
// as the batch is updated.
type spillingQueue struct {
	unlimitedAllocator *Allocator
	maxMemoryLimit     int64

	typs             []coltypes.T
	items            []coldata.Batch
	curHeadIdx       int
	curTailIdx       int
	numInMemoryItems int
	numOnDiskItems   int

	diskQueueCfg colcontainer.DiskQueueCfg
	diskQueue    colcontainer.Queue
}

// newSpillingQueue creates a new spillingQueue. An unlimited allocator must be
// passed in. The spillingQueue will use this allocator to check whether memory
// usage exceeds the given memory limit and use disk if so.
func newSpillingQueue(
	unlimitedAllocator *Allocator,
	typs []coltypes.T,
	memoryLimit int64,
	cfg colcontainer.DiskQueueCfg,
	batchSize int,
) *spillingQueue {
	// Reduce the memory limit by what the DiskQueue may need to buffer
	// writes/reads.
	memoryLimit -= int64(cfg.BufferSizeBytes)
	if memoryLimit < 0 {
		memoryLimit = 0
	}
	itemsLen := memoryLimit / int64(estimateBatchSizeBytes(typs, batchSize))
	if itemsLen == 0 {
		// Make items at least of length 1. Even though batches will spill to disk
		// directly (this can only happen with a very low memory limit), it's nice
		// to have at least one item in order to be able to deserialize from disk
		// into this slice.
		itemsLen = 1
	}
	return &spillingQueue{
		unlimitedAllocator: unlimitedAllocator,
		maxMemoryLimit:     memoryLimit,
		typs:               typs,
		items:              make([]coldata.Batch, itemsLen),
		diskQueueCfg:       cfg,
	}
}

func (q *spillingQueue) enqueue(batch coldata.Batch) error {
	if batch.Length() == 0 {
		return nil
	}

	if q.numOnDiskItems > 0 || q.unlimitedAllocator.Used() > q.maxMemoryLimit || q.numInMemoryItems == len(q.items) {
		// In this case, there is not enough memory available to keep this batch in
		// memory, or the in-memory circular buffer has no slots available (we do
		// an initial estimate of how many batches would fit into the buffer, which
		// might be wrong). The tail of the queue might also already be on disk, in
		// which case that is where the batch must be enqueued to maintain order.
		if err := q.maybeSpillToDisk(); err != nil {
			return err
		}
		q.unlimitedAllocator.ReleaseBatch(batch)
		q.numOnDiskItems++
		return q.diskQueue.Enqueue(batch)
	}

	q.items[q.curTailIdx] = batch
	q.curTailIdx++
	if q.curTailIdx == len(q.items) {
		q.curTailIdx = 0
	}
	q.numInMemoryItems++
	return nil
}

func (q *spillingQueue) dequeue() (coldata.Batch, error) {
	if q.empty() {
		return coldata.ZeroBatch, nil
	}

	if q.numInMemoryItems == 0 {
		// No more in-memory items. Fill the circular buffer as much as possible.
		// Note that there must be at least one element on disk.
		if q.curHeadIdx != q.curTailIdx {
			execerror.VectorizedInternalPanic(fmt.Sprintf("assertion failed in spillingQueue: curHeadIdx != curTailIdx, %d != %d", q.curHeadIdx, q.curTailIdx))
		}
		// NOTE: Only one item is dequeued from disk since a deserialized batch is
		// only valid until the next call to Dequeue. In practice we could Dequeue
		// up until a new file region is loaded (which will overwrite the memory of
		// the previous batches), but Dequeue calls are already amortized, so this
		// is acceptable.
		if q.items[q.curTailIdx] == nil {
			// This occurs when q.items is not enqueued to. This might be due to
			// some batches taking too much memory at the front of the buffer, so
			// we never end up using the whole length of q.items and prefer to
			// enqueue to disk directly.
			q.items[q.curTailIdx] = q.unlimitedAllocator.NewMemBatchWithSize(q.typs, 0 /* size */)
		}
		// Release a batch to make space for a new batch from disk.
		q.unlimitedAllocator.ReleaseBatch(q.items[q.curTailIdx])
		ok, err := q.diskQueue.Dequeue(q.items[q.curTailIdx])
		if err != nil {
			return nil, err
		}
		if !ok {
			// There was no batch to dequeue from disk. This should not really
			// happen, as it should have been caught by the q.empty() check above.
			execerror.VectorizedInternalPanic("disk queue was not empty but failed to dequeue element in spillingQueue")
		}
		q.numOnDiskItems--
		q.numInMemoryItems++
		// Account for this batch's memory.
		q.unlimitedAllocator.RetainBatch(q.items[q.curTailIdx])
		// Move to the next slot in the circular buffer.
		q.curTailIdx++
		if q.curTailIdx == len(q.items) {
			q.curTailIdx = 0
		}
	}

	res := q.items[q.curHeadIdx]
	q.curHeadIdx++
	if q.curHeadIdx == len(q.items) {
		q.curHeadIdx = 0
	}
	q.numInMemoryItems--
	return res, nil
}

func (q *spillingQueue) maybeSpillToDisk() error {
	if q.diskQueue != nil {
		return nil
	}
	diskQueue, err := colcontainer.NewDiskQueue(q.typs, q.diskQueueCfg)
	if err != nil {
		return err
	}
	q.diskQueue = diskQueue
	return nil
}

// empty returns whether there are currently no items to be dequeued.
func (q *spillingQueue) empty() bool {
	return q.numInMemoryItems == 0 && q.numOnDiskItems == 0
}

func (q *spillingQueue) spilled() bool {
	return q.diskQueue != nil
}

func (q *spillingQueue) close() error {
	if q.diskQueue != nil {
		return q.diskQueue.Close()
	}
	return nil
}

func (q *spillingQueue) reset() {
	if err := q.close(); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	q.diskQueue = nil
	q.numInMemoryItems = 0
	q.numOnDiskItems = 0
	q.curHeadIdx = 0
	q.curTailIdx = 0
}
