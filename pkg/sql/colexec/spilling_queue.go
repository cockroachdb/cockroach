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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// spillingQueue is a Queue that uses a fixed-size in-memory circular buffer
// and spills to disk if spillingQueue.items has no more slots available to hold
// a reference to an enqueued batch or the allocator reports that more memory
// than the caller-provided maxMemoryLimit is in use. spillingQueue.items is
// growing dynamically until the estimated maximum of slots available to the
// queue based on the memory limit.
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
	unlimitedAllocator *colmem.Allocator
	maxMemoryLimit     int64

	typs             []*types.T
	items            []coldata.Batch
	maxItemsLen      int
	curHeadIdx       int
	curTailIdx       int
	numInMemoryItems int
	numOnDiskItems   int
	closed           bool

	// nextInMemBatchCapacity indicates the capacity which the new batch that
	// we'll append to items should be allocated with. It'll increase
	// dynamically until coldata.BatchSize().
	nextInMemBatchCapacity int

	diskQueueCfg                colcontainer.DiskQueueCfg
	diskQueue                   colcontainer.Queue
	diskQueueDeselectionScratch coldata.Batch
	fdSemaphore                 semaphore.Semaphore
	dequeueScratch              coldata.Batch

	rewindable      bool
	rewindableState struct {
		numItemsDequeued int
	}

	testingKnobs struct {
		// numEnqueues tracks the number of times enqueue() has been called with
		// non-zero batch.
		numEnqueues int
		// maxNumBatchesEnqueuedInMemory, if greater than 0, indicates the
		// maximum number of batches that are attempted to be enqueued to the
		// in-memory buffer 'items' (other limiting conditions might occur
		// earlier). Once numEnqueues reaches this limit, all consequent calls
		// to enqueue() will use the disk queue.
		maxNumBatchesEnqueuedInMemory int
	}

	diskAcc *mon.BoundAccount
}

// spillingQueueInitialItemsLen is the initial capacity of the in-memory buffer
// of the spilling queues (memory limit permitting).
const spillingQueueInitialItemsLen = int64(64)

// NewSpillingQueueArgs encompasses all necessary arguments to newSpillingQueue.
type NewSpillingQueueArgs struct {
	UnlimitedAllocator *colmem.Allocator
	Types              []*types.T
	MemoryLimit        int64
	DiskQueueCfg       colcontainer.DiskQueueCfg
	FDSemaphore        semaphore.Semaphore
	DiskAcc            *mon.BoundAccount
}

// newSpillingQueue creates a new spillingQueue. An unlimited allocator must be
// passed in. The spillingQueue will use this allocator to check whether memory
// usage exceeds the given memory limit and use disk if so.
// If fdSemaphore is nil, no Acquire or Release calls will happen. The caller
// may want to do this if requesting FDs up front.
func newSpillingQueue(args *NewSpillingQueueArgs) *spillingQueue {
	// Reduce the memory limit by what the DiskQueue may need to buffer
	// writes/reads.
	memoryLimit := args.MemoryLimit
	memoryLimit -= int64(args.DiskQueueCfg.BufferSizeBytes)
	if memoryLimit < 0 {
		memoryLimit = 0
	}
	perItemMem := int64(colmem.EstimateBatchSizeBytes(args.Types, coldata.BatchSize()))
	// Account for the size of items slice.
	perItemMem += int64(unsafe.Sizeof(coldata.Batch(nil)))
	maxItemsLen := memoryLimit / perItemMem
	if maxItemsLen == 0 {
		// Make items at least of length 1. Even though batches will spill to disk
		// directly (this can only happen with a very low memory limit), it's nice
		// to have at least one item in order to be able to deserialize from disk
		// into this slice.
		maxItemsLen = 1
	}
	itemsLen := spillingQueueInitialItemsLen
	if itemsLen > maxItemsLen {
		itemsLen = maxItemsLen
	}
	return &spillingQueue{
		unlimitedAllocator: args.UnlimitedAllocator,
		maxMemoryLimit:     memoryLimit,
		typs:               args.Types,
		items:              make([]coldata.Batch, itemsLen),
		maxItemsLen:        int(maxItemsLen),
		diskQueueCfg:       args.DiskQueueCfg,
		fdSemaphore:        args.FDSemaphore,
		diskAcc:            args.DiskAcc,
	}
}

// newRewindableSpillingQueue creates a new spillingQueue that can be rewinded
// in order to dequeue all enqueued batches all over again. An unlimited
// allocator must be passed in. The queue will use this allocator to check
// whether memory usage exceeds the given memory limit and use disk if so.
func newRewindableSpillingQueue(args *NewSpillingQueueArgs) *spillingQueue {
	q := newSpillingQueue(args)
	q.rewindable = true
	return q
}

// enqueue adds the provided batch to the queue. Zero-length batch needs to be
// added as the last one.
//
// Passed-in batch is deeply copied, so it can safely reused by the caller. The
// spilling queue coalesces all input tuples into the batches of dynamically
// increasing capacity when those are kept in-memory. It also performs a
// deselection step if necessary when adding the batch to the disk queue.
func (q *spillingQueue) enqueue(ctx context.Context, batch coldata.Batch) error {
	n := batch.Length()
	if n == 0 {
		if q.diskQueue != nil {
			if err := q.diskQueue.Enqueue(ctx, batch); err != nil {
				return err
			}
		}
		return nil
	}
	q.testingKnobs.numEnqueues++

	alreadySpilled := q.numOnDiskItems > 0
	memoryLimitReached := q.unlimitedAllocator.Used() > q.maxMemoryLimit
	maxItemsLenReached := q.numInMemoryItems == len(q.items) && q.numInMemoryItems == q.maxItemsLen
	maxInMemEnqueuesExceeded := q.testingKnobs.maxNumBatchesEnqueuedInMemory != 0 && q.testingKnobs.numEnqueues > q.testingKnobs.maxNumBatchesEnqueuedInMemory
	if alreadySpilled || memoryLimitReached || maxItemsLenReached || maxInMemEnqueuesExceeded {
		// In this case, one of the following conditions is true:
		// 1. the tail of the queue might also already be on disk, in which case
		//    that is where the batch must be enqueued to maintain order
		// 2. there is not enough memory available to keep this batch in memory
		// 3. the in-memory circular buffer has no slots available (we do an
		//    initial estimate of how many batches would fit into the buffer,
		//    which might be wrong)
		// 4. we reached the testing limit on the number of items added to the
		//    in-memory buffer
		// so we have to add batch to the disk queue.
		if err := q.maybeSpillToDisk(ctx); err != nil {
			return err
		}
		q.unlimitedAllocator.ReleaseBatch(batch)
		if sel := batch.Selection(); sel != nil {
			// We need to perform the deselection since the disk queue
			// ignores the selection vectors.
			q.diskQueueDeselectionScratch, _ = q.unlimitedAllocator.ResetMaybeReallocate(
				q.typs, q.diskQueueDeselectionScratch, n,
			)
			q.unlimitedAllocator.PerformOperation(q.diskQueueDeselectionScratch.ColVecs(), func() {
				for i := range q.typs {
					q.diskQueueDeselectionScratch.ColVec(i).Copy(
						coldata.CopySliceArgs{
							SliceArgs: coldata.SliceArgs{
								Src:       batch.ColVec(i),
								Sel:       sel,
								SrcEndIdx: n,
							},
						},
					)
				}
				q.diskQueueDeselectionScratch.SetLength(n)
			})
			batch = q.diskQueueDeselectionScratch
		}
		if err := q.diskQueue.Enqueue(ctx, batch); err != nil {
			return err
		}
		q.numOnDiskItems++
		return nil
	}

	if q.numInMemoryItems == len(q.items) {
		// We need to reallocate the items slice, and we still have the capacity
		// for it (meaning q.numInMemoryItems < q.maxItemsLen).
		newItemsLen := q.numInMemoryItems * 2
		if newItemsLen > q.maxItemsLen {
			newItemsLen = q.maxItemsLen
		}
		newItems := make([]coldata.Batch, newItemsLen)
		if q.curHeadIdx < q.curTailIdx {
			copy(newItems, q.items[q.curHeadIdx:q.curTailIdx])
		} else {
			copy(newItems, q.items[q.curHeadIdx:])
			offset := q.numInMemoryItems - q.curHeadIdx
			copy(newItems[offset:], q.items[:q.curTailIdx])
		}
		q.curHeadIdx = 0
		q.curTailIdx = q.numInMemoryItems
		q.items = newItems
	}

	alreadyCopied := 0
	if q.numInMemoryItems > 0 {
		// If we have already enqueued at least one batch, let's try to copy
		// as many tuples into it as it has the capacity for.
		tailBatchIdx := q.curTailIdx - 1
		if tailBatchIdx < 0 {
			tailBatchIdx = len(q.items) - 1
		}
		tailBatch := q.items[tailBatchIdx]
		if l, c := tailBatch.Length(), tailBatch.Capacity(); l < c {
			alreadyCopied = c - l
			if alreadyCopied > n {
				alreadyCopied = n
			}
			q.unlimitedAllocator.PerformOperation(tailBatch.ColVecs(), func() {
				for i := range q.typs {
					tailBatch.ColVec(i).Copy(
						coldata.CopySliceArgs{
							SliceArgs: coldata.SliceArgs{
								Src:         batch.ColVec(i),
								Sel:         batch.Selection(),
								DestIdx:     l,
								SrcStartIdx: 0,
								SrcEndIdx:   alreadyCopied,
							},
						},
					)
				}
				tailBatch.SetLength(l + alreadyCopied)
			})
			if alreadyCopied == n {
				// We were able to append all of the tuples, so we return early
				// since we don't need to update any of the state.
				return nil
			}
		}
	}

	var newBatchCapacity int
	if q.nextInMemBatchCapacity == coldata.BatchSize() {
		// At this point we only allocate batches with maximum capacity.
		newBatchCapacity = coldata.BatchSize()
	} else {
		newBatchCapacity = n - alreadyCopied
		if q.nextInMemBatchCapacity > newBatchCapacity {
			newBatchCapacity = q.nextInMemBatchCapacity
		}
		q.nextInMemBatchCapacity = 2 * newBatchCapacity
		if q.nextInMemBatchCapacity > coldata.BatchSize() {
			q.nextInMemBatchCapacity = coldata.BatchSize()
		}
	}

	// Note: we could have used NewMemBatchWithFixedCapacity here, but we choose
	// not to in order to indicate that the capacity of the new batches has
	// dynamic behavior.
	newBatch, _ := q.unlimitedAllocator.ResetMaybeReallocate(q.typs, nil /* oldBatch */, newBatchCapacity)
	q.unlimitedAllocator.PerformOperation(newBatch.ColVecs(), func() {
		for i := range q.typs {
			newBatch.ColVec(i).Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:         batch.ColVec(i),
						Sel:         batch.Selection(),
						SrcStartIdx: alreadyCopied,
						SrcEndIdx:   n,
					},
				},
			)
		}
		newBatch.SetLength(n - alreadyCopied)
	})

	q.items[q.curTailIdx] = newBatch
	q.curTailIdx++
	if q.curTailIdx == len(q.items) {
		q.curTailIdx = 0
	}
	q.numInMemoryItems++
	return nil
}

func (q *spillingQueue) dequeue(ctx context.Context) (coldata.Batch, error) {
	if q.empty() {
		return coldata.ZeroBatch, nil
	}

	if (q.rewindable && q.numInMemoryItems <= q.rewindableState.numItemsDequeued) ||
		(!q.rewindable && q.numInMemoryItems == 0) {
		// No more in-memory items. Fill the circular buffer as much as possible.
		// Note that there must be at least one element on disk.
		if !q.rewindable && q.curHeadIdx != q.curTailIdx {
			colexecerror.InternalError(errors.AssertionFailedf("assertion failed in spillingQueue: curHeadIdx != curTailIdx, %d != %d", q.curHeadIdx, q.curTailIdx))
		}
		// NOTE: Only one item is dequeued from disk since a deserialized batch is
		// only valid until the next call to Dequeue. In practice we could Dequeue
		// up until a new file region is loaded (which will overwrite the memory of
		// the previous batches), but Dequeue calls are already amortized, so this
		// is acceptable.
		// Release a batch to make space for a new batch from disk.
		if q.dequeueScratch != nil {
			q.unlimitedAllocator.ReleaseBatch(q.dequeueScratch)
		} else {
			q.dequeueScratch = q.unlimitedAllocator.NewMemBatchWithFixedCapacity(q.typs, coldata.BatchSize())
		}
		ok, err := q.diskQueue.Dequeue(ctx, q.dequeueScratch)
		if err != nil {
			return nil, err
		}
		if !ok {
			// There was no batch to dequeue from disk. This should not really
			// happen, as it should have been caught by the q.empty() check above.
			colexecerror.InternalError(errors.AssertionFailedf("disk queue was not empty but failed to dequeue element in spillingQueue"))
		}
		// Account for this batch's memory.
		q.unlimitedAllocator.RetainBatch(q.dequeueScratch)
		if q.rewindable {
			q.rewindableState.numItemsDequeued++
			return q.dequeueScratch, nil
		}
		q.numOnDiskItems--
		q.numInMemoryItems++
		q.items[q.curTailIdx] = q.dequeueScratch
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
	if q.rewindable {
		q.rewindableState.numItemsDequeued++
	} else {
		q.numInMemoryItems--
	}
	return res, nil
}

func (q *spillingQueue) numFDsOpenAtAnyGivenTime() int {
	if q.diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		// The access pattern must be write-everything then read-everything so
		// either a read FD or a write FD are open at any one point.
		return 1
	}
	// Otherwise, both will be open.
	return 2
}

func (q *spillingQueue) maybeSpillToDisk(ctx context.Context) error {
	if q.diskQueue != nil {
		return nil
	}
	var err error
	// Acquire two file descriptors for the DiskQueue: one for the write file and
	// one for the read file.
	if q.fdSemaphore != nil {
		if err = q.fdSemaphore.Acquire(ctx, q.numFDsOpenAtAnyGivenTime()); err != nil {
			return err
		}
	}
	log.VEvent(ctx, 1, "spilled to disk")
	var diskQueue colcontainer.Queue
	if q.rewindable {
		diskQueue, err = colcontainer.NewRewindableDiskQueue(ctx, q.typs, q.diskQueueCfg, q.diskAcc)
	} else {
		diskQueue, err = colcontainer.NewDiskQueue(ctx, q.typs, q.diskQueueCfg, q.diskAcc)
	}
	if err != nil {
		return err
	}
	// Only assign q.diskQueue if there was no error, otherwise the returned value
	// may be non-nil but invalid.
	q.diskQueue = diskQueue
	return nil
}

// empty returns whether there are currently no items to be dequeued.
func (q *spillingQueue) empty() bool {
	if q.rewindable {
		return q.numInMemoryItems+q.numOnDiskItems == q.rewindableState.numItemsDequeued
	}
	return q.numInMemoryItems == 0 && q.numOnDiskItems == 0
}

func (q *spillingQueue) spilled() bool {
	return q.diskQueue != nil
}

func (q *spillingQueue) close(ctx context.Context) error {
	if q.closed {
		return nil
	}
	if q.diskQueue != nil {
		if err := q.diskQueue.Close(ctx); err != nil {
			return err
		}
		if q.fdSemaphore != nil {
			q.fdSemaphore.Release(q.numFDsOpenAtAnyGivenTime())
		}
		q.closed = true
		return nil
	}
	return nil
}

func (q *spillingQueue) rewind() error {
	if !q.rewindable {
		return errors.Newf("unexpectedly rewind() called when spilling queue is not rewindable")
	}
	if q.diskQueue != nil {
		if err := q.diskQueue.(colcontainer.RewindableQueue).Rewind(); err != nil {
			return err
		}
	}
	q.curHeadIdx = 0
	q.rewindableState.numItemsDequeued = 0
	return nil
}

func (q *spillingQueue) reset(ctx context.Context) {
	if err := q.close(ctx); err != nil {
		colexecerror.InternalError(err)
	}
	q.diskQueue = nil
	q.closed = false
	q.numInMemoryItems = 0
	q.numOnDiskItems = 0
	q.curHeadIdx = 0
	q.curTailIdx = 0
	q.nextInMemBatchCapacity = 0
	q.rewindableState.numItemsDequeued = 0
	q.testingKnobs.numEnqueues = 0
}
