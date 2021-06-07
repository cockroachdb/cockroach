// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// SpillingQueue is a Queue that uses a fixed-size in-memory circular buffer and
// spills to disk if the allocator reports that more memory than the
// caller-provided maxMemoryLimit is in use. SpillingQueue.items is growing
// dynamically.
type SpillingQueue struct {
	unlimitedAllocator *colmem.Allocator
	maxMemoryLimit     int64

	typs             []*types.T
	items            []coldata.Batch
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
	// lastDequeuedBatchMemUsage is the memory footprint of the last batch
	// returned by Dequeue().
	//
	// We track the size instead of the reference to the batch because it is
	// possible that the caller appends new columns, and the memory footprint of
	// those columns is registered with the caller's allocator; therefore, if
	// after the columns have been appended to the last dequeued batch, its
	// footprint would be higher than what the spilling queue's allocator was
	// registered with, and we could mistakenly release excessive amount of
	// memory.
	lastDequeuedBatchMemUsage int64

	rewindable      bool
	rewindableState struct {
		numItemsDequeued int
	}

	testingKnobs struct {
		// numEnqueues tracks the number of times Enqueue() has been called with
		// non-zero batch.
		numEnqueues int
		// maxNumBatchesEnqueuedInMemory, if greater than 0, indicates the
		// maximum number of batches that are attempted to be enqueued to the
		// in-memory buffer 'items' (other limiting conditions might occur
		// earlier). Once numEnqueues reaches this limit, all consequent calls
		// to Enqueue() will use the disk queue.
		maxNumBatchesEnqueuedInMemory int
	}

	// testingObservability stores some observability information about the
	// state of the spilling queue when it was closed.
	testingObservability struct {
		spilled     bool
		memoryUsage int64
	}

	diskAcc *mon.BoundAccount
}

// spillingQueueInitialItemsLen is the initial capacity of the in-memory buffer
// of the spilling queues (memory limit permitting).
var spillingQueueInitialItemsLen = int64(util.ConstantWithMetamorphicTestRange(
	"spilling-queue-initial-len",
	64 /* defaultValue */, 1 /* min */, 16, /* max */
))

// NewSpillingQueueArgs encompasses all necessary arguments to NewSpillingQueue.
type NewSpillingQueueArgs struct {
	UnlimitedAllocator *colmem.Allocator
	Types              []*types.T
	MemoryLimit        int64
	DiskQueueCfg       colcontainer.DiskQueueCfg
	FDSemaphore        semaphore.Semaphore
	DiskAcc            *mon.BoundAccount
}

// NewSpillingQueue creates a new SpillingQueue. An unlimited allocator must be
// passed in. The SpillingQueue will use this allocator to check whether memory
// usage exceeds the given memory limit and use disk if so.
// If fdSemaphore is nil, no Acquire or Release calls will happen. The caller
// may want to do this if requesting FDs up front.
func NewSpillingQueue(args *NewSpillingQueueArgs) *SpillingQueue {
	var items []coldata.Batch
	if args.MemoryLimit > 0 {
		items = make([]coldata.Batch, spillingQueueInitialItemsLen)
	}
	return &SpillingQueue{
		unlimitedAllocator: args.UnlimitedAllocator,
		maxMemoryLimit:     args.MemoryLimit,
		typs:               args.Types,
		items:              items,
		diskQueueCfg:       args.DiskQueueCfg,
		fdSemaphore:        args.FDSemaphore,
		diskAcc:            args.DiskAcc,
	}
}

// NewRewindableSpillingQueue creates a new SpillingQueue that can be rewinded
// in order to Dequeue all enqueued batches all over again. An unlimited
// allocator must be passed in. The queue will use this allocator to check
// whether memory usage exceeds the given memory limit and use disk if so.
//
// WARNING: when using a rewindable queue all Enqueue() operations *must* occur
// before any Dequeue() calls (it is a limitation of
// colcontainer.RewindableQueue interface).
func NewRewindableSpillingQueue(args *NewSpillingQueueArgs) *SpillingQueue {
	q := NewSpillingQueue(args)
	q.rewindable = true
	return q
}

// Enqueue adds the provided batch to the queue. Zero-length batch needs to be
// added as the last one.
//
// Passed-in batch is deeply copied, so it can be safely reused by the caller.
// The spilling queue coalesces all input tuples into the batches of dynamically
// increasing capacity when those are kept in-memory. It also performs a
// deselection step if necessary when adding the batch to the disk queue.
//
// The ownership of the batch still lies with the caller, so the caller is
// responsible for accounting for the memory used by batch (although the
// spilling queue will account for memory used by the in-memory copies).
func (q *SpillingQueue) Enqueue(ctx context.Context, batch coldata.Batch) {
	if q.rewindable && q.rewindableState.numItemsDequeued > 0 {
		colexecerror.InternalError(errors.Errorf("attempted to Enqueue to rewindable SpillingQueue after Dequeue has been called"))
	}

	n := batch.Length()
	if n == 0 {
		if q.diskQueue != nil {
			if err := q.diskQueue.Enqueue(ctx, batch); err != nil {
				HandleErrorFromDiskQueue(err)
			}
		}
		return
	}
	q.testingKnobs.numEnqueues++

	alreadySpilled := q.numOnDiskItems > 0
	memoryLimitReached := q.unlimitedAllocator.Used() > q.maxMemoryLimit || q.maxMemoryLimit <= 0
	maxInMemEnqueuesExceeded := q.testingKnobs.maxNumBatchesEnqueuedInMemory != 0 && q.testingKnobs.numEnqueues > q.testingKnobs.maxNumBatchesEnqueuedInMemory
	if alreadySpilled || memoryLimitReached || maxInMemEnqueuesExceeded {
		// In this case, one of the following conditions is true:
		// 1. the tail of the queue might also already be on disk, in which case
		//    that is where the batch must be enqueued to maintain order
		// 2. there is not enough memory available to keep this batch in memory
		// 3. we reached the testing limit on the number of items added to the
		//    in-memory buffer
		// so we have to add batch to the disk queue.
		if err := q.maybeSpillToDisk(ctx); err != nil {
			HandleErrorFromDiskQueue(err)
		}
		if sel := batch.Selection(); sel != nil {
			// We need to perform the deselection since the disk queue
			// ignores the selection vectors.
			//
			// We want to fit all deselected tuples into a single batch, so we
			// don't enforce footprint based memory limit on a batch size.
			const maxBatchMemSize = math.MaxInt64
			q.diskQueueDeselectionScratch, _ = q.unlimitedAllocator.ResetMaybeReallocate(
				q.typs, q.diskQueueDeselectionScratch, n, maxBatchMemSize,
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
			HandleErrorFromDiskQueue(err)
		}
		q.numOnDiskItems++
		return
	}

	if q.numInMemoryItems == len(q.items) {
		// We need to reallocate the items slice. Note that actually we might
		// reach the memory limit before we fill up the newly allocated slice,
		// in which case we will start adding incoming batches to the disk queue
		// (condition 2. from above).
		newItems := make([]coldata.Batch, q.numInMemoryItems*2)
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
				return
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
	newBatch, _ := q.unlimitedAllocator.ResetMaybeReallocate(
		q.typs,
		nil, /* oldBatch */
		newBatchCapacity,
		// No limit on the batch mem size here, however, we will be paying
		// attention to the memory registered with the unlimited allocator, and
		// we will stop adding tuples into this batch and spill when needed.
		math.MaxInt64, /* maxBatchMemSize */
	)
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
}

// Dequeue returns the next batch from the queue which is valid only until the
// next call to Dequeue(). The memory usage of the returned batch is still
// retained by the spilling queue's allocator, so the caller doesn't have to be
// concerned with memory management.
//
// If the spilling queue is rewindable, the batch *cannot* be modified
// (otherwise, after Rewind(), the queue will contain the corrupted data).
//
// If the spilling queue is not rewindable, the caller is free to modify the
// batch.
func (q *SpillingQueue) Dequeue(ctx context.Context) (coldata.Batch, error) {
	if q.Empty() {
		if (!q.rewindable || q.numOnDiskItems != 0) && q.lastDequeuedBatchMemUsage != 0 {
			// We need to release the memory used by the last dequeued batch in
			// all cases except for when that batch came from the in-memory
			// buffer of the rewindable queue.
			q.unlimitedAllocator.ReleaseMemory(q.lastDequeuedBatchMemUsage)
			q.lastDequeuedBatchMemUsage = 0
		}
		return coldata.ZeroBatch, nil
	}

	if (q.rewindable && q.numInMemoryItems <= q.rewindableState.numItemsDequeued) ||
		(!q.rewindable && q.numInMemoryItems == 0) {
		// No more in-memory items. Fill the circular buffer as much as possible.
		// Note that there must be at least one element on disk.
		if !q.rewindable && q.curHeadIdx != q.curTailIdx {
			colexecerror.InternalError(errors.AssertionFailedf("assertion failed in SpillingQueue: curHeadIdx != curTailIdx, %d != %d", q.curHeadIdx, q.curTailIdx))
		}
		// NOTE: Only one item is dequeued from disk since a deserialized batch is
		// only valid until the next call to Dequeue. In practice we could Dequeue
		// up until a new file region is loaded (which will overwrite the memory of
		// the previous batches), but Dequeue calls are already amortized, so this
		// is acceptable.
		if q.dequeueScratch == nil {
			// In order to have precise memory accounting, we use the following
			// scheme for the newly allocated dequeueScratch.
			// 1. a new batch is allocated, its estimated memory usage is
			//    registered with the allocator
			// 2. we release the batch's memory right away so that the estimate
			//    is unregistered
			// 3. once the actual data is dequeued from disk into the batch, we
			//    update the allocator with the actual memory usage.
			q.dequeueScratch = q.unlimitedAllocator.NewMemBatchWithFixedCapacity(q.typs, coldata.BatchSize())
			q.unlimitedAllocator.ReleaseMemory(colmem.GetBatchMemSize(q.dequeueScratch))
		}
		ok, err := q.diskQueue.Dequeue(ctx, q.dequeueScratch)
		if err != nil {
			return nil, err
		}
		if !ok {
			// There was no batch to Dequeue from disk. This should not really
			// happen, as it should have been caught by the q.empty() check above.
			colexecerror.InternalError(errors.AssertionFailedf("disk queue was not empty but failed to Dequeue element in SpillingQueue"))
		}
		// Release the memory used by the batch returned on the previous call
		// to Dequeue() since that batch is no longer valid. Note that it
		// doesn't matter whether that previous batch came from the in-memory
		// buffer or from the disk queue since in the former case the reference
		// to the batch is lost and in the latter case we've just reused the
		// batch to Dequeue() from disk into it.
		q.unlimitedAllocator.ReleaseMemory(q.lastDequeuedBatchMemUsage)
		q.lastDequeuedBatchMemUsage = colmem.GetBatchMemSize(q.dequeueScratch)
		q.unlimitedAllocator.AdjustMemoryUsage(q.lastDequeuedBatchMemUsage)
		if q.rewindable {
			q.rewindableState.numItemsDequeued++
		} else {
			q.numOnDiskItems--
		}
		return q.dequeueScratch, nil
	}

	res := q.items[q.curHeadIdx]
	if q.rewindable {
		// Note that in case of a rewindable queue we do not update the memory
		// accounting since all of the batches in the in-memory buffer are still
		// kept.
		q.rewindableState.numItemsDequeued++
	} else {
		// Release the reference to the batch eagerly.
		q.items[q.curHeadIdx] = nil
		// Release the memory used by the batch returned on the previous call
		// to Dequeue() since that batch is no longer valid. Since res came from
		// the in-memory buffer, the previous batch must have come from the
		// in-memory buffer too and we released the reference to it on the
		// previous call.
		q.unlimitedAllocator.ReleaseMemory(q.lastDequeuedBatchMemUsage)
		q.lastDequeuedBatchMemUsage = colmem.GetBatchMemSize(res)
		q.numInMemoryItems--
	}
	q.curHeadIdx++
	if q.curHeadIdx == len(q.items) {
		q.curHeadIdx = 0
	}
	return res, nil
}

func (q *SpillingQueue) numFDsOpenAtAnyGivenTime() int {
	if q.diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		// The access pattern must be write-everything then read-everything so
		// either a read FD or a write FD are open at any one point.
		return 1
	}
	// Otherwise, both will be open.
	return 2
}

func (q *SpillingQueue) maybeSpillToDisk(ctx context.Context) error {
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
	// Decrease the memory limit by the amount the disk queue will use to buffer
	// writes/reads.
	q.maxMemoryLimit -= int64(q.diskQueueCfg.BufferSizeBytes)
	// We are definitely exceeding the memory limit now, so we will move as many
	// batches from the tail of the in-memory buffer onto the disk queue as
	// needed to satisfy the limit again.
	//
	// queueTailToMove will contain the batches from the tail of the queue to
	// move to disk. The batches are in the reversed order (i.e the last, second
	// to last, third to last, etc).
	//
	// Note that if the queue is rewindable, then Dequeue() hasn't been called
	// yet (otherwise, an assertion in Enqueue() would have fired), so we don't
	// need to concern ourselves with the rewindable state.
	var queueTailToMove []coldata.Batch
	for q.numInMemoryItems > 0 && q.unlimitedAllocator.Used() > q.maxMemoryLimit {
		tailBatchIdx := q.curTailIdx - 1
		if tailBatchIdx < 0 {
			tailBatchIdx = len(q.items) - 1
		}
		tailBatch := q.items[tailBatchIdx]
		queueTailToMove = append(queueTailToMove, tailBatch)
		q.items[tailBatchIdx] = nil
		// We will release the memory a bit early (before enqueueing to the disk
		// queue) since it simplifies the calculation of how many batches should
		// be moved.
		q.unlimitedAllocator.ReleaseMemory(colmem.GetBatchMemSize(tailBatch))
		q.numInMemoryItems--
		q.curTailIdx--
		if q.curTailIdx < 0 {
			q.curTailIdx = len(q.items) - 1
		}
	}
	for i := len(queueTailToMove) - 1; i >= 0; i-- {
		// Note that these batches definitely do not have selection vectors
		// since the deselection is performed during the copying in Enqueue().
		if err := q.diskQueue.Enqueue(ctx, queueTailToMove[i]); err != nil {
			return err
		}
		q.numOnDiskItems++
	}
	return nil
}

// Empty returns whether there are currently no items to be dequeued.
func (q *SpillingQueue) Empty() bool {
	if q.rewindable {
		return q.numInMemoryItems+q.numOnDiskItems == q.rewindableState.numItemsDequeued
	}
	return q.numInMemoryItems == 0 && q.numOnDiskItems == 0
}

// Spilled returns whether the spilling queue has spilled to disk. Note that if
// the spilling queue has been closed, then it returns whether the queue spilled
// before being closed.
func (q *SpillingQueue) Spilled() bool {
	if q.closed {
		return q.testingObservability.spilled
	}
	return q.diskQueue != nil
}

// MemoryUsage reports the current memory usage of the spilling queue in bytes.
// Note that if the spilling queue has been closed, then it returns the memory
// usage of the queue before its closure.
func (q *SpillingQueue) MemoryUsage() int64 {
	if q.closed {
		return q.testingObservability.memoryUsage
	}
	return q.unlimitedAllocator.Used()
}

// Close closes the spilling queue.
func (q *SpillingQueue) Close(ctx context.Context) error {
	if q == nil || q.closed {
		return nil
	}
	q.closed = true
	q.testingObservability.spilled = q.diskQueue != nil
	q.testingObservability.memoryUsage = q.unlimitedAllocator.Used()
	q.unlimitedAllocator.ReleaseMemory(q.unlimitedAllocator.Used())
	// Eagerly release references to the in-memory items and scratch batches.
	// Note that we don't lose the reference to 'items' slice itself so that it
	// can be reused in case Close() is called by Reset().
	for i := range q.items {
		q.items[i] = nil
	}
	q.diskQueueDeselectionScratch = nil
	q.dequeueScratch = nil
	if q.diskQueue != nil {
		if err := q.diskQueue.Close(ctx); err != nil {
			return err
		}
		q.diskQueue = nil
		if q.fdSemaphore != nil {
			q.fdSemaphore.Release(q.numFDsOpenAtAnyGivenTime())
		}
		q.maxMemoryLimit += int64(q.diskQueueCfg.BufferSizeBytes)
	}
	return nil
}

// Rewind rewinds the spilling queue.
func (q *SpillingQueue) Rewind() error {
	if !q.rewindable {
		return errors.Newf("unexpectedly Rewind() called when spilling queue is not rewindable")
	}
	if q.diskQueue != nil {
		if err := q.diskQueue.(colcontainer.RewindableQueue).Rewind(); err != nil {
			return err
		}
	}
	q.curHeadIdx = 0
	q.lastDequeuedBatchMemUsage = 0
	q.rewindableState.numItemsDequeued = 0
	return nil
}

// Reset resets the spilling queue.
func (q *SpillingQueue) Reset(ctx context.Context) {
	if err := q.Close(ctx); err != nil {
		colexecerror.InternalError(err)
	}
	q.closed = false
	q.numInMemoryItems = 0
	q.numOnDiskItems = 0
	q.curHeadIdx = 0
	q.curTailIdx = 0
	q.nextInMemBatchCapacity = 0
	q.lastDequeuedBatchMemUsage = 0
	q.rewindableState.numItemsDequeued = 0
	q.testingKnobs.numEnqueues = 0
}
