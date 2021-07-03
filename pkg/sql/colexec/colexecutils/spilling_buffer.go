// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// SpillingBuffer wraps an AppendOnlyBufferedBatch to store tuples. It supports
// an append operation, as well as access to a (read-only) windowed batch
// starting at a given index.
//
// Once the memory limit is reached, SpillingBuffer spills to a disk queue.
// SpillingBuffer makes a best effort to predict whether appending more tuples
// will cause AppendOnlyBufferedBatch to exceed the memory limit. If this effort
// succeeds, the tuples that are already in memory can be kept there once we
// have spilled to disk. However, if the prediction fails and the in-memory
// tuples exceed the memory limit, all tuples will have to be moved to disk.
//
// Note that due to the use of a disk queue to support direct access operations,
// SpillingBuffer can be slow for certain access patterns once it has spilled to
// disk.
type SpillingBuffer struct {
	memoryLimit        int64
	diskReservedMem    int64
	unlimitedAllocator *colmem.Allocator
	typs               []*types.T

	// colIdxs contains the indexes of the columns that should be taken from the
	// input batch during calls to AppendTuples.
	colIdxs []int

	// bufferedTuples is used to store tuples in-memory.
	bufferedTuples *AppendOnlyBufferedBatch

	// scratch is used when enqueueing into the disk queue to ensure that only the
	// desired rows and columns are added. We do not allocate memory for it
	// because it only provides a window into either input or output data.
	scratch coldata.Batch

	diskQueueCfg colcontainer.DiskQueueCfg
	diskQueue    colcontainer.RewindableQueue
	fdSemaphore  semaphore.Semaphore
	diskAcc      *mon.BoundAccount

	dequeueScratch            coldata.Batch
	lastDequeuedBatchMemUsage int64
	numDequeued               int

	// doneAppending is used to signal whether the disk queue needs to have
	// enqueue called with a zero length batch to signal that no more enqueues
	// will happen, as well as to ensure that no tuples are appended after
	// GetBatchWithTuple is called.
	doneAppending bool
	length        int
	closed        bool

	testingKnobs struct {
		// maxTuplesStoredInMemory, if greater than 0, indicates the maximum number
		// of tuples that can be appended to the in-memory batch bufferedTuples
		// (other limiting conditions might occur earlier). Once the length of
		// bufferedTuples reaches this limit, all subsequent calls to AppendTuples
		// will use the disk queue.
		maxTuplesStoredInMemory int
	}
}

// NewSpillingBuffer creates a new SpillingBuffer.
//
// The column indexes that are passed as the last argument(s) are used to
// determine which columns should be used from input batches during calls to
// AppendTuples. If nil, columns at indices 0...len(typs)-1 will be used. Note
// that the given typs slice defines the types of the columns that will be
// stored, but the input batches may have a different schema when colIdxs is not
// nil.
//
// WARNING: when using SpillingBuffer all AppendTuples calls must occur
// before any GetBatchWithTuple calls. This is due to a limitation of the
// rewindable disk queue that is used in the event of spilling to disk.
func NewSpillingBuffer(
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	typs []*types.T,
	diskAcc *mon.BoundAccount,
	colIdxs ...int,
) *SpillingBuffer {
	// Memory used by the AppendOnlyBufferedBatch cannot be partially released
	// (and we would like to keep as many tuples in-memory as possible), so we
	// must reserve memory for the disk queue and dequeue scratch batch.
	diskReservedMem := int64(colmem.EstimateBatchSizeBytes(typs, coldata.BatchSize())) +
		int64(diskQueueCfg.BufferSizeBytes)
	// The SpillingBuffer disk queue always uses
	// DiskQueueCacheModeClearAndReuseCache since all writes happen before any
	// reads.
	diskQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
	if colIdxs == nil {
		colIdxs = make([]int, len(typs))
		for i := range colIdxs {
			colIdxs[i] = i
		}
	}
	if len(colIdxs) != len(typs) {
		colexecerror.InternalError(
			errors.AssertionFailedf("expected %d columns, but got: %d", len(typs), len(colIdxs)))
	}
	return &SpillingBuffer{
		unlimitedAllocator: unlimitedAllocator,
		memoryLimit:        memoryLimit,
		diskReservedMem:    diskReservedMem,
		typs:               typs,
		colIdxs:            colIdxs,
		bufferedTuples:     NewAppendOnlyBufferedBatch(unlimitedAllocator, typs, nil /* colsToStore */),
		scratch:            unlimitedAllocator.NewMemBatchNoCols(typs, 0 /* capacity */),
		diskQueueCfg:       diskQueueCfg,
		fdSemaphore:        fdSemaphore,
		diskAcc:            diskAcc,
	}
}

// The disk queue will always use DiskQueueCacheModeClearAndReuseCache, so
// all writes will always occur before all reads.
const numSpillingBufferFDs = 1

// AppendTuples adds the columns from the given batch signified by colIdxs to
// the SpillingBuffer. Any selection vector on the input batch is ignored.
func (b *SpillingBuffer) AppendTuples(
	ctx context.Context, batch coldata.Batch, startIdx, endIdx int,
) {
	var err error
	if b.doneAppending {
		colexecerror.InternalError(
			errors.AssertionFailedf("attempted to append to SpillingBuffer after calling GetBatchWithTuple"))
	}
	if startIdx >= endIdx || startIdx < 0 || endIdx > batch.Length() {
		colexecerror.InternalError(
			errors.AssertionFailedf("invalid indexes into source batch: %d, %d", startIdx, endIdx))
	}
	// Create a window into the correct columns and rows of the input batch.
	for i, idx := range b.colIdxs {
		window := batch.ColVec(idx).Window(startIdx, endIdx)
		b.scratch.ReplaceCol(window, i)
	}
	b.scratch.SetLength(endIdx - startIdx)
	b.length += endIdx - startIdx
	memLimitReached := b.unlimitedAllocator.Used()+b.diskReservedMem > b.memoryLimit
	maxInMemTuplesLimitReached := b.testingKnobs.maxTuplesStoredInMemory > 0 &&
		b.bufferedTuples.Length() >= b.testingKnobs.maxTuplesStoredInMemory
	if !memLimitReached && b.diskQueue == nil && !maxInMemTuplesLimitReached {
		b.unlimitedAllocator.PerformAppend(b.bufferedTuples, func() {
			b.bufferedTuples.AppendTuples(b.scratch, 0 /* startIdx */, b.scratch.Length())
		})
		return
	}
	// Not all tuples could be stored in-memory; they will have to be placed in
	// the queue.
	if b.diskQueue == nil {
		if b.fdSemaphore != nil {
			if err = b.fdSemaphore.Acquire(ctx, numSpillingBufferFDs); err != nil {
				colexecerror.InternalError(err)
			}
		}
		if b.diskQueue, err =
			colcontainer.NewRewindableDiskQueue(ctx, b.typs, b.diskQueueCfg, b.diskAcc); err != nil {
			colexecerror.InternalError(err)
		}
		log.VEvent(ctx, 1, "spilled to disk")
	}
	if err = b.diskQueue.Enqueue(ctx, b.scratch); err != nil {
		HandleErrorFromDiskQueue(err)
	}
}

// GetBatchWithTuple returns a batch that contains the tuple at the given index
// into all buffered tuples. It also returns the index of the requested tuple
// within the returned batch. The batch is not allowed to be modified, or the
// data stored in SpillingBuffer will be corrupted.
//
// No guarantees are made about the size of the batch beyond that it will
// contain the requested tuple at the returned index (it may contain tuples that
// precede or follow the requested tuple). This is useful for callers that wish
// to iterate through adjacent tuples, and minimizes overhead when the tuple is
// stored in-memory (since the in-memory batch can simply be returned).
//
// For tuples stored on-disk, GetBatchWithTuple optimizes for the case when
// subsequent calls access tuples from the same batch, and to a lesser degree
// when tuples from a subsequent batch are accessed. If the index is less than
// zero or greater than or equal to the buffer length, GetBatchWithTuple will
// panic.
func (b *SpillingBuffer) GetBatchWithTuple(ctx context.Context, idx int) (coldata.Batch, int) {
	var err error
	if idx < 0 || idx >= b.Length() {
		colexecerror.InternalError(
			errors.AssertionFailedf("index out of range for spilling buffer: %d", idx))
	}
	if idx < b.bufferedTuples.Length() {
		// The requested tuple is stored in-memory.
		return b.bufferedTuples, idx
	}
	// The requested tuple is stored on-disk. It will have to be retrieved from
	// the rewindable queue. Normalize the index to refer to a location within the
	// set of all enqueued tuples rather than all buffered tuples.
	idx -= b.bufferedTuples.Length()
	if idx < b.numDequeued {
		// The idx'th tuple is located before the current head of the queue, so we
		// need to rewind. TODO(drewk): look for a more efficient way to handle
		// spilling.
		if err = b.diskQueue.Rewind(); err != nil {
			colexecerror.InternalError(err)
		}
		b.numDequeued = 0
		if b.dequeueScratch != nil {
			b.dequeueScratch.SetLength(0)
		}
	}
	if b.dequeueScratch == nil {
		// Similarly to SpillingQueue, we will unregister the memory estimate for
		// now and then update the memory accounting once we have dequeued into it.
		b.dequeueScratch = b.unlimitedAllocator.NewMemBatchWithFixedCapacity(b.typs, coldata.BatchSize())
		b.unlimitedAllocator.ReleaseMemory(colmem.GetBatchMemSize(b.dequeueScratch))
	}
	if !b.doneAppending {
		b.doneAppending = true
		// We have to enqueue a zero-length batch to the disk queue before we can
		// call Dequeue.
		if err = b.diskQueue.Enqueue(ctx, coldata.ZeroBatch); err != nil {
			HandleErrorFromDiskQueue(err)
		}
	}
	// Dequeue batches until we reach the one with the idx'th tuple.
	for {
		if idx-b.numDequeued < b.dequeueScratch.Length() {
			// The requested tuple is within the dequeued batch. Return the dequeued
			// batch along with the index of the requested tuple within it.
			tupleIdx := idx - b.numDequeued

			// Release the memory for the last dequeued batch since we've just reused
			// it, then account for the current one.
			b.unlimitedAllocator.ReleaseMemory(b.lastDequeuedBatchMemUsage)
			b.lastDequeuedBatchMemUsage = colmem.GetBatchMemSize(b.dequeueScratch)
			b.unlimitedAllocator.AdjustMemoryUsage(b.lastDequeuedBatchMemUsage)
			return b.dequeueScratch, tupleIdx
		}
		// The requested tuple must be located further into the disk queue.
		var ok bool
		b.numDequeued += b.dequeueScratch.Length()
		if ok, err = b.diskQueue.Dequeue(ctx, b.dequeueScratch); err != nil {
			colexecerror.InternalError(err)
		}
		if !ok || b.dequeueScratch.Length() == 0 {
			colexecerror.InternalError(
				errors.AssertionFailedf("index out of range for SpillingBuffer"))
		}
	}
}

// Length returns the number of tuples stored in the SpillingBuffer.
func (b *SpillingBuffer) Length() int {
	return b.length
}

// Close closes the SpillingBuffer.
func (b *SpillingBuffer) Close(ctx context.Context) error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.bufferedTuples = nil
	if b.diskQueue != nil {
		if err := b.diskQueue.Close(ctx); err != nil {
			return err
		}
		if b.fdSemaphore != nil {
			b.fdSemaphore.Release(numSpillingBufferFDs)
		}
		b.diskQueue = nil
	}
	return nil
}

// Reset resets the SpillingBuffer.
func (b *SpillingBuffer) Reset(ctx context.Context) {
	if err := b.Close(ctx); err != nil {
		colexecerror.InternalError(err)
	}
	b.closed = false
	b.unlimitedAllocator.ReleaseMemory(b.unlimitedAllocator.Used())
	// TODO(drewk): consider performing a shallow reset here to avoid losing the
	// references to allocated memory between resets.
	b.bufferedTuples = NewAppendOnlyBufferedBatch(b.unlimitedAllocator, b.typs, nil /* colsToStore */)
	b.scratch = b.unlimitedAllocator.NewMemBatchNoCols(b.typs, 0 /* capacity */)
	b.lastDequeuedBatchMemUsage = 0
	b.dequeueScratch = nil
	b.numDequeued = 0
	b.diskQueue = nil
	b.doneAppending = false
	b.length = 0
}
