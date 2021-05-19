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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/marusama/semaphore"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// SpillingBuffer wraps an AppendOnlyBufferedBatch to store tuples. It
// supports an append operation, as well as access to a windowed batch starting
// at a given index. Once the memory limit is reached, SpillingBuffer spills
// to a disk queue. Note that due to the use of a disk queue to support
// random access operations, SpillingBuffer can be slow for certain access
// patterns once it has spilled to disk.
type SpillingBuffer struct {
	maxMemoryLimit int64
	typs           []*types.T
	unlimitedAllocator      *colmem.Allocator

	// bufferedTuples is used to store tuples in-memory.
	bufferedTuples *AppendOnlyBufferedBatch

	// scratch is used to both load and output tuples. We do not allocate memory
	// for it because it only provides a window into either input or output data.
	scratch        coldata.Batch

	diskQueueCfg   colcontainer.DiskQueueCfg
	diskQueue      colcontainer.Queue
	fdSemaphore    semaphore.Semaphore
	diskAcc        *mon.BoundAccount

	dequeueScratch coldata.Batch
	numDequeued    int
	alreadySpilled bool

	closed bool
}

// NewSpillingBuffer creates a new SpillingBuffer.
//
// WARNING: when using SpillingBuffer all AppendTuples calls must occur
// before any GetWindowedBatch calls. This is due to a limitation of the
// rewindable disk queue that is used in the event of spilling to disk.
func NewSpillingBuffer(
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	typs []*types.T,
	diskAcc *mon.BoundAccount,
) *SpillingBuffer {
	// Memory used by the AppendOnlyBufferedBatch cannot be partially released
	// (and we would like to keep as many tuples in-memory as possible), so we
	// must reserve memory for the disk queue and dequeue scratch batch.
	maxMemoryLimit := memoryLimit - int64(colmem.EstimateBatchSizeBytes(typs, coldata.BatchSize()))
	maxMemoryLimit -= int64(diskQueueCfg.BufferSizeBytes)
	return &SpillingBuffer{
		typs: typs,
		unlimitedAllocator: unlimitedAllocator,
		maxMemoryLimit: maxMemoryLimit,
		bufferedTuples: NewAppendOnlyBufferedBatch(unlimitedAllocator, typs, nil /*colsToStore */),
		scratch: unlimitedAllocator.NewMemBatchNoCols(typs, 0 /* capacity */),
		diskQueueCfg: diskQueueCfg,
		fdSemaphore: fdSemaphore,
		diskAcc: diskAcc,
	}
}

// AppendTuples adds the columns from the given batch signified by colIdxs to
// the randomAccessBuffer. It expects that the given batch does not use a
// selection vector, and that each index in colIdxs corresponds to a type in
// b.typs (and vice-versa). If colIdxs is nil, AppendTuples appends to each
// column the one from batch in the same ordinal position.
func (b *SpillingBuffer) AppendTuples(
	ctx context.Context, batch coldata.Batch, startIdx, endIdx int, colIdxs ...int,
) {
	var err error
	if batch.Selection() != nil {
		colexecerror.InternalError(errors.AssertionFailedf("expected no selection vector"))
	}
	if colIdxs == nil {
		colIdxs = make([]int, len(b.typs))
		for i := range colIdxs {
			colIdxs[i] = i
		}
	}
	if !b.alreadySpilled {
		// length is the number of tuples to be copied into the
		// AppendOnlyBufferedBatch. We want to make an effort to fill it with
		// tuples, since disk access is slow.
		length := endIdx - startIdx
		var additionalUsedMem int64
		for {
			// TODO(drewk): we could make a better effort here to fit as many tuples
			//  as possible in-memory - maybe implement the inverse of
			//  EstimateBatchSizeBytes to get the maximum allowed length?
			additionalUsedMem = int64(colmem.EstimateBatchSizeBytes(b.typs, length))
			if length <= 0 || b.unlimitedAllocator.Used() + additionalUsedMem < b.maxMemoryLimit {
				break
			}
			length /= 2
		}
		if length > 0 {
			// There is room to append at least some of the tuples to the in-memory
			// storage.
			for i, idx := range colIdxs {
				window := batch.ColVec(idx).Window(startIdx, startIdx + length)
				b.scratch.ReplaceCol(window, i)
			}
			b.scratch.SetLength(length)
			b.bufferedTuples.AppendTuples(b.scratch, 0 /* startIdx */, length)
			b.unlimitedAllocator.AdjustMemoryUsage(additionalUsedMem)
		}
		startIdx += length
	}
	if startIdx < endIdx {
		// Not all tuples could be stored in-memory; they will have to be placed in
		// the queue.
		b.alreadySpilled = true
		if b.diskQueue == nil {
			if b.fdSemaphore != nil {
				if err = b.fdSemaphore.Acquire(ctx, b.numFDsOpenAtAnyGivenTime()); err != nil {
					colexecerror.InternalError(err)
				}
			}
			if b.diskQueue, err =
				colcontainer.NewRewindableDiskQueue(ctx, b.typs, b.diskQueueCfg, b.diskAcc); err != nil {
				colexecerror.InternalError(err)
			}
			log.VEvent(ctx, 1, "spilled to disk")
		}
		for i, idx := range colIdxs {
			window := batch.ColVec(idx).Window(startIdx, endIdx)
			b.scratch.ReplaceCol(window, i)
		}
		b.scratch.SetLength(endIdx - startIdx)
		if err = b.diskQueue.Enqueue(ctx, b.scratch); err != nil {
			colexecerror.InternalError(err)
		}
	}
}

// GetWindowedBatch returns a windowed batch that starts with the tuple at the
// given index into all queued tuples. The batch is not allowed to be modified,
// or the data stored in SpillingBuffer may be corrupted. No guarantees are
// made about the size of the batch beyond that it will contain the requested
// tuple at index zero (it may contain tuples that follow the requested tuple).
// This is useful for callers that wish to iterate through adjacent tuples.
//
// getTuples optimizes for the case when subsequent calls access tuples from the
// same batch, and to a lesser degree when tuples from a subsequent batch are
// accessed. It assumes that startIdx is within the range: [0, num_tuples); it
// is up to the caller to ensure that this is the case.
func (b *SpillingBuffer) GetWindowedBatch(ctx context.Context, idx int) coldata.Batch {
	var err error
	if idx < b.bufferedTuples.Length() {
		// The requested tuple is stored in-memory.
		endIdx := b.bufferedTuples.Length()
		for i := range b.typs {
			window := b.bufferedTuples.ColVec(i).Window(idx, endIdx)
			b.scratch.ReplaceCol(window, i)
		}
		b.scratch.SetLength(endIdx - idx)
		return b.scratch
	}

	// The requested tuple is stored on-disk. It will have to be retrieved from
	// the rewindable queue. Normalize the index to refer to a location within the
	// set of all enqueued tuples rather than all buffered tuples.
	idx -= b.bufferedTuples.Length()
	if idx < b.numDequeued {
		// The idx'th tuple is located before the current head of the queue, so we
		// need to rewind. TODO(drewk): look for a more efficient way to handle
		// spilling.
		if err = b.diskQueue.(colcontainer.RewindableQueue).Rewind(); err != nil {
			colexecerror.InternalError(err)
		}
		b.numDequeued = 0
		b.dequeueScratch = nil
	}
	for {
		// Dequeue batches until we reach the one with the idx'th tuple.
		if b.dequeueScratch == nil {
			// We have already set aside enough memory capacity for the dequeueScratch
			// batch to have the maximum batch size, so the allocation will not exceed
			// the memory limit. Unregister the memory usage estimate so it doesn't
			// carry over between calls to Reset().
			b.dequeueScratch = b.unlimitedAllocator.NewMemBatchWithFixedCapacity(b.typs, coldata.BatchSize())
			b.unlimitedAllocator.ReleaseMemory(colmem.GetBatchMemSize(b.dequeueScratch))
		}
		if b.dequeueScratch.Length() == 0 {
			if _, err = b.diskQueue.Dequeue(ctx, b.dequeueScratch); err != nil {
				colexecerror.InternalError(err)
			}
			if b.dequeueScratch.Length() == 0 {
				colexecerror.InternalError(
					errors.AssertionFailedf("index out of range for SpillingBuffer"))
			}
		}
		if idx-b.numDequeued < b.dequeueScratch.Length() {
			// The requested tuple is within the dequeued batch. Return a window into
			// the dequeued batch starting from the given index.
			startIdx := idx - b.numDequeued
			endIdx := b.dequeueScratch.Length()
			for i := range b.typs {
				window := b.dequeueScratch.ColVec(i).Window(startIdx, endIdx)
				b.scratch.ReplaceCol(window, i)
			}
			b.scratch.SetLength(endIdx - startIdx)
			return b.scratch
		}
		b.numDequeued += b.dequeueScratch.Length()
		if _, err = b.diskQueue.Dequeue(ctx, b.dequeueScratch); err != nil {
			colexecerror.InternalError(err)
		}
		if b.dequeueScratch.Length() == 0 {
			colexecerror.InternalError(
				errors.AssertionFailedf("index out of range for SpillingBuffer"))
		}
	}
}

func (b *SpillingBuffer) numFDsOpenAtAnyGivenTime() int {
	if b.diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		// The access pattern must be write-everything then read-everything so
		// either a read FD or a write FD are open at any one point.
		return 1
	}
	// Otherwise, both will be open.
	return 2
}

func (b *SpillingBuffer) Close(ctx context.Context) error {
	if b.closed {
		return nil
	}
	if b.diskQueue != nil {
		if err := b.diskQueue.Close(ctx); err != nil {
			return err
		}
		if b.fdSemaphore != nil {
			b.fdSemaphore.Release(b.numFDsOpenAtAnyGivenTime())
		}
		b.closed = true
	}
	return nil
}

// Reset resets the SpillingBuffer.
func (b *SpillingBuffer) Reset(ctx context.Context) {
	if err := b.Close(ctx); err != nil {
		colexecerror.InternalError(err)
	}
	b.bufferedTuples.SetLength(0)
	b.scratch.SetLength(0)
	b.numDequeued = 0
	b.diskQueue = nil
	b.closed = false
	b.dequeueScratch = nil
}
