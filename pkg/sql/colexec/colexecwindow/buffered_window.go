// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// newBufferedWindowOperator creates a new Operator that computes the given
// window function.
func newBufferedWindowOperator(
	windower bufferedWindower,
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAcc *mon.BoundAccount,
	input colexecop.Operator,
	inputTypes []*types.T,
	outputColType *types.T,
	outputColIdx int,
) colexecop.Operator {
	outputTypes := make([]*types.T, len(inputTypes), len(inputTypes)+1)
	copy(outputTypes, inputTypes)
	outputTypes = append(outputTypes, outputColType)
	input = colexecutils.NewVectorTypeEnforcer(unlimitedAllocator, input, outputColType, outputColIdx)
	return &bufferedWindowOp{
		windowInitFields: windowInitFields{
			OneInputNode: colexecop.NewOneInputNode(input),
			allocator:    unlimitedAllocator,
			memoryLimit:  memoryLimit,
			diskQueueCfg: diskQueueCfg,
			fdSemaphore:  fdSemaphore,
			outputTypes:  outputTypes,
			diskAcc:      diskAcc,
			outputColIdx: outputColIdx,
		},
		windower: windower,
	}
}

type windowState int

// Transition graph for windowState:
//     ┌─────────────────┬─────────────┐
//     │                 │windowLoading├───────────────┐
//     │    ┌───────────►└──────┬──────┘               │
//     │    │                   │                      │
//     │    │                   │                      │
//     │    │                   │                      │
//     │    │                   │                      │
//     │    │                   │                      │
//     ▼    │                   ▼                      ▼
// ┌────────┴────┐◄─────┬────────────────┐      ┌──────────────┐
// │windowSeeking│      │windowProcessing├─────►│windowFinished│
// └─────────────┴─────►└──┬─────────────┘      └──────────────┘
//                         │        ▲
//                         │        │
//                         │        │
//                         └────────┘

const (
	// windowLoading is the state in which the window operators load an additional
	// input batch into the currentBatch field. If necessary, the old value of
	// currentBatch will be pushed to the buffer queue.
	//
	// windowLoading transitions to windowSeeking unless the end of the input has
	// been reached, in which case the next state is windowProcessing or
	// windowFinished depending on whether all rows have already been emitted.
	windowLoading windowState = iota
	// windowSeeking is the state in which the window operators seek to the index of
	// the next partition and perform function-specific pre-processing for the current
	// partition.
	//
	// windowSeeking transitions to windowLoading or windowProcessing, depending on
	// whether the end of the current partition has been found.
	windowSeeking
	// windowProcessing is the state in which the window operators fill in the
	// current partition for all loaded batches with window bucket values. Batches
	// are emitted as soon as the window output column has been filled.
	//
	// windowProcessing transitions to itself when a batch has been emitted but the
	// partition is not fully processed. It transitions to windowSeeking when a
	// partition has been fully processed. Finally, windowProcessing will
	// transition to windowFinished when all batches have been fully processed and
	// emitted.
	windowProcessing
	// windowFinished is the state in which the window operators close any
	// non-closed disk resources and emit the zero-length batch. windowFinished is
	// the terminal state.
	windowFinished
)

// bufferedWindower provides an interface for any window function that needs to
// buffer all tuples from each partition.
type bufferedWindower interface {
	Init(ctx context.Context)
	Close()

	// seekNextPartition is called during the windowSeeking state on the current
	// batch. It gives windowers a chance to perform any necessary pre-processing,
	// for example, getting the number of tuples in the partition.
	//
	// seekNextPartition is expected to return the index of the next partition
	// within the given batch, or the length of the batch if the next partition
	// does not begin within it.
	seekNextPartition(batch coldata.Batch, startIdx int, isPartitionStart bool) (nextPartitionIdx int)

	// processBatch is called during windowProcessing when a windower needs to
	// fill in the output column values in the given range for the given batch.
	processBatch(batch coldata.Batch, startIdx, endIdx int)

	// transitionToProcessing is called before bufferedWindowOp transitions from
	// the windowLoading or windowSeeking states to the windowProcessing state. It
	// gives windowers a chance to reset fields before filling the output column.
	transitionToProcessing()

	// startNewPartition is called before the windowSeeking state begins for a
	// partition. It gives windowers a chance to reset fields before processing of
	// a partition begins.
	startNewPartition()
}

// windowInitFields extracts common initializations for buffered window
// operators. Note that it is not an operator itself and should not be used
// directly.
type windowInitFields struct {
	colexecop.OneInputNode
	colexecop.InitHelper

	allocator    *colmem.Allocator
	memoryLimit  int64
	diskQueueCfg colcontainer.DiskQueueCfg
	fdSemaphore  semaphore.Semaphore
	outputTypes  []*types.T
	diskAcc      *mon.BoundAccount
	outputColIdx int
}

// bufferedWindowOp extracts common fields for the various window operators
// that need to fully buffer each partition before it can be processed. It
// buffers all batches for each partition, and calls functions on the
// bufferedWindower for each batch that allow the window output to be
// calculated.
type bufferedWindowOp struct {
	colexecop.CloserHelper
	windowInitFields

	// windower houses the fields and logic specific to the window function being
	// calculated.
	windower bufferedWindower

	// state is used to ensure that the window operators can continue processing
	// where they left off after emitting fully processed batches in streaming
	// fashion.
	state windowState

	// currentBatch is the batch that is currently being probed to determine the
	// size of the current partition.
	currentBatch coldata.Batch

	// bufferQueue stores batches that are waiting to be fully processed and
	// emitted. Note that since processed batches are emitted once the end of a
	// partition is reached, all batches in bufferQueue with the possible
	// exception of the head belong to the same partition.
	bufferQueue *colexecutils.SpillingQueue

	// nextPartitionIdx is the index of the start of the next partition within the
	// current batch. If the next partition does not start in the current batch,
	// nextPartitionIdx is set to the length of the current batch.
	nextPartitionIdx int

	// processingIdx is an index into all tuples currently buffered, beginning
	// with those in the queue and ending with those in currentBatch. It is used
	// to preserve state when the windowProcessing state spans more than one
	// batch.
	processingIdx int
}

func (b *bufferedWindowOp) Init(ctx context.Context) {
	if !b.InitHelper.Init(ctx) {
		return
	}
	b.Input.Init(b.Ctx)
	b.windower.Init(b.Ctx)
	b.state = windowLoading
	b.bufferQueue = colexecutils.NewSpillingQueue(
		&colexecutils.NewSpillingQueueArgs{
			UnlimitedAllocator: b.allocator,
			Types:              b.outputTypes,
			MemoryLimit:        b.memoryLimit,
			DiskQueueCfg:       b.diskQueueCfg,
			FDSemaphore:        b.fdSemaphore,
			DiskAcc:            b.diskAcc,
		},
	)
	b.windower.startNewPartition()
}

var _ colexecop.Operator = &bufferedWindowOp{}

func (b *bufferedWindowOp) Next() coldata.Batch {
	var err error
	for {
		switch b.state {
		case windowLoading:
			batch := b.Input.Next()
			if batch.Length() == 0 {
				// We have reached the end of the input.
				if !b.bufferQueue.Empty() || (b.currentBatch != nil && b.currentBatch.Length() > 0) {
					// There are still tuples that need to be processed.
					b.state = windowProcessing
					b.windower.transitionToProcessing()
					break
				}
				// The last batch (if any) was already emitted.
				b.state = windowFinished
				break
			}
			// Load the next batch into currentBatch. If currentBatch still has data,
			// move it into the queue.
			if b.currentBatch != nil && b.currentBatch.Length() > 0 {
				b.bufferQueue.Enqueue(b.Ctx, b.currentBatch)
			}
			// We have to copy the input batch data because calling Next on the input
			// may invalidate the contents of the last Batch returned by Next. Note
			// that the batch will be densely copied, so currentBatch will never have
			// a selection vector.
			n := batch.Length()
			sel := batch.Selection()
			b.currentBatch, _ = b.allocator.ResetMaybeReallocate(
				b.outputTypes, b.currentBatch, batch.Length(), math.MaxInt64)
			b.allocator.PerformOperation(b.currentBatch.ColVecs(), func() {
				for colIdx, vec := range batch.ColVecs() {
					if colIdx == b.outputColIdx {
						// There is no need to copy the uninitialized output column.
						continue
					}
					b.currentBatch.ColVec(colIdx).Copy(
						coldata.CopySliceArgs{
							SliceArgs: coldata.SliceArgs{
								Src:       vec,
								Sel:       sel,
								SrcEndIdx: n,
							},
						},
					)
				}
				b.currentBatch.SetLength(n)
			})
			b.state = windowSeeking
		case windowSeeking:
			isPartitionStart := b.bufferQueue.Empty()
			startIdx := 0
			if isPartitionStart {
				// We have transitioned to a new partition that starts within the
				// current batch at index nextPartitionIdx. Since nextPartitionIdx
				// hasn't been updated yet, it refers to the start of what is now the
				// current partition.
				startIdx = b.nextPartitionIdx
			}
			b.nextPartitionIdx = b.windower.seekNextPartition(b.currentBatch, startIdx, isPartitionStart)
			if b.nextPartitionIdx >= b.currentBatch.Length() {
				// The start of the next partition is not located in the current batch.
				b.state = windowLoading
				break
			}
			// The end of the current partition has been found and all pre-processing
			// completed, so the output values can now be calculated for this
			// partition.
			b.state = windowProcessing
			b.windower.transitionToProcessing()
		case windowProcessing:
			if !b.bufferQueue.Empty() {
				// The partition ends in the current batch, so all batches in the queue
				// can be processed and emitted.
				var output coldata.Batch
				if output, err = b.bufferQueue.Dequeue(b.Ctx); err != nil {
					colexecerror.InternalError(err)
				}
				// Set all the window output values that remain unset, then emit this
				// batch. Note that because the beginning of the next partition will
				// always be located in currentBatch, the current partition will always
				// end beyond any batch that is stored in the queue. Therefore we call
				// processBatch with the length of the batch as the end index.
				b.windower.processBatch(output, b.processingIdx, output.Length())
				b.processingIdx -= output.Length()
				if b.processingIdx < 0 {
					// processingIdx was located somewhere within this batch, meaning the
					// first of the tuples yet to be processed was somewhere in the batch.
					b.processingIdx = 0
				}
				// Although we didn't change the length of the batch, it is necessary to
				// set the length anyway (to maintain the invariant of flat bytes).
				output.SetLength(output.Length())
				return output
			}
			if b.currentBatch.Length() > 0 {
				b.windower.processBatch(b.currentBatch, b.processingIdx, b.nextPartitionIdx)
				if b.nextPartitionIdx >= b.currentBatch.Length() {
					// This was the last batch and it has been entirely filled. Although
					// we didn't change the length of the batch, it is necessary to set
					// the length anyway (to maintain the invariant of flat bytes).
					b.currentBatch.SetLength(b.currentBatch.Length())
					b.state = windowFinished
					return b.currentBatch
				}
				// The next partition begins within this batch. Set processingIdx to the
				// beginning of the next partition and seek to the end of the next
				// partition.
				b.processingIdx = b.nextPartitionIdx
				b.windower.startNewPartition()
				b.state = windowSeeking
				break
			}
			colexecerror.InternalError(
				errors.AssertionFailedf("window operator in processing state without buffered rows"))
		case windowFinished:
			if err = b.Close(); err != nil {
				colexecerror.InternalError(err)
			}
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(errors.AssertionFailedf("window operator in unhandled state"))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (b *bufferedWindowOp) Close() error {
	if !b.CloserHelper.Close() || b.Ctx == nil {
		// Either Close() has already been called or Init() was never called. In
		// both cases there is nothing to do.
		return nil
	}
	if err := b.bufferQueue.Close(b.EnsureCtx()); err != nil {
		return err
	}
	b.windower.Close()
	return nil
}
