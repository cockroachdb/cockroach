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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewCountRowsOperator creates a new Operator that computes the count_rows
// aggregate window function.
func NewCountRowsOperator(
	args *WindowArgs, frame *execinfrapb.WindowerSpec_Frame, ordering *execinfrapb.Ordering,
) colexecop.Operator {
	// Because the buffer is potentially used multiple times per-row, it is
	// important to prevent it from spilling to disk if possible. For this reason,
	// we give the buffer half of the memory budget even though it will generally
	// store less columns than the queue.
	bufferMemLimit := int64(float64(args.MemoryLimit) * 0.5)
	mainMemLimit := args.MemoryLimit - bufferMemLimit
	framer := newWindowFramer(args.EvalCtx, frame, ordering, args.InputTypes, args.PeersColIdx)
	colsToStore := framer.getColsToStore(nil /* oldColsToStore */)
	buffer := colexecutils.NewSpillingBuffer(
		args.BufferAllocator, bufferMemLimit, args.QueueCfg,
		args.FdSemaphore, args.InputTypes, args.DiskAcc, colsToStore...)
	windower := &countRowsWindowAggregator{
		partitionSeekerBase: partitionSeekerBase{
			partitionColIdx: args.PartitionColIdx,
			buffer:          buffer,
		},
		allocator:    args.MainAllocator,
		outputColIdx: args.OutputColIdx,
		framer:       framer,
	}
	return newBufferedWindowOperator(args, windower, types.Int, mainMemLimit)
}

type countRowsWindowAggregator struct {
	partitionSeekerBase
	colexecop.CloserHelper
	allocator    *colmem.Allocator
	outputColIdx int
	framer       windowFramer
}

var _ bufferedWindower = &countRowsWindowAggregator{}

// transitionToProcessing implements the bufferedWindower interface.
func (a *countRowsWindowAggregator) transitionToProcessing() {
	a.framer.startPartition(a.Ctx, a.partitionSize, a.buffer)
}

// startNewPartition implements the bufferedWindower interface.
func (a *countRowsWindowAggregator) startNewPartition() {
	a.partitionSize = 0
	a.buffer.Reset(a.Ctx)
}

// Init implements the bufferedWindower interface.
func (a *countRowsWindowAggregator) Init(ctx context.Context) {
	a.InitHelper.Init(ctx)
}

// Close implements the bufferedWindower interface.
func (a *countRowsWindowAggregator) Close(ctx context.Context) {
	if !a.CloserHelper.Close() {
		return
	}
	a.framer.close()
	a.buffer.Close(ctx)
}

// processBatch implements the bufferedWindower interface.
func (a *countRowsWindowAggregator) processBatch(batch coldata.Batch, startIdx, endIdx int) {
	if startIdx >= endIdx {
		// No processing needs to be performed.
		return
	}
	outVec := batch.ColVec(a.outputColIdx)
	a.allocator.PerformOperation([]coldata.Vec{outVec}, func() {
		outCol := outVec.Int64()
		_, _ = outCol[startIdx], outCol[endIdx-1]
		for i := startIdx; i < endIdx; i++ {
			var cnt int
			a.framer.next(a.Ctx)
			for _, interval := range a.framer.frameIntervals() {
				cnt += interval.end - interval.start
			}
			//gcassert:bce
			outCol[i] = int64(cnt)
		}
	})
}
