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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewWindowAggregatorOperator creates a new Operator that computes aggregate
// window functions. outputColIdx specifies in which coldata.Vec the operator
// should put its output (if there is no such column, a new column is appended).
func NewWindowAggregatorOperator(
	args *WindowArgs,
	frame *execinfrapb.WindowerSpec_Frame,
	ordering *execinfrapb.Ordering,
	argIdxs []int,
	outputType *types.T,
	aggAlloc *colexecagg.AggregateFuncsAlloc,
	closers colexecop.Closers,
) colexecop.Operator {
	// Because the buffer is used multiple times per-row, it is important to
	// prevent it from spilling to disk if possible. For this reason, we give the
	// buffer half of the memory budget even though it will generally store less
	// columns than the queue.
	bufferMemLimit := int64(float64(args.MemoryLimit) * 0.5)
	framer := newWindowFramer(args.EvalCtx, frame, ordering, args.InputTypes, args.PeersColIdx)
	colsToStore := framer.getColsToStore(append([]int{}, argIdxs...))
	buffer := colexecutils.NewSpillingBuffer(
		args.BufferAllocator, bufferMemLimit, args.QueueCfg,
		args.FdSemaphore, args.InputTypes, args.DiskAcc, colsToStore...)
	inputIdxs := make([]uint32, len(argIdxs))
	for i := range inputIdxs {
		// We will always store the arg columns first in the buffer.
		inputIdxs[i] = uint32(i)
	}
	windower := &windowAggregator{
		partitionSeekerBase: partitionSeekerBase{
			partitionColIdx: args.PartitionColIdx,
			buffer:          buffer,
		},
		allocator:    args.MainAllocator,
		outputColIdx: args.OutputColIdx,
		inputIdxs:    inputIdxs,
		agg:          aggAlloc.MakeAggregateFuncs()[0],
		framer:       framer,
		closers:      closers,
		vecs:         make([]coldata.Vec, len(inputIdxs)),
	}
	return newBufferedWindowOperator(args, windower, outputType)
}

type windowAggregator struct {
	partitionSeekerBase
	colexecop.CloserHelper
	closers   colexecop.Closers
	allocator *colmem.Allocator

	outputColIdx int
	inputIdxs    []uint32
	vecs         []coldata.Vec

	agg    colexecagg.AggregateFunc
	framer windowFramer
}

var _ bufferedWindower = &windowAggregator{}

type windowInterval struct {
	start int
	end   int
}

// processBatch implements the bufferedWindower interface.
func (a *windowAggregator) processBatch(batch coldata.Batch, startIdx, endIdx int) {
	outVec := batch.ColVec(a.outputColIdx)
	a.agg.SetOutput(outVec)
	a.allocator.PerformOperation([]coldata.Vec{outVec}, func() {
		for i := startIdx; i < endIdx; i++ {
			a.framer.next(a.Ctx)
			for _, interval := range a.framer.frameIntervals() {
				// intervalIdx maintains the index up to which the current interval has
				// already been processed.
				intervalIdx := interval.start
				start, end := interval.start, interval.end
				intervalLen := interval.end - interval.start
				for intervalLen > 0 {
					for j, idx := range a.inputIdxs {
						a.vecs[j], start, end = a.buffer.GetVecWithTuple(a.Ctx, int(idx), intervalIdx)
					}
					if intervalLen < (end - start) {
						// This is the last batch in the current interval.
						end = start + intervalLen
					}
					intervalIdx += end - start
					intervalLen -= end - start
					a.agg.Compute(a.vecs, a.inputIdxs, start, end, nil /* sel */)
				}
			}
			a.agg.Flush(i)
			a.agg.Reset()
		}
	})
}

// transitionToProcessing implements the bufferedWindower interface.
func (a *windowAggregator) transitionToProcessing() {
	a.framer.startPartition(a.Ctx, a.partitionSize, a.buffer)
}

// startNewPartition implements the bufferedWindower interface.
func (a *windowAggregator) startNewPartition() {
	a.partitionSize = 0
	a.buffer.Reset(a.Ctx)
	a.agg.Reset()
}

// Init implements the bufferedWindower interface.
func (a *windowAggregator) Init(ctx context.Context) {
	if !a.InitHelper.Init(ctx) {
		return
	}
}

// Close implements the bufferedWindower interface.
func (a *windowAggregator) Close() {
	if !a.CloserHelper.Close() {
		return
	}
	if err := a.closers.Close(); err != nil {
		colexecerror.InternalError(err)
	}
	a.buffer.Close(a.EnsureCtx())
	a.agg.Reset()
	*a = windowAggregator{CloserHelper: a.CloserHelper}
}
