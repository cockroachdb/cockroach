// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for min_max_removable_agg.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ tree.AggType
	_ apd.Context
	_ duration.Duration
	_ json.JSON
	_ = coldataext.CompareDatum
	_ = colexecerror.InternalError
	_ = memsize.Uint32
)

// {{/*
// Declarations to make the template compile properly.

// _ASSIGN_CMP is the template function for assigning true to the first input
// if the second input compares successfully to the third input. The comparison
// operator is tree.LT for MIN and is tree.GT for MAX.
func _ASSIGN_CMP(_, _, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

const (
	// The argument column is always the first column in the SpillingBuffer.
	argColIdx = 0

	// The slice of uint32s in the deque can have up to 10,000 values (40KB).
	maxQueueLength = 10000
)

type minMaxRemovableAggBase struct {
	partitionSeekerBase
	colexecop.CloserHelper
	allocator    *colmem.Allocator
	outputColIdx int
	framer       windowFramer

	// A partial deque of indices into the current partition ordered by the value
	// of the input column at each index. It contains only indices that are part
	// of the current window frame. The first value in the queue is the index of
	// the current value for the aggregation (NULL if empty). Under the
	// simplifying assumption that the window frame has no exclusion clause, the
	// queue does not need to contain any indices smaller than the best index -
	// this keeps the queue small in many common cases.
	queue minMaxQueue

	// omittedIndex tracks the index where we reached the limit of the length of
	// the queue, in which case we may be omitting values that could become
	// relevant as the frame shrinks. If the queue becomes empty while this
	// index is set, we have to aggregate over the previously omitted values.
	// The default (unset) value is -1.
	omittedIndex int

	scratchIntervals []windowInterval
}

// Init implements the bufferedWindower interface.
func (b *minMaxRemovableAggBase) Init(ctx context.Context) {
	b.InitHelper.Init(ctx)
}

// transitionToProcessing implements the bufferedWindower interface.
func (b *minMaxRemovableAggBase) transitionToProcessing() {
	b.framer.startPartition(b.Ctx, b.partitionSize, b.buffer)
}

// startNewPartition implements the bufferedWindower interface.
func (b *minMaxRemovableAggBase) startNewPartition() {
	b.partitionSize = 0
	b.buffer.Reset(b.Ctx)
	b.queue.reset()
}

// {{range .}}
// {{$agg := .Agg}}

func new_AGG_TITLERemovableAggregator(
	args *WindowArgs, framer windowFramer, buffer *colexecutils.SpillingBuffer, argTyp *types.T,
) bufferedWindower {
	// Reserve the maximum memory usable by the queue up front to ensure that it
	// isn't used by the SpillingBuffer.
	args.BufferAllocator.AdjustMemoryUsage(maxQueueLength * memsize.Uint32)
	base := minMaxRemovableAggBase{
		partitionSeekerBase: partitionSeekerBase{
			partitionColIdx: args.PartitionColIdx,
			buffer:          buffer,
		},
		allocator:    args.MainAllocator,
		outputColIdx: args.OutputColIdx,
		framer:       framer,
		queue:        newMinMaxQueue(maxQueueLength),
		omittedIndex: -1,
	}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(argTyp.Family()) {
	// {{range .Overloads}}
	case _CANONICAL_TYPE_FAMILY:
		switch argTyp.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &_AGG_TYPEAggregator{minMaxRemovableAggBase: base}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(
		errors.AssertionFailedf("unexpectedly didn't find _AGG overload for %s type family", argTyp.Name()))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// {{range .Overloads}}
// {{range .WidthOverloads}}

type _AGG_TYPEAggregator struct {
	minMaxRemovableAggBase
	// curAgg holds the running min/max, so we can index into the output column
	// once per row, instead of on each iteration.
	// NOTE: if the length of the queue is zero, curAgg is undefined.
	curAgg _GOTYPE
}

// processBatch implements the bufferedWindower interface.
func (a *_AGG_TYPEAggregator) processBatch(batch coldata.Batch, startIdx, endIdx int) {
	if endIdx <= startIdx {
		// There is no processing to be done.
		return
	}
	outVec := batch.ColVec(a.outputColIdx)
	outNulls := outVec.Nulls()
	outCol := outVec.TemplateType()
	// {{if not .IsBytesLike}}
	_, _ = outCol.Get(startIdx), outCol.Get(endIdx-1)
	// {{end}}
	a.allocator.PerformOperation([]coldata.Vec{outVec}, func() {
		for i := startIdx; i < endIdx; i++ {
			a.framer.next(a.Ctx)
			toAdd, toRemove := a.framer.slidingWindowIntervals()

			// Process the toRemove intervals first.
			if !a.queue.isEmpty() {
				prevBestIdx := a.queue.getFirst()
				for _, interval := range toRemove {
					if uint32(interval.start) > a.queue.getFirst() {
						colexecerror.InternalError(errors.AssertionFailedf(
							"expected default exclusion clause for min/max sliding window operator"))
					}
					a.queue.removeAllBefore(uint32(interval.end))
				}
				if !a.queue.isEmpty() {
					newBestIdx := a.queue.getFirst()
					if newBestIdx != prevBestIdx {
						// We need to update curAgg.
						vec, idx, _ := a.buffer.GetVecWithTuple(a.Ctx, argColIdx, int(newBestIdx))
						col := vec.TemplateType()
						val := col.Get(idx)
						execgen.COPYVAL(a.curAgg, val)
					}
				}
			}

			// Now aggregate over the toAdd intervals.
			if a.queue.isEmpty() && a.omittedIndex != -1 {
				// We have exhausted all the values that fit in the queue - we need to
				// re-aggregate over the current window frame starting from the first
				// omitted index.
				a.scratchIntervals = getIntervalsGEIdx(
					a.framer.frameIntervals(), a.scratchIntervals, a.omittedIndex)
				a.omittedIndex = -1
				a.aggregateOverIntervals(a.scratchIntervals)
			} else {
				a.aggregateOverIntervals(toAdd)
			}

			// Set the output value for the current row.
			if a.queue.isEmpty() {
				outNulls.SetNull(i)
			} else {
				// {{if not .IsBytesLike}}
				// gcassert:bce
				// {{end}}
				outCol.Set(i, a.curAgg)
			}
		}
	})
}

// aggregateOverIntervals accumulates all rows represented by the given
// intervals into the current aggregate.
func (a *_AGG_TYPEAggregator) aggregateOverIntervals(intervals []windowInterval) {
	for _, interval := range intervals {
		var cmp bool
		for j := interval.start; j < interval.end; j++ {
			idxToAdd := uint32(j)
			vec, idx, _ := a.buffer.GetVecWithTuple(a.Ctx, argColIdx, j)
			nulls := vec.Nulls()
			col := vec.TemplateType()
			if !nulls.MaybeHasNulls() || !nulls.NullAt(idx) {
				val := col.Get(idx)

				// If this is the first value in the frame, it is the best so far.
				isBest := a.queue.isEmpty()
				if !a.queue.isEmpty() {
					// Compare to the best value seen so far.
					_ASSIGN_CMP(cmp, val, a.curAgg, _, col, _)
					if cmp {
						// Reset the queue because the current value replaces all others.
						isBest = true
						a.queue.reset()
					}
					isBest = cmp
				}
				if isBest {
					// The queue is already empty, so just add to the end of the queue.
					// If any values were omitted from the queue, they would be dominated
					// by this one anyway, so reset omittedIndex.
					a.queue.addLast(idxToAdd)
					execgen.COPYVAL(a.curAgg, val)
					a.omittedIndex = -1
					continue
				}

				// This is not the best value in the window frame, but we still need to
				// keep it in the queue. Iterate from the end of the queue, removing any
				// values that are dominated by the current one. Add the current value
				// once the last value in the queue is better than the current one.
				if !a.queue.isEmpty() {
					// We have to make a copy of val because GetVecWithTuple
					// calls below might reuse the same underlying vector.
					var valCopy _GOTYPE
					execgen.COPYVAL(valCopy, val)
					for !a.queue.isEmpty() {
						cmpVec, cmpIdx, _ := a.buffer.GetVecWithTuple(a.Ctx, argColIdx, int(a.queue.getLast()))
						cmpVal := cmpVec.TemplateType().Get(cmpIdx)
						_ASSIGN_CMP(cmp, cmpVal, valCopy, _, col, _)
						if cmp {
							break
						}
						// Any values that could not fit in the queue would also have been
						// dominated by the current one, so reset omittedIndex.
						a.queue.removeLast()
						a.omittedIndex = -1
					}
				}
				if a.queue.addLast(idxToAdd) && a.omittedIndex == -1 {
					// The value couldn't fit in the queue. Keep track of the first index
					// from which the queue could no longer store values.
					a.omittedIndex = j
				}
			}
		}
	}
}

func (a *_AGG_TYPEAggregator) Close(ctx context.Context) {
	a.queue.close()
	a.framer.close()
	a.buffer.Close(ctx)
	*a = _AGG_TYPEAggregator{}
}

// {{end}}
// {{end}}
// {{end}}

// getIntervalsGEIdx returns a set of intervals representing all indexes in the
// 'intervals' slice at or after the given index.
func getIntervalsGEIdx(intervals, scratch []windowInterval, idx int) []windowInterval {
	scratch = scratch[:0]
	for _, interval := range intervals {
		if interval.end <= idx {
			continue
		}
		if interval.start >= idx {
			scratch = append(scratch, interval)
			continue
		}
		scratch = append(scratch, windowInterval{start: idx, end: interval.end})
	}
	return scratch
}
