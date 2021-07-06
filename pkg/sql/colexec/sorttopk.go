// Copyright 2019 The Cockroach Authors.
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
	"container/heap"
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const (
	topKVecIdx  = 0
	inputVecIdx = 1
)

// NewTopKSorter returns a new sort operator, which sorts its input on the
// columns given in orderingCols and returns the first K rows. The inputTypes
// must correspond 1-1 with the columns in the input operator.
func NewTopKSorter(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	inputTypes []*types.T,
	orderingCols []execinfrapb.Ordering_Column,
	k uint64,
) colexecop.ResettableOperator {
	return &topKSorter{
		allocator:    allocator,
		OneInputNode: colexecop.NewOneInputNode(input),
		inputTypes:   inputTypes,
		orderingCols: orderingCols,
		k:            k,
	}
}

var _ colexecop.BufferingInMemoryOperator = &topKSorter{}
var _ colexecop.Resetter = &topKSorter{}

// topKSortState represents the state of the sort operator.
type topKSortState int

const (
	// topKSortSpooling is the initial state of the operator, where it spools
	// its input.
	topKSortSpooling topKSortState = iota
	// topKSortEmitting is the second state of the operator, indicating that
	// each call to Next will return another batch of the sorted data.
	topKSortEmitting
	// topKSortDone is the final state of the operator, where it always returns
	// a zero batch.
	topKSortDone
)

type topKSorter struct {
	colexecop.OneInputNode
	colexecop.InitHelper

	allocator    *colmem.Allocator
	orderingCols []execinfrapb.Ordering_Column
	inputTypes   []*types.T
	k            uint64

	// state is the current state of the sort.
	state topKSortState
	// inputBatch is the last read batch from the input.
	inputBatch coldata.Batch
	// firstUnprocessedTupleIdx indicates the index of the first tuple in
	// inputBatch that hasn't been processed yet.
	firstUnprocessedTupleIdx int
	// comparators stores one comparator per ordering column.
	comparators []vecComparator
	// topK stores the top K rows. It is not sorted internally.
	topK *colexecutils.AppendOnlyBufferedBatch
	// heap is a max heap which stores indices into topK.
	heap []int
	// sel is a selection vector which specifies an ordering on topK.
	sel []int
	// emitted is the count of rows which have been emitted so far.
	emitted int
	output  coldata.Batch

	exportedFromTopK  int
	exportedFromBatch int
	windowedBatch     coldata.Batch
}

func (t *topKSorter) Init(ctx context.Context) {
	if !t.InitHelper.Init(ctx) {
		return
	}
	t.Input.Init(t.Ctx)
	t.topK = colexecutils.NewAppendOnlyBufferedBatch(t.allocator, t.inputTypes, nil /* colsToStore */)
	t.comparators = make([]vecComparator, len(t.inputTypes))
	for i, typ := range t.inputTypes {
		t.comparators[i] = GetVecComparator(typ, 2)
	}
	// TODO(yuzefovich): switch to calling this method on allocator. This will
	// require plumbing unlimited allocator to work correctly in tests with
	// memory limit of 1.
	t.windowedBatch = coldata.NewMemBatchNoCols(t.inputTypes, coldata.BatchSize())
}

func (t *topKSorter) Next() coldata.Batch {
	for {
		switch t.state {
		case topKSortSpooling:
			t.spool()
			t.state = topKSortEmitting
		case topKSortEmitting:
			output := t.emit()
			if output.Length() == 0 {
				t.state = topKSortDone
				continue
			}
			return output
		case topKSortDone:
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(errors.AssertionFailedf("invalid sort state %v", t.state))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (t *topKSorter) Reset(ctx context.Context) {
	if r, ok := t.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	t.state = topKSortSpooling
	t.firstUnprocessedTupleIdx = 0
	t.topK.ResetInternalBatch()
	t.emitted = 0
}

// spool reads in the entire input, always storing the top K rows it has seen so
// far in o.topK. This is done by maintaining a max heap of indices into o.topK.
// Whenever we encounter a row which is smaller than the max row in the heap,
// we replace the max with that row.
//
// After all the input has been read, we pop everything off the heap to
// determine the final output ordering. This is used in emit() to output the rows
// in sorted order.
func (t *topKSorter) spool() {
	// Fill up t.topK by spooling up to K rows from the input.
	t.inputBatch = t.Input.Next()
	remainingRows := t.k
	for remainingRows > 0 && t.inputBatch.Length() > 0 {
		fromLength := t.inputBatch.Length()
		if remainingRows < uint64(t.inputBatch.Length()) {
			// t.topK will be full after this batch.
			fromLength = int(remainingRows)
		}
		t.firstUnprocessedTupleIdx = fromLength
		t.allocator.PerformAppend(t.topK, func() {
			t.topK.AppendTuples(t.inputBatch, 0 /* startIdx */, fromLength)
		})
		remainingRows -= uint64(fromLength)
		if fromLength == t.inputBatch.Length() {
			t.inputBatch = t.Input.Next()
			t.firstUnprocessedTupleIdx = 0
		}
	}
	t.updateComparators(topKVecIdx, t.topK)

	// Initialize the heap.
	if cap(t.heap) < t.topK.Length() {
		t.heap = make([]int, t.topK.Length())
	} else {
		t.heap = t.heap[:t.topK.Length()]
	}
	for i := range t.heap {
		t.heap[i] = i
	}
	heap.Init(t)

	// Read the remainder of the input. Whenever a row is less than the heap max,
	// swap it in.
	for t.inputBatch.Length() > 0 {
		t.updateComparators(inputVecIdx, t.inputBatch)
		sel := t.inputBatch.Selection()
		t.allocator.PerformOperation(
			t.topK.ColVecs(),
			func() {
				for i := t.firstUnprocessedTupleIdx; i < t.inputBatch.Length(); i++ {
					idx := i
					if sel != nil {
						idx = sel[i]
					}
					maxIdx := t.heap[0]
					if t.compareRow(inputVecIdx, topKVecIdx, idx, maxIdx) < 0 {
						for j := range t.inputTypes {
							t.comparators[j].set(inputVecIdx, topKVecIdx, idx, maxIdx)
						}
						heap.Fix(t, 0)
					}
				}
				t.firstUnprocessedTupleIdx = t.inputBatch.Length()
			},
		)
		t.inputBatch = t.Input.Next()
		t.firstUnprocessedTupleIdx = 0
	}

	// t.topK now contains the top K rows unsorted. Create a selection vector
	// which specifies the rows in sorted order by popping everything off the
	// heap. Note that it's a max heap so we need to fill the selection vector in
	// reverse.
	t.sel = make([]int, t.topK.Length())
	for i := 0; i < t.topK.Length(); i++ {
		t.sel[len(t.sel)-i-1] = heap.Pop(t).(int)
	}
}

func (t *topKSorter) emit() coldata.Batch {
	toEmit := t.topK.Length() - t.emitted
	if toEmit == 0 {
		// We're done.
		return coldata.ZeroBatch
	}
	if toEmit > coldata.BatchSize() {
		toEmit = coldata.BatchSize()
	}
	// For now, we don't enforce any footprint-based memory limit.
	// TODO(yuzefovich): refactor this.
	const maxBatchMemSize = math.MaxInt64
	t.output, _ = t.allocator.ResetMaybeReallocate(t.inputTypes, t.output, toEmit, maxBatchMemSize)
	for i := range t.inputTypes {
		vec := t.output.ColVec(i)
		// At this point, we have already fully sorted the input. It is ok to do
		// this Copy outside of the allocator - the work has been done, but
		// theoretically it is possible to hit the limit here (mainly with
		// variable-sized types like Bytes). Nonetheless, for performance reasons
		// it would be sad to fallback to disk at this point.
		vec.Copy(
			coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					Src:         t.topK.ColVec(i),
					Sel:         t.sel,
					SrcStartIdx: t.emitted,
					SrcEndIdx:   t.emitted + toEmit,
				},
			},
		)
	}
	t.output.SetLength(toEmit)
	t.emitted += toEmit
	return t.output
}

func (t *topKSorter) compareRow(vecIdx1, vecIdx2 int, rowIdx1, rowIdx2 int) int {
	for i := range t.orderingCols {
		info := t.orderingCols[i]
		res := t.comparators[info.ColIdx].compare(vecIdx1, vecIdx2, rowIdx1, rowIdx2)
		if res != 0 {
			switch d := info.Direction; d {
			case execinfrapb.Ordering_Column_ASC:
				return res
			case execinfrapb.Ordering_Column_DESC:
				return -res
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unexpected direction value %d", d))
			}
		}
	}
	return 0
}

func (t *topKSorter) updateComparators(vecIdx int, batch coldata.Batch) {
	for i := range t.inputTypes {
		t.comparators[i].setVec(vecIdx, batch.ColVec(i))
	}
}

func (t *topKSorter) ExportBuffered(colexecop.Operator) coldata.Batch {
	topKLen := t.topK.Length()
	// First, we check whether we have exported all tuples from the topK vector.
	if t.exportedFromTopK < topKLen {
		newExportedFromTopK := t.exportedFromTopK + coldata.BatchSize()
		if newExportedFromTopK > topKLen {
			newExportedFromTopK = topKLen
		}
		for i := range t.inputTypes {
			window := t.topK.ColVec(i).Window(t.exportedFromTopK, newExportedFromTopK)
			t.windowedBatch.ReplaceCol(window, i)
		}
		t.windowedBatch.SetSelection(false)
		t.windowedBatch.SetLength(newExportedFromTopK - t.exportedFromTopK)
		t.exportedFromTopK = newExportedFromTopK
		return t.windowedBatch
	}
	// Next, we check whether we have exported all tuples from the last read
	// batch.
	if t.inputBatch != nil && t.firstUnprocessedTupleIdx+t.exportedFromBatch < t.inputBatch.Length() {
		colexecutils.MakeWindowIntoBatch(t.windowedBatch, t.inputBatch, t.firstUnprocessedTupleIdx, t.inputTypes)
		t.exportedFromBatch = t.windowedBatch.Length()
		return t.windowedBatch
	}
	return coldata.ZeroBatch
}

// Len is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Len() int {
	return len(t.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Less(i, j int) bool {
	return t.compareRow(topKVecIdx, topKVecIdx, t.heap[i], t.heap[j]) > 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Swap(i, j int) {
	t.heap[i], t.heap[j] = t.heap[j], t.heap[i]
}

// Push is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Push(x interface{}) {
	t.heap = append(t.heap, x.(int))
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Pop() interface{} {
	x := t.heap[len(t.heap)-1]
	t.heap = t.heap[:len(t.heap)-1]
	return x
}
