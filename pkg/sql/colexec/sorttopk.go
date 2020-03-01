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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

const (
	topKVecIdx  = 0
	inputVecIdx = 1
)

// NewTopKSorter returns a new sort operator, which sorts its input on the
// columns given in orderingCols and returns the first K rows. The inputTypes
// must correspond 1-1 with the columns in the input operator.
func NewTopKSorter(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	orderingCols []execinfrapb.Ordering_Column,
	k uint16,
) Operator {
	return &topKSorter{
		allocator:    allocator,
		OneInputNode: NewOneInputNode(input),
		inputTypes:   inputTypes,
		orderingCols: orderingCols,
		k:            k,
	}
}

var _ bufferingInMemoryOperator = &topKSorter{}

// topKSortState represents the state of the sort operator.
type topKSortState int

const (
	// sortSpooling is the initial state of the operator, where it spools its
	// input.
	topKSortSpooling topKSortState = iota
	// sortEmitting is the second state of the operator, indicating that each call
	// to Next will return another batch of the sorted data.
	topKSortEmitting
)

type topKSorter struct {
	OneInputNode

	allocator    *Allocator
	orderingCols []execinfrapb.Ordering_Column
	inputTypes   []coltypes.T
	k            uint16 // TODO(solon): support larger k values

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
	topK coldata.Batch
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

func (t *topKSorter) Init() {
	t.input.Init()
	t.topK = t.allocator.NewMemBatchWithSize(t.inputTypes, 0 /* size */)
	t.comparators = make([]vecComparator, len(t.inputTypes))
	for i := range t.inputTypes {
		typ := t.inputTypes[i]
		t.comparators[i] = GetVecComparator(typ, 2)
	}
	// TODO(yuzefovich): switch to calling this method on allocator. This will
	// require plumbing unlimited allocator to work correctly in tests with
	// memory limit of 1.
	t.windowedBatch = coldata.NewMemBatchNoCols(t.inputTypes, coldata.BatchSize())
}

func (t *topKSorter) Next(ctx context.Context) coldata.Batch {
	switch t.state {
	case topKSortSpooling:
		t.spool(ctx)
		t.state = topKSortEmitting
		fallthrough
	case topKSortEmitting:
		return t.emit()
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid sort state %v", t.state))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// spool reads in the entire input, always storing the top K rows it has seen so
// far in o.topK. This is done by maintaining a max heap of indices into o.topK.
// Whenever we encounter a row which is smaller than the max row in the heap,
// we replace the max with that row.
//
// After all the input has been read, we pop everything off the heap to
// determine the final output ordering. This is used in emit() to output the rows
// in sorted order.
func (t *topKSorter) spool(ctx context.Context) {
	// Fill up t.topK by spooling up to K rows from the input.
	t.inputBatch = t.input.Next(ctx)
	spooledRows := 0
	remainingRows := int(t.k)
	for remainingRows > 0 && t.inputBatch.Length() > 0 {
		toLength := spooledRows
		fromLength := t.inputBatch.Length()
		if remainingRows < t.inputBatch.Length() {
			// t.topK will be full after this batch.
			fromLength = remainingRows
		}
		t.firstUnprocessedTupleIdx = fromLength
		t.allocator.PerformOperation(t.topK.ColVecs(), func() {
			for i := range t.inputTypes {
				destVec := t.topK.ColVec(i)
				vec := t.inputBatch.ColVec(i)
				colType := t.inputTypes[i]
				destVec.Append(
					coldata.SliceArgs{
						ColType:   colType,
						Src:       vec,
						Sel:       t.inputBatch.Selection(),
						DestIdx:   toLength,
						SrcEndIdx: fromLength,
					},
				)
			}
			spooledRows += fromLength
			t.topK.SetLength(spooledRows)
		})
		remainingRows -= fromLength
		if fromLength == t.inputBatch.Length() {
			t.inputBatch = t.input.Next(ctx)
			t.firstUnprocessedTupleIdx = 0
		}
	}
	t.updateComparators(topKVecIdx, t.topK)

	// Initialize the heap.
	t.heap = make([]int, t.topK.Length())
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
		t.inputBatch = t.input.Next(ctx)
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

func (t *topKSorter) resetOutput() {
	if t.output == nil {
		t.output = t.allocator.NewMemBatchWithSize(t.inputTypes, coldata.BatchSize())
	} else {
		t.output.ResetInternalBatch()
	}
}

func (t *topKSorter) emit() coldata.Batch {
	t.resetOutput()
	toEmit := t.topK.Length() - t.emitted
	if toEmit == 0 {
		// We're done.
		return coldata.ZeroBatch
	}
	if toEmit > coldata.BatchSize() {
		toEmit = coldata.BatchSize()
	}
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
					ColType:     t.inputTypes[i],
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
				execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected direction value %d", d))
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

func (t *topKSorter) ExportBuffered(Operator) coldata.Batch {
	topKLen := t.topK.Length()
	// First, we check whether we have exported all tuples from the topK vector.
	if t.exportedFromTopK < topKLen {
		newExportedFromTopK := t.exportedFromTopK + coldata.BatchSize()
		if newExportedFromTopK > topKLen {
			newExportedFromTopK = topKLen
		}
		for i, typ := range t.inputTypes {
			window := t.topK.ColVec(i).Window(typ, t.exportedFromTopK, newExportedFromTopK)
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
		makeWindowIntoBatch(t.windowedBatch, t.inputBatch, t.firstUnprocessedTupleIdx, t.inputTypes)
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
