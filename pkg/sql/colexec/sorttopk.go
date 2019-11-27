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

var _ Operator = &topKSorter{}

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
	// comparators stores one comparator per ordering column.
	comparators []vecComparator
	// topK stores the top K rows. It is not sorted internally.
	topK coldata.Batch
	// heap is a max heap which stores indices into topK.
	heap []uint16
	// sel is a selection vector which specifies an ordering on topK.
	sel []uint16
	// emitted is the count of rows which have been emitted so far.
	emitted uint16
	output  coldata.Batch
}

func (t *topKSorter) Init() {
	t.input.Init()
	t.topK = t.allocator.NewMemBatchWithSize(t.inputTypes, int(t.k))
	t.comparators = make([]vecComparator, len(t.inputTypes))
	for i := range t.inputTypes {
		typ := t.inputTypes[i]
		t.comparators[i] = GetVecComparator(typ, 2)
	}
	t.output = t.allocator.NewMemBatchWithSize(t.inputTypes, int(coldata.BatchSize()))
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
	inputBatch := t.input.Next(ctx)
	inputBatchIdx := uint16(0)
	spooledRows := uint16(0)
	remainingRows := t.k
	for remainingRows > 0 && inputBatch.Length() > 0 {
		toLength := uint64(spooledRows)
		fromLength := inputBatch.Length()
		if remainingRows < inputBatch.Length() {
			// t.topK will be full after this batch.
			fromLength = remainingRows
			inputBatchIdx = fromLength
		}
		for i := range t.inputTypes {
			destVec := t.topK.ColVec(i)
			vec := inputBatch.ColVec(i)
			colType := t.inputTypes[i]
			t.allocator.Append(
				destVec,
				coldata.SliceArgs{
					ColType:   colType,
					Src:       vec,
					Sel:       inputBatch.Selection(),
					DestIdx:   toLength,
					SrcEndIdx: uint64(fromLength),
				},
			)
		}
		spooledRows += fromLength
		remainingRows -= fromLength
		if fromLength == inputBatch.Length() {
			inputBatch = t.input.Next(ctx)
		}
	}
	t.topK.SetLength(spooledRows)
	t.updateComparators(topKVecIdx, t.topK)

	// Initialize the heap.
	t.heap = make([]uint16, t.topK.Length())
	for i := range t.heap {
		t.heap[i] = uint16(i)
	}
	heap.Init(t)

	// Read the remainder of the input. Whenever a row is less than the heap max,
	// swap it in.
	for inputBatch.Length() > 0 {
		t.updateComparators(inputVecIdx, inputBatch)
		sel := inputBatch.Selection()
		t.allocator.performOperation(
			t.topK.ColVecs(),
			func() {
				for i := inputBatchIdx; i < inputBatch.Length(); i++ {
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
			},
		)
		inputBatch = t.input.Next(ctx)
		inputBatchIdx = 0
	}

	// t.topK now contains the top K rows unsorted. Create a selection vector
	// which specifies the rows in sorted order by popping everything off the
	// heap. Note that it's a max heap so we need to fill the selection vector in
	// reverse.
	t.sel = make([]uint16, t.topK.Length())
	for i := 0; i < int(t.topK.Length()); i++ {
		t.sel[len(t.sel)-i-1] = heap.Pop(t).(uint16)
	}
}

func (t *topKSorter) emit() coldata.Batch {
	t.output.ResetInternalBatch()
	toEmit := t.topK.Length() - t.emitted
	if toEmit == 0 {
		// We're done.
		t.output.SetLength(0)
		return t.output
	}
	if toEmit > coldata.BatchSize() {
		toEmit = coldata.BatchSize()
	}
	for i := range t.inputTypes {
		vec := t.output.ColVec(i)
		t.allocator.Copy(
			vec,
			coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:     t.inputTypes[i],
					Src:         t.topK.ColVec(i),
					Sel:         t.sel,
					SrcStartIdx: uint64(t.emitted),
					SrcEndIdx:   uint64(t.emitted + toEmit),
				},
			},
		)
	}
	t.output.SetLength(toEmit)
	t.emitted += toEmit
	return t.output
}

func (t *topKSorter) compareRow(vecIdx1, vecIdx2 int, rowIdx1, rowIdx2 uint16) int {
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
	t.heap = append(t.heap, x.(uint16))
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Pop() interface{} {
	x := t.heap[len(t.heap)-1]
	t.heap = t.heap[:len(t.heap)-1]
	return x
}
