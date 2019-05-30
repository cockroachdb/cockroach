// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package exec

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

const (
	topKVecIdx  = 0
	inputVecIdx = 1
)

// NewTopKSorter returns a new sort operator, which sorts its input on the
// columns given in orderingCols and returns the first K rows. The inputTypes
// must correspond 1-1 with the columns in the input operator.
func NewTopKSorter(
	input Operator, inputTypes []types.T, orderingCols []distsqlpb.Ordering_Column, k uint16,
) Operator {
	return &topKSorter{
		input:        input,
		inputTypes:   inputTypes,
		orderingCols: orderingCols,
		k:            k,
	}
}

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
	input        Operator
	orderingCols []distsqlpb.Ordering_Column
	inputTypes   []types.T
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

func (o *topKSorter) Init() {
	o.input.Init()
	o.topK = coldata.NewMemBatchWithSize(o.inputTypes, int(o.k))
	o.comparators = make([]vecComparator, len(o.orderingCols))
	for i := range o.orderingCols {
		typ := o.inputTypes[o.orderingCols[i].ColIdx]
		// one vec for output batch and one for current input batch
		o.comparators[i] = GetVecComparator(typ, 2)
	}
	o.output = coldata.NewMemBatchWithSize(o.inputTypes, coldata.BatchSize)
}

func (o *topKSorter) Next(ctx context.Context) coldata.Batch {
	switch o.state {
	case topKSortSpooling:
		o.spool(ctx)
		o.state = topKSortEmitting
		fallthrough
	case topKSortEmitting:
		return o.emit()
	}
	panic(fmt.Sprintf("invalid sort state %v", o.state))
}

// spool reads in the entire input, always storing the top K rows it has seen so
// far in o.topK. This is done by maintaining a max heap of indices into o.topK.
// Whenever we encounter a row which is smaller than the max row in the heap,
// we replace the max with that row.
//
// After all the input has been read, we pop everything off the heap to
// determine the final output ordering. This is used in emit() to output the rows
// in sorted order.
func (o *topKSorter) spool(ctx context.Context) {
	// Fill up o.topK by spooling up to K rows from the input.
	inputBatch := o.input.Next(ctx)
	inputBatchIdx := uint16(0)
	spooledRows := uint16(0)
	remainingRows := o.k
	for remainingRows > 0 && inputBatch.Length() > 0 {
		toLength := uint64(spooledRows)
		fromLength := inputBatch.Length()
		if remainingRows < inputBatch.Length() {
			// o.topK will be full after this batch.
			fromLength = remainingRows
			inputBatchIdx = fromLength
		}
		for i := range o.inputTypes {
			destVec := o.topK.ColVec(i)
			vec := inputBatch.ColVec(i)
			colType := o.inputTypes[i]
			if inputBatch.Selection() == nil {
				destVec.Append(vec, colType, toLength, fromLength)
			} else {
				destVec.AppendWithSel(vec, inputBatch.Selection(), fromLength, colType, toLength)
			}
		}
		spooledRows += fromLength
		remainingRows -= fromLength
		if fromLength == inputBatch.Length() {
			inputBatch = o.input.Next(ctx)
		}
	}
	o.topK.SetLength(spooledRows)
	o.updateComparators(topKVecIdx, o.topK)

	// Initialize the heap.
	o.heap = make([]uint16, o.topK.Length())
	for i := range o.heap {
		o.heap[i] = uint16(i)
	}
	heap.Init(o)

	// Read the remainder of the input. Whenever a row is less than the heap max,
	// swap it in.
	for inputBatch.Length() > 0 {
		o.updateComparators(inputVecIdx, inputBatch)
		sel := inputBatch.Selection()
		for i := inputBatchIdx; i < inputBatch.Length(); i++ {
			idx := i
			if sel != nil {
				idx = sel[i]
			}
			maxIdx := o.heap[0]
			if o.compareRow(inputVecIdx, topKVecIdx, idx, maxIdx) < 0 {
				for j := range o.inputTypes {
					// TODO(solon): Make this copy more efficient, perhaps by adding a
					// copy method to the vecComparator interface. This would avoid
					// needing to switch on the column type every time.
					o.topK.ColVec(j).AppendSlice(
						inputBatch.ColVec(j), o.inputTypes[j], uint64(maxIdx), uint16(idx), uint16(idx)+1)

				}
				heap.Fix(o, 0)
			}
		}
		inputBatch = o.input.Next(ctx)
		inputBatchIdx = 0
	}

	// o.topK now contains the top K rows unsorted. Create a selection vector
	// which specifies the rows in sorted order by popping everything off the
	// heap. Note that it's a max heap so we need to fill the selection vector in
	// reverse.
	o.sel = make([]uint16, o.topK.Length())
	for i := 0; i < int(o.topK.Length()); i++ {
		o.sel[len(o.sel)-i-1] = heap.Pop(o).(uint16)
	}
}

func (o *topKSorter) emit() coldata.Batch {
	toEmit := o.topK.Length() - o.emitted
	if toEmit == 0 {
		// We're done.
		return coldata.NewMemBatchWithSize(o.inputTypes, 0)
	}
	if toEmit > coldata.BatchSize {
		toEmit = coldata.BatchSize
	}
	for i := range o.inputTypes {
		vec := o.output.ColVec(i)
		vec.CopyWithSelInt16(o.topK.ColVec(i), o.sel, toEmit, o.inputTypes[i])
	}
	o.output.SetLength(toEmit)
	o.emitted += toEmit
	return o.output
}

func (o *topKSorter) compareRow(vecIdx1, vecIdx2 int, rowIdx1, rowIdx2 uint16) int {
	for i := range o.orderingCols {
		info := o.orderingCols[i]
		res := o.comparators[i].compare(vecIdx1, vecIdx2, rowIdx1, rowIdx2)
		if res != 0 {
			switch d := info.Direction; d {
			case distsqlpb.Ordering_Column_ASC:
				return res
			case distsqlpb.Ordering_Column_DESC:
				return -res
			default:
				panic(fmt.Sprintf("unexpected direction value %d", d))
			}
		}
	}
	return 0
}

func (o *topKSorter) updateComparators(vecIdx int, batch coldata.Batch) {
	for i := range o.orderingCols {
		vec := batch.ColVec(int(o.orderingCols[i].ColIdx))
		o.comparators[i].setVec(vecIdx, vec)
	}
}

// Len is part of heap.Interface and is only meant to be used internally.
func (o *topKSorter) Len() int {
	return len(o.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (o *topKSorter) Less(i, j int) bool {
	return o.compareRow(topKVecIdx, topKVecIdx, o.heap[i], o.heap[j]) > 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (o *topKSorter) Swap(i, j int) {
	o.heap[i], o.heap[j] = o.heap[j], o.heap[i]
}

// Push is part of heap.Interface and is only meant to be used internally.
func (o *topKSorter) Push(x interface{}) {
	o.heap = append(o.heap, x.(uint16))
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (o *topKSorter) Pop() interface{} {
	x := o.heap[len(o.heap)-1]
	o.heap = o.heap[:len(o.heap)-1]
	return x
}
