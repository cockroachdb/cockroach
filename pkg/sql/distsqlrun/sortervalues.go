// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// sorterValues is the internal wrapper around the collection of rows added to
// a sorter strategy, it is at this level that the rows to be sorted are stored.
type sorterValues struct {
	rows          sqlbase.EncDatumRows
	err           error // err can be set by the RowLess function.
	invertSorting bool  // Inverts the sorting predicate.
	ordering      sqlbase.ColumnOrdering
	tmpRow        sqlbase.EncDatumRow // Used to store temporary rows.
	alloc         sqlbase.DatumAlloc
}

var _ heap.Interface = &sorterValues{}

// Len is part of heap.Interface and is only meant to be used internally.
func (sv *sorterValues) Len() int {
	return len(sv.rows)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (sv *sorterValues) Less(i, j int) bool {
	ri := sv.rows[i]
	rj := sv.rows[j]

	return sv.invertSorting != sv.RowLess(ri, rj)
}

// RowLess reports whether the first row should sort before the second.
func (sv *sorterValues) RowLess(ri, rj sqlbase.EncDatumRow) bool {
	cmp, err := ri.Compare(&sv.alloc, sv.ordering, rj)
	if err != nil {
		sv.err = err
		return false
	}

	return cmp < 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (sv *sorterValues) Swap(i, j int) {
	sv.rows[i], sv.rows[j] = sv.rows[j], sv.rows[i]
}

// Pop implements the heap.Interface interface.
func (sv *sorterValues) Pop() interface{} {
	idx := len(sv.rows) - 1
	// Returning a pointer to avoid an allocation when storing the slice in an
	// interface{}.
	x := &(sv.rows)[idx]
	sv.rows = sv.rows[:idx]
	return x
}

// Next returns the first row from the heap representation of sorterValues.
func (sv *sorterValues) NextRow() sqlbase.EncDatumRow {
	if len(sv.rows) == 0 {
		return nil
	}

	x := heap.Pop(sv)
	return *x.(*sqlbase.EncDatumRow)
}

// Push implements the heap.Interface interface.
func (sv *sorterValues) Push(x interface{}) {
	sv.rows = append(sv.rows, sv.tmpRow)
}

// Add pushes the given row into the heap representation
// of the sorterValues.
func (sv *sorterValues) PushRow(row sqlbase.EncDatumRow) {
	// Avoid passing slice through interface{} to avoid allocation.
	sv.tmpRow = row
	heap.Push(sv, nil)
}

// Initializes the rows contained within sorterValues as a MaxHeap.
func (sv *sorterValues) InitMaxHeap() {
	sv.invertSorting = true
	heap.Init(sv)
}

// Initializes the rows contained within sorterValues as a MinHeap.
func (sv *sorterValues) InitMinHeap() {
	sv.invertSorting = false
	heap.Init(sv)
}

// Sort sorts all values in the sv.rows slice.
// When re-initialized to a MinHeap it essentially pops all values in the heap,
// resulting in the inverted ordering being sorted in reverse. Therefore, the
// slice is ordered correctly in-place.
func (sv *sorterValues) Sort() error {
	sv.InitMinHeap()
	return sv.err
}
