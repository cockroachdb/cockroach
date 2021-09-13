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
// +build execgen_template
//
// This file is the execgen template for sorttopk_tmpl.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/errors"
)

// execgen:template<partialOrder>
//gcassert:inline
func nextBatch(t *topKSorter, partialOrder bool) {
	t.inputBatch = t.Input.Next()
	if partialOrder {
		t.distincterInput.SetBatch(t.inputBatch)
		t.distincter.Next()
	}
	t.firstUnprocessedTupleIdx = 0
}

// spool reads in the entire input, always storing the top K rows it has seen so
// far in o.topK. This is done by maintaining a max heap of indices into o.topK.
// Whenever we encounter a row which is smaller than the max row in the heap,
// we replace the max with that row.
//
// After all the input has been read, we pop everything off the heap to
// determine the final output ordering. This is used in emit() to output the rows
// in sorted order.
// execgen:template<partialOrder>
func spool(t *topKSorter, partialOrder bool) {
	// Fill up t.topK by spooling up to K rows from the input.
	// We don't need to check for distinct groups until after we have filled
	// t.topK.
	// TODO(harding): We could emit the first N < K rows if the N rows are in one
	// or more distinct and complete groups, and then use a K-N size heap to find
	// the remaining top K-N rows.
	nextBatch(t, partialOrder)
	remainingRows := t.k
	groupId := 0
	for remainingRows > 0 && t.inputBatch.Length() > 0 {
		fromLength := t.inputBatch.Length()
		if remainingRows < uint64(t.inputBatch.Length()) {
			// t.topK will be full after this batch.
			fromLength = int(remainingRows)
		}
		if partialOrder {
			// Find the group id for each tuple just added to topK.
			sel := t.inputBatch.Selection()
			for i, k := 0, t.topK.Length(); i < fromLength; i, k = i+1, k+1 {
				idx := i
				if sel != nil {
					idx = sel[i]
				}
				if t.distinctOutput[idx] {
					groupId++
				}
				t.group[k] = groupId
			}
		}
		t.firstUnprocessedTupleIdx = fromLength
		t.topK.AppendTuples(t.inputBatch, 0 /* startIdx */, fromLength)
		remainingRows -= uint64(fromLength)
		if fromLength == t.inputBatch.Length() {
			nextBatch(t, partialOrder)
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
	// swap it in. When we find the end of the group, we can finish reading the
	// input.
	groupDone := false
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
					if partialOrder {
						// If this is a distinct group, we have already found the top K input,
						// so we can stop comparing the rest of this and subsequent batches.
						if t.distinctOutput[idx] {
							groupDone = true
							return
						}
					}
					maxIdx := t.heap[0]
					groupMaxIdx := 0
					if partialOrder {
						groupMaxIdx = t.group[maxIdx]
					}
					if compareRow(t, inputVecIdx, topKVecIdx, idx, maxIdx, groupId, groupMaxIdx, partialOrder) < 0 {
						for j := range t.inputTypes {
							t.comparators[j].set(inputVecIdx, topKVecIdx, idx, maxIdx)
						}
						if partialOrder {
							t.group[maxIdx] = groupId
						}
						heap.Fix(t, 0)
					}
				}
				t.firstUnprocessedTupleIdx = t.inputBatch.Length()
			},
		)
		if groupDone {
			break
		}
		nextBatch(t, partialOrder)
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

// execgen:template<partialOrder>
func compareRowInternal(
	t *topKSorter, vecIdx1 int, vecIdx2 int, rowIdx1 int, rowIdx2 int, partialOrder bool,
) int {
	if partialOrder {
		return compareRow(t, vecIdx1, vecIdx2, rowIdx1, rowIdx2, t.group[t.heap[rowIdx1]], t.group[t.heap[rowIdx2]], partialOrder)
	} else {
		return compareRow(t, vecIdx1, vecIdx2, rowIdx1, rowIdx2, 0 /* groupIdx1 */, 0 /* groupIdx2 */, partialOrder)
	}
}

// execgen:template<partialOrder>
func compareRow(
	t *topKSorter,
	vecIdx1 int,
	vecIdx2 int,
	rowIdx1 int,
	rowIdx2 int,
	groupIdx1 int,
	groupIdx2 int,
	partialOrder bool,
) int {
	for i := range t.orderingCols {
		if partialOrder {
			if i < t.matchLen && groupIdx1 == groupIdx2 {
				// If the tuples being compared are in the same group, we only need to
				// compare the columns that are not already ordered.
				continue
			}
		}
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

// spool reads in the entire input, always storing the top K rows it has seen so
// far in o.topK. This is done by maintaining a max heap of indices into o.topK.
// Whenever we encounter a row which is smaller than the max row in the heap,
// we replace the max with that row.
//
// After all the input has been read, we pop everything off the heap to
// determine the final output ordering. This is used in emit() to output the rows
// in sorted order.
func (t *topKSorter) spool() {
	if t.hasPartialOrder {
		spool(t, true /* partialOrder */)
	} else {
		spool(t, false /* partialOrder */)
	}
}

// Less is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Less(i, j int) bool {
	return compareRowInternal(t, topKVecIdx, topKVecIdx, t.heap[i], t.heap[j], t.hasPartialOrder) > 0
}
