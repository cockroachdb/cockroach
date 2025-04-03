// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for sorttopk.eg.go. It's formatted in a
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
// execgen:inline
func nextBatch(t *topKSorter, partialOrder bool) {
	t.cancelChecker.CheckEveryCall()
	t.inputBatch = t.Input.Next()
	if partialOrder {
		t.orderState.distincterInput.SetBatch(t.inputBatch)
		t.orderState.distincter.Next()
	}
	t.firstUnprocessedTupleIdx = 0
}

// processGroupsInBatch associates a row in the top K heap with its distinct
// partially ordered column group. It returns the most recently found groupId.
// execgen:template<useSel>
// execgen:inline
func processGroupsInBatch(
	t *topKSorter, fromLength int, groupIdStart int, sel []int, useSel bool,
) (groupId int) {
	groupId = groupIdStart
	for i, k := 0, t.topK.Length(); i < fromLength; i, k = i+1, k+1 {
		if useSel {
			idx := sel[i]
		} else {
			idx := i
		}
		if t.orderState.distinctOutput[idx] {
			groupId++
		}
		t.orderState.group[k] = groupId
	}
	return groupId
}

// processBatch checks whether each tuple in a batch should be added to the topK
// heap. If partialOrder is true, processing stops when the current distinct
// ordered group is complete. If useSel is true, we use the selection vector.
// execgen:template<partialOrder,useSel>
// execgen:inline
func processBatch(
	t *topKSorter, groupId int, sel []int, partialOrder bool, useSel bool,
) (groupDone bool) {
	for ; t.firstUnprocessedTupleIdx < t.inputBatch.Length(); t.firstUnprocessedTupleIdx++ {
		if useSel {
			idx := sel[t.firstUnprocessedTupleIdx]
		} else {
			idx := t.firstUnprocessedTupleIdx
		}
		if partialOrder {
			// If this is a distinct group, we have already found the top K input,
			// so we can stop comparing the rest of this and subsequent batches.
			if t.orderState.distinctOutput[idx] {
				return true
			}
		}
		maxIdx := t.heap[0]
		groupMaxIdx := 0
		if partialOrder {
			groupMaxIdx = t.orderState.group[maxIdx]
		}
		if compareRow(t, inputVecIdx, topKVecIdx, idx, maxIdx, groupId, groupMaxIdx, partialOrder) < 0 {
			for j := range t.inputTypes {
				t.comparators[j].set(inputVecIdx, topKVecIdx, idx, maxIdx)
			}
			if partialOrder {
				t.orderState.group[maxIdx] = groupId
			}
			heap.Fix(t.heaper, 0)
		}
	}
	return false
}

// spool reads in the entire input, always storing the top K rows it has seen so
// far in o.topK. This is done by maintaining a max heap of indices into o.topK.
// Whenever we encounter a row which is smaller than the max row in the heap,
// we replace the max with that row.
//
// After all the input has been read, we pop everything off the heap to
// determine the final output ordering. This is used in emit() to output the rows
// in sorted order.
//
// If partialOrder is true, then we chunk the input into distinct groups based
// on the partially ordered input, and stop adding to the max heap after K rows
// and the group of the Kth row have been processed. If it's false, we assume
// that the input is unordered, and process all rows.
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
			if sel != nil {
				groupId = processGroupsInBatch(t, fromLength, groupId, sel, true)
			} else {
				groupId = processGroupsInBatch(t, fromLength, groupId, sel, false)
			}
		}
		// Importantly, the memory limit can only be reached _after_ the tuples
		// are appended to topK, so all fromLength tuples are considered
		// "processed".
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
	heap.Init(t.heaper)
	// Read the remainder of the input. Whenever a row is less than the heap max,
	// swap it in. When we find the end of the group, we can finish reading the
	// input.
	if partialOrder {
		groupDone := false
	}
	for t.inputBatch.Length() > 0 {
		t.updateComparators(inputVecIdx, t.inputBatch)
		sel := t.inputBatch.Selection()
		t.allocator.PerformOperation(
			t.topK.ColVecs(),
			func() {
				if sel != nil {
					if partialOrder {
						groupDone = processBatch(t, groupId, sel, true, true)
					} else {
						processBatch(t, groupId, sel, false, true)
					}
				} else {
					if partialOrder {
						groupDone = processBatch(t, groupId, sel, true, false)
					} else {
						processBatch(t, groupId, sel, false, false)
					}
				}
			},
		)
		if partialOrder {
			if groupDone {
				break
			}
		}
		nextBatch(t, partialOrder)
	}
	// t.topK now contains the top K rows unsorted. Create a selection vector
	// which specifies the rows in sorted order by popping everything off the
	// heap. Note that it's a max heap so we need to fill the selection vector in
	// reverse.
	t.sel = make([]int, t.topK.Length())
	for i := 0; i < t.topK.Length(); i++ {
		t.sel[len(t.sel)-i-1] = heap.Pop(t.heaper).(int)
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
			// TODO(harding): If groupIdx1 != groupIdx2, we may be able to do some
			// optimization if the ordered columns are in the same direction.
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

// topKHeaper implements part of the heap.Interface for non-ordered input.
type topKHeaper struct {
	*topKSorter
}

var _ heap.Interface = &topKHeaper{}

// Less is part of heap.Interface and is only meant to be used internally.
func (t *topKHeaper) Less(i, j int) bool {
	return compareRow(t.topKSorter, topKVecIdx, topKVecIdx, t.heap[i], t.heap[j], 0, 0, false) > 0
}

// topKHeaper implements part of the heap.Interface for partially ordered input.
type topKPartialOrderHeaper struct {
	*topKSorter
}

var _ heap.Interface = &topKPartialOrderHeaper{}

// Less is part of heap.Interface and is only meant to be used internally.
func (t *topKPartialOrderHeaper) Less(i, j int) bool {
	return compareRow(t.topKSorter, topKVecIdx, topKVecIdx, t.heap[i], t.heap[j], t.orderState.group[t.heap[i]], t.orderState.group[t.heap[j]], true) > 0
}
