// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package unique

import (
	"bytes"
	"reflect"
	"sort"
)

// UniquifyByteSlices takes as input a slice of slices of bytes, and
// deduplicates them using a sort and unique. The output will not contain any
// duplicates but it will be sorted.
func UniquifyByteSlices(slices [][]byte) [][]byte {
	if len(slices) == 0 {
		return slices
	}
	// First sort:
	sort.Slice(slices, func(i int, j int) bool {
		return bytes.Compare(slices[i], slices[j]) < 0
	})
	// Then distinct: (wouldn't it be nice if Go had generics?)
	lastUniqueIdx := 0
	for i := 1; i < len(slices); i++ {
		if !bytes.Equal(slices[i], slices[lastUniqueIdx]) {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			slices[lastUniqueIdx] = slices[i]
		}
	}
	slices = slices[:lastUniqueIdx+1]
	return slices
}

// UniquifyAcrossSlices removes elements from both slices that are duplicated
// across both of the slices. For example, inputs [1,2,3], [2,3,4] would remove
// 2 and 3 from both lists.
// It assumes that both slices are pre-sorted using the same comparison metric
// as cmpFunc provides, and also already free of duplicates internally. It
// returns the slices, which will have also been sorted as a side effect.
// cmpFunc compares the lth index of left to the rth index of right. It must
// return less than 0 if the left element is less than the right element, 0 if
// equal, and greater than 0 otherwise.
// setLeft sets the ith index of left to the jth index of left.
// setRight sets the ith index of right to the jth index of right.
// The function returns the new lengths of both input slices, whose elements
// will have been mutated, but whose lengths must be set the new lengths by
// the caller.
func UniquifyAcrossSlices(
	left interface{},
	right interface{},
	cmpFunc func(l, r int) int,
	setLeft func(i, j int),
	setRight func(i, j int),
) (leftLen, rightLen int) {
	leftSlice := reflect.ValueOf(left)
	rightSlice := reflect.ValueOf(right)

	lLen := leftSlice.Len()
	rLen := rightSlice.Len()

	var lIn, lOut int
	var rIn, rOut int

	// Remove entries that are duplicated across both entry lists.
	// This loop walks through both lists using a merge strategy. Two pointers per
	// list are maintained. One is the "input pointer", which is always the ith
	// element of the input list. One is the "output pointer", which is the index
	// after the most recent unique element in the list. Every time we bump the
	// input pointer, we also set the element at the output pointer to that at
	// the input pointer, so we don't have to use extra space - we're
	// deduplicating in-place.
	for rIn < rLen || lIn < lLen {
		var cmp int
		if lIn == lLen {
			cmp = 1
		} else if rIn == rLen {
			cmp = -1
		} else {
			cmp = cmpFunc(lIn, rIn)
		}
		if cmp < 0 {
			setLeft(lOut, lIn)
			lIn++
			lOut++
		} else if cmp > 0 {
			setRight(rOut, rIn)
			rIn++
			rOut++
		} else {
			// Elements are identical - we want to remove them from the list. So
			// we increment our input indices without touching our output indices.
			// Next time through the loop, we'll shift the next element back to
			// the last output index which is now lagging behind the input index.
			lIn++
			rIn++
		}
	}
	return lOut, rOut
}
