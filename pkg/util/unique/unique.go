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

// UniquifyAcrossSlices removes things that are duplicated across both of the
// slices from both of the slices. It assumes that both slices are already free
// of duplicates internally. It returns the slices, which will have also been
// sorted as a side effect.
// left and right have to be pointers to pre-sorted slices. The slices must be
// the same types, and they must be pre-sorted along the same metric that
// cmpFunc checks.
// cmpFunc compares the lth index of left to the rth index of right.
// leftSet sets the ith index of left to the jth index of left.
// rightSet sets the ith index of right to the jth index of right.
func UniquifyAcrossSlices(
	left interface{},
	right interface{},
	cmpFunc func(l, r int) int,
	leftSet func(i, j int),
	rightSet func(i, j int),
) {

	leftSlice := reflect.ValueOf(left).Elem()
	rightSlice := reflect.ValueOf(right).Elem()

	leftLen := leftSlice.Len()
	rightLen := rightSlice.Len()

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
	for {
		if rIn >= rightLen && lIn >= leftLen {
			break
		} else if rIn >= rightLen {
			leftSet(lOut, lIn)
			lIn++
			lOut++
			continue
		} else if lIn >= leftLen {
			rightSet(rOut, rIn)
			rIn++
			rOut++
			continue
		}
		cmp := cmpFunc(lIn, rIn)
		switch cmp {
		case -1:
			leftSet(lOut, lIn)
			lIn++
			lOut++
		case 1:
			rightSet(rOut, rIn)
			rIn++
			rOut++
		case 0:
			// Elements are identical - we want to remove them from the list. So
			// we increment our input indices without touching our output indices.
			// Next time through the loop, we'll shift the next element back to
			// the last output index which is now lagging behind the input index.
			lIn++
			rIn++
		}
	}
	leftSlice.SetLen(lOut)
	rightSlice.SetLen(rOut)
}
