// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"fmt"
	"math/rand"
	"sort"
)

// moveTopKToFront swaps elements in the range [start, end) so that all elements
// in the range [start, k) are <= than all elements in the range [k, end).
func moveTopKToFront(data sort.Interface, start, end, k int, rng *rand.Rand) {
	if k < start || k > end {
		panic(fmt.Sprintf("k (%d) outside of range [%d, %d)", k, start, end))
	}
	if k == start || k == end {
		return
	}

	// The strategy is to choose a random pivot and partition the data into
	// three regions: elements < pivot, elements == pivot, elements > pivot.
	//
	// We first partition into two regions: elements <= pivot and
	// elements > pivot and further refine the first region if necessary.

	// Choose a random pivot and move it to the front.
	data.Swap(start, start+rng.Intn(end-start))
	pivot := start
	l, r := start+1, end
	for l < r {
		// Invariants:
		//  - elements in the range [start, l) are <= pivot
		//  - elements in the range [r, end) are > pivot
		if !data.Less(pivot, l) {
			l++
		} else if data.Less(pivot, r-1) {
			r--
		} else {
			data.Swap(l, r-1)
			l++
			r--
		}
	}
	mid := l
	// Everything in the range [start, mid) is <= than the pivot.
	// Everything in the range [mid, end) is > than the pivot.
	if k >= mid {
		// In this case, we eliminated at least the pivot (and all elements
		// equal to it).
		moveTopKToFront(data, mid, end, k, rng)
		return
	}

	// If we eliminated a decent amount of elements, we can recurse on [0, mid).
	// If the elements were distinct we would do this unconditionally, but in
	// general we could have a lot of elements equal to the pivot.
	if end-mid > (end-start)/4 {
		moveTopKToFront(data, start, mid, k, rng)
		return
	}

	// Now we work on the range [0, mid). Move everything that is equal to the
	// pivot to the back.
	data.Swap(pivot, mid-1)
	pivot = mid - 1
	for l, r = start, pivot-1; l <= r; {
		if data.Less(l, pivot) {
			l++
		} else {
			data.Swap(l, r)
			r--
		}
	}
	// Now everything in the range [start, l) is < than the pivot. Everything in the
	// range [l, mid) is equal to the pivot. If k is in the [l, mid) range we
	// are done, otherwise we recurse on [start, l).
	if k <= l {
		moveTopKToFront(data, start, l, k, rng)
	}
}

// MoveTopKToFront moves the top K elements to the front. It makes O(n) calls to
// data.Less and data.Swap (with very high probability). It uses Hoare's
// selection algorithm (aka quickselect).
func MoveTopKToFront(data sort.Interface, k int) {
	if data.Len() <= k {
		return
	}
	// We want the call to be deterministic so we use a predictable seed.
	r := rand.New(rand.NewSource(int64(data.Len()*1000 + k)))
	moveTopKToFront(data, 0, data.Len(), k, r)
}
