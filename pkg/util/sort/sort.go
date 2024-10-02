// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sort

import (
	"math/bits"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
)

// The next few sorting functions are largely unmodified
// from the GO standard library implementation, save for occasional
// cancellation checks.
//
// TODO(itsbilal): Consider referencing the RowContainer pointer type
// directly instead of redirecting all calls through sort.Interface.

type sortedHint int // hint for pdqsort when choosing the pivot

const (
	unknownHint sortedHint = iota
	increasingHint
	decreasingHint
)

// xorshift paper: https://www.jstatsoft.org/article/view/v008i14/xorshift.pdf
type xorshift uint64

func (r *xorshift) Next() uint64 {
	*r ^= *r << 13
	*r ^= *r >> 17
	*r ^= *r << 5
	return uint64(*r)
}

func nextPowerOfTwo(length int) uint {
	shift := uint(bits.Len(uint(length)))
	return uint(1 << shift)
}

// insertionSort sorts data[a:b] using insertion sort.
func insertionSort(data sort.Interface, a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && data.Less(j, j-1); j-- {
			data.Swap(j, j-1)
		}
	}
}

// siftDown implements the heap property on data[lo:hi].
// first is an offset into the array where the root of the heap lies.
func siftDown(data sort.Interface, lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && data.Less(first+child, first+child+1) {
			child++
		}
		if !data.Less(first+root, first+child) {
			return
		}
		data.Swap(first+root, first+child)
		root = child
	}
}

func heapSort(data sort.Interface, a, b int, cancelChecker *cancelchecker.CancelChecker) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		if cancelChecker.Check() != nil {
			return
		}
		siftDown(data, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data.Swap(first, first+i)
		siftDown(data, lo, i, first)
	}
}

// pdqsort sorts data[a:b].
// The algorithm based on pattern-defeating quicksort(pdqsort), but without the optimizations from BlockQuicksort.
// pdqsort paper: https://arxiv.org/pdf/2106.05123.pdf
// C++ implementation: https://github.com/orlp/pdqsort
// Rust implementation: https://docs.rs/pdqsort/latest/pdqsort/
// limit is the number of allowed bad (very unbalanced) pivots before falling back to heapsort.
func pdqsort(data sort.Interface, a, b, limit int, cancelChecker *cancelchecker.CancelChecker) {
	const maxInsertion = 12

	var (
		wasBalanced    = true // whether the last partitioning was reasonably balanced
		wasPartitioned = true // whether the slice was already partitioned
	)

	for {
		length := b - a

		if length <= maxInsertion {
			insertionSort(data, a, b)
			return
		}

		// Fall back to heapsort if too many bad choices were made.
		if limit == 0 {
			heapSort(data, a, b, cancelChecker)
			return
		}

		// If the last partitioning was imbalanced, we need to breaking patterns.
		if !wasBalanced {
			breakPatterns(data, a, b)
			limit--
		}

		// Short-circuit sort if necessary.
		if cancelChecker.Check() != nil {
			return
		}

		pivot, hint := choosePivot(data, a, b)
		if hint == decreasingHint {
			reverseRange(data, a, b)
			// The chosen pivot was pivot-a elements after the start of the array.
			// After reversing it is pivot-a elements before the end of the array.
			// The idea came from Rust's implementation.
			pivot = (b - 1) - (pivot - a)
			hint = increasingHint
		}

		// The slice is likely already sorted.
		if wasBalanced && wasPartitioned && hint == increasingHint {
			if partialInsertionSort(data, a, b) {
				return
			}
		}

		// Probably the slice contains many duplicate elements, partition the slice into
		// elements equal to and elements greater than the pivot.
		if a > 0 && !data.Less(a-1, pivot) {
			mid := partitionEqual(data, a, b, pivot)
			a = mid
			continue
		}

		mid, alreadyPartitioned := partition(data, a, b, pivot)
		wasPartitioned = alreadyPartitioned

		leftLen, rightLen := mid-a, b-mid
		balanceThreshold := length / 8
		if leftLen < rightLen {
			wasBalanced = leftLen >= balanceThreshold
			pdqsort(data, a, mid, limit, cancelChecker)
			a = mid + 1
		} else {
			wasBalanced = rightLen >= balanceThreshold
			pdqsort(data, mid+1, b, limit, cancelChecker)
			b = mid
		}
	}
}

// partition does one quicksort partition.
// Let p = data[pivot]
// Moves elements in data[a:b] around, so that data[i]<p and data[j]>=p for i<newpivot and j>newpivot.
// On return, data[newpivot] = p
func partition(data sort.Interface, a, b, pivot int) (newpivot int, alreadyPartitioned bool) {
	data.Swap(a, pivot)
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for i <= j && data.Less(i, a) {
		i++
	}
	for i <= j && !data.Less(j, a) {
		j--
	}
	if i > j {
		data.Swap(j, a)
		return j, true
	}
	data.Swap(i, j)
	i++
	j--

	for {
		for i <= j && data.Less(i, a) {
			i++
		}
		for i <= j && !data.Less(j, a) {
			j--
		}
		if i > j {
			break
		}
		data.Swap(i, j)
		i++
		j--
	}
	data.Swap(j, a)
	return j, false
}

// partitionEqual partitions data[a:b] into elements equal to data[pivot] followed by elements greater than data[pivot].
// It assumed that data[a:b] does not contain elements smaller than the data[pivot].
func partitionEqual(data sort.Interface, a, b, pivot int) (newpivot int) {
	data.Swap(a, pivot)
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for {
		for i <= j && !data.Less(a, i) {
			i++
		}
		for i <= j && data.Less(a, j) {
			j--
		}
		if i > j {
			break
		}
		data.Swap(i, j)
		i++
		j--
	}
	return i
}

// partialInsertionSort partially sorts a slice, returns true if the slice is sorted at the end.
func partialInsertionSort(data sort.Interface, a, b int) bool {
	const (
		maxSteps         = 5  // maximum number of adjacent out-of-order pairs that will get shifted
		shortestShifting = 50 // don't shift any elements on short arrays
	)
	i := a + 1
	for j := 0; j < maxSteps; j++ {
		for i < b && !data.Less(i, i-1) {
			i++
		}

		if i == b {
			return true
		}

		if b-a < shortestShifting {
			return false
		}

		data.Swap(i, i-1)

		// Shift the smaller one to the left.
		if i-a >= 2 {
			for j := i - 1; j >= 1; j-- {
				if !data.Less(j, j-1) {
					break
				}
				data.Swap(j, j-1)
			}
		}
		// Shift the greater one to the right.
		if b-i >= 2 {
			for j := i + 1; j < b; j++ {
				if !data.Less(j, j-1) {
					break
				}
				data.Swap(j, j-1)
			}
		}
	}
	return false
}

// breakPatterns scatters some elements around in an attempt to break some patterns
// that might cause imbalanced partitions in quicksort.
func breakPatterns(data sort.Interface, a, b int) {
	length := b - a
	if length >= 8 {
		random := xorshift(length)
		modulus := nextPowerOfTwo(length)

		for idx := a + (length/4)*2 - 1; idx <= a+(length/4)*2+1; idx++ {
			other := int(uint(random.Next()) & (modulus - 1))
			if other >= length {
				other -= length
			}
			data.Swap(idx, a+other)
		}
	}
}

// choosePivot chooses a pivot in data[a:b].
//
// [0,8): chooses a static pivot.
// [8,shortestNinther): uses the simple median-of-three method.
// [shortestNinther,∞): uses the Tukey ninther method.
func choosePivot(data sort.Interface, a, b int) (pivot int, hint sortedHint) {
	const (
		shortestNinther = 50
		maxSwaps        = 4 * 3
	)

	l := b - a

	var (
		swaps int
		i     = a + l/4*1
		j     = a + l/4*2
		k     = a + l/4*3
	)

	if l >= 8 {
		if l >= shortestNinther {
			// Tukey ninther method, the idea came from Rust's implementation.
			i = medianAdjacent(data, i, &swaps)
			j = medianAdjacent(data, j, &swaps)
			k = medianAdjacent(data, k, &swaps)
		}
		// Find the median among i, j, k and stores it into j.
		j = median(data, i, j, k, &swaps)
	}

	switch swaps {
	case 0:
		return j, increasingHint
	case maxSwaps:
		return j, decreasingHint
	default:
		return j, unknownHint
	}
}

// order2 returns x,y where data[x] <= data[y], where x,y=a,b or x,y=b,a.
func order2(data sort.Interface, a, b int, swaps *int) (int, int) {
	if data.Less(b, a) {
		*swaps++
		return b, a
	}
	return a, b
}

// median returns x where data[x] is the median of data[a],data[b],data[c], where x is a, b, or c.
func median(data sort.Interface, a, b, c int, swaps *int) int {
	a, b = order2(data, a, b, swaps)
	b, _ = order2(data, b, c, swaps)
	_, b = order2(data, a, b, swaps)
	return b
}

// medianAdjacent finds the median of data[a - 1], data[a], data[a + 1] and stores the index into a.
func medianAdjacent(data sort.Interface, a int, swaps *int) int {
	return median(data, a-1, a, a+1, swaps)
}

func reverseRange(data sort.Interface, a, b int) {
	i := a
	j := b - 1
	for i < j {
		data.Swap(i, j)
		i++
		j--
	}
}

// Sort sorts data.
// It makes one call to data.Len to determine n, and O(n*log(n)) calls to
// data.Less and data.Swap. The sort is not guaranteed to be stable.
func Sort(data sort.Interface, cancelChecker *cancelchecker.CancelChecker) {
	n := data.Len()
	if n <= 1 {
		return
	}
	limit := bits.Len(uint(n))
	pdqsort(data, 0, n, limit, cancelChecker)
}
