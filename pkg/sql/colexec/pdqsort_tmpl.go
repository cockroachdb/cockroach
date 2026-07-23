// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// {{/*
//go:build execgen_template

// */}}

package colexec

import "math/bits"

// This file is copied from the Go standard library's sort
// implementation, found in https://golang.org/src/sort/sort.go. The only
// modifications are to template each function into each sort_* struct, so
// that the sort methods can be called without incurring any interface overhead
// for the Swap and Less methods.

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

// {{range .}}
// {{$nulls := .Nulls}}
// {{range .DirOverloads}}
// {{$dir := .DirString}}
// {{range .FamilyOverloads}}
// {{range .WidthOverloads}}

// insertionSort sorts data[a:b] using insertion sort.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) insertionSort(a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && p.Less(j, j-1); j-- {
			p.Swap(j, j-1)
		}
	}
}

// siftDown implements the heap property on data[lo:hi].
// first is an offset into the array where the root of the heap lies.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) siftDown(lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && p.Less(first+child, first+child+1) {
			child++
		}
		if !p.Less(first+root, first+child) {
			return
		}
		p.Swap(first+root, first+child)
		root = child
	}
}

func (p *sort_TYPE_DIR_HANDLES_NULLSOp) heapSort(a, b int) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		p.cancelChecker.Check()
		p.siftDown(i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		p.Swap(first, first+i)
		p.siftDown(lo, i, first)
	}
}

// pdqsort sorts data[a:b].
// The algorithm based on pattern-defeating quicksort(pdqsort), but without the optimizations from BlockQuicksort.
// pdqsort paper: https://arxiv.org/pdf/2106.05123.pdf
// C++ implementation: https://github.com/orlp/pdqsort
// Rust implementation: https://docs.rs/pdqsort/latest/pdqsort/
// limit is the number of allowed bad (very unbalanced) pivots before falling back to heapsort.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) pdqsort(a, b, limit int) {
	const maxInsertion = 12

	var (
		wasBalanced    = true // whether the last partitioning was reasonably balanced
		wasPartitioned = true // whether the slice was already partitioned
	)

	for {
		length := b - a

		if length <= maxInsertion {
			p.insertionSort(a, b)
			return
		}

		// Fall back to heapsort if too many bad choices were made.
		if limit == 0 {
			p.heapSort(a, b)
			return
		}

		// If the last partitioning was imbalanced, we need to breaking patterns.
		if !wasBalanced {
			p.breakPatterns(a, b)
			limit--
		}

		p.cancelChecker.Check()
		pivot, hint := p.choosePivot(a, b)
		if hint == decreasingHint {
			p.reverseRange(a, b)
			// The chosen pivot was pivot-a elements after the start of the array.
			// After reversing it is pivot-a elements before the end of the array.
			// The idea came from Rust's implementation.
			pivot = (b - 1) - (pivot - a)
			hint = increasingHint
		}

		// The slice is likely already sorted.
		if wasBalanced && wasPartitioned && hint == increasingHint {
			if p.partialInsertionSort(a, b) {
				return
			}
		}

		// Probably the slice contains many duplicate elements, partition the slice into
		// elements equal to and elements greater than the pivot.
		if a > 0 && !p.Less(a-1, pivot) {
			mid := p.partitionEqual(a, b, pivot)
			a = mid
			continue
		}

		mid, alreadyPartitioned := p.partition(a, b, pivot)
		wasPartitioned = alreadyPartitioned

		leftLen, rightLen := mid-a, b-mid
		balanceThreshold := length / 8
		if leftLen < rightLen {
			wasBalanced = leftLen >= balanceThreshold
			p.pdqsort(a, mid, limit)
			a = mid + 1
		} else {
			wasBalanced = rightLen >= balanceThreshold
			p.pdqsort(mid+1, b, limit)
			b = mid
		}
	}
}

// partition does one quicksort partition.
// Let p = data[pivot]
// Moves elements in data[a:b] around, so that data[i]<p and data[j]>=p for i<newpivot and j>newpivot.
// On return, data[newpivot] = p
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) partition(
	a, b, pivot int,
) (newpivot int, alreadyPartitioned bool) {
	p.Swap(a, pivot)
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for i <= j && p.Less(i, a) {
		i++
	}
	for i <= j && !p.Less(j, a) {
		j--
	}
	if i > j {
		p.Swap(j, a)
		return j, true
	}
	p.Swap(i, j)
	i++
	j--

	for {
		for i <= j && p.Less(i, a) {
			i++
		}
		for i <= j && !p.Less(j, a) {
			j--
		}
		if i > j {
			break
		}
		p.Swap(i, j)
		i++
		j--
	}
	p.Swap(j, a)
	return j, false
}

// partitionEqual partitions data[a:b] into elements equal to data[pivot] followed by elements greater than data[pivot].
// It assumed that data[a:b] does not contain elements smaller than the data[pivot].
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) partitionEqual(a, b, pivot int) (newpivot int) {
	p.Swap(a, pivot)
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for {
		for i <= j && !p.Less(a, i) {
			i++
		}
		for i <= j && p.Less(a, j) {
			j--
		}
		if i > j {
			break
		}
		p.Swap(i, j)
		i++
		j--
	}
	return i
}

// partialInsertionSort partially sorts a slice, returns true if the slice is sorted at the end.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) partialInsertionSort(a, b int) bool {
	const (
		maxSteps         = 5  // maximum number of adjacent out-of-order pairs that will get shifted
		shortestShifting = 50 // don't shift any elements on short arrays
	)
	i := a + 1
	for j := 0; j < maxSteps; j++ {
		for i < b && !p.Less(i, i-1) {
			i++
		}

		if i == b {
			return true
		}

		if b-a < shortestShifting {
			return false
		}

		p.Swap(i, i-1)

		// Shift the smaller one to the left.
		if i-a >= 2 {
			for j := i - 1; j >= 1; j-- {
				if !p.Less(j, j-1) {
					break
				}
				p.Swap(j, j-1)
			}
		}
		// Shift the greater one to the right.
		if b-i >= 2 {
			for j := i + 1; j < b; j++ {
				if !p.Less(j, j-1) {
					break
				}
				p.Swap(j, j-1)
			}
		}
	}
	return false
}

// breakPatterns scatters some elements around in an attempt to break some patterns
// that might cause imbalanced partitions in quicksort.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) breakPatterns(a, b int) {
	length := b - a
	if length >= 8 {
		random := xorshift(length)
		modulus := nextPowerOfTwo(length)

		for idx := a + (length/4)*2 - 1; idx <= a+(length/4)*2+1; idx++ {
			other := int(uint(random.Next()) & (modulus - 1))
			if other >= length {
				other -= length
			}
			p.Swap(idx, a+other)
		}
	}
}

// choosePivot chooses a pivot in data[a:b].
//
// [0,8): chooses a static pivot.
// [8,shortestNinther): uses the simple median-of-three method.
// [shortestNinther,∞): uses the Tukey ninther method.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) choosePivot(a, b int) (pivot int, hint sortedHint) {
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
			i = p.medianAdjacent(i, &swaps)
			j = p.medianAdjacent(j, &swaps)
			k = p.medianAdjacent(k, &swaps)
		}
		// Find the median among i, j, k and stores it into j.
		j = p.median(i, j, k, &swaps)
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
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) order2(a, b int, swaps *int) (int, int) {
	if p.Less(b, a) {
		*swaps++
		return b, a
	}
	return a, b
}

// median returns x where data[x] is the median of data[a],data[b],data[c], where x is a, b, or c.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) median(a, b, c int, swaps *int) int {
	a, b = p.order2(a, b, swaps)
	b, _ = p.order2(b, c, swaps)
	_, b = p.order2(a, b, swaps)
	return b
}

// medianAdjacent finds the median of data[a - 1], data[a], data[a + 1] and stores the index into a.
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) medianAdjacent(a int, swaps *int) int {
	return p.median(a-1, a, a+1, swaps)
}

func (p *sort_TYPE_DIR_HANDLES_NULLSOp) reverseRange(a, b int) {
	i := a
	j := b - 1
	for i < j {
		p.Swap(i, j)
		i++
		j--
	}
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
