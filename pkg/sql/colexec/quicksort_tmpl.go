// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// {{/*
// +build execgen_template
// */}}

package colexec

// This file is copied from the Go standard library's sort
// implementation, found in https://golang.org/src/sort/sort.go. The only
// modifications are to template each function into each sort_* struct, so
// that the sort methods can be called without incurring any interface overhead
// for the Swap and Less methods.

// maxDepth returns a threshold at which quicksort should switch
// to heapsort. It returns 2*ceil(lg(n+1)).
func maxDepth(n int) int {
	var depth int
	for i := n; i > 0; i >>= 1 {
		depth++
	}
	return depth * 2
}

// {{range .}}
// {{$nulls := .Nulls}}
// {{range .DirOverloads}}
// {{$dir := .DirString}}
// {{range .FamilyOverloads}}
// {{range .WidthOverloads}}

// Insertion sort
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) insertionSort(a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && p.Less(j, j-1); j-- {
			p.Swap(j, j-1)
		}
	}
}

// siftDown implements the heap property on data[lo, hi).
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

// Quicksort, loosely following Bentley and McIlroy,
// ``Engineering a Sort Function,'' SP&E November 1993.

// medianOfThree moves the median of the three values data[m0], data[m1], data[m2] into data[m1].
func (p *sort_TYPE_DIR_HANDLES_NULLSOp) medianOfThree(m1, m0, m2 int) {
	// sort 3 elements
	if p.Less(m1, m0) {
		p.Swap(m1, m0)
	}
	// data[m0] <= data[m1]
	if p.Less(m2, m1) {
		p.Swap(m2, m1)
		// data[m0] <= data[m2] && data[m1] < data[m2]
		if p.Less(m1, m0) {
			p.Swap(m1, m0)
		}
	}
	// now data[m0] <= data[m1] <= data[m2]
}

func (p *sort_TYPE_DIR_HANDLES_NULLSOp) swapRange(a, b, n int) {
	for i := 0; i < n; i++ {
		p.Swap(a+i, b+i)
	}
}

func (p *sort_TYPE_DIR_HANDLES_NULLSOp) doPivot(lo, hi int) (midlo, midhi int) {
	m := int(uint(lo+hi) >> 1) // Written like this to avoid integer overflow.
	if hi-lo > 40 {
		// Tukey's ``Ninther,'' median of three medians of three.
		s := (hi - lo) / 8
		p.medianOfThree(lo, lo+s, lo+2*s)
		p.medianOfThree(m, m-s, m+s)
		p.medianOfThree(hi-1, hi-1-s, hi-1-2*s)
	}
	p.medianOfThree(lo, m, hi-1)

	// Invariants are:
	//	data[lo] = pivot (set up by ChoosePivot)
	//	data[lo < i < a] < pivot
	//	data[a <= i < b] <= pivot
	//	data[b <= i < c] unexamined
	//	data[c <= i < hi-1] > pivot
	//	data[hi-1] >= pivot
	pivot := lo
	a, c := lo+1, hi-1

	for ; a < c && p.Less(a, pivot); a++ {
	}
	b := a
	for {
		for ; b < c && !p.Less(pivot, b); b++ { // data[b] <= pivot
		}
		for ; b < c && p.Less(pivot, c-1); c-- { // data[c-1] > pivot
		}
		if b >= c {
			break
		}
		// data[b] > pivot; data[c-1] <= pivot
		p.Swap(b, c-1)
		b++
		c--
	}
	// If hi-c<3 then there are duplicates (by property of median of nine).
	// Let be a bit more conservative, and set border to 5.
	protect := hi-c < 5
	if !protect && hi-c < (hi-lo)/4 {
		// Lets test some points for equality to pivot
		dups := 0
		if !p.Less(pivot, hi-1) { // data[hi-1] = pivot
			p.Swap(c, hi-1)
			c++
			dups++
		}
		if !p.Less(b-1, pivot) { // data[b-1] = pivot
			b--
			dups++
		}
		// m-lo = (hi-lo)/2 > 6
		// b-lo > (hi-lo)*3/4-1 > 8
		// ==> m < b ==> data[m] <= pivot
		if !p.Less(m, pivot) { // data[m] = pivot
			p.Swap(m, b-1)
			b--
			dups++
		}
		// if at least 2 points are equal to pivot, assume skewed distribution
		protect = dups > 1
	}
	if protect {
		// Protect against a lot of duplicates
		// Add invariant:
		//	data[a <= i < b] unexamined
		//	data[b <= i < c] = pivot
		for {
			for ; a < b && !p.Less(b-1, pivot); b-- { // data[b] == pivot
			}
			for ; a < b && p.Less(a, pivot); a++ { // data[a] < pivot
			}
			if a >= b {
				break
			}
			// data[a] == pivot; data[b-1] < pivot
			p.Swap(a, b-1)
			a++
			b--
		}
	}
	// Swap pivot into middle
	p.Swap(pivot, b-1)
	return b - 1, c
}

func (p *sort_TYPE_DIR_HANDLES_NULLSOp) quickSort(a, b, maxDepth int) {
	for b-a > 12 { // Use ShellSort for slices <= 12 elements
		if maxDepth == 0 {
			p.heapSort(a, b)
			return
		}
		maxDepth--
		p.cancelChecker.Check()
		mlo, mhi := p.doPivot(a, b)
		// Avoiding recursion on the larger subproblem guarantees
		// a stack depth of at most lg(b-a).
		if mlo-a < b-mhi {
			p.quickSort(a, mlo, maxDepth)
			a = mhi // i.e., quickSort(data, mhi, b)
		} else {
			p.quickSort(mhi, b, maxDepth)
			b = mlo // i.e., quickSort(data, a, mlo)
		}
	}
	if b-a > 1 {
		// Do ShellSort pass with gap 6
		// It could be written in this simplified form cause b-a <= 12
		for i := a + 6; i < b; i++ {
			if p.Less(i, i-6) {
				p.Swap(i, i-6)
			}
		}
		p.insertionSort(a, b)
	}
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
