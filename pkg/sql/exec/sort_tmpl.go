// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Copyright 2018 The Cockroach Authors.
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

// {{/*
// +build execgen_template
//
// This file is the execgen template for sort.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/apd"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the second input < the third input.
func _ASSIGN_LT(_, _, _ string) bool {
	panic("")
}

// */}}

type sortSpec struct {
	sortCol    uint32
	inputTypes []types.T
}

// sortState represents the state of the processor.
type sortState int

const (
	sortSpooling sortState = iota

	sortEmitting
)

func NewSorter(input Operator, inputTypes []types.T, sortCol uint32) (Operator, error) {
	t := inputTypes[sortCol]
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &sort_TYPEOp{
			input: input,
			spec: sortSpec{
				sortCol:    sortCol,
				inputTypes: inputTypes,
			},
		}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sort type %s", t)
	}
}

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

type sort_TYPEOp struct {
	spec sortSpec

	input Operator

	state sortState
	// nTuples is the number of tuples we've seen from our input.
	nTuples uint64
	// values stores all the values from the input
	values []ColVec
	order  []uint64

	sortCol []_GOTYPE

	// emitted is the index of the last value that was emitted by Next()
	emitted uint64
	output  ColBatch
}

var _ Operator = &sort_TYPEOp{}

func (p *sort_TYPEOp) Init() {
	p.input.Init()
	p.output = NewMemBatch(p.spec.inputTypes)
	p.values = make([]ColVec, len(p.spec.inputTypes))
	for i := 0; i < len(p.spec.inputTypes); i++ {
		p.values[i] = newMemColumn(p.spec.inputTypes[i], 0)
	}
}

func (p *sort_TYPEOp) Next() ColBatch {
	switch p.state {
	case sortSpooling:
		p.spool()
		p.state = sortEmitting
		fallthrough
	case sortEmitting:
		newEmitted := p.emitted + ColBatchSize
		if newEmitted > p.nTuples {
			newEmitted = p.nTuples
		}
		p.output.SetLength(uint16(newEmitted - p.emitted))
		if p.output.Length() == 0 {
			return p.output
		}

		sortColIdx := int(p.spec.sortCol)
		for j := 0; j < len(p.values); j++ {
			if j == sortColIdx {
				// the sortCol'th vec is already sorted, so just fill it directly.
				p.output.ColVec(j).Copy(p.values[j], p.emitted, newEmitted, p.spec.inputTypes[j])
			} else {
				p.output.ColVec(j).CopyWithSelInt64(p.values[j], p.order[p.emitted:], p.output.Length(), p.spec.inputTypes[j])
			}
		}
		p.emitted = newEmitted
		return p.output
	default:
		panic(fmt.Sprintf("invalid sorter state %d", p.state))
	}

}

func (p *sort_TYPEOp) spool() {
	batch := p.input.Next()
	for ; batch.Length() != 0; batch = p.input.Next() {
		// First, copy all vecs into values.
		for i := 0; i < len(p.values); i++ {
			if batch.Selection() == nil {
				p.values[i].Append(batch.ColVec(i),
					p.spec.inputTypes[i],
					p.nTuples,
					batch.Length(),
				)
			} else {
				p.values[i].AppendWithSel(batch.ColVec(i),
					batch.Selection(),
					batch.Length(),
					p.spec.inputTypes[i],
					p.nTuples,
				)
			}
		}
		p.nTuples += uint64(batch.Length())
	}

	p.order = make([]uint64, p.nTuples)
	for i := uint64(0); i < uint64(len(p.order)); i++ {
		p.order[i] = i
	}

	p.sortCol = p.values[p.spec.sortCol]._TemplateType()

	n := p.Len()
	p.quickSort(0, n, maxDepth(n))
}

func (s *sort_TYPEOp) Less(i, j int) bool {
	var lt bool
	_ASSIGN_LT("lt", "s.sortCol[i]", "s.sortCol[j]")
	return lt
}

func (s *sort_TYPEOp) Swap(i, j int) {
	// Swap needs to swap the values in the column being sorted, as otherwise
	// subsequent calls to Less would be incorrect.
	// We also store the swap order in s.order to swap all the other columns.
	s.sortCol[i], s.sortCol[j] = s.sortCol[j], s.sortCol[i]
	s.order[i], s.order[j] = s.order[j], s.order[i]
}

func (s *sort_TYPEOp) Len() int {
	return int(s.nTuples)
}

// Insertion sort
func (p *sort_TYPEOp) insertionSort(a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && p.Less(j, j-1); j-- {
			p.Swap(j, j-1)
		}
	}
}

// siftDown implements the heap property on data[lo, hi).
// first is an offset into the array where the root of the heap lies.
func (p *sort_TYPEOp) siftDown(lo, hi, first int) {
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

func (p *sort_TYPEOp) heapSort(a, b int) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
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
func (p *sort_TYPEOp) medianOfThree(m1, m0, m2 int) {
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

func (p *sort_TYPEOp) swapRange(a, b, n int) {
	for i := 0; i < n; i++ {
		p.Swap(a+i, b+i)
	}
}

func (p *sort_TYPEOp) doPivot(lo, hi int) (midlo, midhi int) {
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

func (p *sort_TYPEOp) quickSort(a, b, maxDepth int) {
	for b-a > 12 { // Use ShellSort for slices <= 12 elements
		if maxDepth == 0 {
			p.heapSort(a, b)
			return
		}
		maxDepth--
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
