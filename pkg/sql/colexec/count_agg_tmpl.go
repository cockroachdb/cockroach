// Copyright 2018 The Cockroach Authors.
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
// This file is the execgen template for count_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

// {{range .}}

func newCount_KINDAgg(allocator *colmem.Allocator) *count_KINDAgg {
	allocator.AdjustMemoryUsage(int64(sizeOfCount_KINDAgg))
	return &count_KINDAgg{}
}

// count_KINDAgg supports either COUNT(*) or COUNT(col) aggregate.
type count_KINDAgg struct {
	groups []bool
	vec    []int64
	nulls  *coldata.Nulls
	curIdx int
	curAgg int64
}

var _ aggregateFunc = &count_KINDAgg{}

const sizeOfCount_KINDAgg = unsafe.Sizeof(&count_KINDAgg{})

func (a *count_KINDAgg) Init(groups []bool, vec coldata.Vec) {
	a.groups = groups
	a.vec = vec.Int64()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *count_KINDAgg) Reset() {
	a.curIdx = -1
	a.curAgg = 0
	a.nulls.UnsetNulls()
}

func (a *count_KINDAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *count_KINDAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *count_KINDAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	inputLen := b.Length()
	sel := b.Selection()

	// {{if not (eq .Kind "Rows")}}
	// If this is a COUNT(col) aggregator and there are nulls in this batch,
	// we must check each value for nullity. Note that it is only legal to do a
	// COUNT aggregate on a single column.
	nulls := b.ColVec(int(inputIdxs[0])).Nulls()
	if nulls.MaybeHasNulls() {
		if sel != nil {
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		} else {
			for i := range a.groups[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		}
	} else
	// {{end}}
	{
		if sel != nil {
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, false)
			}
		} else {
			for i := range a.groups[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, false)
			}
		}
	}
}

func (a *count_KINDAgg) Flush() {
	a.vec[a.curIdx] = a.curAgg
	a.curIdx++
}

func (a *count_KINDAgg) HandleEmptyInputScalar() {
	a.vec[0] = 0
}

// {{end}}

// {{/*
// _ACCUMULATE_COUNT aggregates the value at index i into the count aggregate.
// _COL_WITH_NULLS indicates whether we should be paying attention to NULLs.
func _ACCUMULATE_COUNT(a *countAgg, nulls *coldata.Nulls, i int, _COL_WITH_NULLS bool) { // */}}
	// {{define "accumulateCount" -}}

	if a.groups[i] {
		if a.curIdx != -1 {
			a.vec[a.curIdx] = a.curAgg
		}
		a.curIdx++
		a.curAgg = int64(0)
	}
	var y int64
	// {{if .ColWithNulls}}
	y = int64(0)
	if !nulls.NullAt(i) {
		y = 1
	}
	// {{else}}
	y = int64(1)
	// {{end}}
	a.curAgg += y
	// {{end}}

	// {{/*
} // */}}
