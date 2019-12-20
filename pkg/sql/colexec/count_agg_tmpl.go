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

import "github.com/cockroachdb/cockroach/pkg/col/coldata"

// newCountRowAgg creates a COUNT(*) aggregate, which counts every row in the
// result unconditionally.
func newCountRowAgg() *countAgg {
	return &countAgg{countRow: true}
}

// newCountAgg creates a COUNT(col) aggregate, which counts every row in the
// result where the value of col is not null.
func newCountAgg() *countAgg {
	return &countAgg{countRow: false}
}

// countAgg supports both the COUNT(*) and COUNT(col) aggregates, which are
// distinguished by the countRow flag.
type countAgg struct {
	groups   []bool
	vec      []int64
	nulls    *coldata.Nulls
	curIdx   int
	done     bool
	countRow bool
}

func (a *countAgg) Init(groups []bool, vec coldata.Vec) {
	a.groups = groups
	a.vec = vec.Int64()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *countAgg) Reset() {
	a.curIdx = -1
	a.nulls.UnsetNulls()
	a.done = false
}

func (a *countAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *countAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(uint16(idx + 1))
	}
}

func (a *countAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		a.curIdx++
		a.done = true
		return
	}

	sel := b.Selection()

	// If this is a COUNT(col) aggregator and there are nulls in this batch,
	// we must check each value for nullity. Note that it is only legal to do a
	// COUNT aggregate on a single column.
	if !a.countRow && b.ColVec(int(inputIdxs[0])).MaybeHasNulls() {
		nulls := b.ColVec(int(inputIdxs[0])).Nulls()
		if sel != nil {
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		} else {
			for i := range a.groups[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		}
	} else {
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

// {{/*
// _ACCUMULATE_COUNT aggregates the value at index i into the count aggregate.
// _COL_WITH_NULLS indicates whether we have COUNT aggregate (i.e. not
// COUNT_ROWS) and there maybe NULLs.
func _ACCUMULATE_COUNT(a *countAgg, nulls *coldata.Nulls, i int, _COL_WITH_NULLS bool) { // */}}
	// {{define "accumulateCount" -}}

	if a.groups[i] {
		a.curIdx++
		a.vec[a.curIdx] = 0
	}
	var y int64
	// {{if .ColWithNulls}}
	y = int64(0)
	if !nulls.NullAt(uint16(i)) {
		y = 1
	}
	// {{else}}
	y = int64(1)
	// {{end}}
	a.vec[a.curIdx] += y
	// {{end}}

	// {{/*
} // */}}

func (a *countAgg) HandleEmptyInputScalar() {
	a.vec[0] = 0
}
