// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for min_max_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

// Remove unused warning.
var _ = colexecerror.InternalError

// {{/*
// Declarations to make the template compile properly.

// _ASSIGN_CMP is the template function for assigning true to the first input
// if the second input compares successfully to the third input. The comparison
// operator is tree.LT for MIN and is tree.GT for MAX.
func _ASSIGN_CMP(_, _, _, _, _, _ string) bool {
	colexecerror.InternalError("")
}

// _COPYVAL_MAYBE_CAST is the template function for copying the second argument
// into the first one, possibly performing a cast in the process.
func _COPYVAL_MAYBE_CAST(_, _ string) bool {
	colexecerror.InternalError("")
}

// */}}

// {{range .}}

type _AGG_TYPEAgg struct {
	allocator *colmem.Allocator
	groups    []bool
	curIdx    int
	// curAgg holds the running min/max, so we can index into the slice once per
	// group, instead of on each iteration.
	// NOTE: if foundNonNullForCurrentGroup is false, curAgg is undefined.
	curAgg _RET_GOTYPE
	// col points to the output vector we are updating.
	col _RET_GOTYPESLICE
	// vec is the same as col before conversion from coldata.Vec.
	vec coldata.Vec
	// nulls points to the output null vector that we are updating.
	nulls *coldata.Nulls
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

var _ aggregateFunc = &_AGG_TYPEAgg{}

const sizeOf_AGG_TYPEAgg = int64(unsafe.Sizeof(_AGG_TYPEAgg{}))

func (a *_AGG_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.vec = v
	a.col = v._RET_TYPE()
	a.nulls = v.Nulls()
	a.Reset()
}

func (a *_AGG_TYPEAgg) Reset() {
	a.curIdx = -1
	a.foundNonNullForCurrentGroup = false
	a.nulls.UnsetNulls()
}

func (a *_AGG_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *_AGG_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *_AGG_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	inputLen := b.Length()
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TYPE(), vec.Nulls()
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				}
			}
		},
	)
}

func (a *_AGG_TYPEAgg) Flush() {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should
	// be null.
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(a.curIdx)
	} else {
		execgen.SET(a.col, a.curIdx, a.curAgg)
	}
	a.curIdx++
}

func (a *_AGG_TYPEAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

type _AGG_TYPEAggAlloc struct {
	aggAllocBase
	aggFuncs []_AGG_TYPEAgg
}

var _ aggregateFuncAlloc = &_AGG_TYPEAggAlloc{}

func (a *_AGG_TYPEAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOf_AGG_TYPEAgg * a.allocSize)
		a.aggFuncs = make([]_AGG_TYPEAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_MINMAX sets the output for the current group to be the value of
// the ith row if it is smaller/larger than the current result. If this is the
// first row of a new group, and no non-nulls have been found for the current
// group, then the output for the current group is set to null.
func _ACCUMULATE_MINMAX(a *_AGG_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}

	// {{define "accumulateMinMax"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If a.curIdx is
		// negative, it means that this is the first group.
		if a.curIdx >= 0 {
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				// {{with .Global}}
				execgen.SET(a.col, a.curIdx, a.curAgg)
				// {{end}}
			}
		}
		a.curIdx++
		a.foundNonNullForCurrentGroup = false
	}
	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	// {{with .Global}}
	if !isNull {
		if !a.foundNonNullForCurrentGroup {
			val := execgen.UNSAFEGET(col, i)
			_COPYVAL_MAYBE_CAST(a.curAgg, val)
			a.foundNonNullForCurrentGroup = true
		} else {
			var cmp bool
			candidate := execgen.UNSAFEGET(col, i)
			_ASSIGN_CMP(cmp, candidate, a.curAgg, _, col, _)
			if cmp {
				_COPYVAL_MAYBE_CAST(a.curAgg, candidate)
			}
		}
	}
	// {{end}}
	// {{end}}

	// {{/*
} // */}}
