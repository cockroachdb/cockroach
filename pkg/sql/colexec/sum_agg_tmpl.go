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
// This file is the execgen template for sum_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
)

// {{/*
// Declarations to make the template compile properly

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _, _, _, _ string) {
	colexecerror.InternalError("")
}

// */}}

// {{range .}}

type sum_KIND_TYPEAgg struct {
	groups  []bool
	scratch struct {
		curIdx int
		// curAgg holds the running total, so we can index into the slice once per
		// group, instead of on each iteration.
		curAgg _RET_GOTYPE
		// vec points to the output vector we are updating.
		vec []_RET_GOTYPE
		// nulls points to the output null vector that we are updating.
		nulls *coldata.Nulls
		// foundNonNullForCurrentGroup tracks if we have seen any non-null values
		// for the group that is currently being aggregated.
		foundNonNullForCurrentGroup bool
	}
	// {{if .NeedsHelper}}
	// {{/*
	// overloadHelper is used only when we perform the summation of integers
	// and get a decimal result which is the case when {{if .NeedsHelper}}
	// evaluates to true. In all other cases we don't want to wastefully
	// allocate the helper.
	// */}}
	overloadHelper overloadHelper
	// {{end}}
}

var _ aggregateFunc = &sum_KIND_TYPEAgg{}

const sizeOfSum_KIND_TYPEAgg = int64(unsafe.Sizeof(sum_KIND_TYPEAgg{}))

func (a *sum_KIND_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.scratch.vec = v._RET_TYPE()
	a.scratch.nulls = v.Nulls()
	a.Reset()
}

func (a *sum_KIND_TYPEAgg) Reset() {
	a.scratch.curIdx = -1
	a.scratch.foundNonNullForCurrentGroup = false
	a.scratch.nulls.UnsetNulls()
}

func (a *sum_KIND_TYPEAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *sum_KIND_TYPEAgg) SetOutputIndex(idx int) {
	a.scratch.curIdx = idx
}

func (a *sum_KIND_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	// {{if .NeedsHelper}}
	// {{/*
	// overloadHelper is used only when we perform the summation of integers
	// and get a decimal result which is the case when {{if .NeedsHelper}}
	// evaluates to true. In all other cases we don't want to wastefully
	// allocate the helper.
	// */}}
	// In order to inline the templated code of overloads, we need to have a
	// "_overloadHelper" local variable of type "overloadHelper".
	_overloadHelper := a.overloadHelper
	// {{end}}
	inputLen := b.Length()
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec.TemplateType(), vec.Nulls()
	if nulls.MaybeHasNulls() {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, nulls, i, true)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, nulls, i, true)
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, nulls, i, false)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, nulls, i, false)
			}
		}
	}
}

func (a *sum_KIND_TYPEAgg) Flush() {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// null.
	if !a.scratch.foundNonNullForCurrentGroup {
		a.scratch.nulls.SetNull(a.scratch.curIdx)
	} else {
		a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
	}
	a.scratch.curIdx++
}

func (a *sum_KIND_TYPEAgg) HandleEmptyInputScalar() {
	a.scratch.nulls.SetNull(0)
}

type sum_KIND_TYPEAggAlloc struct {
	aggAllocBase
	aggFuncs []sum_KIND_TYPEAgg
}

var _ aggregateFuncAlloc = &sum_KIND_TYPEAggAlloc{}

func (a *sum_KIND_TYPEAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOfSum_KIND_TYPEAgg * a.allocSize)
		a.aggFuncs = make([]sum_KIND_TYPEAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_SUM adds the value of the ith row to the output for the current
// group. If this is the first row of a new group, and no non-nulls have been
// found for the current group, then the output for the current group is set to
// null.
func _ACCUMULATE_SUM(a *sum_KIND_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}

	// {{define "accumulateSum"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If
		// a.scratch.curIdx is negative, it means that this is the first group.
		if a.scratch.curIdx >= 0 {
			if !a.scratch.foundNonNullForCurrentGroup {
				a.scratch.nulls.SetNull(a.scratch.curIdx)
			} else {
				a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
			}
		}
		a.scratch.curIdx++
		// {{with .Global}}
		a.scratch.curAgg = zero_RET_TYPEValue
		// {{end}}

		// {{/*
		// We only need to reset this flag if there are nulls. If there are no
		// nulls, this will be updated unconditionally below.
		// */}}
		// {{if .HasNulls}}
		a.scratch.foundNonNullForCurrentGroup = false
		// {{end}}
	}
	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !isNull {
		_ASSIGN_ADD(a.scratch.curAgg, a.scratch.curAgg, col[i], _, _, col)
		a.scratch.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
