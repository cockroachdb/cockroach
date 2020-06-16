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
// This file is the execgen template for avg_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*
// Declarations to make the template compile properly

// _ASSIGN_DIV_INT64 is the template division function for assigning the first
// input to the result of the second input / the third input, where the third
// input is an int64.
func _ASSIGN_DIV_INT64(_, _, _, _, _, _ string) {
	colexecerror.InternalError("")
}

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _, _, _, _ string) {
	colexecerror.InternalError("")
}

// */}}

func newAvg_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch t.Family() {
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return &avgInt16_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
		case 32:
			return &avgInt32_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
		default:
			return &avgInt64_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
		}
	case types.DecimalFamily:
		return &avgDecimal_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
	case types.FloatFamily:
		return &avgFloat64_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
	case types.IntervalFamily:
		return &avgInterval_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
	default:
		return nil, errors.Errorf("unsupported avg agg type %s", t.Name())
	}
}

// {{range .}}

type avg_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	groups []bool
	// {{end}}
	scratch struct {
		curIdx int
		// curSum keeps track of the sum of elements belonging to the current group,
		// so we can index into the slice once per group, instead of on each
		// iteration.
		curSum _RET_GOTYPE
		// curCount keeps track of the number of elements that we've seen
		// belonging to the current group.
		curCount int64
		// vec points to the output vector.
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
	// and get a decimal result which is the case when NeedsHelper is true. In
	// all other cases we don't want to wastefully allocate the helper.
	// */}}
	overloadHelper overloadHelper
	// {{end}}
}

var _ aggregateFunc = &avg_TYPE_AGGKINDAgg{}

const sizeOfAvg_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(avg_TYPE_AGGKINDAgg{}))

func (a *avg_TYPE_AGGKINDAgg) Init(groups []bool, v coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.groups = groups
	// {{end}}
	a.scratch.vec = v._RET_TYPE()
	a.scratch.nulls = v.Nulls()
	a.Reset()
}

func (a *avg_TYPE_AGGKINDAgg) Reset() {
	a.scratch.curIdx = 0
	a.scratch.curSum = zero_RET_TYPEValue
	a.scratch.curCount = 0
	a.scratch.foundNonNullForCurrentGroup = false
	a.scratch.nulls.UnsetNulls()
}

func (a *avg_TYPE_AGGKINDAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *avg_TYPE_AGGKINDAgg) SetOutputIndex(idx int) {
	a.scratch.curIdx = idx
}

func (a *avg_TYPE_AGGKINDAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	// {{if .NeedsHelper}}
	// {{/*
	// overloadHelper is used only when we perform the summation of integers
	// and get a decimal result which is the case when NeedsHelper is true. In
	// all other cases we don't want to wastefully allocate the helper.
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
				_ACCUMULATE_AVG(a, nulls, i, true)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_AVG(a, nulls, i, true)
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_AVG(a, nulls, i, false)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_AVG(a, nulls, i, false)
			}
		}
	}
}

func (a *avg_TYPE_AGGKINDAgg) Flush() {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// NULL.
	if !a.scratch.foundNonNullForCurrentGroup {
		a.scratch.nulls.SetNull(a.scratch.curIdx)
	} else {
		_ASSIGN_DIV_INT64(a.scratch.vec[a.scratch.curIdx], a.scratch.curSum, a.scratch.curCount, a.scratch.vec, _, _)
	}
	a.scratch.curIdx++
}

func (a *avg_TYPE_AGGKINDAgg) HandleEmptyInputScalar() {
	a.scratch.nulls.SetNull(0)
}

type avg_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []avg_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &avg_TYPE_AGGKINDAggAlloc{}

func (a *avg_TYPE_AGGKINDAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOfAvg_TYPE_AGGKINDAgg * a.allocSize)
		a.aggFuncs = make([]avg_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_AVG updates the total sum/count for current group using the value
// of the ith row. If this is the first row of a new group, then the average is
// computed for the current group. If no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _ACCUMULATE_AVG(a *_AGG_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "accumulateAvg"}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null.
		if !a.scratch.foundNonNullForCurrentGroup {
			a.scratch.nulls.SetNull(a.scratch.curIdx)
		} else {
			// {{with .Global}}
			_ASSIGN_DIV_INT64(a.scratch.vec[a.scratch.curIdx], a.scratch.curSum, a.scratch.curCount, a.scratch.vec, _, _)
			// {{end}}
		}
		a.scratch.curIdx++
		// {{with .Global}}
		a.scratch.curSum = zero_RET_TYPEValue
		// {{end}}
		a.scratch.curCount = 0

		// {{/*
		// We only need to reset this flag if there are nulls. If there are no
		// nulls, this will be updated unconditionally below.
		// */}}
		// {{if .HasNulls}}
		a.scratch.foundNonNullForCurrentGroup = false
		// {{end}}
	}
	// {{end}}

	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !isNull {
		_ASSIGN_ADD(a.scratch.curSum, a.scratch.curSum, col[i], _, _, col)
		a.scratch.curCount++
		a.scratch.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
