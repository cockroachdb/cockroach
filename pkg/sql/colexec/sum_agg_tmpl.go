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
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*
// Declarations to make the template compile properly

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _, _, _, _ string) {
	colexecerror.InternalError("")
}

// */}}

func newSum_SUMKIND_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch t.Family() {
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return &sum_SUMKINDInt16_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
		case 32:
			return &sum_SUMKINDInt32_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
		default:
			return &sum_SUMKINDInt64_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
		}
	// {{if eq .SumKind ""}}
	case types.DecimalFamily:
		return &sumDecimal_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
	case types.FloatFamily:
		return &sumFloat64_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
	case types.IntervalFamily:
		return &sumInterval_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sum %s agg type %s", strings.ToLower("_SUMKIND"), t.Name())
	}
}

// {{range .Infos}}

type sum_SUMKIND_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	groups []bool
	// {{end}}
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

var _ aggregateFunc = &sum_SUMKIND_TYPE_AGGKINDAgg{}

const sizeOfSum_SUMKIND_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(sum_SUMKIND_TYPE_AGGKINDAgg{}))

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Init(groups []bool, v coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.groups = groups
	// {{end}}
	a.scratch.vec = v._RET_TYPE()
	a.scratch.nulls = v.Nulls()
	a.Reset()
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Reset() {
	a.scratch.curIdx = 0
	a.scratch.foundNonNullForCurrentGroup = false
	a.scratch.nulls.UnsetNulls()
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) SetOutputIndex(idx int) {
	a.scratch.curIdx = idx
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
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

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Flush() {
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

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) HandleEmptyInputScalar() {
	a.scratch.nulls.SetNull(0)
}

type sum_SUMKIND_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []sum_SUMKIND_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &sum_SUMKIND_TYPE_AGGKINDAggAlloc{}

func (a *sum_SUMKIND_TYPE_AGGKINDAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOfSum_SUMKIND_TYPE_AGGKINDAgg * a.allocSize)
		a.aggFuncs = make([]sum_SUMKIND_TYPE_AGGKINDAgg, a.allocSize)
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
func _ACCUMULATE_SUM(a *sum_SUMKIND_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "accumulateSum"}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null.
		if !a.scratch.foundNonNullForCurrentGroup {
			a.scratch.nulls.SetNull(a.scratch.curIdx)
		} else {
			a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
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
	// {{end}}

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
