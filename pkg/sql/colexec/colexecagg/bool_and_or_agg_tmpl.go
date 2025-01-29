// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for bool_and_or_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/errors"
)

// Remove unused warning.
var _ = colexecerror.InternalError

// {{/*

// _ASSIGN_BOOL_OP is the template boolean operation function for assigning the
// first input to the result of a boolean operation of the second and the third
// inputs.
func _ASSIGN_BOOL_OP(_, _, _ string) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// {{range .}}

func newBool_OP_TYPE_AGGKINDAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &bool_OP_TYPE_AGGKINDAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type bool_OP_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	col coldata.Bools
	// {{else}}
	unorderedAggregateFuncBase
	// {{end}}
	curAgg bool
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

var _ AggregateFunc = &bool_OP_TYPE_AGGKINDAgg{}

// {{if eq "_AGGKIND" "Ordered"}}
func (a *bool_OP_TYPE_AGGKINDAgg) SetOutput(vec *coldata.Vec) {
	a.orderedAggregateFuncBase.SetOutput(vec)
	a.col = vec.Bool()
}

// {{end}}

func (a *bool_OP_TYPE_AGGKINDAgg) Compute(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bool(), vec.Nulls()
	// {{if not (eq "_AGGKIND" "Window")}}
	a.allocator.PerformOperation([]*coldata.Vec{a.vec}, func() {
		// {{if eq "_AGGKIND" "Ordered"}}
		// Capture groups and col to force bounds check to work. See
		// https://github.com/golang/go/issues/39756
		groups := a.groups
		col := col
		// {{/*
		// We don't need to check whether sel is non-nil when performing
		// hash aggregation because the hash aggregator always uses non-nil
		// sel to specify the tuples to be aggregated.
		// */}}
		if sel == nil {
			_, _ = groups[endIdx-1], groups[startIdx]
			_, _ = col.Get(endIdx-1), col.Get(startIdx)
			if nulls.MaybeHasNulls() {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_BOOLEAN(a, nulls, i, true, false)
				}
			} else {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_BOOLEAN(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[startIdx:endIdx]
			if nulls.MaybeHasNulls() {
				for _, i := range sel {
					_ACCUMULATE_BOOLEAN(a, nulls, i, true, true)
				}
			} else {
				for _, i := range sel {
					_ACCUMULATE_BOOLEAN(a, nulls, i, false, true)
				}
			}
		}
	},
	)
	// {{else}}
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_BOOLEAN(a, nulls, i, true, false)
		}
	} else {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_BOOLEAN(a, nulls, i, false, false)
		}
	}
	// {{end}}
	execgen.SETVARIABLESIZE(newCurAggSize, a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *bool_OP_TYPE_AGGKINDAgg) Flush(outputIdx int) {
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	col := a.col
	// {{else}}
	col := a.vec.Bool()
	// {{end}}
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		col[outputIdx] = a.curAgg
	}
}

func (a *bool_OP_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	// {{/*
	// _DEFAULT_VAL indicates whether we are doing an AND aggregate or OR
	// aggregate. For bool_and the _DEFAULT_VAL is true and for bool_or the
	// _DEFAULT_VAL is false.
	// */}}
	a.curAgg = _DEFAULT_VAL
	a.foundNonNullForCurrentGroup = false
}

type bool_OP_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []bool_OP_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &bool_OP_TYPE_AGGKINDAggAlloc{}

const sizeOfBool_OP_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(bool_OP_TYPE_AGGKINDAgg{}))
const bool_OP_TYPE_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]bool_OP_TYPE_AGGKINDAgg{}))

func (a *bool_OP_TYPE_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(bool_OP_TYPE_AGGKINDAggSliceOverhead + sizeOfBool_OP_TYPE_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]bool_OP_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	f.Reset()
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{if eq "_AGGKIND" "Window"}}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go). This allows bool_and and bool_or operators to be
// used when the window frame only grows. For the case when the window frame can
// shrink, the default quadratic-scaling implementation is necessary.
func (*bool_OP_TYPE_AGGKINDAgg) Remove(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int,
) {
	colexecerror.InternalError(
		errors.AssertionFailedf("Remove called on bool_OP_TYPE_AGGKINDAgg"),
	)
}

// {{end}}
// {{end}}

// {{/*
// _ACCUMULATE_BOOLEAN aggregates the boolean value at index i into the boolean aggregate.
func _ACCUMULATE_BOOLEAN(
	a *bool_OP_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool, _HAS_SEL bool,
) { // */}}
	// {{define "accumulateBoolean" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	// {{if not .HasSel}}
	//gcassert:bce
	// {{end}}
	if groups[i] {
		if !a.isFirstGroup {
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				a.col[a.curIdx] = a.curAgg
			}
			a.curIdx++
			// {{with .Global}}
			a.curAgg = _DEFAULT_VAL
			// {{end}}
			a.foundNonNullForCurrentGroup = false
		}
		a.isFirstGroup = false
	}
	// {{end}}

	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !isNull {
		// {{if not .HasSel}}
		//gcassert:bce
		// {{end}}
		// {{with .Global}}
		_ASSIGN_BOOL_OP(a.curAgg, a.curAgg, col[i])
		// {{end}}
		a.foundNonNullForCurrentGroup = true
	}

	// {{end}}

	// {{/*
} // */}}
