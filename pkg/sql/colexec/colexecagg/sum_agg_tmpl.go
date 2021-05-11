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

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ tree.AggType
	_ apd.Context
	_ duration.Duration
)

// {{/*
// Declarations to make the template compile properly

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _, _, _, _ string) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

func newSum_SUMKIND_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch t.Family() {
	// {{range .Infos}}
	case _TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			// {{with .Overload}}
			return &sum_SUMKIND_TYPE_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
			// {{end}}
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported sum agg type %s", t.Name())
}

// {{range .Infos}}
// {{range .WidthOverloads}}
// {{with .Overload}}

type sum_SUMKIND_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	// curAgg holds the running total, so we can index into the slice once per
	// group, instead of on each iteration.
	curAgg _RET_GOTYPE
	// col points to the output vector we are updating.
	col []_RET_GOTYPE
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
	// {{if .NeedsHelper}}
	// {{/*
	// overloadHelper is used only when we perform the summation of integers
	// and get a decimal result which is the case when {{if .NeedsHelper}}
	// evaluates to true. In all other cases we don't want to wastefully
	// allocate the helper.
	// */}}
	overloadHelper execgen.OverloadHelper
	// {{end}}
}

var _ AggregateFunc = &sum_SUMKIND_TYPE_AGGKINDAgg{}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) SetOutput(vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.SetOutput(vec)
	// {{else}}
	a.hashAggregateFuncBase.SetOutput(vec)
	// {{end}}
	a.col = vec._RET_TYPE()
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
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
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.TemplateType(), vec.Nulls()
	a.allocator.PerformOperation([]coldata.Vec{a.vec}, func() {
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
			_ = groups[inputLen-1]
			_ = col.Get(inputLen - 1)
			if nulls.MaybeHasNulls() {
				for i := 0; i < inputLen; i++ {
					_ACCUMULATE_SUM(a, nulls, i, true, false)
				}
			} else {
				for i := 0; i < inputLen; i++ {
					_ACCUMULATE_SUM(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[:inputLen]
			if nulls.MaybeHasNulls() {
				for _, i := range sel {
					_ACCUMULATE_SUM(a, nulls, i, true, true)
				}
			} else {
				for _, i := range sel {
					_ACCUMULATE_SUM(a, nulls, i, false, true)
				}
			}
		}
	},
	)
	execgen.SETVARIABLESIZE(newCurAggSize, a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Flush(outputIdx int) {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// null.
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	// {{end}}
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		a.col[outputIdx] = a.curAgg
	}
}

func (a *sum_SUMKIND_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.curAgg = zero_RET_TYPEValue
	a.foundNonNullForCurrentGroup = false
}

type sum_SUMKIND_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []sum_SUMKIND_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &sum_SUMKIND_TYPE_AGGKINDAggAlloc{}

const sizeOfSum_SUMKIND_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(sum_SUMKIND_TYPE_AGGKINDAgg{}))
const sum_SUMKIND_TYPE_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]sum_SUMKIND_TYPE_AGGKINDAgg{}))

func (a *sum_SUMKIND_TYPE_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sum_SUMKIND_TYPE_AGGKINDAggSliceOverhead + sizeOfSum_SUMKIND_TYPE_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]sum_SUMKIND_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}
// {{end}}
// {{end}}

// {{/*
// _ACCUMULATE_SUM adds the value of the ith row to the output for the current
// group. If this is the first row of a new group, and no non-nulls have been
// found for the current group, then the output for the current group is set to
// null.
func _ACCUMULATE_SUM(
	a *sum_SUMKIND_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool, _HAS_SEL bool,
) { // */}}
	// {{define "accumulateSum"}}

	// {{if eq "_AGGKIND" "Ordered"}}
	// {{if not .HasSel}}
	//gcassert:bce
	// {{end}}
	if groups[i] {
		if !a.isFirstGroup {
			// If we encounter a new group, and we haven't found any non-nulls for the
			// current group, the output for this group should be null.
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				a.col[a.curIdx] = a.curAgg
			}
			a.curIdx++
			// {{with .Global}}
			a.curAgg = zero_RET_TYPEValue
			// {{end}}

			// {{/*
			// We only need to reset this flag if there are nulls. If there are no
			// nulls, this will be updated unconditionally below.
			// */}}
			// {{if .HasNulls}}
			a.foundNonNullForCurrentGroup = false
			// {{end}}
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
		v := col.Get(i)
		_ASSIGN_ADD(a.curAgg, a.curAgg, v, _, _, col)
		a.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
