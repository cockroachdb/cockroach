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

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
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
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _, _, _, _ string) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
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
	orderedAggregateFuncBase
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	scratch struct {
		// curSum keeps track of the sum of elements belonging to the current group,
		// so we can index into the slice once per group, instead of on each
		// iteration.
		curSum _RET_GOTYPE
		// curCount keeps track of the number of elements that we've seen
		// belonging to the current group.
		curCount int64
		// vec points to the output vector.
		vec []_RET_GOTYPE
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
	overloadHelper execgen.OverloadHelper
	// {{end}}
}

var _ AggregateFunc = &avg_TYPE_AGGKINDAgg{}

func (a *avg_TYPE_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Init(groups, vec)
	// {{else}}
	a.hashAggregateFuncBase.Init(groups, vec)
	// {{end}}
	a.scratch.vec = vec._RET_TYPE()
	a.Reset()
}

func (a *avg_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{else}}
	a.hashAggregateFuncBase.Reset()
	// {{end}}
	a.scratch.curSum = zero_RET_TYPEValue
	a.scratch.curCount = 0
	a.scratch.foundNonNullForCurrentGroup = false
}

func (a *avg_TYPE_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
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
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.TemplateType(), vec.Nulls()
	// {{if eq "_AGGKIND" "Ordered"}}
	groups := a.groups
	// {{/*
	// We don't need to check whether sel is non-nil when performing
	// hash aggregation because the hash aggregator always uses non-nil
	// sel to specify the tuples to be aggregated.
	// */}}
	if sel == nil {
		_ = groups[inputLen-1]
		col = col[:inputLen]
		if nulls.MaybeHasNulls() {
			for i := range col {
				_ACCUMULATE_AVG(a, nulls, i, true)
			}
		} else {
			for i := range col {
				_ACCUMULATE_AVG(a, nulls, i, false)
			}
		}
	} else
	// {{end}}
	{
		sel = sel[:inputLen]
		if nulls.MaybeHasNulls() {
			for _, i := range sel {
				_ACCUMULATE_AVG(a, nulls, i, true)
			}
		} else {
			for _, i := range sel {
				_ACCUMULATE_AVG(a, nulls, i, false)
			}
		}
	}
}

func (a *avg_TYPE_AGGKINDAgg) Flush(outputIdx int) {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should be
	// NULL.
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	// {{end}}
	if !a.scratch.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		_ASSIGN_DIV_INT64(a.scratch.vec[outputIdx], a.scratch.curSum, a.scratch.curCount, a.scratch.vec, _, _)
	}
}

type avg_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []avg_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &avg_TYPE_AGGKINDAggAlloc{}

const sizeOfAvg_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(avg_TYPE_AGGKINDAgg{}))
const avg_TYPE_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]avg_TYPE_AGGKINDAgg{}))

func (a *avg_TYPE_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(avg_TYPE_AGGKINDAggSliceOverhead + sizeOfAvg_TYPE_AGGKINDAgg*a.allocSize)
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
	if groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null.
		if !a.scratch.foundNonNullForCurrentGroup {
			a.nulls.SetNull(a.curIdx)
		} else {
			// {{with .Global}}
			_ASSIGN_DIV_INT64(a.scratch.vec[a.curIdx], a.scratch.curSum, a.scratch.curCount, a.scratch.vec, _, _)
			// {{end}}
		}
		a.curIdx++
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
