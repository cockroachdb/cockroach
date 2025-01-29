// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for avg_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
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
	_ = typeconv.TypeFamilyToCanonicalTypeFamily
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

// _ASSIGN_SUBTRACT is the template subtraction function for assigning the first
// input to the result of the second input - the third input.
func _ASSIGN_SUBTRACT(_, _, _, _, _, _ string) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// _ALLOC_CODE is the template variable that is replaced in agg_gen_util.go by
// the template code for sharing allocator objects.
const _ALLOC_CODE = 0

// */}}

// {{if eq "_AGGKIND" "Ordered"}}

const avgNumOverloads = 6

// {{end}}

var _ = _ALLOC_CODE

func newAvg_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch t.Family() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			// {{with .Overload}}
			return &avg_TYPE_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
			// {{end}}
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.AssertionFailedf("unsupported avg agg type %s", t.Name())
}

// {{range .}}
// {{range .WidthOverloads}}
// {{with .Overload}}

type avg_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// col points to the statically-typed output vector.
	col _RET_GOTYPESLICE
	// {{else}}
	unorderedAggregateFuncBase
	// {{end}}
	// curSum keeps track of the sum of elements belonging to the current group,
	// so we can index into the slice once per group, instead of on each
	// iteration.
	curSum _RET_GOTYPE
	// curCount keeps track of the number of non-null elements that we've seen
	// belonging to the current group.
	curCount int64
}

var _ AggregateFunc = &avg_TYPE_AGGKINDAgg{}

// {{if eq "_AGGKIND" "Ordered"}}
func (a *avg_TYPE_AGGKINDAgg) SetOutput(vec *coldata.Vec) {
	a.orderedAggregateFuncBase.SetOutput(vec)
	a.col = vec._RET_TYPE()
}

// {{end}}

func (a *avg_TYPE_AGGKINDAgg) Compute(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	execgen.SETVARIABLESIZE(oldCurSumSize, a.curSum)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.TemplateType(), vec.Nulls()
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
					_ACCUMULATE_AVG(a, nulls, i, true, false)
				}
			} else {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_AVG(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[startIdx:endIdx]
			if nulls.MaybeHasNulls() {
				for _, i := range sel {
					_ACCUMULATE_AVG(a, nulls, i, true, true)
				}
			} else {
				for _, i := range sel {
					_ACCUMULATE_AVG(a, nulls, i, false, true)
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
			_ACCUMULATE_AVG(a, nulls, i, true, false)
		}
	} else {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_AVG(a, nulls, i, false, false)
		}
	}
	// {{end}}
	execgen.SETVARIABLESIZE(newCurSumSize, a.curSum)
	if newCurSumSize != oldCurSumSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurSumSize - oldCurSumSize))
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
	col := a.col
	// {{else}}
	col := a.vec._RET_TYPE()
	// {{end}}
	if a.curCount == 0 {
		a.nulls.SetNull(outputIdx)
	} else {
		_ASSIGN_DIV_INT64(col[outputIdx], a.curSum, a.curCount, a.col, _, _)
	}
}

func (a *avg_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.curSum = zero_RET_TYPEValue
	a.curCount = 0
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
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{if eq "_AGGKIND" "Window"}}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go).
func (a *avg_TYPE_AGGKINDAgg) Remove(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int,
) {
	execgen.SETVARIABLESIZE(oldCurSumSize, a.curSum)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.TemplateType(), vec.Nulls()
	_, _ = col.Get(endIdx-1), col.Get(startIdx)
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {
			_REMOVE_ROW(a, nulls, i, true)
		}
	} else {
		for i := startIdx; i < endIdx; i++ {
			_REMOVE_ROW(a, nulls, i, false)
		}
	}
	execgen.SETVARIABLESIZE(newCurSumSize, a.curSum)
	if newCurSumSize != oldCurSumSize {
		a.allocator.AdjustMemoryUsage(int64(newCurSumSize - oldCurSumSize))
	}
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}

// {{/*
// _ACCUMULATE_AVG updates the total sum/count for current group using the value
// of the ith row. If this is the first row of a new group, then the average is
// computed for the current group. If no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _ACCUMULATE_AVG(
	a *_AGG_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool, _HAS_SEL bool,
) { // */}}
	// {{define "accumulateAvg"}}

	// {{if eq "_AGGKIND" "Ordered"}}
	// {{if not .HasSel}}
	//gcassert:bce
	// {{end}}
	if groups[i] {
		if !a.isFirstGroup {
			// If we encounter a new group, and we haven't found any non-nulls for the
			// current group, the output for this group should be null.
			if a.curCount == 0 {
				a.nulls.SetNull(a.curIdx)
			} else {
				// {{with .Global}}
				_ASSIGN_DIV_INT64(a.col[a.curIdx], a.curSum, a.curCount, a.col, _, _)
				// {{end}}
			}
			a.curIdx++
			// {{with .Global}}
			a.curSum = zero_RET_TYPEValue
			// {{end}}
			a.curCount = 0
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
		_ASSIGN_ADD(a.curSum, a.curSum, v, _, _, col)
		a.curCount++
	}
	// {{end}}

	// {{/*
} // */}}

// {{/*
// _REMOVE_ROW removes the value of the ith row from the output for the
// current aggregation.
func _REMOVE_ROW(a *_AGG_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "removeRow"}}
	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !isNull {
		//gcassert:bce
		v := col.Get(i)
		_ASSIGN_SUBTRACT(a.curSum, a.curSum, v, _, _, col)
		a.curCount--
	}
	// {{end}}
	// {{/*
} // */}}
