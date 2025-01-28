// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for min_max_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ tree.AggType
	_ apd.Context
	_ duration.Duration
	_ json.JSON
	_ = coldataext.CompareDatum
)

// Remove unused warning.
var _ = colexecerror.InternalError

// {{/*
// Declarations to make the template compile properly.

// _ASSIGN_CMP is the template function for assigning true to the first input
// if the second input compares successfully to the third input. The comparison
// operator is tree.LT for MIN and is tree.GT for MAX.
func _ASSIGN_CMP(_, _, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// _ALLOC_CODE is the template variable that is replaced in agg_gen_util.go by
// the template code for sharing allocator objects.
const _ALLOC_CODE = 0

// */}}

// {{if eq "_AGGKIND" "Ordered"}}

const minMaxNumOverloads = 11

// {{end}}

// {{range .}}
// {{with .Overloads}}

var _ = _ALLOC_CODE

// {{/*
//      The range loop is over an array of two items corresponding to min
//      and max functions, but we want to generate the code for sharing
//      allocators only once, so we break out of the loop.
// */}}

// {{break}}

// {{end}}
// {{end}}

// {{range .}}
// {{$agg := .Agg}}

func new_AGG_TITLE_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) aggregateFuncAlloc {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .Overloads}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &_AGG_TYPE_AGGKINDAggAlloc{aggAllocBase: allocBase}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find _AGG overload for %s type family", t.Name()))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// {{range .Overloads}}
// {{range .WidthOverloads}}

type _AGG_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// col points to the output vector we are updating.
	col _GOTYPESLICE
	// {{else}}
	unorderedAggregateFuncBase
	// {{end}}
	// curAgg holds the running min/max, so we can index into the slice once per
	// group, instead of on each iteration.
	// NOTE: if numNonNull is zero, curAgg is undefined.
	curAgg _GOTYPE
	// numNonNull tracks the number of non-null values we have seen for the group
	// that is currently being aggregated.
	numNonNull uint64
}

var _ AggregateFunc = &_AGG_TYPE_AGGKINDAgg{}

// {{if eq "_AGGKIND" "Ordered"}}
func (a *_AGG_TYPE_AGGKINDAgg) SetOutput(vec *coldata.Vec) {
	a.orderedAggregateFuncBase.SetOutput(vec)
	a.col = vec._TYPE()
}

// {{end}}

func (a *_AGG_TYPE_AGGKINDAgg) Compute(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec._TYPE(), vec.Nulls()
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
					_ACCUMULATE_MINMAX(a, nulls, i, true, false)
				}
			} else {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_MINMAX(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[startIdx:endIdx]
			if nulls.MaybeHasNulls() {
				for _, i := range sel {
					_ACCUMULATE_MINMAX(a, nulls, i, true, true)
				}
			} else {
				for _, i := range sel {
					_ACCUMULATE_MINMAX(a, nulls, i, false, true)
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
			_ACCUMULATE_MINMAX(a, nulls, i, true, false)
		}
	} else {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_MINMAX(a, nulls, i, false, false)
		}
	}
	// {{end}}
	execgen.SETVARIABLESIZE(newCurAggSize, a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *_AGG_TYPE_AGGKINDAgg) Flush(outputIdx int) {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should
	// be null.
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	col := a.col
	// {{else}}
	col := a.vec._TYPE()
	// {{end}}
	if a.numNonNull == 0 {
		a.nulls.SetNull(outputIdx)
	} else {
		col.Set(outputIdx, a.curAgg)
	}
	// {{if and (not (eq "_AGGKIND" "Window")) (or (.IsBytesLike) (eq .VecMethod "Datum"))}}
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	// Release the reference to curAgg eagerly. We can't do this for the window
	// variants because they may reuse curAgg between subsequent window frames.
	a.allocator.AdjustMemoryUsage(-int64(oldCurAggSize))
	a.curAgg = nil
	// {{end}}
}

func (a *_AGG_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.numNonNull = 0
	// {{if or (.IsBytesLike) (eq .VecMethod "Datum")}}
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	// Release the reference to curAgg.
	a.allocator.AdjustMemoryUsage(-int64(oldCurAggSize))
	a.curAgg = nil
	// {{end}}
}

type _AGG_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []_AGG_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &_AGG_TYPE_AGGKINDAggAlloc{}

const sizeOf_AGG_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(_AGG_TYPE_AGGKINDAgg{}))
const _AGG_TYPE_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]_AGG_TYPE_AGGKINDAgg{}))

func (a *_AGG_TYPE_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(_AGG_TYPE_AGGKINDAggSliceOverhead + sizeOf_AGG_TYPE_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]_AGG_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{if eq "_AGGKIND" "Window"}}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go). This allows min and max operators to be used when
// the window frame only grows. For the case when the window frame can shrink,
// a specialized implementation is needed (see min_max_removable_agg_tmpl.go).
func (*_AGG_TYPE_AGGKINDAgg) Remove(vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int) {
	colexecerror.InternalError(errors.AssertionFailedf("Remove called on _AGG_TYPE_AGGKINDAgg"))
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}

// {{/*
// _ACCUMULATE_MINMAX sets the output for the current group to be the value of
// the ith row if it is smaller/larger than the current result. If this is the
// first row of a new group, and no non-nulls have been found for the current
// group, then the output for the current group is set to null.
func _ACCUMULATE_MINMAX(
	a *_AGG_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool, _HAS_SEL bool,
) { // */}}
	// {{define "accumulateMinMax"}}

	// {{if eq "_AGGKIND" "Ordered"}}
	// {{if not .HasSel}}
	//gcassert:bce
	// {{end}}
	if groups[i] {
		if !a.isFirstGroup {
			// If we encounter a new group, and we haven't found any non-nulls for the
			// current group, the output for this group should be null.
			if a.numNonNull == 0 {
				a.nulls.SetNull(a.curIdx)
			} else {
				a.col.Set(a.curIdx, a.curAgg)
			}
			a.curIdx++
			a.numNonNull = 0
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
		if a.numNonNull == 0 {
			// {{if and (.Sliceable) (not .HasSel)}}
			//gcassert:bce
			// {{end}}
			val := col.Get(i)
			// {{with .Global}}
			execgen.COPYVAL(a.curAgg, val)
			// {{end}}
		} else {
			var cmp bool
			// {{if and (.Sliceable) (not .HasSel)}}
			//gcassert:bce
			// {{end}}
			candidate := col.Get(i)
			// {{with .Global}}
			_ASSIGN_CMP(cmp, candidate, a.curAgg, _, col, _)
			if cmp {
				execgen.COPYVAL(a.curAgg, candidate)
			}
			// {{end}}
		}
		a.numNonNull++
	}
	// {{end}}

	// {{/*
} // */}}
