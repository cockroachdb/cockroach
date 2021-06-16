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

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v2"
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
	_ coldataext.Datum
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

// */}}

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
	// {{end}}
	// col points to the output vector we are updating.
	col _GOTYPESLICE
	// {{if eq "_AGGKIND" "Hash"}}
	hashAggregateFuncBase
	// {{end}}
	// curAgg holds the running min/max, so we can index into the slice once per
	// group, instead of on each iteration.
	// NOTE: if foundNonNullForCurrentGroup is false, curAgg is undefined.
	curAgg _GOTYPE
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

var _ AggregateFunc = &_AGG_TYPE_AGGKINDAgg{}

func (a *_AGG_TYPE_AGGKINDAgg) SetOutput(vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.SetOutput(vec)
	// {{else}}
	a.hashAggregateFuncBase.SetOutput(vec)
	// {{end}}
	a.col = vec._TYPE()
}

func (a *_AGG_TYPE_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec._TYPE(), vec.Nulls()
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
					_ACCUMULATE_MINMAX(a, nulls, i, true, false)
				}
			} else {
				for i := 0; i < inputLen; i++ {
					_ACCUMULATE_MINMAX(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[:inputLen]
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
	execgen.SETVARIABLESIZE(newCurAggSize, a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
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
	// {{end}}
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		a.col.Set(outputIdx, a.curAgg)
	}
	// {{if or (.IsBytesLike) (eq .VecMethod "Datum")}}
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	// Release the reference to curAgg eagerly.
	a.allocator.AdjustMemoryUsage(-int64(oldCurAggSize))
	a.curAgg = nil
	// {{end}}
}

func (a *_AGG_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.foundNonNullForCurrentGroup = false
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
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				a.col.Set(a.curIdx, a.curAgg)
			}
			a.curIdx++
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
		if !a.foundNonNullForCurrentGroup {
			// {{if and (.Sliceable) (not .HasSel)}}
			//gcassert:bce
			// {{end}}
			val := col.Get(i)
			// {{with .Global}}
			execgen.COPYVAL(a.curAgg, val)
			// {{end}}
			a.foundNonNullForCurrentGroup = true
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
	}
	// {{end}}

	// {{/*
} // */}}
