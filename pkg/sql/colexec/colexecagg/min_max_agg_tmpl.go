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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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

func newMin_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) aggregateFuncAlloc {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	case types.BoolFamily:
		return &minBool_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.BytesFamily:
		return &minBytes_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.DecimalFamily:
		return &minDecimal_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return &minInt16_AGGKINDAggAlloc{aggAllocBase: allocBase}
		case 32:
			return &minInt32_AGGKINDAggAlloc{aggAllocBase: allocBase}
		default:
			return &minInt64_AGGKINDAggAlloc{aggAllocBase: allocBase}
		}
	case types.FloatFamily:
		return &minFloat64_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.TimestampTZFamily:
		return &minTimestamp_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.IntervalFamily:
		return &minInterval_AGGKINDAggAlloc{aggAllocBase: allocBase}
	default:
		return &minDatum_AGGKINDAggAlloc{aggAllocBase: allocBase}
	}
}

func newMax_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) aggregateFuncAlloc {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	case types.BoolFamily:
		return &maxBool_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.BytesFamily:
		return &maxBytes_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.DecimalFamily:
		return &maxDecimal_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return &maxInt16_AGGKINDAggAlloc{aggAllocBase: allocBase}
		case 32:
			return &maxInt32_AGGKINDAggAlloc{aggAllocBase: allocBase}
		default:
			return &maxInt64_AGGKINDAggAlloc{aggAllocBase: allocBase}
		}
	case types.FloatFamily:
		return &maxFloat64_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.TimestampTZFamily:
		return &maxTimestamp_AGGKINDAggAlloc{aggAllocBase: allocBase}
	case types.IntervalFamily:
		return &maxInterval_AGGKINDAggAlloc{aggAllocBase: allocBase}
	default:
		return &maxDatum_AGGKINDAggAlloc{aggAllocBase: allocBase}
	}
}

// {{range .}}
// {{$agg := .Agg}}
// {{range .Overloads}}
// {{range .WidthOverloads}}

type _AGG_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	allocator *colmem.Allocator
	// curAgg holds the running min/max, so we can index into the slice once per
	// group, instead of on each iteration.
	// NOTE: if foundNonNullForCurrentGroup is false, curAgg is undefined.
	curAgg _GOTYPE
	// col points to the output vector we are updating.
	col _GOTYPESLICE
	// vec is the same as col before conversion from coldata.Vec.
	vec coldata.Vec
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

var _ AggregateFunc = &_AGG_TYPE_AGGKINDAgg{}

func (a *_AGG_TYPE_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Init(groups, vec)
	// {{else}}
	a.hashAggregateFuncBase.Init(groups, vec)
	// {{end}}
	a.vec = vec
	a.col = vec._TYPE()
	a.Reset()
}

func (a *_AGG_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{else}}
	a.hashAggregateFuncBase.Reset()
	// {{end}}
	a.foundNonNullForCurrentGroup = false
}

func (a *_AGG_TYPE_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	// {{if eq .VecMethod "Bytes"}}
	oldCurAggSize := len(a.curAgg)
	// {{end}}
	// {{if eq .VecMethod "Datum"}}
	var oldCurAggSize uintptr
	if a.curAgg != nil {
		oldCurAggSize = a.curAgg.(*coldataext.Datum).Size()
	}
	// {{end}}
	vec := vecs[inputIdxs[0]]
	col, nulls := vec._TYPE(), vec.Nulls()
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			// {{if eq "_AGGKIND" "Ordered"}}
			groups := a.groups
			// {{/*
			// We don't need to check whether sel is non-nil when performing
			// hash aggregation because the hash aggregator always uses non-nil
			// sel to specify the tuples to be aggregated.
			// */}}
			if sel == nil {
				_ = groups[inputLen-1]
				col = execgen.SLICE(col, 0, inputLen)
				if nulls.MaybeHasNulls() {
					for i := 0; i < inputLen; i++ {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				} else {
					for i := 0; i < inputLen; i++ {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				}
			} else
			// {{end}}
			{
				sel = sel[:inputLen]
				if nulls.MaybeHasNulls() {
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				} else {
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				}
			}
		},
	)
	// {{if eq .VecMethod "Bytes"}}
	newCurAggSize := len(a.curAgg)
	// {{end}}
	// {{if eq .VecMethod "Datum"}}
	var newCurAggSize uintptr
	if a.curAgg != nil {
		newCurAggSize = a.curAgg.(*coldataext.Datum).Size()
	}
	// {{end}}
	// {{if or (eq .VecMethod "Bytes") (eq .VecMethod "Datum")}}
	a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	// {{end}}
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
		execgen.SET(a.col, outputIdx, a.curAgg)
	}
	// {{if or (eq .VecMethod "Bytes") (eq .VecMethod "Datum")}}
	// Release the reference to curAgg eagerly.
	// {{if eq .VecMethod "Bytes"}}
	a.allocator.AdjustMemoryUsage(-int64(len(a.curAgg)))
	// {{else}}
	if d, ok := a.curAgg.(*coldataext.Datum); ok {
		a.allocator.AdjustMemoryUsage(-int64(d.Size()))
	}
	// {{end}}
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

// {{end}}
// {{end}}
// {{end}}

// {{/*
// _ACCUMULATE_MINMAX sets the output for the current group to be the value of
// the ith row if it is smaller/larger than the current result. If this is the
// first row of a new group, and no non-nulls have been found for the current
// group, then the output for the current group is set to null.
func _ACCUMULATE_MINMAX(a *_AGG_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "accumulateMinMax"}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null.
		if !a.foundNonNullForCurrentGroup {
			a.nulls.SetNull(a.curIdx)
		} else {
			// {{with .Global}}
			execgen.SET(a.col, a.curIdx, a.curAgg)
			// {{end}}
		}
		a.curIdx++
		a.foundNonNullForCurrentGroup = false
	}
	// {{end}}

	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	// {{with .Global}}
	if !isNull {
		if !a.foundNonNullForCurrentGroup {
			val := col.Get(i)
			execgen.COPYVAL(a.curAgg, val)
			a.foundNonNullForCurrentGroup = true
		} else {
			var cmp bool
			candidate := col.Get(i)
			_ASSIGN_CMP(cmp, candidate, a.curAgg, _, col, _)
			if cmp {
				execgen.COPYVAL(a.curAgg, candidate)
			}
		}
	}
	// {{end}}
	// {{end}}

	// {{/*
} // */}}
