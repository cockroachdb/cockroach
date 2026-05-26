// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for concat_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

func newConcat_AGGKINDAggAlloc(allocator *colmem.Allocator, allocSize int64) aggregateFuncAlloc {
	return &concat_AGGKINDAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type concat_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// col points to the output vector we are updating.
	col *coldata.Bytes
	// {{else}}
	unorderedAggregateFuncBase
	// {{end}}
	// curAgg holds the running total.
	curAgg []byte
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

// {{if eq "_AGGKIND" "Ordered"}}
func (a *concat_AGGKINDAgg) SetOutput(vec *coldata.Vec) {
	a.orderedAggregateFuncBase.SetOutput(vec)
	a.col = vec.Bytes()
}

// {{end}}

func (a *concat_AGGKINDAgg) Compute(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bytes(), vec.Nulls()
	// {{if not (eq "_AGGKIND" "Window")}}
	a.allocator.PerformOperation([]*coldata.Vec{a.vec}, func() {
		// {{if eq "_AGGKIND" "Ordered"}}
		// Capture groups to force bounds check to work. See
		// https://github.com/golang/go/issues/39756
		groups := a.groups
		// {{/*
		// We don't need to check whether sel is non-nil when performing
		// hash aggregation because the hash aggregator always uses non-nil
		// sel to specify the tuples to be aggregated.
		// */}}
		if sel == nil {
			_, _ = groups[endIdx-1], groups[startIdx]
			if nulls.MaybeHasNulls() {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_CONCAT(a, nulls, i, true, false)
				}
			} else {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_CONCAT(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[startIdx:endIdx]
			if nulls.MaybeHasNulls() {
				for _, i := range sel {
					_ACCUMULATE_CONCAT(a, nulls, i, true, true)
				}
			} else {
				for _, i := range sel {
					_ACCUMULATE_CONCAT(a, nulls, i, false, true)
				}
			}
		}
	},
	)
	// {{else}}
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_CONCAT(a, nulls, i, true, false)
		}
	} else {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_CONCAT(a, nulls, i, false, false)
		}
	}
	// {{end}}
	execgen.SETVARIABLESIZE(newCurAggSize, a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsageAfterAllocation(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *concat_AGGKINDAgg) Flush(outputIdx int) {
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	col := a.col
	// {{else}}
	col := a.vec.Bytes()
	// {{end}}
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		col.Set(outputIdx, a.curAgg)
	}
	// Release the reference to curAgg eagerly.
	a.allocator.AdjustMemoryUsage(-int64(len(a.curAgg)))
	a.curAgg = nil
}

func (a *concat_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.curAgg = nil
	a.foundNonNullForCurrentGroup = false
}

type concat_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []concat_AGGKINDAgg
}

var _ aggregateFuncAlloc = &concat_AGGKINDAggAlloc{}

const sizeOfConcat_AGGKINDAgg = int64(unsafe.Sizeof(concat_AGGKINDAgg{}))
const concat_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]concat_AGGKINDAgg{}))

func (a *concat_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(concat_AGGKINDAggSliceOverhead + sizeOfConcat_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]concat_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{/*
func _ACCUMULATE_CONCAT(
	a *concat_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool, _HAS_SEL bool,
) { // */}}
	// {{define "accumulateConcat"}}
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
			a.curAgg = zeroBytesValue

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
		a.curAgg = append(a.curAgg, col.Get(i)...)
		a.foundNonNullForCurrentGroup = true
	}
	// {{end}}
	// {{/*
} // */}}
