// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for count_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

// {{range .}}

func newCount_COUNTKIND_AGGKINDAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &count_COUNTKIND_AGGKINDAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

// count_COUNTKIND_AGGKINDAgg supports either COUNT(*) or COUNT(col) aggregate.
type count_COUNTKIND_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	col coldata.Int64s
	// {{else}}
	unorderedAggregateFuncBase
	// {{end}}
	curAgg int64
}

var _ AggregateFunc = &count_COUNTKIND_AGGKINDAgg{}

// {{if eq "_AGGKIND" "Ordered"}}
func (a *count_COUNTKIND_AGGKINDAgg) SetOutput(vec *coldata.Vec) {
	a.orderedAggregateFuncBase.SetOutput(vec)
	a.col = vec.Int64()
}

// {{end}}

func (a *count_COUNTKIND_AGGKINDAgg) Compute(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	// {{if not (eq .CountKind "Rows")}}
	// If this is a COUNT(col) aggregator and there are nulls in this batch,
	// we must check each value for nullity. Note that it is only legal to do a
	// COUNT aggregate on a single column.
	nulls := vecs[inputIdxs[0]].Nulls()
	// {{end}}
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
			// {{if not (eq .CountKind "Rows")}}
			if nulls.MaybeHasNulls() {
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_COUNT(a, nulls, i, true, false)
				}
			} else
			// {{end}}
			{
				for i := startIdx; i < endIdx; i++ {
					_ACCUMULATE_COUNT(a, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			// {{if not (eq .CountKind "Rows")}}
			if nulls.MaybeHasNulls() {
				for _, i := range sel[startIdx:endIdx] {
					_ACCUMULATE_COUNT(a, nulls, i, true, true)
				}
			} else
			// {{end}}
			{
				// {{if eq "_AGGKIND" "Hash"}}
				// We don't need to pay attention to nulls (either because it's a
				// COUNT_ROWS aggregate or because there are no nulls), and we're
				// performing a hash aggregation (meaning there is a single group),
				// so all endIdx-startIdx tuples contribute to the count.
				a.curAgg += int64(endIdx - startIdx)
				// {{else}}
				for _, i := range sel[startIdx:endIdx] {
					_ACCUMULATE_COUNT(a, nulls, i, false, true)
				}
				// {{end}}
			}
		}
	},
	)
	// {{else}}
	// Unnecessary memory accounting can have significant overhead for window
	// aggregate functions because Compute is called at least once for every row.
	// For this reason, we do not use PerformOperation here.
	// {{if not (eq .CountKind "Rows")}}
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_COUNT(a, nulls, i, true, false)
		}
	} else
	// {{end}}
	{
		for i := startIdx; i < endIdx; i++ {
			_ACCUMULATE_COUNT(a, nulls, i, false, false)
		}
	}
	// {{end}}
}

func (a *count_COUNTKIND_AGGKINDAgg) Flush(outputIdx int) {
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	col := a.col
	// {{else}}
	col := a.vec.Int64()
	// {{end}}
	col[outputIdx] = a.curAgg
}

// {{if eq "_AGGKIND" "Ordered"}}
func (a *count_COUNTKIND_AGGKINDAgg) HandleEmptyInputScalar() {
	// COUNT aggregates are special because they return zero in case of an
	// empty input in the scalar context.
	a.col[0] = 0
}

// {{end}}

func (a *count_COUNTKIND_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.curAgg = 0
}

// {{if and (eq "_AGGKIND" "Window") (not (eq .CountKind "Rows"))}}

// Remove implements the slidingWindowAggregateFunc interface (see
// window_aggregator_tmpl.go).
func (a *count_COUNTKIND_AGGKINDAgg) Remove(
	vecs []*coldata.Vec, inputIdxs []uint32, startIdx, endIdx int,
) {
	nulls := vecs[inputIdxs[0]].Nulls()
	if nulls.MaybeHasNulls() {
		for i := startIdx; i < endIdx; i++ {
			_REMOVE_ROW(a, nulls, i, true)
		}
	} else {
		for i := startIdx; i < endIdx; i++ {
			_REMOVE_ROW(a, nulls, i, false)
		}
	}
}

// {{end}}

type count_COUNTKIND_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []count_COUNTKIND_AGGKINDAgg
}

var _ aggregateFuncAlloc = &count_COUNTKIND_AGGKINDAggAlloc{}

const sizeOfCount_COUNTKIND_AGGKINDAgg = int64(unsafe.Sizeof(count_COUNTKIND_AGGKINDAgg{}))
const count_COUNTKIND_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]count_COUNTKIND_AGGKINDAgg{}))

func (a *count_COUNTKIND_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(count_COUNTKIND_AGGKINDAggSliceOverhead + sizeOfCount_COUNTKIND_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]count_COUNTKIND_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_COUNT aggregates the value at index i into the count aggregate.
// _COL_WITH_NULLS indicates whether we should be paying attention to NULLs.
func _ACCUMULATE_COUNT(
	a *countAgg, nulls *coldata.Nulls, i int, _COL_WITH_NULLS bool, _HAS_SEL bool,
) { // */}}
	// {{define "accumulateCount" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	// {{if not .HasSel}}
	//gcassert:bce
	// {{end}}
	if groups[i] {
		if !a.isFirstGroup {
			a.col[a.curIdx] = a.curAgg
			a.curIdx++
			a.curAgg = int64(0)
		}
		a.isFirstGroup = false
	}
	// {{end}}

	var y int64
	// {{if .ColWithNulls}}
	y = int64(0)
	if !nulls.NullAt(i) {
		y = 1
	}
	// {{else}}
	y = int64(1)
	// {{end}}
	a.curAgg += y
	// {{end}}

	// {{/*
} // */}}

// {{/*
// _REMOVE_ROW removes the value of the ith row from the output for the
// current aggregation.
func _REMOVE_ROW(a *countAgg, nulls *coldata.Nulls, i int, _COL_WITH_NULLS bool) { // */}}
	// {{define "removeRow"}}
	var y int64
	// {{if .ColWithNulls}}
	y = int64(0)
	if !nulls.NullAt(i) {
		y = 1
	}
	// {{else}}
	y = int64(1)
	// {{end}}
	a.curAgg -= y
	// {{end}}
	// {{/*
} // */}}
