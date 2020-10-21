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
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	vec    []int64
	curAgg int64
}

var _ AggregateFunc = &count_COUNTKIND_AGGKINDAgg{}

func (a *count_COUNTKIND_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Init(groups, vec)
	// {{else}}
	a.hashAggregateFuncBase.Init(groups, vec)
	// {{end}}
	a.vec = vec.Int64()
	a.Reset()
}

func (a *count_COUNTKIND_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{else}}
	a.hashAggregateFuncBase.Reset()
	// {{end}}
	a.curAgg = 0
}

func (a *count_COUNTKIND_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	// {{if not (eq .CountKind "Rows")}}
	// If this is a COUNT(col) aggregator and there are nulls in this batch,
	// we must check each value for nullity. Note that it is only legal to do a
	// COUNT aggregate on a single column.
	nulls := vecs[inputIdxs[0]].Nulls()
	// {{end}}
	// {{if eq "_AGGKIND" "Ordered"}}
	groups := a.groups
	// {{/*
	// We don't need to check whether sel is non-nil when performing
	// hash aggregation because the hash aggregator always uses non-nil
	// sel to specify the tuples to be aggregated.
	// */}}
	if sel == nil {
		_ = groups[inputLen-1]
		// {{if not (eq .CountKind "Rows")}}
		if nulls.MaybeHasNulls() {
			for i := 0; i < inputLen; i++ {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		} else
		// {{end}}
		{
			for i := 0; i < inputLen; i++ {
				_ACCUMULATE_COUNT(a, nulls, i, false)
			}
		}
	} else
	// {{end}}
	{
		// {{if not (eq .CountKind "Rows")}}
		if nulls.MaybeHasNulls() {
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		} else
		// {{end}}
		{
			// {{if eq "_AGGKIND" "Hash"}}
			// We don't need to pay attention to nulls (either because it's a
			// COUNT_ROWS aggregate or because there are no nulls), and we're
			// performing a hash aggregation (meaning there is a single group),
			// so all inputLen tuples contribute to the count.
			a.curAgg += int64(inputLen)
			// {{else}}
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, false)
			}
			// {{end}}
		}
	}
}

func (a *count_COUNTKIND_AGGKINDAgg) Flush(outputIdx int) {
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	// {{end}}
	a.vec[outputIdx] = a.curAgg
}

func (a *count_COUNTKIND_AGGKINDAgg) HandleEmptyInputScalar() {
	// COUNT aggregates are special because they return zero in case of an
	// empty input in the scalar context.
	a.vec[0] = 0
}

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
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_COUNT aggregates the value at index i into the count aggregate.
// _COL_WITH_NULLS indicates whether we should be paying attention to NULLs.
func _ACCUMULATE_COUNT(a *countAgg, nulls *coldata.Nulls, i int, _COL_WITH_NULLS bool) { // */}}
	// {{define "accumulateCount" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if groups[i] {
		a.vec[a.curIdx] = a.curAgg
		a.curIdx++
		a.curAgg = int64(0)
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
