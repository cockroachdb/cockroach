// Copyright 2020 The Cockroach Authors.
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
// This file is the execgen template for concat_agg.eg.go. It's formatted in a
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

func newConcat_AGGKINDAggAlloc(allocator *colmem.Allocator, allocSize int64) aggregateFuncAlloc {
	return &concat_AGGKINDAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type concat_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	allocator *colmem.Allocator
	// curAgg holds the running total.
	curAgg []byte
	// col points to the output vector we are updating.
	col *coldata.Bytes
	// vec is the same as col before conversion from coldata.Vec.
	vec coldata.Vec
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

func (a *concat_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Init(groups, vec)
	// {{else}}
	a.hashAggregateFuncBase.Init(groups, vec)
	// {{end}}
	a.vec = vec
	a.col = vec.Bytes()
	a.Reset()
}

func (a *concat_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{else}}
	a.hashAggregateFuncBase.Reset()
	// {{end}}
	a.foundNonNullForCurrentGroup = false
	a.curAgg = zeroBytesValue
}

func (a *concat_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	oldCurAggSize := len(a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bytes(), vec.Nulls()
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
				if nulls.MaybeHasNulls() {
					for i := 0; i < inputLen; i++ {
						_ACCUMULATE_CONCAT(a, nulls, i, true)
					}
				} else {
					for i := 0; i < inputLen; i++ {
						_ACCUMULATE_CONCAT(a, nulls, i, false)
					}
				}
			} else
			// {{end}}
			{
				sel = sel[:inputLen]
				if nulls.MaybeHasNulls() {
					for _, i := range sel {
						_ACCUMULATE_CONCAT(a, nulls, i, true)
					}
				} else {
					for _, i := range sel {
						_ACCUMULATE_CONCAT(a, nulls, i, false)
					}
				}
			}
		},
	)
	newCurAggSize := len(a.curAgg)
	a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
}

func (a *concat_AGGKINDAgg) Flush(outputIdx int) {
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
	// Release the reference to curAgg eagerly.
	a.allocator.AdjustMemoryUsage(-int64(len(a.curAgg)))
	a.curAgg = nil
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
func _ACCUMULATE_CONCAT(a *concat_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "accumulateConcat" }}
	// {{if eq "_AGGKIND" "Ordered"}}
	if groups[i] {
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
