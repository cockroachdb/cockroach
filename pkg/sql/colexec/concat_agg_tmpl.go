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

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

func (a *concat_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.groups = groups
	// {{end}}
	a.vec = vec
	a.col = vec.Bytes()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *concat_AGGKINDAgg) Reset() {
	a.curIdx = 0
	a.foundNonNullForCurrentGroup = false
	a.curAgg = zeroBytesValue
	a.nulls.UnsetNulls()
}

func (a *concat_AGGKINDAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *concat_AGGKINDAgg) SetOutputIndex(idx int) {
	a.curIdx = idx
}

func (a *concat_AGGKINDAgg) Compute(batch coldata.Batch, inputIdxs []uint32) {
	inputLen := batch.Length()
	vec, sel := batch.ColVec(int(inputIdxs[0])), batch.Selection()
	col, nulls := vec.Bytes(), vec.Nulls()
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			previousAggValMemoryUsage := a.aggValMemoryUsage()
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_CONCAT(a, nulls, i, true)
					}
				} else {
					for i := 0; i < inputLen; i++ {
						_ACCUMULATE_CONCAT(a, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_CONCAT(a, nulls, i, false)
					}
				} else {
					for i := 0; i < inputLen; i++ {
						_ACCUMULATE_CONCAT(a, nulls, i, false)
					}
				}
			}
			currentAggValMemoryUsage := a.aggValMemoryUsage()
			a.allocator.AdjustMemoryUsage(currentAggValMemoryUsage - previousAggValMemoryUsage)
		},
	)
}

func (a *concat_AGGKINDAgg) Flush() {
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec}, func() {
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				a.col.Set(a.curIdx, a.curAgg)
			}
			a.allocator.AdjustMemoryUsage(-a.aggValMemoryUsage())
			// release reference to curAgg eagerly
			a.curAgg = nil
			a.curIdx++
		})
}

func (a *concat_AGGKINDAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

func (a *concat_AGGKINDAgg) aggValMemoryUsage() int64 {
	return int64(len(a.curAgg))
}

func (a *concat_AGGKINDAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOfConcat_AGGKINDAgg * a.allocSize)
		a.aggFuncs = make([]concat_AGGKINDAgg, a.allocSize)
		for i := range a.aggFuncs {
			a.aggFuncs[i].allocator = a.allocator
		}
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

const sizeOfConcat_AGGKINDAgg = int64(unsafe.Sizeof(concat_AGGKINDAgg{}))

func newConcat_AGGKINDAggAlloc(allocator *colmem.Allocator, allocSize int64) aggregateFuncAlloc {
	return &concat_AGGKINDAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type concat_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []concat_AGGKINDAgg
}

type concat_AGGKINDAgg struct {
	// allocator is the allocator used to create this aggregateFunc
	// memory usage of output vector and curAgg varies during aggregation,
	// we need the allocator to monitor this change.
	allocator *colmem.Allocator
	// {{if eq "_AGGKIND" "Ordered"}}
	groups []bool
	// {{end}}
	curIdx int
	// curAgg holds the running total.
	curAgg []byte
	// col points to the output vector we are updating.
	col *coldata.Bytes
	// vec is the same as col before conversion from coldata.Vec.
	vec coldata.Vec
	// nulls points to the output null vector that we are updating.
	nulls *coldata.Nulls
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

// {{/*
func _ACCUMULATE_CONCAT(a *concat_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "accumulateConcat" }}
	// {{if eq "_AGGKIND" "Ordered"}}
	if a.groups[i] {
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
