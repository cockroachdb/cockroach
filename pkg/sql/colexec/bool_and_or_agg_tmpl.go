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
// This file is the execgen template for bool_and_or_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/errors"
)

// Remove unused warning.
var _ = colexecerror.InternalError

// {{/*

// _ASSIGN_BOOL_OP is the template boolean operation function for assigning the
// first input to the result of a boolean operation of the second and the third
// inputs.
func _ASSIGN_BOOL_OP(_, _, _ string) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// {{range .}}

func newBool_OP_TYPE_AGGKINDAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &bool_OP_TYPE_AGGKINDAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type bool_OP_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	sawNonNull bool
	vec        []bool
	curAgg     bool
}

var _ aggregateFunc = &bool_OP_TYPE_AGGKINDAgg{}

func (a *bool_OP_TYPE_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Init(groups, vec)
	// {{else}}
	a.hashAggregateFuncBase.Init(groups, vec)
	// {{end}}
	a.vec = vec.Bool()
	a.Reset()
}

func (a *bool_OP_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{else}}
	a.hashAggregateFuncBase.Reset()
	// {{end}}
	// _DEFAULT_VAL indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the _DEFAULT_VAL is true and for bool_or the _DEFAULT_VAL is false.
	a.curAgg = _DEFAULT_VAL
}

func (a *bool_OP_TYPE_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bool(), vec.Nulls()
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
				_ACCUMULATE_BOOLEAN(a, nulls, i, true)
			}
		} else {
			for i := range col {
				_ACCUMULATE_BOOLEAN(a, nulls, i, false)
			}
		}
	} else
	// {{end}}
	{
		sel = sel[:inputLen]
		if nulls.MaybeHasNulls() {
			for _, i := range sel {
				_ACCUMULATE_BOOLEAN(a, nulls, i, true)
			}
		} else {
			for _, i := range sel {
				_ACCUMULATE_BOOLEAN(a, nulls, i, false)
			}
		}
	}
}

func (a *bool_OP_TYPE_AGGKINDAgg) Flush(outputIdx int) {
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	// {{end}}
	if !a.sawNonNull {
		a.nulls.SetNull(outputIdx)
	} else {
		a.vec[outputIdx] = a.curAgg
	}
}

type bool_OP_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []bool_OP_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &bool_OP_TYPE_AGGKINDAggAlloc{}

const sizeOfBool_OP_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(bool_OP_TYPE_AGGKINDAgg{}))
const bool_OP_TYPE_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]bool_OP_TYPE_AGGKINDAgg{}))

func (a *bool_OP_TYPE_AGGKINDAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(bool_OP_TYPE_AGGKINDAggSliceOverhead + sizeOfBool_OP_TYPE_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]bool_OP_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_BOOLEAN aggregates the boolean value at index i into the boolean aggregate.
func _ACCUMULATE_BOOLEAN(a *bool_OP_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "accumulateBoolean" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if groups[i] {
		if !a.sawNonNull {
			a.nulls.SetNull(a.curIdx)
		} else {
			a.vec[a.curIdx] = a.curAgg
		}
		a.curIdx++
		// {{with .Global}}
		a.curAgg = _DEFAULT_VAL
		// {{end}}
		a.sawNonNull = false
	}
	// {{end}}

	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !isNull {
		// {{with .Global}}
		_ASSIGN_BOOL_OP(a.curAgg, a.curAgg, col[i])
		// {{end}}
		a.sawNonNull = true
	}

	// {{end}}

	// {{/*
} // */}}
