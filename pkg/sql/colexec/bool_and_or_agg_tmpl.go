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
)

// Remove unused warning.
var _ = colexecerror.InternalError

// {{/*

// _ASSIGN_BOOL_OP is the template boolean operation function for assigning the
// first input to the result of a boolean operation of the second and the third
// inputs.
func _ASSIGN_BOOL_OP(_, _, _ string) {
	colexecerror.InternalError("")
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
	sawNonNull bool
	// {{if eq "_AGGKIND" "Ordered"}}
	groups []bool
	// {{end}}
	vec    []bool
	nulls  *coldata.Nulls
	curIdx int
	curAgg bool
}

var _ aggregateFunc = &bool_OP_TYPE_AGGKINDAgg{}

const sizeOfBool_OP_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(bool_OP_TYPE_AGGKINDAgg{}))

func (b *bool_OP_TYPE_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	b.groups = groups
	// {{end}}
	b.vec = vec.Bool()
	b.nulls = vec.Nulls()
	b.Reset()
}

func (b *bool_OP_TYPE_AGGKINDAgg) Reset() {
	b.curIdx = 0
	b.nulls.UnsetNulls()
	// _DEFAULT_VAL indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the _DEFAULT_VAL is true and for bool_or the _DEFAULT_VAL is false.
	b.curAgg = _DEFAULT_VAL
}

func (b *bool_OP_TYPE_AGGKINDAgg) CurrentOutputIndex() int {
	return b.curIdx
}

func (b *bool_OP_TYPE_AGGKINDAgg) SetOutputIndex(idx int) {
	b.curIdx = idx
}

func (b *bool_OP_TYPE_AGGKINDAgg) Compute(batch coldata.Batch, inputIdxs []uint32) {
	inputLen := batch.Length()
	vec, sel := batch.ColVec(int(inputIdxs[0])), batch.Selection()
	col, nulls := vec.Bool(), vec.Nulls()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			_ACCUMULATE_BOOLEAN(b, nulls, i)
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			_ACCUMULATE_BOOLEAN(b, nulls, i)
		}
	}
}

func (b *bool_OP_TYPE_AGGKINDAgg) Flush() {
	if !b.sawNonNull {
		b.nulls.SetNull(b.curIdx)
	} else {
		b.vec[b.curIdx] = b.curAgg
	}
	b.curIdx++
}

func (b *bool_OP_TYPE_AGGKINDAgg) HandleEmptyInputScalar() {
	b.nulls.SetNull(0)
}

type bool_OP_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []bool_OP_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &bool_OP_TYPE_AGGKINDAggAlloc{}

func (a *bool_OP_TYPE_AGGKINDAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOfBool_OP_TYPE_AGGKINDAgg * a.allocSize)
		a.aggFuncs = make([]bool_OP_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}

// {{/*
// _ACCUMULATE_BOOLEAN aggregates the boolean value at index i into the boolean aggregate.
func _ACCUMULATE_BOOLEAN(b *bool_OP_TYPE_AGGKINDAgg, nulls *coldata.Nulls, i int) { // */}}
	// {{define "accumulateBoolean" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if b.groups[i] {
		if !b.sawNonNull {
			b.nulls.SetNull(b.curIdx)
		} else {
			b.vec[b.curIdx] = b.curAgg
		}
		b.curIdx++
		// {{with .Global}}
		b.curAgg = _DEFAULT_VAL
		// {{end}}
		b.sawNonNull = false
	}
	// {{end}}

	// TODO(yuzefovich): template out has nulls vs no nulls cases.
	isNull := nulls.NullAt(i)
	if !isNull {
		// {{with .Global}}
		_ASSIGN_BOOL_OP(b.curAgg, b.curAgg, col[i])
		// {{end}}
		b.sawNonNull = true
	}

	// {{end}}

	// {{/*
} // */}}
