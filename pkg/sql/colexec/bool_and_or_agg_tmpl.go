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
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// */}}
	// HACK: crlfmt removes the "*/}}" comment if it's the last line in the import
	// block. This was picked because it sorts after "pkg/sql/colexec/execerror" and
	// has no deps.
	_ "github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

// {{/*

// _ASSIGN_BOOL_OP is the template boolean operation function for assigning the
// first input to the result of a boolean operation of the second and the third
// inputs.
func _ASSIGN_BOOL_OP(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{range .}}

func newBool_OP_TYPEAgg(allocator *Allocator) aggregateFunc {
	allocator.AdjustMemoryUsage(int64(sizeOfBool_OP_TYPEAgg))
	return &bool_OP_TYPEAgg{}
}

type bool_OP_TYPEAgg struct {
	done       bool
	sawNonNull bool

	groups []bool
	vec    []bool

	nulls  *coldata.Nulls
	curIdx int
	curAgg bool
}

var _ aggregateFunc = &bool_OP_TYPEAgg{}

const sizeOfBool_OP_TYPEAgg = unsafe.Sizeof(bool_OP_TYPEAgg{})

func (b *bool_OP_TYPEAgg) Init(groups []bool, vec coldata.Vec) {
	b.groups = groups
	b.vec = vec.Bool()
	b.nulls = vec.Nulls()
	b.Reset()
}

func (b *bool_OP_TYPEAgg) Reset() {
	b.curIdx = -1
	b.nulls.UnsetNulls()
	b.done = false
	// _DEFAULT_VAL indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the _DEFAULT_VAL is true and for bool_or the _DEFAULT_VAL is false.
	b.curAgg = _DEFAULT_VAL
}

func (b *bool_OP_TYPEAgg) CurrentOutputIndex() int {
	return b.curIdx
}

func (b *bool_OP_TYPEAgg) SetOutputIndex(idx int) {
	if b.curIdx != -1 {
		b.curIdx = idx
		b.nulls.UnsetNullsAfter(idx)
	}
}

func (b *bool_OP_TYPEAgg) Compute(batch coldata.Batch, inputIdxs []uint32) {
	if b.done {
		return
	}
	inputLen := batch.Length()
	if inputLen == 0 {
		if !b.sawNonNull {
			b.nulls.SetNull(b.curIdx)
		} else {
			b.vec[b.curIdx] = b.curAgg
		}
		b.curIdx++
		b.done = true
		return
	}
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

func (b *bool_OP_TYPEAgg) HandleEmptyInputScalar() {
	b.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _ACCUMULATE_BOOLEAN aggregates the boolean value at index i into the boolean aggregate.
func _ACCUMULATE_BOOLEAN(b *bool_OP_TYPEAgg, nulls *coldata.Nulls, i int) { // */}}
	// {{define "accumulateBoolean" -}}
	if b.groups[i] {
		if b.curIdx >= 0 {
			if !b.sawNonNull {
				b.nulls.SetNull(b.curIdx)
			} else {
				b.vec[b.curIdx] = b.curAgg
			}
		}
		b.curIdx++
		// {{with .Global}}
		b.curAgg = _DEFAULT_VAL
		// {{end}}
		b.sawNonNull = false
	}
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
