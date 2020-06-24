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
// This file is the execgen template for any_not_null_agg.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*

// Declarations to make the template compile properly.

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

func newAnyNotNull_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &anyNotNull_TYPE_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported any not null agg type %s", t.Name())
}

// {{range .}}
// {{range .WidthOverloads}}

// anyNotNull_TYPE_AGGKINDAgg implements the ANY_NOT_NULL aggregate, returning the
// first non-null value in the input column.
type anyNotNull_TYPE_AGGKINDAgg struct {
	allocator *colmem.Allocator
	// {{if eq "_AGGKIND" "Ordered"}}
	groups []bool
	// {{end}}
	vec                         coldata.Vec
	col                         _GOTYPESLICE
	nulls                       *coldata.Nulls
	curIdx                      int
	curAgg                      _GOTYPE
	foundNonNullForCurrentGroup bool
}

var _ aggregateFunc = &anyNotNull_TYPE_AGGKINDAgg{}

const sizeOfAnyNotNull_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(anyNotNull_TYPE_AGGKINDAgg{}))

func (a *anyNotNull_TYPE_AGGKINDAgg) Init(groups []bool, vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.groups = groups
	// {{end}}
	a.vec = vec
	a.col = vec.TemplateType()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *anyNotNull_TYPE_AGGKINDAgg) Reset() {
	a.curIdx = 0
	a.foundNonNullForCurrentGroup = false
	a.nulls.UnsetNulls()
}

func (a *anyNotNull_TYPE_AGGKINDAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *anyNotNull_TYPE_AGGKINDAgg) SetOutputIndex(idx int) {
	a.curIdx = idx
}

func (a *anyNotNull_TYPE_AGGKINDAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	// {{if eq "_AGGKIND" "Hash"}}
	if a.foundNonNullForCurrentGroup {
		// We have already seen non-null for the current group, and since there
		// is at most a single group when performing hash aggregation, we can
		// finish computing.
		return
	}
	// {{end}}

	inputLen := b.Length()
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec.TemplateType(), vec.Nulls()

	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			// Capture col to force bounds check to work. See
			// https://github.com/golang/go/issues/39756
			col := col
			_ = execgen.UNSAFEGET(col, inputLen-1)
			// {{if eq "_AGGKIND" "Ordered"}}
			groups := a.groups
			// {{end}}
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, groups, nulls, i, true)
					}
				} else {
					// {{if eq "_AGGKIND" "Ordered"}}
					_ = groups[inputLen-1]
					// {{end}}
					for i := 0; i < inputLen; i++ {
						_FIND_ANY_NOT_NULL(a, groups, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, groups, nulls, i, false)
					}
				} else {
					// {{if eq "_AGGKIND" "Ordered"}}
					_ = groups[inputLen-1]
					// {{end}}
					for i := 0; i < inputLen; i++ {
						_FIND_ANY_NOT_NULL(a, groups, nulls, i, false)
					}
				}
			}
		},
	)
}

func (a *anyNotNull_TYPE_AGGKINDAgg) Flush() {
	// If we haven't found any non-nulls for this group so far, the output for
	// this group should be null.
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(a.curIdx)
	} else {
		execgen.SET(a.col, a.curIdx, a.curAgg)
	}
	a.curIdx++
}

func (a *anyNotNull_TYPE_AGGKINDAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

type anyNotNull_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []anyNotNull_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &anyNotNull_TYPE_AGGKINDAggAlloc{}

func (a *anyNotNull_TYPE_AGGKINDAggAlloc) newAggFunc() aggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(sizeOfAnyNotNull_TYPE_AGGKINDAgg * a.allocSize)
		a.aggFuncs = make([]anyNotNull_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}
// {{end}}

// {{/*
// _FIND_ANY_NOT_NULL finds a non-null value for the group that contains the ith
// row. If a non-null value was already found, then it does nothing. If this is
// the first row of a new group, and no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _FIND_ANY_NOT_NULL(
	a *anyNotNull_TYPE_AGGKINDAgg, groups []bool, nulls *coldata.Nulls, i int, _HAS_NULLS bool,
) { // */}}
	// {{define "findAnyNotNull" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	if groups[i] {
		// If this is a new group, check if any non-nulls have been found for the
		// current group.
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
	if !a.foundNonNullForCurrentGroup && !isNull {
		// If we haven't seen any non-nulls for the current group yet, and the
		// current value is non-null, then we can pick the current value to be
		// the output.
		// {{with .Global}}
		val := execgen.UNSAFEGET(col, i)
		execgen.COPYVAL(a.curAgg, val)
		// {{end}}
		a.foundNonNullForCurrentGroup = true
		// {{if eq "_AGGKIND" "Hash"}}
		// We have already seen non-null for the current group, and since there
		// is at most a single group when performing hash aggregation, we can
		// finish computing.
		return
		// {{end}}
	}
	// {{end}}

	// {{/*
} // */}}
