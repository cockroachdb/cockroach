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
	"time"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

func newAnyNotNullAgg(allocator *colmem.Allocator, t *types.T) (aggregateFunc, error) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily[t.Family()] {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			allocator.AdjustMemoryUsage(int64(sizeOfAnyNotNull_TYPEAgg))
			return &anyNotNull_TYPEAgg{allocator: allocator}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported any not null agg type %s", t.Name())
}

// {{range .}}
// {{range .WidthOverloads}}

// anyNotNull_TYPEAgg implements the ANY_NOT_NULL aggregate, returning the
// first non-null value in the input column.
type anyNotNull_TYPEAgg struct {
	allocator                   *colmem.Allocator
	groups                      []bool
	vec                         coldata.Vec
	col                         _GOTYPESLICE
	nulls                       *coldata.Nulls
	curIdx                      int
	curAgg                      _GOTYPE
	foundNonNullForCurrentGroup bool
}

var _ aggregateFunc = &anyNotNull_TYPEAgg{}

const sizeOfAnyNotNull_TYPEAgg = unsafe.Sizeof(&anyNotNull_TYPEAgg{})

func (a *anyNotNull_TYPEAgg) Init(groups []bool, vec coldata.Vec) {
	a.groups = groups
	a.vec = vec
	a.col = vec.TemplateType()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *anyNotNull_TYPEAgg) Reset() {
	a.curIdx = -1
	a.foundNonNullForCurrentGroup = false
	a.nulls.UnsetNulls()
}

func (a *anyNotNull_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *anyNotNull_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *anyNotNull_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	inputLen := b.Length()
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec.TemplateType(), vec.Nulls()

	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, nulls, i, true)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_FIND_ANY_NOT_NULL(a, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, nulls, i, false)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_FIND_ANY_NOT_NULL(a, nulls, i, false)
					}
				}
			}
		},
	)
}

func (a *anyNotNull_TYPEAgg) Flush() {
	// If we haven't found any non-nulls for this group so far, the output for
	// this group should be null.
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(a.curIdx)
	} else {
		execgen.SET(a.col, a.curIdx, a.curAgg)
	}
	a.curIdx++
}

func (a *anyNotNull_TYPEAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

// {{end}}
// {{end}}

// {{/*
// _FIND_ANY_NOT_NULL finds a non-null value for the group that contains the ith
// row. If a non-null value was already found, then it does nothing. If this is
// the first row of a new group, and no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _FIND_ANY_NOT_NULL(a *anyNotNull_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "findAnyNotNull" -}}

	if a.groups[i] {
		// The `a.curIdx` check is necessary because for the first
		// group in the result set there is no "current group."
		if a.curIdx >= 0 {
			// If this is a new group, check if any non-nulls have been found for the
			// current group.
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				// {{with .Global}}
				execgen.SET(a.col, a.curIdx, a.curAgg)
				// {{end}}
			}
		}
		a.curIdx++
		a.foundNonNullForCurrentGroup = false
	}
	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !a.foundNonNullForCurrentGroup && !isNull {
		// If we haven't seen any non-nulls for the current group yet, and the
		// current value is non-null, then we can pick the current value to be the
		// output.
		// {{with .Global}}
		val := execgen.UNSAFEGET(col, i)
		execgen.COPYVAL(a.curAgg, val)
		// {{end}}
		a.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
