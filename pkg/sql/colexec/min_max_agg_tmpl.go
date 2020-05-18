// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for min_max_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"time"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// Remove unused warning.
var _ = colexecerror.InternalError

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _ASSIGN_CMP is the template function for assigning true to the first input
// if the second input compares successfully to the third input. The comparison
// operator is tree.LT for MIN and is tree.GT for MAX.
func _ASSIGN_CMP(_, _, _ string) bool {
	colexecerror.InternalError("")
}

// */}}

// {{range .}} {{/* for each aggregation (min and max) */}}

// {{/* Capture the aggregation name so we can use it in the inner loop. */}}
// {{$agg := .AggNameLower}}

func new_AGG_TITLEAgg(allocator *colmem.Allocator, t *types.T) (aggregateFunc, error) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .Overloads}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			allocator.AdjustMemoryUsage(int64(sizeOf_AGG_TYPEAgg))
			return &_AGG_TYPEAgg{allocator: allocator}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported _AGG agg type %s", t.Name())
}

// {{range .Overloads}}
// {{range .WidthOverloads}}

type _AGG_TYPEAgg struct {
	allocator *colmem.Allocator
	groups    []bool
	curIdx    int
	// curAgg holds the running min/max, so we can index into the slice once per
	// group, instead of on each iteration.
	// NOTE: if foundNonNullForCurrentGroup is false, curAgg is undefined.
	curAgg _GOTYPE
	// col points to the output vector we are updating.
	col _GOTYPESLICE
	// vec is the same as col before conversion from coldata.Vec.
	vec coldata.Vec
	// nulls points to the output null vector that we are updating.
	nulls *coldata.Nulls
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

var _ aggregateFunc = &_AGG_TYPEAgg{}

const sizeOf_AGG_TYPEAgg = unsafe.Sizeof(&_AGG_TYPEAgg{})

func (a *_AGG_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.vec = v
	a.col = v._TYPE()
	a.nulls = v.Nulls()
	a.Reset()
}

func (a *_AGG_TYPEAgg) Reset() {
	a.curIdx = -1
	a.foundNonNullForCurrentGroup = false
	a.nulls.UnsetNulls()
}

func (a *_AGG_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *_AGG_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *_AGG_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	inputLen := b.Length()
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TYPE(), vec.Nulls()
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				}
			}
		},
	)
}

func (a *_AGG_TYPEAgg) Flush() {
	// The aggregation is finished. Flush the last value. If we haven't found
	// any non-nulls for this group so far, the output for this group should
	// be null.
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(a.curIdx)
	} else {
		execgen.SET(a.col, a.curIdx, a.curAgg)
	}
	a.curIdx++
}

func (a *_AGG_TYPEAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

// {{end}}
// {{end}}
// {{end}}

// {{/*
// _ACCUMULATE_MINMAX sets the output for the current group to be the value of
// the ith row if it is smaller/larger than the current result. If this is the
// first row of a new group, and no non-nulls have been found for the current
// group, then the output for the current group is set to null.
func _ACCUMULATE_MINMAX(a *_AGG_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}

	// {{define "accumulateMinMax"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If a.curIdx is
		// negative, it means that this is the first group.
		if a.curIdx >= 0 {
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
	// {{with .Global}}
	if !isNull {
		if !a.foundNonNullForCurrentGroup {
			val := execgen.UNSAFEGET(col, i)
			execgen.COPYVAL(a.curAgg, val)
			a.foundNonNullForCurrentGroup = true
		} else {
			var cmp bool
			candidate := execgen.UNSAFEGET(col, i)
			_ASSIGN_CMP(cmp, candidate, a.curAgg)
			if cmp {
				execgen.COPYVAL(a.curAgg, candidate)
			}
		}
	}
	// {{end}}
	// {{end}}

	// {{/*
} // */}}
