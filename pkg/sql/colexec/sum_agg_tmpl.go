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
// This file is the execgen template for sum_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// */}}

func newSumAgg(t coltypes.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &sum_TYPEAgg{}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sum agg type %s", t)
	}
}

// {{range .}}

type sum_TYPEAgg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// curAgg holds the running total, so we can index into the slice once per
		// group, instead of on each iteration.
		curAgg _GOTYPE
		// vec points to the output vector we are updating.
		vec []_GOTYPE
		// nulls points to the output null vector that we are updating.
		nulls *coldata.Nulls
		// foundNonNullForCurrentGroup tracks if we have seen any non-null values
		// for the group that is currently being aggregated.
		foundNonNullForCurrentGroup bool
	}
}

var _ aggregateFunc = &sum_TYPEAgg{}

func (a *sum_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.scratch.vec = v._TemplateType()
	a.scratch.nulls = v.Nulls()
	a.Reset()
}

func (a *sum_TYPEAgg) Reset() {
	a.scratch.curIdx = -1
	a.scratch.foundNonNullForCurrentGroup = false
	a.scratch.nulls.UnsetNulls()
	a.done = false
}

func (a *sum_TYPEAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *sum_TYPEAgg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		a.scratch.nulls.UnsetNullsAfter(uint16(idx + 1))
	}
}

func (a *sum_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value. If we haven't found
		// any non-nulls for this group so far, the output for this group should be
		// null.
		if !a.scratch.foundNonNullForCurrentGroup {
			a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
		} else {
			a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
		}
		a.scratch.curIdx++
		a.done = true
		return
	}
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TemplateType(), vec.Nulls()
	if nulls.MaybeHasNulls() {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, nulls, i, true)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, nulls, i, true)
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, nulls, i, false)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, nulls, i, false)
			}
		}
	}
}

func (a *sum_TYPEAgg) HandleEmptyInputScalar() {
	a.scratch.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _ACCUMULATE_SUM adds the value of the ith row to the output for the current
// group. If this is the first row of a new group, and no non-nulls have been
// found for the current group, then the output for the current group is set to
// null.
func _ACCUMULATE_SUM(a *sum_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}

	// {{define "accumulateSum"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If
		// a.scratch.curIdx is negative, it means that this is the first group.
		if a.scratch.curIdx >= 0 {
			if !a.scratch.foundNonNullForCurrentGroup {
				a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
			} else {
				a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
			}
		}
		a.scratch.curIdx++
		// {{with .Global}}
		a.scratch.curAgg = zero_TYPEValue
		// {{end}}

		// {{/*
		// We only need to reset this flag if there are nulls. If there are no
		// nulls, this will be updated unconditionally below.
		// */}}
		// {{ if .HasNulls }}
		a.scratch.foundNonNullForCurrentGroup = false
		// {{ end }}
	}
	var isNull bool
	// {{ if .HasNulls }}
	isNull = nulls.NullAt(uint16(i))
	// {{ else }}
	isNull = false
	// {{ end }}
	if !isNull {
		_ASSIGN_ADD(a.scratch.curAgg, a.scratch.curAgg, col[i])
		a.scratch.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
