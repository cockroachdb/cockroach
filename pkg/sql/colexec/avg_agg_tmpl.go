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
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _ASSIGN_DIV_INT64 is the template division function for assigning the first
// input to the result of the second input / the third input, where the third
// input is an int64.
func _ASSIGN_DIV_INT64(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// */}}

func newAvgAgg(t coltypes.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &avg_TYPEAgg{}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported avg agg type %s", t)
	}
}

// {{range .}}

type avg_TYPEAgg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// curSum keeps track of the sum of elements belonging to the current group,
		// so we can index into the slice once per group, instead of on each
		// iteration.
		curSum _GOTYPE
		// curCount keeps track of the number of elements that we've seen
		// belonging to the current group.
		curCount int64
		// vec points to the output vector.
		vec []_GOTYPE
		// nulls points to the output null vector that we are updating.
		nulls *coldata.Nulls
		// foundNonNullForCurrentGroup tracks if we have seen any non-null values
		// for the group that is currently being aggregated.
		foundNonNullForCurrentGroup bool
	}
}

var _ aggregateFunc = &avg_TYPEAgg{}

func (a *avg_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.scratch.vec = v._TemplateType()
	a.scratch.nulls = v.Nulls()
	a.Reset()
}

func (a *avg_TYPEAgg) Reset() {
	a.scratch.curIdx = -1
	a.scratch.curSum = zero_TYPEValue
	a.scratch.curCount = 0
	a.scratch.foundNonNullForCurrentGroup = false
	a.scratch.nulls.UnsetNulls()
	a.done = false
}

func (a *avg_TYPEAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *avg_TYPEAgg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		a.scratch.nulls.UnsetNullsAfter(uint16(idx + 1))
	}
}

func (a *avg_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value. If we haven't found
		// any non-nulls for this group so far, the output for this group should be
		// NULL.
		if !a.scratch.foundNonNullForCurrentGroup {
			a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
		} else {
			_ASSIGN_DIV_INT64("a.scratch.vec[a.scratch.curIdx]", "a.scratch.curSum", "a.scratch.curCount")
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
				_ACCUMULATE_AVG(a, nulls, i, true)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_AVG(a, nulls, i, true)
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_AVG(a, nulls, i, false)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_AVG(a, nulls, i, false)
			}
		}
	}
}

func (a *avg_TYPEAgg) HandleEmptyInputScalar() {
	a.scratch.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _ACCUMULATE_AVG updates the total sum/count for current group using the value
// of the ith row. If this is the first row of a new group, then the average is
// computed for the current group. If no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _ACCUMULATE_AVG(a *_AGG_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}

	// {{define "accumulateAvg"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If
		// a.scratch.curIdx is negative, it means that this is the first group.
		if a.scratch.curIdx >= 0 {
			if !a.scratch.foundNonNullForCurrentGroup {
				a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
			} else {
				// {{with .Global}}
				_ASSIGN_DIV_INT64("a.scratch.vec[a.scratch.curIdx]", "a.scratch.curSum", "a.scratch.curCount")
				// {{end}}
			}
		}
		a.scratch.curIdx++
		// {{with .Global}}
		a.scratch.curSum = zero_TYPEValue
		// {{end}}
		a.scratch.curCount = 0

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
		_ASSIGN_ADD("a.scratch.curSum", "a.scratch.curSum", "col[i]")
		a.scratch.curCount++
		a.scratch.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
