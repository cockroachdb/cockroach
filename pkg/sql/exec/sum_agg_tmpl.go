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

package exec

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _ string) {
	panic("")
}

// */}}

func newSumAgg(t types.T) (aggregateFunc, error) {
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
	copy(a.scratch.vec, zero_TYPEColumn)
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
		copy(a.scratch.vec[idx+1:], zero_TYPEColumn)
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
		// null. If a.scratch.curIdx is negative, it means the input has zero rows, and
		// there should be no output at all.
		if !a.scratch.foundNonNullForCurrentGroup && a.scratch.curIdx >= 0 {
			a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
		}
		a.scratch.curIdx++
		a.done = true
		return
	}
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TemplateType(), vec.Nulls()
	if nulls.HasNulls() {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM_WITH_NULL(a, nulls, i)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM_WITH_NULL(a, nulls, i)
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, i)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, i)
			}
		}
	}
}

// {{end}}

// {{/*
// _ACCUMULATE_SUM finds a non-null value for the group that contains the ith
// row. If a non-null value was already found, then it does nothing. If this is
// the first row of a new group, and no non-nulls have been found for the
// current group, then the output for the current group is set to null. This
// function presumes that the current batch has no null values.
func _ACCUMULATE_SUM(a *sum_TYPEAgg, i int) { // */}}

	// {{define "accumulateSum"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If
		// a.scratch.curIdx is negative, it means that this is the first group.
		if !a.scratch.foundNonNullForCurrentGroup && a.scratch.curIdx >= 0 {
			a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
		}
		a.scratch.curIdx += 1
		a.scratch.foundNonNullForCurrentGroup = false
	}
	_ASSIGN_ADD("a.scratch.vec[a.scratch.curIdx]", "a.scratch.vec[a.scratch.curIdx]", "col[i]")
	a.scratch.foundNonNullForCurrentGroup = true
	// {{end}}

	// {{/*
} // */}}

// {{/*
// _ACCUMULATE_SUM_WITH_NULL behaves the same as _ACCUMULATE_SUM but handles
// the presence of nulls in the input batch.
func _ACCUMULATE_SUM_WITH_NULL(a *sum_TYPEAgg, nulls *coldata.Nulls, i int) { // */}}

	// {{define "accumulateSumWithNull"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If
		// a.scratch.curIdx is negative, it means that this is the first group.
		if !a.scratch.foundNonNullForCurrentGroup && a.scratch.curIdx >= 0 {
			a.scratch.nulls.SetNull(uint16(a.scratch.curIdx))
		}
		a.scratch.curIdx += 1
		a.scratch.foundNonNullForCurrentGroup = false
	}
	if !nulls.NullAt(uint16(i)) {
		_ASSIGN_ADD("a.scratch.vec[a.scratch.curIdx]", "a.scratch.vec[a.scratch.curIdx]", "col[i]")
		a.scratch.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
