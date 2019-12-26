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
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _GOTYPESLICE is the template Go type slice variable for this operator. It
// will be replaced by the Go slice representation for each type in coltypes.T, for
// example []int64 for coltypes.Int64.
type _GOTYPESLICE interface{}

// _ASSIGN_CMP is the template function for assigning true to the first input
// if the second input compares successfully to the third input. The comparison
// operator is tree.LT for MIN and is tree.GT for MAX.
func _ASSIGN_CMP(_, _, _ string) bool {
	execerror.VectorizedInternalPanic("")
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// {{range .}} {{/* for each aggregation (min and max) */}}

// {{/* Capture the aggregation name so we can use it in the inner loop. */}}
// {{$agg := .AggNameLower}}

func new_AGG_TITLEAgg(allocator *Allocator, t coltypes.T) (aggregateFunc, error) {
	switch t {
	// {{range .Overloads}}
	case _TYPES_T:
		return &_AGG_TYPEAgg{allocator: allocator}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported min agg type %s", t)
	}
}

// {{range .Overloads}}

type _AGG_TYPEAgg struct {
	allocator *Allocator
	done      bool
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
	a.done = false
}

func (a *_AGG_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *_AGG_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(uint16(idx + 1))
	}
}

func (a *_AGG_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value. If we haven't found
		// any non-nulls for this group so far, the output for this group should
		// be null.
		if !a.foundNonNullForCurrentGroup {
			a.nulls.SetNull(uint16(a.curIdx))
		} else {
			a.allocator.performOperation(
				[]coldata.Vec{a.vec},
				func() {
					execgen.SET(a.col, a.curIdx, a.curAgg)
				},
			)
		}
		a.curIdx++
		a.done = true
		return
	}
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TYPE(), vec.Nulls()
	a.allocator.performOperation(
		[]coldata.Vec{a.vec},
		func() {
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_ACCUMULATE_MINMAX(a, nulls, i, true)
					}
				} else {
					col = execgen.SLICE(col, 0, int(inputLen))
					for execgen.RANGE(i, col, 0, int(inputLen)) {
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
					col = execgen.SLICE(col, 0, int(inputLen))
					for execgen.RANGE(i, col, 0, int(inputLen)) {
						_ACCUMULATE_MINMAX(a, nulls, i, false)
					}
				}
			}
		},
	)
}

func (a *_AGG_TYPEAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

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
				a.nulls.SetNull(uint16(a.curIdx))
			} else {
				execgen.SET(a.col, a.curIdx, a.curAgg)
			}
		}
		a.curIdx++
		a.foundNonNullForCurrentGroup = false
	}
	var isNull bool
	// {{ if .HasNulls }}
	isNull = nulls.NullAt(uint16(i))
	// {{ else }}
	isNull = false
	// {{ end }}
	if !isNull {
		if !a.foundNonNullForCurrentGroup {
			a.curAgg = execgen.UNSAFEGET(col, int(i))
			a.foundNonNullForCurrentGroup = true
		} else {
			var cmp bool
			candidate := execgen.UNSAFEGET(col, int(i))
			_ASSIGN_CMP("cmp", "candidate", "a.curAgg")
			if cmp {
				a.curAgg = candidate
			}
		}
	}
	// {{end}}

	// {{/*
} // */}}
