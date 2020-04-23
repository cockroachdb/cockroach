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
// This file is the execgen template for hash_aggregator.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Operator

// Dummy import to pull in "colmem" package.
var _ colmem.ContextWrappedDatum

// Dummy import to pull in "math" package.
var _ int = math.MaxInt16

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _ASSIGN_NE is the template function for assigning the result of comparing
// the second input to the third input into the first input.
func _ASSIGN_NE(_, _, _ interface{}) int {
	colexecerror.InternalError("")
}

// */}}

// {{/*
func _MATCH_LOOP(
	sel []int,
	lhs coldata.Vec,
	rhs coldata.Vec,
	aggKeyIdx int,
	lhsNull bool,
	diff []bool,
	_LHS_MAYBE_HAS_NULLS bool,
	_RHS_MAYBE_HAS_NULLS bool,
) { // */}}
	// {{define "matchLoop" -}}

	lhsVal := execgen.UNSAFEGET(lhsCol, aggKeyIdx)

	for selIdx, rowIdx := range sel {
		// {{if .LhsMaybeHasNulls}}
		// {{if .RhsMaybeHasNulls}}
		rhsNull := rhs.Nulls().NullAt(rowIdx)
		if lhsNull && rhsNull {
			// Both values are NULLs, and we do not consider them different.
			continue
		} else if lhsNull || rhsNull {
			diff[selIdx] = true
			continue
		}
		// {{else}}
		if lhsNull {
			diff[selIdx] = true
			continue
		}
		// {{end}}
		// {{end}}

		rhsVal := execgen.UNSAFEGET(rhsCol, rowIdx)

		var cmp bool
		_ASSIGN_NE(cmp, lhsVal, rhsVal)
		diff[selIdx] = diff[selIdx] || cmp
	}

	// {{end}}
	// {{/*
} // */}}

// match takes a selection vector and compares it against the values of the key
// of its aggregation function. It returns a selection vector representing the
// unmatched tuples and a boolean to indicate whether or not there are any
// matching tuples. It directly writes the result of matched tuples into the
// selection vector of 'b' and sets the length of the batch to the number of
// matching tuples. match also takes a diff boolean slice for internal use.
// This slice need to be allocated to be at at least as big as sel and set to
// all false. diff will be reset to all false when match returns. This is to
// avoid additional slice allocation.
// - firstDefiniteMatch indicates whether we know that tuple with index sel[0]
//   matches the key of the aggregation function and whether we can short
//   circuit probing that tuple.
// NOTE: the return vector will reuse the memory allocated for the selection
//       vector.
func (v hashAggFuncs) match(
	sel []int,
	b coldata.Batch,
	keyCols []uint32,
	keyTypes []types.T,
	keyMapping coldata.Batch,
	diff []bool,
	firstDefiniteMatch bool,
) (bool, []int) {
	// We want to directly write to the selection vector to avoid extra
	// allocation.
	b.SetSelection(true)
	matched := b.Selection()[:0]

	aggKeyIdx := v.keyIdx

	if firstDefiniteMatch {
		matched = append(matched, sel[0])
		sel = sel[1:]
		diff = diff[:len(diff)-1]
	}

	if len(sel) > 0 {
		for keyIdx, colIdx := range keyCols {
			lhs := keyMapping.ColVec(keyIdx)
			lhsHasNull := lhs.MaybeHasNulls()

			rhs := b.ColVec(int(colIdx))
			rhsHasNull := rhs.MaybeHasNulls()

			keyTyp := keyTypes[keyIdx]

			switch typeconv.FromColumnType(&keyTyp) {
			// {{range .}}
			case _TYPES_T:
				lhsCol := lhs._TemplateType()
				rhsCol := rhs._TemplateType()
				if lhsHasNull {
					lhsNull := lhs.Nulls().NullAt(v.keyIdx)
					if rhsHasNull {
						_MATCH_LOOP(sel, lhs, rhs, aggKeyIdx, lhsNull, diff, true, true)
					} else {
						_MATCH_LOOP(sel, lhs, rhs, aggKeyIdx, lhsNull, diff, true, false)
					}
				} else {
					if rhsHasNull {
						_MATCH_LOOP(sel, lhs, rhs, aggKeyIdx, lhsNull, diff, false, true)
					} else {
						_MATCH_LOOP(sel, lhs, rhs, aggKeyIdx, lhsNull, diff, false, false)
					}
				}
			// {{end}}
			default:
				colexecerror.InternalError(fmt.Sprintf("unhandled type %s", &keyTyp))
			}
		}
	}

	remaining := sel[:0]
	for selIdx, tupleIdx := range sel {
		if diff[selIdx] {
			remaining = append(remaining, tupleIdx)
		} else {
			matched = append(matched, tupleIdx)
		}
	}

	anyMatched := false
	if len(matched) > 0 {
		b.SetLength(len(matched))
		anyMatched = true
	}

	// Reset diff slice back to all false.
	for n := 0; n < len(diff); n += copy(diff, zeroBoolColumn) {
	}

	return anyMatched, remaining
}
