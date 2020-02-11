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
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Operator

// Dummy import to pull in "math" package.
var _ int = math.MaxInt16

// _ASSIGN_CMP is the template function for assigning the result of comparing
// the second input to the third input into the first input.
func _ASSIGN_NE(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// match takes a selection vector and compare it against the values of the key
// of its aggregation function. It returns a selection vector representing the
// unmatched tuples. It also takes a pointer to a vector which it will directly
// write the result of matched tuples into that vector. Caller is responsible
// for allocating memory for the matched vector.
// NOTE: the return vector will reuse the memory allocated for the selection
//       vector.
func (v hashAggFunc) match(
	sel []uint16,
	b coldata.Batch,
	keyCols []uint32,
	keyTypes []coltypes.T,
	keyMapping coldata.Batch,
	matched *[]uint16,
) []uint16 {
	diff := make([]bool, len(sel))

	for keyIdx, colIdx := range keyCols {
		lhs := keyMapping.ColVec(keyIdx)
		rhs := b.ColVec(int(colIdx))
		lhsNull := lhs.Nulls().NullAt(v.keyIdx)
		keyTyp := keyTypes[keyIdx]

		for selIdx, rowIdx := range sel {
			if lhs.MaybeHasNulls() {
				if rhs.MaybeHasNulls() {
					// we checked the nulls first
					if lhsNull != rhs.Nulls().NullAt(rowIdx) {
						diff[selIdx] = diff[selIdx] || true
						break
					}
				}
			}

			// then we checked for vector equality
			switch keyTyp {
			// {{range .}}
			case _TYPES_T:
				lhsVec := lhs._TemplateType()
				rhsVec := rhs._TemplateType()
				lhsVal := execgen.UNSAFEGET(lhsVec, int(v.keyIdx))
				rhsVal := execgen.UNSAFEGET(rhsVec, int(rowIdx))
				var cmp bool
				_ASSIGN_NE("cmp", "lhsVal", "rhsVal")
				diff[selIdx] = diff[selIdx] || cmp
				// {{end}}
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", keyTyp))
			}
		}

	}

	remaining := sel[:0]
	for selIdx, isDiff := range diff {
		if isDiff {
			remaining = append(remaining, sel[selIdx])
		} else {
			*matched = append(*matched, sel[selIdx])
		}
	}

	*matched = (*matched)[:len(*matched)]

	return remaining
}
