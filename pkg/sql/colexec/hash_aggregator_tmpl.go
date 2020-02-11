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
func _ASSIGN_CMP(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// given a selection vector, partition this selection vector into
// a `matched` vector and `remaining` vector.
func (v hashAggFunc) match(sel []uint16, b coldata.Batch, keyCols []uint32, keyTypes []coltypes.T,
) ([]uint16, []uint16) {
	matched := make([]uint16, 0, len(sel))
	remaining := make([]uint16, 0, len(sel))

	for _, rowIdx := range sel {
		match := true
		for keyIdx, colIdx := range keyCols {
			lhs := v.keys[keyIdx]
			rhs := b.ColVec(int(colIdx))

			if lhs.MaybeHasNulls() {
				if rhs.MaybeHasNulls() {
					// we checked the nulls first
					if lhs.Nulls().NullAt(0) != rhs.Nulls().NullAt(rowIdx) {
						match = false
						break
					}
				}
			}

			// then we checked for vector equality
			switch typ := keyTypes[keyIdx]; typ {
			// {{range .}}
			case _TYPES_T:
				lhsVec := lhs._TemplateType()
				rhsVec := rhs._TemplateType()
				lhsVal := execgen.UNSAFEGET(lhsVec, 0)
				rhsVal := execgen.UNSAFEGET(rhsVec, int(rowIdx))
				_ASSIGN_CMP("match", "lhsVal", "rhsVal")
				// {{end}}
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", typ))
			}
		}

		if match {
			matched = append(matched, rowIdx)
		} else {
			remaining = append(remaining, rowIdx)
		}
	}

	return matched, remaining
}
