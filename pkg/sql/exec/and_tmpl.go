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
// This file is the execgen template for and.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

type andOp struct {
	OneInputNode

	leftIdx   int
	rightIdx  int
	outputIdx int
}

// NewAndOp returns a new operator that logical-ANDs the boolean columns at
// leftIdx and rightIdx, returning the result in outputIdx.
func NewAndOp(input Operator, leftIdx, rightIdx, outputIdx int) Operator {
	return &andOp{
		OneInputNode: NewOneInputNode(input),
		leftIdx:      leftIdx,
		rightIdx:     rightIdx,
		outputIdx:    outputIdx,
	}
}

func (a *andOp) Init() {
	a.input.Init()
}

// {{/*
// This code snippet sets the result of AND'ing two boolean vectors while
// paying attention to null values.
// The rules for AND'ing two booleans are:
// 1. if at least one of the values is FALSE, then the result is also FALSE
// 2. if both values are TRUE, then the result is also TRUE
// 3. in all other cases (one is TRUE and the other is NULL or both are NULL),
//    the result is NULL.
func _SET_VALUES(_L_HAS_NULLS bool, _R_HAS_NULLS bool) { // */}}
	// {{ define "setValues" -}}
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel[:n] {
			// {{ if _L_HAS_NULLS }}
			isLeftNull := leftNulls.NullAt(i)
			// {{ else }}
			isLeftNull := false
			// {{ end }}
			// {{ if _R_HAS_NULLS }}
			isRightNull := rightNulls.NullAt(i)
			// {{ else }}
			isRightNull := false
			// {{ end }}
			leftVal := leftColVals[i]
			rightVal := rightColVals[i]
			if (!leftVal && !isLeftNull) || (!rightVal && !isRightNull) {
				// Rule 1: at least one boolean is FALSE.
				outputColVals[i] = false
				outputNulls.UnsetNull(i)
			} else if (leftVal && !isLeftNull) && (rightVal && !isRightNull) {
				// Rule 2: both booleans are TRUE.
				outputColVals[i] = true
				outputNulls.UnsetNull(i)
			} else {
				// Rule 3.
				outputNulls.SetNull(i)
			}
		}
	} else {
		_ = rightColVals[n-1]
		_ = outputColVals[n-1]
		for i := range leftColVals[:n] {
			// {{ if _L_HAS_NULLS }}
			isLeftNull := leftNulls.NullAt(uint16(i))
			// {{ else }}
			isLeftNull := false
			// {{ end }}
			// {{ if _R_HAS_NULLS }}
			isRightNull := rightNulls.NullAt(uint16(i))
			// {{ else }}
			isRightNull := false
			// {{ end }}
			leftVal := leftColVals[i]
			rightVal := rightColVals[i]
			if (!leftVal && !isLeftNull) || (!rightVal && !isRightNull) {
				// Rule 1: at least one boolean is FALSE.
				outputColVals[i] = false
				outputNulls.UnsetNull(uint16(i))
			} else if (leftVal && !isLeftNull) && (rightVal && !isRightNull) {
				// Rule 2: both booleans are TRUE.
				outputColVals[i] = true
				outputNulls.UnsetNull(uint16(i))
			} else {
				// Rule 3.
				outputNulls.SetNull(uint16(i))
			}
		}
	}
	// {{ end }}
	// {{/*
}

// */}}

func (a *andOp) Next(ctx context.Context) coldata.Batch {
	batch := a.input.Next(ctx)
	if a.outputIdx == batch.Width() {
		batch.AppendCol(coltypes.Bool)
	}
	n := batch.Length()
	if n == 0 {
		return batch
	}
	leftCol := batch.ColVec(a.leftIdx)
	rightCol := batch.ColVec(a.rightIdx)
	outputCol := batch.ColVec(a.outputIdx)

	leftColVals := leftCol.Bool()
	rightColVals := rightCol.Bool()
	outputColVals := outputCol.Bool()
	outputNulls := outputCol.Nulls()
	if leftCol.MaybeHasNulls() {
		leftNulls := leftCol.Nulls()
		if rightCol.MaybeHasNulls() {
			rightNulls := rightCol.Nulls()
			_SET_VALUES(true, true)
		} else {
			_SET_VALUES(true, false)
		}
	} else {
		if rightCol.MaybeHasNulls() {
			rightNulls := rightCol.Nulls()
			_SET_VALUES(false, true)
		} else {
			_SET_VALUES(false, false)
		}
	}

	return batch
}
