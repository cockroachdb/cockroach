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
// This file is the execgen template for and_or_projection.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
)

// {{range .}}

type _OP_LOWERProjOp struct {
	allocator *colmem.Allocator
	input     colexecbase.Operator

	leftProjOpChain  colexecbase.Operator
	rightProjOpChain colexecbase.Operator
	leftFeedOp       *feedOperator
	rightFeedOp      *feedOperator

	leftIdx   int
	rightIdx  int
	outputIdx int

	// origSel is a buffer used to keep track of the original selection vector of
	// the input batch. We need to do this because we're going to modify the
	// selection vector in order to do the short-circuiting of logical operators.
	origSel []int
}

// New_OP_TITLEProjOp returns a new projection operator that logical-_OP_TITLE's
// the boolean columns at leftIdx and rightIdx, returning the result in
// outputIdx.
func New_OP_TITLEProjOp(
	allocator *colmem.Allocator,
	input, leftProjOpChain, rightProjOpChain colexecbase.Operator,
	leftFeedOp, rightFeedOp *feedOperator,
	leftIdx, rightIdx, outputIdx int,
) colexecbase.Operator {
	return &_OP_LOWERProjOp{
		allocator:        allocator,
		input:            input,
		leftProjOpChain:  leftProjOpChain,
		rightProjOpChain: rightProjOpChain,
		leftFeedOp:       leftFeedOp,
		rightFeedOp:      rightFeedOp,
		leftIdx:          leftIdx,
		rightIdx:         rightIdx,
		outputIdx:        outputIdx,
		origSel:          make([]int, coldata.BatchSize()),
	}
}

func (o *_OP_LOWERProjOp) ChildCount(verbose bool) int {
	return 3
}

func (o *_OP_LOWERProjOp) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		return o.input
	case 1:
		return o.leftProjOpChain
	case 2:
		return o.rightProjOpChain
	default:
		colexecerror.InternalError(fmt.Sprintf("invalid idx %d", nth))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

func (o *_OP_LOWERProjOp) Init() {
	o.input.Init()
}

// Next is part of the Operator interface.
// The idea to handle the short-circuiting logic is similar to what caseOp
// does: a logical operator has an input and two projection chains. First,
// it runs the left chain on the input batch. Then, it "subtracts" the
// tuples for which we know the result of logical operation based only on
// the left side projection (e.g. if the left side is false and we're
// doing AND operation, then the result is also false) and runs the right
// side projection only on the remaining tuples (i.e. those that were not
// "subtracted"). Next, it restores the original selection vector and
// populates the result of the logical operation.
func (o *_OP_LOWERProjOp) Next(ctx context.Context) coldata.Batch {
	batch := o.input.Next(ctx)
	origLen := batch.Length()
	if origLen == 0 {
		return coldata.ZeroBatch
	}
	usesSel := false
	if sel := batch.Selection(); sel != nil {
		copy(o.origSel[:origLen], sel[:origLen])
		usesSel = true
	}

	// In order to support the short-circuiting logic, we need to be quite tricky
	// here. First, we set the input batch for the left projection to run and
	// actually run the projection.
	o.leftFeedOp.batch = batch
	batch = o.leftProjOpChain.Next(ctx)

	// Now we need to populate a selection vector on the batch in such a way that
	// those tuples that we already know the result of logical operation for do
	// not get the projection for the right side.
	//
	// knownResult indicates the boolean value which if present on the left side
	// fully determines the result of the logical operation.
	var (
		knownResult             bool
		isLeftNull, isRightNull bool
	)
	// {{if _IS_OR_OP}}
	knownResult = true
	// {{end}}
	leftCol := batch.ColVec(o.leftIdx)
	leftColVals := leftCol.Bool()
	var curIdx int
	if usesSel {
		sel := batch.Selection()
		origSel := o.origSel[:origLen]
		if leftCol.MaybeHasNulls() {
			leftNulls := leftCol.Nulls()
			for _, i := range origSel {
				_ADD_TUPLE_FOR_RIGHT(true)
			}
		} else {
			for _, i := range origSel {
				_ADD_TUPLE_FOR_RIGHT(false)
			}
		}
	} else {
		batch.SetSelection(true)
		sel := batch.Selection()
		if leftCol.MaybeHasNulls() {
			leftNulls := leftCol.Nulls()
			for i := 0; i < origLen; i++ {
				_ADD_TUPLE_FOR_RIGHT(true)
			}
		} else {
			for i := 0; i < origLen; i++ {
				_ADD_TUPLE_FOR_RIGHT(false)
			}
		}
	}

	var ranRightSide bool
	if curIdx > 0 {
		// We only run the right-side projection if there are non-zero number of
		// remaining tuples.
		batch.SetLength(curIdx)
		o.rightFeedOp.batch = batch
		batch = o.rightProjOpChain.Next(ctx)
		ranRightSide = true
	}

	// Now we need to restore the original selection vector and length.
	if usesSel {
		sel := batch.Selection()
		copy(sel[:origLen], o.origSel[:origLen])
	} else {
		batch.SetSelection(false)
	}
	batch.SetLength(origLen)

	var (
		rightCol     coldata.Vec
		rightColVals []bool
	)
	if ranRightSide {
		rightCol = batch.ColVec(o.rightIdx)
		rightColVals = rightCol.Bool()
	}
	outputCol := batch.ColVec(o.outputIdx)
	outputColVals := outputCol.Bool()
	outputNulls := outputCol.Nulls()
	if outputCol.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		outputNulls.UnsetNulls()
	}
	// This is where we populate the output - do the actual evaluation of the
	// logical operation.
	if leftCol.MaybeHasNulls() {
		leftNulls := leftCol.Nulls()
		if rightCol != nil && rightCol.MaybeHasNulls() {
			rightNulls := rightCol.Nulls()
			_SET_VALUES(_IS_OR_OP, true, true)
		} else {
			_SET_VALUES(_IS_OR_OP, true, false)
		}
	} else {
		if rightCol != nil && rightCol.MaybeHasNulls() {
			rightNulls := rightCol.Nulls()
			_SET_VALUES(_IS_OR_OP, false, true)
		} else {
			_SET_VALUES(_IS_OR_OP, false, false)
		}
	}

	return batch
}

// {{end}}

// {{/*
// This code snippet decides whether to include the tuple with index i into
// the selection vector to be used by the right side projection. The tuple is
// excluded if we already know the result of logical operation (i.e. we do the
// short-circuiting for it).
func _ADD_TUPLE_FOR_RIGHT(_L_HAS_NULLS bool) { // */}}
	// {{define "addTupleForRight" -}}
	// {{if _L_HAS_NULLS}}
	isLeftNull = leftNulls.NullAt(i)
	// {{else}}
	isLeftNull = false
	// {{end}}
	if isLeftNull || leftColVals[i] != knownResult {
		// We add the tuple into the selection vector if the left value is NULL or
		// it is different from knownResult.
		sel[curIdx] = i
		curIdx++
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet sets the result of applying a logical operation AND or OR
// to two boolean vectors while paying attention to null values.
func _SET_VALUES(_IS_OR_OP bool, _L_HAS_NULLS bool, _R_HAS_NULLS bool) { // */}}
	// {{define "setValues" -}}
	if sel := batch.Selection(); sel != nil {
		for _, idx := range sel[:origLen] {
			_SET_SINGLE_VALUE(_IS_OR_OP, _L_HAS_NULLS, _R_HAS_NULLS)
		}
	} else {
		if ranRightSide {
			_ = rightColVals[origLen-1]
		}
		_ = outputColVals[origLen-1]
		for idx := range leftColVals[:origLen] {
			_SET_SINGLE_VALUE(_IS_OR_OP, _L_HAS_NULLS, _R_HAS_NULLS)
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet sets the result of applying a logical operation AND or OR
// to two boolean values which can be null.
func _SET_SINGLE_VALUE(_IS_OR_OP bool, _L_HAS_NULLS bool, _R_HAS_NULLS bool) { // */}}
	// {{define "setSingleValue" -}}
	// {{if _L_HAS_NULLS}}
	isLeftNull = leftNulls.NullAt(idx)
	// {{else}}
	isLeftNull = false
	// {{end}}
	leftVal := leftColVals[idx]
	if !isLeftNull && leftVal == knownResult {
		outputColVals[idx] = leftVal
	} else {
		// {{if _R_HAS_NULLS}}
		isRightNull = rightNulls.NullAt(idx)
		// {{else}}
		isRightNull = false
		// {{end}}
		rightVal := rightColVals[idx]
		// {{if _IS_OR_OP}}
		// The rules for OR'ing two booleans are:
		// 1. if at least one of the values is TRUE, then the result is also TRUE
		// 2. if both values are FALSE, then the result is also FALSE
		// 3. in all other cases (one is FALSE and the other is NULL or both are NULL),
		//    the result is NULL.
		if (leftVal && !isLeftNull) || (rightVal && !isRightNull) {
			// Rule 1: at least one boolean is TRUE.
			outputColVals[idx] = true
		} else if (!leftVal && !isLeftNull) && (!rightVal && !isRightNull) {
			// Rule 2: both booleans are FALSE.
			outputColVals[idx] = false
		} else {
			// Rule 3.
			outputNulls.SetNull(idx)
		}
		// {{else}}
		// The rules for AND'ing two booleans are:
		// 1. if at least one of the values is FALSE, then the result is also FALSE
		// 2. if both values are TRUE, then the result is also TRUE
		// 3. in all other cases (one is TRUE and the other is NULL or both are NULL),
		//    the result is NULL.
		if (!leftVal && !isLeftNull) || (!rightVal && !isRightNull) {
			// Rule 1: at least one boolean is FALSE.
			outputColVals[idx] = false
		} else if (leftVal && !isLeftNull) && (rightVal && !isRightNull) {
			// Rule 2: both booleans are TRUE.
			outputColVals[idx] = true
		} else {
			// Rule 3.
			outputNulls.SetNull(idx)
		}
		// {{end}}
	}
	// {{end}}
	// {{/*
}

// */}}
