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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{range .Operations}}
// {{$Operation := .}}

func New_OPERATIONProjOp(
	allocator *colmem.Allocator,
	input, leftProjOpChain, rightProjOpChain colexecop.Operator,
	leftFeedOp, rightFeedOp *colexecop.FeedOperator,
	leftInputType, rightInputType *types.T,
	leftIdx, rightIdx, outputIdx int,
) (colexecop.Operator, error) {
	leftFamily := leftInputType.Family()
	leftIsBool := leftFamily == types.BoolFamily
	leftIsNull := leftFamily == types.UnknownFamily
	rightFamily := rightInputType.Family()
	rightIsBool := rightFamily == types.BoolFamily
	rightIsNull := rightFamily == types.UnknownFamily
	if (!leftIsBool && !leftIsNull && !rightIsBool && !rightIsNull) ||
		(leftIsNull && rightIsNull) {
		// Either one of the families are neither Bool nor Unknown (which is
		// unexpected because the logical operations can be applied only to
		// bools or NULLs) or both families are Unknown (which is unexpected
		// because we assume that the optimizer is smart enough to reduce such
		// cases), so we return an assertion failure.
		return nil, errors.AssertionFailedf(
			"unexpected input families for _OPERATION logical operation: %s, %s", leftFamily, rightFamily,
		)
	}
	if leftIsNull {
		return new_OPERATIONLeftNullProjOp(
			allocator, input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp, leftIdx, rightIdx, outputIdx,
		), nil
	} else if rightIsNull {
		return new_OPERATIONRightNullProjOp(
			allocator, input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp, leftIdx, rightIdx, outputIdx,
		), nil
	} else {
		return new_OPERATIONProjOp(
			allocator, input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp, leftIdx, rightIdx, outputIdx,
		), nil
	}
}

// {{end}}

// {{range .Overloads}}

type _OP_LOWERProjOp struct {
	colexecop.InitHelper

	allocator *colmem.Allocator
	input     colexecop.Operator

	leftProjOpChain  colexecop.Operator
	rightProjOpChain colexecop.Operator
	leftFeedOp       *colexecop.FeedOperator
	rightFeedOp      *colexecop.FeedOperator

	leftIdx   int
	rightIdx  int
	outputIdx int

	// origSel is a buffer used to keep track of the original selection vector of
	// the input batch. We need to do this because we're going to modify the
	// selection vector in order to do the short-circuiting of logical operators.
	origSel []int
}

// new_OP_TITLEProjOp returns a new projection operator that logical-_OP_TITLE's
// the boolean columns at leftIdx and rightIdx, returning the result in
// outputIdx.
func new_OP_TITLEProjOp(
	allocator *colmem.Allocator,
	input, leftProjOpChain, rightProjOpChain colexecop.Operator,
	leftFeedOp, rightFeedOp *colexecop.FeedOperator,
	leftIdx, rightIdx, outputIdx int,
) colexecop.Operator {
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
		colexecerror.InternalError(errors.AssertionFailedf("invalid idx %d", nth))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

// Init is part of the colexecop.Operator interface.
func (o *_OP_LOWERProjOp) Init(ctx context.Context) {
	if !o.InitHelper.Init(ctx) {
		return
	}
	o.input.Init(o.Ctx)
}

// Next is part of the colexecop.Operator interface.
// The idea to handle the short-circuiting logic is similar to what caseOp
// does: a logical operator has an input and two projection chains. First,
// it runs the left chain on the input batch. Then, it "subtracts" the
// tuples for which we know the result of logical operation based only on
// the left side projection (e.g. if the left side is false and we're
// doing AND operation, then the result is also false) and runs the right
// side projection only on the remaining tuples (i.e. those that were not
// "subtracted"). Next, it restores the original selection vector and
// populates the result of the logical operation.
func (o *_OP_LOWERProjOp) Next() coldata.Batch {
	batch := o.input.Next()
	origLen := batch.Length()
	if origLen == 0 {
		return coldata.ZeroBatch
	}
	usesSel := false
	if sel := batch.Selection(); sel != nil {
		o.origSel = colexecutils.EnsureSelectionVectorLength(o.origSel, origLen)
		copy(o.origSel, sel)
		usesSel = true
	}

	// In order to support the short-circuiting logic, we need to be quite tricky
	// here. First, we set the input batch for the left projection to run and
	// actually run the projection.
	o.leftFeedOp.SetBatch(batch)
	batch = o.leftProjOpChain.Next()

	// Now we need to populate a selection vector on the batch in such a way that
	// those tuples that we already know the result of logical operation for do
	// not get the projection for the right side.
	//
	// knownResult indicates the boolean value which if present on the left side
	// fully determines the result of the logical operation.
	var (
		knownResult                   bool
		leftVec, rightVec             coldata.Vec
		leftValIsNull, rightValIsNull bool
		leftVal, rightVal             bool
	)
	// {{if _IS_OR_OP}}
	knownResult = true
	// {{end}}

	leftVec = batch.ColVec(o.leftIdx)
	var curIdx int
	// {{if _L_IS_NULL_VECTOR}}
	// Left vector represents a constant NULL value, so we need to include
	// all of the tuples in the batch for the right side to be evaluated.
	if usesSel {
		sel := batch.Selection()
		copy(sel[:origLen], o.origSel[:origLen])
	} else {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < origLen; i++ {
			sel[i] = i
		}
	}
	curIdx = origLen
	// {{else}}
	leftVals := leftVec.Bool()
	if usesSel {
		sel := batch.Selection()
		origSel := o.origSel[:origLen]
		if leftVec.MaybeHasNulls() {
			leftNulls := leftVec.Nulls()
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
		if leftVec.MaybeHasNulls() {
			leftNulls := leftVec.Nulls()
			for i := 0; i < origLen; i++ {
				_ADD_TUPLE_FOR_RIGHT(true)
			}
		} else {
			for i := 0; i < origLen; i++ {
				_ADD_TUPLE_FOR_RIGHT(false)
			}
		}
	}
	// {{end}}

	// {{if not _R_IS_NULL_VECTOR}}
	// {{/* We don't need to run the right side if we have a NULL vector on the right. */}}
	var rightVals []bool
	if curIdx > 0 {
		// We only run the right-side projection if there are non-zero number of
		// remaining tuples.
		batch.SetLength(curIdx)
		o.rightFeedOp.SetBatch(batch)
		batch = o.rightProjOpChain.Next()
		rightVec = batch.ColVec(o.rightIdx)
		rightVals = rightVec.Bool()
	}
	// {{end}}

	// Now we need to restore the original selection vector and length.
	if usesSel {
		sel := batch.Selection()
		copy(sel[:origLen], o.origSel[:origLen])
	} else {
		batch.SetSelection(false)
	}
	batch.SetLength(origLen)

	outputCol := batch.ColVec(o.outputIdx)
	outputVals := outputCol.Bool()
	outputNulls := outputCol.Nulls()
	if outputCol.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		outputNulls.UnsetNulls()
	}
	// This is where we populate the output - do the actual evaluation of the
	// logical operation.
	if leftVec.MaybeHasNulls() {
		// {{if not _L_IS_NULL_VECTOR}}
		leftNulls := leftVec.Nulls()
		// {{end}}
		if rightVec != nil && rightVec.MaybeHasNulls() {
			// {{if not _R_IS_NULL_VECTOR}}
			rightNulls := rightVec.Nulls()
			// {{end}}
			_SET_VALUES(_IS_OR_OP, _L_IS_NULL_VECTOR, _R_IS_NULL_VECTOR, true, true)
		} else {
			_SET_VALUES(_IS_OR_OP, _L_IS_NULL_VECTOR, _R_IS_NULL_VECTOR, true, false)
		}
	} else {
		if rightVec != nil && rightVec.MaybeHasNulls() {
			// {{if not _R_IS_NULL_VECTOR}}
			rightNulls := rightVec.Nulls()
			// {{end}}
			_SET_VALUES(_IS_OR_OP, _L_IS_NULL_VECTOR, _R_IS_NULL_VECTOR, false, true)
		} else {
			_SET_VALUES(_IS_OR_OP, _L_IS_NULL_VECTOR, _R_IS_NULL_VECTOR, false, false)
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
	leftValIsNull = leftNulls.NullAt(i)
	// {{else}}
	leftValIsNull = false
	// {{end}}
	if leftValIsNull || leftVals[i] != knownResult {
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
// to two vectors (either of which could be a boolean vector or a NULL vector,
// but both cannot be NULL vectors at the same time) while paying attention to
// null values.
func _SET_VALUES(
	_IS_OR_OP bool,
	_L_IS_NULL_VECTOR bool,
	_R_IS_NULL_VECTOR bool,
	_L_HAS_NULLS bool,
	_R_HAS_NULLS bool,
) { // */}}
	// {{define "setValues" -}}
	if sel := batch.Selection(); sel != nil {
		for _, idx := range sel[:origLen] {
			_SET_SINGLE_VALUE(_IS_OR_OP, _L_IS_NULL_VECTOR, _R_IS_NULL_VECTOR, _L_HAS_NULLS, _R_HAS_NULLS)
		}
	} else {
		// {{if not _L_IS_NULL_VECTOR}}
		_ = leftVals[origLen-1]
		// {{end}}
		// {{if not _R_IS_NULL_VECTOR}}
		if rightVals != nil {
			_ = rightVals[origLen-1]
		}
		// {{end}}
		_ = outputVals[origLen-1]
		for idx := 0; idx < origLen; idx++ {
			_SET_SINGLE_VALUE(_IS_OR_OP, _L_IS_NULL_VECTOR, _R_IS_NULL_VECTOR, _L_HAS_NULLS, _R_HAS_NULLS)
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet sets the result of applying a logical operation AND or OR
// to two boolean values which can be null.
func _SET_SINGLE_VALUE(
	_IS_OR_OP bool,
	_L_IS_NULL_VECTOR bool,
	_R_IS_NULL_VECTOR bool,
	_L_HAS_NULLS bool,
	_R_HAS_NULLS bool,
) { // */}}
	// {{define "setSingleValue" -}}
	// {{if _L_IS_NULL_VECTOR}}
	leftValIsNull = true
	// {{else}}
	// {{if _L_HAS_NULLS}}
	leftValIsNull = leftNulls.NullAt(idx)
	// {{else}}
	leftValIsNull = false
	// {{end}}
	leftVal = leftVals[idx]
	// {{end}}
	if !leftValIsNull && leftVal == knownResult {
		// In this case, the result is fully determined by the left argument,
		// so the right argument wasn't even evaluated.
		outputVals[idx] = leftVal
	} else {
		// {{if _R_IS_NULL_VECTOR}}
		rightValIsNull = true
		// {{else}}
		// {{if _R_HAS_NULLS}}
		rightValIsNull = rightNulls.NullAt(idx)
		// {{else}}
		rightValIsNull = false
		// {{end}}
		rightVal = rightVals[idx]
		// {{end}}
		// The rules for performing a logical operation on two booleans are:
		// 1. if at least one of the values is "knownResult", then the result
		// is also "knownResult"
		// 2. if both values are the opposite of "knownResult", then the result
		// is also the opposite of "knownResult"
		// 3. in all other cases, the result is NULL.
		if (!leftValIsNull && leftVal == knownResult) || (!rightValIsNull && rightVal == knownResult) {
			outputVals[idx] = knownResult
		} else if (!leftValIsNull && leftVal != knownResult) && (!rightValIsNull && rightVal != knownResult) {
			outputVals[idx] = !knownResult
		} else {
			outputNulls.SetNull(idx)
		}
	}
	// {{end}}
	// {{/*
}

// */}}
