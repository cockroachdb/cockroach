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
// This file is the execgen template for proj_non_const_ops.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _LEFT_CANONICAL_TYPE_FAMILY is the template variable.
const _LEFT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _LEFT_TYPE_WIDTH is the template variable.
const _LEFT_TYPE_WIDTH = 0

// _RIGHT_CANONICAL_TYPE_FAMILY is the template variable.
const _RIGHT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _RIGHT_TYPE_WIDTH is the template variable.
const _RIGHT_TYPE_WIDTH = 0

// _ASSIGN is the template function for assigning the first input to the result
// of computation an operation on the second and the third inputs.
func _ASSIGN(_, _, _ interface{}) {
	colexecerror.InternalError("")
}

// _L_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _L_TYP.
func _L_UNSAFEGET(_, _ interface{}) interface{} {
	colexecerror.InternalError("")
}

// _R_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _R_TYP.
func _R_UNSAFEGET(_, _ interface{}) interface{} {
	colexecerror.InternalError("")
}

// _RETURN_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _RET_TYP.
func _RETURN_UNSAFEGET(_, _ interface{}) interface{} {
	colexecerror.InternalError("")
}

// */}}

// projConstOpBase contains all of the fields for projections with a constant,
// except for the constant itself.
// NOTE: this struct should be declared in proj_const_ops_tmpl.go, but if we do
// so, it'll be redeclared because we execute that template twice. To go
// around the problem we specify it here.
type projConstOpBase struct {
	OneInputNode
	allocator      *colmem.Allocator
	colIdx         int
	outputIdx      int
	decimalScratch decimalOverloadScratch
}

// projOpBase contains all of the fields for non-constant projections.
type projOpBase struct {
	OneInputNode
	allocator      *colmem.Allocator
	col1Idx        int
	col2Idx        int
	outputIdx      int
	decimalScratch decimalOverloadScratch
}

// {{define "projOp"}}

type _OP_NAME struct {
	projOpBase
}

func (p _OP_NAME) Next(ctx context.Context) coldata.Batch {
	// In order to inline the templated code of overloads, we need to have a
	// `decimalScratch` local variable of type `decimalOverloadScratch`.
	decimalScratch := p.decimalScratch
	// However, the scratch is not used in all of the projection operators, so
	// we add this to go around "unused" error.
	_ = decimalScratch
	batch := p.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	projVec := batch.ColVec(p.outputIdx)
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		projVec.Nulls().UnsetNulls()
	}
	projCol := projVec._RET_TYP()
	vec1 := batch.ColVec(p.col1Idx)
	vec2 := batch.ColVec(p.col2Idx)
	col1 := vec1._L_TYP()
	col2 := vec2._R_TYP()
	if vec1.Nulls().MaybeHasNulls() || vec2.Nulls().MaybeHasNulls() {
		_SET_PROJECTION(true)
	} else {
		_SET_PROJECTION(false)
	}

	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

func (p _OP_NAME) Init() {
	p.input.Init()
}

// {{end}}

// {{/*
func _SET_PROJECTION(_HAS_NULLS bool) {
	// */}}
	// {{define "setProjection" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	// {{if _HAS_NULLS}}
	col1Nulls := vec1.Nulls()
	col2Nulls := vec2.Nulls()
	// {{end}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS)
		}
	} else {
		// {{if not (eq .Left.VecMethod "Bytes")}}
		// {{/* Slice is a noop for Bytes type, so colLen below might contain an
		// incorrect value. In order to keep bounds check elimination for all other
		// types, we simply omit this code snippet for Bytes. */}}
		col1 = execgen.SLICE(col1, 0, n)
		colLen := execgen.LEN(col1)
		_ = _RETURN_UNSAFEGET(projCol, colLen-1)
		_ = _R_UNSAFEGET(col2, colLen-1)
		// {{end}}
		for execgen.RANGE(i, col1, 0, n) {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS)
		}
	}
	// {{if _HAS_NULLS}}
	projVec.SetNulls(col1Nulls.Or(col2Nulls))
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
func _SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS bool) { // */}}
	// {{define "setSingleTupleProjection" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	// {{if _HAS_NULLS}}
	if !col1Nulls.NullAt(i) && !col2Nulls.NullAt(i) {
		// We only want to perform the projection operation if both values are not
		// null.
		// {{end}}
		arg1 := _L_UNSAFEGET(col1, i)
		arg2 := _R_UNSAFEGET(col2, i)
		_ASSIGN(projCol[i], arg1, arg2)
		// {{if _HAS_NULLS}}
	}
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{range .BinOps}}
// {{range .LeftFamilies}}
// {{range .LeftWidths}}
// {{range .RightFamilies}}
// {{range .RightWidths}}

// {{template "projOp" .}}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}

// {{range .CmpOps}}
// {{range .LeftFamilies}}
// {{range .LeftWidths}}
// {{range .RightFamilies}}
// {{range .RightWidths}}

// {{template "projOp" .}}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}

// GetProjectionOperator returns the appropriate projection operator for the
// given left and right column types and operation.
func GetProjectionOperator(
	allocator *colmem.Allocator,
	leftType *types.T,
	rightType *types.T,
	outputType *types.T,
	op tree.Operator,
	input execinfra.Operator,
	col1Idx int,
	col2Idx int,
	outputIdx int,
) (execinfra.Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, outputType, outputIdx)
	projOpBase := projOpBase{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		col1Idx:      col1Idx,
		col2Idx:      col2Idx,
		outputIdx:    outputIdx,
	}

	switch op.(type) {
	case tree.BinaryOperator:
		switch op {
		// {{range .BinOps}}
		case tree._NAME:
			switch typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) {
			// {{range .LeftFamilies}}
			case _LEFT_CANONICAL_TYPE_FAMILY:
				switch leftType.Width() {
				// {{range .LeftWidths}}
				case _LEFT_TYPE_WIDTH:
					switch typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) {
					// {{range .RightFamilies}}
					case _RIGHT_CANONICAL_TYPE_FAMILY:
						switch rightType.Width() {
						// {{range .RightWidths}}
						case _RIGHT_TYPE_WIDTH:
							return &_OP_NAME{projOpBase: projOpBase}, nil
							// {{end}}
						}
						// {{end}}
					}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
	case tree.ComparisonOperator:
		switch op {
		// {{range .CmpOps}}
		case tree._NAME:
			switch typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) {
			// {{range .LeftFamilies}}
			case _LEFT_CANONICAL_TYPE_FAMILY:
				switch leftType.Width() {
				// {{range .LeftWidths}}
				case _LEFT_TYPE_WIDTH:
					switch typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) {
					// {{range .RightFamilies}}
					case _RIGHT_CANONICAL_TYPE_FAMILY:
						switch rightType.Width() {
						// {{range .RightWidths}}
						case _RIGHT_TYPE_WIDTH:
							return &_OP_NAME{projOpBase: projOpBase}, nil
							// {{end}}
						}
						// {{end}}
					}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
	}
	return nil, errors.Errorf("couldn't find overload for %s %s %s", leftType.Name(), op, rightType.Name())
}
