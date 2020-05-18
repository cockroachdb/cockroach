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
// This file is the execgen template for proj_const_{left,right}_ops.eg.go.
// It's formatted in a special way, so it's both valid Go and a valid
// text/template input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
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

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "coldataext" package.
var _ coldataext.Datum

// _LEFT_CANONICAL_TYPE_FAMILY is the template variable.
const _LEFT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _LEFT_TYPE_WIDTH is the template variable.
const _LEFT_TYPE_WIDTH = 0

// _RIGHT_CANONICAL_TYPE_FAMILY is the template variable.
const _RIGHT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _RIGHT_TYPE_WIDTH is the template variable.
const _RIGHT_TYPE_WIDTH = 0

// _NON_CONST_GOTYPESLICE is a template Go type slice variable.
type _NON_CONST_GOTYPESLICE interface{}

// _ASSIGN is the template function for assigning the first input to the result
// of computation an operation on the second and the third inputs.
func _ASSIGN(_, _, _, _, _, _ interface{}) {
	colexecerror.InternalError("")
}

// _RETURN_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _RET_TYP.
func _RETURN_UNSAFEGET(_, _ interface{}) interface{} {
	colexecerror.InternalError("")
}

// */}}

// {{define "projConstOp"}}

type _OP_CONST_NAME struct {
	projConstOpBase
	// {{if _IS_CONST_LEFT}}
	constArg _L_GO_TYPE
	// {{else}}
	constArg _R_GO_TYPE
	// {{end}}
}

func (p _OP_CONST_NAME) Next(ctx context.Context) coldata.Batch {
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
	vec := batch.ColVec(p.colIdx)
	var col _NON_CONST_GOTYPESLICE
	// {{if _IS_CONST_LEFT}}
	col = vec._R_TYP()
	// {{else}}
	col = vec._L_TYP()
	// {{end}}
	projVec := batch.ColVec(p.outputIdx)
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		projVec.Nulls().UnsetNulls()
	}
	projCol := projVec._RET_TYP()
	if vec.Nulls().MaybeHasNulls() {
		_SET_PROJECTION(true)
	} else {
		_SET_PROJECTION(false)
	}
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

func (p _OP_CONST_NAME) Init() {
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
	colNulls := vec.Nulls()
	// {{end}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS)
		}
	} else {
		col = execgen.SLICE(col, 0, n)
		_ = _RETURN_UNSAFEGET(projCol, n-1)
		for execgen.RANGE(i, col, 0, n) {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS)
		}
	}
	// {{if _HAS_NULLS}}
	colNullsCopy := colNulls.Copy()
	projVec.SetNulls(&colNullsCopy)
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
	if !colNulls.NullAt(i) {
		// We only want to perform the projection operation if the value is not null.
		// {{end}}
		arg := execgen.UNSAFEGET(col, i)
		// {{if _IS_CONST_LEFT}}
		_ASSIGN(projCol[i], p.constArg, arg, projCol, _, col)
		// {{else}}
		_ASSIGN(projCol[i], arg, p.constArg, projCol, col, _)
		// {{end}}
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

// {{template "projConstOp" .}}

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

// {{template "projConstOp" .}}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}

// GetProjection_CONST_SIDEConstOperator returns the appropriate constant
// projection operator for the given left and right column types and operation.
func GetProjection_CONST_SIDEConstOperator(
	allocator *colmem.Allocator,
	leftType *types.T,
	rightType *types.T,
	outputType *types.T,
	op tree.Operator,
	input colexecbase.Operator,
	colIdx int,
	constArg tree.Datum,
	outputIdx int,
) (colexecbase.Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, outputType, outputIdx)
	projConstOpBase := projConstOpBase{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    outputIdx,
	}
	var (
		c   interface{}
		err error
	)
	// {{if _IS_CONST_LEFT}}
	c, err = getDatumToPhysicalFn(leftType)(constArg)
	// {{else}}
	c, err = getDatumToPhysicalFn(rightType)(constArg)
	// {{end}}
	if err != nil {
		return nil, err
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
							return &_OP_CONST_NAME{
								projConstOpBase: projConstOpBase,
								// {{if _IS_CONST_LEFT}}
								constArg: c.(_L_GO_TYPE),
								// {{else}}
								constArg: c.(_R_GO_TYPE),
								// {{end}}
							}, nil
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
							return &_OP_CONST_NAME{
								projConstOpBase: projConstOpBase,
								// {{if _IS_CONST_LEFT}}
								constArg: c.(_L_GO_TYPE),
								// {{else}}
								constArg: c.(_R_GO_TYPE),
								// {{end}}
							}, nil
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
