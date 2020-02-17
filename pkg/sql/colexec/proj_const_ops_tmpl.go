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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
)

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

// Dummy import to pull in "coltypes" package.
var _ coltypes.T

// _ASSIGN is the template function for assigning the first input to the result
// of computation an operation on the second and the third inputs.
func _ASSIGN(_, _, _ interface{}) {
	execerror.VectorizedInternalPanic("")
}

// _RET_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _RET_TYP.
func _RET_UNSAFEGET(_, _ interface{}) interface{} {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{define "projConstOp" }}

type _OP_CONST_NAME struct {
	projConstOpBase
	// {{ if _IS_CONST_LEFT }}
	constArg _L_GO_TYPE
	// {{ else }}
	constArg _R_GO_TYPE
	// {{ end }}
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
	p.allocator.MaybeAddColumn(batch, coltypes._RET_TYP, p.outputIdx)
	vec := batch.ColVec(p.colIdx)
	// {{if _IS_CONST_LEFT}}
	col := vec._R_TYP()
	// {{else}}
	col := vec._L_TYP()
	// {{end}}
	projVec := batch.ColVec(p.outputIdx)
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
		col = execgen.SLICE(col, 0, int(n))
		_ = _RET_UNSAFEGET(projCol, int(n)-1)
		for execgen.RANGE(i, col, 0, int(n)) {
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
	if !colNulls.NullAt(uint16(i)) {
		// We only want to perform the projection operation if the value is not null.
		// {{end}}
		arg := execgen.UNSAFEGET(col, int(i))
		// {{if _IS_CONST_LEFT}}
		_ASSIGN("projCol[i]", "p.constArg", "arg")
		// {{else}}
		_ASSIGN("projCol[i]", "arg", "p.constArg")
		// {{end}}
		// {{if _HAS_NULLS }}
	}
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// The outer range is a coltypes.T (the left type). The middle range is also a
// coltypes.T (the right type). The inner is the overloads associated with
// those two types.
// */}}
// {{range .}}
// {{range .}}
// {{range .}}

// {{template "projConstOp" .}}

// {{end}}
// {{end}}
// {{end}}

// GetProjection_CONST_SIDEConstOperator returns the appropriate constant
// projection operator for the given left and right column types and operation.
func GetProjection_CONST_SIDEConstOperator(
	allocator *Allocator,
	leftColType *types.T,
	rightColType *types.T,
	op tree.Operator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
	outputIdx int,
) (Operator, error) {
	projConstOpBase := projConstOpBase{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    outputIdx,
	}
	// {{if _IS_CONST_LEFT}}
	c, err := typeconv.GetDatumToPhysicalFn(leftColType)(constArg)
	// {{else}}
	c, err := typeconv.GetDatumToPhysicalFn(rightColType)(constArg)
	// {{end}}
	if err != nil {
		return nil, err
	}
	switch leftType := typeconv.FromColumnType(leftColType); leftType {
	// {{range $lTyp, $rTypToOverloads := .}}
	case coltypes._L_TYP_VAR:
		switch rightType := typeconv.FromColumnType(rightColType); rightType {
		// {{range $rTyp, $overloads := $rTypToOverloads}}
		case coltypes._R_TYP_VAR:
			switch op.(type) {
			case tree.BinaryOperator:
				switch op {
				// {{range $overloads}}
				// {{if .IsBinOp}}
				case tree._NAME:
					return &_OP_CONST_NAME{
						projConstOpBase: projConstOpBase,
						// {{if _IS_CONST_LEFT}}
						constArg: c.(_L_GO_TYPE),
						// {{else}}
						constArg: c.(_R_GO_TYPE),
						// {{end}}
					}, nil
				// {{end}}
				// {{end}}
				default:
					return nil, errors.Errorf("unhandled binary operator: %s", op)
				}
			case tree.ComparisonOperator:
				switch op {
				// {{range $overloads}}
				// {{if .IsCmpOp}}
				case tree._NAME:
					return &_OP_CONST_NAME{
						projConstOpBase: projConstOpBase,
						// {{if _IS_CONST_LEFT}}
						constArg: c.(_L_GO_TYPE),
						// {{else}}
						constArg: c.(_R_GO_TYPE),
						// {{end}}
					}, nil
				// {{end}}
				// {{end}}
				default:
					return nil, errors.Errorf("unhandled comparison operator: %s", op)
				}
			default:
				return nil, errors.New("unhandled operator type")
			}
		// {{end}}
		default:
			return nil, errors.Errorf("unhandled right type: %s", rightType)
		}
	// {{end}}
	default:
		return nil, errors.Errorf("unhandled left type: %s", leftType)
	}
}
