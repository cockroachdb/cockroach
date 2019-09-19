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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "context" package.
var _ context.Context

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "coldata" package.
var _ = coldata.BatchSize

// Dummy import to pull in "coltypes" package.
var _ = coltypes.Bool

// Dummy import to pull in "execerror" package.
var _ = execerror.VectorizedInternalPanic

// Dummy import to pull in "tree" package.
var _ tree.Datum

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
