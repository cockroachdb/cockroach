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
// This file is the execgen template for cast.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// {{/*

type _ALLTYPES interface{}
type _FROMTYPE interface{}
type _TOTYPE interface{}
type _GOTYPE interface{}

var _ apd.Decimal
var _ = math.MaxInt8
var _ tree.Datum

func _ASSIGN_CAST(to, from interface{}) {
	execerror.VectorizedInternalPanic("")
}

// This will be replaced with execgen.UNSAFEGET
func _FROM_TYPE_UNSAFEGET(to, from interface{}) interface{} {
	execerror.VectorizedInternalPanic("")
}

// This will be replaced with execgen.SET.
func _TO_TYPE_SET(to, from interface{}) {
	execerror.VectorizedInternalPanic("")
}

// This will be replaced with execgen.SLICE.
func _FROM_TYPE_SLICE(col, i, j interface{}) interface{} {
	execerror.VectorizedInternalPanic("")
}

// */}}

func cast(fromType, toType coltypes.T, inputVec, outputVec coldata.Vec, n int, sel []int) {
	switch fromType {
	// {{ range $typ, $overloads := . }}
	case coltypes._ALLTYPES:
		switch toType {
		// {{ range $overloads }}
		// {{ if isCastFuncSet . }}
		case coltypes._TOTYPE:
			inputCol := inputVec._FROMTYPE()
			outputCol := outputVec._TOTYPE()
			if inputVec.MaybeHasNulls() {
				inputNulls := inputVec.Nulls()
				outputNulls := outputVec.Nulls()
				if sel != nil {
					sel = sel[:n]
					for _, i := range sel {
						if inputNulls.NullAt(i) {
							outputNulls.SetNull(i)
						} else {
							v := _FROM_TYPE_UNSAFEGET(inputCol, i)
							var r _GOTYPE
							_ASSIGN_CAST(r, v)
							_TO_TYPE_SET(outputCol, i, r)
						}
					}
				} else {
					inputCol = _FROM_TYPE_SLICE(inputCol, 0, n)
					for execgen.RANGE(i, inputCol, 0, n) {
						if inputNulls.NullAt(i) {
							outputNulls.SetNull(i)
						} else {
							v := _FROM_TYPE_UNSAFEGET(inputCol, i)
							var r _GOTYPE
							_ASSIGN_CAST(r, v)
							_TO_TYPE_SET(outputCol, i, r)
						}
					}
				}
			} else {
				if sel != nil {
					sel = sel[:n]
					for _, i := range sel {
						v := _FROM_TYPE_UNSAFEGET(inputCol, i)
						var r _GOTYPE
						_ASSIGN_CAST(r, v)
						_TO_TYPE_SET(outputCol, i, r)
					}
				} else {
					inputCol = _FROM_TYPE_SLICE(inputCol, 0, n)
					for execgen.RANGE(i, inputCol, 0, n) {
						v := _FROM_TYPE_UNSAFEGET(inputCol, i)
						var r _GOTYPE
						_ASSIGN_CAST(r, v)
						_TO_TYPE_SET(outputCol, i, r)
					}
				}
			}
			// {{end}}
			// {{end}}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled cast FROM -> TO type: %s -> %s", fromType, toType))
		}
		// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled FROM type: %s", fromType))
	}
}

func GetCastOperator(
	allocator *Allocator,
	input Operator,
	colIdx int,
	resultIdx int,
	fromType *semtypes.T,
	toType *semtypes.T,
) (Operator, error) {
	to := typeconv.FromColumnType(toType)
	input = newVectorTypeEnforcer(allocator, input, to, resultIdx)
	if fromType.Family() == semtypes.UnknownFamily {
		return &castOpNullAny{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			colIdx:       colIdx,
			outputIdx:    resultIdx,
		}, nil
	}
	switch from := typeconv.FromColumnType(fromType); from {
	// {{ range $typ, $overloads := . }}
	case coltypes._ALLTYPES:
		switch to {
		// {{ range $overloads }}
		// {{ if isCastFuncSet . }}
		case coltypes._TOTYPE:
			return &castOp{
				OneInputNode: NewOneInputNode(input),
				allocator:    allocator,
				colIdx:       colIdx,
				outputIdx:    resultIdx,
				fromType:     from,
				toType:       to,
			}, nil
			// {{end}}
			// {{end}}
		default:
			return nil, errors.Errorf("unhandled cast FROM -> TO type: %s -> %s", from, to)
		}
		// {{end}}
	default:
		return nil, errors.Errorf("unhandled FROM type: %s", from)
	}
}

type castOpNullAny struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
}

var _ Operator = &castOpNullAny{}

func (c *castOpNullAny) Init() {
	c.input.Init()
}

func (c *castOpNullAny) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(c.colIdx)
	projVec := batch.ColVec(c.outputIdx)
	vecNulls := vec.Nulls()
	projNulls := projVec.Nulls()
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			if vecNulls.NullAt(i) {
				projNulls.SetNull(i)
			} else {
				execerror.VectorizedInternalPanic(errors.Errorf("unexpected non-null at index %d", i))
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if vecNulls.NullAt(i) {
				projNulls.SetNull(i)
			} else {
				execerror.VectorizedInternalPanic(fmt.Errorf("unexpected non-null at index %d", i))
			}
		}
	}
	return batch
}

type castOp struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
	fromType  coltypes.T
	toType    coltypes.T
}

var _ Operator = &castOp{}

func (c *castOp) Init() {
	c.input.Init()
}

func (c *castOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(c.colIdx)
	projVec := batch.ColVec(c.outputIdx)
	c.allocator.PerformOperation(
		[]coldata.Vec{projVec}, func() { cast(c.fromType, c.toType, vec, projVec, n, batch.Selection()) },
	)
	return batch
}
