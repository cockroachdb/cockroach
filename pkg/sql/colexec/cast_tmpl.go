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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// {{/*

type _ALLTYPES interface{}
type _FROMTYPE interface{}
type _TOTYPE interface{}
type _GOTYPE interface{}

var _ apd.Decimal
var _ = math.MaxInt8
var _ tree.Datum
var _ coltypes.T

func _ASSIGN_CAST(to, from interface{}) {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.UNSAFEGET
func _FROM_TYPE_UNSAFEGET(to, from interface{}) interface{} {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.SET.
func _TO_TYPE_SET(to, from interface{}) {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.SLICE.
func _FROM_TYPE_SLICE(col, i, j interface{}) interface{} {
	colexecerror.InternalError("")
}

// */}}

func cast(fromType, toType *types.T, inputVec, outputVec coldata.Vec, n int, sel []int) {
	switch typeconv.FromColumnType(fromType) {
	// {{ range $typ, $overloads := . }}
	case coltypes._ALLTYPES:
		switch typeconv.FromColumnType(toType) {
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
			colexecerror.InternalError(fmt.Sprintf("unhandled cast FROM -> TO type: %s -> %s", fromType, toType))
		}
		// {{end}}
	default:
		colexecerror.InternalError(fmt.Sprintf("unhandled FROM type: %s", fromType))
	}
}

func GetCastOperator(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	colIdx int,
	resultIdx int,
	fromType *types.T,
	toType *types.T,
) (colexecbase.Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, toType, resultIdx)
	if fromType.Family() == types.UnknownFamily {
		return &castOpNullAny{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			colIdx:       colIdx,
			outputIdx:    resultIdx,
		}, nil
	}
	switch typeconv.FromColumnType(fromType) {
	// {{ range $typ, $overloads := . }}
	case coltypes._ALLTYPES:
		switch typeconv.FromColumnType(toType) {
		// {{ range $overloads }}
		// {{ if isCastFuncSet . }}
		case coltypes._TOTYPE:
			return &castOp{
				OneInputNode: NewOneInputNode(input),
				allocator:    allocator,
				colIdx:       colIdx,
				outputIdx:    resultIdx,
				fromType:     fromType,
				toType:       toType,
			}, nil
			// {{end}}
			// {{end}}
		default:
			return nil, errors.Errorf("unhandled cast FROM -> TO type: %s -> %s", fromType, toType)
		}
		// {{end}}
	default:
		return nil, errors.Errorf("unhandled FROM type: %s", fromType)
	}
}

type castOpNullAny struct {
	OneInputNode
	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
}

var _ colexecbase.Operator = &castOpNullAny{}

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
				colexecerror.InternalError(errors.Errorf("unexpected non-null at index %d", i))
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if vecNulls.NullAt(i) {
				projNulls.SetNull(i)
			} else {
				colexecerror.InternalError(fmt.Errorf("unexpected non-null at index %d", i))
			}
		}
	}
	return batch
}

type castOp struct {
	OneInputNode
	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
	fromType  *types.T
	toType    *types.T
}

var _ colexecbase.Operator = &castOp{}

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
