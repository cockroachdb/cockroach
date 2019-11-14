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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// {{/*

type _ALLTYPES interface{}
type _OVERLOADTYPES interface{}
type _TOTYPE interface{}
type _GOTYPE interface{}
type _FROMTYPE interface{}

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

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

func GetCastOperator(
	allocator *Allocator,
	input Operator,
	colIdx int,
	resultIdx int,
	fromType *semtypes.T,
	toType *semtypes.T,
) (Operator, error) {
	if fromType.Family() == semtypes.UnknownFamily {
		return &castOpNullAny{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			colIdx:       colIdx,
			outputIdx:    resultIdx,
			toType:       typeconv.FromColumnType(toType),
		}, nil
	}
	switch from := typeconv.FromColumnType(fromType); from {
	// {{ range $typ, $overloads := . }}
	case coltypes._ALLTYPES:
		switch to := typeconv.FromColumnType(toType); to {
		// {{ range $overloads }}
		// {{ if isCastFuncSet . }}
		case coltypes._OVERLOADTYPES:
			return &castOp_FROMTYPE_TOTYPE{
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
	toType    coltypes.T
}

var _ Operator = &castOpNullAny{}

func (c *castOpNullAny) Init() {
	c.input.Init()
}

func (c *castOpNullAny) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	if c.outputIdx == batch.Width() {
		c.allocator.AppendColumn(batch, c.toType)
	}
	n := batch.Length()
	if n == 0 {
		return batch
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
		for i := uint16(0); i < n; i++ {
			if vecNulls.NullAt(uint16(i)) {
				projNulls.SetNull(uint16(i))
			} else {
				execerror.VectorizedInternalPanic(fmt.Errorf("unexpected non-null at index %d", i))
			}
		}
	}
	return batch
}

// {{ range $typ, $overloads := . }}
// {{ range $overloads }}
// {{ if isCastFuncSet . }}

type castOp_FROMTYPE_TOTYPE struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
	fromType  coltypes.T
	toType    coltypes.T
}

var _ Operator = &castOp_FROMTYPE_TOTYPE{}

func (c *castOp_FROMTYPE_TOTYPE) Init() {
	c.input.Init()
}

func (c *castOp_FROMTYPE_TOTYPE) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}
	if c.outputIdx == batch.Width() {
		c.allocator.AppendColumn(batch, coltypes._TOTYPE)
	}
	vec := batch.ColVec(c.colIdx)
	col := vec._FROMTYPE()
	projVec := batch.ColVec(c.outputIdx)
	projCol := projVec._TOTYPE()
	if vec.MaybeHasNulls() {
		vecNulls := vec.Nulls()
		projNulls := projVec.Nulls()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				if vecNulls.NullAt(i) {
					projNulls.SetNull(i)
				} else {
					v := _FROM_TYPE_UNSAFEGET(col, int(i))
					var r _GOTYPE
					_ASSIGN_CAST(r, v)
					_TO_TYPE_SET(projCol, int(i), r)
				}
			}
		} else {
			col = _FROM_TYPE_SLICE(col, 0, int(n))
			for execgen.RANGE(i, col) {
				if vecNulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					v := _FROM_TYPE_UNSAFEGET(col, int(i))
					var r _GOTYPE
					_ASSIGN_CAST(r, v)
					_TO_TYPE_SET(projCol, int(i), r)
				}
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				v := _FROM_TYPE_UNSAFEGET(col, int(i))
				var r _GOTYPE
				_ASSIGN_CAST(r, v)
				_TO_TYPE_SET(projCol, int(i), r)
			}
		} else {
			col = _FROM_TYPE_SLICE(col, 0, int(n))
			for execgen.RANGE(i, col) {
				v := _FROM_TYPE_UNSAFEGET(col, int(i))
				var r _GOTYPE
				_ASSIGN_CAST(r, v)
				_TO_TYPE_SET(projCol, int(i), r)
			}
		}
	}
	return batch
}

// {{end}}
// {{end}}
// {{end}}
