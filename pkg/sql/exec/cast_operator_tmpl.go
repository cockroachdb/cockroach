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
// This file is the execgen template for cast_operator.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
)

// {{/*

type _ALLTYPES interface{}
type _OVERLOADTYPES interface{}
type _TOTYPE interface{}
type _FROMTYPE interface{}

var _ apd.Decimal

//var _ types.T
//var _ bytes.Buffer

func _ASSIGN_CAST(to, from interface{}) {
	panic("")
}

// */}}

func GetCastOperator(
	input Operator, colIdx int, resultIdx int, fromType *semtypes.T, toType *semtypes.T,
) (Operator, error) {
	switch from := conv.FromColumnType(fromType); from {
	// {{ range $typ, $overloads := . }}
	case types._ALLTYPES:
		switch to := conv.FromColumnType(toType); to {
		// {{ range $overloads }}
		// {{ if isCastFuncSet . }}
		case types._OVERLOADTYPES:
			return &castOp_FROMTYPE_TOTYPE{
				OneInputNode: NewOneInputNode(input),
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

// {{ range $typ, $overloads := . }}
// {{ range $overloads }}
// {{ if isCastFuncSet . }}

type castOp_FROMTYPE_TOTYPE struct {
	OneInputNode
	colIdx    int
	outputIdx int
	fromType  types.T
	toType    types.T
}

var _ StaticMemoryOperator = &castOp_FROMTYPE_TOTYPE{}

func (c *castOp_FROMTYPE_TOTYPE) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]types.T{c.toType}, coldata.BatchSize)
}

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
		batch.AppendCol(types._TOTYPE)
	}
	vec := batch.ColVec(c.colIdx)
	col := vec._FROMTYPE()
	projVec := batch.ColVec(c.outputIdx)
	projCol := projVec._TOTYPE()
	if vec.MaybeHasNulls() {
		vecNulls := vec.Nulls()
		projNulls := projVec.Nulls()
		if sel := batch.Selection(); sel != nil {
			for _, i := range sel {
				if vecNulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					_ASSIGN_CAST(projCol[i], col[i])
				}
			}
		} else {
			col = col[:n]
			projCol = projCol[:n]
			for i := range col {
				if vecNulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					_ASSIGN_CAST(projCol[i], col[i])
				}
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			for _, i := range sel {
				_ASSIGN_CAST(projCol[i], col[i])
			}
		} else {
			for i := range col {
				_ASSIGN_CAST(projCol[i], col[i])
			}
		}
	}
	return batch
}

// {{end}}
// {{end}}
// {{end}}
