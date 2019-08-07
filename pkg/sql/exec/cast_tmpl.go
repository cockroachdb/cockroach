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

package exec

import (
	"context"
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/typeconv"
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

func _ASSIGN_CAST(to, from interface{}) {
	execerror.VectorizedInternalPanic("")
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.GET

func GetCastOperator(
	input Operator, colIdx int, resultIdx int, fromType *semtypes.T, toType *semtypes.T,
) (Operator, error) {
	switch from := typeconv.FromColumnType(fromType); from {
	// {{ range $typ, $overloads := . }}
	case coltypes._ALLTYPES:
		switch to := typeconv.FromColumnType(toType); to {
		// {{ range $overloads }}
		// {{ if isCastFuncSet . }}
		case coltypes._OVERLOADTYPES:
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
	fromType  coltypes.T
	toType    coltypes.T
}

var _ StaticMemoryOperator = &castOp_FROMTYPE_TOTYPE{}

func (c *castOp_FROMTYPE_TOTYPE) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]coltypes.T{c.toType}, coldata.BatchSize)
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
		batch.AppendCol(coltypes._TOTYPE)
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
					v := execgen.GET(col, int(i))
					var r _GOTYPE
					_ASSIGN_CAST(r, v)
					execgen.SET(projCol, int(i), r)
				}
			}
		} else {
			for execgen.RANGE(i, col) {
				if vecNulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					v := execgen.GET(col, i)
					var r _GOTYPE
					_ASSIGN_CAST(r, v)
					execgen.SET(projCol, int(i), r)
				}
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				v := execgen.GET(col, int(i))
				var r _GOTYPE
				_ASSIGN_CAST(r, v)
				execgen.SET(projCol, int(i), r)
			}
		} else {
			for execgen.RANGE(i, col) {
				v := execgen.GET(col, i)
				var r _GOTYPE
				_ASSIGN_CAST(r, v)
				execgen.SET(projCol, int(i), r)
			}
		}
	}
	return batch
}

// {{end}}
// {{end}}
// {{end}}
