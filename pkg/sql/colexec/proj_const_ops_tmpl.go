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
// This file is the execgen template for a projection operator with one
// constant argument. It is separated from proj_const_ops_gen_tmpl.go because
// this template is also used by other template files.
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
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

func (p _OP_CONST_NAME) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]coltypes.T{coltypes._RET_TYP}, coldata.BatchSize)
}

func (p _OP_CONST_NAME) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	n := batch.Length()
	if p.outputIdx == batch.Width() {
		batch.AppendCol(coltypes._RET_TYP)
	}
	if n == 0 {
		return batch
	}
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
	projColNulls := projVec.Nulls()
	colNulls := vec.Nulls()
	// {{end}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			// {{if _HAS_NULLS}}
			if colNulls.NullAt(i) {
				projColNulls.SetNull(i)
			} else
			// {{end}}
			{
				arg := execgen.UNSAFEGET(col, int(i))
				// {{if _IS_CONST_LEFT}}
				_ASSIGN("projCol[i]", "p.constArg", "arg")
				// {{else}}
				_ASSIGN("projCol[i]", "arg", "p.constArg")
				// {{end}}
			}
		}
	} else {
		col = execgen.SLICE(col, 0, int(n))
		colLen := execgen.LEN(col)
		_ = _RET_UNSAFEGET(projCol, colLen-1)
		for execgen.RANGE(i, col) {
			// {{if _HAS_NULLS}}
			if colNulls.NullAt(uint16(i)) {
				projColNulls.SetNull(uint16(i))
			} else
			// {{end}}
			{
				arg := execgen.UNSAFEGET(col, i)
				// {{if _IS_CONST_LEFT}}
				_ASSIGN("projCol[i]", "p.constArg", "arg")
				// {{else}}
				_ASSIGN("projCol[i]", "arg", "p.constArg")
				// {{end}}
			}
		}
	}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}
