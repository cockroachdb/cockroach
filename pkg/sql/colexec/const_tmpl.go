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
// This file is the execgen template for const.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
)

// Remove unused warning.
var _ = execgen.UNSAFEGET

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// */}}

// NewConstOp creates a new operator that produces a constant value constVal of
// type t at index outputIdx.
func NewConstOp(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	t *types.T,
	constVal interface{},
	outputIdx int,
) (colexecbase.Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, t, outputIdx)
	switch typeconv.FromColumnType(t) {
	// {{range .}}
	case _TYPES_T:
		return &const_TYPEOp{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			outputIdx:    outputIdx,
			constVal:     constVal.(_GOTYPE),
		}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported const type %s", t)
	}
}

// {{range .}}

type const_TYPEOp struct {
	OneInputNode

	allocator *colmem.Allocator
	outputIdx int
	constVal  _GOTYPE
}

func (c const_TYPEOp) Init() {
	c.input.Init()
}

func (c const_TYPEOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(c.outputIdx)
	col := vec._TemplateType()
	c.allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			if sel := batch.Selection(); sel != nil {
				for _, i := range sel[:n] {
					execgen.SET(col, i, c.constVal)
				}
			} else {
				col = execgen.SLICE(col, 0, n)
				for execgen.RANGE(i, col, 0, n) {
					execgen.SET(col, i, c.constVal)
				}
			}
		},
	)
	return batch
}

// {{end}}

// NewConstNullOp creates a new operator that produces a constant (untyped) NULL
// value at index outputIdx.
func NewConstNullOp(
	allocator *colmem.Allocator, input colexecbase.Operator, outputIdx int, typ *types.T,
) colexecbase.Operator {
	input = newVectorTypeEnforcer(allocator, input, typ, outputIdx)
	return &constNullOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		outputIdx:    outputIdx,
	}
}

type constNullOp struct {
	OneInputNode
	allocator *colmem.Allocator
	outputIdx int
}

var _ colexecbase.Operator = &constNullOp{}

func (c constNullOp) Init() {
	c.input.Init()
}

func (c constNullOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	col := batch.ColVec(c.outputIdx)
	nulls := col.Nulls()
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel[:n] {
			nulls.SetNull(i)
		}
	} else {
		nulls.SetNulls()
	}
	return batch
}
