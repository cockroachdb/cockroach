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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/pkg/errors"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// NewConstOp creates a new operator that produces a constant value constVal of
// type t at index outputIdx.
func NewConstOp(
	allocator *Allocator, input Operator, t coltypes.T, constVal interface{}, outputIdx int,
) (Operator, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &const_TYPEOp{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			outputIdx:    outputIdx,
			typ:          t,
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

	allocator *Allocator
	typ       coltypes.T
	outputIdx int
	constVal  _GOTYPE
}

func (c const_TYPEOp) Init() {
	c.input.Init()
}

func (c const_TYPEOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if batch.Width() == c.outputIdx {
		c.allocator.AppendColumn(batch, c.typ)
	}
	if n == 0 {
		return batch
	}
	vec := batch.ColVec(c.outputIdx)
	col := vec._TemplateType()
	c.allocator.performOperation(
		[]coldata.Vec{vec},
		func() {
			if sel := batch.Selection(); sel != nil {
				for _, i := range sel[:n] {
					execgen.SET(col, int(i), c.constVal)
				}
			} else {
				col = execgen.SLICE(col, 0, int(n))
				for execgen.RANGE(i, col, 0, int(n)) {
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
func NewConstNullOp(allocator *Allocator, input Operator, outputIdx int, typ coltypes.T) Operator {
	return &constNullOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		outputIdx:    outputIdx,
		typ:          typ,
	}
}

type constNullOp struct {
	OneInputNode
	allocator *Allocator
	outputIdx int
	typ       coltypes.T
}

var _ Operator = &constNullOp{}

func (c constNullOp) Init() {
	c.input.Init()
}

func (c constNullOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()

	if batch.Width() == c.outputIdx {
		c.allocator.AppendColumn(batch, c.typ)
	}

	if n == 0 {
		return batch
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
