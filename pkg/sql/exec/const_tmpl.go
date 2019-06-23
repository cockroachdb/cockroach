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
// This file is the execgen template for distinct.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/pkg/errors"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// */}}

// NewConstOp creates a new operator that produces a constant value constVal of
// type t at index outputIdx.
func NewConstOp(input Operator, t types.T, constVal interface{}, outputIdx int) (Operator, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &const_TYPEOp{
			input:     input,
			outputIdx: outputIdx,
			typ:       t,
			constVal:  constVal.(_GOTYPE),
		}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported const type %s", t)
	}
}

// {{range .}}

type const_TYPEOp struct {
	input Operator

	typ       types.T
	outputIdx int
	constVal  _GOTYPE
}

func (c const_TYPEOp) Init() {
	c.input.Init()
}

func (c const_TYPEOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	if batch.Length() == 0 {
		return batch
	}

	if batch.Width() == c.outputIdx {
		batch.AppendCol(c.typ)
		col := batch.ColVec(c.outputIdx)._TemplateType()
		for i := range col {
			col[i] = c.constVal
		}
	}
	return batch
}

// {{end}}

// NewConstNullOp creates a new operator that produces a constant (untyped) NULL
// value at index outputIdx.
func NewConstNullOp(input Operator, outputIdx int) Operator {
	return &constNullOp{
		input:     input,
		outputIdx: outputIdx,
	}
}

type constNullOp struct {
	input     Operator
	outputIdx int
}

func (c constNullOp) Init() {
	c.input.Init()
}

func (c constNullOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	if batch.Length() == 0 {
		return batch
	}

	if batch.Width() == c.outputIdx {
		batch.AppendCol(types.Int8)
		col := batch.ColVec(c.outputIdx)
		col.Nulls().SetNulls()
	}
	return batch
}
