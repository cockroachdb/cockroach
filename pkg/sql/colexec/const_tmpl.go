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
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
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

// _GOTYPE is the template variable.
type _GOTYPE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

// NewConstOp creates a new operator that produces a constant value constVal of
// type t at index outputIdx.
func NewConstOp(
	allocator *colmem.Allocator,
	input execinfra.Operator,
	t *types.T,
	constVal interface{},
	outputIdx int,
) (execinfra.Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, t, outputIdx)
	switch typeconv.TypeFamilyToCanonicalTypeFamily[t.Family()] {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &const_TYPEOp{
				OneInputNode: NewOneInputNode(input),
				allocator:    allocator,
				outputIdx:    outputIdx,
				constVal:     constVal.(_GOTYPE),
			}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported const type %s", t.Name())
}

// {{range .}}
// {{range .WidthOverloads}}

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
	col := vec.TemplateType()
	if vec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		vec.Nulls().UnsetNulls()
	}
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
// {{end}}

// NewConstNullOp creates a new operator that produces a constant (untyped) NULL
// value at index outputIdx.
func NewConstNullOp(
	allocator *colmem.Allocator, input execinfra.Operator, outputIdx int, typ *types.T,
) execinfra.Operator {
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

var _ execinfra.Operator = &constNullOp{}

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
	if col.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		nulls.UnsetNulls()
	}
	if sel := batch.Selection(); sel != nil {
		for _, i := range sel[:n] {
			nulls.SetNull(i)
		}
	} else {
		nulls.SetNulls()
	}
	return batch
}
