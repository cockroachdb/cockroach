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

package colexecbase

import (
	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ apd.Context
	_ duration.Duration
	_ json.JSON
)

// {{/*

// Declarations to make the template compile properly.

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
	input colexecop.Operator,
	t *types.T,
	constVal interface{},
	outputIdx int,
) (colexecop.Operator, error) {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, t, outputIdx)
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &const_TYPEOp{
				OneInputHelper: colexecop.MakeOneInputHelper(input),
				allocator:      allocator,
				outputIdx:      outputIdx,
				constVal:       constVal.(_GOTYPE),
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
	colexecop.OneInputHelper

	allocator *colmem.Allocator
	outputIdx int
	constVal  _GOTYPE
}

func (c const_TYPEOp) Next() coldata.Batch {
	batch := c.Input.Next()
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
			// Shallow copy col to work around Go issue
			// https://github.com/golang/go/issues/39756 which prevents bound check
			// elimination from working in this case.
			col := col
			if sel := batch.Selection(); sel != nil {
				for _, i := range sel[:n] {
					col.Set(i, c.constVal)
				}
			} else {
				_ = col.Get(n - 1)
				for i := 0; i < n; i++ {
					// {{if .Sliceable}}
					//gcassert:bce
					// {{end}}
					col.Set(i, c.constVal)
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
	allocator *colmem.Allocator, input colexecop.Operator, outputIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Unknown, outputIdx)
	return &constNullOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		outputIdx:      outputIdx,
	}
}

type constNullOp struct {
	colexecop.OneInputHelper
	outputIdx int
}

var _ colexecop.Operator = &constNullOp{}

func (c constNullOp) Next() coldata.Batch {
	batch := c.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	batch.ColVec(c.outputIdx).Nulls().SetNulls()
	return batch
}
