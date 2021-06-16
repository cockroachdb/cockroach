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

package colexecbase

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var _ coldataext.Datum

// {{/*

type _R_GO_TYPE interface{}

var _ apd.Decimal
var _ = math.MaxInt8
var _ tree.Datum

// _LEFT_CANONICAL_TYPE_FAMILY is the template variable.
const _LEFT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _LEFT_TYPE_WIDTH is the template variable.
const _LEFT_TYPE_WIDTH = 0

// _RIGHT_CANONICAL_TYPE_FAMILY is the template variable.
const _RIGHT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _RIGHT_TYPE_WIDTH is the template variable.
const _RIGHT_TYPE_WIDTH = 0

func _CAST(to, from, fromCol, toType interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

func GetCastOperator(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	colIdx int,
	resultIdx int,
	fromType *types.T,
	toType *types.T,
) (colexecop.Operator, error) {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, toType, resultIdx)
	if fromType.Family() == types.UnknownFamily {
		return &castOpNullAny{
			OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
			allocator:                allocator,
			colIdx:                   colIdx,
			outputIdx:                resultIdx,
		}, nil
	}
	leftType, rightType := fromType, toType
	switch typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) {
	// {{range .LeftFamilies}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch leftType.Width() {
		// {{range .LeftWidths}}
		case _LEFT_TYPE_WIDTH:
			switch typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) {
			// {{range .RightFamilies}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch rightType.Width() {
				// {{range .RightWidths}}
				case _RIGHT_TYPE_WIDTH:
					return &cast_NAMEOp{
						OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
						allocator:                allocator,
						colIdx:                   colIdx,
						outputIdx:                resultIdx,
						toType:                   toType,
					}, nil
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unhandled cast %s -> %s", fromType, toType)
}

func IsCastSupported(fromType, toType *types.T) bool {
	if fromType.Family() == types.UnknownFamily {
		return true
	}
	leftType, rightType := fromType, toType
	switch typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) {
	// {{range .LeftFamilies}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch leftType.Width() {
		// {{range .LeftWidths}}
		case _LEFT_TYPE_WIDTH:
			switch typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) {
			// {{range .RightFamilies}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch rightType.Width() {
				// {{range .RightWidths}}
				case _RIGHT_TYPE_WIDTH:
					return true
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	return false
}

type castOpNullAny struct {
	colexecop.OneInputInitCloserHelper

	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
}

var _ colexecop.ClosableOperator = &castOpNullAny{}

func (c *castOpNullAny) Next() coldata.Batch {
	batch := c.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(c.colIdx)
	projVec := batch.ColVec(c.outputIdx)
	vecNulls := vec.Nulls()
	projNulls := projVec.Nulls()
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over nulls values in the
		// output vector.
		projNulls.UnsetNulls()
	}
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

// TODO(yuzefovich): refactor castOp so that it is type-specific (meaning not
// canonical type family specific, but actual type specific). This will
// probably require changing the way we handle cast overloads as well.

// {{range .LeftFamilies}}
// {{$leftFamily := .LeftCanonicalFamilyStr}}
// {{range .LeftWidths}}
// {{range .RightFamilies}}
// {{$rightFamily := .RightCanonicalFamilyStr}}
// {{range .RightWidths}}

type cast_NAMEOp struct {
	colexecop.OneInputInitCloserHelper

	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
	toType    *types.T
	// {{if and (eq $leftFamily "types.DecimalFamily") (eq $rightFamily "types.IntFamily")}}
	// {{/*
	// overloadHelper is used only when we perform the cast from decimals to
	// ints. In all other cases we don't want to wastefully allocate the helper.
	// */}}
	overloadHelper execgen.OverloadHelper
	// {{end}}
}

var _ colexecop.ResettableOperator = &cast_NAMEOp{}
var _ colexecop.ClosableOperator = &cast_NAMEOp{}

func (c *cast_NAMEOp) Reset(ctx context.Context) {
	if r, ok := c.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
}

func (c *cast_NAMEOp) Next() coldata.Batch {
	batch := c.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	// {{if and (eq $leftFamily "types.DecimalFamily") (eq $rightFamily "types.IntFamily")}}
	// In order to inline the templated code of overloads, we need to have a
	// "_overloadHelper" local variable of type "execgen.OverloadHelper".
	_overloadHelper := c.overloadHelper
	// {{end}}
	sel := batch.Selection()
	inputVec := batch.ColVec(c.colIdx)
	outputVec := batch.ColVec(c.outputIdx)
	c.allocator.PerformOperation(
		[]coldata.Vec{outputVec}, func() {
			inputCol := inputVec._L_TYP()
			outputCol := outputVec._R_TYP()
			outputNulls := outputVec.Nulls()
			if inputVec.MaybeHasNulls() {
				inputNulls := inputVec.Nulls()
				outputNulls.Copy(inputNulls)
				if sel != nil {
					_CAST_TUPLES(true, true)
				} else {
					_CAST_TUPLES(true, false)
				}
			} else {
				// We need to make sure that there are no left over null values
				// in the output vector.
				outputNulls.UnsetNulls()
				if sel != nil {
					_CAST_TUPLES(false, true)
				} else {
					_CAST_TUPLES(false, false)
				}
			}
		},
	)
	return batch
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}

// {{/*
// This code snippet casts all non-null tuples from the vector named 'inputCol'
// to the vector named 'outputCol'.
func _CAST_TUPLES(_HAS_NULLS, _HAS_SEL bool) { // */}}
	// {{define "castTuples" -}}
	// {{$hasNulls := .HasNulls}}
	// {{$hasSel := .HasSel}}
	// {{with .Global}}
	// {{if $hasSel}}
	sel = sel[:n]
	// {{else}}
	// Remove bounds checks for inputCol[i] and outputCol[i].
	_ = inputCol.Get(n - 1)
	_ = outputCol.Get(n - 1)
	// {{end}}
	var tupleIdx int
	for i := 0; i < n; i++ {
		// {{if $hasSel}}
		tupleIdx = sel[i]
		// {{else}}
		tupleIdx = i
		// {{end}}
		// {{if $hasNulls}}
		if inputNulls.NullAt(tupleIdx) {
			continue
		}
		// {{end}}
		// {{if not $hasSel}}
		//gcassert:bce
		// {{end}}
		v := inputCol.Get(tupleIdx)
		var r _R_GO_TYPE
		_CAST(r, v, inputCol, c.toType)
		// {{if and (.Right.Sliceable) (not $hasSel)}}
		//gcassert:bce
		// {{end}}
		outputCol.Set(tupleIdx, r)
		// {{if eq .Right.VecMethod "Datum"}}
		// Casting to datum-backed vector might produce a null value on
		// non-null tuple, so we need to check that case after the cast was
		// performed.
		if r == tree.DNull {
			outputNulls.SetNull(tupleIdx)
		}
		// {{end}}
	}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}
