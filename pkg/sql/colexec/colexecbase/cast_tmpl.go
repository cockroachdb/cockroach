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

type _TO_GO_TYPE interface{}

var _ apd.Decimal
var _ = math.MaxInt8
var _ tree.Datum

// _TYPE_FAMILY is the template variable.
const _TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _GENERATE_CAST_OP is a "fake" value that will be replaced by executing
// "castOp" template in the scope of this value's "callsite".
const _GENERATE_CAST_OP = 0

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
	if toType.Identical(fromType) {
		// We have an identity cast, so we use a custom identity cast operator.
		return &castIdentityOp{
			OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
			allocator:                allocator,
			colIdx:                   colIdx,
			outputIdx:                resultIdx,
		}, nil
	}
	// TODO(yuzefovich): remove this rebinding in the follow-up commit.
	leftType, rightType := fromType, toType
	switch leftType.Family() {
	// {{range .FromNative}}
	case _TYPE_FAMILY:
		switch leftType.Width() {
		// {{range .Widths}}
		case _TYPE_WIDTH:
			switch rightType.Family() {
			// {{$fromInfo := .}}
			// {{range .To}}
			case _TYPE_FAMILY:
				switch rightType.Width() {
				// {{range .Widths}}
				case _TYPE_WIDTH:
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
	if typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) == typeconv.DatumVecCanonicalTypeFamily {
		switch rightType.Family() {
		// {{range .FromDatum}}
		// {{$fromInfo := .}}
		case _TYPE_FAMILY:
			switch rightType.Width() {
			// {{range .Widths}}
			case _TYPE_WIDTH:
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

		if typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) == typeconv.DatumVecCanonicalTypeFamily {
			return &castDatumDatumOp{
				OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
				allocator:                allocator,
				colIdx:                   colIdx,
				outputIdx:                resultIdx,
				toType:                   toType,
			}, nil
		}
	}
	return nil, errors.Errorf("unhandled cast %s -> %s", fromType, toType)
}

func IsCastSupported(fromType, toType *types.T) bool {
	if fromType.Family() == types.UnknownFamily {
		return true
	}
	if toType.Identical(fromType) {
		return true
	}
	// TODO(yuzefovich): remove this rebinding in the follow-up commit.
	leftType, rightType := fromType, toType
	switch leftType.Family() {
	// {{range .FromNative}}
	case _TYPE_FAMILY:
		switch leftType.Width() {
		// {{range .Widths}}
		case _TYPE_WIDTH:
			switch rightType.Family() {
			// {{range .To}}
			case _TYPE_FAMILY:
				switch rightType.Width() {
				// {{range .Widths}}
				case _TYPE_WIDTH:
					return true
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	if typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) == typeconv.DatumVecCanonicalTypeFamily {
		switch rightType.Family() {
		// {{range .FromDatum}}
		case _TYPE_FAMILY:
			switch rightType.Width() {
			// {{range .Widths}}
			case _TYPE_WIDTH:
				return true
				// {{end}}
			}
			// {{end}}
		}

		if typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) == typeconv.DatumVecCanonicalTypeFamily {
			return true
		}
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

// castIdentityOp is a special cast operator for the case when "from" and "to"
// types are identical. The job of this operator is to simply copy the input
// column into the output column, without performing the deselection step. Not
// performing the deselection is justified by the following:
// 1. to be in line with other cast operators
// 2. AND/OR projection operators cannot handle when a different batch is
//    returned than the one they fed into the projection chain (which might
//    contain casts)
// 3. performing the deselection would require copying over all vectors, not
//    just the output one.
// This operator should be planned rarely enough (if ever) to not be very
// important.
type castIdentityOp struct {
	colexecop.OneInputInitCloserHelper

	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
}

var _ colexecop.ClosableOperator = &castIdentityOp{}

func (c *castIdentityOp) Next() coldata.Batch {
	batch := c.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	projVec := batch.ColVec(c.outputIdx)
	c.allocator.PerformOperation([]coldata.Vec{projVec}, func() {
		maxIdx := n
		if sel := batch.Selection(); sel != nil {
			// We don't want to perform the deselection during copying, so we
			// will copy everything up to (and including) the last selected
			// element, without the selection vector.
			maxIdx = sel[n-1] + 1
		}
		projVec.Copy(coldata.SliceArgs{
			Src:       batch.ColVec(c.colIdx),
			SrcEndIdx: maxIdx,
		})
	})
	return batch
}

// {{define "castOp"}}

// {{$fromInfo := .FromInfo}}
// {{$fromFamily := .FromFamily}}
// {{$toFamily := .ToFamily}}
// {{with .Global}}

type cast_NAMEOp struct {
	colexecop.OneInputInitCloserHelper

	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
	toType    *types.T
	// {{if and (eq $fromFamily "types.DecimalFamily") (eq $toFamily "types.IntFamily")}}
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
	// {{if and (eq $fromFamily "types.DecimalFamily") (eq $toFamily "types.IntFamily")}}
	// In order to inline the templated code of overloads, we need to have a
	// "_overloadHelper" local variable of type "execgen.OverloadHelper".
	_overloadHelper := c.overloadHelper
	// {{end}}
	sel := batch.Selection()
	inputVec := batch.ColVec(c.colIdx)
	outputVec := batch.ColVec(c.outputIdx)
	c.allocator.PerformOperation(
		[]coldata.Vec{outputVec}, func() {
			inputCol := inputVec._FROM_TYPE()
			outputCol := outputVec._TO_TYPE()
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

// {{range .FromNative}}
// {{$fromFamily := .TypeFamily}}
// {{range .Widths}}
// {{$fromInfo := .}}
// {{range .To}}
// {{$toFamily := .TypeFamily}}
// {{range .Widths}}

// _GENERATE_CAST_OP

// {{end}}
// {{end}}
// {{end}}
// {{end}}

// {{range .FromDatum}}
// {{$fromInfo := .}}
// {{$fromFamily := "DatumVecCanonicalTypeFamily"}}
// {{$toFamily := .TypeFamily}}
// {{range .Widths}}

// _GENERATE_CAST_OP

// {{end}}
// {{end}}

// {{$fromInfo := .BetweenDatums}}
// {{$fromFamily := "DatumVecCanonicalTypeFamily"}}
// {{$toFamily := "DatumVecCanonicalTypeFamily"}}

// {{with .BetweenDatums}}

// _GENERATE_CAST_OP

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
		var r _TO_GO_TYPE
		_CAST(r, v, inputCol, c.toType)
		// {{if and (.Sliceable) (not $hasSel)}}
		//gcassert:bce
		// {{end}}
		outputCol.Set(tupleIdx, r)
		// {{if eq .VecMethod "Datum"}}
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
