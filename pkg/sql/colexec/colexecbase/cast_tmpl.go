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
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ duration.Duration
	_ json.JSON
	_ = lex.DecodeRawBytesToByteArrayAuto
	_ = uuid.FromBytes
	_ = oid.T_name
	_ = util.TruncateString
)

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

func isIdentityCast(fromType, toType *types.T) bool {
	if fromType.Identical(toType) {
		return true
	}
	if fromType.Family() == types.FloatFamily && toType.Family() == types.FloatFamily {
		// Casts between floats are identical because all floats are represented
		// by float64 physically.
		return true
	}
	if fromType.Family() == types.UuidFamily && toType.Family() == types.BytesFamily {
		// The cast from UUID to Bytes is an identity because we don't need to
		// perform any conversion since both are represented in the same way.
		return true
	}
	return false
}

func GetCastOperator(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	colIdx int,
	resultIdx int,
	fromType *types.T,
	toType *types.T,
	evalCtx *tree.EvalContext,
) (colexecop.Operator, error) {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, toType, resultIdx)
	base := castOpBase{
		OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
		allocator:                allocator,
		colIdx:                   colIdx,
		outputIdx:                resultIdx,
		evalCtx:                  evalCtx,
	}
	if fromType.Family() == types.UnknownFamily {
		return &castOpNullAny{castOpBase: base}, nil
	}
	if isIdentityCast(fromType, toType) {
		return &castIdentityOp{castOpBase: base}, nil
	}
	isFromDatum := typeconv.TypeFamilyToCanonicalTypeFamily(fromType.Family()) == typeconv.DatumVecCanonicalTypeFamily
	isToDatum := typeconv.TypeFamilyToCanonicalTypeFamily(toType.Family()) == typeconv.DatumVecCanonicalTypeFamily
	if isFromDatum {
		if isToDatum {
			return &castDatumDatumOp{castOpBase: base}, nil
		}
		switch toType.Family() {
		// {{range .FromDatum}}
		// {{$fromInfo := .}}
		case _TYPE_FAMILY:
			switch toType.Width() {
			// {{range .Widths}}
			case _TYPE_WIDTH:
				return &cast_NAMEOp{castOpBase: base}, nil
				// {{end}}
			}
			// {{end}}
		}
	} else {
		if isToDatum {
			return &castNativeToDatumOp{castOpBase: base}, nil
		}
		switch fromType.Family() {
		// {{range .FromNative}}
		case _TYPE_FAMILY:
			switch fromType.Width() {
			// {{range .Widths}}
			case _TYPE_WIDTH:
				switch toType.Family() {
				// {{$fromInfo := .}}
				// {{range .To}}
				case _TYPE_FAMILY:
					switch toType.Width() {
					// {{range .Widths}}
					case _TYPE_WIDTH:
						return &cast_NAMEOp{castOpBase: base}, nil
						// {{end}}
					}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
	}
	return nil, errors.Errorf("unhandled cast %s -> %s", fromType.SQLString(), toType.SQLString())
}

func IsCastSupported(fromType, toType *types.T) bool {
	if fromType.Family() == types.UnknownFamily {
		return true
	}
	if isIdentityCast(fromType, toType) {
		return true
	}
	isFromDatum := typeconv.TypeFamilyToCanonicalTypeFamily(fromType.Family()) == typeconv.DatumVecCanonicalTypeFamily
	isToDatum := typeconv.TypeFamilyToCanonicalTypeFamily(toType.Family()) == typeconv.DatumVecCanonicalTypeFamily
	if isFromDatum {
		if isToDatum {
			return true
		}
		switch toType.Family() {
		// {{range .FromDatum}}
		case _TYPE_FAMILY:
			switch toType.Width() {
			// {{range .Widths}}
			case _TYPE_WIDTH:
				return true
				// {{end}}
			}
			// {{end}}
		}
	} else {
		if isToDatum {
			return true
		}
		switch fromType.Family() {
		// {{range .FromNative}}
		case _TYPE_FAMILY:
			switch fromType.Width() {
			// {{range .Widths}}
			case _TYPE_WIDTH:
				switch toType.Family() {
				// {{range .To}}
				case _TYPE_FAMILY:
					switch toType.Width() {
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
	}
	return false
}

type castOpBase struct {
	colexecop.OneInputInitCloserHelper

	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
	evalCtx   *tree.EvalContext
}

func (c *castOpBase) Reset(ctx context.Context) {
	if r, ok := c.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
}

type castOpNullAny struct {
	castOpBase
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
	castOpBase
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

type castNativeToDatumOp struct {
	castOpBase

	scratch []tree.Datum
	da      rowenc.DatumAlloc
}

var _ colexecop.ClosableOperator = &castNativeToDatumOp{}

func (c *castNativeToDatumOp) Next() coldata.Batch {
	batch := c.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	inputVec := batch.ColVec(c.colIdx)
	outputVec := batch.ColVec(c.outputIdx)
	outputCol := outputVec.Datum()
	outputNulls := outputVec.Nulls()
	toType := outputVec.Type()
	c.allocator.PerformOperation([]coldata.Vec{outputVec}, func() {
		if n > c.da.AllocSize {
			c.da.AllocSize = n
		}
		if cap(c.scratch) < n {
			c.scratch = make([]tree.Datum, n)
		} else {
			c.scratch = c.scratch[:n]
		}
		scratch := c.scratch
		sel := batch.Selection()
		colconv.ColVecToDatumAndDeselect(scratch, inputVec, n, sel, &c.da)
		if sel != nil {
			if inputVec.Nulls().MaybeHasNulls() {
				for scratchIdx, outputIdx := range sel[:n] {
					setNativeToDatumCast(outputCol, outputNulls, scratch, scratchIdx, outputIdx, toType, c.evalCtx, true, false)
				}
			} else {
				for scratchIdx, outputIdx := range sel[:n] {
					setNativeToDatumCast(outputCol, outputNulls, scratch, scratchIdx, outputIdx, toType, c.evalCtx, false, false)
				}
			}
		} else {
			_ = scratch[n-1]
			if inputVec.Nulls().MaybeHasNulls() {
				for idx := 0; idx < n; idx++ {
					setNativeToDatumCast(outputCol, outputNulls, scratch, idx, idx, toType, c.evalCtx, true, true)
				}
			} else {
				for idx := 0; idx < n; idx++ {
					setNativeToDatumCast(outputCol, outputNulls, scratch, idx, idx, toType, c.evalCtx, false, true)
				}
			}
		}
	})
	return batch
}

// setNativeToDatumCast performs the cast of the converted datum in
// scratch[scratchIdx] to toType and sets the result into position outputIdx of
// outputCol (or into the output nulls bitmap).
// execgen:inline
// execgen:template<hasNulls,scratchBCE>
func setNativeToDatumCast(
	outputCol coldata.DatumVec,
	outputNulls *coldata.Nulls,
	scratch []tree.Datum,
	scratchIdx int,
	outputIdx int,
	toType *types.T,
	evalCtx *tree.EvalContext,
	hasNulls bool,
	scratchBCE bool,
) {
	if scratchBCE {
		//gcassert:bce
	}
	converted := scratch[scratchIdx]
	if hasNulls && converted == tree.DNull {
		outputNulls.SetNull(outputIdx)
		continue
	}
	res, err := tree.PerformCast(evalCtx, converted, toType)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	outputCol.Set(outputIdx, res)
}

// {{define "castOp"}}

// {{$fromInfo := .FromInfo}}
// {{$fromFamily := .FromFamily}}
// {{$toFamily := .ToFamily}}
// {{with .Global}}

type cast_NAMEOp struct {
	castOpBase

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
	toType := outputVec.Type()
	// Remove unused warnings.
	_ = toType
	c.allocator.PerformOperation(
		[]coldata.Vec{outputVec}, func() {
			inputCol := inputVec._FROM_TYPE()
			inputNulls := inputVec.Nulls()
			outputCol := outputVec._TO_TYPE()
			outputNulls := outputVec.Nulls()
			// {{if and (eq $fromFamily "DatumVecCanonicalTypeFamily") (not (eq $toFamily "DatumVecCanonicalTypeFamily"))}}
			converter := colconv.GetDatumToPhysicalFn(toType)
			// {{end}}
			if inputVec.MaybeHasNulls() {
				outputNulls.Copy(inputNulls)
				if sel != nil {
					castTuples(inputCol, inputNulls, outputCol, outputNulls, toType, n, sel, c.evalCtx, true, true)
				} else {
					castTuples(inputCol, inputNulls, outputCol, outputNulls, toType, n, sel, c.evalCtx, true, false)
				}
			} else {
				// We need to make sure that there are no left over null values
				// in the output vector.
				outputNulls.UnsetNulls()
				if sel != nil {
					castTuples(inputCol, inputNulls, outputCol, outputNulls, toType, n, sel, c.evalCtx, false, true)
				} else {
					castTuples(inputCol, inputNulls, outputCol, outputNulls, toType, n, sel, c.evalCtx, false, false)
				}
			}
			// {{/*
			// Although we didn't change the length of the batch, it is
			// necessary to set the length anyway (this helps maintaining the
			// invariant of flat bytes).
			// */}}
			batch.SetLength(n)
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

// castTuples casts all non-null tuples from the vector named 'inputCol' to the
// vector named 'outputCol'.
// execgen:inline
// execgen:template<hasNulls,hasSel>
func castTuples(
	inputCol interface{},
	inputNulls *coldata.Nulls,
	outputCol interface{},
	outputNulls *coldata.Nulls,
	toType *types.T,
	n int,
	sel []int,
	evalCtx *tree.EvalContext,
	hasNulls bool,
	hasSel bool,
) {
	// Silence unused warning.
	_ = evalCtx
	if !hasSel {
		// {{if $fromInfo.Sliceable}}
		_ = inputCol.Get(n - 1)
		// {{end}}
		// {{if .Sliceable}}
		_ = outputCol.Get(n - 1)
		// {{end}}
	}
	var tupleIdx int
	for i := 0; i < n; i++ {
		if hasSel {
			tupleIdx = sel[i]
		} else {
			tupleIdx = i
		}
		if hasNulls && inputNulls.NullAt(tupleIdx) {
			continue
		}
		if !hasSel {
			// {{if $fromInfo.Sliceable}}
			//gcassert:bce
			// {{end}}
		}
		v := inputCol.Get(tupleIdx)
		var r _TO_GO_TYPE
		_CAST(r, v, evalCtx, toType)
		if !hasSel {
			// {{if .Sliceable}}
			//gcassert:bce
			// {{end}}
		}
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
}
