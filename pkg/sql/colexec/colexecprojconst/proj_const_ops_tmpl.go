// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for proj_const_{left,right}_ops.eg.go.
// It's formatted in a special way, so it's both valid Go and a valid
// text/template input. This permits editing this file with editor support.
//
// */}}

package colexecprojconst

import (
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexeccmp"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ apd.Context
	_ duration.Duration
	_ sqltelemetry.EnumTelemetryType
	_ telemetry.Counter
	_ json.JSON
	_ = coldataext.CompareDatum
	_ = encoding.UnsafeConvertStringToBytes
)

// {{/*
// Declarations to make the template compile properly.

// _LEFT_CANONICAL_TYPE_FAMILY is the template variable.
const _LEFT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _LEFT_TYPE_WIDTH is the template variable.
const _LEFT_TYPE_WIDTH = 0

// _RIGHT_CANONICAL_TYPE_FAMILY is the template variable.
const _RIGHT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _RIGHT_TYPE_WIDTH is the template variable.
const _RIGHT_TYPE_WIDTH = 0

// _NON_CONST_GOTYPESLICE is a template Go type slice variable.
type _NON_CONST_GOTYPESLICE interface{}

// _ASSIGN is the template function for assigning the first input to the result
// of computation an operation on the second and the third inputs.
func _ASSIGN(_, _, _, _, _, _ interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// {{define "projConstOp"}}

type _OP_CONST_NAME struct {
	// {{if .NeedsBinaryOverloadHelper}}
	colexecutils.BinaryOverloadHelper
	// {{end}}
	projConstOpBase
	// {{if _IS_CONST_LEFT}}
	constArg _L_GO_TYPE
	// {{else}}
	constArg _R_GO_TYPE
	// {{end}}
	// {{if .Negatable}}
	negate bool
	// {{end}}
	// {{if .CaseInsensitive}}
	caseInsensitive bool
	// {{end}}
}

func (p _OP_CONST_NAME) Next() coldata.Batch {
	// {{if .NeedsBinaryOverloadHelper}}
	// {{/*
	//     In order to inline the templated code of the binary overloads
	//     operating on datums, we need to have a `_overloadHelper` local
	//     variable of type `colexecutils.BinaryOverloadHelper`.
	// */}}
	_overloadHelper := p.BinaryOverloadHelper
	_ctx := p.Ctx
	// {{end}}
	// {{if .Negatable}}
	// {{/*
	//     In order to inline the templated code of the LIKE overloads, we need
	//     to have a `_negate` local variable indicating whether the assignment
	//     should be negated.
	// */}}
	_negate := p.negate
	// {{ end }}
	// {{if .CaseInsensitive}}
	// {{/*
	//     In order to inline the templated code of the LIKE overloads, we need
	//     to have a `_caseInsensitive` local variable indicating whether the
	//     operator is case insensitive.
	// */}}
	_caseInsensitive := p.caseInsensitive
	// {{end}}
	batch := p.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(p.colIdx)
	var col _NON_CONST_GOTYPESLICE
	// {{if _IS_CONST_LEFT}}
	col = vec._R_TYP()
	// {{else}}
	col = vec._L_TYP()
	// {{end}}
	projVec := batch.ColVec(p.outputIdx)
	p.allocator.PerformOperation([]*coldata.Vec{projVec}, func() {
		// Capture col to force bounds check to work. See
		// https://github.com/golang/go/issues/39756
		col := col
		projCol := projVec._RET_TYP()
		// {{/*
		// Some operators can result in NULL with non-NULL inputs, like the JSON
		// fetch value operator, ->. Therefore, _outNulls is defined to allow
		// updating the output Nulls from within _ASSIGN functions when the result
		// of a projection is Null.
		// */}}
		_outNulls := projVec.Nulls()
		if vec.Nulls().MaybeHasNulls() {
			_SET_PROJECTION(true)
		} else {
			_SET_PROJECTION(false)
		}
	})
	return batch
}

// {{end}}

// {{/*
func _SET_PROJECTION(_HAS_NULLS bool) {
	// */}}
	// {{define "setProjection" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	// {{$isDatum := (and (eq .Left.VecMethod "Datum") (eq .Right.VecMethod "Datum"))}}
	// {{if _HAS_NULLS}}
	colNulls := vec.Nulls()
	// {{end}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS, true)
		}
	} else {
		_ = projCol.Get(n - 1)
		_ = col.Get(n - 1)
		for i := 0; i < n; i++ {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS, false)
		}
	}
	// {{/*
	// _outNulls has been updated from within the _ASSIGN function to include
	// any NULLs that resulted from the projection.
	// If _HAS_NULLS is true, union _outNulls with the set of input Nulls.
	// If _HAS_NULLS is false, then there are no input Nulls. _outNulls is
	// projVec.Nulls() so there is no need to call projVec.SetNulls().
	// */}}
	// {{if _HAS_NULLS}}
	// {{if $isDatum}}
	if !p.calledOnNullInput {
		// {{end}}
		projVec.SetNulls(_outNulls.Or(*colNulls))
		// {{if $isDatum}}
	}
	// {{end}}
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
func _SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS bool, _HAS_SEL bool) { // */}}
	// {{define "setSingleTupleProjection" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{$hasSel := $.HasSel}}
	// {{with $.Overload}}
	// {{$isDatum := (and (eq .Left.VecMethod "Datum") (eq .Right.VecMethod "Datum"))}}
	// {{if _HAS_NULLS}}
	if p.calledOnNullInput || !colNulls.NullAt(i) {
		// We only want to perform the projection operation if the value is not null.
		// {{end}}
		// {{if _IS_CONST_LEFT}}
		// {{if and (.Left.Sliceable) (not _HAS_SEL)}}
		//gcassert:bce
		// {{end}}
		// {{else}}
		// {{if and (.Right.Sliceable) (not _HAS_SEL)}}
		//gcassert:bce
		// {{end}}
		// {{end}}
		arg := col.Get(i)
		// {{if (and _HAS_NULLS $isDatum)}}
		if colNulls.NullAt(i) {
			// {{/*
			// If we entered this branch for a null value, calledOnNullInput must be
			// true. This means the projection should be calculated on null arguments.
			// When a value is null, the underlying data in the slice is invalid and
			// can be anything, so we need to overwrite it here. calledOnNullInput is
			// currently only true for ConcatDatumDatum, so only the datum case needs
			// to be handled.
			// */}}
			arg = tree.DNull
		}
		// {{end}}
		// {{if _IS_CONST_LEFT}}
		_ASSIGN(projCol[i], p.constArg, arg, projCol, _, col)
		// {{else}}
		_ASSIGN(projCol[i], arg, p.constArg, projCol, col, _)
		// {{end}}
		// {{if _HAS_NULLS}}
	}
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{range .BinOps}}
// {{range .LeftFamilies}}
// {{range .LeftWidths}}
// {{range .RightFamilies}}
// {{range .RightWidths}}

// {{template "projConstOp" .}}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}

// {{range .CmpOps}}
// {{range .LeftFamilies}}
// {{range .LeftWidths}}
// {{range .RightFamilies}}
// {{range .RightWidths}}

// {{if not _IS_CONST_LEFT}}
// {{/*
//     Comparison operators are always normalized so that the constant is on
//     the right side, so we skip generating the code when the constant is on
//     the left.
// */}}

// {{template "projConstOp" .}}

// {{end}}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
// {{end}}

// GetProjection_CONST_SIDEConstOperator returns the appropriate constant
// projection operator for the given left and right column types and operation.
func GetProjection_CONST_SIDEConstOperator(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	constType *types.T,
	outputType *types.T,
	op tree.Operator,
	input colexecop.Operator,
	colIdx int,
	constArg tree.Datum,
	outputIdx int,
	evalCtx *eval.Context,
	binOp tree.BinaryEvalOp,
	cmpExpr *tree.ComparisonExpr,
	calledOnNullInput bool,
) (colexecop.Operator, error) {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, outputType, outputIdx)
	projConstOpBase := projConstOpBase{
		OneInputHelper:    colexecop.MakeOneInputHelper(input),
		allocator:         allocator,
		colIdx:            colIdx,
		outputIdx:         outputIdx,
		calledOnNullInput: calledOnNullInput,
	}
	c := colconv.GetDatumToPhysicalFn(constType)(constArg)
	// {{if _IS_CONST_LEFT}}
	leftType, rightType := constType, inputTypes[colIdx]
	// {{else}}
	leftType, rightType := inputTypes[colIdx], constType
	// {{end}}
	switch op := op.(type) {
	case treebin.BinaryOperator:
		switch op.Symbol {
		// {{range .BinOps}}
		case treebin._NAME:
			switch typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) {
			// {{range .LeftFamilies}}
			// {{$leftFamilyStr := .LeftCanonicalFamilyStr}}
			case _LEFT_CANONICAL_TYPE_FAMILY:
				switch leftType.Width() {
				// {{range .LeftWidths}}
				case _LEFT_TYPE_WIDTH:
					switch typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) {
					// {{range .RightFamilies}}
					// {{$rightFamilyStr := .RightCanonicalFamilyStr}}
					case _RIGHT_CANONICAL_TYPE_FAMILY:
						switch rightType.Width() {
						// {{range .RightWidths}}
						case _RIGHT_TYPE_WIDTH:
							op := &_OP_CONST_NAME{
								projConstOpBase: projConstOpBase,
								// {{if _IS_CONST_LEFT}}
								// {{if eq $leftFamilyStr "typeconv.DatumVecCanonicalTypeFamily"}}
								constArg: constArg,
								// {{else}}
								constArg: c.(_L_GO_TYPE),
								// {{end}}
								// {{else}}
								// {{if eq $rightFamilyStr "typeconv.DatumVecCanonicalTypeFamily"}}
								constArg: constArg,
								// {{else}}
								constArg: c.(_R_GO_TYPE),
								// {{end}}
								// {{end}}
							}
							// {{if .NeedsBinaryOverloadHelper}}
							op.BinaryOverloadHelper = colexecutils.BinaryOverloadHelper{BinOp: binOp, EvalCtx: evalCtx}
							// {{end}}
							return op, nil
							// {{end}}
						}
						// {{end}}
					}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
	// {{if not _IS_CONST_LEFT}}
	// {{/*
	//     Comparison operators are always normalized so that the constant is on
	//     the right side, so we skip generating the code when the constant is on
	//     the left.
	// */}}
	case treecmp.ComparisonOperator:
		if leftType.Family() != types.TupleFamily && rightType.Family() != types.TupleFamily {
			// Tuple comparison has special null-handling semantics, so we will
			// fallback to the default comparison operator if either of the
			// input vectors is of a tuple type.
			switch op.Symbol {
			// {{range .CmpOps}}
			case treecmp._NAME:
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
								return &_OP_CONST_NAME{
									projConstOpBase: projConstOpBase,
									constArg:        c.(_R_GO_TYPE),
								}, nil
								// {{end}}
							}
							// {{end}}
						}
						// {{end}}
					}
					// {{end}}
				}
				// {{end}}
			}
		}
		return &defaultCmp_CONST_SIDEConstProjOp{
			projConstOpBase:     projConstOpBase,
			adapter:             colexeccmp.NewComparisonExprAdapter(cmpExpr, evalCtx),
			constArg:            constArg,
			toDatumConverter:    colconv.NewVecToDatumConverter(len(inputTypes), []int{colIdx}, true /* willRelease */),
			datumToVecConverter: colconv.GetDatumToPhysicalFn(outputType),
		}, nil
		// {{end}}
	}
	return nil, errors.Errorf("couldn't find overload for %s %s %s", leftType.Name(), op, rightType.Name())
}
