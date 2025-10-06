// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// This file is used as input to the evalgen command to generate the methods
// and interface for eval_op_generated.go. Note that operations which define
// their own Eval method will not have one generated for them and will not be
// a part of the relevant Evaluator interface.

// CompareScalarOp is a BinaryEvalOp.
type CompareScalarOp struct {
	treecmp.ComparisonOperator
}

// CompareBox2DOp is a BinaryEvalOp.
type CompareBox2DOp struct {
	Op func(left, right Datum) bool
}

// InTupleOp is a BinaryEvalOp.
type InTupleOp struct{}

// CompareTupleOp is a BinaryEvalOp.
type CompareTupleOp struct {
	treecmp.ComparisonOperator
}

// CompareAnyTupleOp is a BinaryEvalOp.
type CompareAnyTupleOp CompareTupleOp

// Eval suppresses an auto-generated binding and membership in the
// OpEvaluator interface.
func (t *CompareAnyTupleOp) Eval(ctx context.Context, e OpEvaluator, a, b Datum) (Datum, error) {
	if a == DNull || b == DNull {
		return MakeDBool(a == DNull && b == DNull), nil
	}
	return e.EvalCompareTupleOp(ctx, (*CompareTupleOp)(t), a, b)
}

// MatchLikeOp is a BinaryEvalOp.
type MatchLikeOp struct {
	CaseInsensitive bool
}

// SimilarToOp is a BinaryEvalOp.
type SimilarToOp struct {
	Escape          rune
	CaseInsensitive bool
}

// MatchRegexpOp is a BinaryEvalOp.
type MatchRegexpOp struct {
	Escape          rune
	CaseInsensitive bool
}

// OverlapsArrayOp is a BinaryEvalOp.
type OverlapsArrayOp struct{}

// OverlapsINetOp is a BinaryEvalOp.
type OverlapsINetOp struct{}

// TSMatchesVectorQueryOp is a BinaryEvalOp.
type TSMatchesVectorQueryOp struct{}

// TSMatchesQueryVectorOp is a BinaryEvalOp.
type TSMatchesQueryVectorOp struct{}

type (
	// DistanceVectorOp is a BinaryEvalOp.
	DistanceVectorOp struct{}
	// CosDistanceVectorOp is a BinaryEvalOp.
	CosDistanceVectorOp struct{}
	// NegInnerProductVectorOp is a BinaryEvalOp.
	NegInnerProductVectorOp struct{}
)

// AppendToMaybeNullArrayOp is a BinaryEvalOp.
type AppendToMaybeNullArrayOp struct {
	Typ *types.T
}

// PrependToMaybeNullArrayOp is a BinaryEvalOp.
type PrependToMaybeNullArrayOp struct {
	Typ *types.T
}

// ConcatArraysOp is a BinaryEvalOp.
type ConcatArraysOp struct {
	Typ *types.T
}

// ConcatOp is a BinaryEvalOp.
type ConcatOp struct {
	Left  *types.T
	Right *types.T
}

type (
	// BitAndIntOp is a BinaryEvalOp.
	BitAndIntOp struct{}
	// BitAndVarBitOp is a BinaryEvalOp.
	BitAndVarBitOp struct{}
	// BitAndINetOp is a BinaryEvalOp.
	BitAndINetOp struct{}
)

type (
	// BitOrIntOp is a BinaryEvalOp.
	BitOrIntOp struct{}
	// BitOrVarBitOp is a BinaryEvalOp.
	BitOrVarBitOp struct{}
	// BitOrINetOp is a BinaryEvalOp.
	BitOrINetOp struct{}
)

type (
	// BitXorIntOp is a BinaryEvalOp.
	BitXorIntOp struct{}
	// BitXorVarBitOp is a BinaryEvalOp.
	BitXorVarBitOp struct{}
)

type (
	// PlusIntOp is a BinaryEvalOp.
	PlusIntOp struct{}
	// PlusFloatOp is a BinaryEvalOp.
	PlusFloatOp struct{}
	// PlusDecimalOp is a BinaryEvalOp.
	PlusDecimalOp struct{}
	// PlusDecimalIntOp is a BinaryEvalOp.
	PlusDecimalIntOp struct{}
	// PlusIntDecimalOp is a BinaryEvalOp.
	PlusIntDecimalOp struct{}
	// PlusDateIntOp is a BinaryEvalOp.
	PlusDateIntOp struct{}
	// PlusIntDateOp is a BinaryEvalOp.
	PlusIntDateOp struct{}
	// PlusDateTimeOp is a BinaryEvalOp.
	PlusDateTimeOp struct{}
	// PlusTimeDateOp is a BinaryEvalOp.
	PlusTimeDateOp struct{}
	// PlusDateTimeTZOp is a BinaryEvalOp.
	PlusDateTimeTZOp struct{}
	// PlusTimeTZDateOp is a BinaryEvalOp.
	PlusTimeTZDateOp struct{}
	// PlusTimeIntervalOp is a BinaryEvalOp.
	PlusTimeIntervalOp struct{}
	// PlusIntervalTimeOp is a BinaryEvalOp.
	PlusIntervalTimeOp struct{}
	// PlusTimeTZIntervalOp is a BinaryEvalOp.
	PlusTimeTZIntervalOp struct{}
	// PlusIntervalTimeTZOp is a BinaryEvalOp.
	PlusIntervalTimeTZOp struct{}
	// PlusTimestampIntervalOp is a BinaryEvalOp.
	PlusTimestampIntervalOp struct{}
	// PlusIntervalTimestampOp is a BinaryEvalOp.
	PlusIntervalTimestampOp struct{}
	// PlusTimestampTZIntervalOp is a BinaryEvalOp.
	PlusTimestampTZIntervalOp struct{}
	// PlusIntervalTimestampTZOp is a BinaryEvalOp.
	PlusIntervalTimestampTZOp struct{}
	// PlusIntervalOp is a BinaryEvalOp.
	PlusIntervalOp struct{}
	// PlusDateIntervalOp is a BinaryEvalOp.
	PlusDateIntervalOp struct{}
	// PlusIntervalDateOp is a BinaryEvalOp.
	PlusIntervalDateOp struct{}
	// PlusINetIntOp is a BinaryEvalOp.
	PlusINetIntOp struct{}
	// PlusIntINetOp is a BinaryEvalOp.
	PlusIntINetOp struct{}
	// PlusDecimalPGLSNOp is a BinaryEvalOp.
	PlusDecimalPGLSNOp struct{}
	// PlusPGLSNDecimalOp is a BinaryEvalOp.
	PlusPGLSNDecimalOp struct{}
	// PlusPGVectorOp is a BinaryEvalOp.
	PlusPGVectorOp struct{}
)

type (
	// MinusIntOp is a BinaryEvalOp.
	MinusIntOp struct{}
	// MinusFloatOp is a BinaryEvalOp.
	MinusFloatOp struct{}
	// MinusDecimalOp is a BinaryEvalOp.
	MinusDecimalOp struct{}
	// MinusDecimalIntOp is a BinaryEvalOp.
	MinusDecimalIntOp struct{}
	// MinusIntDecimalOp is a BinaryEvalOp.
	MinusIntDecimalOp struct{}
	// MinusDateIntOp is a BinaryEvalOp.
	MinusDateIntOp struct{}
	// MinusDateOp is a BinaryEvalOp.
	MinusDateOp struct{}
	// MinusDateTimeOp is a BinaryEvalOp.
	MinusDateTimeOp struct{}
	// MinusTimeOp is a BinaryEvalOp.
	MinusTimeOp struct{}
	// MinusTimestampOp is a BinaryEvalOp.
	MinusTimestampOp struct{}
	// MinusTimestampTZOp is a BinaryEvalOp.
	MinusTimestampTZOp struct{}
	// MinusTimestampTimestampTZOp is a BinaryEvalOp.
	MinusTimestampTimestampTZOp struct{}
	// MinusTimestampTZTimestampOp is a BinaryEvalOp.
	MinusTimestampTZTimestampOp struct{}
	// MinusTimeIntervalOp is a BinaryEvalOp.
	MinusTimeIntervalOp struct{}
	// MinusTimeTZIntervalOp is a BinaryEvalOp.
	MinusTimeTZIntervalOp struct{}
	// MinusTimestampIntervalOp is a BinaryEvalOp.
	MinusTimestampIntervalOp struct{}
	// MinusTimestampTZIntervalOp is a BinaryEvalOp.
	MinusTimestampTZIntervalOp struct{}
	// MinusDateIntervalOp is a BinaryEvalOp.
	MinusDateIntervalOp struct{}
	// MinusIntervalOp is a BinaryEvalOp.
	MinusIntervalOp struct{}
	// MinusJsonbStringOp is a BinaryEvalOp.
	MinusJsonbStringOp struct{}
	// MinusJsonbIntOp is a BinaryEvalOp.
	MinusJsonbIntOp struct{}
	// MinusJsonbStringArrayOp is a BinaryEvalOp.
	MinusJsonbStringArrayOp struct{}
	// MinusINetOp is a BinaryEvalOp.
	MinusINetOp struct{}
	// MinusINetIntOp is a BinaryEvalOp.
	MinusINetIntOp struct{}
	// MinusPGLSNDecimalOp is a BinaryEvalOp.
	MinusPGLSNDecimalOp struct{}
	// MinusPGLSNOp is a BinaryEvalOp.
	MinusPGLSNOp struct{}
	// MinusPGVectorOp is a BinaryEvalOp.
	MinusPGVectorOp struct{}
)
type (
	// MultDecimalIntOp is a BinaryEvalOp.
	MultDecimalIntOp struct{}
	// MultDecimalIntervalOp is a BinaryEvalOp.
	MultDecimalIntervalOp struct{}
	// MultDecimalOp is a BinaryEvalOp.
	MultDecimalOp struct{}
	// MultFloatIntervalOp is a BinaryEvalOp.
	MultFloatIntervalOp struct{}
	// MultFloatOp is a BinaryEvalOp.
	MultFloatOp struct{}
	// MultIntDecimalOp is a BinaryEvalOp.
	MultIntDecimalOp struct{}
	// MultIntIntervalOp is a BinaryEvalOp.
	MultIntIntervalOp struct{}
	// MultIntOp is a BinaryEvalOp.
	MultIntOp struct{}
	// MultIntervalDecimalOp is a BinaryEvalOp.
	MultIntervalDecimalOp struct{}
	// MultIntervalFloatOp is a BinaryEvalOp.
	MultIntervalFloatOp struct{}
	// MultIntervalIntOp is a BinaryEvalOp.
	MultIntervalIntOp struct{}
	// MultPGVectorOp is a BinaryEvalOp.
	MultPGVectorOp struct{}
)

type (
	// DivDecimalIntOp is a BinaryEvalOp.
	DivDecimalIntOp struct{}
	// DivDecimalOp is a BinaryEvalOp.
	DivDecimalOp struct{}
	// DivFloatOp is a BinaryEvalOp.
	DivFloatOp struct{}
	// DivIntDecimalOp is a BinaryEvalOp.
	DivIntDecimalOp struct{}
	// DivIntOp is a BinaryEvalOp.
	DivIntOp struct{}
	// DivIntervalFloatOp is a BinaryEvalOp.
	DivIntervalFloatOp struct{}
	// DivIntervalIntOp is a BinaryEvalOp.
	DivIntervalIntOp struct{}
)

type (
	// FloorDivDecimalIntOp is a BinaryEvalOp.
	FloorDivDecimalIntOp struct{}
	// FloorDivDecimalOp is a BinaryEvalOp.
	FloorDivDecimalOp struct{}
	// FloorDivFloatOp is a BinaryEvalOp.
	FloorDivFloatOp struct{}
	// FloorDivIntDecimalOp is a BinaryEvalOp.
	FloorDivIntDecimalOp struct{}
	// FloorDivIntOp is a BinaryEvalOp.
	FloorDivIntOp struct{}
)

type (
	// ModDecimalIntOp is a BinaryEvalOp.
	ModDecimalIntOp struct{}
	// ModDecimalOp is a BinaryEvalOp.
	ModDecimalOp struct{}
	// ModFloatOp is a BinaryEvalOp.
	ModFloatOp struct{}
	// ModIntDecimalOp is a BinaryEvalOp.
	ModIntDecimalOp struct{}
	// ModIntOp is a BinaryEvalOp.
	ModIntOp struct{}
	// ModStringOp is a BinaryEvalOp.
	ModStringOp struct{}
)

type (
	// ConcatBytesOp is a BinaryEvalOp.
	ConcatBytesOp struct{}
	// ConcatJsonbOp is a BinaryEvalOp.
	ConcatJsonbOp struct{}
	// ConcatStringOp is a BinaryEvalOp.
	ConcatStringOp struct{}
	// ConcatVarBitOp is a BinaryEvalOp.
	ConcatVarBitOp struct{}
)

type (
	// LShiftINetOp is a BinaryEvalOp.
	LShiftINetOp struct{}
	// LShiftIntOp is a BinaryEvalOp.
	LShiftIntOp struct{}
	// LShiftVarBitIntOp is a BinaryEvalOp.
	LShiftVarBitIntOp struct{}
)

type (
	// RShiftINetOp is a BinaryEvalOp.
	RShiftINetOp struct{}
	// RShiftIntOp is a BinaryEvalOp.
	RShiftIntOp struct{}
	// RShiftVarBitIntOp is a BinaryEvalOp.
	RShiftVarBitIntOp struct{}
)

type (
	// PowDecimalIntOp is a BinaryEvalOp.
	PowDecimalIntOp struct{}
	// PowDecimalOp is a BinaryEvalOp.
	PowDecimalOp struct{}
	// PowFloatOp is a BinaryEvalOp.
	PowFloatOp struct{}
	// PowIntDecimalOp is a BinaryEvalOp.
	PowIntDecimalOp struct{}
	// PowIntOp is a BinaryEvalOp.
	PowIntOp struct{}
)

type (
	// JSONFetchValIntOp is a BinaryEvalOp.
	JSONFetchValIntOp struct{}
	// JSONFetchValStringOp is a BinaryEvalOp.
	JSONFetchValStringOp struct{}
)
type (
	// JSONFetchTextIntOp is a BinaryEvalOp.
	JSONFetchTextIntOp struct{}
	// JSONFetchTextStringOp is a BinaryEvalOp.
	JSONFetchTextStringOp struct{}
)

// JSONExistsOp is a BinaryEvalOp.
type JSONExistsOp struct{}

// JSONSomeExistsOp is a BinaryEvalOp.
type JSONSomeExistsOp struct{}

// JSONAllExistsOp is a BinaryEvalOp.
type JSONAllExistsOp struct{}

// JSONFetchValPathOp is a BinaryEvalOp.
type JSONFetchValPathOp struct{}

// JSONFetchTextPathOp is a BinaryEvalOp.
type JSONFetchTextPathOp struct{}

// ContainsArrayOp is a BinaryEvalOp.
type ContainsArrayOp struct{}

// ContainsJsonbOp is a BinaryEvalOp.
type ContainsJsonbOp struct{}

// ContainedByArrayOp is a BinaryEvalOp.
type ContainedByArrayOp struct{}

// ContainedByJsonbOp is a BinaryEvalOp.
type ContainedByJsonbOp struct{}
