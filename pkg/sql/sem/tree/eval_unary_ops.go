// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "context"

// UnaryNoop is a UnaryEvalOp.
type UnaryNoop struct{}

// Eval of UnaryNoop does nothing and returns the passed Datum.
func (v *UnaryNoop) Eval(ctx context.Context, evaluator OpEvaluator, d Datum) (Datum, error) {
	return d, nil
}

type (
	// UnaryMinusFloatOp is a UnaryEvalOp.
	UnaryMinusFloatOp struct{}
	// UnaryMinusIntervalOp is a UnaryEvalOp.
	UnaryMinusIntervalOp struct{}
	// UnaryMinusIntOp is a UnaryEvalOp.
	UnaryMinusIntOp struct{}
	// UnaryMinusDecimalOp is a UnaryEvalOp.
	UnaryMinusDecimalOp struct{}
)
type (
	// ComplementIntOp is a UnaryEvalOp.
	ComplementIntOp struct{}
	// ComplementVarBitOp is a UnaryEvalOp.
	ComplementVarBitOp struct{}
	// ComplementINetOp is a UnaryEvalOp.
	ComplementINetOp struct{}
)
type (
	// SqrtFloatOp is a UnaryEvalOp.
	SqrtFloatOp struct{}
	// SqrtDecimalOp is a UnaryEvalOp.
	SqrtDecimalOp struct{}
	// CbrtFloatOp is a UnaryEvalOp.
	CbrtFloatOp struct{}
	// CbrtDecimalOp is a UnaryEvalOp.
	CbrtDecimalOp struct{}
)
