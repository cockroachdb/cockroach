// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// This file is used as input to the evalgen command to generate the methods
// and interface for eval_op_generated.go. Note that operations which define
// their own Eval method will not have one generated for them and will not be
// a part of the relevant Evaluator interface.

// UnaryNoop is a UnaryEvalOp.
type UnaryNoop struct{}

// Eval of UnaryNoop does nothing and returns the passed Datum.
func (v *UnaryNoop) Eval(evaluator OpEvaluator, d Datum) (Datum, error) {
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
