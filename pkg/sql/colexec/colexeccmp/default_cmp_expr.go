// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package colexeccmp exposes some comparison definitions for vectorized
// operations.
package colexeccmp

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ComparisonExprAdapter is a utility interface that is implemented by several
// structs that behave as an adapter from tree.ComparisonExpr to a vectorized
// friendly model.
type ComparisonExprAdapter interface {
	Eval(ctx context.Context, left, right tree.Datum) (tree.Datum, error)
}

// NewComparisonExprAdapter returns a new ComparisonExprAdapter for the provided
// expression.
func NewComparisonExprAdapter(
	expr *tree.ComparisonExpr, evalCtx *eval.Context,
) ComparisonExprAdapter {
	base := cmpExprAdapterBase{
		op:      expr.Op.EvalOp,
		evalCtx: evalCtx,
	}
	op := expr.Operator
	if op.Symbol.HasSubOperator() {
		return &cmpWithSubOperatorExprAdapter{
			cmpExprAdapterBase: base,
			expr:               expr,
		}
	}
	nullable := expr.Op.CalledOnNullInput
	_, _, _, flipped, negate := tree.FoldComparisonExpr(op, nil /* left */, nil /* right */)
	if nullable {
		if flipped {
			if negate {
				return &cmpNullableFlippedNegateExprAdapter{cmpExprAdapterBase: base}
			}
			return &cmpNullableFlippedExprAdapter{cmpExprAdapterBase: base}
		}
		if negate {
			return &cmpNullableNegateExprAdapter{cmpExprAdapterBase: base}
		}
		return &cmpNullableExprAdapter{cmpExprAdapterBase: base}
	}
	if flipped {
		if negate {
			return &cmpFlippedNegateExprAdapter{cmpExprAdapterBase: base}
		}
		return &cmpFlippedExprAdapter{cmpExprAdapterBase: base}
	}
	if negate {
		return &cmpNegateExprAdapter{cmpExprAdapterBase: base}
	}
	return &cmpExprAdapter{cmpExprAdapterBase: base}
}
