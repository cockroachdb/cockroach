// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package transform

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// ExprTransformContext supports the methods that test expression
// properties and can normalize expressions, defined below.  This
// should be used in planner instance to avoid re-allocation of these
// visitors between uses.
type ExprTransformContext struct {
	normalizeVisitor   normalize.Visitor
	isAggregateVisitor isAggregateVisitor
}

// NormalizeExpr is a wrapper around EvalContex.Expr which
// avoids allocation of a normalizeVisitor. See var_expr.go for
// details.
func (t *ExprTransformContext) NormalizeExpr(
	ctx context.Context, evalCtx *eval.Context, typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	if evalCtx.SkipNormalize {
		return typedExpr, nil
	}
	t.normalizeVisitor = normalize.MakeNormalizeVisitor(ctx, evalCtx)
	expr, _ := tree.WalkExpr(&t.normalizeVisitor, typedExpr)
	if err := t.normalizeVisitor.Err(); err != nil {
		return nil, err
	}
	return expr.(tree.TypedExpr), nil
}

// AggregateInExpr determines if an Expr contains an aggregate function.
// TODO(knz/radu): this is not the right way to go about checking
// these things. Instead whatever analysis occurs prior on the expression
// should collect scalar properties (see tree.ScalarProperties) and
// then the collected properties should be tested directly.
func (t *ExprTransformContext) AggregateInExpr(
	ctx context.Context, expr tree.Expr, searchPath sessiondata.SearchPath,
) bool {
	if expr == nil {
		return false
	}

	t.isAggregateVisitor = isAggregateVisitor{
		ctx:        ctx,
		searchPath: searchPath,
	}
	tree.WalkExprConst(&t.isAggregateVisitor, expr)
	return t.isAggregateVisitor.Aggregated
}
