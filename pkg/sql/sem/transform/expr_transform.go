// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package transform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// ExprTransformContext supports the methods that test expression
// properties and can normalize expressions, defined below.  This
// should be used in planner instance to avoid re-allocation of these
// visitors between uses.
type ExprTransformContext struct {
	normalizeVisitor   tree.NormalizeVisitor
	isAggregateVisitor IsAggregateVisitor
}

// NormalizeExpr is a wrapper around EvalContex.NormalizeExpr which
// avoids allocation of a normalizeVisitor. See normalize.go for
// details.
func (t *ExprTransformContext) NormalizeExpr(
	ctx *tree.EvalContext, typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	if ctx.SkipNormalize {
		return typedExpr, nil
	}
	t.normalizeVisitor = tree.MakeNormalizeVisitor(ctx)
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
	expr tree.Expr, searchPath sessiondata.SearchPath,
) bool {
	if expr == nil {
		return false
	}

	t.isAggregateVisitor = IsAggregateVisitor{
		searchPath: searchPath,
	}
	tree.WalkExprConst(&t.isAggregateVisitor, expr)
	return t.isAggregateVisitor.Aggregated
}
