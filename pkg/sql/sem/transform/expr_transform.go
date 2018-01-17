// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package transform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// ExprTransformContext supports the methods that test expression
// properties and can normalize expressions, defined below.  This
// should be used in planner instance to avoid re-allocation of these
// visitors between uses.
type ExprTransformContext struct {
	normalizeVisitor      tree.NormalizeVisitor
	isAggregateVisitor    IsAggregateVisitor
	containsWindowVisitor ContainsWindowVisitor
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
func (t *ExprTransformContext) AggregateInExpr(
	expr tree.Expr, searchPath sessiondata.SearchPath,
) bool {
	if expr == nil {
		return false
	}

	t.isAggregateVisitor.searchPath = searchPath
	defer t.isAggregateVisitor.Reset()
	tree.WalkExprConst(&t.isAggregateVisitor, expr)
	return t.isAggregateVisitor.Aggregated
}

// IsAggregate determines if the given SelectClause contains an
// aggregate function.
func (t *ExprTransformContext) IsAggregate(
	n *tree.SelectClause, searchPath sessiondata.SearchPath,
) bool {
	if n.Having != nil || len(n.GroupBy) > 0 {
		return true
	}

	t.isAggregateVisitor.searchPath = searchPath
	defer t.isAggregateVisitor.Reset()
	// Check SELECT expressions.
	for _, target := range n.Exprs {
		tree.WalkExprConst(&t.isAggregateVisitor, target.Expr)
		if t.isAggregateVisitor.Aggregated {
			return true
		}
	}
	// Check DISTINCT ON expressions too.
	for _, expr := range n.DistinctOn {
		tree.WalkExprConst(&t.isAggregateVisitor, expr)
		if t.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// AssertNoAggregationOrWindowing checks if the provided expression contains either
// aggregate functions or window functions, returning an error in either case.
func (t *ExprTransformContext) AssertNoAggregationOrWindowing(
	expr tree.Expr, op string, searchPath sessiondata.SearchPath,
) error {
	if t.AggregateInExpr(expr, searchPath) {
		return pgerror.NewErrorf(pgerror.CodeGroupingError, "aggregate functions are not allowed in %s", op)
	}
	if t.WindowFuncInExpr(expr) {
		return pgerror.NewErrorf(pgerror.CodeWindowingError, "window functions are not allowed in %s", op)
	}
	return nil
}

// WindowFuncInExpr determines if an Expr contains a window function, using
// the Parser's embedded visitor.
func (t *ExprTransformContext) WindowFuncInExpr(expr tree.Expr) bool {
	return t.containsWindowVisitor.ContainsWindowFunc(expr)
}

// WindowFuncInExprs determines if any of the provided TypedExpr contains a
// window function, using the Parser's embedded visitor.
func (t *ExprTransformContext) WindowFuncInExprs(exprs []tree.TypedExpr) bool {
	for _, expr := range exprs {
		if t.WindowFuncInExpr(expr) {
			return true
		}
	}
	return false
}
