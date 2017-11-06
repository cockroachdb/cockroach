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

package parser

import "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

// ExprTransformContext supports the methods that test expression
// properties and can normalize expressions, defined below.  This
// should be used in planner instance to avoid re-allocation of these
// visitors between uses.
type ExprTransformContext struct {
	normalizeVisitor      normalizeVisitor
	isAggregateVisitor    IsAggregateVisitor
	containsWindowVisitor ContainsWindowVisitor
}

// NormalizeExpr is wrapper around ctx.NormalizeExpr which avoids allocation of
// a normalizeVisitor.
func (t *ExprTransformContext) NormalizeExpr(
	ctx *EvalContext, typedExpr TypedExpr,
) (TypedExpr, error) {
	if ctx.SkipNormalize {
		return typedExpr, nil
	}
	t.normalizeVisitor = makeNormalizeVisitor(ctx)
	expr, _ := WalkExpr(&t.normalizeVisitor, typedExpr)
	if err := t.normalizeVisitor.err; err != nil {
		return nil, err
	}
	return expr.(TypedExpr), nil
}

// AggregateInExpr determines if an Expr contains an aggregate function.
func (t *ExprTransformContext) AggregateInExpr(expr Expr, searchPath SearchPath) bool {
	if expr != nil {
		t.isAggregateVisitor.searchPath = searchPath
		defer t.isAggregateVisitor.Reset()
		WalkExprConst(&t.isAggregateVisitor, expr)
		if t.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// IsAggregate determines if SelectClause contains an aggregate function.
func (t *ExprTransformContext) IsAggregate(n *SelectClause, searchPath SearchPath) bool {
	if n.Having != nil || len(n.GroupBy) > 0 {
		return true
	}

	t.isAggregateVisitor.searchPath = searchPath
	defer t.isAggregateVisitor.Reset()
	for _, target := range n.Exprs {
		WalkExprConst(&t.isAggregateVisitor, target.Expr)
		if t.isAggregateVisitor.Aggregated {
			return true
		}
	}
	return false
}

// AssertNoAggregationOrWindowing checks if the provided expression contains either
// aggregate functions or window functions, returning an error in either case.
func (t *ExprTransformContext) AssertNoAggregationOrWindowing(
	expr Expr, op string, searchPath SearchPath,
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
func (t *ExprTransformContext) WindowFuncInExpr(expr Expr) bool {
	return t.containsWindowVisitor.ContainsWindowFunc(expr)
}

// WindowFuncInExprs determines if any of the provided TypedExpr contains a
// window function, using the Parser's embedded visitor.
func (t *ExprTransformContext) WindowFuncInExprs(exprs []TypedExpr) bool {
	for _, expr := range exprs {
		if t.WindowFuncInExpr(expr) {
			return true
		}
	}
	return false
}
