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

// WindowFuncInExpr determines if an Expr contains a window function, using
// the Parser's embedded visitor.
// TODO(knz/radu): this is not the right way to go about checking
// these things. Instead whatever analysis occurs prior on the expression
// should collect scalar properties (see tree.ScalarProperties) and
// then the collected properties should be tested directly.
func (t *ExprTransformContext) WindowFuncInExpr(expr tree.Expr) bool {
	t.containsWindowVisitor = ContainsWindowVisitor{}
	return t.containsWindowVisitor.ContainsWindowFunc(expr)
}
