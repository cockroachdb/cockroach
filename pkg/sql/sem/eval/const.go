// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// IsConst returns whether the expression is constant. A constant expression
// does not contain variables, as defined by ContainsVars, nor impure functions.
func IsConst(evalCtx *Context, expr tree.TypedExpr) bool {
	v := isConstVisitor{ctx: evalCtx}
	return v.run(expr)
}

type isConstVisitor struct {
	ctx     *Context
	isConst bool
}

var _ tree.Visitor = &isConstVisitor{}

func (v *isConstVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.isConst {
		if !tree.OperatorIsImmutable(expr) ||
			IsVar(v.ctx, expr, true /*allowConstPlaceholders*/) {
			v.isConst = false
			return false, expr
		}
	}
	return true, expr
}

func (*isConstVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (v *isConstVisitor) run(expr tree.Expr) bool {
	v.isConst = true
	tree.WalkExprConst(v, expr)
	return v.isConst
}

// IsVar returns true if the expression's value can vary during plan
// execution. The parameter allowConstPlaceholders should be true
// in the common case of scalar expressions that will be evaluated
// in the context of the execution of a prepared query, where the
// placeholder will have the same value for every row processed.
// It is set to false for scalar expressions that are not
// evaluated as part of query execution, eg. tree.DEFAULT expressions.
func IsVar(evalCtx *Context, expr tree.Expr, allowConstPlaceholders bool) bool {
	switch expr.(type) {
	case tree.VariableExpr:
		return true
	case *tree.Placeholder:
		if allowConstPlaceholders {
			if evalCtx == nil || !evalCtx.HasPlaceholders() {
				// The placeholder cannot be resolved -- it is variable.
				return true
			}
			return evalCtx.Placeholders.IsUnresolvedPlaceholder(expr)
		}
		// Placeholders considered always variable.
		return true
	}
	return false
}
