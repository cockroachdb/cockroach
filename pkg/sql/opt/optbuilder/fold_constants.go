// Copyright 2018 The Cockroach Authors.
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

package optbuilder

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// foldConstantsVisitor supports the folding of constants.
type foldConstantsVisitor struct {
	ctx *tree.EvalContext
	err error

	fastIsConstVisitor fastIsConstVisitor
}

var _ tree.Visitor = &foldConstantsVisitor{}

// makeFoldConstantsVisitor creates a foldConstantsVisitor instance.
func makeFoldConstantsVisitor(ctx *tree.EvalContext) foldConstantsVisitor {
	return foldConstantsVisitor{ctx: ctx, fastIsConstVisitor: fastIsConstVisitor{ctx: ctx}}
}

// Err retrieves the error field in the foldConstantsVisitor.
func (v *foldConstantsVisitor) Err() error { return v.err }

// VisitPre implements the Visitor interface.
func (v *foldConstantsVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch expr.(type) {
	case *tree.Subquery:
		// Subqueries are pre-normalized during semantic analysis. There
		// is nothing to do here.
		return false, expr
	}

	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *foldConstantsVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}
	// We don't propagate errors during this step because errors might involve a
	// branch of code that isn't traversed by normal execution (for example,
	// IF(2 = 2, 1, 1 / 0)).

	// Evaluate all constant expressions.
	if v.isConst(expr) {
		value, err := expr.(tree.TypedExpr).Eval(v.ctx)
		if err != nil {
			// Ignore any errors here (e.g. division by zero), so they can happen
			// during execution where they are correctly handled. Note that in some
			// cases we might not even get an error (if this particular expression
			// does not get evaluated when the query runs, e.g. it's inside a CASE).
			return expr
		}
		if value == tree.DNull {
			// We don't want to return an expression that has a different type; cast
			// the NULL if necessary.
			var newExpr tree.TypedExpr
			newExpr, v.err = tree.ReType(tree.DNull, expr.(tree.TypedExpr).ResolvedType())
			if v.err != nil {
				return expr
			}
			return newExpr
		}
		return value
	}

	return expr
}

func (v *foldConstantsVisitor) isConst(expr tree.Expr) bool {
	return v.fastIsConstVisitor.run(expr)
}

// fastIsConstVisitor determines if an expression is constant by visiting
// at most two levels of the tree (with one exception, see below).
// In essence, it determines whether an expression is constant by checking
// whether its children are const Datums.
//
// This can be used by foldConstantsVisitor since constants are evaluated
// bottom-up. If a child is *not* a const Datum, that means it was already
// determined to be non-constant, and therefore was not evaluated.
type fastIsConstVisitor struct {
	ctx     *tree.EvalContext
	isConst bool

	// visited indicates whether we have already visited one level of the tree.
	// fastIsConstVisitor only visits at most two levels of the tree, with one
	// exception: If the second level has a Cast expression, fastIsConstVisitor
	// may visit three levels.
	visited bool
}

var _ tree.Visitor = &fastIsConstVisitor{}

func (v *fastIsConstVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.visited {
		if _, ok := expr.(*tree.CastExpr); ok {
			// We recurse one more time for cast expressions, since the
			// foldConstantsVisitor may have wrapped a NULL.
			return true, expr
		}
		if _, ok := expr.(tree.Datum); !ok || isVar(v.ctx, expr) {
			// If the child expression is not a const Datum, the parent expression is
			// not constant. Note that all constant literals have already been
			// normalized to Datum in TypeCheck.
			v.isConst = false
		}
		return false, expr
	}
	v.visited = true

	// If the parent expression is a variable or impure function, we know that it
	// is not constant.

	if isVar(v.ctx, expr) {
		v.isConst = false
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsImpure() {
			v.isConst = false
			return false, expr
		}
	}

	return true, expr
}

func (*fastIsConstVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (v *fastIsConstVisitor) run(expr tree.Expr) bool {
	v.isConst = true
	v.visited = false
	tree.WalkExprConst(v, expr)
	return v.isConst
}

// isVar returns true if the expression's value can vary during plan
// execution.
func isVar(evalCtx *tree.EvalContext, expr tree.Expr) bool {
	switch expr.(type) {
	case tree.VariableExpr:
		return true
	case *tree.Placeholder:
		return evalCtx != nil && (!evalCtx.HasPlaceholders() || evalCtx.Placeholders.IsUnresolvedPlaceholder(expr))
	}
	return false
}
