// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import "fmt"

type normalizableExpr interface {
	Expr
	normalize(*normalizeVisitor) TypedExpr
}

func (expr *AndExpr) normalize(v *normalizeVisitor) TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()

	// Use short-circuit evaluation to simplify AND expressions.
	if v.isConst(left) {
		left, v.err = left.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if left != DNull {
			if d, err := GetBool(left.(Datum)); err == nil {
				if !d {
					return left
				}
				return right
			}
			return DNull
		}
		return NewTypedAndExpr(
			left,
			right,
		)
	}
	if v.isConst(right) {
		right, v.err = right.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if right != DNull {
			if d, err := GetBool(expr.Right.(Datum)); err == nil {
				if !d {
					return right
				}
				return left
			}
			return DNull
		}
		return NewTypedAndExpr(
			left,
			right,
		)
	}
	return expr
}

func (expr *ComparisonExpr) normalize(v *normalizeVisitor) TypedExpr {

	switch expr.Operator {
	case EQ, GE, GT, LE, LT:
		// We want var nodes (VariableExpr, QualifiedName, etc) to be immediate
		// children of the comparison expression and not second or third
		// children. That is, we want trees that look like:
		//
		//    cmp            cmp
		//   /   \          /   \
		//  a    op        op    a
		//      /  \      /  \
		//     1    2    1    2
		//
		// Not trees that look like:
		//
		//      cmp          cmp        cmp          cmp
		//     /   \        /   \      /   \        /   \
		//    op    2      op    2    1    op      1    op
		//   /  \         /  \            /  \         /  \
		//  a    1       1    a          a    2       2    a
		//
		// We loop attempting to simplify the comparison expression. As a
		// pre-condition, we know there is at least one variable in the expression
		// tree or we would not have entered this code path.
		exprCopied := false
		for {
			if v.isConst(expr.Left) {
				switch expr.Right.(type) {
				case *BinaryExpr, VariableExpr:
					break
				default:
					return expr
				}
				// The left side is const and the right side is a binary expression or a
				// variable. Flip the comparison op so that the right side is const and
				// the left side is a binary expression or variable.
				// Create a new ComparisonExpr so the function cache isn't reused.
				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}
				expr = NewTypedComparisonExpr(
					invertComparisonOp(expr.Operator),
					expr.TypedRight(),
					expr.TypedLeft(),
				)
			} else if !v.isConst(expr.Right) {
				return expr
			}

			left, ok := expr.Left.(*BinaryExpr)
			if !ok {
				return expr
			}

			// The right is const and the left side is a binary expression. Rotate the
			// comparison combining portions that are const.

			switch {
			case v.isConst(left.Right) &&
				(left.Operator == Plus || left.Operator == Minus || left.Operator == Div):

				//        cmp          cmp
				//       /   \        /   \
				//    [+-/]   2  ->  a   [-+*]
				//   /     \            /     \
				//  a       1          2       1

				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}

				leftCopy := *left
				left = &leftCopy

				switch left.Operator {
				case Plus:
					left.Operator = Minus
				case Minus:
					left.Operator = Plus
				case Div:
					left.Operator = Mult
				}

				expr.Left = left.Left
				left.Left = expr.Right

				left.memoizeFn()
				expr.Right, v.err = left.Eval(v.ctx)
				if v.err != nil {
					return nil
				}
				expr.memoizeFn()
				if !isVar(expr.Left) {
					// Continue as long as the left side of the comparison is not a
					// variable.
					continue
				}

			case v.isConst(left.Left) && (left.Operator == Plus || left.Operator == Minus):
				//       cmp              cmp
				//      /   \            /   \
				//    [+-]   2  ->     [+-]   a
				//   /    \           /    \
				//  1      a         1      2

				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}

				leftCopy := *left
				left = &leftCopy

				// Clear the function caches; we're about to change stuff.
				left.Right, expr.Right = expr.Right, left.Right
				if left.Operator == Plus {
					left.Operator = Minus
					left.Left, left.Right = left.Right, left.Left
				} else {
					expr.Operator = invertComparisonOp(expr.Operator)
				}

				left.memoizeFn()
				expr.Left, v.err = left.Eval(v.ctx)
				if v.err != nil {
					return nil
				}
				expr.Left, expr.Right = expr.Right, expr.Left
				expr.memoizeFn()
				if !isVar(expr.Left) {
					// Continue as long as the left side of the comparison is not a
					// variable.
					continue
				}
			}

			// We've run out of work to do.
			break
		}
	case In, NotIn:
		// If the right tuple in an In or NotIn comparison expression is constant, it can
		// be normalized.
		tuple, ok := expr.Right.(*DTuple)
		if ok {
			tupleCopy := *tuple
			tupleCopy.Normalize()
			exprCopy := *expr
			expr = &exprCopy
			expr.Right = &tupleCopy
		}
	}

	return expr
}

func (expr *OrExpr) normalize(v *normalizeVisitor) TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()

	// Use short-circuit evaluation to simplify OR expressions.
	if v.isConst(left) {
		left, v.err = left.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if left != DNull {
			if d, err := GetBool(left.(Datum)); err == nil {
				if d {
					return left
				}
				return right
			}
			return DNull
		}
		return NewTypedOrExpr(
			left,
			right,
		)
	}
	if v.isConst(right) {
		right, v.err = right.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if right != DNull {
			if d, err := GetBool(right.(Datum)); err == nil {
				if d {
					return right
				}
				return left
			}
			return DNull
		}
		return NewTypedOrExpr(
			left,
			right,
		)
	}
	return expr
}

func (expr *ParenExpr) normalize(v *normalizeVisitor) TypedExpr {
	return expr.TypedInnerExpr()
}

func (expr *RangeCond) normalize(v *normalizeVisitor) TypedExpr {
	if expr.Not {
		// "a NOT BETWEEN b AND c" -> "a < b OR a > c"
		return NewTypedOrExpr(
			NewTypedComparisonExpr(
				LT,
				expr.TypedLeft(),
				expr.TypedFrom(),
			),
			NewTypedComparisonExpr(
				GT,
				expr.TypedLeft(),
				expr.TypedTo(),
			),
		)
	}

	// "a BETWEEN b AND c" -> "a >= b AND a <= c"
	return NewTypedAndExpr(
		NewTypedComparisonExpr(
			GE,
			expr.TypedLeft(),
			expr.TypedFrom(),
		),
		NewTypedComparisonExpr(
			LE,
			expr.TypedLeft(),
			expr.TypedTo(),
		),
	)
}

// NormalizeExpr normalizes a typed expression, simplifying where possible,
// but guaranteeing that the result of evaluating the expression is
// unchanged and that resulting expression tree is still well-typed.
// Example normalizations:
//
//   (a)                   -> a
//   a = 1 + 1             -> a = 2
//   a + 1 = 2             -> a = 1
//   a BETWEEN b AND c     -> (a >= b) AND (a <= c)
//   a NOT BETWEEN b AND c -> (a < b) OR (a > c)
func (ctx EvalContext) NormalizeExpr(typedExpr TypedExpr) (TypedExpr, error) {
	v := normalizeVisitor{ctx: ctx}
	expr, _ := WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, v.err
	}
	return expr.(TypedExpr), nil
}

type normalizeVisitor struct {
	ctx EvalContext
	err error

	isConstVisitor isConstVisitor
}

var _ Visitor = &normalizeVisitor{}

func (v *normalizeVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.err != nil {
		return false, expr
	}

	// Normalize expressions that know how to normalize themselves.
	if normalizeable, ok := expr.(normalizableExpr); ok {
		expr = normalizeable.normalize(v)
		if v.err != nil {
			return false, expr
		}
	}

	switch expr.(type) {
	case *CaseExpr, *IfExpr, *NullIfExpr, *CoalesceExpr:
		// Conditional expressions need to be evaluated during the downward
		// traversal in order to avoid evaluating sub-expressions which should
		// not be evaluated due to the case/conditional.
		if v.isConst(expr) {
			expr, v.err = expr.(TypedExpr).Eval(v.ctx)
			if v.err != nil {
				return false, expr
			}
		}
	case *Subquery:
		// Avoid normalizing subqueries. We need the subquery to be expanded in
		// order to do so properly.
		// TODO(knz) This should happen when the prepare and execute phases are
		//     separated for SelectClause.
		return false, expr
	}

	return true, expr
}

func (v *normalizeVisitor) VisitPost(expr Expr) Expr {
	if v.err != nil {
		return expr
	}

	// Normalize expressions that know how to normalize themselves.
	if normalizeable, ok := expr.(normalizableExpr); ok {
		expr = normalizeable.normalize(v)
		if v.err != nil {
			return expr
		}
	}

	// Evaluate all constant expressions.
	if v.isConst(expr) {
		expr, v.err = expr.(TypedExpr).Eval(v.ctx)
	}
	return expr
}

func (v *normalizeVisitor) isConst(expr Expr) bool {
	return v.isConstVisitor.run(expr)
}

func invertComparisonOp(op ComparisonOperator) ComparisonOperator {
	switch op {
	case EQ:
		return EQ
	case GE:
		return LE
	case GT:
		return LT
	case LE:
		return GE
	case LT:
		return GT
	default:
		panic(fmt.Sprintf("unable to invert: %s", op))
	}
}

type isConstVisitor struct {
	isConst bool
}

var _ Visitor = &isConstVisitor{}

func (v *isConstVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.isConst {
		if isVar(expr) {
			v.isConst = false
			return false, expr
		}

		switch t := expr.(type) {
		case *Subquery:
			v.isConst = false
			return false, expr
		case *FuncExpr:
			if t.fn.impure {
				v.isConst = false
				return false, expr
			}
		}
	}
	return true, expr
}

func (*isConstVisitor) VisitPost(expr Expr) Expr { return expr }

func (v *isConstVisitor) run(expr Expr) bool {
	v.isConst = true
	WalkExprConst(v, expr)
	return v.isConst
}

func isVar(expr Expr) bool {
	_, ok := expr.(VariableExpr)
	return ok
}

type containsVarsVisitor struct {
	containsVars bool
}

var _ Visitor = &containsVarsVisitor{}

func (v *containsVarsVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if !v.containsVars && isVar(expr) {
		v.containsVars = true
	}
	if v.containsVars {
		return false, expr
	}
	return true, expr
}

func (*containsVarsVisitor) VisitPost(expr Expr) Expr { return expr }

// ContainsVars returns true if the expression contains any variables.
func ContainsVars(expr Expr) bool {
	v := containsVarsVisitor{containsVars: false}
	WalkExprConst(&v, expr)
	return v.containsVars
}
