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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"fmt"
	"math"
)

type normalizableExpr interface {
	Expr
	normalize(*normalizeVisitor) Expr
}

func (expr *AndExpr) normalize(v *normalizeVisitor) Expr {
	// Use short-circuit evaluation to simplify AND expressions.
	if isConst(expr.Left) {
		expr.Left, v.err = expr.Left.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if expr.Left != DNull {
			if d, err := getBool(expr.Left.(Datum)); err != nil {
				return DNull
			} else if !d {
				return expr.Left
			}
			return expr.Right
		}
		return expr
	}
	if isConst(expr.Right) {
		expr.Right, v.err = expr.Right.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if expr.Right != DNull {
			if d, err := getBool(expr.Right.(Datum)); err != nil {
				return DNull
			} else if d {
				return expr.Left
			}
			return expr.Right
		}
		return expr
	}
	return expr
}

func (expr *ComparisonExpr) normalize(v *normalizeVisitor) Expr {
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
		for {
			if isConst(expr.Left) {
				switch expr.Right.(type) {
				case *BinaryExpr, VariableExpr, *QualifiedName, ValArg:
					break
				default:
					return expr
				}
				// The left side is const and the right side is a binary expression or a
				// variable. Flip the comparison op so that the right side is const and
				// the left side is a binary expression or variable.
				expr.Operator = invertComparisonOp(expr.Operator)
				expr.Left, expr.Right = expr.Right, expr.Left
			} else if !isConst(expr.Right) {
				return expr
			}

			left, ok := expr.Left.(*BinaryExpr)
			if !ok {
				return expr
			}

			// The right is const and the left side is a binary expression. Rotate the
			// comparison combining portions that are const.

			switch {
			case isConst(left.Right):
				//        cmp          cmp
				//       /   \        /   \
				//    [+-/]   2  ->  a   [-+*]
				//   /     \            /     \
				//  a       1          2       1

				switch left.Operator {
				case Plus, Minus, Div:
					expr.Left = left.Left
					left.Left = expr.Right
					if left.Operator == Plus {
						left.Operator = Minus
					} else if left.Operator == Minus {
						left.Operator = Plus
					} else {
						left.Operator = Mult
					}
					// Clear the function cache now that we've changed the operator.
					left.fn.fn = nil
					expr.Right, v.err = left.Eval(v.ctx)
					if v.err != nil {
						return nil
					}
					if !isVar(expr.Left) {
						// Continue as long as the left side of the comparison is not a
						// variable.
						continue
					}
				}

			case isConst(left.Left):
				//       cmp              cmp
				//      /   \            /   \
				//    [+-]   2  ->     [+-]   a
				//   /    \           /    \
				//  1      a         1      2

				switch left.Operator {
				case Plus, Minus:
					left.Right, expr.Right = expr.Right, left.Right
					if left.Operator == Plus {
						left.Operator = Minus
						left.Left, left.Right = left.Right, left.Left
					} else {
						expr.Operator = invertComparisonOp(expr.Operator)
					}
					expr.Left, v.err = left.Eval(v.ctx)
					if v.err != nil {
						return nil
					}
					expr.Left, expr.Right = expr.Right, expr.Left
					if !isVar(expr.Left) {
						// Continue as long as the left side of the comparison is not a
						// variable.
						continue
					}
				}
			}

			// We've run out of work to do.
			break
		}
	case In, NotIn:
		// If the right tuple in an In or NotIn comparison expression is constant, it can
		// be normalized.
		tuple, ok := expr.Right.(DTuple)
		if ok {
			tuple.Normalize()
			expr.Right = tuple
		}
	}

	return expr
}

func (expr *OrExpr) normalize(v *normalizeVisitor) Expr {
	// Use short-circuit evaluation to simplify OR expressions.
	if isConst(expr.Left) {
		expr.Left, v.err = expr.Left.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if expr.Left != DNull {
			if d, err := getBool(expr.Left.(Datum)); err != nil {
				return DNull
			} else if d {
				return expr.Left
			}
			return expr.Right
		}
	}
	if isConst(expr.Right) {
		expr.Right, v.err = expr.Right.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if expr.Right != DNull {
			if d, err := getBool(expr.Right.(Datum)); err != nil {
				return DNull
			} else if d {
				return expr.Right
			}
			return expr.Left
		}
	}
	return expr
}

func (expr *ParenExpr) normalize(v *normalizeVisitor) Expr {
	return expr.Expr
}

func (expr *RangeCond) normalize(v *normalizeVisitor) Expr {
	if expr.Not {
		// "a NOT BETWEEN b AND c" -> "a < b OR a > c"
		return &OrExpr{
			Left: &ComparisonExpr{
				Operator: LT,
				Left:     expr.Left,
				Right:    expr.From,
			},
			Right: &ComparisonExpr{
				Operator: GT,
				Left:     expr.Left,
				Right:    expr.To,
			},
		}
	}

	// "a BETWEEN b AND c" -> "a >= b AND a <= c"
	return &AndExpr{
		Left: &ComparisonExpr{
			Operator: GE,
			Left:     expr.Left,
			Right:    expr.From,
		},
		Right: &ComparisonExpr{
			Operator: LE,
			Left:     expr.Left,
			Right:    expr.To,
		},
	}
}

func (expr *UnaryExpr) normalize(v *normalizeVisitor) Expr {
	// Ugliness: when we see a UnaryMinus, check to see if the expression
	// being negated is math.MinInt64. This IntVal is only possible if we
	// parsed "9223372036854775808" as a signed int and is the only negative
	// IntVal that can be output from the scanner.
	//
	// TODO(pmattis): Seems like this should happen in Eval, yet if we
	// put it there we blow up during normalization when we try to
	// Eval("9223372036854775808") a few lines down from here before
	// doing Eval("- 9223372036854775808"). Perhaps we can move
	// expression evaluation during normalization to the downward
	// traversal. Or do it during the downward traversal for const
	// UnaryExprs.
	if expr.Operator == UnaryMinus {
		if d, ok := expr.Expr.(IntVal); ok && d == math.MinInt64 {
			return DInt(math.MinInt64)
		}
	}

	return expr
}

func (expr Row) normalize(v *normalizeVisitor) Expr {
	return Tuple(expr)
}

func (expr DTuple) normalize(_ *normalizeVisitor) Expr {
	expr.Normalize()
	return expr
}

// NormalizeExpr normalizes an expression, simplifying where possible, but
// guaranteeing that the result of evaluating the expression is
// unchanged. Example normalizations:
//
//   (a)                   -> a
//   ROW(a, b, c)          -> (a, b, c)
//   a = 1 + 1             -> a = 2
//   a + 1 = 2             -> a = 1
//   a BETWEEN b AND c     -> (a >= b) AND (a <= c)
//   a NOT BETWEEN b AND c -> (a < b) OR (a > c)
func (ctx EvalContext) NormalizeExpr(expr Expr) (Expr, error) {
	v := normalizeVisitor{ctx: ctx}
	expr = WalkExpr(&v, expr)
	return expr, v.err
}

type normalizeVisitor struct {
	ctx EvalContext
	err error
}

var _ Visitor = &normalizeVisitor{}

func (v *normalizeVisitor) Visit(expr Expr, pre bool) (Visitor, Expr) {
	if v.err != nil {
		return nil, expr
	}

	// Normalize expressions that know how to normalize themselves.
	if normalizeable, ok := expr.(normalizableExpr); ok {
		expr = normalizeable.normalize(v)
		if v.err != nil {
			return nil, expr
		}
	}

	if pre {
		switch expr.(type) {
		case *CaseExpr, *IfExpr, *NullIfExpr, *CoalesceExpr:
			// Conditional expressions need to be evaluated during the downward
			// traversal in order to avoid evaluating sub-expressions which should
			// not be evaluated due to the case/conditional.
			if isConst(expr) {
				expr, v.err = expr.Eval(v.ctx)
				if v.err != nil {
					return nil, expr
				}
			}
		}
	} else {
		// Evaluate all constant expressions.
		if isConst(expr) {
			expr, v.err = expr.Eval(v.ctx)
			if v.err != nil {
				return nil, expr
			}
		}
	}

	return v, expr
}

func invertComparisonOp(op ComparisonOp) ComparisonOp {
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

func (v *isConstVisitor) Visit(expr Expr, pre bool) (Visitor, Expr) {
	if pre && v.isConst {
		if isVar(expr) {
			v.isConst = false
			return nil, expr
		}

		switch t := expr.(type) {
		case *Subquery:
			v.isConst = false
			return nil, expr
		case *FuncExpr:
			// typeCheckFuncExpr populates t.fn.impure.
			if _, err := t.TypeCheck(); err != nil || t.fn.impure {
				v.isConst = false
				return nil, expr
			}
		}
	}
	return v, expr
}

// isConst returns true if the expression contains only constant values
// (i.e. it does not contain a VariableExpr, QualifiedName, etc).
func isConst(expr Expr) bool {
	v := isConstVisitor{isConst: true}
	expr = WalkExpr(&v, expr)
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

func (v *containsVarsVisitor) Visit(expr Expr, pre bool) (Visitor, Expr) {
	if pre && !v.containsVars {
		if isVar(expr) {
			v.containsVars = true
			return nil, expr
		}
	}
	return v, expr
}

// ContainsVars returns true if the expression contains any variables.
func ContainsVars(expr Expr) bool {
	v := containsVarsVisitor{containsVars: false}
	expr = WalkExpr(&v, expr)
	return v.containsVars
}
