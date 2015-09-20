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
func NormalizeExpr(ctx EvalContext, expr Expr) (Expr, error) {
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

	if pre {
		switch t := expr.(type) {
		case *ParenExpr:
			// (a) -> a
			return v.Visit(t.Expr, true)

		case Row:
			// ROW(a, b, c) -> (a, b, c)
			return v.Visit(Tuple(t), true)

		case *RangeCond:
			return v.Visit(v.normalizeRangeCond(t), true)

		case *UnaryExpr:
			// Ugliness: when we see a UnaryMinus, check to see if the expression
			// being negated is math.MinInt64. This IntVal is only possible if we
			// parsed "9223372036854775808" as a signed int and is the only negative
			// IntVal that can be output from the scanner.
			//
			// TODO(pmattis): Seems like this should happen in EvalExpr, yet if we
			// put it there we blow up during normalization when we try to
			// EvalExpr("9223372036854775808") a few lines down from here before
			// doing EvalExpr("- 9223372036854775808"). Perhaps we can move
			// expression evaluation during normalization to the downward
			// traversal. Or do it during the downward traversal for const
			// UnaryExprs.
			if t.Operator == UnaryMinus {
				if d, ok := t.Expr.(IntVal); ok && d == math.MinInt64 {
					return v, DInt(math.MinInt64)
				}
			}
		}

		return v, expr
	}

	// Evaluate constant expressions.
	if isConst(expr) {
		expr, v.err = v.ctx.EvalExpr(expr)
		if v.err != nil {
			return nil, expr
		}
	} else {
		switch t := expr.(type) {
		case *AndExpr:
			return v.normalizeAndExpr(t)

		case *OrExpr:
			return v.normalizeOrExpr(t)

		case *ComparisonExpr:
			return v.normalizeComparisonExpr(t)
		}
	}
	return v, expr
}

func (v *normalizeVisitor) normalizeRangeCond(n *RangeCond) Expr {
	var expr Expr
	if n.Not {
		// "a NOT BETWEEN b AND c" -> "a < b OR a > c"
		expr = &OrExpr{
			Left: &ComparisonExpr{
				Operator: LT,
				Left:     n.Left,
				Right:    n.From,
			},
			Right: &ComparisonExpr{
				Operator: GT,
				Left:     n.Left,
				Right:    n.To,
			},
		}
	} else {
		// "a BETWEEN b AND c" -> "a >= b AND a <= c"
		expr = &AndExpr{
			Left: &ComparisonExpr{
				Operator: GE,
				Left:     n.Left,
				Right:    n.From,
			},
			Right: &ComparisonExpr{
				Operator: LE,
				Left:     n.Left,
				Right:    n.To,
			},
		}
	}
	return expr
}

func (v *normalizeVisitor) normalizeAndExpr(n *AndExpr) (Visitor, Expr) {
	// Use short-circuit evaluation to simplify AND expressions.
	if isConst(n.Left) {
		n.Left, v.err = v.ctx.EvalExpr(n.Left)
		if v.err != nil {
			return nil, n
		}
		if n.Left != DNull {
			if d, err := getBool(n.Left.(Datum)); err != nil {
				return v, DNull
			} else if !d {
				return v, n.Left
			}
			return v, n.Right
		}
		return v, n
	}
	if isConst(n.Right) {
		n.Right, v.err = v.ctx.EvalExpr(n.Right)
		if v.err != nil {
			return nil, n
		}
		if n.Right != DNull {
			if d, err := getBool(n.Right.(Datum)); err != nil {
				return v, DNull
			} else if d {
				return v, n.Left
			}
			return v, n.Right
		}
		return v, n
	}
	return v, n
}

func (v *normalizeVisitor) normalizeOrExpr(n *OrExpr) (Visitor, Expr) {
	// Use short-circuit evaluation to simplify OR expressions.
	if isConst(n.Left) {
		n.Left, v.err = v.ctx.EvalExpr(n.Left)
		if v.err != nil {
			return nil, n
		}
		if n.Left != DNull {
			if d, err := getBool(n.Left.(Datum)); err != nil {
				return v, DNull
			} else if d {
				return v, n.Left
			}
			return v, n.Right
		}
	}
	if isConst(n.Right) {
		n.Right, v.err = v.ctx.EvalExpr(n.Right)
		if v.err != nil {
			return nil, n
		}
		if n.Right != DNull {
			if d, err := getBool(n.Right.(Datum)); err != nil {
				return v, DNull
			} else if d {
				return v, n.Right
			}
			return v, n.Left
		}
	}
	return v, n
}

func (v *normalizeVisitor) normalizeComparisonExpr(n *ComparisonExpr) (Visitor, Expr) {
	// We want var nodes (DReference, QualifiedName, etc) to be immediate
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

	switch n.Operator {
	case EQ, GE, GT, LE, LT:
		break
	default:
		return v, n
	}

	// We loop attempting to simplify the comparison expression. As a
	// pre-condition, we know there is at least one variable in the expression
	// tree or we would not have entered this code path.
	for {
		if isConst(n.Left) {
			switch n.Right.(type) {
			case *BinaryExpr, DReference, *QualifiedName, ValArg:
				break
			default:
				return v, n
			}
			// The left side is const and the right side is a binary expression or a
			// variable. Flip the comparison op so that the right side is const and
			// the left side is a binary expression or variable.
			n.Operator = invertComparisonOp(n.Operator)
			n.Left, n.Right = n.Right, n.Left
		} else if !isConst(n.Right) {
			return v, n
		}

		left, ok := n.Left.(*BinaryExpr)
		if !ok {
			return v, n
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
				n.Left = left.Left
				left.Left = n.Right
				if left.Operator == Plus {
					left.Operator = Minus
				} else if left.Operator == Minus {
					left.Operator = Plus
				} else {
					left.Operator = Mult
				}
				// Clear the function cache now that we've changed the operator.
				left.fn.fn = nil
				n.Right, v.err = v.ctx.EvalExpr(left)
				if v.err != nil {
					return nil, nil
				}
				if !isVar(n.Left) {
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
				left.Right, n.Right = n.Right, left.Right
				if left.Operator == Plus {
					left.Operator = Minus
					left.Left, left.Right = left.Right, left.Left
				} else {
					n.Operator = invertComparisonOp(n.Operator)
				}
				n.Left, v.err = v.ctx.EvalExpr(left)
				if v.err != nil {
					return nil, nil
				}
				n.Left, n.Right = n.Right, n.Left
				if !isVar(n.Left) {
					// Continue as long as the left side of the comparison is not a
					// variable.
					continue
				}
			}
		}

		// We've run out of work to do.
		return v, n
	}
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
			if _, err := typeCheckFuncExpr(t); err != nil || t.fn.impure {
				v.isConst = false
				return nil, expr
			}
		}
	}
	return v, expr
}

// isConst returns true if the expression contains only constant values
// (i.e. it does not contain a DReference).
func isConst(expr Expr) bool {
	v := isConstVisitor{isConst: true}
	expr = WalkExpr(&v, expr)
	return v.isConst
}

func isVar(expr Expr) bool {
	switch expr.(type) {
	case DReference, *QualifiedName, ValArg:
		return true
	}
	return false
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
