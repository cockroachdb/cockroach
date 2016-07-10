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

import "github.com/pkg/errors"

type normalizableExpr interface {
	Expr
	normalize(*normalizeVisitor) TypedExpr
}

func (expr *CastExpr) normalize(v *normalizeVisitor) TypedExpr {
	if expr.Expr == DNull {
		return DNull
	}
	return expr
}

func (expr *CoalesceExpr) normalize(v *normalizeVisitor) TypedExpr {
	// This normalization checks whether COALESCE can be simplified
	// based on constant expressions at the start of the COALESCE
	// argument list. All known-null constant arguments are simply
	// removed, and any known-nonnull constant argument before
	// non-constant argument cause the entire COALESCE expression to
	// collapse to that argument.
	last := len(expr.Exprs) - 1
	for i := range expr.Exprs {
		subExpr := expr.TypedExprAt(i)

		if i == last {
			return subExpr
		}

		if !v.isConst(subExpr) {
			exprCopy := *expr
			exprCopy.Exprs = expr.Exprs[i:]
			return &exprCopy
		}

		val, err := subExpr.Eval(v.ctx)
		if err != nil {
			v.err = err
			return expr
		}

		if val != DNull {
			return subExpr
		}
	}
	return expr
}

func (expr *IfExpr) normalize(v *normalizeVisitor) TypedExpr {
	if v.isConst(expr.Cond) {
		cond, err := expr.TypedCondExpr().Eval(v.ctx)
		if err != nil {
			v.err = err
			return expr
		}
		if d, err := GetBool(cond); err == nil {
			if d {
				return expr.TypedTrueExpr()
			}
			return expr.TypedElseExpr()
		}
		return DNull
	}
	return expr
}

func (expr *UnaryExpr) normalize(v *normalizeVisitor) TypedExpr {
	val := expr.TypedInnerExpr()

	if val == DNull {
		return val
	}

	switch expr.Operator {
	case UnaryPlus:
		// +a -> a
		return val
	case UnaryMinus:
		// -0 -> 0 (except for float which has negative zero)
		if !val.ReturnType().TypeEqual(TypeFloat) && IsNumericZero(val) {
			return val
		}
		switch b := val.(type) {
		// -(a - b) -> (b - a)
		case *BinaryExpr:
			if b.Operator == Minus {
				newBinExpr := newBinExprIfValidOverload(Minus,
					b.TypedRight(), b.TypedLeft())
				if newBinExpr != nil {
					newBinExpr.memoizeFn()
					b = newBinExpr
				}
				return b
			}
		// - (- a) -> a
		case *UnaryExpr:
			if b.Operator == UnaryMinus {
				return b.TypedInnerExpr()
			}
		}
	}

	return expr
}

func (expr *BinaryExpr) normalize(v *normalizeVisitor) TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	expectedType := expr.ReturnType()

	if left == DNull || right == DNull {
		return DNull
	}

	var final TypedExpr

	switch expr.Operator {
	case Plus:
		if IsNumericZero(right) {
			final, v.err = ReType(left, expectedType)
			break
		}
		if IsNumericZero(left) {
			final, v.err = ReType(right, expectedType)
			break
		}
	case Minus:
		if IsNumericZero(right) {
			final, v.err = ReType(left, expectedType)
			break
		}
	case Mult:
		if IsNumericOne(right) {
			final, v.err = ReType(left, expectedType)
			break
		}
		if IsNumericOne(left) {
			final, v.err = ReType(right, expectedType)
			break
		}
		// We can't simplify multiplication by zero to zero,
		// because if the other operand is NULL during evaluation
		// the result must be NULL.
	case Div, FloorDiv:
		if IsNumericOne(right) {
			final, v.err = ReType(left, expectedType)
			break
		}
	}

	if final == nil {
		return expr
	}
	return final
}

func (expr *AndExpr) normalize(v *normalizeVisitor) TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	var dleft, dright Datum

	if left == DNull && right == DNull {
		return DNull
	}

	// Use short-circuit evaluation to simplify AND expressions.
	if v.isConst(left) {
		dleft, v.err = left.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if dleft != DNull {
			if d, err := GetBool(dleft); err == nil {
				if !d {
					return dleft
				}
				return right
			}
			return DNull
		}
		return NewTypedAndExpr(
			dleft,
			right,
		)
	}
	if v.isConst(right) {
		dright, v.err = right.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if dright != DNull {
			if d, err := GetBool(dright); err == nil {
				if !d {
					return right
				}
				return left
			}
			return DNull
		}
		return NewTypedAndExpr(
			left,
			dright,
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
			if expr.TypedLeft() == DNull || expr.TypedRight() == DNull {
				return DNull
			}

			if v.isConst(expr.Left) {
				switch expr.Right.(type) {
				case *BinaryExpr, VariableExpr:
					break
				default:
					return expr
				}

				invertedOp, err := invertComparisonOp(expr.Operator)
				if err != nil {
					v.err = nil
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

				expr = NewTypedComparisonExpr(invertedOp, expr.TypedRight(), expr.TypedLeft())
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
				var op BinaryOperator
				switch left.Operator {
				case Plus:
					op = Minus
				case Minus:
					op = Plus
				case Div:
					op = Mult
				}

				newBinExpr := newBinExprIfValidOverload(op,
					expr.TypedRight(), left.TypedRight())
				if newBinExpr == nil {
					// Substitution is not possible type-wise. Nothing else to do.
					break
				}

				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}

				expr.Left = left.Left
				expr.Right, v.err = newBinExpr.Eval(v.ctx)
				if v.err != nil {
					return expr
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

				op := expr.Operator
				var newBinExpr *BinaryExpr

				switch left.Operator {
				case Plus:
					//
					// (A + X) cmp B => X cmp (B - C)
					//
					newBinExpr = newBinExprIfValidOverload(Minus,
						expr.TypedRight(), left.TypedLeft())
				case Minus:
					//
					// (A - X) cmp B => X cmp' (A - B)
					//
					newBinExpr = newBinExprIfValidOverload(Minus,
						left.TypedLeft(), expr.TypedRight())
					op, v.err = invertComparisonOp(op)
					if v.err != nil {
						return expr
					}
				}

				if newBinExpr == nil {
					break
				}

				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}

				expr.Operator = op
				expr.Left = left.Right
				expr.Right, v.err = newBinExpr.Eval(v.ctx)
				if v.err != nil {
					return nil
				}

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
		if expr.TypedLeft() == DNull {
			return DNull
		}

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
	case NE,
		Like, NotLike,
		ILike, NotILike,
		SimilarTo, NotSimilarTo,
		RegMatch, NotRegMatch,
		RegIMatch, NotRegIMatch:
		if expr.TypedLeft() == DNull || expr.TypedRight() == DNull {
			return DNull
		}
	}

	return expr
}

func (expr *OrExpr) normalize(v *normalizeVisitor) TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	var dleft, dright Datum

	if left == DNull && right == DNull {
		return DNull
	}

	// Use short-circuit evaluation to simplify OR expressions.
	if v.isConst(left) {
		dleft, v.err = left.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if dleft != DNull {
			if d, err := GetBool(dleft); err == nil {
				if d {
					return dleft
				}
				return right
			}
			return DNull
		}
		return NewTypedOrExpr(
			dleft,
			right,
		)
	}
	if v.isConst(right) {
		dright, v.err = right.Eval(v.ctx)
		if v.err != nil {
			return expr
		}
		if dright != DNull {
			if d, err := GetBool(dright); err == nil {
				if d {
					return right
				}
				return left
			}
			return DNull
		}
		return NewTypedOrExpr(
			left,
			dright,
		)
	}
	return expr
}

func (expr *ParenExpr) normalize(v *normalizeVisitor) TypedExpr {
	newExpr := expr.TypedInnerExpr()
	if normalizeable, ok := newExpr.(normalizableExpr); ok {
		newExpr = normalizeable.normalize(v)
		if v.err != nil {
			return expr
		}
	}
	return newExpr
}

func (expr *AnnotateTypeExpr) normalize(v *normalizeVisitor) TypedExpr {
	// Type annotations have no runtime effect, so they can be removed after
	// semantic analysis.
	return expr.TypedInnerExpr()
}

func (expr *RangeCond) normalize(v *normalizeVisitor) TypedExpr {
	left, from, to := expr.TypedLeft(), expr.TypedFrom(), expr.TypedTo()
	if left == DNull || from == DNull || to == DNull {
		return DNull
	}

	if expr.Not {
		// "a NOT BETWEEN b AND c" -> "a < b OR a > c"
		newLeft := NewTypedComparisonExpr(LT, left, from).normalize(v)
		if v.err != nil {
			return expr
		}
		newRight := NewTypedComparisonExpr(GT, left, to).normalize(v)
		if v.err != nil {
			return expr
		}
		return NewTypedOrExpr(newLeft, newRight).normalize(v)
	}

	// "a BETWEEN b AND c" -> "a >= b AND a <= c"
	newLeft := NewTypedComparisonExpr(GE, left, from).normalize(v)
	if v.err != nil {
		return expr
	}
	newRight := NewTypedComparisonExpr(LE, left, to).normalize(v)
	if v.err != nil {
		return expr
	}
	return NewTypedAndExpr(newLeft, newRight).normalize(v)
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
func (ctx *EvalContext) NormalizeExpr(typedExpr TypedExpr) (TypedExpr, error) {
	v := normalizeVisitor{ctx: ctx}
	expr, _ := WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, v.err
	}
	return expr.(TypedExpr), nil
}

type normalizeVisitor struct {
	ctx *EvalContext
	err error

	isConstVisitor isConstVisitor
}

var _ Visitor = &normalizeVisitor{}

func (v *normalizeVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.err != nil {
		return false, expr
	}

	switch expr.(type) {
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
		newExpr, err := expr.(TypedExpr).Eval(v.ctx)
		if err != nil {
			return expr
		}
		expr = newExpr
	}
	return expr
}

func (v *normalizeVisitor) isConst(expr Expr) bool {
	return v.isConstVisitor.run(expr)
}

func invertComparisonOp(op ComparisonOperator) (ComparisonOperator, error) {
	switch op {
	case EQ:
		return EQ, nil
	case GE:
		return LE, nil
	case GT:
		return LT, nil
	case LE:
		return GE, nil
	case LT:
		return GT, nil
	default:
		return op, errors.Errorf("internal error: unable to invert: %s", op)
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

// DecimalZero represents the constant 0 as DECIMAL.
var DecimalZero DDecimal

// DecimalOne represents the constant 1 as DECIMAL.
var DecimalOne DDecimal

func init() {
	DecimalOne.Dec.SetUnscaled(1).SetScale(0)
	DecimalZero.Dec.SetUnscaled(0).SetScale(0)
}

// IsNumericZero returns true if the datum is a number and equal to
// zero.
func IsNumericZero(expr TypedExpr) bool {
	if d, ok := expr.(Datum); ok {
		switch t := d.(type) {
		case *DDecimal:
			return t.Dec.Cmp(&DecimalZero.Dec) == 0
		case *DFloat:
			return *t == 0
		case *DInt:
			return *t == 0
		}
	}
	return false
}

// IsNumericOne returns true if the datum is a number and equal to
// one.
func IsNumericOne(expr TypedExpr) bool {
	if d, ok := expr.(Datum); ok {
		switch t := d.(type) {
		case *DDecimal:
			return t.Dec.Cmp(&DecimalOne.Dec) == 0
		case *DFloat:
			return *t == 1.0
		case *DInt:
			return *t == 1
		}
	}
	return false
}

// ReType ensures that the given numeric expression evaluates
// to the requested type, inserting a cast if necessary.
func ReType(expr TypedExpr, wantedType Datum) (TypedExpr, error) {
	if expr.ReturnType().TypeEqual(wantedType) {
		return expr, nil
	}
	reqType, err := DatumTypeToColumnType(wantedType)
	if err != nil {
		return nil, err
	}
	res := &CastExpr{Expr: expr, Type: reqType}
	res.typ = wantedType
	return res, nil
}
