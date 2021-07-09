// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type normalizableExpr interface {
	Expr
	normalize(*NormalizeVisitor) TypedExpr
}

func (expr *CastExpr) normalize(v *NormalizeVisitor) TypedExpr {
	return expr
}

func (expr *CoalesceExpr) normalize(v *NormalizeVisitor) TypedExpr {
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

func (expr *IfExpr) normalize(v *NormalizeVisitor) TypedExpr {
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

func (expr *UnaryExpr) normalize(v *NormalizeVisitor) TypedExpr {
	val := expr.TypedInnerExpr()

	if val == DNull {
		return val
	}

	switch expr.Operator.Symbol {
	case UnaryMinus:
		// -0 -> 0 (except for float which has negative zero)
		if val.ResolvedType().Family() != types.FloatFamily && v.isNumericZero(val) {
			return val
		}
		switch b := val.(type) {
		// -(a - b) -> (b - a)
		case *BinaryExpr:
			if b.Operator.Symbol == Minus {
				newBinExpr := newBinExprIfValidOverload(
					MakeBinaryOperator(Minus),
					b.TypedRight(),
					b.TypedLeft(),
				)
				if newBinExpr != nil {
					newBinExpr.memoizeFn()
					b = newBinExpr
				}
				return b
			}
		// - (- a) -> a
		case *UnaryExpr:
			if b.Operator.Symbol == UnaryMinus {
				return b.TypedInnerExpr()
			}
		}
	}

	return expr
}

func (expr *BinaryExpr) normalize(v *NormalizeVisitor) TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	expectedType := expr.ResolvedType()

	if !expr.Fn.NullableArgs && (left == DNull || right == DNull) {
		return DNull
	}

	var final TypedExpr

	switch expr.Operator.Symbol {
	case Plus:
		if v.isNumericZero(right) {
			final = ReType(left, expectedType)
			break
		}
		if v.isNumericZero(left) {
			final = ReType(right, expectedType)
			break
		}
	case Minus:
		if types.IsAdditiveType(left.ResolvedType()) && v.isNumericZero(right) {
			final = ReType(left, expectedType)
			break
		}
	case Mult:
		if v.isNumericOne(right) {
			final = ReType(left, expectedType)
			break
		}
		if v.isNumericOne(left) {
			final = ReType(right, expectedType)
			break
		}
		// We can't simplify multiplication by zero to zero,
		// because if the other operand is NULL during evaluation
		// the result must be NULL.
	case Div, FloorDiv:
		if v.isNumericOne(right) {
			final = ReType(left, expectedType)
			break
		}
	}

	if final == nil {
		return expr
	}
	return final
}

func (expr *AndExpr) normalize(v *NormalizeVisitor) TypedExpr {
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

func (expr *ComparisonExpr) normalize(v *NormalizeVisitor) TypedExpr {
	switch expr.Operator.Symbol {
	case EQ, GE, GT, LE, LT:
		// We want var nodes (VariableExpr, VarName, etc) to be immediate
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
					v.err = err
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
				(left.Operator.Symbol == Plus || left.Operator.Symbol == Minus || left.Operator.Symbol == Div):

				//        cmp          cmp
				//       /   \        /   \
				//    [+-/]   2  ->  a   [-+*]
				//   /     \            /     \
				//  a       1          2       1
				var op BinaryOperator
				switch left.Operator.Symbol {
				case Plus:
					op = MakeBinaryOperator(Minus)
				case Minus:
					op = MakeBinaryOperator(Plus)
				case Div:
					op = MakeBinaryOperator(Mult)
					if expr.Operator.Symbol != EQ {
						// In this case, we must remember to *flip* the inequality if the
						// divisor is negative, since we are in effect multiplying both sides
						// of the inequality by a negative number.
						divisor, err := left.TypedRight().Eval(v.ctx)
						if err != nil {
							v.err = err
							return expr
						}
						if divisor.Compare(v.ctx, DZero) < 0 {
							if !exprCopied {
								exprCopy := *expr
								expr = &exprCopy
								exprCopied = true
							}

							invertedOp, err := invertComparisonOp(expr.Operator)
							if err != nil {
								v.err = err
								return expr
							}
							expr = NewTypedComparisonExpr(invertedOp, expr.TypedLeft(), expr.TypedRight())
						}
					}
				}

				newBinExpr := newBinExprIfValidOverload(op,
					expr.TypedRight(), left.TypedRight())
				if newBinExpr == nil {
					// Substitution is not possible type-wise. Nothing else to do.
					break
				}

				newRightExpr, err := newBinExpr.Eval(v.ctx)
				if err != nil {
					// In the case of an error during Eval, give up on normalizing this
					// expression. There are some expected errors here if, for example,
					// normalization produces a result that overflows an int64.
					break
				}

				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}

				expr.Left = left.Left
				expr.Right = newRightExpr
				expr.memoizeFn()
				if !isVar(v.ctx, expr.Left, true /*allowConstPlaceholders*/) {
					// Continue as long as the left side of the comparison is not a
					// variable.
					continue
				}

			case v.isConst(left.Left) && (left.Operator.Symbol == Plus || left.Operator.Symbol == Minus):
				//       cmp              cmp
				//      /   \            /   \
				//    [+-]   2  ->     [+-]   a
				//   /    \           /    \
				//  1      a         1      2

				op := expr.Operator
				var newBinExpr *BinaryExpr

				switch left.Operator.Symbol {
				case Plus:
					//
					// (A + X) cmp B => X cmp (B - C)
					//
					newBinExpr = newBinExprIfValidOverload(
						MakeBinaryOperator(Minus),
						expr.TypedRight(),
						left.TypedLeft(),
					)
				case Minus:
					//
					// (A - X) cmp B => X cmp' (A - B)
					//
					newBinExpr = newBinExprIfValidOverload(
						MakeBinaryOperator(Minus),
						left.TypedLeft(),
						expr.TypedRight(),
					)
					op, v.err = invertComparisonOp(op)
					if v.err != nil {
						return expr
					}
				}

				if newBinExpr == nil {
					break
				}

				newRightExpr, err := newBinExpr.Eval(v.ctx)
				if err != nil {
					break
				}

				if !exprCopied {
					exprCopy := *expr
					expr = &exprCopy
					exprCopied = true
				}

				expr.Operator = op
				expr.Left = left.Right
				expr.Right = newRightExpr
				expr.memoizeFn()
				if !isVar(v.ctx, expr.Left, true /*allowConstPlaceholders*/) {
					// Continue as long as the left side of the comparison is not a
					// variable.
					continue
				}

			case expr.Operator.Symbol == EQ && left.Operator.Symbol == JSONFetchVal && v.isConst(left.Right) &&
				v.isConst(expr.Right):
				// This is a JSONB inverted index normalization, changing things of the form
				// x->y=z to x @> {y:z} which can be used to build spans for inverted index
				// lookups.

				if left.TypedRight().ResolvedType().Family() != types.StringFamily {
					break
				}

				str, err := left.TypedRight().Eval(v.ctx)
				if err != nil {
					break
				}
				// Check that we still have a string after evaluation.
				if _, ok := str.(*DString); !ok {
					break
				}

				rhs, err := expr.TypedRight().Eval(v.ctx)
				if err != nil {
					break
				}

				rjson := rhs.(*DJSON).JSON
				t := rjson.Type()
				if t == json.ObjectJSONType || t == json.ArrayJSONType {
					// We can't make this transformation in cases like
					//
					//   a->'b' = '["c"]',
					//
					// because containment is not equivalent to equality for non-scalar types.
					break
				}

				j := json.NewObjectBuilder(1)
				j.Add(string(*str.(*DString)), rjson)

				dj, err := MakeDJSON(j.Build())
				if err != nil {
					break
				}

				typedJ, err := dj.TypeCheck(v.ctx.Context, nil, types.Jsonb)
				if err != nil {
					break
				}

				return NewTypedComparisonExpr(MakeComparisonOperator(Contains), left.TypedLeft(), typedJ)
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
			tupleCopy.Normalize(v.ctx)

			// If the tuple only contains NULL values, Normalize will have reduced
			// it to a single NULL value.
			if len(tupleCopy.D) == 1 && tupleCopy.D[0] == DNull {
				return DNull
			}
			if len(tupleCopy.D) == 0 {
				// NULL IN <empty-tuple> is false.
				if expr.Operator.Symbol == In {
					return DBoolFalse
				}
				return DBoolTrue
			}
			if expr.TypedLeft() == DNull {
				// NULL IN <non-empty-tuple> is NULL.
				return DNull
			}

			exprCopy := *expr
			expr = &exprCopy
			expr.Right = &tupleCopy
		}
	case IsDistinctFrom, IsNotDistinctFrom:
		left := expr.TypedLeft()
		right := expr.TypedRight()

		if v.isConst(left) && !v.isConst(right) {
			// Switch operand order so that constant expression is on the right.
			// This helps support index selection rules.
			return NewTypedComparisonExpr(expr.Operator, right, left)
		}
	case NE,
		Like, NotLike,
		ILike, NotILike,
		SimilarTo, NotSimilarTo,
		RegMatch, NotRegMatch,
		RegIMatch, NotRegIMatch,
		Any, Some, All:
		if expr.TypedLeft() == DNull || expr.TypedRight() == DNull {
			return DNull
		}
	}

	return expr
}

func (expr *OrExpr) normalize(v *NormalizeVisitor) TypedExpr {
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

func (expr *NotExpr) normalize(v *NormalizeVisitor) TypedExpr {
	inner := expr.TypedInnerExpr()
	switch t := inner.(type) {
	case *NotExpr:
		return t.TypedInnerExpr()
	}
	return expr
}

func (expr *ParenExpr) normalize(v *NormalizeVisitor) TypedExpr {
	return expr.TypedInnerExpr()
}

func (expr *AnnotateTypeExpr) normalize(v *NormalizeVisitor) TypedExpr {
	// Type annotations have no runtime effect, so they can be removed after
	// semantic analysis.
	return expr.TypedInnerExpr()
}

func (expr *RangeCond) normalize(v *NormalizeVisitor) TypedExpr {
	leftFrom, from := expr.TypedLeftFrom(), expr.TypedFrom()
	leftTo, to := expr.TypedLeftTo(), expr.TypedTo()
	// The visitor hasn't walked down into leftTo; do it now.
	if leftTo, v.err = v.ctx.NormalizeExpr(leftTo); v.err != nil {
		return expr
	}

	if (leftFrom == DNull || from == DNull) && (leftTo == DNull || to == DNull) {
		return DNull
	}

	leftCmp := GE
	rightCmp := LE
	if expr.Not {
		leftCmp = LT
		rightCmp = GT
	}

	// "a BETWEEN b AND c" -> "a >= b AND a <= c"
	// "a NOT BETWEEN b AND c" -> "a < b OR a > c"
	transform := func(from, to TypedExpr) TypedExpr {
		var newLeft, newRight TypedExpr
		if from == DNull {
			newLeft = DNull
		} else {
			newLeft = NewTypedComparisonExpr(MakeComparisonOperator(leftCmp), leftFrom, from).normalize(v)
			if v.err != nil {
				return expr
			}
		}
		if to == DNull {
			newRight = DNull
		} else {
			newRight = NewTypedComparisonExpr(MakeComparisonOperator(rightCmp), leftTo, to).normalize(v)
			if v.err != nil {
				return expr
			}
		}
		if expr.Not {
			return NewTypedOrExpr(newLeft, newRight).normalize(v)
		}
		return NewTypedAndExpr(newLeft, newRight).normalize(v)
	}

	out := transform(from, to)
	if expr.Symmetric {
		if expr.Not {
			// "a NOT BETWEEN SYMMETRIC b AND c" -> "(a < b OR a > c) AND (a < c OR a > b)"
			out = NewTypedAndExpr(out, transform(to, from)).normalize(v)
		} else {
			// "a BETWEEN SYMMETRIC b AND c" -> "(a >= b AND a <= c) OR (a >= c OR a <= b)"
			out = NewTypedOrExpr(out, transform(to, from)).normalize(v)
		}
	}
	return out
}

func (expr *Tuple) normalize(v *NormalizeVisitor) TypedExpr {
	// A Tuple should be directly evaluated into a DTuple if it's either fully
	// constant or contains only constants and top-level Placeholders.
	isConst := true
	for _, subExpr := range expr.Exprs {
		if !v.isConst(subExpr) {
			isConst = false
			break
		}
	}
	if !isConst {
		return expr
	}
	e, err := expr.Eval(v.ctx)
	if err != nil {
		v.err = err
	}
	return e
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
	v := MakeNormalizeVisitor(ctx)
	expr, _ := WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, v.err
	}
	return expr.(TypedExpr), nil
}

// NormalizeVisitor supports the execution of NormalizeExpr.
type NormalizeVisitor struct {
	ctx *EvalContext
	err error

	fastIsConstVisitor fastIsConstVisitor
}

var _ Visitor = &NormalizeVisitor{}

// MakeNormalizeVisitor creates a NormalizeVisitor instance.
func MakeNormalizeVisitor(ctx *EvalContext) NormalizeVisitor {
	return NormalizeVisitor{ctx: ctx, fastIsConstVisitor: fastIsConstVisitor{ctx: ctx}}
}

// Err retrieves the error field in the NormalizeVisitor.
func (v *NormalizeVisitor) Err() error { return v.err }

// VisitPre implements the Visitor interface.
func (v *NormalizeVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.err != nil {
		return false, expr
	}

	switch expr.(type) {
	case *Subquery:
		// Subqueries are pre-normalized during semantic analysis. There
		// is nothing to do here.
		return false, expr
	}

	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *NormalizeVisitor) VisitPost(expr Expr) Expr {
	if v.err != nil {
		return expr
	}
	// We don't propagate errors during this step because errors might involve a
	// branch of code that isn't traversed by normal execution (for example,
	// IF(2 = 2, 1, 1 / 0)).

	// Normalize expressions that know how to normalize themselves.
	if normalizable, ok := expr.(normalizableExpr); ok {
		expr = normalizable.normalize(v)
		if v.err != nil {
			return expr
		}
	}

	// Evaluate all constant expressions.
	if v.isConst(expr) {
		value, err := expr.(TypedExpr).Eval(v.ctx)
		if err != nil {
			// Ignore any errors here (e.g. division by zero), so they can happen
			// during execution where they are correctly handled. Note that in some
			// cases we might not even get an error (if this particular expression
			// does not get evaluated when the query runs, e.g. it's inside a CASE).
			return expr
		}
		if value == DNull {
			// We don't want to return an expression that has a different type; cast
			// the NULL if necessary.
			return ReType(DNull, expr.(TypedExpr).ResolvedType())
		}
		return value
	}

	return expr
}

func (v *NormalizeVisitor) isConst(expr Expr) bool {
	return v.fastIsConstVisitor.run(expr)
}

// isNumericZero returns true if the datum is a number and equal to
// zero.
func (v *NormalizeVisitor) isNumericZero(expr TypedExpr) bool {
	if d, ok := expr.(Datum); ok {
		switch t := UnwrapDatum(v.ctx, d).(type) {
		case *DDecimal:
			return t.Decimal.Sign() == 0
		case *DFloat:
			return *t == 0
		case *DInt:
			return *t == 0
		}
	}
	return false
}

// isNumericOne returns true if the datum is a number and equal to
// one.
func (v *NormalizeVisitor) isNumericOne(expr TypedExpr) bool {
	if d, ok := expr.(Datum); ok {
		switch t := UnwrapDatum(v.ctx, d).(type) {
		case *DDecimal:
			return t.Decimal.Cmp(&DecimalOne.Decimal) == 0
		case *DFloat:
			return *t == 1.0
		case *DInt:
			return *t == 1
		}
	}
	return false
}

func invertComparisonOp(op ComparisonOperator) (ComparisonOperator, error) {
	switch op.Symbol {
	case EQ:
		return MakeComparisonOperator(EQ), nil
	case GE:
		return MakeComparisonOperator(LE), nil
	case GT:
		return MakeComparisonOperator(LT), nil
	case LE:
		return MakeComparisonOperator(GE), nil
	case LT:
		return MakeComparisonOperator(GT), nil
	default:
		return op, errors.AssertionFailedf("unable to invert: %s", op)
	}
}

type isConstVisitor struct {
	ctx     *EvalContext
	isConst bool
}

var _ Visitor = &isConstVisitor{}

func (v *isConstVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.isConst {
		if !operatorIsImmutable(expr) || isVar(v.ctx, expr, true /*allowConstPlaceholders*/) {
			v.isConst = false
			return false, expr
		}
	}
	return true, expr
}

func operatorIsImmutable(expr Expr) bool {
	switch t := expr.(type) {
	case *FuncExpr:
		return t.fnProps.Class == NormalClass && t.fn.Volatility <= VolatilityImmutable

	case *CastExpr:
		volatility, ok := LookupCastVolatility(t.Expr.(TypedExpr).ResolvedType(), t.typ)
		return ok && volatility <= VolatilityImmutable

	case *UnaryExpr:
		return t.fn.Volatility <= VolatilityImmutable

	case *BinaryExpr:
		return t.Fn.Volatility <= VolatilityImmutable

	case *ComparisonExpr:
		return t.Fn.Volatility <= VolatilityImmutable

	default:
		return true
	}
}

func (*isConstVisitor) VisitPost(expr Expr) Expr { return expr }

func (v *isConstVisitor) run(expr Expr) bool {
	v.isConst = true
	WalkExprConst(v, expr)
	return v.isConst
}

// IsConst returns whether the expression is constant. A constant expression
// does not contain variables, as defined by ContainsVars, nor impure functions.
func IsConst(evalCtx *EvalContext, expr TypedExpr) bool {
	v := isConstVisitor{ctx: evalCtx}
	return v.run(expr)
}

// fastIsConstVisitor is similar to isConstVisitor, but it only visits
// at most two levels of the tree (with one exception, see below).
// In essence, it determines whether an expression is constant by checking
// whether its children are const Datums.
//
// This can be used during normalization since constants are evaluated
// bottom-up. If a child is *not* a const Datum, that means it was already
// determined to be non-constant, and therefore was not evaluated.
type fastIsConstVisitor struct {
	ctx     *EvalContext
	isConst bool

	// visited indicates whether we have already visited one level of the tree.
	// fastIsConstVisitor only visits at most two levels of the tree, with one
	// exception: If the second level has a Cast expression, fastIsConstVisitor
	// may visit three levels.
	visited bool
}

var _ Visitor = &fastIsConstVisitor{}

func (v *fastIsConstVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.visited {
		if _, ok := expr.(*CastExpr); ok {
			// We recurse one more time for cast expressions, since the
			// NormalizeVisitor may have wrapped a NULL.
			return true, expr
		}
		if _, ok := expr.(Datum); !ok || isVar(v.ctx, expr, true /*allowConstPlaceholders*/) {
			// If the child expression is not a const Datum, the parent expression is
			// not constant. Note that all constant literals have already been
			// normalized to Datum in TypeCheck.
			v.isConst = false
		}
		return false, expr
	}
	v.visited = true

	// If the parent expression is a variable or non-immutable operator, we know
	// that it is not constant.

	if !operatorIsImmutable(expr) || isVar(v.ctx, expr, true /*allowConstPlaceholders*/) {
		v.isConst = false
		return false, expr
	}

	return true, expr
}

func (*fastIsConstVisitor) VisitPost(expr Expr) Expr { return expr }

func (v *fastIsConstVisitor) run(expr Expr) bool {
	v.isConst = true
	v.visited = false
	WalkExprConst(v, expr)
	return v.isConst
}

// isVar returns true if the expression's value can vary during plan
// execution. The parameter allowConstPlaceholders should be true
// in the common case of scalar expressions that will be evaluated
// in the context of the execution of a prepared query, where the
// placeholder will have the same value for every row processed.
// It is set to false for scalar expressions that are not
// evaluated as part of query execution, eg. DEFAULT expressions.
func isVar(evalCtx *EvalContext, expr Expr, allowConstPlaceholders bool) bool {
	switch expr.(type) {
	case VariableExpr:
		return true
	case *Placeholder:
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

type containsVarsVisitor struct {
	containsVars bool
}

var _ Visitor = &containsVarsVisitor{}

func (v *containsVarsVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if !v.containsVars && isVar(nil, expr, false /*allowConstPlaceholders*/) {
		v.containsVars = true
	}
	if v.containsVars {
		return false, expr
	}
	return true, expr
}

func (*containsVarsVisitor) VisitPost(expr Expr) Expr { return expr }

// ContainsVars returns true if the expression contains any variables.
// (variables = sub-expressions, placeholders, indexed vars, etc.)
func ContainsVars(expr Expr) bool {
	v := containsVarsVisitor{containsVars: false}
	WalkExprConst(&v, expr)
	return v.containsVars
}

// DecimalOne represents the constant 1 as DECIMAL.
var DecimalOne DDecimal

func init() {
	DecimalOne.SetInt64(1)
}

// ReType ensures that the given expression evaluates
// to the requested type, inserting a cast if necessary.
func ReType(expr TypedExpr, wantedType *types.T) TypedExpr {
	resolvedType := expr.ResolvedType()
	if wantedType.Family() == types.AnyFamily || resolvedType.Identical(wantedType) {
		return expr
	}
	res := &CastExpr{Expr: expr, Type: wantedType}
	res.typ = wantedType
	return res
}
