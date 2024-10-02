// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package normalize

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func normalizeExpr(v *Visitor, n tree.TypedExpr) tree.TypedExpr {
	switch n := n.(type) {
	case *tree.AndExpr:
		return normalizeAndExpr(v, n)
	case *tree.BinaryExpr:
		return normalizeBinaryExpr(v, n)
	case *tree.CoalesceExpr:
		return normalizeCoalesceExpr(v, n)
	case *tree.ComparisonExpr:
		return normalizeComparisonExpr(v, n)
	case *tree.IfExpr:
		return normalizeIfExpr(v, n)
	case *tree.NotExpr:
		return normalizeNotExpr(v, n)
	case *tree.OrExpr:
		return normalizeOrExpr(v, n)
	case *tree.ParenExpr:
		return normalizeParenExpr(v, n)
	case *tree.RangeCond:
		return normalizeRangeCond(v, n)
	case *tree.Tuple:
		return normalizeTuple(v, n)
	case *tree.UnaryExpr:
		return normalizeUnaryExpr(v, n)
	default:
		return n
	}
}

func normalizeAndExpr(v *Visitor, expr *tree.AndExpr) tree.TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	var dleft, dright tree.Datum

	if left == tree.DNull && right == tree.DNull {
		return tree.DNull
	}

	// Use short-circuit evaluation to simplify AND expressions.
	if v.isConst(left) {
		dleft, v.err = eval.Expr(v.ctx, v.evalCtx, left)
		if v.err != nil {
			return expr
		}
		if dleft != tree.DNull {
			if d, err := tree.GetBool(dleft); err == nil {
				if !d {
					return dleft
				}
				return right
			}
			return tree.DNull
		}
		return tree.NewTypedAndExpr(
			dleft,
			right,
		)
	}
	if v.isConst(right) {
		dright, v.err = eval.Expr(v.ctx, v.evalCtx, right)
		if v.err != nil {
			return expr
		}
		if dright != tree.DNull {
			if d, err := tree.GetBool(dright); err == nil {
				if !d {
					return right
				}
				return left
			}
			return tree.DNull
		}
		return tree.NewTypedAndExpr(
			left,
			dright,
		)
	}
	return expr
}

func normalizeBinaryExpr(v *Visitor, expr *tree.BinaryExpr) tree.TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	expectedType := expr.ResolvedType()

	if !expr.Op.CalledOnNullInput && (left == tree.DNull || right == tree.DNull) {
		return tree.DNull
	}

	var final tree.TypedExpr

	switch expr.Operator.Symbol {
	case treebin.Plus:
		if v.isNumericZero(right) {
			final, _ = eval.ReType(left, expectedType)
			break
		}
		if v.isNumericZero(left) {
			final, _ = eval.ReType(right, expectedType)
			break
		}
	case treebin.Minus:
		if types.IsAdditiveType(left.ResolvedType()) && v.isNumericZero(right) {
			final, _ = eval.ReType(left, expectedType)
			break
		}
	case treebin.Mult:
		if v.isNumericOne(right) {
			final, _ = eval.ReType(left, expectedType)
			break
		}
		if v.isNumericOne(left) {
			final, _ = eval.ReType(right, expectedType)
			break
		}
		// We can't simplify multiplication by zero to zero,
		// because if the other operand is NULL during evaluation
		// the result must be NULL.
	case treebin.Div, treebin.FloorDiv:
		if v.isNumericOne(right) {
			final, _ = eval.ReType(left, expectedType)
			break
		}
	}

	// final is nil when the binary expression did not match the cases above,
	// or when ReType was unsuccessful.
	if final == nil {
		return expr
	}
	return final
}

func normalizeCoalesceExpr(v *Visitor, expr *tree.CoalesceExpr) tree.TypedExpr {
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

		val, err := eval.Expr(v.ctx, v.evalCtx, subExpr)
		if err != nil {
			v.err = err
			return expr
		}

		if val != tree.DNull {
			return subExpr
		}
	}
	return expr
}

func normalizeComparisonExpr(v *Visitor, expr *tree.ComparisonExpr) tree.TypedExpr {
	switch expr.Operator.Symbol {
	case treecmp.EQ, treecmp.GE, treecmp.GT, treecmp.LE, treecmp.LT:
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
		if expr.TypedLeft() == tree.DNull || expr.TypedRight() == tree.DNull {
			return tree.DNull
		}

		if v.isConst(expr.Left) {
			switch expr.Right.(type) {
			case *tree.BinaryExpr, tree.VariableExpr:
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

			expr = tree.NewTypedComparisonExpr(invertedOp, expr.TypedRight(), expr.TypedLeft())
		} else if !v.isConst(expr.Right) {
			return expr
		}
	case treecmp.In, treecmp.NotIn:
		// If the right tuple in an In or NotIn comparison expression is constant, it can
		// be normalized.
		tuple, ok := expr.Right.(*tree.DTuple)
		if ok {
			tupleCopy := *tuple
			tupleCopy.Normalize(v.ctx, v.evalCtx)

			// If the tuple only contains NULL values, Normalize will have reduced
			// it to a single NULL value.
			if len(tupleCopy.D) == 1 && tupleCopy.D[0] == tree.DNull {
				return tree.DNull
			}
			if len(tupleCopy.D) == 0 {
				// NULL IN <empty-tuple> is false.
				if expr.Operator.Symbol == treecmp.In {
					return tree.DBoolFalse
				}
				return tree.DBoolTrue
			}
			if expr.TypedLeft() == tree.DNull {
				// NULL IN <non-empty-tuple> is NULL.
				return tree.DNull
			}

			exprCopy := *expr
			expr = &exprCopy
			expr.Right = &tupleCopy
		}
	case treecmp.IsDistinctFrom, treecmp.IsNotDistinctFrom:
		left := expr.TypedLeft()
		right := expr.TypedRight()

		if v.isConst(left) && !v.isConst(right) {
			// Switch operand order so that constant expression is on the right.
			// This helps support index selection rules.
			return tree.NewTypedComparisonExpr(expr.Operator, right, left)
		}
	case treecmp.NE,
		treecmp.Like, treecmp.NotLike,
		treecmp.ILike, treecmp.NotILike,
		treecmp.SimilarTo, treecmp.NotSimilarTo,
		treecmp.RegMatch, treecmp.NotRegMatch,
		treecmp.RegIMatch, treecmp.NotRegIMatch:
		if expr.TypedLeft() == tree.DNull || expr.TypedRight() == tree.DNull {
			return tree.DNull
		}
	case treecmp.Any, treecmp.Some, treecmp.All:
		if expr.TypedRight() == tree.DNull {
			return tree.DNull
		}
	}

	return expr
}

func normalizeIfExpr(v *Visitor, expr *tree.IfExpr) tree.TypedExpr {
	if v.isConst(expr.Cond) {
		cond, err := eval.Expr(v.ctx, v.evalCtx, expr.TypedCondExpr())
		if err != nil {
			v.err = err
			return expr
		}
		if d, err := tree.GetBool(cond); err == nil {
			if d {
				return expr.TypedTrueExpr()
			}
			return expr.TypedElseExpr()
		}
		return tree.DNull
	}
	return expr
}

func normalizeNotExpr(_ *Visitor, expr *tree.NotExpr) tree.TypedExpr {
	inner := expr.TypedInnerExpr()
	switch t := inner.(type) {
	case *tree.NotExpr:
		return t.TypedInnerExpr()
	}
	return expr
}

func normalizeOrExpr(v *Visitor, expr *tree.OrExpr) tree.TypedExpr {
	left := expr.TypedLeft()
	right := expr.TypedRight()
	var dleft, dright tree.Datum

	if left == tree.DNull && right == tree.DNull {
		return tree.DNull
	}

	// Use short-circuit evaluation to simplify OR expressions.
	if v.isConst(left) {
		dleft, v.err = eval.Expr(v.ctx, v.evalCtx, left)
		if v.err != nil {
			return expr
		}
		if dleft != tree.DNull {
			if d, err := tree.GetBool(dleft); err == nil {
				if d {
					return dleft
				}
				return right
			}
			return tree.DNull
		}
		return tree.NewTypedOrExpr(
			dleft,
			right,
		)
	}
	if v.isConst(right) {
		dright, v.err = eval.Expr(v.ctx, v.evalCtx, right)
		if v.err != nil {
			return expr
		}
		if dright != tree.DNull {
			if d, err := tree.GetBool(dright); err == nil {
				if d {
					return right
				}
				return left
			}
			return tree.DNull
		}
		return tree.NewTypedOrExpr(
			left,
			dright,
		)
	}
	return expr
}

func normalizeParenExpr(_ *Visitor, expr *tree.ParenExpr) tree.TypedExpr {
	return expr.TypedInnerExpr()
}

func normalizeRangeCond(v *Visitor, expr *tree.RangeCond) tree.TypedExpr {
	leftFrom, from := expr.TypedLeftFrom(), expr.TypedFrom()
	leftTo, to := expr.TypedLeftTo(), expr.TypedTo()
	// The visitor hasn't walked down into leftTo; do it now.
	if leftTo, v.err = Expr(v.ctx, v.evalCtx, leftTo); v.err != nil {
		return expr
	}

	if (leftFrom == tree.DNull || from == tree.DNull) &&
		(leftTo == tree.DNull || to == tree.DNull) {
		return tree.DNull
	}

	leftCmp := treecmp.GE
	rightCmp := treecmp.LE
	if expr.Not {
		leftCmp = treecmp.LT
		rightCmp = treecmp.GT
	}

	// "a BETWEEN b AND c" -> "a >= b AND a <= c"
	// "a NOT BETWEEN b AND c" -> "a < b OR a > c"
	transform := func(from, to tree.TypedExpr) tree.TypedExpr {
		var newLeft, newRight tree.TypedExpr
		if from == tree.DNull {
			newLeft = tree.DNull
		} else {
			newLeft = normalizeExpr(v, tree.NewTypedComparisonExpr(
				treecmp.MakeComparisonOperator(leftCmp), leftFrom, from),
			)
			if v.err != nil {
				return expr
			}
		}
		if to == tree.DNull {
			newRight = tree.DNull
		} else {
			newRight = normalizeExpr(v, tree.NewTypedComparisonExpr(
				treecmp.MakeComparisonOperator(rightCmp), leftTo, to,
			))
			if v.err != nil {
				return expr
			}
		}
		if expr.Not {
			return normalizeExpr(v, tree.NewTypedOrExpr(newLeft, newRight))
		}
		return normalizeExpr(v, tree.NewTypedAndExpr(newLeft, newRight))
	}

	out := transform(from, to)
	if expr.Symmetric {
		if expr.Not {
			// "a NOT BETWEEN SYMMETRIC b AND c" -> "(a < b OR a > c) AND (a < c OR a > b)"
			out = normalizeExpr(v, tree.NewTypedAndExpr(out, transform(to, from)))
		} else {
			// "a BETWEEN SYMMETRIC b AND c" -> "(a >= b AND a <= c) OR (a >= c OR a <= b)"
			out = normalizeExpr(v, tree.NewTypedOrExpr(out, transform(to, from)))
		}
	}
	return out
}

func normalizeTuple(v *Visitor, expr *tree.Tuple) tree.TypedExpr {
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
	e, err := eval.Expr(v.ctx, v.evalCtx, expr)
	if err != nil {
		v.err = err
	}
	return e
}

func normalizeUnaryExpr(v *Visitor, expr *tree.UnaryExpr) tree.TypedExpr {
	val := expr.TypedInnerExpr()

	if val == tree.DNull {
		return val
	}

	switch expr.Operator.Symbol {
	case tree.UnaryMinus:
		if expr.Operator.IsExplicitOperator {
			return expr
		}
		// -0 -> 0 (except for float which has negative zero)
		if val.ResolvedType().Family() != types.FloatFamily && v.isNumericZero(val) {
			return val
		}
		switch b := val.(type) {
		// -(a - b) -> (b - a)
		case *tree.BinaryExpr:
			if b.Operator.Symbol == treebin.Minus {
				newBinExpr := tree.NewBinExprIfValidOverload(
					treebin.MakeBinaryOperator(treebin.Minus),
					b.TypedRight(),
					b.TypedLeft(),
				)
				if newBinExpr != nil {
					b = newBinExpr
				}
				return b
			}
		// - (- a) -> a
		case *tree.UnaryExpr:
			if b.Operator.Symbol == tree.UnaryMinus {
				return b.TypedInnerExpr()
			}
		}
	}

	return expr
}
