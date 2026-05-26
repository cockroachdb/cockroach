// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"

// FoldComparisonExpr folds a given comparison operation and its expressions
// into an equivalent operation that will hit in the CmpOps map, returning
// this new operation, along with potentially flipped operands and "flipped"
// and "not" flags.
func FoldComparisonExpr(
	op treecmp.ComparisonOperator, left, right Expr,
) (newOp treecmp.ComparisonOperator, newLeft Expr, newRight Expr, flipped bool, not bool) {
	switch op.Symbol {
	case treecmp.NE:
		// NE(left, right) is implemented as !EQ(left, right).
		return treecmp.MakeComparisonOperator(treecmp.EQ), left, right, false, true
	case treecmp.GT:
		// GT(left, right) is implemented as LT(right, left)
		return treecmp.MakeComparisonOperator(treecmp.LT), right, left, true, false
	case treecmp.GE:
		// GE(left, right) is implemented as LE(right, left)
		return treecmp.MakeComparisonOperator(treecmp.LE), right, left, true, false
	case treecmp.NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		return treecmp.MakeComparisonOperator(treecmp.In), left, right, false, true
	case treecmp.NotLike:
		// NotLike(left, right) is implemented as !Like(left, right)
		return treecmp.MakeComparisonOperator(treecmp.Like), left, right, false, true
	case treecmp.NotILike:
		// NotILike(left, right) is implemented as !ILike(left, right)
		return treecmp.MakeComparisonOperator(treecmp.ILike), left, right, false, true
	case treecmp.NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return treecmp.MakeComparisonOperator(treecmp.SimilarTo), left, right, false, true
	case treecmp.NotRegMatch:
		// NotRegMatch(left, right) is implemented as !RegMatch(left, right)
		return treecmp.MakeComparisonOperator(treecmp.RegMatch), left, right, false, true
	case treecmp.NotRegIMatch:
		// NotRegIMatch(left, right) is implemented as !RegIMatch(left, right)
		return treecmp.MakeComparisonOperator(treecmp.RegIMatch), left, right, false, true
	case treecmp.IsDistinctFrom:
		// IsDistinctFrom(left, right) is implemented as !IsNotDistinctFrom(left, right)
		// Note: this seems backwards, but IS NOT DISTINCT FROM is an extended
		// version of IS and IS DISTINCT FROM is an extended version of IS NOT.
		return treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), left, right, false, true
	}
	return op, left, right, false, false
}

// FoldComparisonExprWithDatums is the same as FoldComparisonExpr, but receives
// and returns Datums instead of Exprs. This allows callers passing Datums and
// expecting Datums to be returned to avoid the expensive interface conversions.
func FoldComparisonExprWithDatums(
	op treecmp.ComparisonOperator, left, right Datum,
) (newOp treecmp.ComparisonOperator, newLeft Datum, newRight Datum, flipped bool, not bool) {
	switch op.Symbol {
	case treecmp.NE:
		// NE(left, right) is implemented as !EQ(left, right).
		return treecmp.MakeComparisonOperator(treecmp.EQ), left, right, false, true
	case treecmp.GT:
		// GT(left, right) is implemented as LT(right, left)
		return treecmp.MakeComparisonOperator(treecmp.LT), right, left, true, false
	case treecmp.GE:
		// GE(left, right) is implemented as LE(right, left)
		return treecmp.MakeComparisonOperator(treecmp.LE), right, left, true, false
	case treecmp.NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		return treecmp.MakeComparisonOperator(treecmp.In), left, right, false, true
	case treecmp.NotLike:
		// NotLike(left, right) is implemented as !Like(left, right)
		return treecmp.MakeComparisonOperator(treecmp.Like), left, right, false, true
	case treecmp.NotILike:
		// NotILike(left, right) is implemented as !ILike(left, right)
		return treecmp.MakeComparisonOperator(treecmp.ILike), left, right, false, true
	case treecmp.NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return treecmp.MakeComparisonOperator(treecmp.SimilarTo), left, right, false, true
	case treecmp.NotRegMatch:
		// NotRegMatch(left, right) is implemented as !RegMatch(left, right)
		return treecmp.MakeComparisonOperator(treecmp.RegMatch), left, right, false, true
	case treecmp.NotRegIMatch:
		// NotRegIMatch(left, right) is implemented as !RegIMatch(left, right)
		return treecmp.MakeComparisonOperator(treecmp.RegIMatch), left, right, false, true
	case treecmp.IsDistinctFrom:
		// IsDistinctFrom(left, right) is implemented as !IsNotDistinctFrom(left, right)
		// Note: this seems backwards, but IS NOT DISTINCT FROM is an extended
		// version of IS and IS DISTINCT FROM is an extended version of IS NOT.
		return treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), left, right, false, true
	}
	return op, left, right, false, false
}
