// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
)

// OperatorIsImmutable returns true if the given expression corresponds to a
// constant operator. Note importantly that this will return true for all
// expr types other than FuncExpr, CastExpr, UnaryExpr, BinaryExpr, and
// ComparisonExpr. It does not do any recursive searching.
func OperatorIsImmutable(expr Expr) bool {
	switch t := expr.(type) {
	case *FuncExpr:
		return t.ResolvedOverload().Class == NormalClass && t.fn.Volatility <= volatility.Immutable

	case *CastExpr:
		v, ok := cast.LookupCastVolatility(t.Expr.(TypedExpr).ResolvedType(), t.typ)
		return ok && v <= volatility.Immutable

	case *UnaryExpr:
		return t.op.Volatility <= volatility.Immutable

	case *BinaryExpr:
		return t.Op.Volatility <= volatility.Immutable

	case *ComparisonExpr:
		return t.Op.Volatility <= volatility.Immutable

	default:
		return true
	}
}
