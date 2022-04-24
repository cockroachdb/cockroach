// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/internal/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// OperatorIsImmutable returns true if the given expression corresponds to a
// constant operator. Note importantly that this will return true for all
// expr types other than FuncExpr, CastExpr, UnaryExpr, BinaryExpr, and
// ComparisonExpr. It does not do any recursive searching.
func OperatorIsImmutable(expr Expr, sd *sessiondata.SessionData) bool {
	switch t := expr.(type) {
	case *FuncExpr:
		return t.fnProps.Class == NormalClass && t.fn.Volatility <= volatility.Immutable

	case *CastExpr:
		v, ok := LookupCastVolatility(t.Expr.(TypedExpr).ResolvedType(), t.typ, sd)
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

// LookupCastVolatility returns the Volatility of a valid cast.
func LookupCastVolatility(
	from, to *types.T, sd *sessiondata.SessionData,
) (_ volatility.Volatility, ok bool) {
	fromFamily := from.Family()
	toFamily := to.Family()
	// Special case for casting between arrays.
	if fromFamily == types.ArrayFamily && toFamily == types.ArrayFamily {
		return LookupCastVolatility(from.ArrayContents(), to.ArrayContents(), sd)
	}
	// Special case for casting between tuples.
	if fromFamily == types.TupleFamily && toFamily == types.TupleFamily {
		fromTypes := from.TupleContents()
		toTypes := to.TupleContents()
		// Handle case where an overload makes a tuple get casted to tuple{}.
		if len(toTypes) == 1 && toTypes[0].Family() == types.AnyFamily {
			return volatility.Stable, true
		}
		if len(fromTypes) != len(toTypes) {
			return 0, false
		}
		maxVolatility := volatility.LeakProof
		for i := range fromTypes {
			v, lookupOk := LookupCastVolatility(fromTypes[i], toTypes[i], sd)
			if !lookupOk {
				return 0, false
			}
			if v > maxVolatility {
				maxVolatility = v
			}
		}
		return maxVolatility, true
	}

	intervalStyleEnabled := false
	dateStyleEnabled := false
	if sd != nil {
		intervalStyleEnabled = sd.IntervalStyleEnabled
		dateStyleEnabled = sd.DateStyleEnabled
	}

	cast, ok := cast.LookupCast(from, to, intervalStyleEnabled, dateStyleEnabled)
	if !ok {
		return 0, false
	}
	return cast.Volatility, true
}
