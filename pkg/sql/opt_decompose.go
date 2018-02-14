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

package sql

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// splitOrExpr flattens a tree of OR expressions returning all of the child
// expressions as a list. Any non-OR expression is returned as a single element
// in the list.
//
//   a OR b OR c OR d -> [a, b, c, d]
func splitOrExpr(
	evalCtx *tree.EvalContext, e tree.TypedExpr, exprs tree.TypedExprs,
) tree.TypedExprs {
	switch t := e.(type) {
	case *tree.OrExpr:
		return splitOrExpr(evalCtx, t.TypedRight(), splitOrExpr(evalCtx, t.TypedLeft(), exprs))
	}
	return append(exprs, e)
}

// splitAndExpr flattens a tree of AND expressions, appending all of the child
// expressions as a list. Any non-AND expression is appended as a single element
// in the list.
//
//   a AND b AND c AND d -> [a, b, c, d]
func splitAndExpr(
	evalCtx *tree.EvalContext, e tree.TypedExpr, exprs tree.TypedExprs,
) tree.TypedExprs {
	switch t := e.(type) {
	case *tree.AndExpr:
		return splitAndExpr(evalCtx, t.TypedRight(), splitAndExpr(evalCtx, t.TypedLeft(), exprs))
	}
	return append(exprs, e)
}

// joinOrExprs performs the inverse operation of splitOrExpr, joining
// together the individual expressions using OrExpr nodes.
func joinOrExprs(exprs tree.TypedExprs) tree.TypedExpr {
	return joinExprs(exprs, func(left, right tree.TypedExpr) tree.TypedExpr {
		return tree.NewTypedOrExpr(left, right)
	})
}

// joinAndExprs performs the inverse operation of splitAndExpr, joining
// together the individual expressions using AndExpr nodes.
func joinAndExprs(exprs tree.TypedExprs) tree.TypedExpr {
	return joinExprs(exprs, func(left, right tree.TypedExpr) tree.TypedExpr {
		return tree.NewTypedAndExpr(left, right)
	})
}

func joinExprs(
	exprs tree.TypedExprs, joinExprsFn func(left, right tree.TypedExpr) tree.TypedExpr,
) tree.TypedExpr {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		a := joinExprsFn(exprs[len(exprs)-2], exprs[len(exprs)-1])
		for i := len(exprs) - 3; i >= 0; i-- {
			a = joinExprsFn(exprs[i], a)
		}
		return a
	}
}

// SimplifyExpr transforms an expression such that it contains only expressions
// involving IndexedVars that can be used for index selection. If an expression is
// encountered that cannot be used for index selection (e.g. "func(val)") that
// part of the expression tree is considered to evaluate to true, possibly
// rendering the entire expression as true. Additionally, various
// normalizations are performed on comparison expressions. For example:
//
//   (a < 1 AND a < 2)  -> (a < 1)
//   (a < 1 AND a > 2)  -> false
//   (a > 1 OR a < 2)   -> true
//   (a > 1 OR func(b)) -> true
//   (b)                -> (b = true)
//   (b != true)        -> (b < true)
//
// Note that simplification is not normalization. Normalization as performed by
// parser.NormalizeExpr returns an expression that is equivalent to the
// original. Simplification can return an expression with parts of the
// expression tree stripped out.
//
// Returns false for equivalent if the resulting expression is not equivalent
// to the original. This occurs for expressions which are currently not handled
// by simplification.
func SimplifyExpr(
	evalCtx *tree.EvalContext, e tree.TypedExpr,
) (simplified tree.TypedExpr, equivalent bool) {
	if e == tree.DNull {
		return e, true
	}
	switch t := e.(type) {
	case *tree.NotExpr:
		return simplifyNotExpr(evalCtx, t)
	case *tree.AndExpr:
		return simplifyAndExpr(evalCtx, t)
	case *tree.OrExpr:
		return simplifyOrExpr(evalCtx, t)
	case *tree.ComparisonExpr:
		return simplifyComparisonExpr(evalCtx, t)
	case *tree.IndexedVar:
		return simplifyBoolVar(evalCtx, t)
	case *tree.DBool:
		return t, true
	}
	// We don't know how to simplify expressions that fall through to here, so
	// consider this part of the expression true.
	return tree.DBoolTrue, false
}

func simplifyNotExpr(evalCtx *tree.EvalContext, n *tree.NotExpr) (tree.TypedExpr, bool) {
	if n.Expr == tree.DNull {
		return tree.DNull, true
	}
	switch t := n.Expr.(type) {
	case *tree.NotExpr:
		return SimplifyExpr(evalCtx, t.TypedInnerExpr())
	case *tree.ComparisonExpr:
		op := t.Operator
		switch op {
		case tree.EQ:
			op = tree.NE
		case tree.NE:
			op = tree.EQ
		case tree.GT:
			op = tree.LE
		case tree.GE:
			op = tree.LT
		case tree.LT:
			op = tree.GE
		case tree.LE:
			op = tree.GT
		case tree.In:
			op = tree.NotIn
		case tree.NotIn:
			op = tree.In
		case tree.Like:
			op = tree.NotLike
		case tree.NotLike:
			op = tree.Like
		case tree.ILike:
			op = tree.NotILike
		case tree.NotILike:
			op = tree.ILike
		case tree.SimilarTo:
			op = tree.NotSimilarTo
		case tree.NotSimilarTo:
			op = tree.SimilarTo
		case tree.RegMatch:
			op = tree.NotRegMatch
		case tree.NotRegMatch:
			op = tree.RegMatch
		case tree.RegIMatch:
			op = tree.NotRegIMatch
		case tree.NotRegIMatch:
			op = tree.RegIMatch
		case tree.IsDistinctFrom:
			op = tree.IsNotDistinctFrom
		case tree.IsNotDistinctFrom:
			op = tree.IsDistinctFrom
		default:
			return tree.DBoolTrue, false
		}
		return SimplifyExpr(evalCtx, tree.NewTypedComparisonExpr(
			op,
			t.TypedLeft(),
			t.TypedRight(),
		))

	case *tree.AndExpr:
		// De Morgan's Law: NOT (a AND b) -> (NOT a) OR (NOT b)
		return SimplifyExpr(evalCtx, tree.NewTypedOrExpr(
			tree.NewTypedNotExpr(t.TypedLeft()),
			tree.NewTypedNotExpr(t.TypedRight()),
		))

	case *tree.OrExpr:
		// De Morgan's Law: NOT (a OR b) -> (NOT a) AND (NOT b)
		return SimplifyExpr(evalCtx, tree.NewTypedAndExpr(
			tree.NewTypedNotExpr(t.TypedLeft()),
			tree.NewTypedNotExpr(t.TypedRight()),
		))
	case *tree.IndexedVar:
		return SimplifyExpr(evalCtx, tree.NewTypedNotExpr(
			boolVarToComparison(t),
		))
	}
	return tree.DBoolTrue, false
}

func isKnownTrue(e tree.TypedExpr) bool {
	if e == tree.DNull {
		return false
	}
	if b, ok := e.(*tree.DBool); ok {
		return bool(*b)
	}
	return false
}

func isKnownFalseOrNull(e tree.TypedExpr) bool {
	if e == tree.DNull {
		return true
	}
	if b, ok := e.(*tree.DBool); ok {
		return !bool(*b)
	}
	return false
}

func simplifyAndExpr(evalCtx *tree.EvalContext, n *tree.AndExpr) (tree.TypedExpr, bool) {
	// a AND b AND c AND d -> [a, b, c, d]
	equivalent := true
	exprs := splitAndExpr(evalCtx, n, nil)
	for i := range exprs {
		var equiv bool
		exprs[i], equiv = SimplifyExpr(evalCtx, exprs[i])
		if !equiv {
			equivalent = false
		}
		if isKnownFalseOrNull(exprs[i]) {
			return tree.DBoolFalse, equivalent
		}
	}
	// Simplifying exprs might have transformed one of the elements into an AND
	// expression.
	texprs, exprs := exprs, nil
	for _, e := range texprs {
		exprs = splitAndExpr(evalCtx, e, exprs)
	}

	// Loop over the expressions looking for simplifications.
	//
	// TODO(pmattis): This is O(n^2) in the number of expressions. Could be
	// optimized by sorting the expressions based on the variables they contain.
outer:
	for i := len(exprs) - 1; i >= 0; i-- {
		for j := i - 1; j >= 0; j-- {
			var equiv bool
			exprs[j], exprs[i], equiv = simplifyOneAndExpr(evalCtx, exprs[j], exprs[i])
			if !equiv {
				equivalent = false
			}
			if isKnownFalseOrNull(exprs[j]) {
				return exprs[j], equivalent
			}
			if isKnownTrue(exprs[i]) {
				exprs[i] = nil
			}
			if exprs[i] == nil {
				// We found a simplification. Strip off the expression that is now nil
				// and continue the outer loop.
				n := len(exprs) - 1
				exprs[i] = exprs[n]
				exprs = exprs[:n]
				continue outer
			}
		}
	}

	// Reform the AND expressions.
	return joinAndExprs(exprs), equivalent
}

func simplifyOneAndExpr(
	evalCtx *tree.EvalContext, left, right tree.TypedExpr,
) (tree.TypedExpr, tree.TypedExpr, bool) {
	lcmp, ok := left.(*tree.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	rcmp, ok := right.(*tree.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	lcmpLeft, lcmpRight := lcmp.TypedLeft(), lcmp.TypedRight()
	rcmpLeft, rcmpRight := rcmp.TypedLeft(), rcmp.TypedRight()
	if !isDatum(lcmpRight) || !isDatum(rcmpRight) {
		return tree.DBoolTrue, nil, false
	}
	if !varEqual(lcmpLeft, rcmpLeft) {
		return left, right, true
	}

	if lcmp.Operator == tree.IsDistinctFrom || rcmp.Operator == tree.IsDistinctFrom {
		switch lcmp.Operator {
		case tree.EQ, tree.GT, tree.GE, tree.LT, tree.LE, tree.In:
			if rcmpRight == tree.DNull {
				// a <cmp> x AND a IS DISTINCT FROM NULL
				return left, nil, true
			}
		case tree.IsNotDistinctFrom:
			if lcmpRight == tree.DNull && rcmpRight == tree.DNull {
				// a IS NOT DISTINCT FROM NULL AND a IS DISTINCT FROM NULL
				return tree.DBoolFalse, nil, true
			}
		case tree.IsDistinctFrom:
			if lcmpRight == tree.DNull {
				switch rcmp.Operator {
				case tree.EQ, tree.GT, tree.GE, tree.LT, tree.LE, tree.In:
					// a IS DISTINCT FROM NULL AND a <cmp> x
					return right, nil, true
				case tree.IsNotDistinctFrom:
					if rcmpRight == tree.DNull {
						// a IS DISTINCT FROM NULL AND a IS NOT DISTINCT FROM NULL
						return tree.DBoolFalse, nil, true
					}
				case tree.IsDistinctFrom:
					if rcmpRight == tree.DNull {
						// a IS DISTINCT FROM NULL AND a IS DISTINCT FROM NULL
						return left, nil, true
					}
				}
			}
		}
		return left, right, true
	}

	if lcmp.Operator == tree.In || rcmp.Operator == tree.In {
		left, right = simplifyOneAndInExpr(evalCtx, lcmp, rcmp)
		return left, right, true
	}

	if reflect.TypeOf(lcmpRight) != reflect.TypeOf(rcmpRight) {
		allowCmp := false
		switch lcmp.Operator {
		case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
			switch rcmp.Operator {
			case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
				// Break, permitting heterogeneous comparison.
				allowCmp = true
			}
		}
		if !allowCmp {
			if lcmp.Operator == tree.IsNotDistinctFrom && lcmpRight == tree.DNull {
				// a IS NOT DISTINCT FROM NULL AND a <cmp> x
				return tree.DBoolFalse, nil, true
			}
			if rcmp.Operator == tree.IsNotDistinctFrom && rcmpRight == tree.DNull {
				// a <cmp> x AND a IS NOT DISTINCT FROM NULL
				return tree.DBoolFalse, nil, true
			}
			// Note that "a IS NOT DISTINCT FROM NULL and a IS NOT DISTINCT FROM NULL" cannot happen here because
			// "reflect.TypeOf(NULL) == reflect.TypeOf(NULL)".
			return left, right, true
		}
	}

	ldatum := lcmpRight.(tree.Datum)
	rdatum := rcmpRight.(tree.Datum)
	cmp := ldatum.Compare(evalCtx, rdatum)

	// Determine which expression to use when either expression (left or right)
	// is valid as a return value but their types are different. The reason
	// to prefer a comparison between a column value and a datum of the same
	// type is that it makes index constraint construction easier.
	either := lcmp
	if !ldatum.ResolvedType().Equivalent(rdatum.ResolvedType()) {
		switch ta := lcmpLeft.(type) {
		case *tree.IndexedVar:
			if ta.ResolvedType().Equivalent(rdatum.ResolvedType()) {
				either = rcmp
			}
		}
	}

	// TODO(pmattis): Figure out how to generate this logic.
	switch lcmp.Operator {
	case tree.EQ:
		switch rcmp.Operator {
		case tree.EQ:
			// a = x AND a = y
			if cmp == 0 {
				// x = y
				return either, nil, true
			}
			return tree.DBoolFalse, nil, true
		case tree.NE:
			// a = x AND a != y
			if cmp == 0 {
				// x = y
				return tree.DBoolFalse, nil, true
			}
			return left, nil, true
		case tree.GT, tree.GE:
			// a = x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == tree.GT) {
				// x < y OR x = y
				return tree.DBoolFalse, nil, true
			}
			return left, nil, true
		case tree.LT, tree.LE:
			// a = x AND (a < y OR a <= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == tree.LT) {
				// x > y OR x = y
				return tree.DBoolFalse, nil, true
			}
			return left, nil, true
		}

	case tree.NE:
		switch rcmp.Operator {
		case tree.EQ:
			// a != x AND a = y
			if cmp == 0 {
				// x = y
				return tree.DBoolFalse, nil, true
			}
			return right, nil, true
		case tree.NE:
			// a != x AND a != y
			if cmp == 0 {
				// x = y
				return either, nil, true
			}
			return left, right, true
		case tree.GT:
			// a != x AND a > y
			return right, nil, cmp <= 0
		case tree.LT:
			// a != x AND a < y
			return right, nil, cmp >= 0
		case tree.GE:
			// a != x AND a >= y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.GT,
					rcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return right, nil, cmp == -1
		case tree.LE:
			// a != x AND a <= y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.LT,
					rcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return right, nil, cmp == +1
		}

	case tree.GT:
		switch rcmp.Operator {
		case tree.EQ:
			// a > x AND a = y
			if cmp != -1 {
				// x >= y
				return tree.DBoolFalse, nil, true
			}
			// x < y
			return right, nil, true
		case tree.NE:
			// a > x AND a != y
			return left, nil, cmp >= 0
		case tree.GT, tree.GE:
			// a > x AND (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return left, nil, true
			}
			// x < y
			return right, nil, true
		case tree.LT, tree.LE:
			// a > x AND (a < y OR a <= y)
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return tree.DBoolFalse, nil, true
		}

	case tree.GE:
		switch rcmp.Operator {
		case tree.EQ:
			// a >= x AND a = y
			if cmp == 1 {
				// x > y
				return tree.DBoolFalse, nil, true
			}
			// x <= y
			return right, nil, true
		case tree.NE:
			// a >= x AND x != y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.GT,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return left, nil, cmp == +1
		case tree.GT, tree.GE:
			// a >= x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == tree.GT) {
				// x < y
				return right, nil, true
			}
			// x >= y
			return left, nil, true
		case tree.LT:
			// a >= x AND a < y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return tree.DBoolFalse, nil, true
		case tree.LE:
			// a >= x AND a <= y
			if cmp == -1 {
				// x < y
				return left, right, true
			} else if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.EQ,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x > y
			return tree.DBoolFalse, nil, true
		}

	case tree.LT:
		switch rcmp.Operator {
		case tree.EQ:
			// a < x AND a = y
			if cmp != 1 {
				// x <= y
				return tree.DBoolFalse, nil, true
			}
			// x > y
			return right, nil, true
		case tree.NE:
			// a < x AND a != y
			return left, nil, cmp <= 0
		case tree.GT, tree.GE:
			// a < x AND (a > y OR a >= y)
			if cmp == 1 {
				// x > y
				return left, right, true
			}
			// x <= y
			return tree.DBoolFalse, nil, true
		case tree.LT, tree.LE:
			// a < x AND (a < y OR a <= y)
			if cmp != 1 {
				// x <= y
				return left, nil, true
			}
			// x > y
			return right, nil, true
		}

	case tree.LE:
		switch rcmp.Operator {
		case tree.EQ:
			// a <= x AND a = y
			if cmp == -1 {
				// x < y
				return tree.DBoolFalse, nil, true
			}
			// x >= y
			return right, nil, true
		case tree.NE:
			// a <= x AND a != y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.LT,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return left, nil, cmp == -1
		case tree.GT:
			// a <= x AND a > y
			if cmp == 1 {
				// x > y
				return left, right, true
			}
			return tree.DBoolFalse, nil, true
		case tree.GE:
			// a <= x AND a >= y
			if cmp == +1 {
				// x > y
				return left, right, true
			} else if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.EQ,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x < y
			return tree.DBoolFalse, nil, true
		case tree.LT, tree.LE:
			// a <= x AND (a > y OR a >= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == tree.LT) {
				// x > y
				return right, nil, true
			}
			// x <= y
			return left, nil, true
		}

	case tree.IsNotDistinctFrom:
		switch rcmp.Operator {
		case tree.IsNotDistinctFrom:
			if lcmpRight == tree.DNull && rcmpRight == tree.DNull {
				// a IS NOT DISTINCT FROM NULL AND a IS NOT DISTINCT FROM NULL
				return left, nil, true
			}
		}
	}

	return tree.DBoolTrue, nil, false
}

func simplifyOneAndInExpr(
	evalCtx *tree.EvalContext, left, right *tree.ComparisonExpr,
) (tree.TypedExpr, tree.TypedExpr) {
	if left.Operator != tree.In && right.Operator != tree.In {
		panic(fmt.Sprintf("IN expression required: %s vs %s", left, right))
	}

	origLeft, origRight := left, right

	switch left.Operator {
	case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE, tree.IsNotDistinctFrom:
		switch right.Operator {
		case tree.In:
			left, right = right, left
		}
		fallthrough

	case tree.In:
		ltuple := left.Right.(*tree.DTuple)
		ltuple.AssertSorted()

		values := ltuple.D

		switch right.Operator {
		case tree.IsNotDistinctFrom:
			if right.Right == tree.DNull {
				return tree.DBoolFalse, nil
			}

		case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
			// Our tuple will be sorted (see simplifyComparisonExpr). Binary search
			// for the right datum.
			datum := right.Right.(tree.Datum)
			i, found := ltuple.SearchSorted(evalCtx, datum)

			switch right.Operator {
			case tree.EQ:
				if found {
					return right, nil
				}
				return tree.DBoolFalse, nil

			case tree.NE:
				if found {
					if len(values) < 2 {
						return tree.DBoolFalse, nil
					}
					values = remove(values, i)
				}
				return tree.NewTypedComparisonExpr(
					tree.In,
					left.TypedLeft(),
					tree.NewDTuple(values...).SetSorted(),
				), nil

			case tree.GT:
				if i < len(values) {
					if found {
						values = values[i+1:]
					} else {
						values = values[i:]
					}
					if len(values) > 0 {
						return tree.NewTypedComparisonExpr(
							tree.In,
							left.TypedLeft(),
							tree.NewDTuple(values...).SetSorted(),
						), nil
					}
				}
				return tree.DBoolFalse, nil

			case tree.GE:
				if i < len(values) {
					values = values[i:]
					if len(values) > 0 {
						return tree.NewTypedComparisonExpr(
							tree.In,
							left.TypedLeft(),
							tree.NewDTuple(values...).SetSorted(),
						), nil
					}
				}
				return tree.DBoolFalse, nil

			case tree.LT:
				if i < len(values) {
					if i == 0 {
						return tree.DBoolFalse, nil
					}
					values = values[:i]
					return tree.NewTypedComparisonExpr(
						tree.In,
						left.TypedLeft(),
						tree.NewDTuple(values...).SetSorted(),
					), nil
				}
				return left, nil

			case tree.LE:
				if i < len(values) {
					if found {
						i++
					}
					if i == 0 {
						return tree.DBoolFalse, nil
					}
					values = values[:i]
					return tree.NewTypedComparisonExpr(
						tree.In,
						left.TypedLeft(),
						tree.NewDTuple(values...).SetSorted(),
					), nil
				}
				return left, nil
			}

		case tree.In:
			// Both of our tuples are sorted. Intersect the lists.
			rtuple := right.Right.(*tree.DTuple)
			intersection := intersectSorted(evalCtx, values, rtuple.D)
			if len(intersection) == 0 {
				return tree.DBoolFalse, nil
			}
			return tree.NewTypedComparisonExpr(
				tree.In,
				left.TypedLeft(),
				tree.NewDTuple(intersection...).SetSorted(),
			), nil
		}
	}

	return origLeft, origRight
}

func simplifyOrExpr(evalCtx *tree.EvalContext, n *tree.OrExpr) (tree.TypedExpr, bool) {
	// a OR b OR c OR d -> [a, b, c, d]
	equivalent := true
	exprs := splitOrExpr(evalCtx, n, nil)
	for i := range exprs {
		var equiv bool
		exprs[i], equiv = SimplifyExpr(evalCtx, exprs[i])
		if !equiv {
			equivalent = false
		}
		if isKnownTrue(exprs[i]) {
			return exprs[i], equivalent
		}
	}
	// Simplifying exprs might have transformed one of the elements into an OR
	// expression.
	texprs, exprs := exprs, nil
	for _, e := range texprs {
		exprs = splitOrExpr(evalCtx, e, exprs)
	}

	// Loop over the expressions looking for simplifications.
	//
	// TODO(pmattis): This is O(n^2) in the number of expressions. Could be
	// optimized by sorting the expressions based on the variables they contain.
outer:
	for i := len(exprs) - 1; i >= 0; i-- {
		for j := i - 1; j >= 0; j-- {
			var equiv bool
			exprs[j], exprs[i], equiv = simplifyOneOrExpr(evalCtx, exprs[j], exprs[i])
			if !equiv {
				equivalent = false
			}
			if isKnownTrue(exprs[j]) {
				return exprs[j], equivalent
			}
			if isKnownFalseOrNull(exprs[i]) {
				exprs[i] = nil
			}
			if exprs[i] == nil {
				// We found a simplification. Strip off the expression that is now nil
				// and continue the outer loop.
				n := len(exprs) - 1
				exprs[i] = exprs[n]
				exprs = exprs[:n]
				continue outer
			}
		}
	}

	// Reform the OR expressions.
	return joinOrExprs(exprs), equivalent
}

func simplifyOneOrExpr(
	evalCtx *tree.EvalContext, left, right tree.TypedExpr,
) (tree.TypedExpr, tree.TypedExpr, bool) {
	lcmp, ok := left.(*tree.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	rcmp, ok := right.(*tree.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	lcmpLeft, lcmpRight := lcmp.TypedLeft(), lcmp.TypedRight()
	rcmpLeft, rcmpRight := rcmp.TypedLeft(), rcmp.TypedRight()
	if !isDatum(lcmpRight) || !isDatum(rcmpRight) {
		return tree.DBoolTrue, nil, false
	}
	if !varEqual(lcmpLeft, rcmpLeft) {
		return left, right, true
	}

	if lcmp.Operator == tree.IsDistinctFrom || rcmp.Operator == tree.IsDistinctFrom {
		switch lcmp.Operator {
		case tree.IsNotDistinctFrom:
			if lcmpRight == tree.DNull && rcmpRight == tree.DNull {
				// a IS NOT DISTINCT FROM NULL OR a IS DISTINCT FROM NULL
				return tree.DBoolTrue, nil, true
			}
		case tree.IsDistinctFrom:
			if lcmpRight == tree.DNull {
				switch rcmp.Operator {
				case tree.IsNotDistinctFrom:
					if rcmpRight == tree.DNull {
						// a IS DISTINCT FROM NULL OR a IS NOT DISTINCT FROM NULL
						return tree.DBoolTrue, nil, true
					}
				case tree.IsDistinctFrom:
					if rcmpRight == tree.DNull {
						// a IS DISTINCT FROM NULL OR a IS DISTINCT FROM NULL
						return left, nil, true
					}
				}
			}
		}
		return left, right, true
	}

	if lcmp.Operator == tree.In || rcmp.Operator == tree.In {
		left, right = simplifyOneOrInExpr(evalCtx, lcmp, rcmp)
		return left, right, true
	}

	if reflect.TypeOf(lcmpRight) != reflect.TypeOf(rcmpRight) {
		allowCmp := false
		switch lcmp.Operator {
		case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
			switch rcmp.Operator {
			case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
				// Break, permitting heterogeneous comparison.
				allowCmp = true
			}
		}
		if !allowCmp {
			// If the types of the left and right datums are different, no
			// simplification is possible.
			return left, right, true
		}
	}

	ldatum := lcmpRight.(tree.Datum)
	rdatum := rcmpRight.(tree.Datum)
	cmp := ldatum.Compare(evalCtx, rdatum)

	// Determine which expression to use when either expression (left or right)
	// is valid as a return value but their types are different. The reason
	// to prefer a comparison between a column value and a datum of the same
	// type is that it makes index constraint construction easier.
	either := lcmp
	if !ldatum.ResolvedType().Equivalent(rdatum.ResolvedType()) {
		switch ta := lcmpLeft.(type) {
		case *tree.IndexedVar:
			if ta.ResolvedType().Equivalent(rdatum.ResolvedType()) {
				either = rcmp
			}
		}
	}

	// TODO(pmattis): Figure out how to generate this logic.
	switch lcmp.Operator {
	case tree.EQ:
		switch rcmp.Operator {
		case tree.EQ:
			// a = x OR a = y
			if cmp == 0 {
				// x = y
				return either, nil, true
			} else if cmp == 1 {
				// x > y
				ldatum, rdatum = rdatum, ldatum
			}
			return tree.NewTypedComparisonExpr(
				tree.In,
				lcmpLeft,
				tree.NewDTuple(ldatum, rdatum).SetSorted(),
			), nil, true
		case tree.NE:
			// a = x OR a != y
			if cmp == 0 {
				// x = y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			return right, nil, true
		case tree.GT:
			// a = x OR a > y
			if cmp == 1 {
				// x > y OR x = y
				return right, nil, true
			} else if cmp == 0 {
				return tree.NewTypedComparisonExpr(
					tree.GE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			return left, right, true
		case tree.GE:
			// a = x OR a >= y
			if cmp != -1 {
				// x >= y
				return right, nil, true
			}
			return left, right, true
		case tree.LT:
			// a = x OR a < y
			if cmp == -1 {
				// x < y OR x = y
				return right, nil, true
			} else if cmp == 0 {
				return tree.NewTypedComparisonExpr(
					tree.LE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			return left, right, true
		case tree.LE:
			// a = x OR a <= y
			if cmp != 1 {
				// x <= y
				return right, nil, true
			}
			return left, right, true
		}

	case tree.NE:
		switch rcmp.Operator {
		case tree.EQ:
			// a != x OR a = y
			if cmp == 0 {
				// x = y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x != y
			return left, nil, true
		case tree.NE:
			// a != x OR a != y
			if cmp == 0 {
				// x = y
				return either, nil, true
			}
			// x != y
			return makeIsDistinctFromNull(lcmpLeft), nil, true
		case tree.GT:
			// a != x OR a > y
			if cmp == 1 {
				// x > y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x <= y
			return left, nil, true
		case tree.GE:
			// a != x OR a >= y
			if cmp != -1 {
				// x >= y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x < y
			return left, nil, true
		case tree.LT:
			// a != x OR a < y
			if cmp == -1 {
				// x < y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x >= y
			return left, nil, true
		case tree.LE:
			// a != x OR a <= y
			if cmp != 1 {
				// x <= y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x > y
			return left, nil, true
		}

	case tree.GT:
		switch rcmp.Operator {
		case tree.EQ:
			// a > x OR a = y
			if cmp == -1 {
				// x < y
				return left, nil, true
			} else if cmp == 0 {
				return tree.NewTypedComparisonExpr(
					tree.GE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x > y
			return left, right, true
		case tree.NE:
			// a > x OR a != y
			if cmp == -1 {
				// x < y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x >= y
			return right, nil, true
		case tree.GT, tree.GE:
			// a > x OR (a > y OR a >= y)
			if cmp == -1 {
				return left, nil, true
			}
			return right, nil, true
		case tree.LT:
			// a > x OR a < y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.NE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			} else if cmp == -1 {
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x != y
			return left, right, true
		case tree.LE:
			// a > x OR a <= y
			if cmp != 1 {
				// x = y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x != y
			return left, right, true
		}

	case tree.GE:
		switch rcmp.Operator {
		case tree.EQ:
			// a >= x OR a = y
			if cmp != 1 {
				// x >. y
				return left, nil, true
			}
			// x < y
			return left, right, true
		case tree.NE:
			// a >= x OR a != y
			if cmp != 1 {
				// x <= y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x > y
			return right, nil, true
		case tree.GT:
			// a >= x OR a > y
			if cmp != 1 {
				// x <= y
				return left, nil, true
			}
			// x > y
			return right, nil, true
		case tree.GE:
			// a >= x OR a >= y
			if cmp == -1 {
				// x < y
				return left, nil, true
			}
			// x >= y
			return right, nil, true
		case tree.LT, tree.LE:
			// a >= x OR a < y
			if cmp != 1 {
				// x <= y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x > y
			return left, right, true
		}

	case tree.LT:
		switch rcmp.Operator {
		case tree.EQ:
			// a < x OR a = y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.LE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			} else if cmp == 1 {
				// x > y
				return left, nil, true
			}
			// x < y
			return left, right, true
		case tree.NE:
			// a < x OR a != y
			if cmp == 1 {
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			return right, nil, true
		case tree.GT:
			// a < x OR a > y
			if cmp == 0 {
				// x = y
				return tree.NewTypedComparisonExpr(
					tree.NE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			} else if cmp == 1 {
				// x > y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			return left, right, true
		case tree.GE:
			// a < x OR a >= y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return makeIsDistinctFromNull(lcmpLeft), nil, true
		case tree.LT, tree.LE:
			// a < x OR (a < y OR a <= y)
			if cmp == 1 {
				// x > y
				return left, nil, true
			}
			// x < y
			return right, nil, true
		}

	case tree.LE:
		switch rcmp.Operator {
		case tree.EQ:
			// a <= x OR a = y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return left, nil, true
		case tree.NE:
			// a <= x OR a != y
			if cmp != -1 {
				// x >= y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x < y
			return right, nil, true
		case tree.GT, tree.GE:
			// a <= x OR (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return makeIsDistinctFromNull(lcmpLeft), nil, true
			}
			// x < y
			return left, right, true
		case tree.LT, tree.LE:
			// a <= x OR a < y
			if cmp == -1 {
				// x < y
				return right, nil, true
			}
			// x >= y
			return left, nil, true
		}

	case tree.IsNotDistinctFrom:
		switch rcmp.Operator {
		case tree.IsNotDistinctFrom:
			if lcmpRight == tree.DNull && rcmpRight == tree.DNull {
				// a IS NOT DISTINCT FROM NULL OR a IS NOT DISTINCT FROM NULL
				return left, nil, true
			}
		}
	}

	return tree.DBoolTrue, nil, false
}

func simplifyOneOrInExpr(
	evalCtx *tree.EvalContext, left, right *tree.ComparisonExpr,
) (tree.TypedExpr, tree.TypedExpr) {
	if left.Operator != tree.In && right.Operator != tree.In {
		panic(fmt.Sprintf("IN expression required: %s vs %s", left, right))
	}

	origLeft, origRight := left, right

	switch left.Operator {
	case tree.EQ, tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
		switch right.Operator {
		case tree.In:
			left, right = right, left
		}
		fallthrough

	case tree.In:
		ltuple := left.Right.(*tree.DTuple)
		if !ltuple.Sorted() {
			return origLeft, origRight
		}

		switch right.Operator {
		case tree.EQ:
			datum := right.Right.(tree.Datum)
			// We keep the tuples for an IN expression in sorted order. So now we just
			// merge the two sorted lists.
			merged := mergeSorted(evalCtx, ltuple.D, tree.Datums{datum})
			return tree.NewTypedComparisonExpr(
				tree.In,
				left.TypedLeft(),
				tree.NewDTuple(merged...).SetSorted(),
			), nil

		case tree.NE, tree.GT, tree.GE, tree.LT, tree.LE:
			datum := right.Right.(tree.Datum)
			i, found := ltuple.SearchSorted(evalCtx, datum)

			switch right.Operator {
			case tree.NE:
				if found {
					return makeIsDistinctFromNull(right.TypedLeft()), nil
				}
				return right, nil

			case tree.GT:
				if i == 0 {
					// datum >= ltuple.D[0]
					if found {
						// datum == ltuple.D[0]
						return tree.NewTypedComparisonExpr(
							tree.GE,
							left.TypedLeft(),
							datum,
						), nil
					}
					return right, nil
				}
			case tree.GE:
				if i == 0 {
					// datum >= ltuple.D[0]
					return right, nil
				}
			case tree.LT:
				if i == len(ltuple.D) {
					// datum > ltuple.D[len(ltuple.D)-1]
					return right, nil
				} else if i == len(ltuple.D)-1 {
					// datum >= ltuple.D[len(ltuple.D)-1]
					if found {
						// datum == ltuple.D[len(ltuple.D)-1]
						return tree.NewTypedComparisonExpr(
							tree.LE,
							left.TypedLeft(),
							datum,
						), nil
					}
				}
			case tree.LE:
				if i == len(ltuple.D) ||
					(i == len(ltuple.D)-1 && ltuple.D[i].Compare(evalCtx, datum) == 0) {
					// datum >= ltuple.D[len(ltuple.D)-1]
					return right, nil
				}
			}

		case tree.In:
			// We keep the tuples for an IN expression in sorted order. So now we
			// just merge the two sorted lists.
			merged := mergeSorted(evalCtx, ltuple.D, right.Right.(*tree.DTuple).D)
			return tree.NewTypedComparisonExpr(
				tree.In,
				left.TypedLeft(),
				tree.NewDTuple(merged...).SetSorted(),
			), nil
		}
	}

	return origLeft, origRight
}

func simplifyComparisonExpr(
	evalCtx *tree.EvalContext, n *tree.ComparisonExpr,
) (tree.TypedExpr, bool) {
	// NormalizeExpr will have left comparisons in the form "<var> <op>
	// <datum>" unless they could not be simplified further in which case
	// SimplifyExpr cannot handle them. For example, "lower(a) = 'foo'"
	left, right := n.TypedLeft(), n.TypedRight()
	if isVar(left) && isDatum(right) {
		if right == tree.DNull {
			switch n.Operator {
			case tree.IsDistinctFrom, tree.IsNotDistinctFrom:
				switch left.(type) {
				case *tree.IndexedVar:
					// "a IS {,NOT} DISTINCT FROM NULL" can be used during index selection to restrict
					// the range of scanned keys.
					return n, true
				}
			default:
				// All of the remaining comparison operators have the property that when
				// comparing to NULL they evaluate to NULL (see evalComparisonOp). NULL is
				// not the same as false, but in the context of a WHERE clause, NULL is
				// considered not-true which is the same as false.
				return tree.DBoolFalse, true
			}
		}

		switch n.Operator {
		case tree.EQ:
			// Translate "(a, b) = (1, 2)" to "(a, b) IN ((1, 2))".
			switch left.(type) {
			case *tree.Tuple:
				return tree.NewTypedComparisonExpr(
					tree.In,
					left,
					tree.NewDTuple(right.(tree.Datum)).SetSorted(),
				), true
			}
			return n, true
		case tree.Contains:
			return n, true
		case tree.NE:
			// Translate "a != MAX" to "a < MAX" and "a != MIN" to "a > MIN".
			// These inequalities can be more easily used for index selection.
			// For datum types with large domains, this isn't particularly
			// useful. However, for types like BOOL, this is important because
			// it means we can use an index constraint for queries like
			// `SELECT * FROM t WHERE b != true`.
			if right.(tree.Datum).IsMax(evalCtx) {
				return tree.NewTypedComparisonExpr(
					tree.LT,
					left,
					right,
				), true
			} else if right.(tree.Datum).IsMin(evalCtx) {
				return tree.NewTypedComparisonExpr(
					tree.GT,
					left,
					right,
				), true
			}
			return n, true
		case tree.GE, tree.LE:
			return n, true
		case tree.GT:
			// This simplification is necessary so that subsequent transformation of
			// > constraint to >= can use Datum.Next without concern about whether a
			// next value exists. Note that if the variable (n.Left) is NULL, this
			// comparison would evaluate to NULL which is equivalent to false for a
			// boolean expression.
			if right.(tree.Datum).IsMax(evalCtx) {
				return tree.DBoolFalse, true
			}
			return n, true
		case tree.LT:
			// Note that if the variable is NULL, this would evaluate to NULL which
			// would equivalent to false for a boolean expression.
			if right.(tree.Datum).IsMin(evalCtx) {
				return tree.DBoolFalse, true
			}
			return n, true
		case tree.In, tree.NotIn:
			tuple := right.(*tree.DTuple).D
			if len(tuple) == 0 {
				return tree.DBoolFalse, true
			}
			return n, true
		case tree.Like:
			// a LIKE 'foo%' -> a >= "foo" AND a < "fop"
			if s, ok := tree.AsDString(right); ok {
				if i := strings.IndexAny(string(s), "_%"); i >= 0 {
					return makePrefixRange(s[:i], left, false), false
				}
				return makePrefixRange(s, left, true), false
			}
			// TODO(pmattis): Support tree.DBytes?
		case tree.SimilarTo:
			// a SIMILAR TO "foo.*" -> a >= "foo" AND a < "fop"
			if s, ok := tree.AsDString(right); ok {
				pattern := tree.SimilarEscape(string(s))
				if re, err := regexp.Compile(pattern); err == nil {
					prefix, complete := re.LiteralPrefix()
					return makePrefixRange(tree.DString(prefix), left, complete), false
				}
			}
			// TODO(pmattis): Support tree.DBytes?
		}
	}
	return tree.DBoolTrue, false
}

// simplifyBoolVar transforms a boolean IndexedVar into a ComparisonExpr
// (e.g. WHERE b -> WHERE b = true) This is so index selection only needs
// to work on ComparisonExprs.
func simplifyBoolVar(evalCtx *tree.EvalContext, n *tree.IndexedVar) (tree.TypedExpr, bool) {
	return SimplifyExpr(evalCtx, boolVarToComparison(n))
}

func boolVarToComparison(n *tree.IndexedVar) tree.TypedExpr {
	return tree.NewTypedComparisonExpr(
		tree.EQ,
		n,
		tree.DBoolTrue,
	)
}

func makePrefixRange(prefix tree.DString, datum tree.TypedExpr, complete bool) tree.TypedExpr {
	if complete {
		return tree.NewTypedComparisonExpr(
			tree.EQ,
			datum,
			&prefix,
		)
	}
	if len(prefix) == 0 {
		return tree.DBoolTrue
	}
	return tree.NewTypedAndExpr(
		tree.NewTypedComparisonExpr(
			tree.GE,
			datum,
			&prefix,
		),
		tree.NewTypedComparisonExpr(
			tree.LT,
			datum,
			tree.NewDString(string(roachpb.Key(prefix).PrefixEnd())),
		),
	)
}

func mergeSorted(evalCtx *tree.EvalContext, a, b tree.Datums) tree.Datums {
	r := make(tree.Datums, 0, len(a)+len(b))
	for len(a) > 0 || len(b) > 0 {
		if len(a) == 0 {
			r = append(r, b...)
			break
		}
		if len(b) == 0 {
			r = append(r, a...)
			break
		}
		switch a[0].Compare(evalCtx, b[0]) {
		case -1:
			r = append(r, a[0])
			a = a[1:]
		case 0:
			r = append(r, a[0])
			a = a[1:]
			b = b[1:]
		case 1:
			r = append(r, b[0])
			b = b[1:]
		}
	}
	return r
}

func intersectSorted(evalCtx *tree.EvalContext, a, b tree.Datums) tree.Datums {
	n := len(a)
	if n > len(b) {
		n = len(b)
	}
	r := make(tree.Datums, 0, n)
	for len(a) > 0 && len(b) > 0 {
		switch a[0].Compare(evalCtx, b[0]) {
		case -1:
			a = a[1:]
		case 0:
			r = append(r, a[0])
			a = a[1:]
			b = b[1:]
		case 1:
			b = b[1:]
		}
	}
	return r
}

func remove(a tree.Datums, i int) tree.Datums {
	r := make(tree.Datums, len(a)-1)
	copy(r, a[:i])
	copy(r[i:], a[i+1:])
	return r
}

func isDatum(e tree.TypedExpr) bool {
	_, ok := e.(tree.Datum)
	return ok
}

// isVar returns true if the expression is an ivar or a tuple composed of
// ivars.
func isVar(e tree.TypedExpr) bool {
	switch t := e.(type) {
	case *tree.IndexedVar:
		return true

	case *tree.Tuple:
		for _, v := range t.Exprs {
			if !isVar(v.(tree.TypedExpr)) {
				return false
			}
		}
		return true
	}

	return false
}

// varEqual returns true if the two expressions are both IndexedVars pointing to
// the same column or are both tuples composed of IndexedVars pointing at the same
// columns.
func varEqual(a, b tree.TypedExpr) bool {
	switch ta := a.(type) {
	case *tree.IndexedVar:
		switch tb := b.(type) {
		case *tree.IndexedVar:
			return ta.Idx == tb.Idx
		}

	case *tree.Tuple:
		switch tb := b.(type) {
		case *tree.Tuple:
			if len(ta.Exprs) == len(tb.Exprs) {
				for i := range ta.Exprs {
					if !varEqual(ta.Exprs[i].(tree.TypedExpr), tb.Exprs[i].(tree.TypedExpr)) {
						return false
					}
				}
				return true
			}
		}
	}

	return false
}

func makeIsDistinctFromNull(left tree.TypedExpr) tree.TypedExpr {
	return tree.NewTypedComparisonExpr(
		tree.IsDistinctFrom,
		left,
		tree.DNull,
	)
}
