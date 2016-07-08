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

package sql

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// analyzeExpr analyzes and simplifies an expression, returning a list of
// expressions of a list of expressions. The top-level list contains
// disjunctions (a.k.a OR expressions). The second-level contains conjunctions
// (a.k.a. AND expressions). For example:
//
//   (a AND b) OR c -> [[a, b], c]
//
// Expression analysis should only be performed on the WHERE clause of SELECT
// statements. The returned expressions are not guaranteed to be semantically
// identical to the original. In particular, expressions that return NULL might
// be transformed into expressions that return false. This is ok in the context
// of WHERE clauses where we care about not-true for filtering
// purposes. Additionally, expressions that analysis does not handle will be
// transformed into true. The caller is required to use the original expression
// (which will be unchanged by analyzeExpr) for filtering.
//
// Returns false for equivalent if the resulting expressions are not equivalent
// to the originals. This occurs for expressions which are currently not
// handled by simplification (they are replaced by "true").
func analyzeExpr(e parser.TypedExpr) (exprs []parser.TypedExprs, equivalent bool) {
	e, equivalent = simplifyExpr(e)
	orExprs := splitOrExpr(e, nil)
	results := make([]parser.TypedExprs, len(orExprs))
	for i := range orExprs {
		results[i] = splitAndExpr(orExprs[i], nil)
	}
	return results, equivalent
}

// splitOrExpr flattens a tree of OR expressions returning all of the child
// expressions as a list. Any non-OR expression is returned as a single element
// in the list.
//
//   a OR b OR c OR d -> [a, b, c, d]
func splitOrExpr(e parser.TypedExpr, exprs parser.TypedExprs) parser.TypedExprs {
	switch t := e.(type) {
	case *parser.OrExpr:
		return splitOrExpr(t.TypedRight(), splitOrExpr(t.TypedLeft(), exprs))
	}
	return append(exprs, e)
}

// splitAndExpr flattens a tree of AND expressions returning all of the child
// expressions as a list. Any non-AND expression is returned as a single
// element in the list.
//
//   a AND b AND c AND d -> [a, b, c, d]
func splitAndExpr(e parser.TypedExpr, exprs parser.TypedExprs) parser.TypedExprs {
	switch t := e.(type) {
	case *parser.AndExpr:
		return splitAndExpr(t.TypedRight(), splitAndExpr(t.TypedLeft(), exprs))
	}
	return append(exprs, e)
}

// joinOrExprs performs the inverse operation of splitOrExpr, joining
// together the individual expressions using OrExpr nodes.
func joinOrExprs(exprs parser.TypedExprs) parser.TypedExpr {
	return joinExprs(exprs, func(left, right parser.TypedExpr) parser.TypedExpr {
		return parser.NewTypedOrExpr(left, right)
	})
}

// joinAndExprs performs the inverse operation of splitAndExpr, joining
// together the individual expressions using AndExpr nodes.
func joinAndExprs(exprs parser.TypedExprs) parser.TypedExpr {
	return joinExprs(exprs, func(left, right parser.TypedExpr) parser.TypedExpr {
		return parser.NewTypedAndExpr(left, right)
	})
}

func joinExprs(exprs parser.TypedExprs, joinExprsFn func(left, right parser.TypedExpr) parser.TypedExpr) parser.TypedExpr {
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

// simplifyExpr transforms an expression such that it contains only expressions
// involving qvalues that can be used for index selection. If an expression is
// encountered that cannot be used for index selection (e.g. "func(val)") that
// part of the expression tree is considered to evaluate to true, possibly
// rendering the entire expression as true. Additionally, various
// normalizations are performed on comparison expressions. For example:
//
//   (a < 1 AND a < 2)  -> (a < 1)
//   (a < 1 AND a > 2)  -> false
//   (a > 1 OR a < 2)   -> true
//   (a > 1 OR func(b)) -> true
//
// Note that simplification is not normalization. Normalization as performed by
// parser.NormalizeExpr returns an expression that is equivalent to the
// original. Simplification can return an expression with parts of the
// expression tree stripped out.
//
// Returns false for equivalent if the resulting expression is not equivalent
// to the original. This occurs for expressions which are currently not handled
// by simplification.
func simplifyExpr(e parser.TypedExpr) (simplified parser.TypedExpr, equivalent bool) {
	if e == parser.DNull {
		return e, true
	}
	switch t := e.(type) {
	case *parser.NotExpr:
		return simplifyNotExpr(t)
	case *parser.AndExpr:
		return simplifyAndExpr(t)
	case *parser.OrExpr:
		return simplifyOrExpr(t)
	case *parser.ComparisonExpr:
		return simplifyComparisonExpr(t)
	case *qvalue, *parser.IndexedVar, *parser.DBool:
		return e, true
	}
	// We don't know how to simplify expressions that fall through to here, so
	// consider this part of the expression true.
	return parser.MakeDBool(true), false
}

func simplifyNotExpr(n *parser.NotExpr) (parser.TypedExpr, bool) {
	if n.Expr == parser.DNull {
		return parser.DNull, true
	}
	switch t := n.Expr.(type) {
	case *parser.ComparisonExpr:
		op := t.Operator
		switch op {
		case parser.EQ:
			op = parser.NE
		case parser.NE:
			op = parser.EQ
		case parser.GT:
			op = parser.LE
		case parser.GE:
			op = parser.LT
		case parser.LT:
			op = parser.GE
		case parser.LE:
			op = parser.GT
		case parser.In:
			op = parser.NotIn
		case parser.NotIn:
			op = parser.In
		case parser.Like:
			op = parser.NotLike
		case parser.NotLike:
			op = parser.Like
		case parser.ILike:
			op = parser.NotILike
		case parser.NotILike:
			op = parser.ILike
		case parser.SimilarTo:
			op = parser.NotSimilarTo
		case parser.NotSimilarTo:
			op = parser.SimilarTo
		case parser.RegMatch:
			op = parser.NotRegMatch
		case parser.RegIMatch:
			op = parser.NotRegIMatch
		default:
			return parser.MakeDBool(true), false
		}
		return simplifyExpr(parser.NewTypedComparisonExpr(
			op,
			t.TypedLeft(),
			t.TypedRight(),
		))

	case *parser.AndExpr:
		// De Morgan's Law: NOT (a AND b) -> (NOT a) OR (NOT b)
		return simplifyExpr(parser.NewTypedOrExpr(
			parser.NewTypedNotExpr(t.TypedLeft()),
			parser.NewTypedNotExpr(t.TypedRight()),
		))

	case *parser.OrExpr:
		// De Morgan's Law: NOT (a OR b) -> (NOT a) AND (NOT b)
		return simplifyExpr(parser.NewTypedAndExpr(
			parser.NewTypedNotExpr(t.TypedLeft()),
			parser.NewTypedNotExpr(t.TypedRight()),
		))
	}
	return parser.MakeDBool(true), false
}

func isKnownTrue(e parser.TypedExpr) bool {
	if e == parser.DNull {
		return false
	}
	if b, ok := e.(*parser.DBool); ok {
		return bool(*b)
	}
	return false
}

func isKnownFalseOrNull(e parser.TypedExpr) bool {
	if e == parser.DNull {
		return true
	}
	if b, ok := e.(*parser.DBool); ok {
		return !bool(*b)
	}
	return false
}

func simplifyAndExpr(n *parser.AndExpr) (parser.TypedExpr, bool) {
	// a AND b AND c AND d -> [a, b, c, d]
	equivalent := true
	exprs := splitAndExpr(n, nil)
	for i := range exprs {
		var equiv bool
		exprs[i], equiv = simplifyExpr(exprs[i])
		if !equiv {
			equivalent = false
		}
		if isKnownFalseOrNull(exprs[i]) {
			return parser.MakeDBool(false), equivalent
		}
	}
	// Simplifying exprs might have transformed one of the elements into an AND
	// expression.
	texprs, exprs := exprs, nil
	for _, e := range texprs {
		exprs = splitAndExpr(e, exprs)
	}

	// Loop over the expressions looking for simplifications.
	//
	// TODO(pmattis): This is O(n^2) in the number of expressions. Could be
	// optimized by sorting the expressions based on the variables they contain.
outer:
	for i := len(exprs) - 1; i >= 0; i-- {
		for j := i - 1; j >= 0; j-- {
			var equiv bool
			exprs[j], exprs[i], equiv = simplifyOneAndExpr(exprs[j], exprs[i])
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

func simplifyOneAndExpr(left, right parser.TypedExpr) (parser.TypedExpr, parser.TypedExpr, bool) {
	lcmp, ok := left.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	rcmp, ok := right.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	lcmpLeft, lcmpRight := lcmp.TypedLeft(), lcmp.TypedRight()
	rcmpLeft, rcmpRight := rcmp.TypedLeft(), rcmp.TypedRight()
	if !isDatum(lcmpRight) || !isDatum(rcmpRight) {
		return parser.MakeDBool(true), nil, false
	}
	if !varEqual(lcmpLeft, rcmpLeft) {
		return left, right, true
	}

	if lcmp.Operator == parser.IsNot || rcmp.Operator == parser.IsNot {
		switch lcmp.Operator {
		case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE, parser.In:
			if rcmpRight == parser.DNull {
				// a <cmp> x AND a IS NOT NULL
				return left, nil, true
			}
		case parser.Is:
			if lcmpRight == parser.DNull && rcmpRight == parser.DNull {
				// a IS NULL AND a IS NOT NULL
				return parser.MakeDBool(false), nil, true
			}
		case parser.IsNot:
			if lcmpRight == parser.DNull {
				switch rcmp.Operator {
				case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE, parser.In:
					// a IS NOT NULL AND a <cmp> x
					return right, nil, true
				case parser.Is:
					if rcmpRight == parser.DNull {
						// a IS NOT NULL AND a IS NULL
						return parser.MakeDBool(false), nil, true
					}
				case parser.IsNot:
					if rcmpRight == parser.DNull {
						// a IS NOT NULL AND a IS NOT NULL
						return left, nil, true
					}
				}
			}
		}
		return left, right, true
	}

	if lcmp.Operator == parser.In || rcmp.Operator == parser.In {
		left, right = simplifyOneAndInExpr(lcmp, rcmp)
		return left, right, true
	}

	if reflect.TypeOf(lcmpRight) != reflect.TypeOf(rcmpRight) {
		allowCmp := false
		switch lcmp.Operator {
		case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
			switch rcmp.Operator {
			case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
				// Break, permitting heterogeneous comparison.
				allowCmp = true
			}
		}
		if !allowCmp {
			if lcmp.Operator == parser.Is && lcmpRight == parser.DNull {
				// a IS NULL AND a <cmp> x
				return parser.MakeDBool(false), nil, true
			}
			if rcmp.Operator == parser.Is && rcmpRight == parser.DNull {
				// a <cmp> x AND a IS NULL
				return parser.MakeDBool(false), nil, true
			}
			// Note that "a IS NULL and a IS NULL" cannot happen here because
			// "reflect.TypeOf(NULL) == reflect.TypeOf(NULL)".
			return left, right, true
		}
	}

	ldatum := lcmpRight.(parser.Datum)
	rdatum := rcmpRight.(parser.Datum)
	cmp := ldatum.Compare(rdatum)

	// Determine which expression to use when either expression (left or right)
	// is valid as a return value but their types are different. The reason
	// to prefer a comparison between a column value and a datum of the same
	// type is that it makes index constraint construction easier.
	either := lcmp
	if !ldatum.TypeEqual(rdatum) {
		switch ta := lcmpLeft.(type) {
		case *qvalue:
			if ta.datum.TypeEqual(rdatum) {
				either = rcmp
			}
		}
	}

	// TODO(pmattis): Figure out how to generate this logic.
	switch lcmp.Operator {
	case parser.EQ:
		switch rcmp.Operator {
		case parser.EQ:
			// a = x AND a = y
			if cmp == 0 {
				// x = y
				return either, nil, true
			}
			return parser.MakeDBool(false), nil, true
		case parser.NE:
			// a = x AND a != y
			if cmp == 0 {
				// x = y
				return parser.MakeDBool(false), nil, true
			}
			return left, nil, true
		case parser.GT, parser.GE:
			// a = x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == parser.GT) {
				// x < y OR x = y
				return parser.MakeDBool(false), nil, true
			}
			return left, nil, true
		case parser.LT, parser.LE:
			// a = x AND (a < y OR a <= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == parser.LT) {
				// x > y OR x = y
				return parser.MakeDBool(false), nil, true
			}
			return left, nil, true
		}

	case parser.NE:
		switch rcmp.Operator {
		case parser.EQ:
			// a != x AND a = y
			if cmp == 0 {
				// x = y
				return parser.MakeDBool(false), nil, true
			}
			return right, nil, true
		case parser.NE:
			// a != x AND a != y
			if cmp == 0 {
				// x = y
				return either, nil, true
			}
			return left, right, true
		case parser.GT:
			// a != x AND a > y
			return right, nil, cmp <= 0
		case parser.LT:
			// a != x AND a < y
			return right, nil, cmp >= 0
		case parser.GE:
			// a != x AND a >= y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.GT,
					rcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return right, nil, cmp == -1
		case parser.LE:
			// a != x AND a <= y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.LT,
					rcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return right, nil, cmp == +1
		}

	case parser.GT:
		switch rcmp.Operator {
		case parser.EQ:
			// a > x AND a = y
			if cmp != -1 {
				// x >= y
				return parser.MakeDBool(false), nil, true
			}
			// x < y
			return right, nil, true
		case parser.NE:
			// a > x AND a != y
			return left, nil, cmp >= 0
		case parser.GT, parser.GE:
			// a > x AND (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return left, nil, true
			}
			// x < y
			return right, nil, true
		case parser.LT, parser.LE:
			// a > x AND (a < y OR a <= y)
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return parser.MakeDBool(false), nil, true
		}

	case parser.GE:
		switch rcmp.Operator {
		case parser.EQ:
			// a >= x AND a = y
			if cmp == 1 {
				// x > y
				return parser.MakeDBool(false), nil, true
			}
			// x <= y
			return right, nil, true
		case parser.NE:
			// a >= x AND x != y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.GT,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return left, nil, cmp == +1
		case parser.GT, parser.GE:
			// a >= x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == parser.GT) {
				// x < y
				return right, nil, true
			}
			// x >= y
			return left, nil, true
		case parser.LT:
			// a >= x AND a < y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return parser.MakeDBool(false), nil, true
		case parser.LE:
			// a >= x AND a <= y
			if cmp == -1 {
				// x < y
				return left, right, true
			} else if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.EQ,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x > y
			return parser.MakeDBool(false), nil, true
		}

	case parser.LT:
		switch rcmp.Operator {
		case parser.EQ:
			// a < x AND a = y
			if cmp != 1 {
				// x <= y
				return parser.MakeDBool(false), nil, true
			}
			// x > y
			return right, nil, true
		case parser.NE:
			// a < x AND a != y
			return left, nil, cmp <= 0
		case parser.GT, parser.GE:
			// a < x AND (a > y OR a >= y)
			if cmp == 1 {
				// x > y
				return left, right, true
			}
			// x <= y
			return parser.MakeDBool(false), nil, true
		case parser.LT, parser.LE:
			// a < x AND (a < y OR a <= y)
			if cmp != 1 {
				// x <= y
				return left, nil, true
			}
			// x > y
			return right, nil, true
		}

	case parser.LE:
		switch rcmp.Operator {
		case parser.EQ:
			// a <= x AND a = y
			if cmp == -1 {
				// x < y
				return parser.MakeDBool(false), nil, true
			}
			// x >= y
			return right, nil, true
		case parser.NE:
			// a <= x AND a != y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.LT,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x != y
			return left, nil, cmp == -1
		case parser.GT:
			// a <= x AND a > y
			if cmp == 1 {
				// x > y
				return left, right, true
			}
			return parser.MakeDBool(false), nil, true
		case parser.GE:
			// a <= x AND a >= y
			if cmp == +1 {
				// x > y
				return left, right, true
			} else if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.EQ,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x < y
			return parser.MakeDBool(false), nil, true
		case parser.LT, parser.LE:
			// a <= x AND (a > y OR a >= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == parser.LT) {
				// x > y
				return right, nil, true
			}
			// x <= y
			return left, nil, true
		}

	case parser.Is:
		switch rcmp.Operator {
		case parser.Is:
			if lcmpRight == parser.DNull && rcmpRight == parser.DNull {
				// a IS NULL AND a IS NULL
				return left, nil, true
			}
		}
	}

	return parser.MakeDBool(true), nil, false
}

func simplifyOneAndInExpr(left, right *parser.ComparisonExpr) (parser.TypedExpr, parser.TypedExpr) {
	if left.Operator != parser.In && right.Operator != parser.In {
		panic(fmt.Sprintf("IN expression required: %s vs %s", left, right))
	}

	origLeft, origRight := left, right

	switch left.Operator {
	case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE, parser.Is:
		switch right.Operator {
		case parser.In:
			left, right = right, left
		}
		fallthrough

	case parser.In:
		ltuple := *left.Right.(*parser.DTuple)
		switch right.Operator {
		case parser.Is:
			if right.Right == parser.DNull {
				return parser.MakeDBool(false), nil
			}

		case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
			// Our tuple will be sorted (see simplifyComparisonExpr). Binary search
			// for the right datum.
			datum := right.Right.(parser.Datum)
			i := sort.Search(len(ltuple), func(i int) bool {
				return ltuple[i].(parser.Datum).Compare(datum) >= 0
			})

			switch right.Operator {
			case parser.EQ:
				if i < len(ltuple) && ltuple[i].Compare(datum) == 0 {
					return right, nil
				}
				return parser.MakeDBool(false), nil

			case parser.NE:
				if i < len(ltuple) && ltuple[i].Compare(datum) == 0 {
					if len(ltuple) < 2 {
						return parser.MakeDBool(false), nil
					}
					ltuple = remove(ltuple, i)
				}
				return parser.NewTypedComparisonExpr(
					parser.In,
					left.TypedLeft(),
					&ltuple,
				), nil

			case parser.GT:
				if i < len(ltuple) {
					if ltuple[i].Compare(datum) == 0 {
						ltuple = ltuple[i+1:]
					} else {
						ltuple = ltuple[i:]
					}
					if len(ltuple) > 0 {
						return parser.NewTypedComparisonExpr(
							parser.In,
							left.TypedLeft(),
							&ltuple,
						), nil
					}
				}
				return parser.MakeDBool(false), nil

			case parser.GE:
				if i < len(ltuple) {
					ltuple = ltuple[i:]
					if len(ltuple) > 0 {
						return parser.NewTypedComparisonExpr(
							parser.In,
							left.TypedLeft(),
							&ltuple,
						), nil
					}
				}
				return parser.MakeDBool(false), nil

			case parser.LT:
				if i < len(ltuple) {
					if i == 0 {
						return parser.MakeDBool(false), nil
					}
					ltuple = ltuple[:i]
					return parser.NewTypedComparisonExpr(
						parser.In,
						left.TypedLeft(),
						&ltuple,
					), nil
				}
				return left, nil

			case parser.LE:
				if i < len(ltuple) {
					if ltuple[i].Compare(datum) == 0 {
						i++
					}
					if i == 0 {
						return parser.MakeDBool(false), nil
					}
					ltuple = ltuple[:i]
					return parser.NewTypedComparisonExpr(
						parser.In,
						left.TypedLeft(),
						&ltuple,
					), nil
				}
				return left, nil
			}

		case parser.In:
			// Both of our tuples are sorted. Intersect the lists.
			rtuple := *right.Right.(*parser.DTuple)
			intersection := intersectSorted(ltuple, rtuple)
			if len(*intersection) == 0 {
				return parser.MakeDBool(false), nil
			}
			return parser.NewTypedComparisonExpr(
				parser.In,
				left.TypedLeft(),
				intersection,
			), nil
		}
	}

	return origLeft, origRight
}

func simplifyOrExpr(n *parser.OrExpr) (parser.TypedExpr, bool) {
	// a OR b OR c OR d -> [a, b, c, d]
	equivalent := true
	exprs := splitOrExpr(n, nil)
	for i := range exprs {
		var equiv bool
		exprs[i], equiv = simplifyExpr(exprs[i])
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
		exprs = splitOrExpr(e, exprs)
	}

	// Loop over the expressions looking for simplifications.
	//
	// TODO(pmattis): This is O(n^2) in the number of expressions. Could be
	// optimized by sorting the expressions based on the variables they contain.
outer:
	for i := len(exprs) - 1; i >= 0; i-- {
		for j := i - 1; j >= 0; j-- {
			var equiv bool
			exprs[j], exprs[i], equiv = simplifyOneOrExpr(exprs[j], exprs[i])
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

func simplifyOneOrExpr(left, right parser.TypedExpr) (parser.TypedExpr, parser.TypedExpr, bool) {
	lcmp, ok := left.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	rcmp, ok := right.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	lcmpLeft, lcmpRight := lcmp.TypedLeft(), lcmp.TypedRight()
	rcmpLeft, rcmpRight := rcmp.TypedLeft(), rcmp.TypedRight()
	if !isDatum(lcmpRight) || !isDatum(rcmpRight) {
		return parser.MakeDBool(true), nil, false
	}
	if !varEqual(lcmpLeft, rcmpLeft) {
		return left, right, true
	}

	if lcmp.Operator == parser.IsNot || rcmp.Operator == parser.IsNot {
		switch lcmp.Operator {
		case parser.Is:
			if lcmpRight == parser.DNull && rcmpRight == parser.DNull {
				// a IS NULL OR a IS NOT NULL
				return parser.MakeDBool(true), nil, true
			}
		case parser.IsNot:
			if lcmpRight == parser.DNull {
				switch rcmp.Operator {
				case parser.Is:
					if rcmpRight == parser.DNull {
						// a IS NOT NULL OR a IS NULL
						return parser.MakeDBool(true), nil, true
					}
				case parser.IsNot:
					if rcmpRight == parser.DNull {
						// a IS NOT NULL OR a IS NOT NULL
						return left, nil, true
					}
				}
			}
		}
		return left, right, true
	}

	if lcmp.Operator == parser.In || rcmp.Operator == parser.In {
		left, right = simplifyOneOrInExpr(lcmp, rcmp)
		return left, right, true
	}

	if reflect.TypeOf(lcmpRight) != reflect.TypeOf(rcmpRight) {
		allowCmp := false
		switch lcmp.Operator {
		case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
			switch rcmp.Operator {
			case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
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

	ldatum := lcmpRight.(parser.Datum)
	rdatum := rcmpRight.(parser.Datum)
	cmp := ldatum.Compare(rdatum)

	// Determine which expression to use when either expression (left or right)
	// is valid as a return value but their types are different. The reason
	// to prefer a comparison between a column value and a datum of the same
	// type is that it makes index constraint construction easier.
	either := lcmp
	if !ldatum.TypeEqual(rdatum) {
		switch ta := lcmpLeft.(type) {
		case *qvalue:
			if ta.datum.TypeEqual(rdatum) {
				either = rcmp
			}
		}
	}

	// TODO(pmattis): Figure out how to generate this logic.
	switch lcmp.Operator {
	case parser.EQ:
		switch rcmp.Operator {
		case parser.EQ:
			// a = x OR a = y
			if cmp == 0 {
				// x = y
				return either, nil, true
			} else if cmp == 1 {
				// x > y
				ldatum, rdatum = rdatum, ldatum
			}
			return parser.NewTypedComparisonExpr(
				parser.In,
				lcmpLeft,
				&parser.DTuple{ldatum, rdatum},
			), nil, true
		case parser.NE:
			// a = x OR a != y
			if cmp == 0 {
				// x = y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			return right, nil, true
		case parser.GT:
			// a = x OR a > y
			if cmp == 1 {
				// x > y OR x = y
				return right, nil, true
			} else if cmp == 0 {
				return parser.NewTypedComparisonExpr(
					parser.GE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			return left, right, true
		case parser.GE:
			// a = x OR a >= y
			if cmp != -1 {
				// x >= y
				return right, nil, true
			}
			return left, right, true
		case parser.LT:
			// a = x OR a < y
			if cmp == -1 {
				// x < y OR x = y
				return right, nil, true
			} else if cmp == 0 {
				return parser.NewTypedComparisonExpr(
					parser.LE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			return left, right, true
		case parser.LE:
			// a = x OR a <= y
			if cmp != 1 {
				// x <= y
				return right, nil, true
			}
			return left, right, true
		}

	case parser.NE:
		switch rcmp.Operator {
		case parser.EQ:
			// a != x OR a = y
			if cmp == 0 {
				// x = y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x != y
			return left, nil, true
		case parser.NE:
			// a != x OR a != y
			if cmp == 0 {
				// x = y
				return either, nil, true
			}
			// x != y
			return makeIsNotNull(lcmpLeft), nil, true
		case parser.GT:
			// a != x OR a > y
			if cmp == 1 {
				// x > y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x <= y
			return left, nil, true
		case parser.GE:
			// a != x OR a >= y
			if cmp != -1 {
				// x >= y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x < y
			return left, nil, true
		case parser.LT:
			// a != x OR a < y
			if cmp == -1 {
				// x < y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x >= y
			return left, nil, true
		case parser.LE:
			// a != x OR a <= y
			if cmp != 1 {
				// x <= y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x > y
			return left, nil, true
		}

	case parser.GT:
		switch rcmp.Operator {
		case parser.EQ:
			// a > x OR a = y
			if cmp == -1 {
				// x < y
				return left, nil, true
			} else if cmp == 0 {
				return parser.NewTypedComparisonExpr(
					parser.GE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			}
			// x > y
			return left, right, true
		case parser.NE:
			// a > x OR a != y
			if cmp == -1 {
				// x < y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x >= y
			return right, nil, true
		case parser.GT, parser.GE:
			// a > x OR (a > y OR a >= y)
			if cmp == -1 {
				return left, nil, true
			}
			return right, nil, true
		case parser.LT:
			// a > x OR a < y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.NE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			} else if cmp == -1 {
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x != y
			return left, right, true
		case parser.LE:
			// a > x OR a <= y
			if cmp != 1 {
				// x = y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x != y
			return left, right, true
		}

	case parser.GE:
		switch rcmp.Operator {
		case parser.EQ:
			// a >= x OR a = y
			if cmp != 1 {
				// x >. y
				return left, nil, true
			}
			// x < y
			return left, right, true
		case parser.NE:
			// a >= x OR a != y
			if cmp != 1 {
				// x <= y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x > y
			return right, nil, true
		case parser.GT:
			// a >= x OR a > y
			if cmp != 1 {
				// x <= y
				return left, nil, true
			}
			// x > y
			return right, nil, true
		case parser.GE:
			// a >= x OR a >= y
			if cmp == -1 {
				// x < y
				return left, nil, true
			}
			// x >= y
			return right, nil, true
		case parser.LT, parser.LE:
			// a >= x OR a < y
			if cmp != 1 {
				// x <= y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x > y
			return left, right, true
		}

	case parser.LT:
		switch rcmp.Operator {
		case parser.EQ:
			// a < x OR a = y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.LE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			} else if cmp == 1 {
				// x > y
				return left, nil, true
			}
			// x < y
			return left, right, true
		case parser.NE:
			// a < x OR a != y
			if cmp == 1 {
				return makeIsNotNull(lcmpLeft), nil, true
			}
			return right, nil, true
		case parser.GT:
			// a < x OR a > y
			if cmp == 0 {
				// x = y
				return parser.NewTypedComparisonExpr(
					parser.NE,
					lcmpLeft,
					either.TypedRight(),
				), nil, true
			} else if cmp == 1 {
				// x > y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			return left, right, true
		case parser.GE:
			// a < x OR a >= y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return makeIsNotNull(lcmpLeft), nil, true
		case parser.LT, parser.LE:
			// a < x OR (a < y OR a <= y)
			if cmp == 1 {
				// x > y
				return left, nil, true
			}
			// x < y
			return right, nil, true
		}

	case parser.LE:
		switch rcmp.Operator {
		case parser.EQ:
			// a <= x OR a = y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return left, nil, true
		case parser.NE:
			// a <= x OR a != y
			if cmp != -1 {
				// x >= y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x < y
			return right, nil, true
		case parser.GT, parser.GE:
			// a <= x OR (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return makeIsNotNull(lcmpLeft), nil, true
			}
			// x < y
			return left, right, true
		case parser.LT, parser.LE:
			// a <= x OR a < y
			if cmp == -1 {
				// x < y
				return right, nil, true
			}
			// x >= y
			return left, nil, true
		}

	case parser.Is:
		switch rcmp.Operator {
		case parser.Is:
			if lcmpRight == parser.DNull && rcmpRight == parser.DNull {
				// a IS NULL OR a IS NULL
				return left, nil, true
			}
		}
	}

	return parser.MakeDBool(true), nil, false
}

func simplifyOneOrInExpr(left, right *parser.ComparisonExpr) (parser.TypedExpr, parser.TypedExpr) {
	if left.Operator != parser.In && right.Operator != parser.In {
		panic(fmt.Sprintf("IN expression required: %s vs %s", left, right))
	}

	origLeft, origRight := left, right

	switch left.Operator {
	case parser.EQ, parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
		switch right.Operator {
		case parser.In:
			left, right = right, left
		}
		fallthrough

	case parser.In:
		tuple := *left.Right.(*parser.DTuple)
		switch right.Operator {
		case parser.EQ:
			datum := right.Right.(parser.Datum)
			// We keep the tuples for an IN expression in sorted order. So now we just
			// merge the two sorted lists.
			return parser.NewTypedComparisonExpr(
				parser.In,
				left.TypedLeft(),
				mergeSorted(tuple, parser.DTuple{datum}),
			), nil

		case parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
			datum := right.Right.(parser.Datum)
			i := sort.Search(len(tuple), func(i int) bool {
				return tuple[i].(parser.Datum).Compare(datum) >= 0
			})

			switch right.Operator {
			case parser.NE:
				if i < len(tuple) && tuple[i].Compare(datum) == 0 {
					return makeIsNotNull(right.TypedLeft()), nil
				}
				return right, nil

			case parser.GT:
				if i == 0 {
					// datum >= tuple[0]
					if tuple[i].Compare(datum) == 0 {
						// datum = tuple[0]
						return parser.NewTypedComparisonExpr(
							parser.GE,
							left.TypedLeft(),
							datum,
						), nil
					}
					return right, nil
				}
			case parser.GE:
				if i == 0 {
					// datum >= tuple[0]
					return right, nil
				}
			case parser.LT:
				if i == len(tuple) {
					// datum > tuple[len(tuple)-1]
					return right, nil
				} else if i == len(tuple)-1 {
					// datum >= tuple[len(tuple)-1]
					if tuple[i].Compare(datum) == 0 {
						// datum == tuple[len(tuple)-1]
						return parser.NewTypedComparisonExpr(
							parser.LE,
							left.TypedLeft(),
							datum,
						), nil
					}
				}
			case parser.LE:
				if i == len(tuple) ||
					(i == len(tuple)-1 && tuple[i].Compare(datum) == 0) {
					// datum >= tuple[len(tuple)-1]
					return right, nil
				}
			}

		case parser.In:
			// We keep the tuples for an IN expression in sorted order. So now we
			// just merge the two sorted lists.
			return parser.NewTypedComparisonExpr(
				parser.In,
				left.TypedLeft(),
				mergeSorted(tuple, *right.Right.(*parser.DTuple)),
			), nil
		}
	}

	return origLeft, origRight
}

func simplifyComparisonExpr(n *parser.ComparisonExpr) (parser.TypedExpr, bool) {
	// NormalizeExpr will have left comparisons in the form "<var> <op>
	// <datum>" unless they could not be simplified further in which case
	// simplifyExpr cannot handle them. For example, "lower(a) = 'foo'"
	left, right := n.TypedLeft(), n.TypedRight()
	if isVar(left) && isDatum(right) {
		if right == parser.DNull {
			switch n.Operator {
			case parser.IsNotDistinctFrom:
				switch left.(type) {
				case *qvalue, *parser.IndexedVar:
					// Transform "a IS NOT DISTINCT FROM NULL" into "a IS NULL".
					return parser.NewTypedComparisonExpr(
						parser.Is,
						left,
						right,
					), true
				}
			case parser.IsDistinctFrom:
				switch left.(type) {
				case *qvalue, *parser.IndexedVar:
					// Transform "a IS DISTINCT FROM NULL" into "a IS NOT NULL".
					return parser.NewTypedComparisonExpr(
						parser.IsNot,
						left,
						right,
					), true
				}
			case parser.Is, parser.IsNot:
				switch left.(type) {
				case *qvalue, *parser.IndexedVar:
					// "a IS {,NOT} NULL" can be used during index selection to restrict
					// the range of scanned keys.
					return n, true
				}
			default:
				// All of the remaining comparison operators have the property that when
				// comparing to NULL they evaluate to NULL (see evalComparisonOp). NULL is
				// not the same as false, but in the context of a WHERE clause, NULL is
				// considered not-true which is the same as false.
				return parser.MakeDBool(false), true
			}
		}

		switch n.Operator {
		case parser.EQ:
			// Translate "(a, b) = (1, 2)" to "(a, b) IN ((1, 2))".
			switch left.(type) {
			case *parser.Tuple:
				return parser.NewTypedComparisonExpr(
					parser.In,
					left,
					&parser.DTuple{right.(parser.Datum)},
				), true
			}
			return n, true
		case parser.NE, parser.GE, parser.LE:
			return n, true
		case parser.GT:
			// This simplification is necessary so that subsequent transformation of
			// > constraint to >= can use Datum.Next without concern about whether a
			// next value exists. Note that if the variable (n.Left) is NULL, this
			// comparison would evaluate to NULL which is equivalent to false for a
			// boolean expression.
			if right.(parser.Datum).IsMax() {
				return parser.MakeDBool(false), true
			}
			return n, true
		case parser.LT:
			// Note that if the variable is NULL, this would evaluate to NULL which
			// would equivalent to false for a boolean expression.
			if right.(parser.Datum).IsMin() {
				return parser.MakeDBool(false), true
			}
			return n, true
		case parser.In, parser.NotIn:
			tuple := *right.(*parser.DTuple)
			if len(tuple) == 0 {
				return parser.MakeDBool(false), true
			}
			return n, true
		case parser.Like:
			// a LIKE 'foo%' -> a >= "foo" AND a < "fop"
			if d, ok := right.(*parser.DString); ok {
				if i := strings.IndexAny(string(*d), "_%"); i >= 0 {
					return makePrefixRange((*d)[:i], left, false), false
				}
				return makePrefixRange(*d, left, true), false
			}
			// TODO(pmattis): Support parser.DBytes?
		case parser.SimilarTo:
			// a SIMILAR TO "foo.*" -> a >= "foo" AND a < "fop"
			if d, ok := right.(*parser.DString); ok {
				pattern := parser.SimilarEscape(string(*d))
				if re, err := regexp.Compile(pattern); err == nil {
					prefix, complete := re.LiteralPrefix()
					return makePrefixRange(parser.DString(prefix), left, complete), false
				}
			}
			// TODO(pmattis): Support parser.DBytes?
		}
	}
	return parser.MakeDBool(true), false
}

func makePrefixRange(prefix parser.DString, datum parser.TypedExpr, complete bool) parser.TypedExpr {
	if complete {
		return parser.NewTypedComparisonExpr(
			parser.EQ,
			datum,
			&prefix,
		)
	}
	if len(prefix) == 0 {
		return parser.MakeDBool(true)
	}
	return parser.NewTypedAndExpr(
		parser.NewTypedComparisonExpr(
			parser.GE,
			datum,
			&prefix,
		),
		parser.NewTypedComparisonExpr(
			parser.LT,
			datum,
			parser.NewDString(string(roachpb.Key(prefix).PrefixEnd())),
		),
	)
}

func mergeSorted(a, b parser.DTuple) *parser.DTuple {
	r := make(parser.DTuple, 0, len(a)+len(b))
	for len(a) > 0 || len(b) > 0 {
		if len(a) == 0 {
			r = append(r, b...)
			break
		}
		if len(b) == 0 {
			r = append(r, a...)
			break
		}
		switch a[0].Compare(b[0]) {
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
	return &r
}

func intersectSorted(a, b parser.DTuple) *parser.DTuple {
	n := len(a)
	if n > len(b) {
		n = len(b)
	}
	r := make(parser.DTuple, 0, n)
	for len(a) > 0 && len(b) > 0 {
		switch a[0].Compare(b[0]) {
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
	return &r
}

func remove(a parser.DTuple, i int) parser.DTuple {
	r := make(parser.DTuple, len(a)-1)
	copy(r, a[:i])
	copy(r[i:], a[i+1:])
	return r
}

func isDatum(e parser.TypedExpr) bool {
	_, ok := e.(parser.Datum)
	return ok
}

// isVar returns true if the expression is a qvalue or a tuple composed of
// qvalues.
func isVar(e parser.TypedExpr) bool {
	switch t := e.(type) {
	case *qvalue, *parser.IndexedVar:
		return true

	case *parser.Tuple:
		for _, v := range t.Exprs {
			if !isVar(v.(parser.TypedExpr)) {
				return false
			}
		}
		return true
	}

	return false
}

// varEqual returns true if the two expressions are both qvalues pointing to
// the same column or are both tuples composed of qvalues pointing at the same
// columns.
func varEqual(a, b parser.TypedExpr) bool {
	switch ta := a.(type) {
	case *qvalue:
		switch tb := b.(type) {
		case *qvalue:
			return ta.colRef == tb.colRef
		}

	case *parser.IndexedVar:
		switch tb := b.(type) {
		case *parser.IndexedVar:
			return ta.Idx == tb.Idx
		}

	case *parser.Tuple:
		switch tb := b.(type) {
		case *parser.Tuple:
			if len(ta.Exprs) == len(tb.Exprs) {
				for i := range ta.Exprs {
					if !varEqual(ta.Exprs[i].(parser.TypedExpr), tb.Exprs[i].(parser.TypedExpr)) {
						return false
					}
				}
				return true
			}
		}
	}

	return false
}

func makeIsNotNull(left parser.TypedExpr) parser.TypedExpr {
	return parser.NewTypedComparisonExpr(
		parser.IsNot,
		left,
		parser.DNull,
	)
}

// analyzeExpr performs semantic analysis of an axpression, including:
// - replacing sub-queries by a sql.subquery node;
// - resolving qnames (optional);
// - type checking (with optional type enforcement);
// - normalization.
// The parameters sources and qvals, if both are non-nil, indicate
// qname resolution should be performed. The qvals map will be filled
// as a result.
func (p *planner) analyzeExpr(
	raw parser.Expr,
	/* arguments for qname resolution */
	sources multiSourceInfo,
	qvals qvalMap,
	/* arguments for type checking */
	expectedType parser.Datum,
	requireType bool,
	typingContext string,
) (parser.TypedExpr, error) {
	// Replace the sub-queries.
	// In all contexts that analyze a single expression, a single value
	// is expected. Tell this to replaceSubqueries.  (See UPDATE for a
	// counter-example; cases where a subquery is an operand of a
	// comparison are handled specially in the subqueryVisitor already.)
	replaced, err := p.replaceSubqueries(raw, 1 /* one value expected */)
	if err != nil {
		return nil, err
	}

	// Perform optional qname resolution.
	var resolved parser.Expr
	if sources == nil || qvals == nil {
		resolved = replaced
	} else {
		resolved, err = resolveQNames(replaced, sources, qvals, &p.qnameVisitor)
		if err != nil {
			return nil, err
		}
	}

	// Type check.
	var typedExpr parser.TypedExpr
	if requireType {
		typedExpr, err = parser.TypeCheckAndRequire(resolved, &p.semaCtx,
			expectedType, typingContext)
	} else {
		typedExpr, err = parser.TypeCheck(resolved, &p.semaCtx, expectedType)
	}
	if err != nil {
		return nil, err
	}

	// Normalize.
	return p.parser.NormalizeExpr(&p.evalCtx, typedExpr)
}
