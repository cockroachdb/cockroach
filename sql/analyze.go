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
func analyzeExpr(e parser.Expr) (exprs []parser.Exprs, equivalent bool) {
	e, equivalent = simplifyExpr(e)
	orExprs := splitOrExpr(e, nil)
	results := make([]parser.Exprs, len(orExprs))
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
func splitOrExpr(e parser.Expr, exprs parser.Exprs) parser.Exprs {
	switch t := e.(type) {
	case *parser.OrExpr:
		return splitOrExpr(t.Right, splitOrExpr(t.Left, exprs))
	}
	return append(exprs, e)
}

// splitAndExpr flattens a tree of AND expressions returning all of the child
// expressions as a list. Any non-AND expression is returned as a single
// element in the list.
//
//   a AND b AND c AND d -> [a, b, c, d]
func splitAndExpr(e parser.Expr, exprs parser.Exprs) parser.Exprs {
	switch t := e.(type) {
	case *parser.AndExpr:
		return splitAndExpr(t.Right, splitAndExpr(t.Left, exprs))
	}
	return append(exprs, e)
}

// joinOrExprs performs the inverse operation of splitOrExpr, joining
// together the individual expressions using OrExpr nodes.
func joinOrExprs(exprs parser.Exprs) parser.Expr {
	return joinExprs(exprs, func(left, right parser.Expr) parser.Expr {
		return &parser.OrExpr{Left: left, Right: right}
	})
}

// joinAndExprs performs the inverse operation of splitAndExpr, joining
// together the individual expressions using AndExpr nodes.
func joinAndExprs(exprs parser.Exprs) parser.Expr {
	return joinExprs(exprs, func(left, right parser.Expr) parser.Expr {
		return &parser.AndExpr{Left: left, Right: right}
	})
}

func joinExprs(exprs parser.Exprs, joinExprsFn func(left, right parser.Expr) parser.Expr) parser.Expr {
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
func simplifyExpr(e parser.Expr) (simplified parser.Expr, equivalent bool) {
	switch t := e.(type) {
	case *parser.NotExpr:
		return simplifyNotExpr(t)
	case *parser.AndExpr:
		return simplifyAndExpr(t)
	case *parser.OrExpr:
		return simplifyOrExpr(t)
	case *parser.ComparisonExpr:
		return simplifyComparisonExpr(t)
	case *qvalue, *scanQValue, parser.DBool:
		return e, true
	}
	// We don't know how to simplify expressions that fall through to here, so
	// consider this part of the expression true.
	return parser.DBool(true), false
}

func simplifyNotExpr(n *parser.NotExpr) (parser.Expr, bool) {
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
		case parser.SimilarTo:
			op = parser.NotSimilarTo
		case parser.NotSimilarTo:
			op = parser.SimilarTo
		default:
			return parser.DBool(true), false
		}
		return simplifyExpr(&parser.ComparisonExpr{
			Operator: op,
			Left:     t.Left,
			Right:    t.Right,
		})

	case *parser.AndExpr:
		// De Morgan's Law: NOT (a AND b) -> (NOT a) OR (NOT b)
		return simplifyExpr(&parser.OrExpr{
			Left:  &parser.NotExpr{Expr: t.Left},
			Right: &parser.NotExpr{Expr: t.Right},
		})

	case *parser.OrExpr:
		// De Morgan's Law: NOT (a OR b) -> (NOT a) AND (NOT b)
		return simplifyExpr(&parser.AndExpr{
			Left:  &parser.NotExpr{Expr: t.Left},
			Right: &parser.NotExpr{Expr: t.Right},
		})
	}
	return parser.DBool(true), false
}

func simplifyAndExpr(n *parser.AndExpr) (parser.Expr, bool) {
	// a AND b AND c AND d -> [a, b, c, d]
	equivalent := true
	exprs := splitAndExpr(n, nil)
	for i := range exprs {
		var equiv bool
		exprs[i], equiv = simplifyExpr(exprs[i])
		if !equiv {
			equivalent = false
		}
		if d, ok := exprs[i].(parser.DBool); ok && !bool(d) {
			return d, equivalent
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
			if d, ok := exprs[j].(parser.DBool); ok && !bool(d) {
				return d, equivalent
			}
			if d, ok := exprs[i].(parser.DBool); ok && bool(d) {
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

func simplifyOneAndExpr(left, right parser.Expr) (parser.Expr, parser.Expr, bool) {
	lcmp, ok := left.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	rcmp, ok := right.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	if !isDatum(lcmp.Right) || !isDatum(rcmp.Right) {
		return parser.DBool(true), nil, false
	}
	if !varEqual(lcmp.Left, rcmp.Left) {
		return left, right, true
	}

	if lcmp.Operator == parser.IsNot || rcmp.Operator == parser.IsNot {
		switch lcmp.Operator {
		case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE, parser.In:
			if rcmp.Right == parser.DNull {
				// a <cmp> x AND a IS NOT NULL
				return left, nil, true
			}
		case parser.Is:
			if lcmp.Right == parser.DNull && rcmp.Right == parser.DNull {
				// a IS NULL AND a IS NOT NULL
				return parser.DBool(false), nil, true
			}
		case parser.IsNot:
			if lcmp.Right == parser.DNull {
				switch rcmp.Operator {
				case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE, parser.In:
					// a IS NOT NULL AND a <cmp> x
					return right, nil, true
				case parser.Is:
					if rcmp.Right == parser.DNull {
						// a IS NOT NULL AND a IS NULL
						return parser.DBool(false), nil, true
					}
				case parser.IsNot:
					if rcmp.Right == parser.DNull {
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

	if reflect.TypeOf(lcmp.Right) != reflect.TypeOf(rcmp.Right) {
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
			if lcmp.Operator == parser.Is && lcmp.Right == parser.DNull {
				// a IS NULL AND a <cmp> x
				return parser.DBool(false), nil, true
			}
			if rcmp.Operator == parser.Is && rcmp.Right == parser.DNull {
				// a <cmp> x AND a IS NULL
				return parser.DBool(false), nil, true
			}
			// Note that "a IS NULL and a IS NULL" cannot happen here because
			// "reflect.TypeOf(NULL) == reflect.TypeOf(NULL)".
			return left, right, true
		}
	}

	ldatum := lcmp.Right.(parser.Datum)
	rdatum := rcmp.Right.(parser.Datum)
	cmp := ldatum.Compare(rdatum)

	// Determine which expression to use when either expression (left or right)
	// is valid as a return value but their types are different. The reason
	// to prefer a comparison between a column value and a datum of the same
	// type is that it makes index constraint construction easier.
	either := lcmp
	if !ldatum.TypeEqual(rdatum) {
		switch ta := lcmp.Left.(type) {
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
			return parser.DBool(false), nil, true
		case parser.NE:
			// a = x AND a != y
			if cmp == 0 {
				// x = y
				return parser.DBool(false), nil, true
			}
			return left, nil, true
		case parser.GT, parser.GE:
			// a = x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == parser.GT) {
				// x < y OR x = y
				return parser.DBool(false), nil, true
			}
			return left, nil, true
		case parser.LT, parser.LE:
			// a = x AND (a < y OR a <= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == parser.LT) {
				// x > y OR x = y
				return parser.DBool(false), nil, true
			}
			return left, nil, true
		}

	case parser.NE:
		switch rcmp.Operator {
		case parser.EQ:
			// a != x AND a = y
			if cmp == 0 {
				// x = y
				return parser.DBool(false), nil, true
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
				return &parser.ComparisonExpr{
					Operator: parser.GT,
					Left:     rcmp.Left,
					Right:    either.Right,
				}, nil, true
			}
			// x != y
			return right, nil, cmp == -1
		case parser.LE:
			// a != x AND a <= y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.LT,
					Left:     rcmp.Left,
					Right:    either.Right,
				}, nil, true
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
				return parser.DBool(false), nil, true
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
			return parser.DBool(false), nil, true
		}

	case parser.GE:
		switch rcmp.Operator {
		case parser.EQ:
			// a >= x AND a = y
			if cmp == 1 {
				// x > y
				return parser.DBool(false), nil, true
			}
			// x <= y
			return right, nil, true
		case parser.NE:
			// a >= x AND x != y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.GT,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
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
			return parser.DBool(false), nil, true
		case parser.LE:
			// a >= x AND a <= y
			if cmp == -1 {
				// x < y
				return left, right, true
			} else if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.EQ,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			}
			// x > y
			return parser.DBool(false), nil, true
		}

	case parser.LT:
		switch rcmp.Operator {
		case parser.EQ:
			// a < x AND a = y
			if cmp != 1 {
				// x <= y
				return parser.DBool(false), nil, true
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
			return parser.DBool(false), nil, true
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
				return parser.DBool(false), nil, true
			}
			// x >= y
			return right, nil, true
		case parser.NE:
			// a <= x AND a != y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.LT,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			}
			// x != y
			return left, nil, cmp == -1
		case parser.GT:
			// a <= x AND a > y
			if cmp == 1 {
				// x > y
				return left, right, true
			}
			return parser.DBool(false), nil, true
		case parser.GE:
			// a <= x AND a >= y
			if cmp == +1 {
				// x > y
				return left, right, true
			} else if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.EQ,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			}
			// x < y
			return parser.DBool(false), nil, true
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
			if lcmp.Right == parser.DNull && rcmp.Right == parser.DNull {
				// a IS NULL AND a IS NULL
				return left, nil, true
			}
		}
	}

	return parser.DBool(true), nil, false
}

func simplifyOneAndInExpr(left, right *parser.ComparisonExpr) (parser.Expr, parser.Expr) {
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
		ltuple := left.Right.(parser.DTuple)
		switch right.Operator {
		case parser.Is:
			if right.Right == parser.DNull {
				return parser.DBool(false), nil
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
				return parser.DBool(false), nil

			case parser.NE:
				if i < len(ltuple) && ltuple[i].Compare(datum) == 0 {
					if len(ltuple) < 2 {
						return parser.DBool(false), nil
					}
					ltuple = remove(ltuple, i)
				}
				return &parser.ComparisonExpr{
					Operator: parser.In,
					Left:     left.Left,
					Right:    ltuple,
				}, nil

			case parser.GT:
				if i < len(ltuple) {
					if ltuple[i].Compare(datum) == 0 {
						ltuple = ltuple[i+1:]
					} else {
						ltuple = ltuple[i:]
					}
					if len(ltuple) > 0 {
						return &parser.ComparisonExpr{
							Operator: parser.In,
							Left:     left.Left,
							Right:    ltuple,
						}, nil
					}
				}
				return parser.DBool(false), nil

			case parser.GE:
				if i < len(ltuple) {
					ltuple = ltuple[i:]
					if len(ltuple) > 0 {
						return &parser.ComparisonExpr{
							Operator: parser.In,
							Left:     left.Left,
							Right:    ltuple,
						}, nil
					}
				}
				return parser.DBool(false), nil

			case parser.LT:
				if i < len(ltuple) {
					if i == 0 {
						return parser.DBool(false), nil
					}
					return &parser.ComparisonExpr{
						Operator: parser.In,
						Left:     left.Left,
						Right:    ltuple[:i],
					}, nil
				}
				return left, nil

			case parser.LE:
				if i < len(ltuple) {
					if ltuple[i].Compare(datum) == 0 {
						i++
					}
					if i == 0 {
						return parser.DBool(false), nil
					}
					return &parser.ComparisonExpr{
						Operator: parser.In,
						Left:     left.Left,
						Right:    ltuple[:i],
					}, nil
				}
				return left, nil
			}

		case parser.In:
			// Both of our tuples are sorted. Intersect the lists.
			rtuple := right.Right.(parser.DTuple)
			intersection := intersectSorted(ltuple, rtuple)
			if len(intersection) == 0 {
				return parser.DBool(false), nil
			}
			return &parser.ComparisonExpr{
				Operator: parser.In,
				Left:     left.Left,
				Right:    intersection,
			}, nil
		}
	}

	return origLeft, origRight
}

func simplifyOrExpr(n *parser.OrExpr) (parser.Expr, bool) {
	// a OR b OR c OR d -> [a, b, c, d]
	equivalent := true
	exprs := splitOrExpr(n, nil)
	for i := range exprs {
		var equiv bool
		exprs[i], equiv = simplifyExpr(exprs[i])
		if !equiv {
			equivalent = false
		}
		if d, ok := exprs[i].(parser.DBool); ok && bool(d) {
			return d, equivalent
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
			if d, ok := exprs[j].(parser.DBool); ok && bool(d) {
				return d, equivalent
			}
			if d, ok := exprs[i].(parser.DBool); ok && !bool(d) {
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

func simplifyOneOrExpr(left, right parser.Expr) (parser.Expr, parser.Expr, bool) {
	lcmp, ok := left.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	rcmp, ok := right.(*parser.ComparisonExpr)
	if !ok {
		return left, right, true
	}
	if !isDatum(lcmp.Right) || !isDatum(rcmp.Right) {
		return parser.DBool(true), nil, false
	}
	if !varEqual(lcmp.Left, rcmp.Left) {
		return left, right, true
	}

	if lcmp.Operator == parser.IsNot || rcmp.Operator == parser.IsNot {
		switch lcmp.Operator {
		case parser.Is:
			if lcmp.Right == parser.DNull && rcmp.Right == parser.DNull {
				// a IS NULL OR a IS NOT NULL
				return parser.DBool(true), nil, true
			}
		case parser.IsNot:
			if lcmp.Right == parser.DNull {
				switch rcmp.Operator {
				case parser.Is:
					if rcmp.Right == parser.DNull {
						// a IS NOT NULL OR a IS NULL
						return parser.DBool(true), nil, true
					}
				case parser.IsNot:
					if rcmp.Right == parser.DNull {
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

	if reflect.TypeOf(lcmp.Right) != reflect.TypeOf(rcmp.Right) {
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

	ldatum := lcmp.Right.(parser.Datum)
	rdatum := rcmp.Right.(parser.Datum)
	cmp := ldatum.Compare(rdatum)

	// Determine which expression to use when either expression (left or right)
	// is valid as a return value but their types are different. The reason
	// to prefer a comparison between a column value and a datum of the same
	// type is that it makes index constraint construction easier.
	either := lcmp
	if !ldatum.TypeEqual(rdatum) {
		switch ta := lcmp.Left.(type) {
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
			return &parser.ComparisonExpr{
				Operator: parser.In,
				Left:     lcmp.Left,
				Right:    parser.DTuple{ldatum, rdatum},
			}, nil, true
		case parser.NE:
			// a = x OR a != y
			if cmp == 0 {
				// x = y
				return makeIsNotNull(lcmp.Left), nil, true
			}
			return right, nil, true
		case parser.GT:
			// a = x OR a > y
			if cmp == 1 {
				// x > y OR x = y
				return right, nil, true
			} else if cmp == 0 {
				return &parser.ComparisonExpr{
					Operator: parser.GE,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
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
				return &parser.ComparisonExpr{
					Operator: parser.LE,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
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
				return makeIsNotNull(lcmp.Left), nil, true
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
			return makeIsNotNull(lcmp.Left), nil, true
		case parser.GT:
			// a != x OR a > y
			if cmp == 1 {
				// x > y
				return makeIsNotNull(lcmp.Left), nil, true
			}
			// x <= y
			return left, nil, true
		case parser.GE:
			// a != x OR a >= y
			if cmp != -1 {
				// x >= y
				return makeIsNotNull(lcmp.Left), nil, true
			}
			// x < y
			return left, nil, true
		case parser.LT:
			// a != x OR a < y
			if cmp == -1 {
				// x < y
				return makeIsNotNull(lcmp.Left), nil, true
			}
			// x >= y
			return left, nil, true
		case parser.LE:
			// a != x OR a <= y
			if cmp != 1 {
				// x <= y
				return makeIsNotNull(lcmp.Left), nil, true
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
				return &parser.ComparisonExpr{
					Operator: parser.GE,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			}
			// x > y
			return left, right, true
		case parser.NE:
			// a > x OR a != y
			if cmp == -1 {
				// x < y
				return makeIsNotNull(lcmp.Left), nil, true
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
				return &parser.ComparisonExpr{
					Operator: parser.NE,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			} else if cmp == -1 {
				return makeIsNotNull(lcmp.Left), nil, true
			}
			// x != y
			return left, right, true
		case parser.LE:
			// a > x OR a <= y
			if cmp != 1 {
				// x = y
				return makeIsNotNull(lcmp.Left), nil, true
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
				return makeIsNotNull(lcmp.Left), nil, true
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
				return makeIsNotNull(lcmp.Left), nil, true
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
				return &parser.ComparisonExpr{
					Operator: parser.LE,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			} else if cmp == 1 {
				// x > y
				return left, nil, true
			}
			// x < y
			return left, right, true
		case parser.NE:
			// a < x OR a != y
			if cmp == 1 {
				return makeIsNotNull(lcmp.Left), nil, true
			}
			return right, nil, true
		case parser.GT:
			// a < x OR a > y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.NE,
					Left:     lcmp.Left,
					Right:    either.Right,
				}, nil, true
			} else if cmp == 1 {
				// x > y
				return makeIsNotNull(lcmp.Left), nil, true
			}
			return left, right, true
		case parser.GE:
			// a < x OR a >= y
			if cmp == -1 {
				// x < y
				return left, right, true
			}
			// x >= y
			return makeIsNotNull(lcmp.Left), nil, true
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
				return makeIsNotNull(lcmp.Left), nil, true
			}
			// x < y
			return right, nil, true
		case parser.GT, parser.GE:
			// a <= x OR (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return makeIsNotNull(lcmp.Left), nil, true
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
			if lcmp.Right == parser.DNull && rcmp.Right == parser.DNull {
				// a IS NULL OR a IS NULL
				return left, nil, true
			}
		}
	}

	return parser.DBool(true), nil, false
}

func simplifyOneOrInExpr(left, right *parser.ComparisonExpr) (parser.Expr, parser.Expr) {
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
		tuple := left.Right.(parser.DTuple)
		switch right.Operator {
		case parser.EQ:
			datum := right.Right.(parser.Datum)
			// We keep the tuples for an IN expression in sorted order. So now we just
			// merge the two sorted lists.
			return &parser.ComparisonExpr{
				Operator: parser.In,
				Left:     left.Left,
				Right:    mergeSorted(tuple, parser.DTuple{datum}),
			}, nil

		case parser.NE, parser.GT, parser.GE, parser.LT, parser.LE:
			datum := right.Right.(parser.Datum)
			i := sort.Search(len(tuple), func(i int) bool {
				return tuple[i].(parser.Datum).Compare(datum) >= 0
			})

			switch right.Operator {
			case parser.NE:
				if i < len(tuple) && tuple[i].Compare(datum) == 0 {
					return makeIsNotNull(right.Left), nil
				}
				return right, nil

			case parser.GT:
				if i == 0 {
					// datum >= tuple[0]
					if tuple[i].Compare(datum) == 0 {
						// datum = tuple[0]
						return &parser.ComparisonExpr{
							Operator: parser.GE,
							Left:     left.Left,
							Right:    datum,
						}, nil
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
						return &parser.ComparisonExpr{
							Operator: parser.LE,
							Left:     left.Left,
							Right:    datum,
						}, nil
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
			return &parser.ComparisonExpr{
				Operator: parser.In,
				Left:     left.Left,
				Right:    mergeSorted(tuple, right.Right.(parser.DTuple)),
			}, nil
		}
	}

	return origLeft, origRight
}

func simplifyComparisonExpr(n *parser.ComparisonExpr) (parser.Expr, bool) {
	// NormalizeExpr will have left comparisons in the form "<var> <op>
	// <datum>" unless they could not be simplified further in which case
	// simplifyExpr cannot handle them. For example, "lower(a) = 'foo'"
	if isVar(n.Left) && isDatum(n.Right) {
		if n.Right == parser.DNull {
			switch n.Operator {
			case parser.IsNotDistinctFrom:
				switch n.Left.(type) {
				case *qvalue, *scanQValue:
					// Transform "a IS NOT DISTINCT FROM NULL" into "a IS NULL".
					return &parser.ComparisonExpr{
						Operator: parser.Is,
						Left:     n.Left,
						Right:    n.Right,
					}, true
				}
			case parser.IsDistinctFrom:
				switch n.Left.(type) {
				case *qvalue, *scanQValue:
					// Transform "a IS DISTINCT FROM NULL" into "a IS NOT NULL".
					return &parser.ComparisonExpr{
						Operator: parser.IsNot,
						Left:     n.Left,
						Right:    n.Right,
					}, true
				}
			case parser.Is, parser.IsNot:
				switch n.Left.(type) {
				case *qvalue, *scanQValue:
					// "a IS {,NOT} NULL" can be used during index selection to restrict
					// the range of scanned keys.
					return n, true
				}
			default:
				// All of the remaining comparison operators have the property that when
				// comparing to NULL they evaluate to NULL (see evalComparisonOp). NULL is
				// not the same as false, but in the context of a WHERE clause, NULL is
				// considered not-true which is the same as false.
				return parser.DBool(false), true
			}
		}

		switch n.Operator {
		case parser.EQ:
			// Translate "(a, b) = (1, 2)" to "(a, b) IN ((1, 2))".
			switch n.Left.(type) {
			case *parser.Tuple:
				return &parser.ComparisonExpr{
					Operator: parser.In,
					Left:     n.Left,
					Right:    parser.DTuple{n.Right.(parser.Datum)},
				}, true
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
			if n.Right.(parser.Datum).IsMax() {
				return parser.DBool(false), true
			}
			return n, true
		case parser.LT:
			// Note that if the variable is NULL, this would evaluate to NULL which
			// would equivalent to false for a boolean expression.
			if n.Right.(parser.Datum).IsMin() {
				return parser.DBool(false), true
			}
			return n, true
		case parser.In, parser.NotIn:
			tuple := n.Right.(parser.DTuple)
			if len(tuple) == 0 {
				return parser.DBool(false), true
			}
			n.Right = tuple
			return n, true
		case parser.Like:
			// a LIKE 'foo%' -> a >= "foo" AND a < "fop"
			if d, ok := n.Right.(parser.DString); ok {
				if i := strings.IndexAny(string(d), "_%"); i >= 0 {
					return makePrefixRange(d[:i], n.Left, false), false
				}
				return makePrefixRange(d, n.Left, true), false
			}
			// TODO(pmattis): Support parser.DBytes?
		case parser.SimilarTo:
			// a SIMILAR TO "foo.*" -> a >= "foo" AND a < "fop"
			if d, ok := n.Right.(parser.DString); ok {
				pattern := parser.SimilarEscape(string(d))
				if re, err := regexp.Compile(pattern); err == nil {
					prefix, complete := re.LiteralPrefix()
					return makePrefixRange(parser.DString(prefix), n.Left, complete), false
				}
			}
			// TODO(pmattis): Support parser.DBytes?
		}
	}
	return parser.DBool(true), false
}

func makePrefixRange(prefix parser.DString, datum parser.Expr, complete bool) parser.Expr {
	if complete {
		return &parser.ComparisonExpr{
			Operator: parser.EQ,
			Left:     datum,
			Right:    prefix,
		}
	}
	if len(prefix) == 0 {
		return parser.DBool(true)
	}
	return &parser.AndExpr{
		Left: &parser.ComparisonExpr{
			Operator: parser.GE,
			Left:     datum,
			Right:    prefix,
		},
		Right: &parser.ComparisonExpr{
			Operator: parser.LT,
			Left:     datum,
			Right:    parser.DString(roachpb.Key(prefix).PrefixEnd()),
		},
	}
}

func mergeSorted(a, b parser.DTuple) parser.DTuple {
	r := make(parser.DTuple, 0, len(a)+len(b))
	for len(a) > 0 || len(b) > 0 {
		if len(a) == 0 {
			return append(r, b...)
		}
		if len(b) == 0 {
			return append(r, a...)
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
	return r
}

func intersectSorted(a, b parser.DTuple) parser.DTuple {
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
	return r
}

func remove(a parser.DTuple, i int) parser.DTuple {
	r := make(parser.DTuple, len(a)-1)
	copy(r, a[:i])
	copy(r[i:], a[i+1:])
	return r
}

func isDatum(e parser.Expr) bool {
	_, ok := e.(parser.Datum)
	return ok
}

// isVar returns true if the expression is a qvalue or a tuple composed of
// qvalues.
func isVar(e parser.Expr) bool {
	switch t := e.(type) {
	case *qvalue, *scanQValue:
		return true

	case *parser.Tuple:
		for _, v := range t.Exprs {
			if !isVar(v) {
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
func varEqual(a, b parser.Expr) bool {
	switch ta := a.(type) {
	case *qvalue:
		switch tb := b.(type) {
		case *qvalue:
			return ta.colRef == tb.colRef
		}

	case *scanQValue:
		switch tb := b.(type) {
		case *scanQValue:
			return ta.colIdx == tb.colIdx
		}

	case *parser.Tuple:
		switch tb := b.(type) {
		case *parser.Tuple:
			if len(ta.Exprs) == len(tb.Exprs) {
				for i := range ta.Exprs {
					if !varEqual(ta.Exprs[i], tb.Exprs[i]) {
						return false
					}
				}
				return true
			}
		}
	}

	return false
}

func makeIsNotNull(left parser.Expr) parser.Expr {
	return &parser.ComparisonExpr{
		Operator: parser.IsNot,
		Left:     left,
		Right:    parser.DNull,
	}
}
