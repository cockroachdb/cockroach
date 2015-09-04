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

package sql

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// analyzeExpr analyzes and simplifies an expression, returning a list of
// expressions of a list of expressions. The top-level list contains
// disjunctions (a.k.a OR expressions). The second-level contains conjuntions
// (a.k.a. AND expressions). For example:
//
//   (a AND b) OR c -> [[a, b], c]
func analyzeExpr(e parser.Expr) []parser.Exprs {
	e = simplifyExpr(e)
	orExprs := splitOrExpr(e, nil)
	results := make([]parser.Exprs, len(orExprs))
	for i := range orExprs {
		results[i] = splitAndExpr(orExprs[i], nil)
	}
	return results
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
func simplifyExpr(e parser.Expr) parser.Expr {
	switch t := e.(type) {
	case *parser.NotExpr:
		return simplifyNotExpr(t)
	case *parser.AndExpr:
		return simplifyAndExpr(t)
	case *parser.OrExpr:
		return simplifyOrExpr(t)
	case *parser.ComparisonExpr:
		return simplifyComparisonExpr(t)
	case *qvalue:
		return e
	}
	// We don't know how to simplify expressions that fall through to here, so
	// consider this part of the expression true.
	return parser.DBool(true)
}

func simplifyNotExpr(n *parser.NotExpr) parser.Expr {
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
			return parser.DBool(true)
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
	return parser.DBool(true)
}

func simplifyAndExpr(n *parser.AndExpr) parser.Expr {
	// a AND b AND c AND d -> [a, b, c, d]
	exprs := splitAndExpr(n, nil)
	for i := range exprs {
		exprs[i] = simplifyExpr(exprs[i])
		if d, ok := exprs[i].(parser.DBool); ok && !bool(d) {
			return d
		}
	}

	// Loop over the expressions looking for simplifications.
	//
	// TODO(pmattis): This is O(n^2) in the number of expressions. Could be
	// optimized by sorting the expressions based on the variables they contain.
outer:
	for i := len(exprs) - 1; i >= 0; i-- {
		for j := i - 1; j >= 0; j-- {
			exprs[j], exprs[i] = simplifyOneAndExpr(exprs[j], exprs[i])
			if d, ok := exprs[j].(parser.DBool); ok && !bool(d) {
				return d
			}
			if d, ok := exprs[i].(parser.DBool); ok && bool(d) {
				exprs[i] = nil
			}
			if exprs[i] == nil {
				// We found a simplification. Strip off the expression that is now nil
				// and continue the outer loop.
				exprs = exprs[:len(exprs)-1]
				continue outer
			}
		}
	}

	// Reform the AND expressions.
	if len(exprs) == 1 {
		return exprs[0]
	}
	a := &parser.AndExpr{Left: exprs[len(exprs)-2], Right: exprs[len(exprs)-1]}
	for i := len(exprs) - 3; i >= 0; i-- {
		a = &parser.AndExpr{Left: exprs[i], Right: a}
	}
	return a
}

func simplifyOneAndExpr(left, right parser.Expr) (parser.Expr, parser.Expr) {
	lcmp, ok := left.(*parser.ComparisonExpr)
	if !ok {
		return left, right
	}
	rcmp, ok := right.(*parser.ComparisonExpr)
	if !ok {
		return left, right
	}
	if !isDatum(lcmp.Right) || !isDatum(rcmp.Right) {
		return parser.DBool(true), nil
	}
	if !varEqual(lcmp.Left, rcmp.Left) {
		return left, right
	}
	if lcmp.Operator == parser.In || rcmp.Operator == parser.In {
		return simplifyOneAndInExpr(lcmp, rcmp)
	}

	if reflect.TypeOf(lcmp.Right) != reflect.TypeOf(rcmp.Right) {
		switch lcmp.Operator {
		case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE:
			switch rcmp.Operator {
			case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE:
				return parser.DBool(false), nil
			}
		}
		return left, right
	}

	ldatum := lcmp.Right.(parser.Datum)
	rdatum := rcmp.Right.(parser.Datum)
	cmp := ldatum.Compare(rdatum)

	// TODO(pmattis): Figure out how to generate this logic.
	switch lcmp.Operator {
	case parser.EQ:
		switch rcmp.Operator {
		case parser.EQ:
			// a = x AND a = y
			if cmp == 0 {
				// x = y
				return left, nil
			}
			return parser.DBool(false), nil
		case parser.NE:
			// a = x AND a != y
			if cmp == 0 {
				// x = y
				return parser.DBool(false), nil
			}
			return left, nil
		case parser.GT, parser.GE:
			// a = x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == parser.GT) {
				// x < y OR x = y
				return parser.DBool(false), nil
			}
			return left, nil
		case parser.LT, parser.LE:
			// a = x AND (a < y OR a <= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == parser.LT) {
				// x > y OR x = y
				return parser.DBool(false), nil
			}
			return left, nil
		}

	case parser.NE:
		switch rcmp.Operator {
		case parser.EQ:
			// a != x AND a = y
			if cmp == 0 {
				// x = y
				return parser.DBool(false), nil
			}
			return right, nil
		case parser.NE:
			// a != x AND a != y
			if cmp == 0 {
				// x = y
				return left, nil
			}
			return left, right
		case parser.GT, parser.LT:
			// a != x AND (a > y OR a < y)
			return right, nil
		case parser.GE:
			// a != x AND a >= y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.GT,
					Left:     rcmp.Left,
					Right:    rcmp.Right,
				}, nil
			}
			// x != y
			return right, nil
		case parser.LE:
			// a != x AND a <= y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.LT,
					Left:     rcmp.Left,
					Right:    rcmp.Right,
				}, nil
			}
			// x != y
			return right, nil
		}

	case parser.GT:
		switch rcmp.Operator {
		case parser.EQ:
			// a > x AND a = y
			if cmp != -1 {
				// x >= y
				return parser.DBool(false), nil
			}
			// x < y
			return right, nil
		case parser.NE:
			// a > x AND a != y
			return left, nil
		case parser.GT, parser.GE:
			// a > x AND (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return left, nil
			}
			// x < y
			return right, nil
		case parser.LT, parser.LE:
			// a > x AND (a < y OR a <= y)
			if cmp == -1 {
				// x < y
				return left, right
			}
			// x >= y
			return parser.DBool(false), nil
		}

	case parser.GE:
		switch rcmp.Operator {
		case parser.EQ:
			// a >= x AND a = y
			if cmp == 1 {
				// x > y
				return parser.DBool(false), nil
			}
			// x <= y
			return right, nil
		case parser.NE:
			// a >= x AND x != y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.GT,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			// x != y
			return left, nil
		case parser.GT, parser.GE:
			// a >= x AND (a > y OR a >= y)
			if cmp == -1 || (cmp == 0 && rcmp.Operator == parser.GT) {
				// x < y
				return right, nil
			}
			// x >= y
			return left, nil
		case parser.LT:
			// a >= x AND a < y
			if cmp == -1 {
				// x < y
				return left, right
			}
			// x >= y
			return parser.DBool(false), nil
		case parser.LE:
			// a >= x AND a <= y
			if cmp == -1 {
				// x < y
				return left, right
			} else if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.EQ,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			// x > y
			return parser.DBool(false), nil
		}

	case parser.LT:
		switch rcmp.Operator {
		case parser.EQ:
			// a < x AND a = y
			if cmp != 1 {
				// x <= y
				return parser.DBool(false), nil
			}
			// x > y
			return right, nil
		case parser.NE:
			// a < x AND a != y
			return left, nil
		case parser.GT, parser.GE:
			// a < x AND (a > y OR a >= y)
			if cmp == 1 {
				// x > y
				return left, right
			}
			// x <= y
			return parser.DBool(false), nil
		case parser.LT, parser.LE:
			// a < x AND (a < y OR a <= y)
			if cmp != 1 {
				// x <= y
				return left, nil
			}
			// x > y
			return right, nil
		}

	case parser.LE:
		switch rcmp.Operator {
		case parser.EQ:
			// a <= x AND a = y
			if cmp == -1 {
				// x < y
				return parser.DBool(false), nil
			}
			// x >= y
			return right, nil
		case parser.NE:
			// a <= x AND a != y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.LT,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			// x != y
			return left, nil
		case parser.GT:
			// a <= x AND a > y
			if cmp == 1 {
				// x > y
				return left, right
			}
			return parser.DBool(false), nil
		case parser.GE:
			// a <= x AND a >= y
			if cmp == +1 {
				// x > y
				return left, right
			} else if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.EQ,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			// x < y
			return parser.DBool(false), nil
		case parser.LT, parser.LE:
			// a <= x AND (a > y OR a >= y)
			if cmp == 1 || (cmp == 0 && rcmp.Operator == parser.LT) {
				// x > y
				return right, nil
			}
			// x <= y
			return left, nil
		}
	}

	return parser.DBool(true), nil
}

func simplifyOneAndInExpr(left, right *parser.ComparisonExpr) (parser.Expr, parser.Expr) {
	if left.Operator != parser.In && right.Operator != parser.In {
		panic(fmt.Sprintf("IN expression required: %s vs %s", left, right))
	}

	switch left.Operator {
	case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE:
		switch right.Operator {
		case parser.In:
			left, right = right, left
		}
		fallthrough

	case parser.In:
		ltuple := left.Right.(parser.DTuple)
		switch right.Operator {
		case parser.EQ, parser.GT, parser.GE, parser.LT, parser.LE:
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
			// Both of our tuples are sorted. Interesect the lists.
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

	return left, right
}

func simplifyOrExpr(n *parser.OrExpr) parser.Expr {
	// a OR b OR c OR d -> [a, b, c, d]
	exprs := splitOrExpr(n, nil)
	for i := range exprs {
		exprs[i] = simplifyExpr(exprs[i])
		if d, ok := exprs[i].(parser.DBool); ok && bool(d) {
			return d
		}
	}

	// Loop over the expressions looking for simplifications.
	//
	// TODO(pmattis): This is O(n^2) in the number of expressions. Could be
	// optimized by sorting the expressions based on the variables they contain.
outer:
	for i := len(exprs) - 1; i >= 0; i-- {
		for j := i - 1; j >= 0; j-- {
			exprs[j], exprs[i] = simplifyOneOrExpr(exprs[j], exprs[i])
			if d, ok := exprs[j].(parser.DBool); ok && bool(d) {
				return d
			}
			if d, ok := exprs[i].(parser.DBool); ok && !bool(d) {
				exprs[i] = nil
			}
			if exprs[i] == nil {
				// We found a simplification. Strip off the expression that is now nil
				// and continue the outer loop.
				exprs = exprs[:len(exprs)-1]
				continue outer
			}
		}
	}

	// Reform the OR expressions.
	if len(exprs) == 1 {
		return exprs[0]
	}
	a := &parser.OrExpr{Left: exprs[len(exprs)-2], Right: exprs[len(exprs)-1]}
	for i := len(exprs) - 3; i >= 0; i-- {
		a = &parser.OrExpr{Left: exprs[i], Right: a}
	}
	return a
}

func simplifyOneOrExpr(left, right parser.Expr) (parser.Expr, parser.Expr) {
	lcmp, ok := left.(*parser.ComparisonExpr)
	if !ok {
		return left, right
	}
	rcmp, ok := right.(*parser.ComparisonExpr)
	if !ok {
		return left, right
	}
	if !isDatum(lcmp.Right) || !isDatum(rcmp.Right) {
		return parser.DBool(true), nil
	}
	if !varEqual(lcmp.Left, rcmp.Left) {
		return left, right
	}
	if lcmp.Operator == parser.In || rcmp.Operator == parser.In {
		return simplifyOneOrInExpr(lcmp, rcmp)
	}

	if reflect.TypeOf(lcmp.Right) != reflect.TypeOf(rcmp.Right) {
		// If the types of the left and right datums are different, no
		// simplification is possible.
		return left, right
	}

	ldatum := lcmp.Right.(parser.Datum)
	rdatum := rcmp.Right.(parser.Datum)
	cmp := ldatum.Compare(rdatum)

	// TODO(pmattis): Figure out how to generate this logic.
	switch lcmp.Operator {
	case parser.EQ:
		switch rcmp.Operator {
		case parser.EQ:
			// a = x OR a = y
			if cmp == 0 {
				// x = y
				return left, nil
			} else if cmp == 1 {
				// x > y
				ldatum, rdatum = rdatum, ldatum
			}
			return &parser.ComparisonExpr{
				Operator: parser.In,
				Left:     lcmp.Left,
				Right:    parser.DTuple{ldatum, rdatum},
			}, nil
		case parser.NE:
			// a = x OR a != y
			if cmp == 0 {
				// x = y
				return parser.DBool(true), nil
			}
			return right, nil
		case parser.GT:
			// a = x OR a > y
			if cmp == 1 {
				// x > y OR x = y
				return right, nil
			} else if cmp == 0 {
				return &parser.ComparisonExpr{
					Operator: parser.GE,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			return left, right
		case parser.GE:
			// a = x OR a >= y
			if cmp != -1 {
				// x >= y
				return right, nil
			}
			return left, right
		case parser.LT:
			// a = x OR a < y
			if cmp == -1 {
				// x < y OR x = y
				return right, nil
			} else if cmp == 0 {
				return &parser.ComparisonExpr{
					Operator: parser.LE,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			return left, right
		case parser.LE:
			// a = x OR a <= y
			if cmp != 1 {
				// x <= y
				return right, nil
			}
			return left, right
		}

	case parser.NE:
		switch rcmp.Operator {
		case parser.EQ:
			// a != x OR a = y
			if cmp == 0 {
				// x = y
				return parser.DBool(true), nil
			}
			// x != y
			return left, nil
		case parser.NE:
			// a != x OR a != y
			if cmp == 0 {
				// x = y
				return left, nil
			}
			// x != y
			return parser.DBool(true), nil
		case parser.GT:
			// a != x OR a > y
			if cmp == 1 {
				// x > y
				return parser.DBool(true), nil
			}
			// x <= y
			return left, nil
		case parser.GE:
			// a != x OR a >= y
			if cmp != -1 {
				// x >= y
				return parser.DBool(true), nil
			}
			// x < y
			return left, nil
		case parser.LT:
			// a != x OR a < y
			if cmp == -1 {
				// x < y
				return parser.DBool(true), nil
			}
			// x >= y
			return left, nil
		case parser.LE:
			// a != x OR a <= y
			if cmp != 1 {
				// x <= y
				return parser.DBool(true), nil
			}
			// x > y
			return left, nil
		}

	case parser.GT:
		switch rcmp.Operator {
		case parser.EQ:
			// a > x OR a = y
			if cmp == -1 {
				// x < y
				return left, nil
			} else if cmp == 0 {
				return &parser.ComparisonExpr{
					Operator: parser.GE,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			}
			// x > y
			return left, right
		case parser.NE:
			// a > x OR a != y
			if cmp == -1 {
				// x < y
				return parser.DBool(true), nil
			}
			// x >= y
			return right, nil
		case parser.GT, parser.GE:
			// a > x OR (a > y OR a >= y)
			if cmp == -1 {
				return left, nil
			}
			return right, nil
		case parser.LT:
			// a > x OR a < y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.NE,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			} else if cmp == -1 {
				return parser.DBool(true), nil
			}
			// x != y
			return left, right
		case parser.LE:
			// a > x OR a <= y
			if cmp != 1 {
				// x = y
				return parser.DBool(true), nil
			}
			// x != y
			return left, right
		}

	case parser.GE:
		switch rcmp.Operator {
		case parser.EQ:
			// a >= x OR a = y
			if cmp != 1 {
				// x >. y
				return left, nil
			}
			// x < y
			return left, right
		case parser.NE:
			// a >= x OR a != y
			if cmp != 1 {
				// x <= y
				return parser.DBool(true), nil
			}
			// x > y
			return right, nil
		case parser.GT:
			// a >= x OR a > y
			if cmp != 1 {
				// x <= y
				return left, nil
			}
			// x > y
			return right, nil
		case parser.GE:
			// a >= x OR a >= y
			if cmp == -1 {
				// x < y
				return left, nil
			}
			// x >= y
			return right, nil
		case parser.LT, parser.LE:
			// a >= x OR a < y
			if cmp != 1 {
				// x <= y
				return parser.DBool(true), nil
			}
			// x > y
			return left, right
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
					Right:    lcmp.Right,
				}, nil
			} else if cmp == 1 {
				// x > y
				return left, nil
			}
			// x < y
			return left, right
		case parser.NE:
			// a < x OR a != y
			if cmp == 1 {
				return parser.DBool(true), nil
			}
			return right, nil
		case parser.GT:
			// a < x OR a > y
			if cmp == 0 {
				// x = y
				return &parser.ComparisonExpr{
					Operator: parser.NE,
					Left:     lcmp.Left,
					Right:    lcmp.Right,
				}, nil
			} else if cmp == 1 {
				// x > y
				return parser.DBool(true), nil
			}
			return left, right
		case parser.GE:
			// a < x OR a >= y
			if cmp == -1 {
				// x < y
				return left, right
			}
			// x >= y
			return parser.DBool(true), nil
		case parser.LT, parser.LE:
			// a < x OR (a < y OR a <= y)
			if cmp == 1 {
				// x > y
				return left, nil
			}
			// x < y
			return right, nil
		}

	case parser.LE:
		switch rcmp.Operator {
		case parser.EQ:
			// a <= x OR a = y
			if cmp == -1 {
				// x < y
				return left, right
			}
			// x >= y
			return left, nil
		case parser.NE:
			// a <= x OR a != y
			if cmp != -1 {
				// x >= y
				return parser.DBool(true), nil
			}
			// x < y
			return right, nil
		case parser.GT, parser.GE:
			// a <= x OR (a > y OR a >= y)
			if cmp != -1 {
				// x >= y
				return parser.DBool(true), nil
			}
			// x < y
			return left, right
		case parser.LT, parser.LE:
			// a <= x OR a < y
			if cmp == -1 {
				// x < y
				return right, nil
			}
			// x >= y
			return left, nil
		}
	}

	return parser.DBool(true), nil
}

func simplifyOneOrInExpr(left, right *parser.ComparisonExpr) (parser.Expr, parser.Expr) {
	if left.Operator != parser.In && right.Operator != parser.In {
		panic(fmt.Sprintf("IN expression required: %s vs %s", left, right))
	}

	switch left.Operator {
	case parser.EQ:
		switch right.Operator {
		case parser.In:
			left, right = right, left
		}
		fallthrough

	case parser.In:
		tuple := left.Right.(parser.DTuple)

		var tuple2 parser.DTuple
		switch right.Operator {
		case parser.EQ:
			rdatum := right.Right.(parser.Datum)
			tuple2 = parser.DTuple{rdatum}
		case parser.In:
			tuple2 = right.Right.(parser.DTuple)
		}

		// We keep the tuples for an in expression in sorted order. So now we just
		// merge the two sorted lists.
		return &parser.ComparisonExpr{
			Operator: parser.In,
			Left:     left.Left,
			Right:    mergeSorted(tuple, tuple2),
		}, nil
	}

	return left, right
}

func simplifyComparisonExpr(n *parser.ComparisonExpr) parser.Expr {
	// NormalizeExpr will have left comparisons in the form "<var> <op>
	// <datum>" unless they could not be simplified further in which case
	// simplifyExpr cannot handle them. For example, "lower(a) = 'foo'"
	if isVar(n.Left) && isDatum(n.Right) {
		// All of the comparison operators have the property that when comparing to
		// NULL they evaulate to NULL (see evalComparisonOp). NULL is not the same
		// as false, but in the context of a WHERE clause, NULL is considered
		// not-true which is the same as false.
		if n.Right == parser.DNull {
			return parser.DBool(false)
		}

		switch n.Operator {
		case parser.EQ, parser.NE, parser.GE, parser.LE:
			return n
		case parser.GT:
			// This simplification is necessary so that subsequent transformation of
			// > constraint to >= can use Datum.Next without concern about whether a
			// next value exists. Note that if the variable (n.Left) is NULL, this
			// comparison would evaluate to NULL which is equivalent to false for a
			// boolean expression.
			if n.Right.(parser.Datum).IsMax() {
				return parser.DBool(false)
			}
			return n
		case parser.LT:
			// Note that if the variable is NULL, this would evaluate to NULL which
			// would equivalent to false for a boolean expression.
			if n.Right.(parser.Datum).IsMin() {
				return parser.DBool(false)
			}
			return n
		case parser.In, parser.NotIn:
			tuple := n.Right.(parser.DTuple)
			sort.Sort(tuple)
			tuple = uniqTuple(tuple)
			if len(tuple) == 0 {
				return parser.DBool(false)
			}
			n.Right = tuple
			return n
		case parser.Like:
			// a LIKE 'foo%' -> a >= "foo" AND a < "fop"
			if d, ok := n.Right.(parser.DString); ok {
				if i := strings.IndexAny(string(d), "_%"); i >= 0 {
					return makePrefixRange(d[:i], n.Left, false)
				}
				return makePrefixRange(d, n.Left, true)
			}
		case parser.SimilarTo:
			// a SIMILAR TO "foo.*" -> a >= "foo" AND a < "fop"
			if d, ok := n.Right.(parser.DString); ok {
				if re, err := regexp.Compile(string(d)); err == nil {
					prefix, complete := re.LiteralPrefix()
					return makePrefixRange(parser.DString(prefix), n.Left, complete)
				}
			}
		}
	}
	return parser.DBool(true)
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
			Right:    parser.DString(proto.Key(prefix).PrefixEnd()),
		},
	}
}

func uniqTuple(tuple parser.DTuple) parser.DTuple {
	n := 0
	for i := 0; i < len(tuple); i++ {
		if tuple[i] == parser.DNull {
			continue
		}
		if n == 0 || tuple[n-1].Compare(tuple[i]) < 0 {
			tuple[n] = tuple[i]
			n++
		}
	}
	return tuple[:n]
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

func isDatum(e parser.Expr) bool {
	_, ok := e.(parser.Datum)
	return ok
}

// isVar returns true if the expression is a qvalue or a tuple composed of
// qvalues.
func isVar(e parser.Expr) bool {
	switch t := e.(type) {
	case *qvalue:
		return true

	case parser.Tuple:
		for _, v := range t {
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
			return ta.col.ID == tb.col.ID
		}

	case parser.Tuple:
		switch tb := b.(type) {
		case parser.Tuple:
			if len(ta) == len(tb) {
				for i := range ta {
					if !varEqual(ta[i], tb[i]) {
						return false
					}
				}
				return true
			}
		}
	}

	return false
}

func findColumnInTuple(tuple parser.Tuple, colID ColumnID) int {
	for i, v := range tuple {
		if q, ok := v.(*qvalue); ok && q.col.ID == colID {
			return i
		}
	}
	return -1
}
