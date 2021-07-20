// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// ConcatLeftDeepAnds concatenates any left-deep And expressions in the right
// expression with any left-deep And expressions in the left expression. The
// result is a combined left-deep And expression. Note that NormalizeNestedAnds
// has already guaranteed that both inputs will already be left-deep.
func (c *CustomFuncs) ConcatLeftDeepAnds(left, right opt.ScalarExpr) opt.ScalarExpr {
	if and, ok := right.(*memo.AndExpr); ok {
		return c.f.ConstructAnd(c.ConcatLeftDeepAnds(left, and.Left), and.Right)
	}
	return c.f.ConstructAnd(left, right)
}

// NegateComparison negates a comparison op like:
//   a.x = 5
// to:
//   a.x <> 5
func (c *CustomFuncs) NegateComparison(
	cmp opt.Operator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	negate := opt.NegateOpMap[cmp]
	return c.f.DynamicConstruct(negate, left, right).(opt.ScalarExpr)
}

// FindRedundantConjunct takes the left and right operands of an Or operator as
// input. It examines each conjunct from the left expression and determines
// whether it appears as a conjunct in the right expression. If so, it returns
// the matching conjunct. Otherwise, it returns ok=false. For example:
//
//   A OR A                               =>  A
//   B OR A                               =>  nil
//   A OR (A AND B)                       =>  A
//   (A AND B) OR (A AND C)               =>  A
//   (A AND B AND C) OR (A AND (D OR E))  =>  A
//
// Once a redundant conjunct has been found, it is extracted via a call to the
// ExtractRedundantConjunct function. Redundant conjuncts are extracted from
// multiple nested Or operators by repeated application of these functions.
func (c *CustomFuncs) FindRedundantConjunct(
	left, right opt.ScalarExpr,
) (_ opt.ScalarExpr, ok bool) {
	// Recurse over each conjunct from the left expression and determine whether
	// it's redundant.
	for {
		// Assume a left-deep And expression tree normalized by NormalizeNestedAnds.
		if and, ok := left.(*memo.AndExpr); ok {
			if c.isConjunct(and.Right, right) {
				return and.Right, true
			}
			left = and.Left
		} else {
			if c.isConjunct(left, right) {
				return left, true
			}
			return nil, false
		}
	}
}

// isConjunct returns true if the candidate expression is a conjunct within the
// given conjunction. The conjunction is assumed to be left-deep (normalized by
// the NormalizeNestedAnds rule).
func (c *CustomFuncs) isConjunct(candidate, conjunction opt.ScalarExpr) bool {
	for {
		if and, ok := conjunction.(*memo.AndExpr); ok {
			if and.Right == candidate {
				return true
			}
			conjunction = and.Left
		} else {
			return conjunction == candidate
		}
	}
}

// ExtractRedundantConjunct extracts a redundant conjunct from an Or expression,
// and returns an And of the conjunct with the remaining Or expression (a
// logically equivalent expression). For example:
//
//   (A AND B) OR (A AND C)  =>  A AND (B OR C)
//
// If extracting the conjunct from one of the OR conditions would result in an
// empty condition, the conjunct itself is returned (a logically equivalent
// expression). For example:
//
//   A OR (A AND B)  =>  A
//
// These transformations are useful for finding a conjunct that can be pushed
// down in the query tree. For example, if the redundant conjunct A is fully
// bound by one side of a join, it can be pushed through the join, even if B and
// C cannot.
func (c *CustomFuncs) ExtractRedundantConjunct(
	conjunct, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	if conjunct == left || conjunct == right {
		return conjunct
	}

	return c.f.ConstructAnd(
		conjunct,
		c.f.ConstructOr(
			c.extractConjunct(conjunct, left.(*memo.AndExpr)),
			c.extractConjunct(conjunct, right.(*memo.AndExpr)),
		),
	)
}

// extractConjunct traverses the And subtree looking for the given conjunct,
// which must be present. Once it's located, it's removed from the tree, and
// the remaining expression is returned.
func (c *CustomFuncs) extractConjunct(conjunct opt.ScalarExpr, and *memo.AndExpr) opt.ScalarExpr {
	if and.Right == conjunct {
		return and.Left
	}
	if and.Left == conjunct {
		return and.Right
	}
	return c.f.ConstructAnd(c.extractConjunct(conjunct, and.Left.(*memo.AndExpr)), and.Right)
}
