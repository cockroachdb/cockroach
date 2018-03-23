// Copyright 2018 The Cockroach Authors.
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

package memo

import (
	"fmt"
	"math"
)

// GroupID identifies a memo group. Groups have numbers greater than 0; a
// GroupID of 0 indicates an invalid group.
type GroupID uint32

// bestOrdinal is the ordinal position of a BestExpr within its memo group.
// Once optimized, each group manages one or more bestExprs that track the
// lowest cost expression for a particular set of required physical properties.
type bestOrdinal uint16

const (
	// normBestOrdinal is a special reserved ordinal that is used by ExprView
	// to indicate that the normalized tree should be traversed rather than the
	// lowest cost expression tree.
	normBestOrdinal bestOrdinal = math.MaxUint16
)

// group stores a set of logically equivalent expressions. See the comments
// on Expr for the definition of logical equivalency. In addition, for each
// required set of physical properties, the group stores the expression that
// provides those properties at the lowest known cost in the bestExprs fields.
type group struct {
	// id is the index of this group within the memo.
	id GroupID

	// logical is the set of logical properties that all memo expressions in
	// the group share.
	logical LogicalProps

	// normExpr holds the first expression in the group, which is always the
	// group's normalized expression. otherExprs holds any expressions beyond
	// the first. These are separated in order to optimize for the common case
	// of a group with only one expression (often true for scalar expressions).
	normExpr   Expr
	otherExprs []Expr

	// firstBestExpr remembers the lowest cost expression in the group that
	// provides a particular set of physical properties. If the group has been
	// optimized with respect to more than the one set of properties, then
	// otherBestExprs remembers the lowest cost expressions for the others.
	// These fields are separated in order to optimize for the common case of a
	// group that has been optimized for only one set of properties.
	firstBestExpr  BestExpr
	otherBestExprs []BestExpr
}

// makeMemoGroup constructs a new memo group with the given id and its
// normalized expression.
func makeMemoGroup(id GroupID, norm Expr) group {
	return group{id: id, normExpr: norm}
}

// exprCount returns the number of logically-equivalent expressions in the
// group.
func (g *group) exprCount() int {
	// Group always contains at least the normalized expression, plus whatever
	// other expressions have been added.
	return 1 + len(g.otherExprs)
}

// expr returns the nth expression in the group. This method is used in concert
// with exprCount to iterate over the expressions in the group:
//   for i := 0; i < g.exprCount(); i++ {
//     e := g.expr(i)
//   }
//
// NOTE: The returned Expr reference is only valid until the next call to
//       addExpr, which may cause a resize of the exprs slice.
func (g *group) expr(nth ExprOrdinal) *Expr {
	if nth == normExprOrdinal {
		return &g.normExpr
	}
	return &g.otherExprs[nth-1]
}

// addExpr adds a new logically equivalent expression to the memo group. This
// is an alternate, denormalized expression that may have a lower cost.
func (g *group) addExpr(denorm Expr) {
	g.otherExprs = append(g.otherExprs, denorm)
}

// bestExprCount returns the number of bestExprs in the group.
func (g *group) bestExprCount() int {
	if !g.firstBestExpr.initialized() {
		return 0
	}
	return 1 + len(g.otherBestExprs)
}

// bestExpr returns the nth BestExpr in the group. This method is used in
// concert with bestExprCount to iterate over the expressions in the group:
//   for i := 0; i < g.bestExprCount(); i++ {
//     e := g.BestExpr(i)
//   }
//
// NOTE: The returned BestExpr reference is only valid until the next call to
//       ensureBestExpr, which may cause a resize of the bestExprs slice.
func (g *group) bestExpr(nth bestOrdinal) *BestExpr {
	if nth == 0 {
		return &g.firstBestExpr
	}
	return &g.otherBestExprs[nth-1]
}

// ensureBestExpr returns the id of the BestExpr that has the lowest cost for
// the given required properties. If no BestExpr exists yet, then
// ensureBestExpr creates a new empty BestExpr and returns its id.
func (g *group) ensureBestExpr(required PhysicalPropsID) BestExprID {
	// Handle case where firstBestExpr is not yet in use.
	if !g.firstBestExpr.initialized() {
		g.firstBestExpr.required = required
		g.firstBestExpr.cost = MaxCost
		return BestExprID{group: g.id, ordinal: 0}
	}

	// Fall back to otherBestExprs.
	for i := range g.otherBestExprs {
		be := &g.otherBestExprs[i]
		if be.required == required {
			// Bias the ordinal to skip firstBestExpr.
			return BestExprID{group: g.id, ordinal: bestOrdinal(i + 1)}
		}
	}

	// Panic if we ever reach 65535 best expressions - something's wrong.
	ordinal := len(g.otherBestExprs) + 1
	if ordinal >= int(normBestOrdinal) {
		panic(fmt.Sprintf("exceeded max number of expressions in group: %+v", g))
	}

	g.otherBestExprs = append(g.otherBestExprs, BestExpr{required: required, cost: MaxCost})
	return BestExprID{group: g.id, ordinal: bestOrdinal(ordinal)}
}
