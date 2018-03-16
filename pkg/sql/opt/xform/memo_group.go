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

package xform

import (
	"math"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// exprOrdinal is the ordinal position of an expression within its memo group.
// Each group stores one or more logically equivalent expressions. The 0th
// expression is always the normalized expression for the group.
type exprOrdinal uint16

const (
	// normExprOrdinal is the ordinal position of the normalized expression in
	// its group.
	normExprOrdinal exprOrdinal = 0
)

// bestOrdinal is the ordinal position of a bestExpr within its memo group.
// Once optimized, each group manages one or more bestExprs that track the
// lowest cost expression for a particular set of required physical properties.
type bestOrdinal uint16

const (
	// normBestOrdinal is a special reserved ordinal that is used by ExprView
	// to indicate that the normalized tree should be traversed rather than the
	// lowest cost expression tree.
	normBestOrdinal bestOrdinal = math.MaxUint16
)

// memoGroup stores a set of logically equivalent expressions. See the comments
// on memoExpr for the definition of logical equivalency. In addition, for each
// required set of physical properties, the group stores the expression that
// provides those properties at the lowest known cost in the bestExprs fields.
type memoGroup struct {
	// id is the index of this group within the memo.
	id opt.GroupID

	// logical is the set of logical properties that all memo expressions in
	// the group share.
	logical LogicalProps

	// normExpr holds the first expression in the group, which is always the
	// group's normalized expression. otherExprs holds any expressions beyond
	// the first. These are separated in order to optimize for the common case
	// of a group with only one expression (often true for scalar expressions).
	normExpr   memoExpr
	otherExprs []memoExpr

	// firstBestExpr remembers the lowest cost expression in the group that
	// provides a particular set of physical properties. If the group has been
	// optimized with respect to more than the one set of properties, then
	// otherBestExprs remembers the lowest cost expressions for the others.
	// These fields are separated in order to optimize for the common case of a
	// group that has been optimized for only one set of properties.
	firstBestExpr  bestExpr
	otherBestExprs []bestExpr
}

// makeMemoGroup constructs a new memo group with the given id and its
// normalized expression.
func makeMemoGroup(id opt.GroupID, norm memoExpr) memoGroup {
	return memoGroup{id: id, normExpr: norm}
}

// exprCount returns the number of logically-equivalent expressions in the
// group.
func (g *memoGroup) exprCount() int {
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
// NOTE: The returned memoExpr reference is only valid until the next call to
//       addExpr, which may cause a resize of the exprs slice.
func (g *memoGroup) expr(nth exprOrdinal) *memoExpr {
	if nth == normExprOrdinal {
		return &g.normExpr
	}
	return &g.otherExprs[nth-1]
}

// bestExprCount returns the number of bestExprs in the group.
func (g *memoGroup) bestExprCount() int {
	if !g.firstBestExpr.initialized() {
		return 0
	}
	return 1 + len(g.otherBestExprs)
}

// bestExpr returns the nth bestExpr in the group. This method is used in
// concert with bestExprCount to iterate over the expressions in the group:
//   for i := 0; i < g.bestExprCount(); i++ {
//     e := g.bestExpr(i)
//   }
//
// NOTE: The returned bestExpr reference is only valid until the next call to
//       ensureBestExpr, which may cause a resize of the bestExprs slice.
func (g *memoGroup) bestExpr(nth bestOrdinal) *bestExpr {
	if nth == 0 {
		return &g.firstBestExpr
	}
	return &g.otherBestExprs[nth-1]
}

// ensureBestExpr returns the id of the bestExpr that has the lowest cost for
// the given required properties. If no bestExpr exists yet, then
// ensureBestExpr creates a new empty bestExpr and returns its id.
func (g *memoGroup) ensureBestExpr(required opt.PhysicalPropsID) bestExprID {
	// Handle case where firstBestExpr is not yet in use.
	if !g.firstBestExpr.initialized() {
		g.firstBestExpr.required = required
		return bestExprID{group: g.id, ordinal: 0}
	}

	// Fall back to otherBestExprs.
	for i := range g.otherBestExprs {
		be := &g.otherBestExprs[i]
		if be.required == required {
			// Bias the ordinal to skip firstBestExpr.
			return bestExprID{group: g.id, ordinal: bestOrdinal(i + 1)}
		}
	}

	// Panic if we ever reach 65535 best expressions - something's wrong.
	ordinal := len(g.otherBestExprs) + 1
	if ordinal >= int(normBestOrdinal) {
		panic(fmt.Sprintf("exceeded max number of expressions in group: %+v", g))
	}

	g.otherBestExprs = append(g.otherBestExprs, bestExpr{required: required})
	return bestExprID{group: g.id, ordinal: bestOrdinal(ordinal)}
}

// ratchetBestExpr overwrites the existing best expression with the given id if
// the candidate expression has a lower cost.
// TODO(andyk): Currently, there is no costing function, so just assume that
//              the first expression is the lowest cost.
func (g *memoGroup) ratchetBestExpr(best bestExprID, candidate *bestExpr) {
	be := g.bestExpr(best.ordinal)
	if be.op == opt.UnknownOp {
		*be = *candidate
	}
}
