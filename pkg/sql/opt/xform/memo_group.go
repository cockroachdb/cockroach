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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// exprID is the index of an expression within its group. exprID = 0 is always
// the normalized expression for the group.
type exprID uint32

const (
	// normExprID is the index of the group's normalized expression.
	normExprID exprID = 0
)

// memoGroup stores a set of logically equivalent expressions. See the comments
// on memoExpr for the definition of logical equivalency. In addition, for each
// required set of physical properties, the group stores the expression that
// provides those properties at the lowest known cost in the bestExprs slice.
type memoGroup struct {
	// Id is the index of this group within the memo.
	id opt.GroupID

	// logical is the set of logical properties that all memo expressions in
	// the group share.
	logical LogicalProps

	// exprs is the set of logically equivalent expressions that are part of
	// the group. The first expression is always the group's normalized
	// expression.
	exprs []memoExpr

	// bestExprs remembers the lowest cost expression in the group that provides
	// a particular set of physical properties. The FastIntMap key is an
	// opt.PhysicalPropID, and the value is an index into bestExprs.
	bestExprsMap util.FastIntMap
	bestExprs    []bestExpr
}

// lookupExpr looks up an expression in the group by its index.
func (g *memoGroup) lookupExpr(eid exprID) *memoExpr {
	return &g.exprs[eid]
}

// lookupBestExpr looks up the bestExpr that has the lowest cost for the given
// required properties. If no bestExpr exists yet, lookupBestExpr returns nil.
func (g *memoGroup) lookupBestExpr(required opt.PhysicalPropsID) *bestExpr {
	index, ok := g.bestExprsMap.Get(int(required))
	if !ok {
		return nil
	}
	return &g.bestExprs[index]
}

// ensureBestExpr looks up the bestExpr that has the lowest cost for the given
// required properties. If no bestExpr exists yet, then ensureBestExpr creates
// an empty bestExpr, adds it to bestExprsMap, and returns it.
func (g *memoGroup) ensureBestExpr(required opt.PhysicalPropsID) *bestExpr {
	best := g.lookupBestExpr(required)
	if best == nil {
		// Add new best expression.
		index := len(g.bestExprs)
		g.bestExprs = append(g.bestExprs, bestExpr{})
		g.bestExprsMap.Set(int(required), index)
		best = &g.bestExprs[index]
	}
	return best
}

// forEachBestExpr iterates over the bestExpr map and calls the given function
// for each pair of
func (g *memoGroup) forEachBestExpr(fn func(required opt.PhysicalPropsID, best *bestExpr)) {
	g.bestExprsMap.ForEach(func(required, index int) {
		fn(opt.PhysicalPropsID(required), &g.bestExprs[index])
	})
}
