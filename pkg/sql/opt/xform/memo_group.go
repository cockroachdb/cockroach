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
	"github.com/cockroachdb/cockroach/pkg/util"
)

type bitmap = util.FastIntSet

// GroupID identifies a memo group. Groups have numbers greater than 0; a
// GroupID of 0 indicates an invalid group.
type GroupID uint32

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
// provides those properties at the lowest known cost.
type memoGroup struct {
	// Id is the index of this group within the memo.
	id GroupID

	// logical is the set of logical properties that all memo expressions in
	// the group share.
	logical LogicalProps

	// exprs is the set of logically equivalent expressions that are part of
	// the group. The first expression is always the group's normalized
	// expression.
	exprs []memoExpr
}

// addExpr appends a new expression to the existing group and returns its id.
func (g *memoGroup) addExpr(mexpr *memoExpr) exprID {
	g.exprs = append(g.exprs, *mexpr)
	return exprID(len(g.exprs) - 1)
}

// lookupExpr looks up an expression in the group by its index.
func (m *memoGroup) lookupExpr(eid exprID) *memoExpr {
	return &m.exprs[eid]
}
