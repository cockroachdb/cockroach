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
	"bytes"
	"fmt"
)

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
//
// TODO(andyk): Need to add the lowest cost expression map.
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

// lookupExpr looks up an expression in the group by its index.
func (g *memoGroup) lookupExpr(eid exprID) *memoExpr {
	return &g.exprs[eid]
}

func (g *memoGroup) memoGroupString(mem *memo) string {
	var buf bytes.Buffer

	for i, mexpr := range g.exprs {
		if i != 0 {
			buf.WriteByte(' ')
		}

		// Wrap the memo expr in ExprView to make it easy to get children.
		e := ExprView{
			mem:      mem,
			loc:      memoLoc{group: g.id, expr: exprID(i)},
			op:       mexpr.op,
			required: minPhysPropsID,
		}

		fmt.Fprintf(&buf, "[%s", e.Operator())

		private := e.Private()
		if private != nil {
			switch t := private.(type) {
			case nil:
			case TableIndex:
				fmt.Fprintf(&buf, " %s", mem.metadata.Table(t).TabName())
			case ColumnIndex:
				fmt.Fprintf(&buf, " %s", mem.metadata.ColumnLabel(t))
			case *ColSet, *ColMap:
				// Don't show anything, because it's mostly redundant.
			default:
				fmt.Fprintf(&buf, " %s", private)
			}
		}

		if e.ChildCount() > 0 {
			fmt.Fprintf(&buf, " [")
			for i := 0; i < e.ChildCount(); i++ {
				child := e.ChildGroup(i)
				if i > 0 {
					buf.WriteString(" ")
				}
				if child <= 0 {
					buf.WriteString("-")
				} else {
					fmt.Fprintf(&buf, "%d", child)
				}
			}
			fmt.Fprintf(&buf, "]")
		}

		buf.WriteString("]")
	}

	return buf.String()
}
