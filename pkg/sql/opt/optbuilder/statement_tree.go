// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/errors"
)

// statementTree tracks the hierarchy of statements within a query. It is used
// for preventing multiple modifications to the same table that can cause index
// corruption (see #70731).
//
// Mutations within UDFs perform reads at the latest sequence number. As a
// result, the restrictions that prevent multiple modifications to the same
// table in the same statement can be relaxed, slightly. For example, it's
// perfectly fine for a root statement without mutations to invoke a UDF that
// has two statements, each of with modifies the same table. The second
// statement in the UDF will not corrupt indexes because it will read the values
// written by the first statement. See CanMutateTable for more details.
//
// The statement tree is built up using only Push and Pop. Push pushes a new
// statement onto the tree as a child of the current statement and sets the new
// statement as the current statement. Pop sets the current statement's parent
// as the new current statement. For example, consider a root statement with two
// children:
//
//	statement1
//	├── statement2
//	└── statement3
//
// This tree is built with the following sequence of Push and Pop:
//
//	var st statementTree
//	st.Push()             // Push statement 1 as the root
//	st.Push()             // Push statement 2 as a child of statement 1
//	st.Pop()              // Pop statement 2
//	st.Push()             // Push statement 3 as a child of statement 1
type statementTree struct {
	curr *statementTreeNode
}

// statementTreeNode represents a single statement in the hierarchy of
// statements within a query.
type statementTreeNode struct {
	parent         *statementTreeNode
	children       []statementTreeNode
	tableMutations map[cat.StableID]mutationTypes
}

// mutationTypes represents a set of mutation types that can be applied to a
// table.
type mutationTypes uint8

const (
	// simpleInsertMutationType represents an INSERT with no ON CONFLICT clause.
	simpleInsertMutationType mutationTypes = 1 << iota
	// defaultMutationType represents all mutations except for a simple INSERT.
	defaultMutationType
)

func (mt mutationTypes) hasMutationType(typ mutationTypes) bool {
	return mt&typ == typ
}

// Push pushes a new statement onto the tree as a descendent of the current
// statement. The newly pushed statement becomes the current statement.
func (st *statementTree) Push() {
	if st.curr == nil {
		st.curr = &statementTreeNode{}
		return
	}
	st.curr.children = append(st.curr.children, statementTreeNode{
		parent: st.curr,
	})
	st.curr = &st.curr.children[len(st.curr.children)-1]
}

// Pop sets the parent of the current statement as the new current statement.
func (st *statementTree) Pop() {
	st.curr = st.curr.parent
}

// CanMutateTable returns true if the table can be mutated without concern for
// index corruption due to multiple modifications to the same table. It returns
// true if, for the current statement and all its ancestors and descendents,
// either of the following is true:
//
//  1. There are no other mutations to the given table.
//  2. The given mutation type is a simple INSERT and there exists only simple
//     INSERT mutations to the given table.
//
// If there is a non-simple-INSERT mutation to a table, it must be the only
// mutation, simple or otherwise, to that table in the direct lineage of any
// statement.
//
// For example, the following statement tree is valid. The two UPDATEs to t2 are
// allowed because neither of them are ancestors nor descendents of the other.
//
//	statement1: UPDATE t1
//	├── statement2: UPDATE t2
//	└── statement3: UPDATE t2
//
// The following statment tree is not valid because the UPDATE to t1 in
// statement3 is a descendent of the UPDATE to t1 in statement1.
//
//	statement1: UPDATE t1
//	├── statement2: UPDATE t2
//	└── statement3: UPDATE t1
func (st *statementTree) CanMutateTable(tabID cat.StableID, typ mutationTypes) bool {
	if st.curr == nil {
		panic(errors.AssertionFailedf("unexpected empty tree"))
	}

	// Check parent statements for a conflict.
	for pn := st.curr.parent; pn != nil; pn = pn.parent {
		parentType, ok := pn.tableMutations[tabID]
		if ok && (typ.hasMutationType(defaultMutationType) ||
			parentType.hasMutationType(defaultMutationType)) {
			return false
		}
	}

	// Check the current statement and children for a conflict.
	var checkChildren func(n *statementTreeNode) bool
	checkChildren = func(n *statementTreeNode) bool {
		nType, ok := n.tableMutations[tabID]
		if ok && (typ.hasMutationType(defaultMutationType) ||
			nType.hasMutationType(defaultMutationType)) {
			return false
		}
		for i := range n.children {
			if !checkChildren(&n.children[i]) {
				return false
			}
		}
		return true
	}
	if !checkChildren(st.curr) {
		return false
	}

	// The new mutation is valid, so track it.
	if st.curr.tableMutations == nil {
		st.curr.tableMutations = make(map[cat.StableID]mutationTypes)
	}
	st.curr.tableMutations[tabID] = st.curr.tableMutations[tabID] | typ
	return true
}
