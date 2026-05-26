// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// statementTree tracks the hierarchy of statements within a query. It is used
// for preventing multiple modifications to the same table that can cause index
// corruption (see #70731). See CanMutateTable for more details.
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
//
// Note that we don't actually build a tree-like data structure to represent a
// statement tree. The API only allows modifying the tree with Push and Pop. A
// statement's children only need to be checked for conflicts after those
// statements have been popped. This means we can simplify the implementation to
// using a stack. In order to use a stack, Pop combines all the mutations of the
// popped statement into a set of children mutations in its parent statement.
// This set of children mutations is checked for conflicts during
// CanMutateTable, which is equivalent to maintaining and traversing the entire
// sub-tree of the popped statement.
//
// +----------------+
// | After Triggers |
// +----------------+
// After triggers are an exceptional case because they are planned lazily during
// execution, and unlike cascades, can include arbitrary logic. This means that
// we can't know ahead of time which tables will be mutated by AFTER triggers.
// Since AFTER triggers are post-queries, they do not conflict with the current
// statement. However, they can conflict with ancestor statements, e.g., if they
// are fired within a routine.
//
// We handle this by deferring the check for AFTER triggers until they are
// actually built. We maintain references to all ancestor statementTreeNodes,
// and use those to initialize the statement tree when building the AFTER
// triggers. Since AFTER triggers are built after the outer query has finished
// planning, all potentially conflicting mutations are known and reflected in
// the initialized statement tree. Note that some care must be taken to ensure
// that the statementTreeNode references remain valid and up-to-date (see the
// stmts comment below).
type statementTree struct {
	// stmts is a stack of statement nodes, as described in the struct comment.
	// It is a slice of pointers to ensure that slice appends don't invalidate
	// references to existing nodes.
	stmts []*statementTreeNode
}

// mutationType represents a set of mutation types that can be applied to a
// table.
type mutationType uint8

const (
	// simpleInsert represents an INSERT with no ON CONFLICT clause.
	simpleInsert mutationType = iota
	// generalMutation represents all types of mutations except for a simple
	// INSERT.
	generalMutation
)

// statementTreeNode represents a single statement in the hierarchy of
// statements within a query.
type statementTreeNode struct {
	simpleInsertTables            intsets.Fast
	generalMutationTables         intsets.Fast
	childrenSimpleInsertTables    intsets.Fast
	childrenGeneralMutationTables intsets.Fast
}

// conflictsWithMutation returns true if the statement node conflicts with the
// given mutation table and type.
func (n *statementTreeNode) conflictsWithMutation(tabID cat.StableID, typ mutationType) bool {
	return typ == generalMutation && n.simpleInsertTables.Contains(int(tabID)) ||
		n.generalMutationTables.Contains(int(tabID))
}

// childrenConflictWithMutation returns true if any children of the statement
// node conflict with the given mutation table and type.
func (n *statementTreeNode) childrenConflictWithMutation(
	tabID cat.StableID, typ mutationType,
) bool {
	return typ == generalMutation && n.childrenSimpleInsertTables.Contains(int(tabID)) ||
		n.childrenGeneralMutationTables.Contains(int(tabID))
}

// Push pushes a new statement onto the tree as a descendent of the current
// statement. The newly pushed statement becomes the current statement.
func (st *statementTree) Push() {
	st.stmts = append(st.stmts, &statementTreeNode{})
}

// Pop sets the parent of the current statement as the new current statement.
func (st *statementTree) Pop() {
	popped := st.stmts[len(st.stmts)-1]
	st.stmts = st.stmts[:len(st.stmts)-1]
	if len(st.stmts) > 0 {
		// Combine the popped statement's mutations and child mutations into the
		// child statements of its parent (the new current statement).
		curr := st.stmts[len(st.stmts)-1]
		curr.childrenSimpleInsertTables.UnionWith(popped.simpleInsertTables)
		curr.childrenSimpleInsertTables.UnionWith(popped.childrenSimpleInsertTables)
		curr.childrenGeneralMutationTables.UnionWith(popped.generalMutationTables)
		curr.childrenGeneralMutationTables.UnionWith(popped.childrenGeneralMutationTables)
	}
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
// allowed because they exist in sibling statements, i.e., neither of the
// statements are ancestors nor descendents of the other.
//
//	statement1: UPDATE t1
//	├── statement2: UPDATE t2
//	└── statement3: UPDATE t2
//
// The following statement tree is not valid because statement1 is the parent of
// statement3, and they both update t1.
//
//	statement1: UPDATE t1
//	├── statement2: UPDATE t2
//	└── statement3: UPDATE t1
//
// isPostStmt indicates that this mutation will be evaluated at the end of the
// statement (e.g., as part of a foreign key constraint).
func (st *statementTree) CanMutateTable(
	tabID cat.StableID, typ mutationType, isPostStmt bool,
) bool {
	if len(st.stmts) == 0 {
		panic(errors.AssertionFailedf("unexpected empty tree"))
	}
	if isPostStmt && len(st.stmts) == 1 {
		return true
	}
	offset := 1
	if isPostStmt {
		// If this mutation will be evaluated at the end of the current statement,
		// the mutation should be added to the parent statement. This is because
		// during execution, we step the transaction sequence point before starting
		// check evaluations, so all updates in the current statement will be
		// visible.
		offset = 2
	}
	curr := st.stmts[len(st.stmts)-offset]
	// Check the children of the current statement for a conflict.
	if !isPostStmt && curr.childrenConflictWithMutation(tabID, typ) {
		return false
	}
	// Check the current statement and all parent statements for a conflict.
	for i := range st.stmts {
		if isPostStmt && i == len(st.stmts)-1 {
			// Don't check against the originating statement since we're adding this
			// mutation to the parent statement.
			break
		}
		n := st.stmts[i]
		if n.conflictsWithMutation(tabID, typ) {
			return false
		}
	}
	// The new mutation is valid, so track it.
	switch typ {
	case simpleInsert:
		if isPostStmt {
			curr.childrenSimpleInsertTables.Add(int(tabID))
		} else {
			curr.simpleInsertTables.Add(int(tabID))
		}
	case generalMutation:
		if isPostStmt {
			curr.childrenGeneralMutationTables.Add(int(tabID))
		} else {
			curr.generalMutationTables.Add(int(tabID))
		}
	}
	return true
}

// GetInitFnForPostQuery returns a function that can be used to initialize the
// statement tree for a post-query that is a child of the current statement.
// This is necessary because the post-query is not built until after the main
// statement has finished executing. The returned function may be nil if the
// statement tree does not need to be initialized for the post-query.
//
// NOTE: cascades are checked up-front by Builder.checkMultipleMutationsCascade,
// but the statement tree still must be propagated to them via this function in
// case the cascade causes a trigger to fire.
func (st *statementTree) GetInitFnForPostQuery() func() statementTree {
	if len(st.stmts) <= 1 {
		return nil
	}
	// Save references to the ancestor statementTreeNodes. Modifications to them
	// after this point should be reflected in the references, ensuring that all
	// ancestor mutations are visible by the time the post-query plan is built.
	//
	// This is necessary because the full set of mutations in the current
	// statement may not be known at the time the statement tree is saved. For
	// example, a CTE in which the first branch triggers a post-query, and the
	// second is a mutation.
	ancestorStatements := make([]*statementTreeNode, len(st.stmts)-1)
	copy(ancestorStatements, st.stmts[:len(st.stmts)-1])
	return func() statementTree {
		// Combine the non-child mutated tables for all ancestor nodes into a single
		// ancestor node. This provides all the information needed to check for
		// conflicts in a trigger run as a post-query. We can omit the child
		// mutation tables because they can only conflict with the ancestor nodes,
		// and that case has already been checked.
		var node statementTreeNode
		for i := range ancestorStatements {
			node.simpleInsertTables.UnionWith(ancestorStatements[i].simpleInsertTables)
			node.generalMutationTables.UnionWith(ancestorStatements[i].generalMutationTables)
		}
		return statementTree{stmts: []*statementTreeNode{&node}}
	}
}
