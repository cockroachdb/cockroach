// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

// StatementVisitor defines methods that are called plpgsql statements during
// a statement walk.
type StatementVisitor interface {
	// Visit is called during a statement walk. If recurse is false for a given
	// node, the node's children (if any) will not be visited.
	Visit(stmt Statement) (newStmt Statement, recurse bool)
}

// Walk traverses the plpgsql statement.
func Walk(v StatementVisitor, stmt Statement) Statement {
	newStmt := stmt.WalkStmt(v)
	return newStmt
}
