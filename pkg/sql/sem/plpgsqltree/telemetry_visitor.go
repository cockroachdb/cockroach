// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

// PLpgSQLStmtVisitor defines methods that are called plpgsql statements during
// a statement walk.
type PLpgSQLStmtVisitor interface {
	// Visit is called during a statement walk.
	Visit(stmt PLpgSQLStatement)
}

// Walk traverses the plpgsql statement.
func Walk(v PLpgSQLStmtVisitor, stmt PLpgSQLStatement) {
	stmt.WalkStmt(v)
}
