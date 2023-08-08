// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plpgsqltree

// StatementVisitor defines methods that are called plpgsql statements during
// a statement walk.
type StatementVisitor interface {
	// Visit is called during a statement walk.
	Visit(stmt Statement)
}

// Walk traverses the plpgsql statement.
func Walk(v StatementVisitor, stmt Statement) {
	stmt.WalkStmt(v)
}
