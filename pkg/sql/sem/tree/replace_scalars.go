// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// ReplaceScalarsWithPlaceholders traverses a statement, replacing scalars with
// placeholders. It returns the new statement and the list of replaced scalars
// in order.
// This is used in tests, not production.
func ReplaceScalarsWithPlaceholders(s Statement) (Statement, Exprs) {
	v := &scalarReplacer{}
	newStmt, _ := walkStmt(v, s)
	return newStmt, v.scalars
}

type scalarReplacer struct {
	scalars Exprs
}

func (s *scalarReplacer) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	//case dNull:
	//	// Don't try to substitute NULL with placeholders, because Go's database/sql
	//	// needs to know the type of the NULL in order to send it properly.
	//	return false, expr
	case Constant, Datum:
		s.scalars = append(s.scalars, t)
		return false, &Placeholder{Idx: PlaceholderIdx(len(s.scalars) - 1)}
	}
	return true, expr
}

func (s scalarReplacer) VisitPost(expr Expr) (newNode Expr) {
	return expr
}
