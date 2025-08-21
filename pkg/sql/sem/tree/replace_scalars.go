// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// TestingReplaceScalarsWithPlaceholders traverses a statement, replacing
// scalars with placeholders. It returns the new statement and the list of
// replaced scalars in order.
// This is used in tests, not production.
func TestingReplaceScalarsWithPlaceholders(s Statement) (Statement, Exprs) {
	v := &scalarReplacer{}
	newStmt, _ := WalkStmt(v, s)
	return newStmt, v.scalars
}

type scalarReplacer struct {
	scalars Exprs
}

func (s *scalarReplacer) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case dNull:
		// Don't try to substitute NULL with placeholders, because Go's database/sql
		// needs to know the type of the NULL in order to send it properly.
		return false, expr
	case Constant, Datum:
		s.scalars = append(s.scalars, t)
		return false, &Placeholder{Idx: PlaceholderIdx(len(s.scalars) - 1)}
	}
	return true, expr
}

func (s scalarReplacer) VisitPost(expr Expr) (newNode Expr) {
	return expr
}
