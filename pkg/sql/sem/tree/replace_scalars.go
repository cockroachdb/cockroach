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

// scalarReplacer replaces scalar constants with placeholders. It implements
// ExtendedVisitor so that WalkStmt also walks into table expressions, join
// conditions, and statement nodes. The VisitStatementPre method collects bare
// integer literals used as column ordinal references (in ORDER BY, GROUP BY,
// and DISTINCT ON) so that VisitPre can skip replacing them.
type scalarReplacer struct {
	scalars Exprs
	// skipExprs tracks specific AST nodes (by pointer identity) that should
	// not be replaced with placeholders. These are bare integer literals used
	// as column ordinal references in ORDER BY, GROUP BY, and DISTINCT ON
	// clauses. Because the map keys are pointers to the original AST nodes,
	// other NumVal nodes with the same value in different parts of the AST
	// (e.g. SELECT 1 FROM t ORDER BY 1) are not affected.
	skipExprs map[Expr]struct{}
}

var _ ExtendedVisitor = &scalarReplacer{}

// skipOrdinal adds expr to the skip set if it is a bare integer NumVal,
// matching the optbuilder's colIndex detection logic.
func (s *scalarReplacer) skipOrdinal(expr Expr) {
	expr = StripParens(expr)
	if n, ok := expr.(*NumVal); ok && n.ShouldBeInt64() {
		if s.skipExprs == nil {
			s.skipExprs = make(map[Expr]struct{})
		}
		s.skipExprs[n] = struct{}{}
	}
}

func (s *scalarReplacer) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case dNull:
		// Don't try to substitute NULL with placeholders, because Go's database/sql
		// needs to know the type of the NULL in order to send it properly.
		return false, expr
	case Constant, Datum:
		if _, skip := s.skipExprs[t]; skip {
			return false, expr
		}
		s.scalars = append(s.scalars, t)
		return false, &Placeholder{Idx: PlaceholderIdx(len(s.scalars) - 1)}
	}
	return true, expr
}

func (s scalarReplacer) VisitPost(expr Expr) (newNode Expr) {
	return expr
}

func (s *scalarReplacer) VisitTablePre(expr TableExpr) (recurse bool, newExpr TableExpr) {
	return true, expr
}

func (s *scalarReplacer) VisitTablePost(expr TableExpr) (newNode TableExpr) {
	return expr
}

func (s *scalarReplacer) VisitStatementPre(stmt Statement) (recurse bool, newStmt Statement) {
	switch t := stmt.(type) {
	case *SelectClause:
		for _, e := range t.GroupBy {
			s.skipOrdinal(e)
		}
		for _, e := range t.DistinctOn {
			s.skipOrdinal(e)
		}
	case *Select:
		for _, o := range t.OrderBy {
			s.skipOrdinal(o.Expr)
		}
	case *Delete:
		for _, o := range t.OrderBy {
			s.skipOrdinal(o.Expr)
		}
	case *Update:
		for _, o := range t.OrderBy {
			s.skipOrdinal(o.Expr)
		}
	}
	return true, stmt
}

func (s *scalarReplacer) VisitStatementPost(stmt Statement) (newStmt Statement) {
	return stmt
}
