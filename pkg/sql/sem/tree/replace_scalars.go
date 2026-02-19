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
	// skipExprs tracks expressions that should not be replaced with
	// placeholders. These are bare integer literals used as column ordinal
	// references in ORDER BY, GROUP BY, and DISTINCT ON clauses.
	skipExprs map[Expr]struct{}
}

var _ ExtendedVisitor = &scalarReplacer{}

// addOrdinalExprs adds bare integer NumVal expressions from the given
// expression list to the skip set, matching the optbuilder's colIndex
// detection logic.
func (s *scalarReplacer) addOrdinalExprs(exprs []Expr) {
	for _, e := range exprs {
		e = StripParens(e)
		if n, ok := e.(*NumVal); ok && n.ShouldBeInt64() {
			if s.skipExprs == nil {
				s.skipExprs = make(map[Expr]struct{})
			}
			s.skipExprs[n] = struct{}{}
		}
	}
}

// addOrderByOrdinals adds bare integer NumVal expressions from ORDER BY
// clauses to the skip set.
func (s *scalarReplacer) addOrderByOrdinals(orderBy OrderBy) {
	for _, o := range orderBy {
		e := StripParens(o.Expr)
		if n, ok := e.(*NumVal); ok && n.ShouldBeInt64() {
			if s.skipExprs == nil {
				s.skipExprs = make(map[Expr]struct{})
			}
			s.skipExprs[n] = struct{}{}
		}
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
		s.addOrdinalExprs([]Expr(t.GroupBy))
		s.addOrdinalExprs([]Expr(t.DistinctOn))
	case *Select:
		s.addOrderByOrdinals(t.OrderBy)
	case *Delete:
		s.addOrderByOrdinals(t.OrderBy)
	case *Update:
		s.addOrderByOrdinals(t.OrderBy)
	}
	return true, stmt
}

func (s *scalarReplacer) VisitStatementPost(stmt Statement) (newStmt Statement) {
	return stmt
}
