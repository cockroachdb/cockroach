// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/types"

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
// conditions, and statement nodes. The VisitStatementPre method collects
// constants that should not be replaced: column ordinals in ORDER BY,
// GROUP BY, and DISTINCT ON, and expressions in AS OF SYSTEM TIME
// clauses (which do not support placeholders).
type scalarReplacer struct {
	scalars Exprs
	// skipExprs tracks specific AST nodes (by pointer identity) that should
	// not be replaced with placeholders. Because the map keys are pointers
	// to the original AST nodes, other nodes with the same value in
	// different parts of the AST (e.g. SELECT 1 FROM t ORDER BY 1) are
	// not affected.
	skipExprs map[Expr]struct{}
}

var _ ExtendedVisitor = &scalarReplacer{}

// skipConstant adds expr to the skip set if it is a constant (NumVal,
// StrVal, or other Constant/Datum). This is used for expressions like
// column ordinals in ORDER BY that must not be replaced.
func (s *scalarReplacer) skipConstant(expr Expr) {
	expr = StripParens(expr)
	switch t := expr.(type) {
	case Constant, Datum:
		if s.skipExprs == nil {
			s.skipExprs = make(map[Expr]struct{})
		}
		s.skipExprs[t] = struct{}{}
	}
}

// skipAllConstants walks an expression tree and adds all constants found
// to the skip set. This is used for AS OF SYSTEM TIME expressions, which
// do not support placeholders.
func (s *scalarReplacer) skipAllConstants(expr Expr) {
	if expr == nil {
		return
	}
	_, _ = SimpleVisit(expr, func(e Expr) (bool, Expr, error) {
		s.skipConstant(e)
		return true, e, nil
	})
}

func (s *scalarReplacer) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case dNull:
		// Don't try to substitute NULL with placeholders, because Go's database/sql
		// needs to know the type of the NULL in order to send it properly.
		return false, expr
	case *NumVal:
		if _, skip := s.skipExprs[t]; skip {
			return false, expr
		}
		s.scalars = append(s.scalars, t)
		placeholder := &Placeholder{Idx: PlaceholderIdx(len(s.scalars) - 1)}
		if t.ShouldBeInt64() {
			// Integer literals are left as bare placeholders so their type is
			// inferred from context. This preserves correct behavior for integer
			// division, LIMIT/OFFSET, and other contexts where the type of the
			// literal matters (e.g. pow(decimal, $1) infers $1 as decimal).
			return false, placeholder
		}
		// Fractional literals are cast to DECIMAL to prevent incorrect type
		// inference from context (e.g. "4.0" being inferred as INT and failing
		// to parse).
		return false, &CastExpr{Expr: placeholder, Type: types.Decimal, SyntaxMode: CastExplicit}
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
			s.skipConstant(e)
		}
		for _, e := range t.DistinctOn {
			s.skipConstant(e)
		}
		s.skipAllConstants(t.From.AsOf.Expr)
	case *Select:
		for _, o := range t.OrderBy {
			s.skipConstant(o.Expr)
		}
	case *Delete:
		for _, o := range t.OrderBy {
			s.skipConstant(o.Expr)
		}
	case *Update:
		for _, o := range t.OrderBy {
			s.skipConstant(o.Expr)
		}
	}
	return true, stmt
}

func (s *scalarReplacer) VisitStatementPost(stmt Statement) (newStmt Statement) {
	return stmt
}
