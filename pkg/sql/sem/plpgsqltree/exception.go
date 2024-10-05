// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type Exception struct {
	StatementImpl
	Conditions []Condition
	Action     []Statement
}

func (s *Exception) CopyNode() *Exception {
	copyNode := *s
	copyNode.Conditions = append([]Condition(nil), copyNode.Conditions...)
	copyNode.Action = append([]Statement(nil), copyNode.Action...)
	return &copyNode
}

func (s *Exception) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("WHEN ")
	for i, cond := range s.Conditions {
		if i > 0 {
			ctx.WriteString(" OR ")
		}
		if cond.SqlErrState != "" {
			ctx.WriteString("SQLSTATE ")
			formatStringQuotes(ctx, cond.SqlErrState)
		} else {
			formatString(ctx, cond.SqlErrName)
		}
	}
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.Action {
		ctx.FormatNode(stmt)
	}
}

func (s *Exception) PlpgSQLStatementTag() string {
	return "proc_exception"
}

func (s *Exception) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	for i, stmt := range s.Action {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*Exception).Action[i] = ns
		}
	}
	return newStmt, changed
}

type Condition struct {
	SqlErrState string
	SqlErrName  string
}
