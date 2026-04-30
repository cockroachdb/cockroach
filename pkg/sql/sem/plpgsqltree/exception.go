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
	for i := range s.Conditions {
		if i > 0 {
			ctx.WriteString(" OR ")
		}
		ctx.FormatNode(&s.Conditions[i])
	}
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.Action {
		ctx.FormatNode(stmt)
	}
}

func (s *Exception) PlpgSQLStatementTag() string {
	return "proc_exception"
}

func (s *Exception) HumanReadableStmtTag() string { return "exception handler" }

func (s *Exception) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)
	if recurse {
		for i, actionStmt := range s.Action {
			newActionStmt := actionStmt.WalkStmt(visitor)
			if newActionStmt != actionStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Exception).Action[i] = newActionStmt
			}
		}
	}
	return newStmt
}

type Condition struct {
	SqlErrState string
	SqlErrName  string
}

func (c *Condition) Format(ctx *tree.FmtCtx) {
	if c.SqlErrState != "" {
		ctx.WriteString("SQLSTATE ")
		ctx.WriteString(formatStringQuotes(ctx, c.SqlErrState))
	} else {
		ctx.WriteString(formatString(ctx, c.SqlErrName))
	}
}
