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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type PLpgSQLException struct {
	PLpgSQLStatementImpl
	Conditions []PLpgSQLCondition
	Action     []PLpgSQLStatement
}

func (s *PLpgSQLException) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("WHEN ")
	for i, cond := range s.Conditions {
		if i > 0 {
			ctx.WriteString(" OR ")
		}
		if cond.SqlErrState != "" {
			ctx.WriteString(fmt.Sprintf("SQLSTATE '%s'", cond.SqlErrState))
		} else {
			ctx.WriteString(cond.SqlErrName)
		}
	}
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.Action {
		stmt.Format(ctx)
	}
}

func (s *PLpgSQLException) PlpgSQLStatementTag() string {
	return "proc_exception"
}

func (s *PLpgSQLException) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Action {
		stmt.WalkStmt(visitor)
	}
}

type PLpgSQLCondition struct {
	SqlErrState string
	SqlErrName  string
}
