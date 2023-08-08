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

type Exception struct {
	StatementImpl
	Conditions []Condition
	Action     []Statement
}

func (s *Exception) Format(ctx *tree.FmtCtx) {
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

func (s *Exception) PlpgSQLStatementTag() string {
	return "proc_exception"
}

func (s *Exception) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Action {
		stmt.WalkStmt(visitor)
	}
}

type Condition struct {
	SqlErrState string
	SqlErrName  string
}
