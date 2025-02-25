// Copyright 2017 The Cockroach Authors.pkg/sql/statement.go
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Statement contains a statement with optional expected result columns and metadata.
type Statement struct {
	statements.Statement[tree.Statement]

	StmtNoConstants string
	StmtSummary     string
	QueryID         clusterunique.ID

	ExpectedTypes colinfo.ResultColumns

	// Prepared is non-nil during the PREPARE phase, as well as during EXECUTE of
	// a previously prepared statement. The Prepared statement can be modified
	// during either phase; the PREPARE phase sets its initial state, and the
	// EXECUTE phase can re-prepare it. This happens when the original plan has
	// been invalidated by schema changes, session data changes, permission
	// changes, or other changes to the context in which the original plan was
	// prepared.
	//
	// Given that the PreparedStatement can be modified during planning, it is
	// not safe for use on multiple threads.
	Prepared *PreparedStatement

	// Application specific tags added by SQL commenter
	SQLCommenterTags map[string]string
}

func makeStatement(
	parserStmt statements.Statement[tree.Statement], queryID clusterunique.ID, fmtFlags tree.FmtFlags,
) Statement {
	sqlCommenterTags := make(map[string]string)
	cl := len(parserStmt.Comments)
	if cl > 0 {
		tags, err := extractSQLCommenterTags(parserStmt.Comments[cl-1])
		if err == nil {
			sqlCommenterTags = tags
		}
	}

	return Statement{
		Statement:        parserStmt,
		StmtNoConstants:  formatStatementHideConstants(parserStmt.AST, fmtFlags),
		StmtSummary:      formatStatementSummary(parserStmt.AST, fmtFlags),
		QueryID:          queryID,
		SQLCommenterTags: sqlCommenterTags,
	}
}

func makeStatementFromPrepared(prepared *PreparedStatement, queryID clusterunique.ID) Statement {
	sqlCommenterTags := make(map[string]string)
	cl := len(prepared.Comments)
	if cl > 0 {
		tags, err := extractSQLCommenterTags(prepared.Comments[cl-1])
		if err == nil {
			sqlCommenterTags = tags
		}
	}

	return Statement{
		Statement:        prepared.Statement,
		Prepared:         prepared,
		ExpectedTypes:    prepared.Columns,
		StmtNoConstants:  prepared.StatementNoConstants,
		StmtSummary:      prepared.StatementSummary,
		QueryID:          queryID,
		SQLCommenterTags: sqlCommenterTags,
	}
}

func (s Statement) String() string {
	// We have the original SQL, but we still use String() because it obfuscates
	// passwords.
	return s.AST.String()
}
