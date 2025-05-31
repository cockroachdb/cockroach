// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/prep"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlcommenter"
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
	Prepared *prep.Statement

	QueryTags []sqlcommenter.QueryTag
}

func makeStatement(
	parserStmt statements.Statement[tree.Statement], queryID clusterunique.ID, fmtFlags tree.FmtFlags,
) Statement {
	comments := parserStmt.Comments
	cl := len(comments)
	var tags []sqlcommenter.QueryTag
	// As per the sqlcommenter spec, query tags should be affixed to the
	// sql statement, so we only check the last comment. See
	// https://google.github.io/sqlcommenter/spec/#format for more details.
	if cl != 0 {
		tags = sqlcommenter.ExtractQueryTags(comments[cl-1])
	}

	return Statement{
		Statement:       parserStmt,
		StmtNoConstants: formatStatementHideConstants(parserStmt.AST, fmtFlags),
		StmtSummary:     formatStatementSummary(parserStmt.AST, fmtFlags),
		QueryID:         queryID,
		QueryTags:       tags,
	}
}

func makeStatementFromPrepared(prepared *prep.Statement, queryID clusterunique.ID) Statement {
	comments := prepared.Comments
	cl := len(comments)
	var tags []sqlcommenter.QueryTag
	// As per the sqlcommenter spec, query tags should be affixed to the
	// sql statement, so we only check the last comment. See
	// https://google.github.io/sqlcommenter/spec/#format for more details.
	if cl != 0 {
		tags = sqlcommenter.ExtractQueryTags(comments[cl-1])
	}
	return Statement{
		Statement:       prepared.Statement,
		Prepared:        prepared,
		ExpectedTypes:   prepared.Columns,
		StmtNoConstants: prepared.StatementNoConstants,
		StmtSummary:     prepared.StatementSummary,
		QueryID:         queryID,
		QueryTags:       tags,
	}
}

func (s Statement) String() string {
	// We have the original SQL, but we still use String() because it obfuscates
	// passwords.
	return s.AST.String()
}
