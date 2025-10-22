// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
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

	// Hints are any external statement hints from the system.statement_hints
	// table that could apply to this statement, based on the statement
	// fingerprint.
	Hints []hintpb.StatementHintUnion

	// HintIDs are the IDs of any external statement hints, which are used for
	// invalidation of cached plans.
	HintIDs []int64

	// HintsGeneration is the generation of the hints cache at the time the
	// hints were retrieved, used for invalidation of cached plans.
	HintsGeneration int64
}

// makeStatement creates a Statement with the metadata necessary to execute the
// statement, including any external statement hints from the statement hints
// cache.
func makeStatement(
	ctx context.Context,
	parserStmt statements.Statement[tree.Statement],
	queryID clusterunique.ID,
	fmtFlags tree.FmtFlags,
	statementHintsCache *hints.StatementHintsCache,
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
	s := Statement{
		Statement:       parserStmt,
		StmtNoConstants: tree.FormatStatementHideConstants(parserStmt.AST, fmtFlags),
		StmtSummary:     tree.FormatStatementSummary(parserStmt.AST, fmtFlags),
		QueryID:         queryID,
		QueryTags:       tags,
		HintsGeneration: -1,
	}
	s.ReloadHintsIfStale(ctx, fmtFlags, statementHintsCache)
	return s
}

// makeStatementFromPrepared creates a Statement using the metadata from the
// prepared statement.
func makeStatementFromPrepared(
	ctx context.Context,
	prepared *prep.Statement,
	queryID clusterunique.ID,
	fmtFlags tree.FmtFlags,
	statementHintsCache *hints.StatementHintsCache,
) Statement {
	comments := prepared.Comments
	cl := len(comments)
	var tags []sqlcommenter.QueryTag
	// As per the sqlcommenter spec, query tags should be affixed to the
	// sql statement, so we only check the last comment. See
	// https://google.github.io/sqlcommenter/spec/#format for more details.
	if cl != 0 {
		tags = sqlcommenter.ExtractQueryTags(comments[cl-1])
	}
	s := Statement{
		Statement:       prepared.Statement,
		Prepared:        prepared,
		ExpectedTypes:   prepared.Columns,
		StmtNoConstants: prepared.StatementNoConstants,
		StmtSummary:     prepared.StatementSummary,
		QueryID:         queryID,
		QueryTags:       tags,
		Hints:           prepared.Hints,
		HintIDs:         prepared.HintIDs,
		HintsGeneration: prepared.HintsGeneration,
	}
	s.ReloadHintsIfStale(ctx, fmtFlags, statementHintsCache)
	return s
}

func (s Statement) String() string {
	// We have the original SQL, but we still use String() because it obfuscates
	// passwords.
	return s.AST.String()
}

// ReloadHintsIfStale reloads the external statement hints from the statement
// hints cache if they are stale.
func (s *Statement) ReloadHintsIfStale(
	ctx context.Context, fmtFlags tree.FmtFlags, statementHintsCache *hints.StatementHintsCache,
) bool {
	var hints []hintpb.StatementHintUnion
	var hintIDs []int64
	var hintsGeneration int64
	if statementHintsCache != nil {
		hintsGeneration = statementHintsCache.GetGeneration()
	}
	if hintsGeneration == s.HintsGeneration {
		return false
	}
	if statementHintsCache != nil {
		hintStmtFingerprint := s.StmtNoConstants
		switch e := s.Statement.AST.(type) {
		case *tree.CopyTo:
			hintStmtFingerprint = tree.FormatStatementHideConstants(e.Statement, fmtFlags)
		case *tree.Explain:
			hintStmtFingerprint = tree.FormatStatementHideConstants(e.Statement, fmtFlags)
		case *tree.ExplainAnalyze:
			hintStmtFingerprint = tree.FormatStatementHideConstants(e.Statement, fmtFlags)
		case *tree.Prepare:
			hintStmtFingerprint = tree.FormatStatementHideConstants(e.Statement, fmtFlags)
		}
		hints, hintIDs = statementHintsCache.MaybeGetStatementHints(ctx, hintStmtFingerprint)
	}
	if slices.Equal(hintIDs, s.HintIDs) {
		return false
	}
	s.Hints = hints
	s.HintIDs = hintIDs
	s.HintsGeneration = hintsGeneration
	return true
}
