// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// This file contains common logic for performing comparator testing
// between legacy and declarative schema changer.

type StmtLineReader interface {
	// HasNextLine returns true if there is more lines of SQL statements to process.
	HasNextLine() bool

	// NextLine retrieve the next line of SQL statements to process.
	NextLine() string
}

// CompareLegacyAndDeclarative is the core logic for performing comparator
// testing between legacy and declarative schema changer.
// It reads sql statements (mostly DDLs), one by one, from `ss` and execute them
// in a cluster using legacy schema changer and in another using declarative
// schema changer. It asserts that, if the statement fails, they must fail with
// a PG error with the same pg code. If the statement succeeded, all descriptors
// in the cluster should end up in the same state.
func CompareLegacyAndDeclarative(t *testing.T, ss StmtLineReader) {
	ctx := context.Background()
	var linesExecutedSoFar []string
	legacyTSI, legacySQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	declarativeTSI, declarativeSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer legacyTSI.Stopper().Stop(ctx)
	defer declarativeTSI.Stopper().Stop(ctx)
	legacyTDB := sqlutils.MakeSQLRunner(legacySQLDB)
	declarativeTDB := sqlutils.MakeSQLRunner(declarativeSQLDB)
	legacyTDB.Exec(t, "SET use_declarative_schema_changer = off;")
	declarativeTDB.Exec(t, "SET use_declarative_schema_changer = on;")

	// A helper to determine whether table `name`'s current primary key column is
	// the implicit `rowid` column.
	isCurrentPrimaryKeyColumnRowID := func(name *tree.UnresolvedObjectName) bool {
		res := legacyTDB.QueryStr(t, fmt.Sprintf(`SELECT crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' -> 'primaryIndex' ->> 'keyColumnNames' FROM system.descriptor WHERE id = '%v'::REGCLASS;`, name.String()))
		return res[0][0] == `["rowid"]`
	}

	// A helper to determine whether the connection is currently in an open
	// transaction.
	isInAnOpenTransaction := func() bool {
		res := legacyTDB.QueryStr(t, `SHOW transaction_status;`)
		return res[0][0] == `Open`
	}

	for ss.HasNextLine() {
		line := ss.NextLine()
		line = modifyBlacklistedStmt(t, line, isCurrentPrimaryKeyColumnRowID, isInAnOpenTransaction)
		_, errLegacy := legacySQLDB.Exec(line)
		if pgcode.MakeCode(string(getPQErrCode(errLegacy))) == pgcode.FeatureNotSupported {
			continue
		}
		_, errDeclarative := declarativeSQLDB.Exec(line)
		requireNoErrOrSameErrCode(t, line, errLegacy, errDeclarative)
		linesExecutedSoFar = append(linesExecutedSoFar, line)

		// Perform a descriptor identity check after a DDL.
		if containsStmtOfType(t, line, tree.TypeDDL) {
			if err := metaDataIdentityCheck(t, legacyTDB, declarativeTDB); err != nil {
				t.Logf("Meta-data mismatch!\nHistory of executed statements:\n%v", strings.Join(linesExecutedSoFar, "\n"))
				err = errors.Wrapf(err, "\ndescriptors diverge after executing %q; "+
					"see logs for the history of executed statements", line)
				t.Fatalf(err.Error())
			}
		}
	}
}

// modifyBlacklistedStmt attempts to detect whether `line` is a known statement
// with different behavior under legacy vs under declarative schema changer.
// Those cases are hard-coded, and if `line` is one of them, we transform it
// into one with the same behavior under those two schema changers.
func modifyBlacklistedStmt(
	t *testing.T,
	line string,
	isCurrentPKRowID func(name *tree.UnresolvedObjectName) bool,
	isInAnOpenTransaction func() bool,
) string {
	line = modifyExprsReferencingSequencesWithTrue(t, line)
	line = modifyAlterPKWithRowIDCol(t, line, isCurrentPKRowID, isInAnOpenTransaction)
	return line
}

// modifyAlterPKWithRowIDCol rewrite any ALTER PK stmt in `line` if the
// current/old primary index column is `rowid` by appending a `DROP COLUMN IF
// EXISTS rowid` to it, so that legacy schema changer will converge to
// declarative schema changer (in which ALTER PK will already drop the `rowid`
// column).
func modifyAlterPKWithRowIDCol(
	t *testing.T,
	line string,
	isCurrentPKRowID func(name *tree.UnresolvedObjectName) bool,
	isInAnOpenTransaction func() bool,
) string {
	parsedStmts, err := parser.Parse(line)
	require.NoError(t, err)

	var newLine []string
	for _, parsedStmt := range parsedStmts {
		newLine = append(newLine, parsedStmt.SQL)
		var isAlterPKWithRowID bool
		var tableName *tree.UnresolvedObjectName
		switch ast := parsedStmt.AST.(type) {
		case *tree.AlterTable:
			for _, cmd := range ast.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAlterPrimaryKey:
					if isCurrentPKRowID(ast.Table) {
						isAlterPKWithRowID = true
						tableName = ast.Table
					}
				case *tree.AlterTableAddConstraint:
					if alterTableAddPK, ok := cmd.ConstraintDef.(*tree.UniqueConstraintTableDef); ok &&
						alterTableAddPK.PrimaryKey && isCurrentPKRowID(ast.Table) {
						isAlterPKWithRowID = true
						tableName = ast.Table
					}
				}
			}
		}
		if isAlterPKWithRowID {
			newLine = append(newLine, "commit", fmt.Sprintf("ALTER TABLE %v DROP COLUMN IF EXISTS rowid", tableName))
			if isInAnOpenTransaction() {
				newLine = append(newLine, "begin")
			}
		}
	}

	return strings.Join(newLine, "; ")
}

// modifyExprsReferencingSequencesWithTrue rewrites any expressions in `line`
// that references sequences to "True".
func modifyExprsReferencingSequencesWithTrue(t *testing.T, line string) string {
	// replaceSeqReferencesWithTrueInExpr detects if `expr` contains any references to
	// sequences. If so, return a new expression "True"; otherwise, return `expr` as is.
	replaceSeqReferencesWithTrueInExpr := func(expr tree.Expr) (newExpr tree.Expr) {
		newExpr = expr
		useSeqs, err := seqexpr.GetUsedSequences(expr)
		require.NoError(t, err)
		if len(useSeqs) > 0 {
			newExpr, err = parser.ParseExpr("true")
			require.NoError(t, err)
		}
		return newExpr
	}

	parsedStmts, err := parser.Parse(line)
	require.NoError(t, err)

	var newLine []string
	for _, parsedStmt := range parsedStmts {
		switch ast := parsedStmt.AST.(type) {
		case *tree.CreateTable:
			for _, colDef := range ast.Defs {
				switch colDef := colDef.(type) {
				case *tree.ColumnTableDef:
					for i, colCkExpr := range colDef.CheckExprs {
						colDef.CheckExprs[i].Expr = replaceSeqReferencesWithTrueInExpr(colCkExpr.Expr)
					}
				case *tree.CheckConstraintTableDef:
					colDef.Expr = replaceSeqReferencesWithTrueInExpr(colDef.Expr)
				}
			}
		case *tree.AlterTable:
			for _, cmd := range ast.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAddColumn:
					for i, colCkExpr := range cmd.ColumnDef.CheckExprs {
						cmd.ColumnDef.CheckExprs[i].Expr = replaceSeqReferencesWithTrueInExpr(colCkExpr.Expr)
					}
				case *tree.AlterTableAddConstraint:
					if ck, ok := cmd.ConstraintDef.(*tree.CheckConstraintTableDef); ok {
						ck.Expr = replaceSeqReferencesWithTrueInExpr(ck.Expr)
					}
				}
			}
		}
		newLine = append(newLine, tree.AsStringWithFlags(parsedStmt.AST, tree.FmtParsable))
	}

	return strings.Join(newLine, "; ")
}

// requireNoErrOrSameErrCode require errors from executing some statement
// from legacy and declarative schema changer clusters to be both nil or
// both PQ error with same code.
func requireNoErrOrSameErrCode(t *testing.T, line string, errLegacy, errDeclarative error) {
	if errLegacy == nil && errDeclarative == nil {
		return
	}

	if errLegacy == nil {
		t.Fatalf("statement %q failed with declarative schema changer (but succeeded with legacy schema changer): %v", line, errDeclarative.Error())
	}
	if errDeclarative == nil {
		t.Fatalf("statement %q failed with legacy schema changer (but succeeded with declarative schema changer): %v", line, errLegacy.Error())
	}
	errLegacyPQCode := getPQErrCode(errLegacy)
	errDeclarativePQCode := getPQErrCode(errDeclarative)
	if errLegacyPQCode == "" || errDeclarativePQCode == "" {
		t.Fatalf("executing statement %q results in non-PQ error:  legacy=%v, declarative=%v ", line, errLegacy.Error(), errDeclarative.Error())
	}
	if errLegacyPQCode != errDeclarativePQCode {
		t.Fatalf("executing statement %q results in different error code: legacy=%v, declarative=%v", line, errLegacyPQCode, errDeclarativePQCode)
	}
}

func getPQErrCode(err error) pq.ErrorCode {
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		return pqErr.Code
	}
	return ""
}

// containsStmtOfType returns true if `line` contains any statement of type `typ`.
func containsStmtOfType(t *testing.T, line string, typ tree.StatementType) bool {
	parsedLine, err := parser.Parse(line)
	require.NoError(t, err)
	for _, parsedStmt := range parsedLine {
		if parsedStmt.AST.StatementType() == typ {
			return true
		}
	}
	return false
}

// metaDataIdentityCheck looks up all descriptors' create_statements in
// `legacy` and `declarative` clusters and assert that they are identical.
func metaDataIdentityCheck(t *testing.T, legacy, declarative *sqlutils.SQLRunner) error {
	legacyDescriptors := parserRoundTrip(t, legacy.QueryStr(t, fetchDescriptorStateQuery))
	declarativeDescriptors := parserRoundTrip(t, declarative.QueryStr(t, fetchDescriptorStateQuery))
	if len(legacyDescriptors) != len(declarativeDescriptors) {
		return errors.Newf("number of descriptors mismatches: "+
			"legacy cluster = %v, declarative cluster = %v", len(legacyDescriptors), len(declarativeDescriptors))
	}
	// Transform the query result [][]string into one string, so we can compare them.
	var createsInLegacy, createsInDeclarative []string
	for i := range legacyDescriptors {
		// Each row should only have one column "create_statement".
		require.Equal(t, 1, len(legacyDescriptors[i]))
		require.Equal(t, 1, len(declarativeDescriptors[i]))
		createsInLegacy = append(createsInLegacy, legacyDescriptors[i][0])
		createsInDeclarative = append(createsInDeclarative, declarativeDescriptors[i][0])
	}
	legacyDescsStr := strings.Join(createsInLegacy, "\n")
	declarativeDescsStr := strings.Join(createsInDeclarative, "\n")
	if legacyDescsStr != declarativeDescsStr {
		return errors.Newf("descriptors mismatch with diff (+ is legacy, - is declarative):\n%v",
			sctestutils.Diff(declarativeDescsStr, legacyDescsStr, sctestutils.DiffArgs{}))
	}
	return nil
}
