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
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
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

	legacyTSI, legacySQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer legacyTSI.Stopper().Stop(ctx)
	legacyConn, err := legacySQLDB.Conn(ctx)
	require.NoError(t, err)
	_, err = legacyConn.ExecContext(ctx, "SET use_declarative_schema_changer = off;")
	require.NoError(t, err)

	declarativeTSI, declarativeSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer declarativeTSI.Stopper().Stop(ctx)
	declarativeConn, err := declarativeSQLDB.Conn(ctx)
	require.NoError(t, err)
	_, err = declarativeConn.ExecContext(ctx, "SET use_declarative_schema_changer = on;")
	require.NoError(t, err)

	// Track executed statements so far for debugging/repro purposes.
	var linesExecutedSoFar []string

	for ss.HasNextLine() {
		line := ss.NextLine()

		syntaxError := hasSyntaxError(line)
		inTxn := isInATransaction(ctx, t, legacyConn)

		// Pre-execution: modify `line` so that executing it produces the same
		// state. This step is to account for the known behavior difference between
		// the two schema changers.
		// Only run when not in a transaction (otherwise certain DDL combo can make
		// sql queries issued during modification break; see commit message).
		if !inTxn && !syntaxError {
			line = modifyBlacklistedStmt(ctx, t, line, legacyConn)
		}

		// Execution: `line` must be executed in both clusters with the same error
		// code.
		_, errLegacy := legacyConn.ExecContext(ctx, line)
		if pgcode.MakeCode(string(getPQErrCode(errLegacy))) == pgcode.FeatureNotSupported {
			continue
		}
		_, errDeclarative := declarativeConn.ExecContext(ctx, line)
		requireNoErrOrSameErrCode(t, line, errLegacy, errDeclarative)
		linesExecutedSoFar = append(linesExecutedSoFar, line)
		t.Logf("Executing %q", line)

		// Post-execution: Check metadata level identity between two clusters.
		// Only run when not in a transaction (because legacy schema changer will be
		// used in both clusters).
		if !inTxn && !syntaxError {
			if containsStmtOfType(t, line, tree.TypeDDL) {
				metaDataIdentityCheck(ctx, t, legacyConn, declarativeConn, linesExecutedSoFar)
			}
		}
	}
}

func hasSyntaxError(line string) bool {
	_, err := parser.Parse(line)
	return err != nil
}

// isInATransaction returns true if connection `db` is currently in a transaction.
func isInATransaction(ctx context.Context, t *testing.T, conn *gosql.Conn) bool {
	rows, err := conn.QueryContext(ctx, "SHOW transaction_status;")
	errCode := pgcode.MakeCode(string(getPQErrCode(err)))
	if errCode == pgcode.InFailedSQLTransaction || errCode == pgcode.InvalidTransactionState {
		// Txn is in `ERROR` state (25P02) or `DONE` state (25000).
		return true
	}
	require.NoError(t, err) // any other error not allowed
	res, err := sqlutils.RowsToStrMatrix(rows)
	require.NoError(t, err)
	return res[0][0] == `Open` // Txn is in `Open` state.
}

// sqlLineModifier is the standard signature that takes as input a sql stmt(s) line
// and perform some kind of modification to it.
// The SQLRunner is there in case the modification requires being able to access
// certain info from the cluster with sql.
type sqlLineModifier func(context.Context, *testing.T, statements.Statements, *gosql.Conn) (statements.Statements, bool)

// modifyBlacklistedStmt attempts to detect whether `line` is a known statement
// with different behavior under legacy vs under declarative schema changer.
// Those cases are hard-coded, and if `line` is one of them, we transform it
// into one with the same behavior under those two schema changers.
func modifyBlacklistedStmt(
	ctx context.Context, t *testing.T, line string, legacyConn *gosql.Conn,
) string {
	parsedLine, err := parser.Parse(line)
	require.NoError(t, err)

	var modify bool
	for _, lm := range []sqlLineModifier{
		modifyExprsReferencingSequencesWithTrue,
		modifyAlterPKWithRowIDCol,
		modifySetDeclarativeSchemaChangerMode,
		modifyCreateTempTable,
	} {
		var m bool
		parsedLine, m = lm(ctx, t, parsedLine, legacyConn)
		modify = modify || m
	}
	if modify {
		t.Logf("Comparator testing framework modifies line %q to %q", line, parsedLine.String())
	}
	return parsedLine.StringWithFlags(tree.FmtSimple | tree.FmtTagDollarQuotes)
}

// modifySetDeclarativeSchemaChangerMode skips stmts that attempt to alter
// schema changer mode via session variable "use_declarative_schema_changer" or
// cluster setting "sql.schema.force_declarative_statements".
func modifySetDeclarativeSchemaChangerMode(
	_ context.Context, t *testing.T, parsedStmts statements.Statements, _ *gosql.Conn,
) (statements.Statements, bool) {
	var newParsedStmts statements.Statements
	for _, parsedStmt := range parsedStmts {
		switch ast := parsedStmt.AST.(type) {
		case *tree.SetVar:
			if ast.Name == "use_declarative_schema_changer" {
				continue
			}
		case *tree.SetClusterSetting:
			if ast.Name == "sql.schema.force_declarative_statements" {
				continue
			}
		}
		newParsedStmts = append(newParsedStmts, parsedStmt)
	}
	return newParsedStmts, false
}

// modifyAlterPKWithRowIDCol modifies any ALTER PK stmt in `line` if the
// current/old primary index column is `rowid` by appending a `DROP COLUMN IF
// EXISTS rowid` to it, so that legacy schema changer will converge to
// declarative schema changer (in which ALTER PK will already drop the `rowid`
// column).
// The returned boolean indicates if such a modification happened.
func modifyAlterPKWithRowIDCol(
	ctx context.Context, t *testing.T, parsedStmts statements.Statements, tdb *gosql.Conn,
) (statements.Statements, bool) {
	// A helper to determine whether table `name`'s current primary key column is
	// the implicitly created `rowid` column.
	isCurrentPrimaryKeyColumnRowID := func(name *tree.UnresolvedObjectName) (bool, string) {
		rows, err := tdb.QueryContext(ctx, fmt.Sprintf(
			`
WITH columns AS
 (SELECT jsonb_array_elements(crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' -> 'columns') AS col 
  FROM system.descriptor 
  WHERE id = '%v'::REGCLASS),
pkcolumnids AS
 (SELECT jsonb_array_elements(crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' -> 'primaryIndex' -> 'keyColumnIds') AS pkcolid 
  FROM system.descriptor 
  WHERE id = '%v'::REGCLASS)          
SELECT col ->> 'name' as "colName", 
       col ->> 'defaultExpr' as "defaultExpr", 
       col ->> 'hidden' as "isHidden", 
       col -> 'type' ->> 'family' as "columnFamilyType" 
FROM columns, pkcolumnids
WHERE col -> 'id' = pkcolid;`, name.String(), name.String()))
		require.NoError(t, err)
		res, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		if len(res) != 1 {
			return false, ""
		}
		colName, colDefExpr, colIsHidden, colType := res[0][0], res[0][1], res[0][2], res[0][3]
		return strings.HasPrefix(colName, "rowid") && colDefExpr == "unique_rowid()" &&
			colIsHidden == "true" && colType == "IntFamily", colName
	}

	var newParsedStmts statements.Statements
	var modified bool
	for _, parsedStmt := range parsedStmts {
		newParsedStmts = append(newParsedStmts, parsedStmt)
		var isAlterPKWithRowID bool
		var tableName *tree.UnresolvedObjectName
		var implicitRowidColname string
		switch ast := parsedStmt.AST.(type) {
		case *tree.AlterTable:
			tableName = ast.Table
			for _, cmd := range ast.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAlterPrimaryKey:
					isAlterPKWithRowID, implicitRowidColname = isCurrentPrimaryKeyColumnRowID(ast.Table)
				case *tree.AlterTableAddConstraint:
					if alterTableAddPK, ok := cmd.ConstraintDef.(*tree.UniqueConstraintTableDef); ok &&
						alterTableAddPK.PrimaryKey {
						isAlterPKWithRowID, implicitRowidColname = isCurrentPrimaryKeyColumnRowID(ast.Table)
					}
				}
			}
		}
		if isAlterPKWithRowID {
			parsedCommit, err := parser.ParseOne("commit")
			require.NoError(t, err)
			parsedDropRowID, err := parser.ParseOne(fmt.Sprintf("ALTER TABLE %v DROP COLUMN IF EXISTS %v", tableName, implicitRowidColname))
			require.NoError(t, err)
			newParsedStmts = append(newParsedStmts, parsedCommit, parsedDropRowID)
			modified = true
		}
	}

	return newParsedStmts, modified
}

// modifyExprsReferencingSequencesWithTrue modifies any expressions in `line`
// that references sequences to "True". The returned boolean indicates whether
// such a modification happened.
func modifyExprsReferencingSequencesWithTrue(
	_ context.Context, t *testing.T, parsedStmts statements.Statements, _ *gosql.Conn,
) (statements.Statements, bool) {
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

	var newParsedStmts statements.Statements
	var modified bool
	for _, parsedStmt := range parsedStmts {
		switch ast := parsedStmt.AST.(type) {
		case *tree.CreateTable:
			for _, colDef := range ast.Defs {
				switch colDef := colDef.(type) {
				case *tree.ColumnTableDef:
					for i, colCkExpr := range colDef.CheckExprs {
						colDef.CheckExprs[i].Expr = replaceSeqReferencesWithTrueInExpr(colCkExpr.Expr)
						modified = true
					}
				case *tree.CheckConstraintTableDef:
					colDef.Expr = replaceSeqReferencesWithTrueInExpr(colDef.Expr)
					modified = true
				}
			}
		case *tree.AlterTable:
			for _, cmd := range ast.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAddColumn:
					for i, colCkExpr := range cmd.ColumnDef.CheckExprs {
						cmd.ColumnDef.CheckExprs[i].Expr = replaceSeqReferencesWithTrueInExpr(colCkExpr.Expr)
						modified = true
					}
				case *tree.AlterTableAddConstraint:
					if ck, ok := cmd.ConstraintDef.(*tree.CheckConstraintTableDef); ok {
						ck.Expr = replaceSeqReferencesWithTrueInExpr(ck.Expr)
						modified = true
					}
				}
			}
		}
		newParsedStmts = append(newParsedStmts, parsedStmt)
	}

	return newParsedStmts, modified
}

// modifyCreateTempTable skips `CREATE TEMP TABLE` statement because its parent schema name,
// constructed from sessionID, is naturally going to be different between the two clusters.
// In short of a more clever way to bake this difference into the metadata identity check,
// we decided to just skip those statements, for now.
func modifyCreateTempTable(
	_ context.Context, t *testing.T, parsedStmts statements.Statements, _ *gosql.Conn,
) (newParsedStmts statements.Statements, modified bool) {
	for _, parsedStmt := range parsedStmts {
		if ast, ok := parsedStmt.AST.(*tree.CreateTable); ok && ast.Persistence.IsTemporary() {
			modified = true
			continue
		}
		newParsedStmts = append(newParsedStmts, parsedStmt)
	}
	return newParsedStmts, modified
}

// requireNoErrOrSameErrCode require errors from executing some statement
// from legacy and declarative schema changer clusters to be both nil or
// both PQ error with same code.
func requireNoErrOrSameErrCode(t *testing.T, line string, errLegacy, errDeclarative error) {
	if errLegacy == nil && errDeclarative == nil {
		return
	}

	if errLegacy == nil {
		t.Fatalf("statement %q failed with declarative schema changer "+
			"(but succeeded with legacy schema changer): %v", line, errDeclarative)
	}
	if errDeclarative == nil {
		t.Fatalf("statement %q failed with legacy schema changer "+
			"(but succeeded with declarative schema changer): %v", line, errLegacy)
	}
	errLegacyPQCode := getPQErrCode(errLegacy)
	errDeclarativePQCode := getPQErrCode(errDeclarative)
	if errLegacyPQCode == "" || errDeclarativePQCode == "" {
		t.Fatalf("executing statement %q results in non-PQ error:  legacy=%v, declarative=%v ", line, errLegacy, errDeclarative)
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
func metaDataIdentityCheck(
	ctx context.Context, t *testing.T, legacy, declarative *gosql.Conn, linesExecutedSoFar []string,
) {
	// A function to fetch all descriptors create statements, collectively known
	// as the "descriptor state".
	fetchDescriptorState := func(conn *gosql.Conn) [][]string {
		rows, err := conn.QueryContext(ctx, fetchDescriptorStateQuery)
		require.NoError(t, err)
		res, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		return res
	}

	legacyDescriptors := parserRoundTrip(t, fetchDescriptorState(legacy))
	declarativeDescriptors := parserRoundTrip(t, fetchDescriptorState(declarative))
	if len(legacyDescriptors) != len(declarativeDescriptors) {
		t.Fatal(errors.Newf("number of descriptors mismatches: "+
			"legacy cluster = %v, declarative cluster = %v", len(legacyDescriptors), len(declarativeDescriptors)))
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
	diff := cmp.Diff(createsInLegacy, createsInDeclarative)
	if len(diff) > 0 {
		t.Logf("Meta-data mismatch!\nHistory of executed statements:\n%v", strings.Join(linesExecutedSoFar, "\n"))
		err := errors.Newf("descriptors mismatch with diff (- is legacy, + is declarative):\n%v", diff)
		err = errors.Wrapf(err, "\ndescriptors diverge after executing %q; "+
			"see logs for the history of executed statements", linesExecutedSoFar[len(linesExecutedSoFar)-1])
		t.Fatalf(err.Error())
	}
}
