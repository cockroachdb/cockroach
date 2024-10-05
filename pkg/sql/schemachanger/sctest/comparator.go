// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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

type Line struct {
	// original is the original line to be fed into the testing framework and
	// cross examined across LSC and DSC clusters.
	original string

	// legacy is a copy of original, possibly with modification, for execution in
	// the LSC cluster.
	legacy string

	// declarative is a copy of original, possibly with modification, for
	// execution in the DSC cluster.
	declarative string

	// hasSyntaxError is True if original has syntax error.
	hasSyntaxError bool
}

func NewLine(line string) Line {
	parsedLine, err := parser.Parse(line)
	syntaxErr := true
	if err == nil {
		line = parsedLine.StringWithFlags(tree.FmtSimple | tree.FmtTagDollarQuotes)
		syntaxErr = false
	}
	return Line{
		original:       line,
		legacy:         line,
		declarative:    line,
		hasSyntaxError: syntaxErr,
	}
}

func (l *Line) isModified() bool {
	return l.original != l.legacy || l.original != l.declarative
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

	for ss.HasNextLine() {
		line := NewLine(ss.NextLine())

		// Pre-execution: modify `line` so that executing it produces the same
		// descriptor state. This step is primarily for accounting for the known
		// behavior difference between the two schema changers.
		line = preExecutionProcessing(ctx, t, line, legacyConn, declarativeConn)

		// Execution: `line` must be executed in both clusters with the same error
		// code.
		// If a line results in feature-not-supported error in LSC, skip it unless
		// line contains COMMIT, in which case we also need to execute the line in
		// DSC cluster to properly close the transaction so that their connection
		// states are in sync.
		_, errLegacy := legacyConn.ExecContext(ctx, line.legacy)
		if isFeatureNotSupported(errLegacy) && !containsCommit(line.original) {
			continue
		}
		_, errDeclarative := declarativeConn.ExecContext(ctx, line.declarative)
		requireNoErrOrSameErrCode(t, line, errLegacy, errDeclarative)

		// Log executed lines for debugging/repro purposes.
		if !line.isModified() {
			t.Logf("Executing %q", line.original)
		} else {
			t.Logf("Executing %q with modifications: %q for legacy; %q for declarative",
				line.original, line.legacy, line.declarative)
		}

		// Post-execution: Check metadata level identity between two clusters. Only
		// run when not in a transaction (because legacy schema changer will be used
		// in both clusters) and current database is not dropped (because the check
		// fetches descriptor within current database).
		if !isInATransaction(ctx, t, legacyConn) && !line.hasSyntaxError && currentDatabaseExist(ctx, legacyConn) {
			if containsStmtOfType(t, line.legacy, tree.TypeDDL) {
				metaDataIdentityCheck(ctx, t, legacyConn, declarativeConn, line.original)
			}
		}
	}
}

func isFeatureNotSupported(err error) bool {
	return pgcode.MakeCode(string(getPQErrCode(err))) == pgcode.FeatureNotSupported
}

func containsCommit(line string) bool {
	stmts, err := parser.Parse(line)
	if err != nil {
		return false
	}
	for _, stmt := range stmts {
		if _, ok := stmt.AST.(*tree.CommitTransaction); ok {
			return true
		}
	}
	return false
}

// currentDatabaseExist returns false if current database (tracked by session
// variable `database`) does not exist, which can happen after one drops the
// current database and before setting `database` to an existing one.
func currentDatabaseExist(ctx context.Context, conn *gosql.Conn) bool {
	_, err := conn.ExecContext(ctx, "SHOW DATABASE")
	return pgcode.MakeCode(string(getPQErrCode(err))) != pgcode.UndefinedDatabase
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
type sqlLineModifier func(context.Context, *testing.T, statements.Statements, *gosql.Conn) statements.Statements

// preExecutionProcessing is where we potentially modify input `line` so that
// executing the returned line, one for LSC and one for DSC, results in the same
// descriptor state in both clusters.
func preExecutionProcessing(
	ctx context.Context, t *testing.T, line Line, legacyConn *gosql.Conn, declarativeConn *gosql.Conn,
) (potentiallyModifiedLine Line) {
	if line.hasSyntaxError {
		return line
	}

	parsedLineForLegacy, _ := parser.Parse(line.legacy)
	parsedLineForDeclarative, _ := parser.Parse(line.declarative)

	// General, framework level modifications: They are applied to both `line.legacy` and `line.declarative`.
	for _, lm := range []sqlLineModifier{
		modifySetDeclarativeSchemaChangerMode,
		modifyCreateTempTable,
	} {
		parsedLineForLegacy = lm(ctx, t, parsedLineForLegacy, legacyConn)
		parsedLineForDeclarative = lm(ctx, t, parsedLineForDeclarative, legacyConn)
	}

	// If `line.declarative` is supported in DSC, apply necessary modifications to ensure LSC
	// and DSC converge after execution to account for known behavioral
	// differences between the two schema changers.
	declarativeLine := parsedLineForDeclarative.StringWithFlags(tree.FmtSimple | tree.FmtTagDollarQuotes)
	if willLineBeExecutedInDSC(ctx, t, declarativeLine, declarativeConn) {
		legacyLineModifiers := []sqlLineModifier{
			modifyExprsReferencingSequencesWithTrue,
			modifyLineMultipleAlterTables,
			modifyAlterPKWithRowIDCol,
			modifyAlterPKWithSamePKColsButDifferentSharding,
			modifyLineToSkipValidateConstraint,
		}
		declarativeLineModifiers := []sqlLineModifier{
			modifyExprsReferencingSequencesWithTrue,
			modifyLineToSkipValidateConstraint,
		}
		for _, lm := range legacyLineModifiers {
			parsedLineForLegacy = lm(ctx, t, parsedLineForLegacy, legacyConn)
		}
		for _, lm := range declarativeLineModifiers {
			parsedLineForDeclarative = lm(ctx, t, parsedLineForDeclarative, legacyConn)
		}
	}

	res := Line{
		original:    line.original,
		legacy:      parsedLineForLegacy.StringWithFlags(tree.FmtSimple | tree.FmtTagDollarQuotes),
		declarative: parsedLineForDeclarative.StringWithFlags(tree.FmtSimple | tree.FmtTagDollarQuotes),
	}
	if res.isModified() {
		t.Logf("Comparator testing framework modifies line %q to %q (for legacy) and to %q (for declarative)",
			line.original, res.legacy, res.declarative)
	}
	return res
}

func willLineBeExecutedInDSC(
	ctx context.Context, t *testing.T, line string, conn *gosql.Conn,
) bool {
	if isInATransaction(ctx, t, conn) {
		return false
	}
	// `EXPLAIN(DDL,SHAPE) stmt` returns an error if statement is not supported in the DSC.
	_, err := conn.ExecContext(ctx, fmt.Sprintf("EXPLAIN(DDL, SHAPE) %s", line))
	return err == nil
}

// modifyAlterPKWithSamePKColsButDifferentSharding modifies any ALTER PK stmt in
// `line` if the current/old primary index is hash-sharded and ALTER PK would
// leave the shard column unused, in which case the declarative schema changer
// would also drop it (but legacy schema changer wouldn't).
// To have a converged behavior, if `parsedStmts` contains such a ALTER PK, we
// would append a `ALTER TABLE .. DROP COLUMN IF EXISTS old-shard-col` to it, so
// that legacy schema changer will also have that old shard column dropped.
// See commentary on `needsToDropOldShardColFn` for criteria of such ALTER PK.
func modifyAlterPKWithSamePKColsButDifferentSharding(
	ctx context.Context, t *testing.T, parsedStmts statements.Statements, conn *gosql.Conn,
) (newParsedStmts statements.Statements) {
	// getPKShardingInfo retrieve all sharding related information from table
	// `name`'s PK.
	getPKShardingInfo := func(name *tree.UnresolvedObjectName) (isSharded bool, shardColName string, shardBuckets int, columnNames []string) {
		var shardingExists bool
		row := conn.QueryRowContext(ctx, fmt.Sprintf(
			`
SELECT (crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' -> 'primaryIndex' -> 'sharded') ? 'isSharded' AS sharded
FROM system.descriptor 
WHERE id = '%v'::REGCLASS
`, name.String()))
		require.NoError(t, row.Scan(&shardingExists))
		if !shardingExists {
			return isSharded, shardColName, shardBuckets, columnNames
		}

		row = conn.QueryRowContext(ctx, fmt.Sprintf(
			`
WITH sharded AS
 (SELECT (crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' -> 'primaryIndex' -> 'sharded') AS sharded 
  FROM system.descriptor 
  WHERE id = '%v'::REGCLASS)          
SELECT sharded ->> 'isSharded' as "isSharded",
       sharded ->> 'name' as "shardColName",
       sharded ->> 'shardBuckets' as "shardBuckets"
FROM sharded;`, name.String()))
		require.NoError(t, row.Scan(&isSharded, &shardColName, &shardBuckets))

		rows, err := conn.QueryContext(ctx, fmt.Sprintf(
			`    
SELECT jsonb_array_elements(crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' -> 'primaryIndex' -> 'sharded' -> 'columnNames')
FROM system.descriptor
WHERE id = '%v'::REGCLASS;`, name.String()))
		require.NoError(t, err)
		rowsAsStr, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		for _, rowAsStr := range rowsAsStr {
			columnNames = append(columnNames, strings.Trim(rowAsStr[0], `"`))
		}

		return isSharded, shardColName, shardBuckets, columnNames
	}

	// needsToDropOldShardColFn is a helper to determine if we need to drop the old
	// shard column from the old PK after altering to the new PK.
	// Currently, we do if (the old PK is hash-sharded) && (the new PK is key'ed on
	// same columns as the old PK) && (new PK is not hash-sharded || new PK is
	// hash-sharded but with a different bucket_count).
	needsToDropOldShardColFn := func(
		tableName *tree.UnresolvedObjectName, newPKColumns tree.IndexElemList, newPKShardingDef *tree.ShardedIndexDef, newPKStorageParams tree.StorageParams,
	) (needsToDrop bool, oldShardColToDrop string) {
		// Two helpers to determine if two shard buckets are the same and if two
		// list of columns are the same.
		sameShardBuckets := func(
			currShardBuckets int32, newPKShardingDef *tree.ShardedIndexDef, newPKStorageParams tree.StorageParams,
		) bool {
			semaCtx := tree.MakeSemaContext()
			evalCtx := eval.Context{
				Settings: cluster.MakeTestingClusterSettings(),
			}
			newShardBuckets, err := tabledesc.EvalShardBucketCount(ctx, &semaCtx, &evalCtx, newPKShardingDef.ShardBuckets, newPKStorageParams)
			if err != nil {
				panic(errors.AssertionFailedf("programming error: cannot get new PK's shard buckets"))
			}
			return currShardBuckets == newShardBuckets
		}
		sameColumns := func(colNames []string, columns tree.IndexElemList) bool {
			if len(colNames) != len(columns) {
				return false
			}
			for i := range colNames {
				if colNames[i] != columns[i].Column.String() {
					return false
				}
			}
			return true
		}

		isSharded, shardColName, shardBuckets, columnNames := getPKShardingInfo(tableName)
		if !isSharded {
			return false, ""
		}
		if !sameColumns(columnNames, newPKColumns) {
			return false, ""
		}
		if newPKShardingDef != nil && sameShardBuckets(int32(shardBuckets), newPKShardingDef, newPKStorageParams) {
			return false, ""
		}
		return true, shardColName
	}

	for _, parsedStmt := range parsedStmts {
		newParsedStmts = append(newParsedStmts, parsedStmt)
		var tableName *tree.UnresolvedObjectName
		var shardColName string
		var needsToDropOldShardCol bool
		switch ast := parsedStmt.AST.(type) {
		case *tree.AlterTable:
			tableName = ast.Table
			for _, cmd := range ast.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAlterPrimaryKey:
					needsToDropOldShardCol, shardColName = needsToDropOldShardColFn(ast.Table, cmd.Columns, cmd.Sharded, cmd.StorageParams)
				case *tree.AlterTableAddConstraint:
					if alterTableAddPK, ok := cmd.ConstraintDef.(*tree.UniqueConstraintTableDef); ok &&
						alterTableAddPK.PrimaryKey {
						needsToDropOldShardCol, shardColName = needsToDropOldShardColFn(ast.Table, alterTableAddPK.Columns, alterTableAddPK.Sharded, alterTableAddPK.StorageParams)
					}
				}
			}
		}
		if needsToDropOldShardCol {
			// Both old and new PK are sharded, and they have the same columns, and they have different bucket_count,
			// then we need to drop the old shard column.
			parsedCommit, err := parser.ParseOne("commit")
			require.NoError(t, err)
			parsedDropRowID, err := parser.ParseOne(fmt.Sprintf("ALTER TABLE %v DROP COLUMN IF EXISTS %v", tableName, shardColName))
			require.NoError(t, err)
			newParsedStmts = append(newParsedStmts, parsedCommit, parsedDropRowID)
		}
	}
	return newParsedStmts
}

// modifySetDeclarativeSchemaChangerMode skips stmts that attempt to alter
// schema changer mode via session variable "use_declarative_schema_changer" or
// cluster setting "sql.schema.force_declarative_statements".
func modifySetDeclarativeSchemaChangerMode(
	_ context.Context, _ *testing.T, parsedStmts statements.Statements, _ *gosql.Conn,
) (newParsedStmts statements.Statements) {
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
	return newParsedStmts
}

// modifyAlterPKWithRowIDCol modifies any ALTER PK stmt in `line` if the
// current/old primary index column is `rowid` by appending a `DROP COLUMN IF
// EXISTS rowid` to it, so that legacy schema changer will converge to
// declarative schema changer (in which ALTER PK will already drop the `rowid`
// column).
func modifyAlterPKWithRowIDCol(
	ctx context.Context, t *testing.T, parsedStmts statements.Statements, tdb *gosql.Conn,
) (newParsedStmts statements.Statements) {
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
		}
	}

	return newParsedStmts
}

// modifyExprsReferencingSequencesWithTrue modifies any expressions in `line`
// that references sequences to "True".
func modifyExprsReferencingSequencesWithTrue(
	_ context.Context, t *testing.T, parsedStmts statements.Statements, _ *gosql.Conn,
) (newParsedStmts statements.Statements) {
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
		newParsedStmts = append(newParsedStmts, parsedStmt)
	}

	return newParsedStmts
}

// modifyLineMultipleAlterTables modifies line so that if there are multiple
// ALTERs in one ALTER TABLE command, they are split up into multiple ALTER
// TABLE statements.
func modifyLineMultipleAlterTables(
	ctx context.Context, t *testing.T, parsedStmts statements.Statements, conn *gosql.Conn,
) (newParsedStmts statements.Statements) {
	for _, parsedStmt := range parsedStmts {
		atCmd, ok := parsedStmt.AST.(*tree.AlterTable)
		if !ok || len(atCmd.Cmds) == 1 {
			newParsedStmts = append(newParsedStmts, parsedStmt)
			continue
		}
		for _, cmd := range atCmd.Cmds {
			newParsedStmts = append(
				newParsedStmts,
				statements.Statement[tree.Statement]{
					AST: &tree.BeginTransaction{},
				},
				statements.Statement[tree.Statement]{
					AST: &tree.AlterTable{
						Table:    atCmd.Table,
						IfExists: atCmd.IfExists,
						Cmds:     []tree.AlterTableCmd{cmd},
					},
				},
				statements.Statement[tree.Statement]{
					AST: &tree.CommitTransaction{},
				},
			)
		}
	}
	return newParsedStmts
}

// modifyLineToSkipValidateConstraint modifies line to skip any `VALIDATE
// CONSTRAINT` command. Validating a constraint in DSC results in a validated
// constraint with new constraintID, whereas in LSC it retains the old
// constraintID. This would subsequently cause the descriptor state identity
// check to fail so we skip/erase any VALIDATE CONSTRAINT command into the
// framework.
func modifyLineToSkipValidateConstraint(
	ctx context.Context, t *testing.T, parsedStmts statements.Statements, conn *gosql.Conn,
) (newParsedStmts statements.Statements) {
	for _, parsedStmt := range parsedStmts {
		if atCmd, ok := parsedStmt.AST.(*tree.AlterTable); ok {
			nonValidateConstraintCmds := make([]tree.AlterTableCmd, 0)
			for _, cmd := range atCmd.Cmds {
				if _, ok := cmd.(*tree.AlterTableValidateConstraint); !ok {
					nonValidateConstraintCmds = append(nonValidateConstraintCmds, cmd)
				}
			}
			if len(nonValidateConstraintCmds) != 0 {
				parsedStmt.AST.(*tree.AlterTable).Cmds = nonValidateConstraintCmds
				newParsedStmts = append(newParsedStmts, parsedStmt)
			}
		} else {
			newParsedStmts = append(newParsedStmts, parsedStmt)
		}
	}
	return newParsedStmts
}

// modifyCreateTempTable skips `CREATE TEMP TABLE` statement because its parent schema name,
// constructed from sessionID, is naturally going to be different between the two clusters.
// In short of a more clever way to bake this difference into the metadata identity check,
// we decided to just skip those statements, for now.
func modifyCreateTempTable(
	_ context.Context, t *testing.T, parsedStmts statements.Statements, _ *gosql.Conn,
) (newParsedStmts statements.Statements) {
	for _, parsedStmt := range parsedStmts {
		if ast, ok := parsedStmt.AST.(*tree.CreateTable); ok && ast.Persistence.IsTemporary() {
			continue
		}
		newParsedStmts = append(newParsedStmts, parsedStmt)
	}
	return newParsedStmts
}

// requireNoErrOrSameErrCode require errors from executing some statement
// from legacy and declarative schema changer clusters to be both nil or
// both PQ error with same code.
func requireNoErrOrSameErrCode(t *testing.T, line Line, errLegacy, errDeclarative error) {
	if errLegacy == nil && errDeclarative == nil {
		return
	}

	if errLegacy == nil {
		t.Fatalf("statement %q failed with declarative schema changer "+
			"(but succeeded with legacy schema changer): %v", line.declarative, errDeclarative)
	}
	if errDeclarative == nil {
		t.Fatalf("statement %q failed with legacy schema changer "+
			"(but succeeded with declarative schema changer): %v", line.legacy, errLegacy)
	}
	errLegacyPQCode := getPQErrCode(errLegacy)
	errDeclarativePQCode := getPQErrCode(errDeclarative)
	if errLegacyPQCode == "" || errDeclarativePQCode == "" {
		t.Fatalf("executing statement %q results in non-PQ error:  legacy=%v, declarative=%v ",
			line.original, errLegacy, errDeclarative)
	}
	if errLegacyPQCode != errDeclarativePQCode {
		t.Fatalf("executing statement %q results in different error code: legacy=%v (%v), declarative=%v (%v)",
			line.original, errLegacyPQCode.Name(), errLegacyPQCode, errDeclarativePQCode.Name(), errDeclarativePQCode)
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
	ctx context.Context, t *testing.T, legacy, declarative *gosql.Conn, lastExecutedLine string,
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
		err := errors.Newf("descriptors mismatch with diff (- is legacy, + is declarative):\n%v", diff)
		err = errors.Wrapf(err, "\ndescriptors diverge after executing %q", lastExecutedLine)
		t.Fatalf(err.Error())
	}
}
