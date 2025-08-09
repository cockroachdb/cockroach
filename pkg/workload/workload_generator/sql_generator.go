// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// internalSchemas is a slice of schema names that are considered internal.
// If more names are identified later, just this variable can be updated.
var internalSchemas = []string{"information_schema"}

// columnName represents a column in a table, with its name and an optional tableAlias.
type columnName struct {
	name       string
	tableAlias string
}

// newColumnName creates a new columnName instance from column name only.
func newColumnNameWithName(colName string) *columnName {
	return &columnName{name: colName}
}

// newColumnName creates a new columnName instance from tree.UnresolvedName.
func newColumnName(u *tree.UnresolvedName) *columnName {
	c := &columnName{name: u.Parts[0]}
	if u.NumParts > 1 {
		c.tableAlias = u.Parts[1]
	}
	return c
}

// placeholderRewriter handles both simple comparisons and IN-lists.
// It is the visitor interface implementation for walking the AST.
type placeholderRewriter struct {
	schemas    map[string]*TableSchema
	tableNames []string
	// tableAliases is a map of table aliases to their names.
	tableAliases map[string]string
}

// generateWorkload extracts and organizes SQL workload from CockroachDB debug logs.
// It scans each node’s statement statistics TSV in the unzipped debug directory, filters
// statements for the given database, rewrites queries using schema metadata, groups them
// by transaction fingerprint, and writes out separate read and write workload files.
//
// Parameters:
//
//	debugZip    – path to the unzipped debug-logs directory (contains “nodes/…” subfolders)
//	allSchemas  – map of table names to *TableSchema objects (as returned by generateDDLs)
//	dbName      – the target database name to filter statements by
//	sqlLocation – directory in which to create the output SQL files
//
// Steps:
//  1. Initialize txnOrder (first-seen transaction IDs) and txnMap (map txnID → []SQL).
//  2. For each node directory under debugZip/nodes:
//     a) Open “crdb_internal.node_statement_statistics.txt” and scan its TSV contents.
//     b) Read the header row to map column names to indices.
//     c) For each data row:
//     • Filter out rows not matching dbName.
//     • Skip internal “job id=” entries.
//     • Extract txn_fingerprint_id and raw SQL (column “key”), dropping statements
//     containing DEALLOCATE or WHEN.
//     • Unquote the TSV literal (strip outer quotes, unescape "").
//     • Call replacePlaceholders(rawSQL, allSchemas) to substitute schema-driven placeholders.
//     • Append the rewritten statement to txnMap[txnID], and record txnID in txnOrder if first-seen.
//  3. After all nodes are processed, determine output file paths:
//     sqlLocation/<dbName>_read.sql and sqlLocation/<dbName>_write.sql.
//  4. Create or truncate these two files, then defer their closure.
//  5. Call writeTransaction(txnOrder, txnMap, outReadFile, outWriteFile) to emit the workloads.
//  6. Propagate any I/O, scanning, or SQL-rewriting errors.
//
// Returns:
//
//	error – if any file I/O, scanner error, or placeholder-replacement error occurs.
func generateWorkload(
	debugLogs string, allSchemas map[string]*TableSchema, dbName, sqlLocation string,
) error {
	// 1) Grouping structures are prepared.
	txnOrder := make([]string, 0)       // first-seen txn IDs
	txnMap := make(map[string][]string) // txnID → []SQL statements

	// 2) Iterating node directories (nodes/1, nodes/2, …).
	nodesRoot := filepath.Join(debugLogs, "nodes")
	for node := 1; ; node++ {
		nodeDir := filepath.Join(nodesRoot, strconv.Itoa(node))
		if fi, err := os.Stat(nodeDir); err != nil || !fi.IsDir() {
			break
		}

		// 3) The statistics tsv is opened to read the transactions from the debug logs.
		statsPath := filepath.Join(nodeDir, "crdb_internal.node_statement_statistics.txt")
		f, err := os.Open(statsPath)
		if err != nil {
			return errors.Wrapf(err, "opening %s", statsPath)
		}
		scanner := bufio.NewScanner(f)

		// 4) Header row and map column→index are read.
		if !scanner.Scan() {
			if err := f.Close(); err != nil {
				return err
			}
			continue
		}
		columnIndex, err := getColumnIndexes(scanner, f, statsPath)
		if err != nil {
			return err
		}

		// 5) Each data row is scanned.
		for scanner.Scan() {
			row := strings.Split(scanner.Text(), "\t")
			// 5a) Filtering by database_name.
			if row[columnIndex[databaseName]] != dbName &&
				(row[columnIndex[databaseName]] != "defaultdb" ||
					!strings.Contains(row[columnIndex[keyColumnName]], dbName) ||
					isInternalTable(row, dbName, columnIndex)) {
				continue
			}
			// 5b) Internal “job id=” lines are skipped.
			app := row[columnIndex[applicationName]]
			if strings.Contains(app, "job id=") {
				continue
			}
			// 5c) txnID and raw SQL are extracted.
			txnID := row[columnIndex[txnFingerprintID]]
			rawSQL := row[columnIndex[keyColumnName]]
			// The TSV’s string literals are unquoted if present:
			if len(rawSQL) >= 2 && rawSQL[0] == '"' && rawSQL[len(rawSQL)-1] == '"' {
				// Outer quotes are stripped and every "" is turned into ".
				rawSQL = rawSQL[1 : len(rawSQL)-1]
				rawSQL = strings.ReplaceAll(rawSQL, `""`, `"`)
			}

			// 5d) The sql query is processed to replace _ and __more__ with new placeholders which contain information about the column they refer to.
			rewritten, err := replacePlaceholders(rawSQL, allSchemas)
			if err != nil {
				if errClose := f.Close(); errClose != nil {
					// Wrap the original placeholder-rewrite error, then attach the close error.
					return errors.WithSecondaryError(
						errors.Wrapf(err, "rewriting SQL %q", rawSQL),
						errClose,
					)
				}
				return errors.Wrapf(err, "rewriting SQL %q", rawSQL)
			}

			// 5e) Grouping into txnMap, tracking first-seen order.
			if _, seen := txnMap[txnID]; !seen {
				txnOrder = append(txnOrder, txnID)
			}
			txnMap[txnID] = append(txnMap[txnID], rewritten)
		}

		if err := f.Close(); err != nil {
			return err
		}
		if err := scanner.Err(); err != nil {
			return errors.Wrapf(err, "scanning %s", statsPath)
		}
	}

	// 6) Writing out <sqlLocation><Read/Write>/<dbName>.sql .
	errTxnWrite := writeTransaction(txnOrder, txnMap, dbName, sqlLocation)
	if errTxnWrite != nil {
		return errTxnWrite
	}
	return nil
}

// replacePlaceholders parses the given SQL and locates all the _, __more__ placeholders.
// The placeholders are then matched to what column's data do they represent and are replaced with information about that column for data generation.
func replacePlaceholders(rawSQL string, allSchemas map[string]*TableSchema) (string, error) {
	stmts, err := parser.Parse(rawSQL)
	if err != nil {
		return "", err
	}

	var out []string
	for _, stmt := range stmts {
		// INSERT…VALUES (<placeholders>) is rewritten.
		if ins, ok := stmt.AST.(*tree.Insert); ok {
			handleInsert(ins, allSchemas)
		}
		// UPDATE ... SET is rewritten.
		if upd, ok := stmt.AST.(*tree.Update); ok {
			// 1) Everything in the SET clause is rewritten.
			handleUpdateSet(upd, allSchemas)
		}
		// Handling limit _
		if sel, ok := stmt.AST.(*tree.Select); ok {
			handleSelectLimit(sel)
		}

		// Setting up the rewriter with the required table name and schemas.
		rewriter := buildPlaceholderRewriter(stmt, allSchemas)
		// Wiring in for the join (col = __) case
		if sel, ok := stmt.AST.(*tree.Select); ok {
			// The SelectClause is unboxed from the Select statement.
			if sc, ok := sel.Select.(*tree.SelectClause); ok {
				for _, tbl := range sc.From.Tables {
					if j, ok := tbl.(*tree.JoinTableExpr); ok {
						// Only handle ON‐joins here
						if on, ok := j.Cond.(*tree.OnJoinCond); ok {
							tree.WalkExpr(rewriter, on.Expr)
						}
					}
				}
			}
		}
		// This covers all the expr relates nodes. So mostly all the where expressions.
		tree.WalkStmt(rewriter, stmt.AST)

		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		stmt.AST.Format(fmtCtx)
		// Grabbing the rewritten SQL from the formatter.
		rewritten := fmtCtx.CloseAndGetString()
		// Cleaning up crdb_internal.force_error() calls.
		// This is most probably used for development purposes to throw particular errors.
		// In our case, since we will never be executing those parts, we are replacing with constant values of error code and error message.
		rewritten = forceErrorRe.ReplaceAllString(rewritten, fmt.Sprintf("crdb_internal.force_error(%s, %s)", forceErrorCode, forceErrorMessage))
		out = append(out, rewritten)
	}
	// Multiple statements are joined with newline.
	return strings.Join(out, "\n"), nil
}

// extractTablesFromExpr recursively extracts table names from a table expression,
// including those in nested JOINs.
func extractTablesFromExpr(expr tree.TableExpr, tables *[]string, aliases map[string]string) {
	switch t := expr.(type) {
	case *tree.TableName, *tree.AliasedTableExpr:
		if name, alias := extractTableName(expr); name != "" {
			*tables = append(*tables, name)
			aliases[alias] = name
		}
	case *tree.JoinTableExpr:
		// Recursively extract tables from both sides of the join
		extractTablesFromExpr(t.Left, tables, aliases)
		extractTablesFromExpr(t.Right, tables, aliases)
	}
}

// buildPlaceholderRewriter creates a placeholderRewriter for the given statement.
// It extracts the table name from the statement and initializes the rewriter with the schema map.
func buildPlaceholderRewriter(
	stmt statements.Statement[tree.Statement], allSchemas map[string]*TableSchema,
) *placeholderRewriter {
	tables := make([]string, 0)
	aliases := make(map[string]string)
	switch s := stmt.AST.(type) {
	case *tree.Insert:
		// ins.Table is a TableName or AliasedTableExpr
		table, alias := extractTableName(s.Table)
		tables = append(tables, table)
		aliases[alias] = table
	case *tree.Update:
		table, alias := extractTableName(s.Table)
		tables = append(tables, table)
		aliases[alias] = table
	case *tree.Delete:
		table, alias := extractTableName(s.Table)
		tables = append(tables, table)
		aliases[alias] = table
	case *tree.Select:
		// Pulling from all FROM tables and joins (including nested joins)
		if sc, ok := s.Select.(*tree.SelectClause); ok {
			for _, tblExpr := range sc.From.Tables {
				extractTablesFromExpr(tblExpr, &tables, aliases)
			}
		}
	}
	// Expression-level rewrites (WHERE, IN, BETWEEN, comparisons) are handled using this visitor.
	return &placeholderRewriter{
		schemas:      allSchemas,
		tableNames:   tables,
		tableAliases: aliases,
	}
}

// newPlaceholderRewriter creates a new placeholderRewriter with the provided schemas, table name, and alias.
func newPlaceholderRewriter(
	schemas map[string]*TableSchema, tableName, tableAlias string,
) *placeholderRewriter {
	return &placeholderRewriter{
		schemas:      schemas,
		tableNames:   []string{tableName},
		tableAliases: map[string]string{tableAlias: tableName},
	}
}

// isInternalTable checks if a row represents an internal table by examining if the key column
// contains "<dbName>.<internalSchema>" for any schema in internalSchemas.
func isInternalTable(row []string, dbName string, columnIndex map[string]int) bool {
	keyValue := row[columnIndex[keyColumnName]]
	// Check if the key column contains "<dbName>.<internalSchema>" for any internal schema
	for _, schema := range internalSchemas {
		if strings.Contains(keyValue, dbName+"."+schema) {
			return true
		}
	}
	return false
}
