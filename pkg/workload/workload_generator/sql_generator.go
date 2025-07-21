// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// placeholderRewriter handles both simple comparisons and IN-lists.
// It is the visitor interface implementation for walking the AST.
type placeholderRewriter struct {
	schemas   map[string]*TableSchema
	tableName string
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
			if row[columnIndex["database_name"]] != dbName {
				continue
			}
			// 5b) Internal “job id=” lines are skipped.
			app := row[columnIndex["application_name"]]
			if strings.Contains(app, "job id=") {
				continue
			}
			// 5c) txnID and raw SQL are extracted.
			txnID := row[columnIndex["txn_fingerprint_id"]]
			rawSQL := row[columnIndex["key"]]
			// These are unhandled clauses for now.
			var skipRE = regexp.MustCompile(`\b(DEALLOCATE|WHEN)\b`)
			if skipRE.MatchString(rawSQL) {
				continue // dropping any statement containing DEALLOCATE or WHEN
			}
			// The TSV’s string literal are unquoted if present:
			if len(rawSQL) >= 2 && rawSQL[0] == '"' && rawSQL[len(rawSQL)-1] == '"' {
				// Outer quotes are stripped and every "" is turned into ".
				rawSQL = rawSQL[1 : len(rawSQL)-1]
				rawSQL = strings.ReplaceAll(rawSQL, `""`, `"`)
			}

			// 5d) The sql query is processed to replace _ and __more__ with new placeholders which contain information about the column they refer to.
			rewritten, err := replacePlaceholders(rawSQL, allSchemas)
			if err != nil {
				f.Close()
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
	// A file for unhandled statements is created other than the read and write.
	// Statements which are still left with bare _ and __more__ placeholders after sql handling are written there.
	// This helps to identify which statements were not handled by the generator. It also lets the workload generator run smoothly without failing on unhandled statements.
	// If wanted, the transactions that end up in that file can be handled manually.
	var outPathRead, outPathWrite, outPathUnhandled string
	if fi, err := os.Stat(sqlLocation); err == nil && fi.IsDir() {
		outPathRead = filepath.Join(sqlLocation, dbName+"_read.sql")
		outPathWrite = filepath.Join(sqlLocation, dbName+"_write.sql")
		outPathUnhandled = filepath.Join(sqlLocation, dbName+"_unhandled.sql")
	}
	outReadFile, errRead := os.Create(outPathRead)
	if errRead != nil {
		return errors.Wrapf(errRead, "creating %s", outPathRead)
	}
	defer outReadFile.Close()
	outWriteFile, errWrite := os.Create(outPathWrite)
	if errWrite != nil {
		return errors.Wrapf(errWrite, "creating %s", outPathWrite)
	}
	defer outWriteFile.Close()
	outUnhandledFile, errUnhandled := os.Create(outPathUnhandled)
	if errUnhandled != nil {
		return errors.Wrapf(errUnhandled, "creating %s", outPathUnhandled)
	}
	defer outUnhandledFile.Close()

	writeTransaction(txnOrder, txnMap, outReadFile, outWriteFile, outUnhandledFile)
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
			// 1) Rewrite everything in the SET clause.
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
		out = append(out, fmtCtx.CloseAndGetString())
	}
	// Join multiple statements with newline.
	return strings.Join(out, "\n"), nil
}

// buildPlaceholderRewriter creates a placeholderRewriter for the given statement.
// It extracts the table name from the statement and initializes the rewriter with the schema map.
func buildPlaceholderRewriter(
	stmt statements.Statement[tree.Statement], allSchemas map[string]*TableSchema,
) *placeholderRewriter {
	var tableName string
	switch stmt := stmt.AST.(type) {
	case *tree.Insert:
		// ins.Table is a TableName or AliasedTableExpr
		tableName = extractTableName(stmt.Table)
	case *tree.Update:
		tableName = extractTableName(stmt.Table)
	case *tree.Delete:
		tableName = extractTableName(stmt.Table)
	case *tree.Select:
		// Pulling from the first FROM table (skip joins/withs)
		if sc, ok := stmt.Select.(*tree.SelectClause); ok {
			if len(sc.From.Tables) > 0 {
				tableName = extractTableName(sc.From.Tables[0])
			}
		}
	}
	// Expression-level rewrites (WHERE, IN, BETWEEN, comparisons) are handled using this visitor.
	rw := &placeholderRewriter{
		schemas:   allSchemas,
		tableName: tableName,
	}
	return rw
}
