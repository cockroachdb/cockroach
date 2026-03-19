// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

// TableWriter provides upsert and delete operations for a Table.
type TableWriter struct {
	conn  *gosql.DB
	table Table

	upsertStmt string
	deleteStmt string
}

// NewTableWriter creates a TableWriter for the given table and connection.
func NewTableWriter(conn *gosql.DB, table Table) *TableWriter {
	var columns, params, updates []string
	for _, col := range table.Cols {
		if col.IsComputed {
			continue
		}
		columns = append(columns, fmt.Sprintf(`"%s"`, col.Name))
		params = append(params,
			fmt.Sprintf("$%d::%s", len(params)+1, col.DataType.SQLString()))
		updates = append(updates, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col.Name, col.Name))
	}

	var pkColumns []string
	for _, col := range table.PrimaryKey {
		pkColumns = append(pkColumns, fmt.Sprintf(`"%s"`, table.Cols[col].Name))
	}

	upsertStmt := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
		table.Name,
		strings.Join(columns, ", "),
		strings.Join(params, ", "),
		strings.Join(pkColumns, ", "),
		strings.Join(updates, ", "),
	)

	var pkComponents []string
	for i, col := range table.Cols {
		if col.IsComputed {
			continue
		}
		if table.PrimaryKeyComponents[i] {
			pkComponents = append(pkComponents,
				fmt.Sprintf(`"%s" = $%d::%s`, col.Name, len(pkComponents)+1, col.DataType.SQLString()))
		}
	}
	deleteStmt := fmt.Sprintf(
		`DELETE FROM %s WHERE %s`,
		table.Name, strings.Join(pkComponents, " AND "),
	)

	return &TableWriter{
		conn:       conn,
		table:      table,
		upsertStmt: upsertStmt,
		deleteStmt: deleteStmt,
	}
}

// UpsertRow inserts or updates a row. The row values must correspond to
// the non-computed columns of the table.
func (w *TableWriter) UpsertRow(ctx context.Context, row []any) error {
	_, err := w.conn.ExecContext(ctx, w.upsertStmt, row...)
	return errors.Wrapf(err, "UpsertRow '%s'", w.upsertStmt)
}

// DeleteRow deletes the row identified by the primary key values in row.
func (w *TableWriter) DeleteRow(ctx context.Context, row []any) error {
	pkValues := []any{}
	valIndex := 0
	for schemaIndex, col := range w.table.Cols {
		if col.IsComputed {
			continue
		}
		if w.table.PrimaryKeyComponents[schemaIndex] {
			pkValues = append(pkValues, row[valIndex])
		}
		valIndex++
	}
	_, err := w.conn.ExecContext(ctx, w.deleteStmt, pkValues...)
	return errors.Wrapf(err, "DeleteRow '%s'", w.deleteStmt)
}
