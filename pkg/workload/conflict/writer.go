// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package conflict

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/cockroachdb/errors"
)

type writer struct {
	conn  *gosql.DB
	table rand.Table

	upsertStmt string
	deleteStmt string
}

func newWriter(ctx context.Context, conn *gosql.DB, table rand.Table) (*writer, error) {
	var columns, params, updates []string
	for _, col := range table.Cols {
		// Can't update a computed column, so skip it
		if col.IsComputed {
			continue
		}
		columns = append(columns, fmt.Sprintf(`"%s"`, col.Name))
		params = append(params, fmt.Sprintf("$%d", len(params)+1))
		updates = append(updates, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col.Name, col.Name))
	}

	var pkColumns []string
	for _, col := range table.PrimaryKey {
		pkColumns = append(pkColumns, fmt.Sprintf(`"%s"`, table.Cols[col].Name))
	}

	upsertStmt := fmt.Sprintf(`
			INSERT INTO %s (%s)
			VALUES (%s)
			ON CONFLICT (%s) DO UPDATE
			SET %s
		`,
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
			pkComponents = append(pkComponents, fmt.Sprintf(`"%s" = $%d`, col.Name, len(pkComponents)+1))
		}
	}
	deleteStmt := fmt.Sprintf(`
		DELETE FROM %s
		WHERE %s
	`, table.Name, strings.Join(pkComponents, " AND "))

	return &writer{conn: conn, upsertStmt: upsertStmt, deleteStmt: deleteStmt, table: table}, nil
}

func (w *writer) upsertRow(ctx context.Context, row []any) error {
	_, err := w.conn.ExecContext(ctx, w.upsertStmt, row...)
	return errors.Wrapf(err, "upsertRow '%s'", w.upsertStmt)
}

func (w *writer) deleteRow(ctx context.Context, row []any) error {
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
	return errors.Wrapf(err, "deleteRow '%s'", w.deleteStmt)
}
