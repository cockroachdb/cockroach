package conflict

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/pkg/errors"
)

type writer struct {
	conn       *sql.DB
	table      rand.Table
	upsertStmt string
	deleteStmt string
}

func newWriter(ctx context.Context, conn *sql.DB, table rand.Table) (*writer, error) {
	// Build upsert statement based on table schema
	columns := make([]string, len(table.Cols))
	params := make([]string, len(table.Cols))
	updates := make([]string, len(table.Cols))
	pkColumns := []string{}

	for i, col := range table.Cols {
		columns[i] = fmt.Sprintf(`"%s"`, col.Name)
		params[i] = fmt.Sprintf("$%d", i+1)
		updates[i] = fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col.Name, col.Name)
	}

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

	// Prepare delete statement using only primary key columns
	pkColumns = make([]string, 0, len(table.PrimaryKey))
	for i, col := range table.PrimaryKey {
		pkColumns = append(pkColumns, fmt.Sprintf(`"%s" = $%d`, table.Cols[col].Name, i+1))
	}

	deleteStmt := fmt.Sprintf(`
		DELETE FROM %s
		WHERE %s
	`, table.Name, strings.Join(pkColumns, " AND "))

	return &writer{conn: conn, upsertStmt: upsertStmt, deleteStmt: deleteStmt, table: table}, nil
}

func (w *writer) upsertRow(ctx context.Context, row []any) error {
	_, err := w.conn.Exec(w.upsertStmt, row...)
	return errors.Wrapf(err, "upsertRow '%s'", w.upsertStmt)
}

func (w *writer) deleteRow(ctx context.Context, row []any) error {
	pkValues := make([]any, len(w.table.PrimaryKey))
	for i, col := range w.table.PrimaryKey {
		pkValues[i] = row[col]
	}
	_, err := w.conn.Exec(w.deleteStmt, pkValues...)
	return errors.Wrapf(err, "deleteRow '%s'", w.deleteStmt)
}
