// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// errStalePreviousValue is returned if the row supplied to UpdateRow,
// UpdateTombstone, or DeleteRow does not match the value in the local
// database.
var errStalePreviousValue = errors.New("stale previous value")

// sqlRowWriter is configured to write rows to a specific table and descriptor
// version.
type sqlRowWriter struct {
	insert statements.Statement[tree.Statement]
	update statements.Statement[tree.Statement]
	delete statements.Statement[tree.Statement]

	scratchDatums []any
	columns       []string
}

// DeleteRow deletes a row from the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *sqlRowWriter) DeleteRow(ctx context.Context, txn isql.Txn, oldRow cdcevent.Row) error {
	s.scratchDatums = s.scratchDatums[:0]

	var err error
	s.scratchDatums, err = appendDatums(s.scratchDatums, oldRow, s.columns)
	if err != nil {
		return err
	}

	rowsAffected, err := txn.ExecParsed(ctx, "delete", txn.KV(),
		sessiondata.NoSessionDataOverride,
		s.delete,
		s.scratchDatums...,
	)
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return errStalePreviousValue
	}
	return nil
}

// InsertRow inserts a row into the table. It will return an error if the row
// already exists.
func (s *sqlRowWriter) InsertRow(ctx context.Context, txn isql.Txn, row cdcevent.Row) error {
	s.scratchDatums = s.scratchDatums[:0]

	var err error
	s.scratchDatums, err = appendDatums(s.scratchDatums, row, s.columns)
	if err != nil {
		return err
	}

	_, err = txn.ExecParsed(ctx, "insert", txn.KV(),
		sessiondata.NoSessionDataOverride,
		s.insert,
		s.scratchDatums...,
	)
	return err
}

// UpdateRow updates a row in the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *sqlRowWriter) UpdateRow(
	ctx context.Context, txn isql.Txn, oldRow cdcevent.Row, newRow cdcevent.Row,
) error {
	s.scratchDatums = s.scratchDatums[:0]

	var err error
	s.scratchDatums, err = appendDatums(s.scratchDatums, oldRow, s.columns)
	if err != nil {
		return err
	}

	s.scratchDatums, err = appendDatums(s.scratchDatums, newRow, s.columns)
	if err != nil {
		return err
	}

	rowsAffected, err := txn.ExecParsed(ctx, "update", txn.KV(),
		sessiondata.NoSessionDataOverride,
		s.update,
		s.scratchDatums...,
	)
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return errStalePreviousValue
	}
	return err
}

// appendDatums appends datums for the specified column names from the cdcevent.Row
// to the datums slice and returns the updated slice.
func appendDatums(datums []any, row cdcevent.Row, columnNames []string) ([]any, error) {
	it, err := row.DatumsNamed(columnNames)
	if err != nil {
		return nil, err
	}

	if err := it.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		if dEnum, ok := d.(*tree.DEnum); ok {
			// Override the type to Unknown to avoid a mismatched type OID error
			// during execution. Note that Unknown is the type used by default
			// when a SQL statement is executed without type hints.
			//
			// TODO(jeffswenson): this feels like the wrong place to do this,
			// but its inspired by the implementation in queryBuilder.AddRow.
			dEnum.EnumTyp = types.Unknown
		}
		datums = append(datums, d)
		return nil
	}); err != nil {
		return nil, err
	}

	return datums, nil
}

func newSQLRowWriter(table catalog.TableDescriptor) (*sqlRowWriter, error) {
	physicalColumns := getPhysicalColumns(table)
	columns := make([]string, len(physicalColumns))
	for i, col := range physicalColumns {
		columns[i] = col.GetName()
	}

	// TODO(jeffswenson): figure out how to manage prepared statements and
	// transactions in an internal executor. The original plan was to prepare
	// statements on initialization then reuse them, but the internal executor
	// is scoped to a single transaction and I couldn't figure out how to
	// maintain prepared statements across different instances of the internal
	// executor.

	insert, err := newInsertStatement(table)
	if err != nil {
		return nil, err
	}

	update, err := newUpdateStatement(table)
	if err != nil {
		return nil, err
	}

	delete, err := newDeleteStatement(table)
	if err != nil {
		return nil, err
	}

	return &sqlRowWriter{
		insert:  insert,
		update:  update,
		delete:  delete,
		columns: columns,
	}, nil
}
