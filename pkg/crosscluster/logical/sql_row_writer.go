// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isession"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// errStalePreviousValue is returned if the row supplied to UpdateRow,
// UpdateTombstone, or DeleteRow does not match the value in the local
// database.
var errStalePreviousValue = errors.New("stale previous value")

// sqlRowWriter is configured to write rows to a specific table and descriptor
// version.
type sqlRowWriter struct {
	session *isession.InternalSession

	insert          isession.InternalStatement
	update          isession.InternalStatement
	delete          isession.InternalStatement
	originTimestamp isession.InternalStatement

	scratchDatums tree.Datums
	columns       []string
}

func (s *sqlRowWriter) setOriginTimestamp(
	ctx context.Context, originTimestamp hlc.Timestamp,
) error {
	_, err := s.session.Execute(ctx, s.originTimestamp, []tree.Datum{tree.NewDString(originTimestamp.AsOfSystemTime())})
	return err
}

// DeleteRow deletes a row from the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *sqlRowWriter) DeleteRow(
	ctx context.Context, originTimestamp hlc.Timestamp, oldRow tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]

	for _, d := range oldRow {
		s.scratchDatums = append(s.scratchDatums, d)
	}

	err := s.setOriginTimestamp(ctx, originTimestamp)
	if err != nil {
		return err
	}

	rowsAffected, err := s.session.Execute(ctx, s.delete, s.scratchDatums)
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
func (s *sqlRowWriter) InsertRow(
	ctx context.Context, originTimestamp hlc.Timestamp, row tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]
	for _, d := range row {
		s.scratchDatums = append(s.scratchDatums, d)
	}
	rowsImpacted, err := s.session.Execute(ctx, s.insert, s.scratchDatums)
	if err != nil {
		return err
	}
	if rowsImpacted != 1 {
		return errors.AssertionFailedf("expected 1 row impacted, got %d", rowsImpacted)
	}
	return nil
}

// UpdateRow updates a row in the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *sqlRowWriter) UpdateRow(
	ctx context.Context, originTimestamp hlc.Timestamp, oldRow tree.Datums, newRow tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]

	for _, d := range oldRow {
		s.scratchDatums = append(s.scratchDatums, d)
	}
	for _, d := range newRow {
		s.scratchDatums = append(s.scratchDatums, d)
	}

	err := s.setOriginTimestamp(ctx, originTimestamp)
	if err != nil {
		return err
	}

	rowsAffected, err := s.session.Execute(ctx, s.update, s.scratchDatums)
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return errStalePreviousValue
	}
	return err
}

func newSQLRowWriter(
	ctx context.Context, table catalog.TableDescriptor, session *isession.InternalSession,
) (*sqlRowWriter, error) {
	columnsToDecode := getColumnSchema(table)
	columns := make([]string, len(columnsToDecode))
	for i, col := range columnsToDecode {
		columns[i] = col.column.GetName()
	}

	// TODO(jeffswenson): figure out how to manage prepared statements and
	// transactions in an internal executor. The original plan was to prepare
	// statements on initialization then reuse them, but the internal executor
	// is scoped to a single transaction and I couldn't figure out how to
	// maintain prepared statements across different instances of the internal
	// executor.

	insert, insertParamTypes, err := newInsertStatement(table)
	if err != nil {
		return nil, err
	}
	preparedInsert, err := session.Prepare(ctx, "insert", insert, insertParamTypes)
	if err != nil {
		return nil, err
	}

	update, updateParamTypes, err := newUpdateStatement(table)
	if err != nil {
		return nil, err
	}
	preparedUpdate, err := session.Prepare(ctx, "update", update, updateParamTypes)
	if err != nil {
		return nil, err
	}

	delete, deleteParamTypes, err := newDeleteStatement(table)
	if err != nil {
		return nil, err
	}
	preparedDelete, err := session.Prepare(ctx, "delete", delete, deleteParamTypes)
	if err != nil {
		return nil, err
	}

	setOriginTimestamp, originTimestampParamTypes, err := setOriginTimestampStatement()
	if err != nil {
		return nil, err
	}
	preparedSetOriginTimestamp, err := session.Prepare(ctx, "set_origin_timestamp", setOriginTimestamp, originTimestampParamTypes)
	if err != nil {
		return nil, err
	}

	return &sqlRowWriter{
		session:         session,
		insert:          preparedInsert,
		update:          preparedUpdate,
		delete:          preparedDelete,
		originTimestamp: preparedSetOriginTimestamp,
		columns:         columns,
	}, nil
}
