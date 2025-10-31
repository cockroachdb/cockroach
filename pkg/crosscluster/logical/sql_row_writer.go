// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
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
	session isql.Session

	insert isql.PreparedStatement
	update isql.PreparedStatement
	delete isql.PreparedStatement

	scratchDatums tree.Datums
	columns       []string
}

func (s *sqlRowWriter) setOriginTimestamp(
	ctx context.Context, originTimestamp hlc.Timestamp,
) error {
	return s.session.ModifySession(ctx, func(m sessionmutator.SessionDataMutator) {
		m.Data.OriginTimestampForLogicalDataReplication = originTimestamp
	})
}

// DeleteRow deletes a row from the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *sqlRowWriter) DeleteRow(
	ctx context.Context, originTimestamp hlc.Timestamp, oldRow tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]
	s.scratchDatums = append(s.scratchDatums, oldRow...)

	err := s.setOriginTimestamp(ctx, originTimestamp)
	if err != nil {
		return err
	}

	rowsAffected, err := s.session.ExecutePrepared(ctx, s.delete, s.scratchDatums)
	if err != nil {
		return errors.Wrap(err, "deleting row")
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
	s.scratchDatums = append(s.scratchDatums, row...)

	err := s.setOriginTimestamp(ctx, originTimestamp)
	if err != nil {
		return err
	}

	rowsImpacted, err := s.session.ExecutePrepared(ctx, s.insert, s.scratchDatums)
	if err != nil {
		return errors.Wrap(err, "inserting row")
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
	s.scratchDatums = append(s.scratchDatums, oldRow...)
	s.scratchDatums = append(s.scratchDatums, newRow...)

	err := s.setOriginTimestamp(ctx, originTimestamp)
	if err != nil {
		return err
	}

	rowsAffected, err := s.session.ExecutePrepared(ctx, s.update, s.scratchDatums)
	if err != nil {
		return errors.Wrap(err, "updating row")
	}
	if rowsAffected != 1 {
		return errStalePreviousValue
	}
	return err
}

func newSQLRowWriter(
	ctx context.Context, table catalog.TableDescriptor, session isql.Session,
) (*sqlRowWriter, error) {
	columnsToDecode := getColumnSchema(table)
	columns := make([]string, len(columnsToDecode))
	for i, col := range columnsToDecode {
		columns[i] = col.column.GetName()
	}

	insert, insertParamTypes, err := newInsertStatement(table)
	if err != nil {
		return nil, err
	}
	preparedInsert, err := session.Prepare(ctx, "insert", insert, insertParamTypes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare insert statement")
	}

	update, updateParamTypes, err := newUpdateStatement(table)
	if err != nil {
		return nil, err
	}
	preparedUpdate, err := session.Prepare(ctx, "update", update, updateParamTypes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare update statement")
	}

	delete, deleteParamTypes, err := newDeleteStatement(table)
	if err != nil {
		return nil, err
	}
	preparedDelete, err := session.Prepare(ctx, "delete", delete, deleteParamTypes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare delete statement")
	}

	return &sqlRowWriter{
		session: session,
		insert:  preparedInsert,
		update:  preparedUpdate,
		delete:  preparedDelete,
		columns: columns,
	}, nil
}
