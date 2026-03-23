// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ErrStalePreviousValue is returned if the row supplied to UpdateRow,
// UpdateTombstone, or DeleteRow does not match the value in the local
// database.
var ErrStalePreviousValue = errors.New("stale previous value")

// RowWriter is configured to write rows to a specific table and descriptor
// version.
type RowWriter struct {
	session isql.Session

	insert isql.PreparedStatement
	update isql.PreparedStatement
	delete isql.PreparedStatement

	scratchDatums tree.Datums
	columns       []string
}

func (s *RowWriter) setOriginTimestamp(ctx context.Context, originTimestamp hlc.Timestamp) error {
	return s.session.ModifySession(ctx, func(m sessionmutator.SessionDataMutator) {
		m.Data.OriginTimestampForLogicalDataReplication = originTimestamp
	})
}

// DeleteRow deletes a row from the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *RowWriter) DeleteRow(
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
		return ErrStalePreviousValue
	}
	return nil
}

// InsertRow inserts a row into the table. It will return an error if the row
// already exists.
func (s *RowWriter) InsertRow(
	ctx context.Context, originTimestamp hlc.Timestamp, row tree.Datums,
) error {
	// We use a savepoint here because LWW may reject the insert if it conflicts
	// with a tombstone or an existing row with a more recent origin timestamp.
	// Without the savepoint, the LWW rejection would abort the entire
	// transaction. Updates and deletes do not need savepoints because a conflict
	// results in zero rows modified, which leaves the transaction in a healthy
	// state.
	err := s.session.Savepoint(ctx, func(ctx context.Context) error {
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
	})
	return err
}

// UpdateRow updates a row in the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *RowWriter) UpdateRow(
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
		return ErrStalePreviousValue
	}
	return err
}

func NewRowWriter(
	ctx context.Context, table catalog.TableDescriptor, session isql.Session,
) (*RowWriter, error) {
	columnsToDecode := GetColumnSchema(table)
	columns := make([]string, len(columnsToDecode))
	for i, col := range columnsToDecode {
		columns[i] = col.Column.GetName()
	}

	insert, insertParamTypes, err := newInsertStatement(table)
	if err != nil {
		return nil, err
	}
	preparedInsert, err := session.Prepare(ctx, fmt.Sprintf("insert_%d", table.GetID()), insert, insertParamTypes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare insert statement")
	}

	update, updateParamTypes, err := newUpdateStatement(table)
	if err != nil {
		return nil, err
	}
	preparedUpdate, err := session.Prepare(ctx, fmt.Sprintf("update_%d", table.GetID()), update, updateParamTypes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare update statement")
	}

	delete, deleteParamTypes, err := newDeleteStatement(table)
	if err != nil {
		return nil, err
	}
	preparedDelete, err := session.Prepare(ctx, fmt.Sprintf("delete_%d", table.GetID()), delete, deleteParamTypes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare delete statement")
	}

	return &RowWriter{
		session: session,
		insert:  preparedInsert,
		update:  preparedUpdate,
		delete:  preparedDelete,
		columns: columns,
	}, nil
}
