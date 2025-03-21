// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	insert          statements.Statement[tree.Statement]
	update          statements.Statement[tree.Statement]
	delete          statements.Statement[tree.Statement]
	sessionOverride sessiondata.InternalExecutorOverride

	scratchDatums []any
	columns       []string
}

func (s *sqlRowWriter) getExecutorOverride(
	originTimestamp hlc.Timestamp,
) sessiondata.InternalExecutorOverride {
	session := s.sessionOverride
	session.OriginTimestampForLogicalDataReplication = originTimestamp
	session.OriginIDForLogicalDataReplication = 1
	return session
}

// DeleteRow deletes a row from the table. It returns errStalePreviousValue
// if the oldRow argument does not match the value in the local database.
func (s *sqlRowWriter) DeleteRow(
	ctx context.Context, txn isql.Txn, originTimestamp hlc.Timestamp, oldRow tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]

	for _, d := range oldRow {
		s.scratchDatums = append(s.scratchDatums, d)
	}

	rowsAffected, err := txn.ExecParsed(ctx, "replicated-delete", txn.KV(),
		s.getExecutorOverride(originTimestamp),
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
func (s *sqlRowWriter) InsertRow(
	ctx context.Context, txn isql.Txn, originTimestamp hlc.Timestamp, row tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]
	for _, d := range row {
		s.scratchDatums = append(s.scratchDatums, d)
	}
	rowsImpacted, err := txn.ExecParsed(ctx, "replicated-insert", txn.KV(),
		s.getExecutorOverride(originTimestamp),
		s.insert,
		s.scratchDatums...,
	)
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
	ctx context.Context,
	txn isql.Txn,
	originTimestamp hlc.Timestamp,
	oldRow tree.Datums,
	newRow tree.Datums,
) error {
	s.scratchDatums = s.scratchDatums[:0]

	for _, d := range oldRow {
		s.scratchDatums = append(s.scratchDatums, d)
	}
	for _, d := range newRow {
		s.scratchDatums = append(s.scratchDatums, d)
	}

	rowsAffected, err := txn.ExecParsed(ctx, "replicated-update", txn.KV(),
		s.getExecutorOverride(originTimestamp),
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

func newSQLRowWriter(
	table catalog.TableDescriptor, sessionOverride sessiondata.InternalExecutorOverride,
) (*sqlRowWriter, error) {
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
		insert:          insert,
		update:          update,
		delete:          delete,
		sessionOverride: sessionOverride,
		columns:         columns,
	}, nil
}
