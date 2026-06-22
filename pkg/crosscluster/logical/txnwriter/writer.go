// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwriter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

type ApplyResult struct {
	// DlqReason is set if the transaction could not be applied and should be sent
	// to the dead-letter-queue. If this is set, then LwwLoserRows and AppliedRows
	// will be zero.
	DlqReason error
	// LwwLoserRows is the number of rows that were dropped as last-write-losers.
	LwwLoserRows int
	// AppliedRows is the number of rows that were written to the local cluster.
	// If the transaction was applied, then LwwLoserRows + AppliedRows ==
	// len(Transaction.Rows)
	AppliedRows int
}

// TODO(jeffswenson): Consider replacing the crud writer with transaction
// writer once the transaction writer supports the tombstone update case.
type TransactionWriter interface {
	// ApplyBatch will apply the batch of transactions. ApplyBatch requires the
	// the transactions to have disjoint write sets.
	ApplyBatch(context.Context, []ldrdecoder.Transaction) ([]ApplyResult, error)
	Close(ctx context.Context)
}

// NewTransactionWriter constructs a TransactionWriter that applies
// replicated rows as user. grants is attached to the session's
// SessionData so that the privilege check for the apply queries (which
// the session runs via Prepare/ExecutePrepared rather than per-op
// InternalExecutorOverrides) sees the destination-table DML bypass.
// LDR job owners typically hold REPLICATIONDEST without direct DML.
func NewTransactionWriter(
	ctx context.Context,
	db isql.DB,
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
	user username.SQLUsername,
	grants map[uint32]sessiondata.DescriptorOverride,
) (TransactionWriter, error) {
	sd := sql.NewInternalSessionData(ctx, settings, "txn-writer")
	sd.UserProto = user.EncodeProto()
	sd.DescriptorOverrides = grants
	session, err := sqlwriter.NewInternalSession(ctx, db, sd, settings)
	if err != nil {
		return nil, errors.Wrap(err, "creating new isql session for transaction writer")
	}

	return &transactionWriter{
		db:           db,
		leaseMgr:     leaseMgr,
		session:      session,
		tableWriters: make(map[descpb.ID]*sqlwriter.RowWriter),
		tableReaders: make(map[descpb.ID]sqlwriter.RowReader),
	}, nil
}

type transactionWriter struct {
	db       isql.DB
	leaseMgr *lease.Manager

	session      isql.Session
	tableWriters map[descpb.ID]*sqlwriter.RowWriter
	tableReaders map[descpb.ID]sqlwriter.RowReader
	// TODO(jeffswenson): add tombstone updater
}

func (tw *transactionWriter) initTable(ctx context.Context, tableID descpb.ID) error {
	if _, exists := tw.tableWriters[tableID]; exists {
		return nil
	}

	// Acquiring then releasing the lease works because we only care about the
	// columns in the schema and the schema is locked while LDR is configured on
	// a table.
	now := tw.db.KV().Clock().Now()
	desc, err := tw.leaseMgr.Acquire(ctx, lease.TimestampToReadTimestamp(now), tableID)
	if err != nil {
		return errors.Wrapf(err, "acquiring lease for table %d", tableID)
	}
	defer desc.Release(ctx)

	writer, err := sqlwriter.NewRowWriter(
		ctx,
		desc.Underlying().(catalog.TableDescriptor),
		tw.session,
	)
	if err != nil {
		return errors.Wrapf(err, "creating sql writer for table %d", tableID)
	}

	reader, err := sqlwriter.NewRowReader(
		ctx,
		desc.Underlying().(catalog.TableDescriptor),
		tw.session,
	)
	if err != nil {
		return errors.Wrapf(err, "creating sql reader for table %d", tableID)
	}

	tw.tableWriters[tableID] = writer
	tw.tableReaders[tableID] = reader
	return nil
}

func (tw *transactionWriter) Close(ctx context.Context) {
	tw.session.Close(ctx)
}
