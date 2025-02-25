// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// maxPreparedTxnGlobalIDLen is the maximum length of a prepared transaction's
// global ID. Taken from Postgres, see GIDSIZE.
const maxPreparedTxnGlobalIDLen = 200

// execPrepareTransactionInOpenState runs a PREPARE TRANSACTION statement inside
// an open txn.
func (ex *connExecutor) execPrepareTransactionInOpenState(
	ctx context.Context, s *tree.PrepareTransaction,
) (fsm.Event, fsm.EventPayload) {
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "prepare sql txn")
	defer sp.Finish()

	// Insert into the system table and prepare the transaction in the KV layer.
	prepareErr := ex.execPrepareTransactionInOpenStateInternal(ctx, s)

	// Roll back the transaction if we encounter an error.
	//
	// From https://www.postgresql.org/docs/16/sql-prepare-transaction.html:
	// > If the PREPARE TRANSACTION command fails for any reason, it becomes a
	// > ROLLBACK: the current transaction is canceled.
	if prepareErr != nil {
		if rbErr := ex.state.mu.txn.Rollback(ctx); rbErr != nil {
			log.Warningf(ctx, "txn rollback failed: err=%s", rbErr)
		}
	}

	// Dissociate the prepared transaction from the current session.
	var p fsm.EventPayload
	if prepareErr != nil {
		p = eventTxnFinishPreparedErrPayload{err: prepareErr}
	}
	return eventTxnFinishPrepared{}, p
}

func (ex *connExecutor) execPrepareTransactionInOpenStateInternal(
	ctx context.Context, s *tree.PrepareTransaction,
) error {
	// TODO(nvanbenschoten): Remove this logic when mixed-version support with
	// v24.3 is no longer necessary.
	if !ex.planner.EvalContext().Settings.Version.IsActive(ctx, clusterversion.V25_1_PreparedTransactionsTable) {
		return pgerror.Newf(pgcode.FeatureNotSupported, "PREPARE TRANSACTION unsupported in mixed-version cluster")
	}

	// TODO(nvanbenschoten): why are these needed here (and in the equivalent
	// functions for commit and rollback)? Shouldn't they be handled by
	// connExecutor.resetExtraTxnState?
	if err := ex.extraTxnState.sqlCursors.closeAll(&ex.planner, cursorCloseForTxnPrepare); err != nil {
		return err
	}
	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)

	// Validate the global ID.
	globalID := s.Transaction.RawString()
	if len(globalID) >= maxPreparedTxnGlobalIDLen {
		return pgerror.Newf(pgcode.InvalidParameterValue, "transaction identifier %q is too long", globalID)
	}

	// Validate that the transaction has not performed any incompatible operations
	// which would prevent it from being prepared.
	if ex.extraTxnState.descCollection.HasUncommittedDescriptors() {
		return pgerror.Newf(pgcode.InvalidTransactionState,
			"cannot prepare a transaction that has already performed schema changes")
	}

	txn := ex.state.mu.txn
	txnID := txn.ID()
	txnKey := txn.Key()
	if !txn.IsOpen() {
		return errors.AssertionFailedf("cannot prepare a transaction that is not open")
	}

	// Insert the prepared transaction's row into the system table. We do this
	// before preparing the transaction in the KV layer so that we can track the
	// existence of the prepared transaction in the event of a crash. We do this
	// non-transactionally so that the system row is committed and readable while
	// the transaction that it references remains in the PREPARED state.
	err := ex.server.cfg.InternalDB.Txn(ctx, func(ctx context.Context, sqlTxn isql.Txn) error {
		return insertPreparedTransaction(
			ctx,
			sqlTxn,
			globalID,
			txnID,
			txnKey,
			ex.sessionData().User().Normalized(),
			ex.sessionData().Database,
		)
	})
	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "transaction identifier %q is already in use", globalID)
		}
		return err
	}

	// Move the transaction into the PREPARED state in the KV layer.
	if err := ex.state.mu.txn.Prepare(ctx); err != nil {
		// The transaction prepare failed. Try to roll it back. If we succeed, we
		// can delete the row from system.prepared_transactions. If we fail, we log
		// a warning and leave the row in place. Either way, we return the original
		// error.
		ex.cleanupAfterFailedPrepareTransaction(ctx, globalID)
		return err
	}

	// TODO(nvanbenschoten): why is these needed here (and in the equivalent
	// functions for commit and rollback)? Shouldn't it be handled by
	// connExecutor.resetExtraTxnState?
	if err := ex.reportSessionDataChanges(func() error {
		ex.sessionDataStack.PopAll()
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (ex *connExecutor) cleanupAfterFailedPrepareTransaction(ctx context.Context, globalID string) {
	// Try to rollback. We only want to cleanup the system.prepared_transactions
	// row if we successfully rollback the transaction.
	err := ex.state.mu.txn.Rollback(ctx)
	if err != nil {
		log.Warningf(ctx, "txn rollback failed: err=%s", err)
		return
	}
	if ctx.Err() != nil {
		// If the context has been canceled, the rollback may have moved to running
		// async, so the absence of an error is not a guarantee that the rollback
		// succeeded. Stop here.
		return
	}

	// We believe we've rolled back the transaction successfully. Query the
	// transaction record again to confirm, to be extra safe.
	txn := ex.state.mu.txn
	txnID := txn.ID()
	txnKey := txn.Key()
	txnRecord, err := queryPreparedTransactionRecord(ctx, ex.server.cfg.DB, txnID, txnKey)
	if err != nil {
		log.Warningf(ctx, "query prepared transaction record after rollback failed: %s", err)
		return
	}
	if txnRecord.Status != roachpb.ABORTED {
		log.Errorf(ctx, "prepared transaction %s not aborted after rollback: %v", globalID, txnRecord)
		return
	}

	// We're certain that the transaction has been rolled back and that its record
	// is not in the PREPARED state. Clean up the system.prepared_transactions row,
	// non-transactionally.
	err = ex.server.cfg.InternalDB.Txn(ctx, func(ctx context.Context, sqlTxn isql.Txn) error {
		return deletePreparedTransaction(ctx, sqlTxn, globalID)
	})
	if err != nil {
		log.Warningf(ctx, "cleanup prepared transaction row failed: %s", err)
	}
}

// CommitPrepared commits a previously prepared transaction and deletes its
// associated entry from the system.prepared_transactions table. This is called
// from COMMIT PREPARED.
func (p *planner) CommitPrepared(ctx context.Context, n *tree.CommitPrepared) (planNode, error) {
	return p.endPreparedTxnNode(n.Transaction, true /* commit */), nil
}

// RollbackPrepared aborts a previously prepared transaction and deletes its
// associated entry from the system.prepared_transactions table. This is called
// from ROLLBACK PREPARED.
func (p *planner) RollbackPrepared(
	ctx context.Context, n *tree.RollbackPrepared,
) (planNode, error) {
	return p.endPreparedTxnNode(n.Transaction, false /* commit */), nil
}

type endPreparedTxnNode struct {
	zeroInputPlanNode
	globalID string
	commit   bool
}

func (p *planner) endPreparedTxnNode(globalID *tree.StrVal, commit bool) *endPreparedTxnNode {
	return &endPreparedTxnNode{
		globalID: globalID.RawString(),
		commit:   commit,
	}
}

func (f *endPreparedTxnNode) startExec(params runParams) error {
	// TODO(nvanbenschoten): Remove this logic when mixed-version support with
	// v24.3 is no longer necessary.
	if !params.EvalContext().Settings.Version.IsActive(params.ctx, clusterversion.V25_1_PreparedTransactionsTable) {
		return pgerror.Newf(pgcode.FeatureNotSupported, "%s unsupported in mixed-version cluster", f.stmtName())
	}

	if err := f.checkNoActiveTxn(params); err != nil {
		return err
	}

	// Retrieve the prepared transaction's row from the system table.
	txnID, txnKey, owner, err := f.selectPreparedTxn(params)
	if err != nil {
		return err
	}

	// Check privileges.
	//
	// From https://www.postgresql.org/docs/16/sql-commit-prepared.html and
	//      https://www.postgresql.org/docs/16/sql-rollback-prepared.html:
	// > To commit / roll back a prepared transaction, you must be either the same
	// > user that executed the transaction originally, or a superuser.
	if params.SessionData().User().Normalized() != owner && !params.SessionData().IsSuperuser {
		return errors.WithHint(pgerror.Newf(pgcode.InsufficientPrivilege,
			"permission denied to finish prepared transaction"),
			"Must be superuser or the user that prepared the transaction.")
	}

	// End (commit or roll back) the prepared transaction in the KV layer.
	if err := f.endPreparedTxn(params, txnID, txnKey); err != nil {
		return err
	}

	// Delete the prepared transaction's row from the system table.
	//
	// It is essential that we only delete the row after the transaction has been
	// moved out of the PREPARED state in the KV layer. This is because the system
	// table row is the only way to track the existence of a prepared transaction
	// so that it can be moved out of the PREPARED state. If we were to delete the
	// row before the transaction was moved out of the PREPARED state, we might
	// lose track of the PREPARED transaction and leave it dangling indefinitely.
	return f.deletePreparedTxn(params)
}

func (f *endPreparedTxnNode) stmtName() string {
	if f.commit {
		return "COMMIT PREPARED"
	}
	return "ROLLBACK PREPARED"
}

// checkNoActiveTxn checks that there is no active transaction in the current
// session. If there is, it returns an error.
func (f *endPreparedTxnNode) checkNoActiveTxn(params runParams) error {
	if params.p.autoCommit {
		return nil
	}
	return pgerror.Newf(pgcode.ActiveSQLTransaction,
		"%s cannot run inside a transaction block", f.stmtName())
}

// selectPreparedTxn queries the prepared transaction from the system table and,
// if found, returns the transaction ID, key, and owner. If the transaction is
// not found, it returns an error.
func (f *endPreparedTxnNode) selectPreparedTxn(
	params runParams,
) (txnID uuid.UUID, txnKey roachpb.Key, owner string, err error) {
	row, err := selectPreparedTransaction(params.ctx, params.p.InternalSQLTxn(), f.globalID)
	if err != nil {
		return uuid.UUID{}, nil, "", err
	}
	if row == nil {
		return uuid.UUID{}, nil, "", pgerror.Newf(pgcode.UndefinedObject,
			"prepared transaction with identifier %q does not exist", f.globalID)
	}

	txnID = tree.MustBeDUuid(row[0]).UUID
	if row[1] != tree.DNull {
		txnKey = roachpb.Key(tree.MustBeDBytes(row[1]))
	}
	owner = string(tree.MustBeDString(row[2]))
	return txnID, txnKey, owner, nil
}

// endPreparedTxn ends the prepared transaction by either committing or rolling
// back the transaction in the KV layer.
func (f *endPreparedTxnNode) endPreparedTxn(
	params runParams, txnID uuid.UUID, txnKey roachpb.Key,
) error {
	// If the transaction had no key, then it was read-only and never wrote a
	// transaction record. In this case, we don't need to do anything besides
	// clean up the system.prepared_transactions row.
	if txnKey == nil {
		return nil
	}

	// Query the prepared transaction's record to determine its current status and
	// to retrieve enough of its metadata to commit or rollback.
	db := params.ExecCfg().DB
	txnRecord, err := queryPreparedTransactionRecord(params.ctx, db, txnID, txnKey)
	if err != nil {
		return err
	}
	if txnRecord.Status.IsFinalized() {
		// The transaction record has already been finalized. Just clean up the
		// system.prepared_transactions row.
		return nil
	}
	if txnRecord.Status != roachpb.PREPARED {
		// The prepared transaction was never moved into the PREPARED state. This
		// can happen if there was a crash after the system.prepared_transactions
		// row was inserted but before the transaction record was PREPARED to
		// commit. In this case, we can't commit the transaction, but we can still
		// roll it back.
		if f.commit {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"prepared transaction with identifier %q not in PREPARED state, cannot COMMIT", f.globalID)
		}
		return nil
	}

	// Set the transaction's read timestamp to its write timestamp. This is	a bit
	// of a hack which falls out of the transaction record only storing the write
	// timestamp and not the read timestamp (see Transaction.AsRecord). To issue a
	// CommitPrepared or RollbackPrepared request, we need to have a read
	// timestamp set. Since the transaction is successfully prepared, the read
	// timestamp can safely be assumed to be equal to the write timestamp.
	txnRecord.ReadTimestamp = txnRecord.WriteTimestamp

	if f.commit {
		err = db.CommitPrepared(params.ctx, txnRecord)
	} else {
		err = db.RollbackPrepared(params.ctx, txnRecord)
	}
	return err
}

// deletePreparedTxn deletes the prepared transaction from the system table.
func (f *endPreparedTxnNode) deletePreparedTxn(params runParams) error {
	return deletePreparedTransaction(params.ctx, params.p.InternalSQLTxn(), f.globalID)
}

func (f *endPreparedTxnNode) Next(params runParams) (bool, error) { return false, nil }
func (f *endPreparedTxnNode) Values() tree.Datums                 { return tree.Datums{} }
func (f *endPreparedTxnNode) Close(ctx context.Context)           {}

func insertPreparedTransaction(
	ctx context.Context,
	sqlTxn isql.Txn,
	globalID string,
	txnID uuid.UUID,
	txnKey roachpb.Key,
	owner, database string,
) error {
	_, err := sqlTxn.ExecEx(
		ctx,
		"insert-prepared-transaction",
		sqlTxn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.prepared_transactions
     (global_id, transaction_id, transaction_key, owner, database)
		 VALUES ($1, $2, $3, $4, $5)`,
		globalID,
		txnID,
		txnKey,
		owner,
		database,
	)
	return err
}

func deletePreparedTransaction(ctx context.Context, sqlTxn isql.Txn, globalID string) error {
	_, err := sqlTxn.ExecEx(
		ctx,
		"delete-prepared-transaction",
		sqlTxn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.prepared_transactions WHERE global_id = $1`,
		globalID,
	)
	return err
}

func selectPreparedTransaction(
	ctx context.Context, sqlTxn isql.Txn, globalID string,
) (tree.Datums, error) {
	return sqlTxn.QueryRowEx(
		ctx,
		"select-prepared-txn",
		sqlTxn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT transaction_id, transaction_key, owner
		 FROM system.prepared_transactions WHERE global_id = $1 FOR UPDATE`,
		globalID,
	)
}

func queryPreparedTransactionRecord(
	ctx context.Context, db *kv.DB, txnID uuid.UUID, txnKey roachpb.Key,
) (*roachpb.Transaction, error) {
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.QueryTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txnKey,
		},
		Txn: enginepb.TxnMeta{
			ID:  txnID,
			Key: txnKey,
		},
	})
	br, pErr := db.NonTransactionalSender().Send(ctx, ba)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	return &br.Responses[0].GetQueryTxn().QueriedTxn, nil
}
