// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// execShowCommitTimestampInOpenState deals with the special statement
// SHOW COMMIT TIMESTAMP while in an open transaction. This statement, only
// allowed in an explicit transaction, will internally commit the underlying
// sql transaction and then write out the timestamp at which that transaction
// committed. The transaction state machine will move to the commitWait state
// much like when you do RELEASE cockroach_restart. One can still use RELEASE
// cockroach_restart; we support this so that SHOW COMMIT TIMESTAMP composes
// well with middleware which internally injects such savepoints and manages
// transaction controls above the client.
func (ex *connExecutor) execShowCommitTimestampInOpenState(
	ctx context.Context, s *tree.ShowCommitTimestamp, res RestrictedCommandResult, canAutoCommit bool,
) (fsm.Event, fsm.EventPayload) {

	// We do not allow SHOW COMMIT TIMESTAMP in the middle of an implicit
	// transaction -- it's not clear what it would mean. Given we don't move
	// to Open from NoTxn when this statement was executed alone, the only
	// possible way to get here is if the statement occurred in the middle of
	// a multi-statement implicit transaction or via a multi-statement
	// transaction using the extended protocol. There's logic in the
	// connExecutor to detect if this statement is the last statement of an
	// implicit transaction using the simple protocol and to instead treat
	// it as though it were not part of the transaction. The advantage of that
	// approach is that it allows a pairing of statements like INSERT and
	// SHOW COMMIT TIMESTAMP to still hit 1PC for the INSERT.
	implicit := ex.implicitTxn()
	if implicit && !canAutoCommit {
		return ex.makeErrEvent(pgerror.Newf(
			pgcode.InvalidTransactionState,
			"cannot use SHOW COMMIT TIMESTAMP in multi-statement implicit transaction"),
			s)
	}

	res.ResetStmtType((*tree.CommitTransaction)(nil))
	err := ex.commitSQLTransactionInternal(ctx)
	if err == nil {

		ts, err := ex.state.mu.txn.CommitTimestamp()
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		if err := writeShowCommitTimestampRow(ctx, res, ts); err != nil {
			return ex.makeErrEvent(err, s)
		}

		// If we have a SAVEPOINT cockroach_restart, then we need to note that
		// fact now, as the SAVEPOINT stack will be destroyed as the state
		// machine moves into COMMIT. This state in extraTxnState will be cleaned
		// up as we process any statement in CommitWait.
		if entry, _ := ex.extraTxnState.savepoints.find(
			commitOnReleaseSavepointName,
		); entry != nil && entry.commitOnRelease {
			ex.extraTxnState.shouldAcceptReleaseCockroachRestartInCommitWait = true
		}

		// If this is an implicit transaction, we must have gotten here through
		// the extended protocol and this must be an auto-commit statement. The
		// event to move the state machine will follow.
		if implicit {
			return nil, nil
		}

		return eventTxnCommittedWithShowCommitTimestamp{}, nil
	}

	// Committing the transaction failed. We'll go to state RestartWait if
	// it's a retriable error, or to state RollbackWait otherwise.
	if errIsRetriable(err) {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		ev := eventRetriableErr{
			IsCommit:     fsm.FromBool(false /* isCommit */),
			CanAutoRetry: fsm.FromBool(canAutoRetry),
		}
		payload := eventRetriableErrPayload{err: err, rewCap: rc}
		return ev, payload
	}

	ev := eventNonRetriableErr{IsCommit: fsm.FromBool(false)}
	payload := eventNonRetriableErrPayload{err: err}
	return ev, payload
}

// execShowCommitTimestampInCommitWaitState deals with the special statement
// SHOW COMMIT TIMESTAMP while in the commitWait state. One can reach this
// point either by issuing SHOW COMMIT TIMESTAMP multiple times or by issuing
// it after RELEASE cockroach_restart.
func (ex *connExecutor) execShowCommitTimestampInCommitWaitState(
	ctx context.Context, s *tree.ShowCommitTimestamp, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	ts, err := ex.state.mu.txn.CommitTimestamp()
	if err != nil {
		return ex.makeErrEvent(err, s)
	}
	if err := writeShowCommitTimestampRow(ctx, res, ts); err != nil {
		return ex.makeErrEvent(err, s)
	}
	return nil, nil
}

// execShowCommitTimestampInNoTxnState deals with the special statement
// SHOW COMMIT TIMESTAMP while in the noTxn state. One can reach this
// by executing the statement after successfully committing an implicit
// or explicit transaction. An error will be returned if either no transaction
// has been previously committed on this session or if the last transaction
// created encountered an error.
func (ex *connExecutor) execShowCommitTimestampInNoTxnState(
	ctx context.Context, s *tree.ShowCommitTimestamp, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	ts := ex.previousTransactionCommitTimestamp
	if ts.IsEmpty() {
		return ex.makeErrEvent(pgerror.Newf(
			pgcode.InvalidTransactionState, "no previous transaction",
		), s)
	}
	if err := writeShowCommitTimestampRow(
		ctx, res, ts,
	); err != nil {
		return ex.makeErrEvent(err, s)
	}
	return nil, nil
}

func writeShowCommitTimestampRow(
	ctx context.Context, res RestrictedCommandResult, ts hlc.Timestamp,
) error {
	res.SetColumns(ctx, colinfo.ShowCommitTimestampColumns)
	return res.AddRow(ctx, tree.Datums{eval.TimestampToDecimalDatum(ts)})
}
