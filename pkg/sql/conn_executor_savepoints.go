// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// execSavepointInOpenState runs a SAVEPOINT statement inside an open
// txn.
func (ex *connExecutor) execSavepointInOpenState(
	ctx context.Context, s *tree.Savepoint, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload, error) {
	env := &ex.extraTxnState.savepoints
	// Sanity check for "SAVEPOINT cockroach_restart".
	commitOnRelease := ex.isCommitOnReleaseSavepoint(s.Name)
	if commitOnRelease {
		/* SAVEPOINT cockroach_restart */
		if !env.isEmpty() {
			err := pgerror.Newf(pgcode.Syntax,
				"SAVEPOINT %s cannot be nested",
				tree.ErrNameString(commitOnReleaseSavepointName))
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}
		// We want to disallow restart SAVEPOINTs to be issued after a KV
		// transaction has started running. The client txn's statement count
		// indicates how many statements have been executed as part of this
		// transaction. It is desirable to allow metadata queries against
		// vtables to proceed before starting a SAVEPOINT for better ORM
		// compatibility.
		//
		// See also:
		// https://github.com/cockroachdb/cockroach/issues/15012
		if ex.state.mu.txn.Active() {
			err := pgerror.Newf(pgcode.Syntax,
				"SAVEPOINT %s needs to be the first statement in a transaction",
				tree.ErrNameString(commitOnReleaseSavepointName))
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}
	}

	token, err := ex.state.mu.txn.CreateSavepoint(ctx)
	if err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}
	entry := savepoint{
		name:            s.Name,
		initial:         !ex.state.mu.txn.Active(),
		commitOnRelease: commitOnRelease,
		kvToken:         token,
		numDDL:          ex.extraTxnState.numDDL,
	}
	if !entry.initial {
		entry.txnID = ex.state.mu.txn.ID()
		entry.epoch = ex.state.mu.txn.Epoch()
	}
	env.push(entry)

	return nil, nil, nil
}

// execRelease runs a RELEASE SAVEPOINT statement inside an open txn.
func (ex *connExecutor) execRelease(
	ctx context.Context, s *tree.ReleaseSavepoint, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	env := &ex.extraTxnState.savepoints
	entry, idx := env.find(s.Savepoint)
	if entry == nil {
		ev, payload := ex.makeErrEvent(
			pgerror.Newf(pgcode.InvalidSavepointSpecification,
				"savepoint %s does not exist", &s.Savepoint), s)
		return ev, payload
	}

	// Discard our savepoint and all further ones. Depending on what happens with
	// the release below, we might add this savepoint back.
	env.popToIdx(idx - 1)

	if entry.commitOnRelease {
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		err := ex.commitSQLTransactionInternal(ctx, s)
		if err == nil {
			return eventTxnReleased{}, nil
		}
		// Committing the transaction failed. We'll go to state RestartWait if
		// it's a retriable error, or to state RollbackWait otherwise.
		if errIsRetriable(err) {
			// Add the savepoint back. We want to allow a ROLLBACK TO SAVEPOINT
			// cockroach_restart (that's the whole point of commitOnRelease).
			env.push(*entry)

			rc, canAutoRetry := ex.getRewindTxnCapability()
			ev := eventRetriableErr{
				IsCommit:     fsm.FromBool(isCommit(s)),
				CanAutoRetry: fsm.FromBool(canAutoRetry),
			}
			payload := eventRetriableErrPayload{err: err, rewCap: rc}
			return ev, payload
		}

		// Non-retriable error. The transaction might have committed (i.e. the
		// error might be ambiguous). We can't allow a ROLLBACK TO SAVEPOINT to
		// recover the transaction, so we're not adding the savepoint back.
		ex.rollbackSQLTransaction(ctx)
		ev := eventNonRetriableErr{IsCommit: fsm.FromBool(false)}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload
	}

	if err := ex.state.mu.txn.ReleaseSavepoint(ctx, entry.kvToken); err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload
	}

	return nil, nil
}

// execRollbackToSavepointInOpenState runs a ROLLBACK TO SAVEPOINT
// statement inside an open txn.
func (ex *connExecutor) execRollbackToSavepointInOpenState(
	ctx context.Context, s *tree.RollbackToSavepoint, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	entry, idx := ex.extraTxnState.savepoints.find(s.Savepoint)
	if entry == nil {
		ev, payload := ex.makeErrEvent(pgerror.Newf(pgcode.InvalidSavepointSpecification,
			"savepoint %s does not exist", &s.Savepoint), s)
		return ev, payload
	}

	// We don't yet support rolling back over DDL. Instead of creating an
	// inconsistent txn or schema state, prefer to tell the users we don't know
	// how to proceed yet. Initial savepoints are a special case - we can always
	// rollback to them because we can reset all the schema change state.
	if !entry.initial && ex.extraTxnState.numDDL > entry.numDDL {
		ev, payload := ex.makeErrEvent(unimplemented.NewWithIssueDetail(10735, "rollback-after-ddl",
			"ROLLBACK TO SAVEPOINT not yet supported after DDL statements"), s)
		return ev, payload
	}

	if err := ex.state.mu.txn.RollbackToSavepoint(ctx, entry.kvToken); err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload
	}

	ex.extraTxnState.savepoints.popToIdx(idx)

	if entry.initial {
		return eventTxnRestart{}, nil
	}
	// No event is necessary; there's nothing for the state machine to do.
	return nil, nil
}

func (ex *connExecutor) execRollbackToSavepointInAbortedState(
	ctx context.Context, s *tree.RollbackToSavepoint,
) (fsm.Event, fsm.EventPayload) {
	makeErr := func(err error) (fsm.Event, fsm.EventPayload) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: err,
		}
		return ev, payload
	}

	entry, idx := ex.extraTxnState.savepoints.find(s.Savepoint)
	if entry == nil {
		return makeErr(pgerror.Newf(pgcode.InvalidSavepointSpecification,
			"savepoint %s does not exist", tree.ErrString(&s.Savepoint)))
	}

	// We can always rollback to initial savepoints, but for non-initial ones we
	// need to check that the underlying KV txn is still copacetic.
	if !entry.initial {
		curID, curEpoch := ex.state.mu.txn.ID(), ex.state.mu.txn.Epoch()
		if !curID.Equal(entry.txnID) {
			return ex.makeErrEvent(roachpb.NewTransactionRetryWithProtoRefreshError(
				"cannot rollback to savepoint because the transaction has been aborted",
				curID,
				// The transaction inside this error doesn't matter.
				roachpb.Transaction{},
			), s)
		}
		if curEpoch != entry.epoch {
			return ex.makeErrEvent(roachpb.NewTransactionRetryWithProtoRefreshError(
				"cannot rollback to savepoint because the transaction experience a serializable restart",
				curID,
				// The transaction inside this error doesn't matter.
				roachpb.Transaction{},
			), s)
		}
	}

	ex.extraTxnState.savepoints.popToIdx(idx)
	if err := ex.state.mu.txn.RollbackToSavepoint(ctx, entry.kvToken); err != nil {
		return makeErr(err)
	}
	if entry.initial {
		return eventTxnRestart{}, nil
	}
	return eventSavepointRollback{}, nil
}

// isCommitOnReleaseSavepoint returns true if the savepoint name implies special
// release semantics: releasing it commits the underlying KV txn.
func (ex *connExecutor) isCommitOnReleaseSavepoint(savepoint tree.Name) bool {
	if ex.sessionData.ForceSavepointRestart {
		// The session setting force_savepoint_restart implies that all
		// uses of the SAVEPOINT statement are targeting restarts.
		return true
	}
	return strings.HasPrefix(string(savepoint), commitOnReleaseSavepointName)
}

type savepoint struct {
	name tree.Name

	// initial is set if this savepoint has been created before performing any KV
	// operations. If so, a ROLLBACK to it will work after a retriable error. If
	// not set, then rolling back to it after a retriable error will return the
	// retriable error again because reads might have been evaluated before the
	// savepoint and such reads cannot have their timestamp forwarded without a
	// refresh.
	initial bool

	// commitOnRelease is set if the special syntax "SAVEPOINT cockroach_restart"
	// was used. Such a savepoint is special in that a RELEASE actually commits
	// the transaction - giving the client a change to find out about any
	// retriable error and issue another "ROLLBACK TO SAVEPOINT cockroach_restart"
	// afterwards. Regular savepoints (even top-level savepoints) cannot commit
	// the transaction on RELEASE.
	//
	// Only an `initial` savepoint can have this set.
	commitOnRelease bool

	kvToken client.SavepointToken

	// In the initial implementation, we refuse to roll back
	// a savepoint if there was DDL performed "under it".
	// TODO(knz): support partial DDL cancellation in pending txns.
	numDDL int

	// txnID and epoch describe the transaction iteration that the savepoint is
	// bound to. "initial" savepoints are not bound to a particular transaction as
	// rolling back to an initial savepoint resets all the transaction state.
	// Other savepoints are bound to a transaction iteration, and rolling back to
	// them is not permitted if the transaction ID or epoch has changed in the
	// meantime:
	// - if the txnID has changed, then the previous transaction was aborted. We
	// can't rollback to a savepoint since all the transaction's writes are gone.
	// - if the epoch has changed, then we can only rollback if we performed a
	// refresh for the reads that are not rolled back. We currently don't do this.
	txnID uuid.UUID
	epoch enginepb.TxnEpoch
}

type savepointStack []savepoint

func (stack savepointStack) isEmpty() bool { return len(stack) == 0 }

func (stack *savepointStack) clear() { *stack = (*stack)[:0] }

func (stack *savepointStack) push(s savepoint) {
	*stack = append(*stack, s)
}

// find finds the most recent savepoint with the given name.
//
// The returned savepoint can be modified (rolling back modifies the kvToken).
// Callers shouldn't maintain references to the returned savepoint, as
// references can be invalidated by further operations on the savepoints.
func (stack savepointStack) find(sn tree.Name) (*savepoint, int) {
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i].name == sn {
			return &stack[i], i
		}
	}
	return nil, -1
}

// popToIdx pops (discards) all the savepoints at higher indexes.
func (stack *savepointStack) popToIdx(idx int) {
	*stack = (*stack)[:idx+1]
}

func (stack savepointStack) clone() savepointStack {
	cpy := make(savepointStack, len(stack))
	copy(cpy, stack)
	return cpy
}

// runShowSavepointState executes a SHOW SAVEPOINT STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSavepointState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{
		{Name: "savepoint_name", Typ: types.String},
		{Name: "is_initial_savepoint", Typ: types.Bool},
	})

	for _, entry := range ex.extraTxnState.savepoints {
		if err := res.AddRow(ctx, tree.Datums{
			tree.NewDString(string(entry.name)),
			tree.MakeDBool(tree.DBool(entry.initial)),
		}); err != nil {
			return err
		}
	}
	return nil
}

// commitOnReleaseSavepointName is the name of the savepoint with special
// release semantics.
const commitOnReleaseSavepointName = "cockroach_restart"
