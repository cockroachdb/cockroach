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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
)

// commitOnReleaseSavepointName is the name of the savepoint with special
// release semantics: releasing this savepoint commits the underlying KV txn.
// This special savepoint is used to catch deferred serializability violations
// and is part of the client-directed transaction retries protocol.
const commitOnReleaseSavepointName = "cockroach_restart"

// execSavepointInOpenState runs a SAVEPOINT statement inside an open
// txn.
func (ex *connExecutor) execSavepointInOpenState(
	ctx context.Context, s *tree.Savepoint, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload, error) {
	savepoints := &ex.extraTxnState.savepoints
	// Sanity check for "SAVEPOINT cockroach_restart".
	commitOnRelease := ex.isCommitOnReleaseSavepoint(s.Name)
	if commitOnRelease {
		// Validate the special savepoint cockroach_restart. It cannot be nested
		// because it has special release semantics.
		active := ex.state.mu.txn.Active()
		l := len(*savepoints)
		// If we've already declared this savepoint, but we haven't done anything
		// with the KV txn yet (or, more importantly, we haven't done an anything
		// with the KV txn since we've rolled back to it), treat the recreation of
		// the savepoint as a no-op instead of erroring out because this savepoint
		// cannot be nested (even within itself).
		// This serves to support the following pattern:
		// SAVEPOINT cockroach_restart
		// <foo> -> serializability failure
		// ROLLBACK TO SAVEPOINT cockroach_restart
		// SAVEPOINT cockroach_restart
		//
		// Some of our examples use this pattern, issuing the SAVEPOINT cockroach_restart
		// inside the retry loop.
		//
		// Of course, this means that the following doesn't work:
		// SAVEPOINT cockroach_restart
		// SAVEPOINT cockroach_restart
		// RELEASE SAVEPOINT cockroach_restart
		// ROLLBACK TO SAVEPOINT cockroach_restart  -> the savepoint no longer exists here
		//
		// Although it would work for any other savepoint but cockroach_restart. But
		// that's natural given the release semantics.
		if l == 1 && (*savepoints)[0].commitOnRelease && !active {
			return nil, nil, nil
		}

		err := func() error {
			if !savepoints.empty() {
				return pgerror.Newf(pgcode.Syntax,
					"SAVEPOINT \"%s\" cannot be nested",
					tree.ErrNameString(commitOnReleaseSavepointName))
			}
			// We want to disallow restart SAVEPOINTs to be issued after a KV
			// transaction has started running. It is desirable to allow metadata
			// queries against vtables to proceed before starting a SAVEPOINT for better
			// ORM compatibility.
			// See also https://github.com/cockroachdb/cockroach/issues/15012.
			if ex.state.mu.txn.Active() {
				return pgerror.Newf(pgcode.Syntax,
					"SAVEPOINT \"%s\" needs to be the first statement in a transaction",
					tree.ErrNameString(commitOnReleaseSavepointName))
			}
			return nil
		}()
		if err != nil {
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}
	}

	token, err := ex.state.mu.txn.CreateSavepoint(ctx)
	if err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}

	sp := savepoint{
		name:            s.Name,
		commitOnRelease: commitOnRelease,
		kvToken:         token,
		numDDL:          ex.extraTxnState.numDDL,
	}
	savepoints.push(sp)

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
				"savepoint \"%s\" does not exist", &s.Savepoint), s)
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
			"savepoint \"%s\" does not exist", &s.Savepoint), s)
		return ev, payload
	}

	if ev, payload, ok := ex.checkRollbackValidity(ctx, s, entry); !ok {
		return ev, payload
	}

	if err := ex.state.mu.txn.RollbackToSavepoint(ctx, entry.kvToken); err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload
	}

	ex.extraTxnState.savepoints.popToIdx(idx)

	if entry.kvToken.Initial() {
		return eventTxnRestart{}, nil
	}
	// No event is necessary; there's nothing for the state machine to do.
	return nil, nil
}

// checkRollbackValidity verifies that a ROLLBACK TO SAVEPOINT
// statement should be allowed in the current txn state.
// It returns ok == false if the operation should be prevented
// from proceeding, in which case it also populates the event
// and payload with a suitable user error.
func (ex *connExecutor) checkRollbackValidity(
	ctx context.Context, s *tree.RollbackToSavepoint, entry *savepoint,
) (ev fsm.Event, payload fsm.EventPayload, ok bool) {
	if ex.extraTxnState.numDDL <= entry.numDDL {
		// No DDL; all the checks below only care about txns containing
		// DDL, so we don't have anything else to do here.
		return ev, payload, true
	}

	if !entry.kvToken.Initial() {
		// We don't yet support rolling back a regular savepoint over
		// DDL. Instead of creating an inconsistent txn or schema state,
		// prefer to tell the users we don't know how to proceed
		// yet. Initial savepoints are a special case - we can always
		// rollback to them because we can reset all the schema change
		// state.
		ev, payload = ex.makeErrEvent(unimplemented.NewWithIssueDetail(10735, "rollback-after-ddl",
			"ROLLBACK TO SAVEPOINT not yet supported after DDL statements"), s)
		return ev, payload, false
	}

	if ex.state.mu.txn.UserPriority() == roachpb.MaxUserPriority {
		// Because we use the same priority (MaxUserPriority) for SET
		// TRANSACTION PRIORITY HIGH and lease acquisitions, we'd get a
		// deadlock if we let DDL proceed at high priority.
		// See https://github.com/cockroachdb/cockroach/issues/46414
		// for details.
		//
		// Note: this check must remain even when regular savepoints are
		// taught to roll back over DDL (that's the other check in
		// execSavepointInOpenState), until #46414 gets solved.
		ev, payload = ex.makeErrEvent(unimplemented.NewWithIssue(46414,
			"cannot use ROLLBACK TO SAVEPOINT in a HIGH PRIORITY transaction containing DDL"), s)
		return ev, payload, false
	}

	return ev, payload, true
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
			"savepoint \"%s\" does not exist", tree.ErrString(&s.Savepoint)))
	}

	if ev, payload, ok := ex.checkRollbackValidity(ctx, s, entry); !ok {
		return ev, payload
	}

	ex.extraTxnState.savepoints.popToIdx(idx)

	if err := ex.state.mu.txn.RollbackToSavepoint(ctx, entry.kvToken); err != nil {
		return ex.makeErrEvent(err, s)
	}

	if entry.kvToken.Initial() {
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

// savepoint represents a SQL savepoint - a snapshot of the current
// transaction's state at a previous point in time.
//
// Savepoints' behavior on RELEASE differs based on commitOnRelease, and their
// behavior on ROLLBACK after retriable errors differs based on
// kvToken.Initial().
type savepoint struct {
	name tree.Name

	// commitOnRelease is set if the special syntax "SAVEPOINT cockroach_restart"
	// was used. Such a savepoint is special in that a RELEASE actually commits
	// the transaction - giving the client a change to find out about any
	// retriable error and issue another "ROLLBACK TO SAVEPOINT cockroach_restart"
	// afterwards. Regular savepoints (even top-level savepoints) cannot commit
	// the transaction on RELEASE.
	//
	// Only an `initial` savepoint can have this set (see
	// client.SavepointToken.Initial()).
	commitOnRelease bool

	kvToken kv.SavepointToken

	// The number of DDL statements that had been executed in the transaction (at
	// the time the savepoint was created). We refuse to roll back a savepoint if
	// more DDL statements were executed since the savepoint's creation.
	// TODO(knz): support partial DDL cancellation in pending txns.
	numDDL int
}

type savepointStack []savepoint

func (stack savepointStack) empty() bool { return len(stack) == 0 }

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
	if len(stack) == 0 {
		// Avoid allocating a slice.
		return nil
	}
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
	res.SetColumns(ctx, colinfo.ResultColumns{
		{Name: "savepoint_name", Typ: types.String},
		{Name: "is_initial_savepoint", Typ: types.Bool},
	})

	for _, entry := range ex.extraTxnState.savepoints {
		if err := res.AddRow(ctx, tree.Datums{
			tree.NewDString(string(entry.name)),
			tree.MakeDBool(tree.DBool(entry.kvToken.Initial())),
		}); err != nil {
			return err
		}
	}
	return nil
}
