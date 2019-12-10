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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
)

// execSavepointInOpenState runs a SAVEPOINT statement inside an open
// txn.
func (ex *connExecutor) execSavepointInOpenState(
	ctx context.Context, s *tree.Savepoint, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	entry := savepointEntry{
		name:               s.Name,
		isRestartSavepoint: ex.isRestartSavepoint(s.Name),
	}

	if entry.isRestartSavepoint {
		/* SAVEPOINT cockroach_restart */

		if !ex.state.savepointEnv.isEmpty() {
			err := pgerror.Newf(pgcode.Syntax,
				"SAVEPOINT %s cannot be nested",
				tree.ErrNameString(restartSavepointName))
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
				tree.ErrNameString(restartSavepointName))
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}

		retEv = eventRetryIntentSet{}
	} else {
		/* other savepoints */

		token, err := ex.state.mu.txn.CreateSavepoint(ctx)
		if err != nil {
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}
		entry.token = token
		// Nothing special - also no special event for the txn.
		retEv = nil
	}

	ex.state.savepointEnv.createSavepoint(entry)

	return retEv, nil, nil
}

// execReleaseSavepointInOpenState runs a RELEASE SAVEPOINT statement
// inside an open txn.
func (ex *connExecutor) execReleaseSavepointInOpenState(
	ctx context.Context, s *tree.ReleaseSavepoint, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	entry, idx, ok := ex.state.savepointEnv.findSavepoint(s.Savepoint)
	if !ok {
		ev, payload := ex.makeErrEvent(
			pgerror.Newf(pgcode.InvalidSavepointSpecification,
				"savepoint %s does not exist", tree.ErrString(&s.Savepoint)), s)
		return ev, payload, nil
	}

	if entry.isRestartSavepoint {
		/* RELEASE SAVEPOINT cockroach_restart */

		if !ex.machine.CurState().(stateOpen).RetryIntent.Get() {
			ev, payload := ex.makeErrEvent(errSavepointNotUsed, s)
			return ev, payload, nil
		}

		// ReleaseSavepoint with a restart savepoint operates as a txn commit.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		var ok bool
		retEv, retPayload, ok = ex.commitSQLTransactionInternal(ctx, s)
		if ok {
			retEv = eventTxnReleased{}
			retPayload = nil
		}
	} else {
		/* regular savepoint */

		if err := ex.state.mu.txn.ReleaseSavepoint(ctx, entry.token); err != nil {
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}
	}

	// RELEASE means the savepoint name is not valid any more.
	// We rewind to one position higher.
	ex.state.savepointEnv.rewindToSavepoint(idx - 1)

	return retEv, retPayload, nil
}

// execRollbackToSavepointInOpenState runs a ROLLBACK TO SAVEPOINT
// statement inside an open txn.
func (ex *connExecutor) execRollbackToSavepointInOpenState(
	ctx context.Context, s *tree.RollbackToSavepoint, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	entry, idx, ok := ex.state.savepointEnv.findSavepoint(s.Savepoint)
	if !ok {
		ev, payload := ex.makeErrEvent(
			pgerror.Newf(pgcode.InvalidSavepointSpecification,
				"savepoint %s does not exist", tree.ErrString(&s.Savepoint)), s)
		return ev, payload, nil
	}

	if entry.isRestartSavepoint {
		/* RELEASE SAVEPOINT cockroach_restart */

		if !ex.machine.CurState().(stateOpen).RetryIntent.Get() {
			ev, payload := ex.makeErrEvent(errSavepointNotUsed, s)
			return ev, payload, nil
		}

		res.ResetStmtType((*tree.Savepoint)(nil))
		retEv = eventTxnRestart{}
	} else {
		/* regular savepoint */

		if err := ex.state.mu.txn.RollbackToSavepoint(ctx, entry.token); err != nil {
			ev, payload := ex.makeErrEvent(err, s)
			return ev, payload, nil
		}
	}

	ex.state.savepointEnv.rewindToSavepoint(idx)

	return retEv, retPayload, nil
}

// execSavepointInAbortedState runs a SAVEPOINT statement when a txn is aborted.
func (ex *connExecutor) execSavepointInAbortedState(
	ctx context.Context, inRestartWait bool, s *tree.Savepoint, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	makeErr := func(err error) (fsm.Event, fsm.EventPayload) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: err,
		}
		return ev, payload
	}

	if !ex.isRestartSavepoint(s.Name) {
		// Cannot establish a regular savepoint if the txn is currently in
		// error.
		return makeErr(sqlbase.NewTransactionAbortedError("" /* customMsg */))
	}

	// Establishing a restart savepoint in a txn in error has three
	// behaviors:
	// - if there was no restart savepoint already, this fails and
	//   informs the client they should have use SAVEPOINT cockroach_restart
	//   at the beginning.
	// - if the txn is not waiting to retry, ditto.
	// - otherwise (there was a restart savepoint and txn ready to retry), then
	//   it behaves as a ROLLBACK to that savepoint, then starting
	//   a new restart cycle.
	_, idx, ok := ex.state.savepointEnv.findRestart()
	if !ok || !(inRestartWait || ex.machine.CurState().(stateAborted).RetryIntent.Get()) {
		return makeErr(errSavepointNotUsed)
	}

	ex.state.savepointEnv.rewindToSavepoint(idx)

	res.ResetStmtType((*tree.RollbackTransaction)(nil))

	if inRestartWait {
		return eventTxnRestart{}, nil
	}

	// We accept new restart SAVEPOINTs even after non-retryable errors
	// to make it easy for client libraries that want to
	// indiscriminately issue SAVEPOINT / ROLLBACK TO SAVEPOINT after
	// every error and possibly follow it with a ROLLBACK and also
	// because we accept ROLLBACK TO SAVEPOINT in the Open state, so
	// this is consistent.
	//
	// We start a new txn with the same sql timestamp and isolation as
	// the current one.

	retEv := eventTxnStart{
		ImplicitTxn: fsm.False,
	}
	rwMode := tree.ReadWrite
	if ex.state.readOnly {
		rwMode = tree.ReadOnly
	}
	retPayload := makeEventTxnStartPayload(
		ex.state.priority, rwMode, ex.state.sqlTimestamp,
		nil /* historicalTimestamp */, ex.transitionCtx)
	return retEv, retPayload
}

func (ex *connExecutor) execRollbackToSavepointInAbortedState(
	ctx context.Context, inRestartWait bool, s *tree.RollbackToSavepoint, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	makeErr := func(err error) (fsm.Event, fsm.EventPayload) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: err,
		}
		return ev, payload
	}

	entry, _, ok := ex.state.savepointEnv.findSavepoint(s.Savepoint)
	if !ok {
		return makeErr(pgerror.Newf(pgcode.InvalidSavepointSpecification,
			"savepoint %s does not exist", tree.ErrString(&s.Savepoint)))
	}

	if !entry.isRestartSavepoint {
		/* regular savepoint */
		return makeErr(unimplemented.NewWithIssue(10735, "ROLLBACK TO SAVEPOINT in aborted state"))
	}

	/* ROLLBACK TO SAVEPOINT cockroach_restart */
	// We treat this as a ROLLBACK followed by a new SAVEPOINT.
	// The SAVEPOINT logic also automatically rewinds to the restart savepoint,
	// so we don't have to do it here.
	return ex.execSavepointInAbortedState(ctx, inRestartWait, &tree.Savepoint{Name: s.Savepoint}, res)
}

// isRestartSavepoint returns true iff the savepoint name implies
// special "restart" semantics.
func (ex *connExecutor) isRestartSavepoint(savepoint tree.Name) bool {
	if ex.sessionData.ForceSavepointRestart {
		// The session sxetting force_savepoint_restart implies that all
		// uses of the SAVEPOINT statement are targeting restarts.
		return true
	}
	return strings.HasPrefix(string(savepoint), restartSavepointName)
}

type savepointEntry struct {
	name               tree.Name
	isRestartSavepoint bool
	token              client.SavepointToken
}

type savepointEnv struct {
	stack []savepointEntry
}

func (env *savepointEnv) isEmpty() bool { return len(env.stack) == 0 }

func (env *savepointEnv) clear() { env.stack = env.stack[:0] }

func (env *savepointEnv) createSavepoint(s savepointEntry) {
	env.stack = append(env.stack, s)
}

// findRestart finds the most recent restart savepoint.
func (env *savepointEnv) findRestart() (savepointEntry, int, bool) {
	for i := len(env.stack) - 1; i >= 0; i-- {
		if env.stack[i].isRestartSavepoint {
			return env.stack[i], i, true
		}
	}
	return savepointEntry{}, -1, false
}

// findSavepoint finds the most recent savepoint with the given name.
func (env *savepointEnv) findSavepoint(sn tree.Name) (savepointEntry, int, bool) {
	for i := len(env.stack) - 1; i >= 0; i-- {
		if env.stack[i].name == sn {
			return env.stack[i], i, true
		}
	}
	return savepointEntry{}, -1, false
}

func (env *savepointEnv) rewindToSavepoint(i int) {
	env.stack = env.stack[:i+1]
}

// clearSavepoints clears all savepoints defined so far. This
// occurs when the SQL txn is closed (abort/commit) and upon
// a top-level restart.
func (ex *connExecutor) clearSavepoints() {
	ex.state.savepointEnv.clear()
}

// runShowSavepointState executes a SHOW SAVEPOINT STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSavepointState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{
		{Name: "savepoint_name", Typ: types.String},
		{Name: "is_restart_savepoint", Typ: types.Bool},
	})

	for _, entry := range ex.state.savepointEnv.stack {
		if err := res.AddRow(ctx, tree.Datums{
			tree.NewDString(string(entry.name)),
			tree.MakeDBool(tree.DBool(entry.isRestartSavepoint)),
		}); err != nil {
			return err
		}
	}
	return nil
}

// restartSavepointName is the only savepoint ident that we accept.
const restartSavepointName = "cockroach_restart"

var errSavepointNotUsed = pgerror.Newf(
	pgcode.SavepointException,
	"savepoint %s has not been used", restartSavepointName)
