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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
)

// execSavepointInOpenState runs a SAVEPOINT statement inside an open
// txn.
func (ex *connExecutor) execSavepointInOpenState(
	ctx context.Context, s *tree.Savepoint, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	// Ensure that the user isn't trying to run BEGIN; SAVEPOINT; SAVEPOINT;
	if ex.state.activeRestartSavepointName != "" {
		err := unimplemented.NewWithIssueDetail(10735, "nested", "SAVEPOINT may not be nested")
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}
	if err := ex.validateSavepointName(s.Name); err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}
	// We want to disallow SAVEPOINTs to be issued after a KV transaction has
	// started running. The client txn's statement count indicates how many
	// statements have been executed as part of this transaction. It is
	// desirable to allow metadata queries against vtables to proceed
	// before starting a SAVEPOINT for better ORM compatibility.
	// See also:
	// https://github.com/cockroachdb/cockroach/issues/15012
	if ex.state.mu.txn.Active() {
		err := pgerror.Newf(pgcode.Syntax,
			"SAVEPOINT %s needs to be the first statement in a transaction", restartSavepointName)
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}
	ex.state.activeRestartSavepointName = s.Name
	// Note that Savepoint doesn't have a corresponding plan node.
	// This here is all the execution there is.
	return eventRetryIntentSet{}, nil /* payload */, nil
}

// execReleaseSavepointInOpenState runs a RELEASE SAVEPOINT statement
// inside an open txn.
func (ex *connExecutor) execReleaseSavepointInOpenState(
	ctx context.Context, s *tree.ReleaseSavepoint, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	if err := ex.validateSavepointName(s.Savepoint); err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}
	if !ex.machine.CurState().(stateOpen).RetryIntent.Get() {
		ev, payload := ex.makeErrEvent(errSavepointNotUsed, s)
		return ev, payload, nil
	}

	// ReleaseSavepoint is executed fully here; there's no plan for it.
	ev, payload := ex.runReleaseRestartSavepointAsTxnCommit(ctx, s)
	res.ResetStmtType((*tree.CommitTransaction)(nil))
	return ev, payload, nil
}

// execRollbackToSavepointInOpenState runs a ROLLBACK TO SAVEPOINT
// statement inside an open txn.
func (ex *connExecutor) execRollbackToSavepointInOpenState(
	ctx context.Context, s *tree.RollbackToSavepoint, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	if err := ex.validateSavepointName(s.Savepoint); err != nil {
		ev, payload := ex.makeErrEvent(err, s)
		return ev, payload, nil
	}
	if !ex.machine.CurState().(stateOpen).RetryIntent.Get() {
		ev, payload := ex.makeErrEvent(errSavepointNotUsed, s)
		return ev, payload, nil
	}
	ex.state.activeRestartSavepointName = ""

	res.ResetStmtType((*tree.Savepoint)(nil))
	return eventTxnRestart{}, nil /* payload */, nil
}

// execSavepointInAbortedState runs a SAVEPOINT statement when a txn is aborted.
// It also contains the logic for ROLLBACK TO SAVEPOINT.
// TODO(knz): split this in different functions.
func (ex *connExecutor) execSavepointInAbortedState(
	ctx context.Context, inRestartWait bool, s tree.Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
	// "SAVEPOINT cockroach_restart" commands to indicate client intent to
	// retry a transaction in a RestartWait state.
	var spName tree.Name
	var isRollback bool
	switch n := s.(type) {
	case *tree.RollbackToSavepoint:
		spName = n.Savepoint
		isRollback = true
	case *tree.Savepoint:
		spName = n.Name
	default:
		panic("unreachable")
	}
	// If the user issued a SAVEPOINT in the abort state, validate
	// as though there were no active savepoint.
	if !isRollback {
		ex.state.activeRestartSavepointName = ""
	}
	if err := ex.validateSavepointName(spName); err != nil {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: err,
		}
		return ev, payload
	}
	// Either clear or reset the current savepoint name so that
	// ROLLBACK TO; SAVEPOINT; works.
	if isRollback {
		ex.state.activeRestartSavepointName = ""
	} else {
		ex.state.activeRestartSavepointName = spName
	}

	if !(inRestartWait || ex.machine.CurState().(stateAborted).RetryIntent.Get()) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: errSavepointNotUsed,
		}
		return ev, payload
	}

	res.ResetStmtType((*tree.RollbackTransaction)(nil))

	if inRestartWait {
		return eventTxnRestart{}, nil
	}
	// We accept ROLLBACK TO SAVEPOINT even after non-retryable errors to make
	// it easy for client libraries that want to indiscriminately issue
	// ROLLBACK TO SAVEPOINT after every error and possibly follow it with a
	// ROLLBACK and also because we accept ROLLBACK TO SAVEPOINT in the Open
	// state, so this is consistent.
	// We start a new txn with the same sql timestamp and isolation as the
	// current one.

	ev := eventTxnStart{
		ImplicitTxn: fsm.False,
	}
	rwMode := tree.ReadWrite
	if ex.state.readOnly {
		rwMode = tree.ReadOnly
	}
	payload := makeEventTxnStartPayload(
		ex.state.priority, rwMode, ex.state.sqlTimestamp,
		nil /* historicalTimestamp */, ex.transitionCtx)
	return ev, payload
}

// execRollbackToSavepointInAbortedState runs a ROLLBACK TO SAVEPOINT
// statement when a txn is aborted.
func (ex *connExecutor) execRollbackToSavepointInAbortedState(
	ctx context.Context, inRestartWait bool, s tree.Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	return ex.execSavepointInAbortedState(ctx, inRestartWait, s, res)
}

// runReleaseRestartSavepointAsTxnCommit executes a commit after
// RELEASE SAVEPOINT statement when using an explicit transaction.
func (ex *connExecutor) runReleaseRestartSavepointAsTxnCommit(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	if ev, payload, ok := ex.commitSQLTransactionInternal(ctx, stmt); !ok {
		return ev, payload
	}
	return eventTxnReleased{}, nil
}

// validateSavepointName validates that it is that the provided ident
// matches the active savepoint name, begins with RestartSavepointName,
// or that force_savepoint_restart==true. We accept everything with the
// desired prefix because at least the C++ libpqxx appends sequence
// numbers to the savepoint name specified by the user.
func (ex *connExecutor) validateSavepointName(savepoint tree.Name) error {
	if ex.state.activeRestartSavepointName != "" {
		if savepoint == ex.state.activeRestartSavepointName {
			return nil
		}
		return pgerror.Newf(pgcode.InvalidSavepointSpecification,
			`SAVEPOINT %q is in use`, tree.ErrString(&ex.state.activeRestartSavepointName))
	}
	if !ex.sessionData.ForceSavepointRestart && !strings.HasPrefix(string(savepoint), restartSavepointName) {
		return unimplemented.NewWithIssueHint(10735,
			"SAVEPOINT not supported except for "+restartSavepointName,
			"Retryable transactions with arbitrary SAVEPOINT names can be enabled "+
				"with SET force_savepoint_restart=true")
	}
	return nil
}

// clearSavepoints clears all savepoints defined so far. This
// occurs when the SQL txn is closed (abort/commit) and upon
// a top-level restart.
func (ex *connExecutor) clearSavepoints() {
	ex.state.activeRestartSavepointName = ""
}

// restartSavepointName is the only savepoint ident that we accept.
const restartSavepointName string = "cockroach_restart"

var errSavepointNotUsed = pgerror.Newf(
	pgcode.SavepointException,
	"savepoint %s has not been used", restartSavepointName)
