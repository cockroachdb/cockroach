// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
//
//
// This file contains the definition of a connection's state machine, expressed
// through the util/fsm library. Also see txn_state.go, which contains the
// txnState structure used as the ExtendedState mutated by all the Actions.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Constants for the String() representation of the session states. Shared with
// the CLI code which needs to recognize them.
const (
	NoTxnStr              = "NoTxn"
	OpenStateStr          = "Open"
	AbortedStateStr       = "Aborted"
	CommitWaitStateStr    = "CommitWait"
	RestartWaitStateStr   = "RestartWait"
	InternalErrorStateStr = "InternalError"
)

/// States.

type stateNoTxn struct{}

var _ fsm.State = &stateNoTxn{}

func (stateNoTxn) String() string {
	return NoTxnStr
}

type stateOpen struct {
	ImplicitTxn fsm.Bool
	// RetryIntent, if set, means the user declared the intention to retry the txn
	// in case of retriable errors by running a SAVEPOINT cockroach_restart. The
	// txn will enter a RestartWait state in case of such errors.
	RetryIntent fsm.Bool
}

var _ fsm.State = &stateOpen{}

func (stateOpen) String() string {
	return OpenStateStr
}

type stateAborted struct {
	// RetryIntent carries over the setting from stateOpen, in case we move back
	// to Open.
	RetryIntent fsm.Bool
}

var _ fsm.State = &stateAborted{}

func (stateAborted) String() string {
	return AbortedStateStr
}

type stateRestartWait struct{}

var _ fsm.State = &stateRestartWait{}

func (stateRestartWait) String() string {
	return RestartWaitStateStr
}

type stateCommitWait struct{}

var _ fsm.State = &stateCommitWait{}

func (stateCommitWait) String() string {
	return CommitWaitStateStr
}

// stateInternalError is used by the InternalExecutor when running statements in
// a higher-level transaction. The fsm is in this state after encountering an
// execution error when running in an external transaction: the "SQL
// transaction" is finished, however the higher-level transaction is not rolled
// back.
type stateInternalError struct{}

var _ fsm.State = &stateInternalError{}

func (stateInternalError) String() string {
	return InternalErrorStateStr
}

func (stateNoTxn) State()         {}
func (stateOpen) State()          {}
func (stateAborted) State()       {}
func (stateRestartWait) State()   {}
func (stateCommitWait) State()    {}
func (stateInternalError) State() {}

/// Events.

type eventTxnStart struct {
	ImplicitTxn fsm.Bool
}
type eventTxnStartPayload struct {
	tranCtx transitionCtx

	pri roachpb.UserPriority
	// txnSQLTimestamp is the timestamp that statements executed in the
	// transaction that is started by this event will report for now(),
	// current_timestamp(), transaction_timestamp().
	txnSQLTimestamp     time.Time
	readOnly            tree.ReadWriteMode
	historicalTimestamp *hlc.Timestamp
}

func makeEventTxnStartPayload(
	pri roachpb.UserPriority,
	readOnly tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	tranCtx transitionCtx,
) eventTxnStartPayload {
	return eventTxnStartPayload{
		pri:                 pri,
		readOnly:            readOnly,
		txnSQLTimestamp:     txnSQLTimestamp,
		historicalTimestamp: historicalTimestamp,
		tranCtx:             tranCtx,
	}
}

// eventRetryIntentSet is generated in the Open state when a SAVEPOINT
// cockroach_restart is seen.
type eventRetryIntentSet struct{}
type eventTxnFinish struct{}

// eventTxnFinishPayload represents the payload for eventTxnFinish.
type eventTxnFinishPayload struct {
	// commit is set if the transaction committed, false if it was aborted.
	commit bool
}

// toEvent turns the eventTxnFinishPayload into a txnEvent.
func (e eventTxnFinishPayload) toEvent() txnEvent {
	if e.commit {
		return txnCommit
	}
	return txnAborted
}

type eventTxnRestart struct{}

type eventNonRetriableErr struct {
	IsCommit fsm.Bool
}

// eventNonRetriableErrPayload represents the payload for eventNonRetriableErr.
type eventNonRetriableErrPayload struct {
	// err is the error that caused the event.
	err error
}

// errorCause implements the payloadWithError interface.
func (p eventNonRetriableErrPayload) errorCause() error {
	return p.err
}

// eventNonRetriableErrorPayload implements payloadWithError.
var _ payloadWithError = eventNonRetriableErrPayload{}

type eventRetriableErr struct {
	CanAutoRetry fsm.Bool
	IsCommit     fsm.Bool
}

// eventRetriableErrPayload represents the payload for eventRetriableErr.
type eventRetriableErrPayload struct {
	// err is the error that caused the event
	err error
	// rewCap must be set if CanAutoRetry is set on the event. It will be passed
	// back to the connExecutor to perform the rewind.
	rewCap rewindCapability
}

// errorCause implements the payloadWithError interface.
func (p eventRetriableErrPayload) errorCause() error {
	return p.err
}

// eventRetriableErrPayload implements payloadWithError.
var _ payloadWithError = eventRetriableErrPayload{}

// eventTxnReleased is generated after a successful RELEASE SAVEPOINT
// cockroach_restart. It moves the state to CommitWait.
type eventTxnReleased struct{}

// payloadWithError is a common interface for the payloads that wrap an error.
type payloadWithError interface {
	errorCause() error
}

func (eventRetryIntentSet) Event()  {}
func (eventTxnStart) Event()        {}
func (eventTxnFinish) Event()       {}
func (eventTxnRestart) Event()      {}
func (eventNonRetriableErr) Event() {}
func (eventRetriableErr) Event()    {}
func (eventTxnReleased) Event()     {}

// TxnStateTransitions describe the transitions used by a connExecutor's
// fsm.Machine. Args.Extended is a txnState, which is muted by the Actions.
//
// This state machine accepts the eventNonRetriableErr{IsCommit: fsm.True} in all
// states. This contract is in place to support the cleanup of connExecutor ->
// this event can always be sent when the connExecutor is tearing down.
//
// NOTE: The Args.Ctx passed to the actions is the connExecutor's context. While
// we are inside a SQL txn, the txn's ctx should be used for operations (i.e
// txnState.Ctx, which is a child ctx). This is so because transitions that move
// in and out of transactions need to have access to both contexts.
//
//go:generate ../util/fsm/gen/reports.sh TxnStateTransitions stateNoTxn
var TxnStateTransitions = fsm.Compile(fsm.Pattern{
	// NoTxn
	//
	// Note that we don't handle any errors in this state. The connExecutor is
	// supposed to send an eventTxnStart before any other statement that may
	// generate an error.
	stateNoTxn{}: {
		eventTxnStart{fsm.Var("implicitTxn")}: {
			Description: "BEGIN, or before a statement running as an implicit txn",
			Next:        stateOpen{ImplicitTxn: fsm.Var("implicitTxn"), RetryIntent: fsm.False},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).noTxnToOpen(
					args.Ctx, args.Event.(eventTxnStart),
					args.Payload.(eventTxnStartPayload))
			},
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			// This event doesn't change state, but it produces a skipBatch advance
			// code.
			Description: "anything but BEGIN or extended protocol command error",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},

	/// Open
	stateOpen{ImplicitTxn: fsm.Any, RetryIntent: fsm.Any}: {
		eventTxnFinish{}: {
			Description: "COMMIT/ROLLBACK, or after a statement running as an implicit txn",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(
					args.Payload.(eventTxnFinishPayload),
				)
			},
		},
		// Handle the error on COMMIT cases: we move to NoTxn as per Postgres error
		// semantics.
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.True}: {
			Description: "Retriable err on COMMIT",
			Next:        stateNoTxn{},
			Action:      cleanupAndFinish,
		},
		eventNonRetriableErr{IsCommit: fsm.True}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinish,
		},
	},
	stateOpen{ImplicitTxn: fsm.Var("implicitTxn"), RetryIntent: fsm.Var("retryIntent")}: {
		// This is the case where we auto-retry.
		eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.Any}: {
			// We leave the transaction in Open. In particular, we don't move to
			// RestartWait, as there'd be nothing to move us back from RestartWait to
			// Open.
			// Note: Preparing the KV txn for restart has already happened by this
			// point.
			Description: "Retriable err; will auto-retry",
			Next:        stateOpen{ImplicitTxn: fsm.Var("implicitTxn"), RetryIntent: fsm.Var("retryIntent")},
			Action: func(args fsm.Args) error {
				// The caller will call rewCap.rewindAndUnlock().
				args.Extended.(*txnState).setAdvanceInfo(
					rewind,
					args.Payload.(eventRetriableErrPayload).rewCap,
					txnRestart)
				return nil
			},
		},
	},
	// Handle the errors in implicit txns. They move us to NoTxn.
	stateOpen{ImplicitTxn: fsm.True, RetryIntent: fsm.False}: {
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinish,
		},
		eventNonRetriableErr{IsCommit: fsm.False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinish,
		},
	},
	// Handle the errors in explicit txns. They move us to Aborted.
	stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.Var("retryIntent")}: {
		eventNonRetriableErr{IsCommit: fsm.False}: {
			Next: stateAborted{RetryIntent: fsm.Var("retryIntent")},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(payloadWithError).errorCause())
				ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
				ts.txnAbortCount.Inc(1)
				return nil
			},
		},
		// SAVEPOINT cockroach_restart: we just change the state (RetryIntent) if it
		// wasn't set already.
		eventRetryIntentSet{}: {
			Description: "SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			Action: func(args fsm.Args) error {
				// We flush after setting the retry intent; we know what statement
				// caused this event and we don't need to rewind past it.
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, noEvent)
				return nil
			},
		},
	},
	stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.False}: {
		// Retriable errors when RetryIntent is not set behave like non-retriable
		// errors.
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Description: "RetryIntent not set, so handled like non-retriable err",
			Next:        stateAborted{RetryIntent: fsm.False},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(payloadWithError).errorCause())
				ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
				ts.txnAbortCount.Inc(1)
				return nil
			},
		},
	},
	stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True}: {
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Next: stateRestartWait{},
			Action: func(args fsm.Args) error {
				// Note: Preparing the KV txn for restart has already happened by this
				// point.
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, txnRestart)
				return nil
			},
		},
		eventTxnReleased{}: {
			Description: "RELEASE SAVEPOINT cockroach_restart",
			Next:        stateCommitWait{},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, txnCommit)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			Action: func(args fsm.Args) error {
				state := args.Extended.(*txnState)
				// NOTE: We don't bump the txn timestamp on this restart. Should we?
				// Well, if we generally supported savepoints and one would issue a
				// rollback to a regular savepoint, clearly we couldn't bump the
				// timestamp in that case. In the special case of the cockroach_restart
				// savepoint, it's not clear to me what a user's expectation might be.
				state.mu.txn.ManualRestart(args.Ctx, hlc.Timestamp{})
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, txnRestart)
				return nil
			},
		},
	},

	/// Aborted
	//
	// Note that we don't handle any error events here. Any statement but a
	// ROLLBACK is expected to not be passed to the state machine.
	stateAborted{RetryIntent: fsm.Var("retryIntent")}: {
		eventTxnFinish{}: {
			Description: "ROLLBACK",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(
					args.Payload.(eventTxnFinishPayload),
				)
			},
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "any other statement",
			Next:        stateAborted{RetryIntent: fsm.Var("retryIntent")},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},
	stateAborted{RetryIntent: fsm.True}: {
		// ROLLBACK TO SAVEPOINT. We accept this in the Aborted state for the
		// convenience of clients who want to issue ROLLBACK TO SAVEPOINT regardless
		// of the preceding query error.
		eventTxnStart{ImplicitTxn: fsm.False}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.finishSQLTxn()

				payload := args.Payload.(eventTxnStartPayload)

				// Note that we pass the connection's context here, not args.Ctx which
				// was the previous txn's context.
				ts.resetForNewSQLTxn(
					ts.connCtx,
					explicitTxn,
					payload.txnSQLTimestamp,
					payload.historicalTimestamp,
					payload.pri, payload.readOnly,
					nil, /* txn */
					args.Payload.(eventTxnStartPayload).tranCtx,
				)
				ts.setAdvanceInfo(advanceOne, noRewind, noEvent)
				return nil
			},
		},
	},

	stateRestartWait{}: {
		// ROLLBACK (and also COMMIT which acts like ROLLBACK)
		eventTxnFinish{}: {
			Description: "ROLLBACK",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(
					args.Payload.(eventTxnFinishPayload),
				)
			},
		},
		// ROLLBACK TO SAVEPOINT
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.True},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, txnRestart)
				return nil
			},
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			Next: stateAborted{RetryIntent: fsm.True},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(eventNonRetriableErrPayload).err)
				ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
				ts.txnAbortCount.Inc(1)
				return nil
			},
		},
	},

	stateCommitWait{}: {
		eventTxnFinish{}: {
			Description: "COMMIT",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(
					args.Payload.(eventTxnFinishPayload),
				)
			},
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			//
			// Note that we don't expect any errors from error on COMMIT in this
			// state.
			Description: "any other statement",
			Next:        stateCommitWait{},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},
})

// cleanupAndFinish rolls back the KV txn and finishes the SQL txn.
func cleanupAndFinish(args fsm.Args) error {
	ts := args.Extended.(*txnState)
	ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(payloadWithError).errorCause())
	ts.finishSQLTxn()
	ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
	return nil
}

// noTxnToOpen implements the side effects of starting a txn. It also calls
// setAdvanceInfo().
func (ts *txnState) noTxnToOpen(
	connCtx context.Context, ev eventTxnStart, payload eventTxnStartPayload,
) error {
	txnTyp := explicitTxn
	advCode := advanceOne
	if ev.ImplicitTxn.Get() {
		txnTyp = implicitTxn
		// For an implicit txn, we want the statement that produced the event to be
		// executed again (this time in state Open).
		advCode = stayInPlace
	}

	ts.resetForNewSQLTxn(
		connCtx,
		txnTyp,
		payload.txnSQLTimestamp,
		payload.historicalTimestamp,
		payload.pri,
		payload.readOnly,
		nil, /* txn */
		payload.tranCtx,
	)
	ts.setAdvanceInfo(advCode, noRewind, txnStart)
	return nil
}

// finishTxn finishes the transaction. It also calls setAdvanceInfo().
func (ts *txnState) finishTxn(payload eventTxnFinishPayload) error {
	ts.finishSQLTxn()
	ts.setAdvanceInfo(advanceOne, noRewind, payload.toEvent())
	return nil
}

// BoundTxnStateTransitions is the state machine used by the InternalExecutor
// when running SQL inside a higher-level txn. It's a very limited state
// machine: it doesn't allow starting or finishing txns, auto-retries, etc.
var BoundTxnStateTransitions = fsm.Compile(fsm.Pattern{
	stateOpen{ImplicitTxn: fsm.False, RetryIntent: fsm.False}: {
		// We accept eventNonRetriableErr with both IsCommit={True, fsm.False}, even
		// those this state machine does not support COMMIT statements because
		// connExecutor.close() sends an eventNonRetriableErr{IsCommit: fsm.True} event.
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			Next: stateInternalError{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.finishSQLTxn()
				ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
				return nil
			},
		},
		eventRetriableErr{CanAutoRetry: fsm.Any, IsCommit: fsm.False}: {
			Next: stateInternalError{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.finishSQLTxn()
				ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
				return nil
			},
		},
	},
})
