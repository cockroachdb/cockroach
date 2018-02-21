// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
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
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	// We dot-import fsm to use common names such as fsm.True/False. State machine
	// implementations using that library are weird beasts intimately inter-twined
	// with that package; therefor this file should stay as small as possible.
	. "github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

/// States.

type stateNoTxn struct{}

func (stateNoTxn) String() string {
	return "NoTxn"
}

type stateOpen struct {
	ImplicitTxn Bool
	// RetryIntent, if set, means the user declared the intention to retry the txn
	// in case of retriable errors by running a SAVEPOINT cockroach_restart. The
	// txn will enter a RestartWait state in case of such errors.
	RetryIntent Bool
}

func (stateOpen) String() string {
	return "Open"
}

type stateAborted struct {
	// RetryIntent carries over the setting from stateOpen, in case we move back
	// to Open.
	RetryIntent Bool
}

func (stateAborted) String() string {
	return "Aborted"
}

type stateRestartWait struct{}

func (stateRestartWait) String() string {
	return "RestartWait"
}

type stateCommitWait struct{}

func (stateCommitWait) String() string {
	return "CommitWait"
}

func (stateNoTxn) State()       {}
func (stateOpen) State()        {}
func (stateAborted) State()     {}
func (stateRestartWait) State() {}
func (stateCommitWait) State()  {}

/// Events.

type eventTxnStart struct {
	ImplicitTxn Bool
}
type eventTxnStartPayload struct {
	iso enginepb.IsolationType
	pri roachpb.UserPriority
	// txnSQLTimestamp is the timestamp that statements executed in the
	// transaction that is started by this event will report for now(),
	// current_timestamp(), transaction_timestamp().
	txnSQLTimestamp time.Time
	tranCtx         transitionCtx
	readOnly        tree.ReadWriteMode
}

func makeEventTxnStartPayload(
	iso enginepb.IsolationType,
	pri roachpb.UserPriority,
	readOnly tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	tranCtx transitionCtx,
) eventTxnStartPayload {
	return eventTxnStartPayload{
		iso:             iso,
		pri:             pri,
		readOnly:        readOnly,
		txnSQLTimestamp: txnSQLTimestamp,
		tranCtx:         tranCtx,
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
	IsCommit Bool
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
	CanAutoRetry Bool
	IsCommit     Bool
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
// This state machine accepts the eventNonRetriableErr{IsCommit: True} in all
// states. This contract is in place to support the cleanup of connExecutor ->
// this event can always be sent when the connExecutor is tearing down.
//
// NOTE: The Args.Ctx passed to the actions is the connExecutor's context. While
// we are inside a SQL txn, the txn's ctx should be used for operations (i.e
// txnState.Ctx, which is a child ctx). This is so because transitions that move
// in and out of transactions need to have access to both contexts.
//
//go:generate ../util/fsm/gen/reports.sh TxnStateTransitions stateNoTxn
var TxnStateTransitions = Compile(Pattern{
	// NoTxn
	//
	// Note that we don't handle any errors in this state. The connExecutor is
	// supposed to send an eventTxnStart before any other statement that may
	// generate an error.
	stateNoTxn{}: {
		eventTxnStart{Var("implicitTxn")}: {
			Description: "BEGIN, or before a statement running as an implicit txn",
			Next:        stateOpen{ImplicitTxn: Var("implicitTxn"), RetryIntent: False},
			Action: func(args Args) error {
				return args.Extended.(*txnState2).noTxnToOpen(
					args.Ctx, args.Event.(eventTxnStart),
					args.Payload.(eventTxnStartPayload))
			},
		},
		eventNonRetriableErr{IsCommit: Any}: {
			// This event doesn't change state, but it produces a skipBatch advance
			// code.
			Description: "anything but BEGIN or extended protocol command error",
			Next:        stateNoTxn{},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},

	/// Open
	stateOpen{ImplicitTxn: Any, RetryIntent: Any}: {
		eventTxnFinish{}: {
			Description: "COMMIT/ROLLBACK, or after a statement running as an implicit txn",
			Next:        stateNoTxn{},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.finishSQLTxn(args.Ctx)
				ts.setAdvanceInfo(
					advanceOne, noRewind, args.Payload.(eventTxnFinishPayload).toEvent())
				return nil
			},
		},
		// Handle the error on COMMIT cases: we move to NoTxn as per Postgres error
		// semantics.
		eventRetriableErr{CanAutoRetry: False, IsCommit: True}: {
			Description: "Retriable err on COMMIT",
			Next:        stateNoTxn{},
			Action:      cleanupAndFinish,
		},
		eventNonRetriableErr{IsCommit: True}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinish,
		},
	},
	stateOpen{ImplicitTxn: Var("implicitTxn"), RetryIntent: Var("retryIntent")}: {
		// This is the case where we auto-retry.
		eventRetriableErr{CanAutoRetry: True, IsCommit: Any}: {
			// We leave the transaction in Open. In particular, we don't move to
			// RestartWait, as there'd be nothing to move us back from RestartWait to
			// Open.
			// Note: Preparing the KV txn for restart has already happened by this
			// point.
			Description: "Retriable err; will auto-retry",
			Next:        stateOpen{ImplicitTxn: Var("implicitTxn"), RetryIntent: Var("retryIntent")},
			Action: func(args Args) error {
				// The caller will call rewCap.rewindAndUnlock().
				args.Extended.(*txnState2).setAdvanceInfo(
					rewind,
					args.Payload.(eventRetriableErrPayload).rewCap,
					txnRestart)
				return nil
			},
		},
	},
	// Handle the errors in implicit txns. They move us to NoTxn.
	stateOpen{ImplicitTxn: True, RetryIntent: False}: {
		eventRetriableErr{CanAutoRetry: False, IsCommit: False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinish,
		},
		eventNonRetriableErr{IsCommit: False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinish,
		},
	},
	// Handle the errors in explicit txns. They move us to Aborted.
	stateOpen{ImplicitTxn: False, RetryIntent: Var("retryIntent")}: {
		eventNonRetriableErr{IsCommit: False}: {
			Next: stateAborted{RetryIntent: Var("retryIntent")},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
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
			Next:        stateOpen{ImplicitTxn: False, RetryIntent: True},
			Action: func(args Args) error {
				// We flush after setting the retry intent; we know what statement
				// caused this event and we don't need to rewind past it.
				args.Extended.(*txnState2).setAdvanceInfo(advanceOne, noRewind, noEvent)
				return nil
			},
		},
	},
	stateOpen{ImplicitTxn: False, RetryIntent: False}: {
		// Retriable errors when RetryIntent is not set behave like non-retriable
		// errors.
		eventRetriableErr{CanAutoRetry: False, IsCommit: False}: {
			Description: "RetryIntent not set, so handled like non-retriable err",
			Next:        stateAborted{RetryIntent: False},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(payloadWithError).errorCause())
				ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
				ts.txnAbortCount.Inc(1)
				return nil
			},
		},
	},
	stateOpen{ImplicitTxn: False, RetryIntent: True}: {
		eventRetriableErr{CanAutoRetry: False, IsCommit: False}: {
			Next: stateRestartWait{},
			Action: func(args Args) error {
				// Note: Preparing the KV txn for restart has already happened by this
				// point.
				args.Extended.(*txnState2).setAdvanceInfo(skipBatch, noRewind, txnRestart)
				return nil
			},
		},
		eventTxnReleased{}: {
			Description: "RELEASE SAVEPOINT cockroach_restart",
			Next:        stateCommitWait{},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(advanceOne, noRewind, txnCommit)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: False, RetryIntent: True},
			Action: func(args Args) error {
				state := args.Extended.(*txnState2)
				// NOTE: We don't bump the txn timestamp on this restart. Should we?
				// Well, if we generally supported savepoints and one would issue a
				// rollback to a regular savepoint, clearly we couldn't bump the
				// timestamp in that case. In the special case of the cockroach_restart
				// savepoint, it's not clear to me what a user's expectation might be.
				state.mu.txn.Proto().Restart(
					0 /* userPriority */, 0 /* upgradePriority */, hlc.Timestamp{})
				args.Extended.(*txnState2).setAdvanceInfo(advanceOne, noRewind, txnRestart)
				return nil
			},
		},
	},

	/// Aborted
	//
	// Note that we don't handle any error events here. Any statement but a
	// ROLLBACK is expected to not be passed to the state machine.
	stateAborted{RetryIntent: Var("retryIntent")}: {
		eventTxnFinish{}: {
			Description: "ROLLBACK",
			Next:        stateNoTxn{},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.finishSQLTxn(ts.Ctx)
				ts.setAdvanceInfo(
					advanceOne, noRewind, args.Payload.(eventTxnFinishPayload).toEvent())
				return nil
			},
		},
		eventNonRetriableErr{IsCommit: Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "any other statement",
			Next:        stateAborted{RetryIntent: Var("retryIntent")},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},
	stateAborted{RetryIntent: True}: {
		// ROLLBACK TO SAVEPOINT. We accept this in the Aborted state for the
		// convenience of clients who want to issue ROLLBACK TO SAVEPOINT regardless
		// of the preceding query error.
		eventTxnStart{ImplicitTxn: False}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: False, RetryIntent: True},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.finishSQLTxn(args.Ctx)

				payload := args.Payload.(eventTxnStartPayload)

				// Note that we pass the connection's context here, not args.Ctx which
				// was the previous txn's context.
				ts.resetForNewSQLTxn(
					ts.connCtx,
					explicitTxn,
					payload.txnSQLTimestamp, payload.iso, payload.pri, payload.readOnly,
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
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.finishSQLTxn(args.Ctx)
				ts.setAdvanceInfo(
					advanceOne, noRewind, args.Payload.(eventTxnFinishPayload).toEvent())
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: False, RetryIntent: True},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(advanceOne, noRewind, txnRestart)
				return nil
			},
		},
		eventNonRetriableErr{IsCommit: Any}: {
			Next: stateAborted{RetryIntent: True},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
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
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.finishSQLTxn(args.Ctx)
				ts.setAdvanceInfo(
					advanceOne, noRewind, args.Payload.(eventTxnFinishPayload).toEvent())
				return nil
			},
		},
		eventNonRetriableErr{IsCommit: Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			//
			// Note that we don't expect any errors from error on COMMIT in this
			// state.
			Description: "any other statement",
			Next:        stateCommitWait{},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},
})

// cleanupAndFinish rolls back the KV txn and finishes the SQL txn.
func cleanupAndFinish(args Args) error {
	ts := args.Extended.(*txnState2)
	ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(payloadWithError).errorCause())
	ts.finishSQLTxn(args.Ctx)
	ts.setAdvanceInfo(skipBatch, noRewind, txnAborted)
	return nil
}

// noTxnToOpen implements the side effects of starting a txn. It also calls
// setAdvanceInfo().
func (ts *txnState2) noTxnToOpen(
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
		payload.iso,
		payload.pri,
		payload.readOnly,
		payload.tranCtx,
	)

	ts.setAdvanceInfo(advCode, noRewind, txnStart)
	return nil
}
