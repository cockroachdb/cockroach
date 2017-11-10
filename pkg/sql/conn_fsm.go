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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	// We dot-import fsm to use common names such as fsm.True/False. State machine
	// implementations using that library are weird beasts intimately inter-twined
	// with that package; therefor this file should stay as small as possible.
	. "github.com/cockroachdb/cockroach/pkg/util/fsm"
)

/// States.

type stateNoTxn struct{}
type stateOpen struct {
	ImplicitTxn Bool
	// RetryIntent, if set, means the user declared the intention to retry the txn
	// in case of retriable errors by running a SAVEPOINT cockroach_restart. The
	// txn will enter a RestartWait state in case of such errors.
	RetryIntent Bool
}
type stateAborted struct {
	// RetryIntent carries over the setting from stateOpen, in case we move back
	// to Open.
	RetryIntent Bool
}
type stateRestartWait struct{}
type stateCommitWait struct{}

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
	tranCtx transitionCtx
}

// eventRetryIntentSet is generated in the Open state when a SAVEPOINT
// cockroach_restart is seen.
type eventRetryIntentSet struct{}
type eventTxnFinish struct{}
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
					args.Payload.(eventTxnStartPayload).tranCtx)
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
				ts.setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
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
					advanceInfo{
						code:   rewind,
						rewCap: args.Payload.(eventRetriableErrPayload).rewCap,
						flush:  false})
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
				ts.setAdvanceInfo(advanceInfo{code: skipQueryStr, flush: true})
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
				ts.setAdvanceInfo(advanceInfo{code: skipQueryStr, flush: true})
				return nil
			},
		},
		// SAVEPOINT cockroach_restart: we just change the state (RetryIntent).
		eventRetryIntentSet{}: {
			Description: "SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: False, RetryIntent: True},
			Action: func(args Args) error {
				// We flush after setting the retry intent; we know what statement
				// caused this event and we don't need to rewind past it.
				args.Extended.(*txnState2).setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
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
				args.Extended.(*txnState2).setAdvanceInfo(advanceInfo{code: skipQueryStr, flush: true})
				return nil
			},
		},
		eventTxnReleased{}: {
			Description: "RELEASE SAVEPOINT cockroach_restart",
			Next:        stateCommitWait{},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
				return nil
			},
		},
	},

	/// Aborted
	//
	// Note that we don't handle any error events here. Any statement but a
	// ROLLBACK is expected to not be passed to the state machine.
	stateAborted{RetryIntent: Any}: {
		eventTxnFinish{}: {
			Description: "ROLLBACK",
			Next:        stateNoTxn{},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.finishSQLTxn(ts.Ctx)
				ts.setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
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

				ts.resetForNewSQLTxn(
					args.Ctx,
					explicitTxn,
					ts.sqlTimestamp, ts.isolation, ts.priority,
					args.Payload.(eventTxnStartPayload).tranCtx,
				)
				ts.setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
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
				ts.setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: False, RetryIntent: True},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
				return nil
			},
		},
		eventNonRetriableErr{IsCommit: False}: {
			Next: stateAborted{RetryIntent: True},
			Action: func(args Args) error {
				ts := args.Extended.(*txnState2)
				ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(eventNonRetriableErrPayload).err)
				ts.mu.txn = nil
				ts.setAdvanceInfo(advanceInfo{code: skipQueryStr, flush: true})
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
				ts.setAdvanceInfo(advanceInfo{code: advanceOne, flush: true})
				return nil
			},
		},
		eventNonRetriableErr{IsCommit: False}: {
			// Note that we don't expect any errors from error on COMMIT in this
			// state.
			Description: "any other statement",
			Next:        stateAborted{RetryIntent: True},
			Action: func(args Args) error {
				args.Extended.(*txnState2).setAdvanceInfo(advanceInfo{code: skipQueryStr, flush: true})
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
	ts.setAdvanceInfo(advanceInfo{code: skipQueryStr, flush: true})
	return nil
}

// noTxnToOpen implements the side effects of starting a txn. It also calls
// setAdvanceInfo(), telling the execution to stayInPlace.
func (ts *txnState2) noTxnToOpen(
	connCtx context.Context, ev eventTxnStart, tranCtx transitionCtx,
) error {
	txnTyp := explicitTxn
	if ev.ImplicitTxn.Get() {
		txnTyp = implicitTxn
	}

	ts.resetForNewSQLTxn(
		connCtx,
		txnTyp,
		tranCtx.clock.PhysicalTime(), /* sqlTimestamp */
		tranCtx.defaultIsolationLevel,
		roachpb.NormalUserPriority,
		tranCtx,
	)
	// When starting a transaction from the NoTxn state, we don't advance the
	// cursor - we want the same statement to be executed again. Note that this is
	// true for both implicit and explicit transactions. See execStmtInNoTxn() for
	// details.
	//
	// Flushing here shouldn't matter - all the results for statements executed
	// before the statement that generated this event should have been already
	// flushed (since we're not in a txn), and we shouldn't have generated any
	// results for the statement that generated this event (which is why we can
	// stayInPlace here).
	ts.setAdvanceInfo(advanceInfo{code: stayInPlace, flush: true})
	return nil
}
