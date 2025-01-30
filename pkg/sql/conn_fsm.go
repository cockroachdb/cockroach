// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
//
//
// This file contains the definition of a connection's state machine, expressed
// through the util/fsm library. Also see txn_state.go, which contains the
// txnState structure used as the ExtendedState mutated by all the Actions.

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlfsm"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Constants for the String() representation of the session states. Shared with
// the CLI code which needs to recognize them.
const (
	NoTxnStateStr         = sqlfsm.NoTxnStateStr
	OpenStateStr          = sqlfsm.OpenStateStr
	AbortedStateStr       = sqlfsm.AbortedStateStr
	CommitWaitStateStr    = sqlfsm.CommitWaitStateStr
	InternalErrorStateStr = sqlfsm.InternalErrorStateStr
)

// States.

type stateNoTxn struct{}

var _ fsm.State = &stateNoTxn{}

func (stateNoTxn) String() string {
	return NoTxnStateStr
}

type stateOpen struct {
	// ImplicitTxn is false if the txn included a BEGIN command.
	ImplicitTxn fsm.Bool
	// WasUpgraded is true if the txn started as implicit, but a BEGIN made it
	// become explicit.
	WasUpgraded fsm.Bool
}

var _ fsm.State = &stateOpen{}

func (stateOpen) String() string {
	return OpenStateStr
}

// stateAborted is entered on errors (retriable and non-retriable). A ROLLBACK
// TO SAVEPOINT can move the transaction back to stateOpen.
type stateAborted struct {
	// WasUpgraded is true if the txn started as implicit, but a BEGIN made it
	// become explicit. This is needed so that when ROLLBACK TO SAVEPOINT moves
	// to stateOpen, we keep tracking WasUpgraded correctly.
	WasUpgraded fsm.Bool
}

var _ fsm.State = &stateAborted{}

func (stateAborted) String() string {
	return AbortedStateStr
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
func (stateCommitWait) State()    {}
func (stateInternalError) State() {}

// Events.

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
	// qualityOfService denotes the user-level admission queue priority to use for
	// any new Txn started using this payload.
	qualityOfService      sessiondatapb.QoSLevel
	isoLevel              isolation.Level
	omitInRangefeeds      bool
	bufferedWritesEnabled bool
}

// makeEventTxnStartPayload creates an eventTxnStartPayload.
func makeEventTxnStartPayload(
	pri roachpb.UserPriority,
	readOnly tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	tranCtx transitionCtx,
	qualityOfService sessiondatapb.QoSLevel,
	isoLevel isolation.Level,
	omitInRangefeeds bool,
	bufferedWritesEnabled bool,
) eventTxnStartPayload {
	return eventTxnStartPayload{
		pri:                   pri,
		readOnly:              readOnly,
		txnSQLTimestamp:       txnSQLTimestamp,
		historicalTimestamp:   historicalTimestamp,
		tranCtx:               tranCtx,
		qualityOfService:      qualityOfService,
		isoLevel:              isoLevel,
		omitInRangefeeds:      omitInRangefeeds,
		bufferedWritesEnabled: bufferedWritesEnabled,
	}
}

type eventTxnUpgradeToExplicit struct{}

type eventTxnFinishCommitted struct{}
type eventTxnFinishAborted struct{}

// eventTxnFinishCommittedPLpgSQL and eventTxnFinishAbortedPLpgSQL are generated
// when a PL/pgSQL stored procedure executes a COMMIT or ROLLBACK statement. The
// current transaction is finished, but the statement buffer is not advanced,
// since the current stored procedure resumes execution in the new transaction.
type eventTxnFinishCommittedPLpgSQL struct{}
type eventTxnFinishAbortedPLpgSQL struct{}

// eventSavepointRollback is generated when we want to move from Aborted to Open
// through a ROLLBACK TO SAVEPOINT <not cockroach_restart>. Note that it is not
// generated when such a savepoint is rolled back to from the Open state. In
// that case no event is necessary.
type eventSavepointRollback struct{}

// eventTxnFinishPrepared is generated when a PREPARE TRANSACTION statement is
// executed. The current transaction is dissociated with the current session.
type eventTxnFinishPrepared struct{}

// eventTxnFinishPreparedErrPayload represents the payload for
// eventTxnFinishPrepared, if the PREPARE TRANSACTION statement encountered an
// error. No payload is used if the statement was successful.
type eventTxnFinishPreparedErrPayload struct {
	// err is the error encountered during the prepare.
	err error
}

// errorCause implements the payloadWithError interface.
func (p eventTxnFinishPreparedErrPayload) errorCause() error {
	return p.err
}

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

// eventTxnRestart is generated by a rollback to a savepoint placed at the
// beginning of the transaction (commonly SAVEPOINT cockroach_restart).
type eventTxnRestart struct{}

// eventTxnReleased is generated after a successful RELEASE SAVEPOINT
// cockroach_restart. It moves the state to CommitWait. The event is not
// generated by releasing regular savepoints.
type eventTxnReleased struct{}

// eventTxnCommittedWithShowCommitTimestamp is generated after a successful
// SHOW COMMIT TIMESTAMP in an explicit transaction. It moves the state to
// CommitWait.
type eventTxnCommittedWithShowCommitTimestamp struct{}

// eventTxnCommittedDueToDDL is generated when a DDL statement is received
// in an explicit transaction, and the autocommit_before_ddl session variable
// is enabled. It moves the state to NoTxn.
type eventTxnCommittedDueToDDL struct{}

// payloadWithError is a common interface for the payloads that wrap an error.
type payloadWithError interface {
	errorCause() error
}

func (eventTxnStart) Event()                            {}
func (eventTxnFinishCommitted) Event()                  {}
func (eventTxnFinishAborted) Event()                    {}
func (eventTxnFinishPrepared) Event()                   {}
func (eventTxnFinishCommittedPLpgSQL) Event()           {}
func (eventTxnFinishAbortedPLpgSQL) Event()             {}
func (eventSavepointRollback) Event()                   {}
func (eventNonRetriableErr) Event()                     {}
func (eventRetriableErr) Event()                        {}
func (eventTxnRestart) Event()                          {}
func (eventTxnReleased) Event()                         {}
func (eventTxnCommittedWithShowCommitTimestamp) Event() {}
func (eventTxnUpgradeToExplicit) Event()                {}
func (eventTxnCommittedDueToDDL) Event()                {}

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
			Next:        stateOpen{ImplicitTxn: fsm.Var("implicitTxn"), WasUpgraded: fsm.False},
			Action:      noTxnToOpen,
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			// This event doesn't change state, but it produces a skipBatch advance
			// code.
			Description: "anything but BEGIN or extended protocol command error",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.setAdvanceInfo(skipBatch, noRewind, txnEvent{eventType: noEvent})
				return nil
			},
		},
	},

	// Open
	stateOpen{ImplicitTxn: fsm.Any, WasUpgraded: fsm.Any}: {
		eventTxnFinishCommitted{}: {
			Description: "COMMIT, or after a statement running as an implicit txn",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				// Note that the KV txn has been committed by the statement execution by
				// this point.
				return args.Extended.(*txnState).finishTxn(txnCommit, advanceOne)
			},
		},
		eventTxnFinishAborted{}: {
			Description: "ROLLBACK, or after a statement running as an implicit txn fails",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				// Note that the KV txn has been rolled back by the statement execution
				// by this point.
				return args.Extended.(*txnState).finishTxn(txnRollback, advanceOne)
			},
		},
		// Handle the error on COMMIT cases: we move to NoTxn as per Postgres error
		// semantics.
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.True}: {
			Description: "Retriable err on COMMIT",
			Next:        stateNoTxn{},
			Action:      cleanupAndFinishOnError,
		},
		eventNonRetriableErr{IsCommit: fsm.True}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinishOnError,
		},
		eventTxnCommittedDueToDDL{}: {
			Description: "auto-commit before DDL",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				finishedTxnID, commitTimestamp := ts.finishSQLTxn()
				ts.setAdvanceInfo(stayInPlace, noRewind, txnEvent{
					eventType: txnCommit, txnID: finishedTxnID, commitTimestamp: commitTimestamp,
				})
				return nil
			},
		},
	},
	stateOpen{ImplicitTxn: fsm.True, WasUpgraded: fsm.False}: {
		// This is the case where we auto-retry.
		eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.Any}: {
			// Rewind and auto-retry - the transaction should stay in the Open state
			Description: "Retriable err; will auto-retry",
			Next:        stateOpen{ImplicitTxn: fsm.True, WasUpgraded: fsm.False},
			Action:      prepareTxnForRetryWithRewind,
		},
		// Handle the errors in implicit txns. They move us to NoTxn.
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinishOnError,
		},
		eventNonRetriableErr{IsCommit: fsm.False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinishOnError,
		},
		// Handle a txn getting upgraded to an explicit txn.
		eventTxnUpgradeToExplicit{}: {
			Next: stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.True},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(
					advanceOne,
					noRewind,
					txnEvent{eventType: txnUpgradeToExplicit},
				)
				return nil
			},
		},
		// Handle transaction management from PL/pgSQL. Note that these statements
		// are not valid within the context of an explicit transaction, so we only
		// have to handle implicit transactions.
		//
		// Use stayInPlace so that the statement buffer doesn't advance. This will
		// allow the stored procedure to resume execution.
		eventTxnFinishCommittedPLpgSQL{}: {
			Description: "COMMIT statement called via PL/pgSQL",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(txnCommit, stayInPlace)
			},
		},
		eventTxnFinishAbortedPLpgSQL{}: {
			Description: "ROLLBACK statement called via PL/pgSQL",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(txnRollback, stayInPlace)
			},
		},
	},
	stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.Var("wasUpgraded")}: {
		// Handle prepared transactions. Note that this statement is only valid in
		// the context of an explicit transaction, so we don't need to handle
		// implicit transactions.
		eventTxnFinishPrepared{}: {
			Description: "PREPARE TRANSACTION",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				// Note that the KV txn has been prepared by the statement execution by
				// this point and is being dissociated from the session.
				return args.Extended.(*txnState).finishTxn(txnPrepare, advanceOne)
			},
		},
		// Handle the errors in explicit txns.
		eventNonRetriableErr{IsCommit: fsm.False}: {
			Next: stateAborted{WasUpgraded: fsm.Var("wasUpgraded")},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				func() {
					ts.mu.Lock()
					defer ts.mu.Unlock()
					if !ts.mu.hasSavepoints {
						_ = ts.mu.txn.Rollback(ts.Ctx)
					}
				}()
				ts.setAdvanceInfo(skipBatch, noRewind, txnEvent{eventType: noEvent})
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT cockroach. There's not much to do other than generating a
		// txnRestart output event.
		// The next state must be an explicit txn, since the SAVEPOINT
		// creation can only happen in an explicit txn.
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.Var("wasUpgraded")},
			Action:      prepareTxnForRetry,
		},
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Next: stateAborted{WasUpgraded: fsm.Var("wasUpgraded")},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(
					skipBatch,
					noRewind,
					txnEvent{eventType: noEvent},
				)
				return nil
			},
		},
		eventTxnCommittedWithShowCommitTimestamp{}: {
			Description: "SHOW COMMIT TIMESTAMP",
			Next:        stateCommitWait{},
			Action:      moveToCommitWaitAfterInternalCommit,
		},
		// This is the case where we auto-retry explicit transactions.
		eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.Any}: {
			// Rewind and auto-retry - the transaction should stay in the Open state.
			// Retrying can cause the transaction to become implicit if we see that
			// the transaction was previously upgraded. During the retry, BEGIN
			// will be executed again and upgrade the transaction back to explicit.
			Description: "Retriable err; will auto-retry",
			Next:        stateOpen{ImplicitTxn: fsm.Var("wasUpgraded"), WasUpgraded: fsm.False},
			Action:      prepareTxnForRetryWithRewind,
		},
		eventTxnReleased{}: {
			Description: "RELEASE SAVEPOINT cockroach_restart",
			Next:        stateCommitWait{},
			Action:      moveToCommitWaitAfterInternalCommit,
		},
	},

	// Aborted
	//
	// Note that we don't handle any error events here. Any statement but a
	// ROLLBACK (TO SAVEPOINT) is expected to not be passed to the state machine.
	stateAborted{WasUpgraded: fsm.Var("wasUpgraded")}: {
		eventTxnFinishAborted{}: {
			Description: "ROLLBACK",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.txnAbortCount.Inc(1)
				// Note that the KV txn has been rolled back by now by statement
				// execution.
				return ts.finishTxn(txnRollback, advanceOne)
			},
		},
		// Any statement.
		eventNonRetriableErr{IsCommit: fsm.False}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "any other statement",
			Next:        stateAborted{WasUpgraded: fsm.Var("wasUpgraded")},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(
					skipBatch,
					noRewind,
					txnEvent{eventType: noEvent},
				)
				return nil
			},
		},
		// ConnExecutor closing.
		eventNonRetriableErr{IsCommit: fsm.True}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "ConnExecutor closing",
			Next:        stateAborted{WasUpgraded: fsm.Var("wasUpgraded")},
			Action:      cleanupAndFinishOnError,
		},
		// ROLLBACK TO SAVEPOINT <not cockroach_restart> success.
		// The next state must be an explicit txn, since the SAVEPOINT
		// creation can only happen in an explicit txn.
		eventSavepointRollback{}: {
			Description: "ROLLBACK TO SAVEPOINT (not cockroach_restart) success",
			Next:        stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.Var("wasUpgraded")},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(
					advanceOne,
					noRewind,
					txnEvent{eventType: noEvent},
				)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT <not cockroach_restart> failed because the txn needs to restart.
		eventRetriableErr{CanAutoRetry: fsm.Any, IsCommit: fsm.Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "ROLLBACK TO SAVEPOINT (not cockroach_restart) failed because txn needs restart",
			Next:        stateAborted{WasUpgraded: fsm.Var("wasUpgraded")},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(
					skipBatch,
					noRewind,
					txnEvent{eventType: noEvent},
				)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT cockroach_restart.
		// The next state must be an explicit txn, since the SAVEPOINT
		// creation can only happen in an explicit txn.
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT cockroach_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.Var("wasUpgraded")},
			Action:      prepareTxnForRetry,
		},
	},

	// Commit Wait
	stateCommitWait{}: {
		eventTxnFinishCommitted{}: {
			Description: "COMMIT",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				// A txnCommit event has been previously generated when we entered
				// stateCommitWait.
				return args.Extended.(*txnState).finishTxn(noEvent, advanceOne)
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
				args.Extended.(*txnState).setAdvanceInfo(
					skipBatch,
					noRewind,
					txnEvent{eventType: noEvent},
				)
				return nil
			},
		},
	},
})

// noTxnToOpen implements the side effects of starting a txn. It also calls
// setAdvanceInfo().
func noTxnToOpen(args fsm.Args) error {
	connCtx := args.Ctx
	ev := args.Event.(eventTxnStart)
	payload := args.Payload.(eventTxnStartPayload)
	ts := args.Extended.(*txnState)

	txnTyp := explicitTxn
	advCode := advanceOne
	if ev.ImplicitTxn.Get() {
		txnTyp = implicitTxn
		// For an implicit txn, we want the statement that produced the event to be
		// executed again (this time in state Open).
		advCode = stayInPlace
	}

	newTxnID := ts.resetForNewSQLTxn(
		connCtx,
		txnTyp,
		payload.txnSQLTimestamp,
		payload.historicalTimestamp,
		payload.pri,
		payload.readOnly,
		nil, /* txn */
		payload.tranCtx,
		payload.qualityOfService,
		payload.isoLevel,
		payload.omitInRangefeeds,
		payload.bufferedWritesEnabled,
	)
	ts.setAdvanceInfo(
		advCode,
		noRewind,
		txnEvent{eventType: txnStart, txnID: newTxnID},
	)
	return nil
}

// finishTxn finishes the transaction. It also calls setAdvanceInfo() with the
// given event.
//
// - advance indicates what action the statement buffer should take. This allows
// normal COMMIT/ROLLBACK to move to the next statement, and PL/pgSQL
// COMMIT/ROLLBACK to resume execution of the calling stored procedure.
func (ts *txnState) finishTxn(ev txnEventType, advance advanceCode) error {
	finishedTxnID, commitTimestamp := ts.finishSQLTxn()
	ts.setAdvanceInfo(advance, noRewind, txnEvent{
		eventType: ev, txnID: finishedTxnID, commitTimestamp: commitTimestamp,
	})
	return nil
}

// cleanupAndFinishOnError rolls back the KV txn and finishes the SQL txn.
func cleanupAndFinishOnError(args fsm.Args) error {
	ts := args.Extended.(*txnState)
	func() {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		_ = ts.mu.txn.Rollback(ts.Ctx)
	}()
	finishedTxnID, _ := ts.finishSQLTxn()
	ts.setAdvanceInfo(
		skipBatch,
		noRewind,
		txnEvent{eventType: txnRollback, txnID: finishedTxnID},
	)
	return nil
}

func prepareTxnForRetry(args fsm.Args) error {
	ts := args.Extended.(*txnState)
	err := func() error {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		return ts.mu.txn.PrepareForRetry(ts.Ctx)
	}()
	if err != nil {
		return err
	}
	ts.setAdvanceInfo(
		advanceOne,
		noRewind,
		txnEvent{eventType: txnRestart},
	)
	return nil
}

func moveToCommitWaitAfterInternalCommit(args fsm.Args) error {
	ts := args.Extended.(*txnState)
	txnID, commitTimestamp, err := func() (uuid.UUID, hlc.Timestamp, error) {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		txnID := ts.mu.txn.ID()
		commitTimestamp, err := ts.mu.txn.CommitTimestamp()
		return txnID, commitTimestamp, err
	}()
	if err != nil {
		return err
	}
	ts.setAdvanceInfo(
		advanceOne,
		noRewind,
		txnEvent{
			eventType:       txnCommit,
			txnID:           txnID,
			commitTimestamp: commitTimestamp,
		},
	)
	return nil
}

func prepareTxnForRetryWithRewind(args fsm.Args) error {
	pl := args.Payload.(eventRetriableErrPayload)
	ts := args.Extended.(*txnState)
	err := func() error {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		ts.mu.autoRetryReason = pl.err
		ts.mu.autoRetryCounter++
		return ts.mu.txn.PrepareForRetry(ts.Ctx)
	}()
	if err != nil {
		return err
	}
	// The caller will call rewCap.rewindAndUnlock().
	ts.setAdvanceInfo(
		rewind,
		pl.rewCap,
		txnEvent{eventType: txnRestart},
	)
	return nil
}

// BoundTxnStateTransitions is the state machine used by the InternalExecutor
// when running SQL inside a higher-level txn. It's a very limited state
// machine: it doesn't allow starting or finishing txns, auto-retries, etc.
var BoundTxnStateTransitions = fsm.Compile(fsm.Pattern{
	stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.False}: {
		// We accept eventNonRetriableErr with both IsCommit={True, fsm.False}, even
		// though this state machine does not support COMMIT statements because
		// connExecutor.close() sends an eventNonRetriableErr{IsCommit: fsm.True} event.
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			Next: stateInternalError{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				finishedTxnID, _ := ts.finishSQLTxn()
				ts.setAdvanceInfo(
					skipBatch,
					noRewind,
					txnEvent{eventType: txnRollback, txnID: finishedTxnID},
				)
				return nil
			},
		},
		eventRetriableErr{CanAutoRetry: fsm.Any, IsCommit: fsm.False}: {
			Next: stateInternalError{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				finishedTxnID, _ := ts.finishSQLTxn()
				ts.setAdvanceInfo(
					skipBatch,
					noRewind,
					txnEvent{eventType: txnRollback, txnID: finishedTxnID},
				)
				return nil
			},
		},
	},
})
