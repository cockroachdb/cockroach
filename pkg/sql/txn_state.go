// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"time"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// txnState contains state associated with an ongoing SQL txn; it constitutes
// the ExtendedState of a connExecutor's state machine (defined in conn_fsm.go).
// It contains fields that are mutated as side-effects of state transitions;
// notably the KV client.Txn.  All mutations to txnState are performed through
// calling fsm.Machine.Apply(event); see conn_fsm.go for the definition of the
// state machine.
type txnState2 struct {
	// Mutable fields accessed from goroutines not synchronized by this txn's
	// session, such as when a SHOW SESSIONS statement is executed on another
	// session.
	// Note that reads of mu.txn from the session's main goroutine do not require
	// acquiring a read lock - since only that goroutine will ever write to
	// mu.txn.
	mu struct {
		syncutil.RWMutex

		txn *client.Txn
	}

	// Ctx is the context for everything running in this SQL txn.
	// This is only set while the session's state is not stateNoTxn.
	Ctx context.Context

	// sp is the span corresponding to the SQL txn. These are often root spans, as
	// SQL txns are frequently the level at which we do tracing.
	sp opentracing.Span

	// cancel is the cancellation function for the above context. Called upon
	// COMMIT/ROLLBACK of the transaction to release resources associated with the
	// context. nil when no txn is in progress.
	cancel context.CancelFunc

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// The transaction's isolation level.
	isolation enginepb.IsolationType

	// The transaction's priority.
	priority roachpb.UserPriority

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation.
	mon *mon.BytesMonitor

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection

	// adv is overwritten after every transition. It represents instructions for
	// for moving the cursor over the stream of input statements to the next
	// statement to be executed.
	// Do not use directly; set through setAdvanceInfo() and read through
	// consumeAdvanceInfo().
	adv advanceInfo
}

// txnType represents the type of a SQL transaction.
type txnType int

//go:generate stringer -type=txnType
const (
	// implicitTxn means that the txn was created for a (single) SQL statement
	// executed outside of a transaction.
	implicitTxn txnType = iota
	// explicitTxn means that the txn was explicitly started with a BEGIN
	// statement.
	explicitTxn
)

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
//
// connCtx: The context in which the new transaction is started (usually a
// 	 connection's context). ts.Ctx will be set to a child context and should be
// 	 used for everything that happens within this SQL transaction.
// txnType: The type of the starting txn.
// sqlTimestamp: The timestamp to report for current_timestamp(), now() etc.
// isolation: The transaction's isolation level.
// priority: The transaction's priority.
// tranCtx: A bag of extra execution context.
func (ts *txnState2) resetForNewSQLTxn(
	connCtx context.Context,
	txnType txnType,
	sqlTimestamp time.Time,
	isolation enginepb.IsolationType,
	priority roachpb.UserPriority,
	tranCtx transitionCtx,
) {
	// Reset state vars to defaults.
	ts.sqlTimestamp = sqlTimestamp

	// Create a context for this transaction. It will include a root span that
	// will contain everything executed as part of the upcoming SQL txn, including
	// (automatic or user-directed) retries. The span is closed by finishSQLTxn().
	// TODO(andrei): figure out how to close these spans on server shutdown? Ties
	// into a larger discussion about how to drain SQL and rollback open txns.
	var sp opentracing.Span
	var opName string
	if txnType == implicitTxn {
		opName = sqlImplicitTxnName
	} else {
		opName = sqlTxnName
	}

	// Create a span for the new txn. The span is always Recordable to support the
	// use of session tracing, which may start recording on it.
	if parentSp := opentracing.SpanFromContext(connCtx); parentSp != nil {
		// Create a child span for this SQL txn.
		sp = parentSp.Tracer().StartSpan(
			opName, opentracing.ChildOf(parentSp.Context()), tracing.Recordable)
	} else {
		// Create a root span for this SQL txn.
		sp = tranCtx.tracer.StartSpan(opName, tracing.Recordable)
	}

	// Put the new span in the context.
	txnCtx := opentracing.ContextWithSpan(connCtx, sp)

	if !tracing.IsRecordable(sp) {
		log.Fatalf(connCtx, "non-recordable transaction span of type: %T", sp)
	}

	ts.sp = sp
	ts.Ctx, ts.cancel = contextutil.WithCancel(txnCtx)

	ts.mon.Start(ts.Ctx, tranCtx.connMon, mon.BoundAccount{} /* reserved */)

	ts.mu.Lock()
	ts.mu.txn = client.NewTxn(tranCtx.db, tranCtx.nodeID, client.RootTxn)
	ts.mu.txn.SetDebugName(opName)
	ts.mu.Unlock()

	if err := ts.setIsolationLevel(isolation); err != nil {
		panic(err)
	}
	if err := ts.setPriority(priority); err != nil {
		panic(err)
	}

	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

// finishSQLTxn finalizes a transaction's results and closes the root span for
// the current SQL txn. This needs to be called before resetForNewSQLTxn() is
// called for starting another SQL txn.
//
// ctx is the connExecutor's context. This will be used once ts.Ctx is
// finalized.
func (ts *txnState2) finishSQLTxn(connCtx context.Context) {
	ts.mon.Stop(ts.Ctx)
	if ts.cancel != nil {
		ts.cancel()
		ts.cancel = nil
	}
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}

	if !ts.mu.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to finishSQLTxn(), but KV txn is not finalized: %+v", ts.mu.txn))
	}

	ts.sp.Finish()
	ts.sp = nil
	ts.Ctx = nil
	ts.mu.txn = nil
}

func (ts *txnState2) setIsolationLevel(isolation enginepb.IsolationType) error {
	if err := ts.mu.txn.SetIsolation(isolation); err != nil {
		return err
	}
	ts.isolation = isolation
	return nil
}

func (ts *txnState2) setPriority(userPriority roachpb.UserPriority) error {
	if err := ts.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	ts.priority = userPriority
	return nil
}

// advanceCode is part of advanceInfo; it instructs the module managing the
// statements buffer on what action to take.
type advanceCode int

//go:generate stringer -type=advanceCode
const (
	advanceUnknown advanceCode = iota
	// stayInPlace means that the cursor should remain where it is. The same
	// statement will be executed next.
	stayInPlace
	// advanceOne means that the cursor should be advanced by one position. This
	// is the code commonly used after a successful statement execution.
	advanceOne
	// skipQueryStr means that the cursor should skip over any remaining
	// statements that are part of the current query string and be positioned on
	// the first statement in the next query string.
	skipQueryStr
	// rewind means that the cursor should be moved back to the position indicated
	// by rewCap.
	rewind
)

// WIP(andrei)
var _ = advanceUnknown

// advanceInfo represents instructions for the connExecutor about what statement
// to execute next (how to move its cursor over the input statements) and how
// to handle the results produced so far - can they be delivered to the client
// ASAP or not. advanceInfo is the "output" of performing a state transition.
type advanceInfo struct {
	code advanceCode

	// flush, if set, means that all results produced so far can be delivered to
	// the client; we will never need to rewind beyond the current point in the
	// results stream.
	// When the connExecutor sees flush set, it instructs the results delivery
	// module to deliver the accumulated results before creating any more
	// StatementResults. Separately, it records the position of the cursor running
	// through input statements *after* applying the advanceCode above as the
	// transaction start position (the point to which a future rewind will move
	// the cursor). Through the advanceCode, the state machine transition has
	// control over whether future rewinds should point to the statement that
	// generated the transition (code == stayInPlace) or the statement after it
	// (code == advanceOne). The former is the case when we start a new
	// transaction (implicit or explicit), the latter is used, for example, when
	// executing a COMMIT.
	flush bool

	// Fields for the rewind code:

	// rewCap is the capability to rewind to the beginning of the transaction.
	// rewCap.rewindAndUnlock() needs to be called to perform the promised rewind.
	//
	// This field should not be set directly; buildRewindInstructions() should be
	// used.
	rewCap rewindCapability
}

// transitionCtx is a bag of fields needed by some state machine events.
type transitionCtx struct {
	db     *client.DB
	nodeID roachpb.NodeID
	clock  *hlc.Clock
	// connMon is the connExecutor's monitor. New transactions will create a child
	// monitor tracking txn-scoped objects.
	connMon *mon.BytesMonitor
	// The Tracer used to create root spans for new txns if the parent ctx doesn't
	// have a span.
	tracer opentracing.Tracer

	defaultIsolationLevel enginepb.IsolationType
}

// setAdvanceInfo sets the adv field. This has to be called as part of any state
// transition. The connExecutor is supposed to inspect adv after any transition
// and act on it.
func (ts *txnState2) setAdvanceInfo(adv advanceInfo) {
	if ts.adv.code != advanceUnknown {
		panic("previous advanceInfo has not been consume()d")
	}
	ts.adv = adv
}

// consumerAdvanceInfo returns the advanceInfo set by the last transition and
// resets the state so that another transition can overwrite it.
func (ts *txnState2) consumeAdvanceInfo() advanceInfo {
	adv := ts.adv
	ts.adv = advanceInfo{}
	return adv
}
