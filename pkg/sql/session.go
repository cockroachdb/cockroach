// Copyright 2015 The Cockroach Authors.
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
// Author: Vivek Menezes (vivek@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// COCKROACH_TRACE_SQL=duration can be used to log SQL transactions that take
// longer than duration to complete. For example, COCKROACH_TRACE_SQL=1s will
// log the trace for any transaction that takes 1s or longer. To log traces for
// all transactions use COCKROACH_TRACE_SQL=1ns. Note that any positive
// duration will enable tracing and will slow down all execution because traces
// are gathered for all transactions even if they are not output.
var traceSQLDuration = envutil.EnvOrDefaultDuration("COCKROACH_TRACE_SQL", 0)
var traceSQL = traceSQLDuration > 0

// COCKROACH_TRACE_7881 can be used to trace all SQL transactions, in the hope
// that we'll catch #7881 and dump the current trace for debugging.
var traceSQLFor7881 = envutil.EnvOrDefaultBool("COCKROACH_TRACE_7881", false)

// span baggage key used for marking a span
const keyFor7881Sample = "found#7881"

// Session contains the state of a SQL client connection.
// Create instances using NewSession().
type Session struct {
	Database string
	// SearchPath is a list of databases that will be searched for a table name
	// before the database. Currently, this is used only for SELECTs.
	//
	// NOTE: If we allow the user to set this, we'll need to handle the case where
	// the session database or pg_catalog are in this path.
	SearchPath  []string
	User        string
	Syntax      int32
	DistSQLMode distSQLExecMode

	virtualSchemas virtualSchemaHolder

	// Info about the open transaction (if any).
	TxnState txnState

	planner            planner
	memMetrics         *MemoryMetrics
	PreparedStatements PreparedStatements
	PreparedPortals    PreparedPortals

	Location              *time.Location
	DefaultIsolationLevel enginepb.IsolationType
	// context is the Session's base context. See Ctx().
	context        context.Context
	finishEventLog bool
	// TODO(andrei): We need to either get rid of this cancel field, or it needs
	// to move to the TxnState and become a per-txn cancel. Right now, we're
	// cancelling all the txns that have ever run on this session when the session
	// is closed, as opposed to cancelling the individual transactions as soon as
	// they COMMIT/ROLLBACK.
	cancel context.CancelFunc

	// mon tracks memory usage for SQL activity within this session.
	// It is not directly used, but rather indirectly used via
	// sessionMon and txnMon. sessionMon tracks session-bound objects
	// like prepared statements and result sets. txnMon tracks txn-bound objects.
	mon        mon.MemoryMonitor
	sessionMon mon.MemoryMonitor
	txnMon     mon.MemoryMonitor
}

// SessionArgs contains arguments for creating a new Session with NewSession().
type SessionArgs struct {
	Database string
	User     string
}

// NewSession creates and initializes a new Session object.
// remote can be nil.
func NewSession(
	ctx context.Context, args SessionArgs, e *Executor, remote net.Addr, memMetrics *MemoryMetrics,
) *Session {
	ctx = e.AnnotateCtx(ctx)
	s := &Session{
		Database:       args.Database,
		SearchPath:     []string{"pg_catalog"},
		User:           args.User,
		Location:       time.UTC,
		virtualSchemas: e.virtualSchemas,
		memMetrics:     memMetrics,
	}
	cfg, cache := e.getSystemConfig()
	s.planner = planner{
		leaseMgr:      e.cfg.LeaseManager,
		systemConfig:  cfg,
		databaseCache: cache,
		session:       s,
		execCfg:       &e.cfg,
	}
	s.PreparedStatements = makePreparedStatements(s)
	s.PreparedPortals = makePreparedPortals(s)

	if opentracing.SpanFromContext(ctx) == nil {
		remoteStr := "<admin>"
		if remote != nil {
			remoteStr = remote.String()
		}
		// Set up an EventLog for session events.
		ctx = log.WithEventLog(ctx, fmt.Sprintf("sql [%s]", args.User), remoteStr)
		s.finishEventLog = true
	}
	s.context, s.cancel = context.WithCancel(ctx)

	return s
}

// Finish releases resources held by the Session.
func (s *Session) Finish(e *Executor) {
	// If we're inside a txn, roll it back.
	if s.TxnState.State.kvTxnIsOpen() {
		s.TxnState.updateStateAndCleanupOnErr(
			errors.Errorf("session closing"), e)
	}
	if s.TxnState.State != NoTxn {
		s.TxnState.finishSQLTxn(s)
	}

	// Cleanup leases. We might have unreleased leases if we're finishing the
	// session abruptly in the middle of a transaction, or, until #7648 is
	// addressed, there might be leases accumulated by preparing statements.
	s.planner.releaseLeases()

	s.ClearStatementsAndPortals()
	s.sessionMon.Stop(s.context)
	s.mon.Stop(s.context)

	if s.finishEventLog {
		log.FinishEventLog(s.context)
	}

	// This will stop the heartbeating of the of the txn record.
	// TODO(andrei): This shouldn't have any effect, since, if there was a
	// transaction, we just explicitly rolled it back above, so the heartbeat loop
	// in the TxnCoordSender should not be waiting on this channel any more.
	// Consider getting rid of this cancel field all-together.
	s.cancel()
}

// Ctx returns the current context for the session: if there is an active SQL
// transaction it returns the transaction context, otherwise it returns the
// session context.
// Note that in some cases we may want the session context even if there is an
// active transaction (an example is when we want to log an event to the session
// event log); in that case s.context should be used directly.
func (s *Session) Ctx() context.Context {
	if s.TxnState.State != NoTxn {
		return s.TxnState.Ctx
	}
	return s.context
}

// TxnStateEnum represents the state of a SQL txn.
type TxnStateEnum int

//go:generate stringer -type=TxnStateEnum
const (
	// No txn is in scope. Either there never was one, or it got committed/rolled back.
	NoTxn TxnStateEnum = iota
	// A txn is in scope.
	Open
	// The txn has encountered a (non-retriable) error.
	// Statements will be rejected until a COMMIT/ROLLBACK is seen.
	Aborted
	// The txn has encountered a retriable error.
	// Statements will be rejected until a RESTART_TRANSACTION is seen.
	RestartWait
	// The KV txn has been committed successfully through a RELEASE.
	// Statements are rejected until a COMMIT is seen.
	CommitWait
)

// Some states mean that a client.Txn is open, others don't.
func (s TxnStateEnum) kvTxnIsOpen() bool {
	return s == Open || s == RestartWait
}

// txnState contains state associated with an ongoing SQL txn.
// There may or may not be an open KV txn associated with the SQL txn.
// For interactive transactions (open across batches of SQL commands sent by a
// user), txnState is intended to be stored as part of a user Session.
type txnState struct {
	txn   *client.Txn
	State TxnStateEnum

	// Ctx is the context for everything running in this SQL txn.
	Ctx context.Context

	// retrying is used to work around the non-idempotence of SAVEPOINT
	// queries.
	//
	// See the comment at the site of its use for more detail.
	retrying bool

	// If set, the user declared the intention to retry the txn in case of retriable
	// errors. The txn will enter a RestartWait state in case of such errors.
	retryIntent bool

	// The transaction will be retried in case of retriable error. The retry will be
	// automatic (done by Txn.Exec()). This field behaves the same as retryIntent,
	// except it's reset in between client round trips.
	autoRetry bool

	// A COMMIT statement has been processed. Useful for allowing the txn to
	// survive retriable errors if it will be auto-retried (BEGIN; ... COMMIT; in
	// the same batch), but not if the error needs to be reported to the user.
	commitSeen bool

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection

	sp opentracing.Span
	// When COCKROACH_TRACE_SQL is enabled, CollectedSpans accumulates spans as
	// they're closed. All the spans pertain to the current txn.
	CollectedSpans []basictracer.RawSpan

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time
}

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
// txnState.State will be set to Open.
func (ts *txnState) resetForNewSQLTxn(e *Executor, s *Session) {
	if ts.sp != nil {
		panic(fmt.Sprintf("txnState.reset() called on ts with active span. How come "+
			"finishSQLTxn() wasn't called previously? ts: %+v", ts))
	}

	*ts = txnState{}

	// Create a context for this transaction. It will include a
	// root span that will contain everything executed as part of the
	// upcoming SQL txn, including (automatic or user-directed) retries.
	// The span is closed by finishSQLTxn().
	// TODO(andrei): figure out how to close these spans on server shutdown?
	ctx := s.context
	if traceSQL {
		sp, err := tracing.JoinOrNewSnowball("coordinator", nil, func(sp basictracer.RawSpan) {
			ts.CollectedSpans = append(ts.CollectedSpans, sp)
		})
		if err != nil {
			log.Warningf(ctx, "unable to create snowball tracer: %s", err)
			return
		}
		// sp is using a new tracer, so put it in the context.
		ctx = tracing.WithTracer(
			opentracing.ContextWithSpan(ctx, sp), sp.Tracer())
	} else if traceSQLFor7881 {
		sp, tr, err := tracing.NewTracerAndSpanFor7881(func(sp basictracer.RawSpan) {
			ts.CollectedSpans = append(ts.CollectedSpans, sp)
		})
		if err != nil {
			panic(fmt.Sprintf("couldn't create a tracer for debugging #7881: %s", err))
		}
		// Put both the new tracer and the span in the txn's context.
		ctx = tracing.WithTracer(
			opentracing.ContextWithSpan(ctx, sp), tr)
	} else {
		var sp opentracing.Span
		if parentSp := opentracing.SpanFromContext(ctx); parentSp != nil {
			// Create a child span for this SQL txn.
			tracer := parentSp.Tracer()
			sp = tracer.StartSpan("sql txn", opentracing.ChildOf(parentSp.Context()))
		} else {
			// Create a root span for this SQL txn.
			tracer := e.cfg.AmbientCtx.Tracer
			sp = tracer.StartSpan("sql txn")
		}
		ctx = opentracing.ContextWithSpan(ctx, sp)
	}
	ts.Ctx = ctx

	s.txnMon.Start("txn-mon",
		s.memMetrics.TxnCurBytesCount,
		s.memMetrics.TxnMaxBytesHist,
		&s.mon, mon.BoundAccount{}, 1)

	ts.txn = client.NewTxn(ts.Ctx, *e.cfg.DB)
	ts.txn.Proto.Isolation = s.DefaultIsolationLevel
	ts.State = Open

	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

func (ts *txnState) willBeRetried() bool {
	return ts.autoRetry || ts.retryIntent
}

// resetStateAndTxn moves the txnState into a specified state, as a result of
// the client.Txn being done.
func (ts *txnState) resetStateAndTxn(state TxnStateEnum) {
	if state != NoTxn && state != Aborted && state != CommitWait {
		panic(fmt.Sprintf("resetStateAndTxn called with unsupported state: %s", state))
	}
	if ts.txn != nil && !ts.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to move SQL txn to state %s inconsistent with KV txn state: %s "+
				"(finalized: %t)", state, ts.txn.Proto.Status, ts.txn.IsFinalized()))
	}

	if ts.txn != nil {
		if len(ts.CollectedSpans) == 0 {
			ts.CollectedSpans = ts.txn.CollectedSpans
		} else {
			ts.CollectedSpans = append(ts.CollectedSpans, ts.txn.CollectedSpans...)
		}
	}
	ts.State = state
	ts.txn = nil
}

// finishSQLTxn closes the root span for the current SQL txn.
// This needs to be called before resetForNewSQLTransaction() is called for
// starting another SQL txn.
func (ts *txnState) finishSQLTxn(s *Session) {
	s.txnMon.Stop(ts.Ctx)
	span := opentracing.SpanFromContext(ts.Ctx)
	if span == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}
	sampledFor7881 := (span.BaggageItem(keyFor7881Sample) != "")
	span.Finish()
	if (traceSQL && timeutil.Since(ts.sqlTimestamp) >= traceSQLDuration) ||
		(traceSQLFor7881 && sampledFor7881) {
		dump := tracing.FormatRawSpans(ts.CollectedSpans)
		if len(dump) > 0 {
			log.Infof(s.context, "SQL trace:\n%s", dump)
		}
	}
}

// updateStateAndCleanupOnErr updates txnState based on the type of error that we
// received. If it's a retriable error and we're going to retry the txn,
// then the state moves to RestartWait. Otherwise, the state moves to Aborted
// and the KV txn is cleaned up.
func (ts *txnState) updateStateAndCleanupOnErr(err error, e *Executor) {
	if err == nil {
		panic("updateStateAndCleanupOnErr called with no error")
	}
	if _, ok := err.(*roachpb.RetryableTxnError); !ok || !ts.willBeRetried() {
		// We can't or don't want to retry this txn, so the txn is over.
		e.TxnAbortCount.Inc(1)
		ts.txn.CleanupOnError(err)
		ts.resetStateAndTxn(Aborted)
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		ts.State = RestartWait
	}
}

type schemaChangerCollection struct {
	// A schemaChangerCollection accumulates schemaChangers from potentially
	// multiple user requests, part of the same SQL transaction. We need to
	// remember what group and index within the group each schemaChanger came
	// from, so we can map failures back to the statement that produced them.
	curGroupNum int

	// The index of the current statement, relative to its group. For statements
	// that have been received from the client in the same batch, the
	// group consists of all statements in the same transaction.
	curStatementIdx int
	// schema change callbacks together with the index of the statement
	// that enqueued it (within its group of statements).
	schemaChangers []struct {
		epoch int
		idx   int
		sc    SchemaChanger
	}
}

func (scc *schemaChangerCollection) queueSchemaChanger(schemaChanger SchemaChanger) {
	scc.schemaChangers = append(
		scc.schemaChangers,
		struct {
			epoch int
			idx   int
			sc    SchemaChanger
		}{scc.curGroupNum, scc.curStatementIdx, schemaChanger})
}

// execSchemaChanges releases schema leases and runs the queued
// schema changers. This needs to be run after the transaction
// scheduling the schema change has finished.
//
// The list of closures is cleared after (attempting) execution.
//
// Args:
//  results: The results from all statements in the group that scheduled the
//    schema changes we're about to execute. Results corresponding to the
//    schema change statements will be changed in case an error occurs.
func (scc *schemaChangerCollection) execSchemaChanges(
	e *Executor, planMaker *planner, results ResultList,
) {
	if planMaker.txn != nil {
		panic("trying to execute schema changes while still in a transaction")
	}
	ctx := e.AnnotateCtx(context.TODO())
	// Release the leases once a transaction is complete.
	planMaker.releaseLeases()
	if e.cfg.SchemaChangerTestingKnobs.SyncFilter != nil {
		e.cfg.SchemaChangerTestingKnobs.SyncFilter(TestingSchemaChangerCollection{scc})
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	for _, scEntry := range scc.schemaChangers {
		sc := &scEntry.sc
		sc.db = *e.cfg.DB
		sc.testingKnobs = e.cfg.SchemaChangerTestingKnobs
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			if done, err := sc.IsDone(); err != nil {
				log.Warning(ctx, err)
				break
			} else if done {
				break
			}
			if err := sc.exec(); err != nil {
				if isSchemaChangeRetryError(err) {
					// Try again
					continue
				}
				// All other errors can be reported; we report it as the result
				// corresponding to the statement that enqueued this changer.
				// There's some sketchiness here: we assume there's a single result
				// per statement and we clobber the result/error of the corresponding
				// statement.
				// There's also another subtlety: we can only report results for
				// statements in the current batch; we can't modify the results of older
				// statements.
				if scEntry.epoch == scc.curGroupNum {
					results[scEntry.idx] = Result{Err: err}
				}
				log.Warningf(ctx, "error executing schema change: %s", err)
			}
			break
		}
	}
	scc.schemaChangers = scc.schemaChangers[:0]
}
