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

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// COCKROACH_TRACE_SQL=duration can be used to log SQL transactions that take
// longer than duration to complete. For example, COCKROACH_TRACE_SQL=1s will
// log the trace for any transaction that takes 1s or longer. To log traces for
// all transactions use COCKROACH_TRACE_SQL=1ns. Note that any positive
// duration will enable tracing and will slow down all execution because traces
// are gathered for all transactions even if they are not output.
var traceSQLDuration = envutil.EnvOrDefaultDuration("COCKROACH_TRACE_SQL", 0)
var traceSQL = traceSQLDuration > 0

// COCKROACH_DISABLE_SQL_EVENT_LOG can be used to disable the event log that is
// normally kept for every SQL connection. The event log has a non-trivial
// performance impact.
var disableSQLEventLog = envutil.EnvOrDefaultBool("COCKROACH_DISABLE_SQL_EVENT_LOG", false)

// COCKROACH_TRACE_7881 can be used to trace all SQL transactions, in the hope
// that we'll catch #7881 and dump the current trace for debugging.
var traceSQLFor7881 = envutil.EnvOrDefaultBool("COCKROACH_TRACE_7881", false)

// span baggage key used for marking a span
const keyFor7881Sample = "found#7881"

// Session contains the state of a SQL client connection.
// Create instances using NewSession().
type Session struct {
	//
	// Session parameters, user-configurable.
	//

	// ApplicationName is the name of the application running the
	// current session. This can be used for logging and per-application
	// statistics. Change via resetApplicationName().
	ApplicationName string
	// Database indicates the "current" database for the purpose of
	// resolving names. See searchAndQualifyDatabase() for details.
	Database string
	// DefaultIsolationLevel indicates the default isolation level of
	// newly created transactions.
	DefaultIsolationLevel enginepb.IsolationType
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode distSQLExecMode
	// Location indicates the current time zone.
	Location *time.Location
	// SearchPath is a list of databases that will be searched for a table name
	// before the database. Currently, this is used only for SELECTs.
	// Names in the search path must have been normalized already.
	SearchPath parser.SearchPath
	// Syntax determine which lexical structure to use for parsing.
	Syntax parser.Syntax
	// User is the name of the user logged into the session.
	User string

	//
	// State structures for the logical SQL session.
	//

	// TxnState carries information about the open transaction (if any),
	// including the retry status and the KV client Txn object.
	TxnState txnState
	// PreparedStatements and PreparedPortals store the statements/portals
	// that have been prepared via pgwire.
	PreparedStatements PreparedStatements
	PreparedPortals    PreparedPortals
	// virtualSchemas aliases Executor.virtualSchemas.
	// It is duplicated in Session to provide easier access to
	// the various methods that need this reference.
	// TODO(knz): place this in an executionContext parameter-passing
	// structure.
	virtualSchemas virtualSchemaHolder

	//
	// Run-time state.
	//

	// execCfg is the configuration of the Executor that is executing this
	// session.
	execCfg *ExecutorConfig
	// planner is the planner in charge of executing statements within this
	// session. While the planner type is logically scoped to the execution
	// of a single statement, this field will be reset and reused between
	// statements. When necessary, it can be cloned so that future modifications
	// to the field do not affect a separate stream of statement execution.
	planner planner
	// distSQLPlanner is in charge of distSQL physical planning and running
	// logic.
	distSQLPlanner *distSQLPlanner
	// context is the Session's base context, to be used for all
	// SQL-related logging. See Ctx().
	context context.Context
	// finishEventLog indicates whether an event log was started for
	// this session.
	finishEventLog bool
	// cancel is a method to call when the session terminates, to
	// release resources associated with the context above.
	// TODO(andrei): We need to either get rid of this cancel field, or
	// it needs to move to the TxnState and become a per-txn
	// cancel. Right now, we're cancelling all the txns that have ever
	// run on this session when the session is closed, as opposed to
	// cancelling the individual transactions as soon as they
	// COMMIT/ROLLBACK.
	cancel context.CancelFunc
	// mon tracks memory usage for SQL activity within this session. It
	// is not directly used, but rather indirectly used via sessionMon
	// and TxnState.mon. sessionMon tracks session-bound objects like prepared
	// statements and result sets.
	//
	// The reason why TxnState.mon and mon are split is to enable
	// separate reporting of statistics per transaction and per
	// session. This is because the "interesting" behavior w.r.t memory
	// is typically caused by transactions, not sessions. The reason why
	// sessionMon and mon are split is to enable separate reporting of
	// statistics for result sets (which escape transactions).
	mon        mon.MemoryMonitor
	sessionMon mon.MemoryMonitor
	// leases holds the state of per-table leases acquired by the leaseMgr.
	leases []*LeaseState
	// leaseMgr manages acquiring and releasing per-table leases.
	leaseMgr *LeaseManager
	// systemConfig holds a copy of the latest system config since the last
	// call to resetForBatch.
	systemConfig config.SystemConfig
	// databaseCache is used as a cache for database names.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	databaseCache *databaseCache
	// If set, contains the in progress COPY FROM columns.
	copyFrom *copyNode

	//
	// Per-session statistics.
	//

	// memMetrics track memory usage by SQL execution.
	memMetrics *MemoryMetrics
	// sqlStats tracks per-application statistics for all
	// applications on each node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics.
	appStats *appStats

	// noCopy is placed here to guarantee that Session objects are not
	// copied.
	noCopy util.NoCopy
}

// SessionArgs contains arguments for creating a new Session with NewSession().
type SessionArgs struct {
	Database        string
	User            string
	ApplicationName string
}

// NewSession creates and initializes a new Session object.
// remote can be nil.
func NewSession(
	ctx context.Context, args SessionArgs, e *Executor, remote net.Addr, memMetrics *MemoryMetrics,
) *Session {
	ctx = e.AnnotateCtx(ctx)
	cfg, cache := e.getSystemConfig()
	s := &Session{
		Database:       args.Database,
		SearchPath:     parser.SearchPath{"pg_catalog"},
		Location:       time.UTC,
		User:           args.User,
		virtualSchemas: e.virtualSchemas,
		execCfg:        &e.cfg,
		distSQLPlanner: e.distSQLPlanner,
		leaseMgr:       e.cfg.LeaseManager,
		systemConfig:   cfg,
		databaseCache:  cache,
		memMetrics:     memMetrics,
		sqlStats:       &e.sqlStats,
	}
	s.planner = planner{
		session: s,
	}
	s.planner.phaseTimes[sessionInit] = timeutil.Now()
	s.resetApplicationName(args.ApplicationName)
	s.PreparedStatements = makePreparedStatements(s)
	s.PreparedPortals = makePreparedPortals(s)

	if !disableSQLEventLog && opentracing.SpanFromContext(ctx) == nil {
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
	if s.mon == (mon.MemoryMonitor{}) {
		// This check won't catch the cases where Finish is never called, but it's
		// proven to be easier to remember to call Finish than it is to call
		// StartMonitor.
		panic("session.Finish: session monitors were never initialized. Missing call " +
			"to session.StartMonitor?")
	}

	// If we're inside a txn, roll it back.
	if s.TxnState.State.kvTxnIsOpen() {
		s.TxnState.updateStateAndCleanupOnErr(
			errors.Errorf("session closing"), e)
	}
	if s.TxnState.State != NoTxn {
		s.TxnState.finishSQLTxn(s.context)
	}

	// Cleanup leases. We might have unreleased leases if we're finishing the
	// session abruptly in the middle of a transaction, or, until #7648 is
	// addressed, there might be leases accumulated by preparing statements.
	s.planner.releaseLeases(s.context)

	s.ClearStatementsAndPortals(s.context)
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

// hijackCtx changes the current transaction's context to the provided one and
// returns a cleanup function to be used to restore the original context when
// the hijack is no longer needed.
// TODO(andrei): delete this when EXPLAIN(TRACE) goes away
func (s *Session) hijackCtx(ctx context.Context) func() {
	if s.TxnState.State != Open {
		// This hijacking is dubious to begin with. Let's at least assert it's being
		// done when the TxnState is in an expected state. In particular, if the
		// state would be NoTxn, then we'd need to hijack session.Ctx instead of the
		// txnState's context.
		log.Fatalf(ctx, "can only hijack while a SQL txn is Open. txnState: %+v",
			&s.TxnState)
	}
	return s.TxnState.hijackCtx(ctx)
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
	// When COCKROACH_TRACE_SQL is enabled, trace accumulates spans as
	// they're closed. All the spans pertain to the current txn.
	trace *tracing.RecordedTrace

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time

	// mon tracks txn-bound objects like the running state of
	// planNode in the midst of performing a computation. We
	// host this here instead of TxnState because TxnState is
	// fully reset upon each call to resetForNewSQLTxn().
	mon mon.MemoryMonitor
}

// resetForNewSQLTxn (re)initializes the txnState for a new transaction.
// It creates a new client.Txn and initializes it using the session defaults.
// txnState.State will be set to Open.
func (ts *txnState) resetForNewSQLTxn(e *Executor, s *Session) {
	if ts.sp != nil {
		panic(fmt.Sprintf("txnState.reset() called on ts with active span. How come "+
			"finishSQLTxn() wasn't called previously? ts: %+v", ts))
	}

	// Reset state vars to defaults.
	ts.retrying = false
	ts.retryIntent = false
	ts.autoRetry = false
	ts.commitSeen = false

	// Create a context for this transaction. It will include a
	// root span that will contain everything executed as part of the
	// upcoming SQL txn, including (automatic or user-directed) retries.
	// The span is closed by finishSQLTxn().
	// TODO(andrei): figure out how to close these spans on server shutdown? Ties
	// into a larger discussion about how to drain SQL and rollback open txns.
	ctx := s.context
	if traceSQL {
		var err error
		ctx, ts.trace, err = tracing.StartSnowballTrace(ctx, "traceSQL")
		if err != nil {
			log.Fatalf(ctx, "unable to create snowball tracer: %s", err)
		}
	} else if traceSQLFor7881 {
		var err error
		ctx, ts.trace, err = tracing.NewTracerAndSpanFor7881(ctx)
		if err != nil {
			log.Fatalf(ctx, "couldn't create a tracer for debugging #7881: %s", err)
		}
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
		// Put the new span in the context.
		ctx = opentracing.ContextWithSpan(ctx, sp)
	}

	ts.sp = opentracing.SpanFromContext(ctx)
	ts.Ctx = ctx

	ts.mon.Start(ctx, &s.mon, mon.BoundAccount{})

	ts.txn = client.NewTxn(e.cfg.DB)
	if err := ts.txn.SetIsolation(s.DefaultIsolationLevel); err != nil {
		panic(err)
	}
	ts.State = Open

	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

// willBeRetried returns true if the SQL transaction is going to be retried
// because of err.
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
				"(finalized: false)", state, ts.txn.Proto().Status))
	}
	ts.State = state
	ts.txn = nil
}

// finishSQLTxn closes the root span for the current SQL txn.
// This needs to be called before resetForNewSQLTransaction() is called for
// starting another SQL txn.
// The session context is just used for logging the SQL trace.
func (ts *txnState) finishSQLTxn(sessionCtx context.Context) {
	ts.mon.Stop(ts.Ctx)
	if ts.sp == nil {
		panic("No span in context? Was resetForNewSQLTxn() called previously?")
	}
	sampledFor7881 := (ts.sp.BaggageItem(keyFor7881Sample) != "")
	ts.sp.Finish()
	ts.sp = nil
	if (traceSQL && timeutil.Since(ts.sqlTimestamp) >= traceSQLDuration) ||
		(traceSQLFor7881 && sampledFor7881) {
		dump := tracing.FormatRawSpans(ts.trace.GetSpans())
		if len(dump) > 0 {
			log.Infof(sessionCtx, "SQL trace:\n%s", dump)
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
	if retErr, ok := err.(*roachpb.RetryableTxnError); !ok ||
		!ts.willBeRetried() ||
		!ts.txn.IsRetryableErrMeantForTxn(retErr) {

		// We can't or don't want to retry this txn, so the txn is over.
		e.TxnAbortCount.Inc(1)
		// This call rolls back a PENDING transaction and cleans up all its
		// intents.
		ts.txn.CleanupOnError(ts.Ctx, err)
		ts.resetStateAndTxn(Aborted)
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		ts.State = RestartWait
	}
}

// hijackCtx changes the transaction's context to the provided one and returns a
// cleanup function to be used to restore the original context when the hijack
// is no longer needed.
// TODO(andrei): delete this when EXPLAIN(TRACE) goes away.
func (ts *txnState) hijackCtx(ctx context.Context) func() {
	origCtx := ts.Ctx
	ts.Ctx = ctx
	return func() {
		ts.Ctx = origCtx
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
	ctx context.Context, e *Executor, planMaker *planner, results ResultList,
) {
	if planMaker.txn != nil {
		panic("trying to execute schema changes while still in a transaction")
	}
	// Release the leases once a transaction is complete.
	planMaker.releaseLeases(ctx)
	if e.cfg.SchemaChangerTestingKnobs.SyncFilter != nil {
		e.cfg.SchemaChangerTestingKnobs.SyncFilter(TestingSchemaChangerCollection{scc})
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	for _, scEntry := range scc.schemaChangers {
		sc := &scEntry.sc
		sc.db = *e.cfg.DB
		sc.testingKnobs = e.cfg.SchemaChangerTestingKnobs
		sc.distSQLPlanner = e.distSQLPlanner
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			if err := sc.exec(ctx); err != nil {
				if err != errExistingSchemaChangeLease {
					log.Warningf(ctx, "Error executing schema change: %s", err)
				}
				if err == sqlbase.ErrDescriptorNotFound {
				} else if sqlbase.IsIntegrityConstraintError(err) {
					// All constraint violations can be reported; we report it as the result
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
				} else {
					// retryable error.
					continue
				}
			}
			break
		}
	}
	scc.schemaChangers = scc.schemaChangers[:0]
}
