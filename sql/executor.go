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
// Author: Tamir Duberstein (tamird@gmail.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")
var errNotRetriable = errors.New("the transaction is not in a retriable state")

const sqlTxnName string = "sql txn"
const sqlImplicitTxnName string = "sql txn implicit"

var defaultRetryOpt = retry.Options{
	InitialBackoff: 20 * time.Millisecond,
	MaxBackoff:     200 * time.Millisecond,
	Multiplier:     2,
}

// Release through releasePlanner().
var plannerPool = sync.Pool{
	New: func() interface{} {
		return makePlanner()
	},
}

func releasePlanner(p *planner) {
	// Ensure future users don't clobber the session just used.
	p.session = nil
	plannerPool.Put(p)
}

type traceResult struct {
	tag   string
	count int
}

func (r *traceResult) String() string {
	if r.count < 0 {
		return r.tag
	}
	return fmt.Sprintf("%s %d", r.tag, r.count)
}

// ResultList represents a list of results for a list of SQL statements.
// There is one result object per SQL statement in the request.
type ResultList []Result

// StatementResults represents a list of results from running a batch of
// SQL statements, plus some meta info about the batch.
type StatementResults struct {
	ResultList
	// Indicates that after parsing, the request contained 0 non-empty statements.
	Empty bool
}

// Result corresponds to the execution of a single SQL statement.
type Result struct {
	PErr *roachpb.Error
	// The type of statement that the result is for.
	Type parser.StatementType
	// The tag of the statement that the result is for.
	PGTag string
	// RowsAffected will be populated if the statement type is "RowsAffected".
	RowsAffected int
	// Columns will be populated if the statement type is "Rows". It will contain
	// the names and types of the columns returned in the result set in the order
	// specified in the SQL statement. The number of columns will equal the number
	// of values in each Row.
	Columns []ResultColumn
	// Rows will be populated if the statement type is "Rows". It will contain
	// the result set of the result.
	// TODO(nvanbenschoten): Can this be streamed from the planNode?
	Rows []ResultRow
}

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  parser.Datum

	// If set, this is an implicit column; used internally.
	hidden bool
}

// ResultRow is a collection of values representing a row in a result.
type ResultRow struct {
	Values []parser.Datum
}

// An Executor executes SQL statements.
// Executor is thread-safe.
type Executor struct {
	nodeID  roachpb.NodeID
	ctx     ExecutorContext
	reCache *parser.RegexpCache

	// Transient stats.
	registry      *metric.Registry
	latency       metric.Histograms
	selectCount   *metric.Counter
	txnBeginCount *metric.Counter

	// txnCommitCount counts the number of times a COMMIT was attempted.
	txnCommitCount *metric.Counter

	txnAbortCount    *metric.Counter
	txnRollbackCount *metric.Counter
	updateCount      *metric.Counter
	insertCount      *metric.Counter
	deleteCount      *metric.Counter
	ddlCount         *metric.Counter
	miscCount        *metric.Counter
	queryCount       *metric.Counter

	// System Config and mutex.
	systemConfig   config.SystemConfig
	databaseCache  *databaseCache
	systemConfigMu sync.RWMutex
	// This uses systemConfigMu in RLocker mode to not block
	// execution of statements. So don't go on changing state after you've
	// Wait()ed on it.
	systemConfigCond *sync.Cond
}

// An ExecutorContext encompasses the auxiliary objects and configuration
// required to create an executor.
// All fields holding a pointer or an interface are required to create
// a Executor; the rest will have sane defaults set if omitted.
type ExecutorContext struct {
	DB           *client.DB
	Gossip       *gossip.Gossip
	LeaseManager *LeaseManager
	Clock        *hlc.Clock

	TestingKnobs *ExecutorTestingKnobs
}

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// WaitForGossipUpdate causes metadata-mutating operations to wait
	// for the new metadata to back-propagate through gossip.
	WaitForGossipUpdate bool

	// CheckStmtStringChange causes Executor.execStmtsInCurrentTxn to verify
	// that executed statements are not modified during execution.
	CheckStmtStringChange bool

	// FixTxnPriority causes transaction priority values to be hardcoded (for
	// each priority level) to avoid the randomness in the normal generation.
	FixTxnPriority bool
}

// NewExecutor creates an Executor and registers a callback on the
// system config.
func NewExecutor(ctx ExecutorContext, stopper *stop.Stopper, registry *metric.Registry) *Executor {
	exec := &Executor{
		ctx:     ctx,
		reCache: parser.NewRegexpCache(512),

		registry:         registry,
		latency:          registry.Latency("latency"),
		txnBeginCount:    registry.Counter("txn.begin.count"),
		txnCommitCount:   registry.Counter("txn.commit.count"),
		txnAbortCount:    registry.Counter("txn.abort.count"),
		txnRollbackCount: registry.Counter("txn.rollback.count"),
		selectCount:      registry.Counter("select.count"),
		updateCount:      registry.Counter("update.count"),
		insertCount:      registry.Counter("insert.count"),
		deleteCount:      registry.Counter("delete.count"),
		ddlCount:         registry.Counter("ddl.count"),
		miscCount:        registry.Counter("misc.count"),
		queryCount:       registry.Counter("query.count"),
	}
	exec.systemConfigCond = sync.NewCond(exec.systemConfigMu.RLocker())

	gossipUpdateC := ctx.Gossip.RegisterSystemConfigChannel()
	stopper.RunWorker(func() {
		for {
			select {
			case <-gossipUpdateC:
				cfg, _ := ctx.Gossip.GetSystemConfig()
				exec.updateSystemConfig(cfg)
			case <-stopper.ShouldStop():
				return
			}
		}
	})

	return exec
}

// SetNodeID sets the node ID for the SQL server. This method must be called
// before actually using the Executor.
func (e *Executor) SetNodeID(nodeID roachpb.NodeID) {
	e.nodeID = nodeID
	e.ctx.LeaseManager.nodeID = uint32(nodeID)
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (e *Executor) updateSystemConfig(cfg config.SystemConfig) {
	e.systemConfigMu.Lock()
	e.systemConfig = cfg
	// The database cache gets reset whenever the system config changes.
	e.databaseCache = &databaseCache{
		databases: map[string]ID{},
	}
	e.systemConfigCond.Broadcast()
	e.systemConfigMu.Unlock()
}

// getSystemConfig returns a pointer to the latest system config. May be nil,
// if the gossip callback has not run.
func (e *Executor) getSystemConfig() (config.SystemConfig, *databaseCache) {
	e.systemConfigMu.RLock()
	cfg, cache := e.systemConfig, e.databaseCache
	e.systemConfigMu.RUnlock()
	return cfg, cache
}

// Prepare returns the result types of the given statement. Args may be a
// partially populated val args map. Prepare will populate the missing val
// args. The column result types are returned (or nil if there are no results).
func (e *Executor) Prepare(ctx context.Context, user string, query string, session *Session, args parser.MapArgs) (
	[]ResultColumn, *roachpb.Error) {
	stmt, err := parser.ParseOne(query, parser.Syntax(session.Syntax))
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	planMaker := plannerPool.Get().(*planner)
	defer releasePlanner(planMaker)

	cfg, cache := e.getSystemConfig()
	*planMaker = planner{
		user: user,
		evalCtx: parser.EvalContext{
			NodeID:      e.nodeID,
			ReCache:     e.reCache,
			GetLocation: session.getLocation,
			Args:        args,
			PrepareOnly: true,
		},
		leaseMgr:      e.ctx.LeaseManager,
		systemConfig:  cfg,
		databaseCache: cache,
		session:       session,
		execCtx:       &e.ctx,
	}

	txn := client.NewTxn(ctx, *e.ctx.DB)
	txn.Proto.Isolation = session.DefaultIsolationLevel

	planMaker.setTxn(txn)
	plan, pErr := planMaker.prepare(stmt)
	if pErr != nil {
		return nil, pErr
	}
	if plan == nil {
		return nil, nil
	}
	cols := plan.Columns()
	for _, c := range cols {
		if err := checkResultDatum(c.Typ); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	return cols, nil
}

type schemaChangerCollection struct {
	// The index of the current statement, relative to its group. For statements
	// statements that have been received from the client in the same batch, the
	// group consists of all statements in the same transaction.
	curStatementIdx int
	// schema change callbacks together with the index of the statement
	// that enqueued it (within its group of statements).
	// TODO(andrei): Schema changers enqueued in a txn are not restored
	// if the txn is not COMMITTED in the same group of statements (#4428).
	schemaChangers []struct {
		idx int
		sc  SchemaChanger
	}
}

func (scc *schemaChangerCollection) queueSchemaChanger(
	schemaChanger SchemaChanger) {
	scc.schemaChangers = append(
		scc.schemaChangers,
		struct {
			idx int
			sc  SchemaChanger
		}{scc.curStatementIdx, schemaChanger})
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
	e *Executor, planMaker *planner, results ResultList) {
	if planMaker.txn != nil {
		panic("trying to execute schema changes while still in a transaction")
	}
	// Release the leases once a transaction is complete.
	planMaker.releaseLeases()
	if len(scc.schemaChangers) == 0 ||
		// Disable execution in some tests.
		disableSyncSchemaChangeExec {
		return
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	retryOpt := defaultRetryOpt
	for _, scEntry := range scc.schemaChangers {
		sc := &scEntry.sc
		sc.db = *e.ctx.DB
		for r := retry.Start(retryOpt); r.Next(); {
			if done, err := sc.IsDone(); err != nil {
				log.Warning(err)
				break
			} else if done {
				break
			}
			if pErr := sc.exec(); pErr != nil {
				if _, ok := pErr.GetDetail().(*roachpb.ExistingSchemaChangeLeaseError); ok {
					// Try again.
					continue
				}
				// All other errors can be reported; we report it as the result
				// corresponding to the statement that enqueued this changer.
				// There's some sketchiness here: we assume there's a single result
				// per statement and we clobber the result/error of the corresponding
				// statement.
				results[scEntry.idx] = Result{PErr: pErr}
			}
			break
		}
	}
	scc.schemaChangers = scc.schemaChangers[:0]
}

// reset creates a new Txn and initializes it using the session defaults.
func (ts *txnState) reset(ctx context.Context, e *Executor, s *Session) {
	*ts = txnState{}
	ts.txn = client.NewTxn(ctx, *e.ctx.DB)
	ts.txn.Proto.Isolation = s.DefaultIsolationLevel
	ts.tr = s.Trace
}

func (ts *txnState) willBeRetried() bool {
	return ts.autoRetry || ts.retryIntent
}

func (ts *txnState) resetStateAndTxn(state TxnStateEnum) {
	ts.State = state
	ts.txn = nil
}

// updateStateAndCleanupOnErr updates txnState based on the type of error that we
// received. If it's a retriable error and we're going to retry the txn,
// then the state moves to RestartWait. Otherwise, the state moves to Aborted
// and the KV txn is cleaned up.
func (ts *txnState) updateStateAndCleanupOnErr(pErr *roachpb.Error, e *Executor) {
	if pErr == nil {
		panic("updateStateAndCleanupOnErr called with no error")
	}
	if pErr.TransactionRestart == roachpb.TransactionRestart_NONE || !ts.willBeRetried() {
		// We can't or don't want to retry this txn, so the txn is over.
		e.txnAbortCount.Inc(1)
		ts.txn.CleanupOnError(pErr)
		ts.resetStateAndTxn(Aborted)
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		switch pErr.TransactionRestart {
		case roachpb.TransactionRestart_BACKOFF:
			// TODO(spencer): Get rid of BACKOFF retries. Note that we don't propagate
			// the backoff hint to the client anyway. See #5249
			fallthrough
		case roachpb.TransactionRestart_IMMEDIATE:
			ts.State = RestartWait
		default:
			panic(fmt.Sprintf("unexpected restart value: %s", pErr.TransactionRestart))
		}
	}
}

// ExecuteStatements executes the given statement(s) and returns a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) ExecuteStatements(
	ctx context.Context, user string, session *Session, stmts string,
	params []parser.Datum) StatementResults {

	planMaker := plannerPool.Get().(*planner)
	defer releasePlanner(planMaker)

	cfg, cache := e.getSystemConfig()
	*planMaker = planner{
		user: user,
		evalCtx: parser.EvalContext{
			NodeID:      e.nodeID,
			ReCache:     e.reCache,
			GetLocation: session.getLocation,
		},
		leaseMgr:      e.ctx.LeaseManager,
		systemConfig:  cfg,
		databaseCache: cache,
		session:       session,
		execCtx:       &e.ctx,
	}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	planMaker.params = parameters(params)
	res := e.execRequest(ctx, &session.TxnState, stmts, planMaker)
	return res
}

// execRequest executes the request using the provided planner.
// It parses the sql into statements, iterates through the statements, creates
// KV transactions and automatically retries them when possible, executes the
// (synchronous attempt of) schema changes.
// It will accumulate a result in Response for each statement.
// It will resume a SQL transaction, if one was previously open for this client.
//
// execRequest handles the mismatch between the SQL interface that the Executor
// provides, based on statements being streamed from the client in the context
// of a session, and the KV client.Txn interface, based on (possibly-retriable)
// callbacks passed to be executed in the context of a transaction. Actual
// execution of statements in the context of a KV txn is delegated to
// runTxnAttempt().
//
// Args:
//  txnState: State about about ongoing transaction (if any). The state will be
//   updated.
func (e *Executor) execRequest(
	ctx context.Context, txnState *txnState, sql string, planMaker *planner) StatementResults {
	var res StatementResults
	stmts, err := planMaker.parser.Parse(sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		pErr := roachpb.NewError(err)
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.txn != nil {
			// Rollback the txn.
			txnState.txn.CleanupOnError(pErr)
			txnState.resetStateAndTxn(Aborted)
		}
		res.ResultList = append(res.ResultList, Result{PErr: pErr})
		return res
	}
	if len(stmts) == 0 {
		res.Empty = true
		return res
	}

	if e.ctx.TestingKnobs.WaitForGossipUpdate {
		// We might need to verify metadata. Lock the system config so that no
		// gossip updates sneak in under us. The point is to be able to assert
		// that the verify callback only succeeds after a gossip update.
		//
		// This lock does not change semantics. Even outside of tests, the
		// planner is initialized with a static systemConfig, so locking
		// the Executor's systemConfig cannot change the semantics of the
		// SQL operation being performed under lock.
		//
		// The case of a multi-request transaction is not handled here,
		// because those transactions outlive the verification callback.
		// TODO(andrei): consider putting this callback on the Session, not
		// on the executor, after Session is not a proto any more. Also, #4646.
		e.systemConfigCond.L.Lock()
		defer func() {
			e.systemConfigCond.L.Unlock()
		}()
	}

	for len(stmts) > 0 {
		// Each iteration consumes a transaction's worth of statements.

		inTxn := txnState.State != NoTxn
		var execOpt client.TxnExecOptions
		// Figure out the statements out of which we're going to try to consume
		// this iteration. If we need to create an implicit txn, only one statement
		// can be consumed.
		stmtsToExec := stmts
		// We can AutoRetry the next batch of statements if we're in a clean state
		// (i.e. the next statements we're going to see are the first statements in
		// a transaction).
		if !inTxn {
			// Detect implicit transactions.
			if _, isBegin := stmts[0].(*parser.BeginTransaction); !isBegin {
				execOpt.AutoCommit = true
				stmtsToExec = stmtsToExec[0:1]
			}
			txnState.reset(ctx, e, planMaker.session)
			txnState.State = Open
			txnState.autoRetry = true
			execOpt.MinInitialTimestamp = e.ctx.Clock.Now()
			if execOpt.AutoCommit {
				txnState.txn.SetDebugName(sqlImplicitTxnName, 0)
			} else {
				txnState.txn.SetDebugName(sqlTxnName, 0)
			}
		} else {
			txnState.autoRetry = false
		}
		execOpt.AutoRetry = txnState.autoRetry
		if txnState.State == NoTxn {
			panic("we failed to initialize a txn")
		}
		// Now actually run some statements.
		var remainingStmts parser.StatementList
		var results []Result
		origState := txnState.State

		txnClosure := func(txn *client.Txn, opt *client.TxnExecOptions) *roachpb.Error {
			if txnState.State == Open && txnState.txn != txn {
				panic(fmt.Sprintf("closure wasn't called in the txn we set up for it."+
					"\ntxnState.txn:%+v\ntxn:%+v\ntxnState:%+v", txnState.txn, txn, txnState))
			}
			txnState.txn = txn
			return runTxnAttempt(e, planMaker, origState, txnState, opt, stmtsToExec,
				&results, &remainingStmts)
		}
		// This is where the magic happens - we ask db to run a KV txn and possibly retry it.
		txn := txnState.txn // this might be nil if the txn was already aborted.
		pErr := txnState.txn.Exec(execOpt, txnClosure)
		res.ResultList = append(res.ResultList, results...)
		// Now make sense of the state we got into and update txnState.
		if txnState.State == RestartWait && txnState.commitSeen {
			// A COMMIT got a retriable error. Too bad, this txn is toast. After we
			// return a result for COMMIT (with the COMMIT pgwire tag), the user can't
			// send any more commands.
			e.txnAbortCount.Inc(1)
			txn.CleanupOnError(pErr)
			txnState.resetStateAndTxn(NoTxn)
		}

		if execOpt.AutoCommit {
			// If execOpt.AutoCommit was set, then the txn no longer exists at this point.
			txnState.resetStateAndTxn(NoTxn)
		}
		// If the txn is in any state but Open, exec the schema changes. They'll
		// short-circuit themselves if the mutation that queued them has been
		// rolled back from the table descriptor.
		if txnState.State != Open {
			planMaker.releaseLeases()
			// Exec the schema changers (if the txn rolled back, the schema changers
			// will short-circuit because the corresponding descriptor mutation is not
			// found).
			txnState.schemaChangers.execSchemaChanges(e, planMaker, res.ResultList)
			stmtsExecuted := stmts[0 : len(stmtsToExec)-len(remainingStmts)]
			e.checkTestingWaitForGossipUpdateOrDie(planMaker, stmtsExecuted)
		}

		// Figure out what statements to run on the next iteration.
		if pErr != nil {
			// Don't execute anything further.
			stmts = nil
		} else if execOpt.AutoCommit {
			stmts = stmts[1:]
		} else {
			stmts = remainingStmts
		}
	}

	return res
}

func (e *Executor) checkTestingWaitForGossipUpdateOrDie(
	planMaker *planner, stmts parser.StatementList) {
	if e.ctx.TestingKnobs.WaitForGossipUpdate {
		if verify := planMaker.testingVerifyMetadata; verify != nil {
			// In the case of a multi-statement request, avoid reusing this
			// callback.
			planMaker.testingVerifyMetadata = nil
			first := true
			for verify(e.systemConfig) != nil {
				first = false
				e.systemConfigCond.Wait()
			}
			if first {
				panic(fmt.Sprintf(
					"expected %q to require a gossip update, but it did not", stmts))
			}
		}
	}
}

// If the plan is a returningNode we can just use the `rowCount`,
// otherwise we fall back to counting via plan.Next().
func countRowsAffected(p planNode) int {
	if a, ok := p.(*returningNode); ok {
		return a.rowCount
	}
	count := 0
	for p.Next() {
		count++
	}
	return count
}

// runTxnAttempt is the closure we pass to txn.Exec(). It will be called
// possibly multiple times (if opt.AutoRetry is set).
// It sets up a planner and delegates execution of statements to
// execStmtsInCurrentTxn().
func runTxnAttempt(
	e *Executor, planMaker *planner, origState TxnStateEnum, txnState *txnState,
	opt *client.TxnExecOptions, stmts parser.StatementList,
	// return values
	results *[]Result, remainingStmts *parser.StatementList) *roachpb.Error {

	// Ignore the state that might have been set by a previous try
	// of this closure.
	txnState.State = origState
	txnState.commitSeen = false

	*results = nil
	// (re)init the schemaChangers.
	// TODO(andrei): figure out how to persist schema changers across
	// different batches of statements in the same txn.
	txnState.schemaChangers = schemaChangerCollection{}
	planMaker.schemaChangeCallback = txnState.schemaChangers.queueSchemaChanger

	planMaker.setTxn(txnState.txn)
	var pErr *roachpb.Error
	*results, *remainingStmts, pErr = e.execStmtsInCurrentTxn(
		stmts, planMaker, txnState,
		opt.AutoCommit /* implicitTxn */, opt.AutoRetry /* txnBeginning */)
	if opt.AutoCommit && len(*remainingStmts) > 0 {
		panic("implicit txn failed to execute all stmts")
	}
	planMaker.resetTxn()
	return pErr
}

// execStmtsInCurrentTxn consumes a prefix of stmts, namely the
// statements belonging to a single SQL transaction. It executes in
// the planner's transaction, which is assumed to exist.
//
// COMMIT/ROLLBACK statements can end the current transaction. If that happens,
// this method returns, and the remaining statements are returned.
//
// If an error occurs while executing a statement, the SQL txn will be
// considered aborted and subsequent statements will be discarded (they will
// not be executed, they will not be returned for future execution, they will
// not generate results). Note that this also includes COMMIT/ROLLBACK
// statements. Further note that SqlTransactionAbortedError is no exception -
// encountering it will discard subsequent statements. This means that, to
// recover from an aborted txn, a COMMIT/ROLLBACK statement needs to be the
// first one in stmts.
//
// Args:
//  txnState: Specifies whether we're executing inside a txn, or inside an aborted txn.
//    The state is updated.
//  implicitTxn: set if the current transaction was implicitly
//    created by the system (i.e. the client sent the statement outside of
//    a transaction).
// Returns:
//  - the list of results (one per executed statement).
//  - the statements that haven't been executed because the transaction has
//    been committed or rolled back. In returning an error, this will be nil.
//  - the error encountered while executing statements, if any. If an error
//    occurred, it is also the last result returned. Subsequent statements
//    have not been executed.
func (e *Executor) execStmtsInCurrentTxn(
	stmts parser.StatementList, planMaker *planner,
	txnState *txnState,
	implicitTxn bool, txnBeginning bool) (
	[]Result, parser.StatementList, *roachpb.Error) {
	var results []Result
	if txnState.State == NoTxn {
		panic("execStmtsInCurrentTransaction called outside of a txn")
	}
	if txnState.State == Open && planMaker.txn == nil {
		panic(fmt.Sprintf("inconsistent planMaker txn state. txnState: %+v", txnState))
	}

	for i, stmt := range stmts {
		if log.V(2) {
			log.Infof("about to execute sql statement (%d/%d): %s", i+1, len(stmts), stmt)
		}
		txnState.schemaChangers.curStatementIdx = i

		stmtTimestamp := e.ctx.Clock.Now()

		var stmtStrBefore string
		if e.ctx.TestingKnobs.CheckStmtStringChange {
			stmtStrBefore = stmt.String()
		}
		var res Result
		var pErr *roachpb.Error
		switch txnState.State {
		case Open:
			res, pErr = e.execStmtInOpenTxn(
				stmt, planMaker, implicitTxn, txnBeginning && (i == 0), /* firstInTxn */
				stmtTimestamp, txnState)
		case Aborted, RestartWait:
			res, pErr = e.execStmtInAbortedTxn(stmt, txnState)
		case CommitWait:
			res, pErr = e.execStmtInCommitWaitTxn(stmt, txnState)
		default:
			panic(fmt.Sprintf("unexpected txn state: %s", txnState.State))
		}
		if e.ctx.TestingKnobs.CheckStmtStringChange {
			after := stmt.String()
			if after != stmtStrBefore {
				panic(fmt.Sprintf("statement changed after exec; before:\n    %s\nafter:\n    %s",
					stmtStrBefore, after))
			}
		}
		results = append(results, res)
		if pErr != nil {
			// After an error happened, skip executing all the remaining statements
			// in this batch.  This is Postgres behavior, and it makes sense as the
			// protocol doesn't let you return results after an error.
			return results, nil, pErr
		}
		if txnState.State == NoTxn {
			// If the transaction is done, return the remaining statements to
			// be executed as a different group.
			return results, stmts[i+1:], nil
		}
	}
	// If we got here, we've managed to consume all statements and we're still in a txn.
	return results, nil, nil
}

// execStmtInAbortedTxn executes a statement in a txn that's in state
// Aborted or RestartWait.
// Everything but COMMIT/ROLLBACK/RESTART causes errors.
func (e *Executor) execStmtInAbortedTxn(
	stmt parser.Statement, txnState *txnState) (Result, *roachpb.Error) {
	if txnState.State != Aborted && txnState.State != RestartWait {
		panic("execStmtInAbortedTxn called outside of an aborted txn")
	}
	switch stmt.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		if txnState.State == RestartWait {
			if pErr := txnState.txn.Rollback(); pErr != nil {
				log.Errorf("failure rolling back transaction: %s", pErr)
			}
		}
		// Reset the state to allow new transactions to start.
		// Note: postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
		txnState.resetStateAndTxn(NoTxn)
		return result, nil
	case *parser.RestartTransaction:
		if txnState.State == RestartWait {
			// Reset the state. Txn is Open again.
			txnState.State = Open
			// TODO(andrei/cdo): add a counter for user-directed retries.
			return Result{}, nil
		}
		pErr := roachpb.NewError(&roachpb.SqlTransactionAbortedError{
			CustomMsg: "RETRY INTENT has not been used or a non-retriable error was encountered."})
		return Result{PErr: pErr}, pErr
	default:
		pErr := roachpb.NewError(&roachpb.SqlTransactionAbortedError{})
		return Result{PErr: pErr}, pErr
	}
}

// execStmtInCommitWaitTxn executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (e *Executor) execStmtInCommitWaitTxn(
	stmt parser.Statement, txnState *txnState) (Result, *roachpb.Error) {
	if txnState.State != CommitWait {
		panic("execStmtInCommitWaitTxn called outside of an aborted txn")
	}
	switch stmt.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		// Reset the state to allow new transactions to start.
		result := Result{PGTag: (*parser.CommitTransaction)(nil).StatementTag()}
		txnState.resetStateAndTxn(NoTxn)
		return result, nil
	default:
		pErr := roachpb.NewError(&roachpb.SqlTransactionCommittedError{})
		return Result{PErr: pErr}, pErr
	}
}

// execStmtInOpenTxn executes one statement in the context
// of the planner's transaction (which is assumed to exist).
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// and delegates everything else to `execStmt`.
// It binds placeholders.
//
// The current transaction might be committed/rolled back when this returns.
// It might also have transitioned to the aborted or RestartWait state.
//
// Args:
// implicitTxn: set if the current transaction was implicitly
//  created by the system (i.e. the client sent the statement outside of
//  a transaction).
//  COMMIT/ROLLBACK statements are rejected if set. Also, the transaction
//  might be auto-committed in this function.
// firstInTxn: set for the first statement in a transaction. Used
//  so that nested BEGIN statements are caught.
// stmtTimestamp: Used as the statement_timestamp().
//
// Returns:
// - a Result
// - an error, if any. In case of error, the result returned also reflects this error.
func (e *Executor) execStmtInOpenTxn(
	stmt parser.Statement, planMaker *planner,
	implicitTxn bool,
	firstInTxn bool,
	stmtTimestamp roachpb.Timestamp,
	txnState *txnState) (Result, *roachpb.Error) {
	if txnState.State != Open {
		panic("execStmtInOpenTxn called outside of an open txn")
	}
	if planMaker.txn == nil {
		panic("execStmtInOpenTxn called with the a txn not set on the planner")
	}

	planMaker.evalCtx.SetStmtTimestamp(stmtTimestamp)

	// TODO(cdo): Figure out how to not double count on retries.
	e.updateStmtCounts(stmt)
	switch stmt.(type) {
	case *parser.BeginTransaction:
		if !firstInTxn {
			txnState.resetStateAndTxn(Aborted)
			pErr := roachpb.NewError(errTransactionInProgress)
			return Result{PErr: pErr}, pErr
		}
	case *parser.CommitTransaction:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		// CommitTransaction is executed fully here; there's no planNode for it
		// and the planner is not involved at all.
		return commitSQLTransaction(txnState, planMaker, commit, e)
	case *parser.ReleaseTransaction:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		// ReleaseTransaction is executed fully here; there's no planNode for it
		// and the planner is not involved at all.
		return commitSQLTransaction(txnState, planMaker, release, e)
	case *parser.RollbackTransaction:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		// RollbackTransaction is executed fully here; there's no planNode for it
		// and the planner is not involved at all.
		// Notice that we don't return any errors on rollback.
		return rollbackSQLTransaction(txnState, planMaker), nil
	case *parser.SetTransaction:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
	case *parser.RetryIntent:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		// We check if the transaction has "started" already by looking inside the txn proto.
		// The executor should not be doing that. But it's also what the planner does for
		// SET TRANSACTION ISOLATION ... It feels ever more wrong here.
		// TODO(andrei): find a better way to track this running state.
		if txnState.txn.Proto.IsInitialized() {
			pErr := roachpb.NewError(
				util.Errorf("RETRY INTENT needs to be the first statement in a transaction"))
			return Result{PErr: pErr}, pErr
		}
		// Note that RetryIntent doesn't have a corresponding plan node.
		// This here is all the execution there is.
		txnState.retryIntent = true
		return Result{}, nil
	case *parser.RestartTransaction:
		// Can't restart if we didn't get an error first, which would've put the
		// txn in a different state.
		txnState.resetStateAndTxn(Aborted)
		pErr := roachpb.NewError(errNotRetriable)
		return Result{PErr: pErr}, pErr
	}

	// Bind all the placeholder variables in the stmt to actual values.
	stmt, err := parser.FillArgs(stmt, &planMaker.params)
	if err != nil {
		txnState.resetStateAndTxn(Aborted)
		pErr := roachpb.NewError(err)
		return Result{PErr: pErr}, pErr
	}

	if txnState.tr != nil {
		txnState.tr.LazyLog(stmt, true /* sensitive */)
	}
	result, pErr := e.execStmt(stmt, planMaker, timeutil.Now(),
		implicitTxn /* autoCommit */)
	if pErr != nil {
		if txnState.tr != nil {
			txnState.tr.LazyPrintf("ERROR: %v", pErr)
		}
		txnState.updateStateAndCleanupOnErr(pErr, e)
		result = Result{PErr: pErr}
	} else if txnState.tr != nil {
		tResult := &traceResult{tag: result.PGTag, count: -1}
		switch result.Type {
		case parser.RowsAffected:
			tResult.count = result.RowsAffected
		case parser.Rows:
			tResult.count = len(result.Rows)
		}
		txnState.tr.LazyLog(tResult, false)
	}
	return result, pErr
}

func (e *Executor) noTransactionHelper(txnState *txnState) (Result, *roachpb.Error) {
	txnState.resetStateAndTxn(Aborted)
	pErr := roachpb.NewError(errNoTransactionInProgress)
	return Result{PErr: pErr}, pErr
}

// rollbackSQLTransaction rolls back a transaction. All errors are swallowed.
func rollbackSQLTransaction(txnState *txnState, p *planner) Result {
	if p.txn != txnState.txn {
		panic("rollbackSQLTransaction called on a different txn than the planner's")
	}
	if txnState.State != Open {
		panic(fmt.Sprintf("rollbackSQLTransaction called on non-open txn: %+v", txnState.txn))
	}
	pErr := p.txn.Rollback()
	result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
	if pErr != nil {
		log.Warningf("txn rollback failed. The error was swallowed: %s", pErr)
		result.PErr = pErr
	}
	// We're done with this txn.
	txnState.resetStateAndTxn(NoTxn)
	// Reset transaction to prevent running further commands on this planner.
	p.resetTxn()
	return result
}

type commitType int

const (
	commit commitType = iota
	release
)

// commitSqlTransaction commits a transaction.
func commitSQLTransaction(txnState *txnState, p *planner,
	commitType commitType, e *Executor) (Result, *roachpb.Error) {

	if p.txn != txnState.txn {
		panic("commitSQLTransaction called on a different txn than the planner's")
	}
	if txnState.State != Open {
		panic(fmt.Sprintf("commitSqlTransaction called on non-open txn: %+v", txnState.txn))
	}
	if commitType == commit {
		txnState.commitSeen = true
	}
	pErr := txnState.txn.CommitNoCleanup()
	result := Result{PGTag: (*parser.CommitTransaction)(nil).StatementTag()}
	if pErr != nil {
		txnState.updateStateAndCleanupOnErr(pErr, e)
		result.PErr = pErr
	} else {
		switch commitType {
		case release:
			// We'll now be waiting for a COMMIT.
			txnState.State = CommitWait
		case commit:
			// We're done with this txn.
			txnState.State = NoTxn
		}
		txnState.txn = nil
	}
	// Reset transaction to prevent running further commands on this planner.
	p.resetTxn()
	return result, pErr
}

// the current transaction might have been committed/rolled back when this returns.
func (e *Executor) execStmt(
	stmt parser.Statement, planMaker *planner,
	timestamp time.Time, autoCommit bool) (Result, *roachpb.Error) {
	var result Result
	plan, pErr := planMaker.makePlan(stmt, autoCommit)
	if pErr != nil {
		return result, pErr
	}

	result.PGTag = stmt.StatementTag()
	result.Type = stmt.StatementType()

	switch result.Type {
	case parser.RowsAffected:
		result.RowsAffected += countRowsAffected(plan)

	case parser.Rows:
		result.Columns = plan.Columns()
		for _, c := range result.Columns {
			if err := checkResultDatum(c.Typ); err != nil {
				return result, roachpb.NewError(err)
			}
		}

		for plan.Next() {
			// The plan.Values DTuple needs to be copied on each iteration.
			values := plan.Values()
			row := ResultRow{Values: make([]parser.Datum, 0, len(values))}
			for _, val := range values {
				if err := checkResultDatum(val); err != nil {
					return result, roachpb.NewError(err)
				}
				row.Values = append(row.Values, val)
			}
			result.Rows = append(result.Rows, row)
		}
	}
	return result, plan.PErr()
}

// updateStmtCounts updates metrics for the number of times the different types of SQL
// statements have been received by this node.
func (e *Executor) updateStmtCounts(stmt parser.Statement) {
	e.queryCount.Inc(1)
	switch stmt.(type) {
	case *parser.BeginTransaction:
		e.txnBeginCount.Inc(1)
	case *parser.Select:
		e.selectCount.Inc(1)
	case *parser.Update:
		e.updateCount.Inc(1)
	case *parser.Insert:
		e.insertCount.Inc(1)
	case *parser.Delete:
		e.deleteCount.Inc(1)
	case *parser.CommitTransaction:
		e.txnCommitCount.Inc(1)
	case *parser.RollbackTransaction:
		e.txnRollbackCount.Inc(1)
	default:
		if stmt.StatementType() == parser.DDL {
			e.ddlCount.Inc(1)
		} else {
			e.miscCount.Inc(1)
		}
	}
}

// Registry returns a registry with the metrics tracked by this executor, which can be used to
// access its stats or be added to another registry.
func (e *Executor) Registry() *metric.Registry {
	return e.registry
}

var _ parser.Args = parameters{}

type parameters []parser.Datum

var errNamedArgument = errors.New("named arguments are not supported")

// processPositionalArgument is a helper function that processes a positional
// integer argument passed to an implementation of parser.Args. Currently, only
// positional arguments (non-negative integers) are supported, but named arguments may
// be supported in the future.
func processPositionalArgument(name string) (int64, error) {
	if len(name) == 0 {
		// This shouldn't happen unless the parser let through an invalid parameter
		// specification.
		panic(fmt.Sprintf("invalid empty parameter name"))
	}
	if ch := name[0]; ch < '0' || ch > '9' {
		// TODO(pmattis): Add support for named parameters (vs the numbered
		// parameter support below).
		return 0, errNamedArgument
	}
	return strconv.ParseInt(name, 10, 0)
}

// Arg implements the parser.Args interface.
func (p parameters) Arg(name string) (parser.Datum, bool) {
	i, err := processPositionalArgument(name)
	if err != nil {
		return nil, false
	}
	if i < 1 || int(i) > len(p) {
		return nil, false
	}
	return p[i-1], true
}

var _ parser.Args = golangParameters{}

type golangParameters []interface{}

// Arg implements the parser.Args interface.
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func (gp golangParameters) Arg(name string) (parser.Datum, bool) {
	i, err := processPositionalArgument(name)
	if err != nil {
		return nil, false
	}
	if i < 1 || int(i) > len(gp) {
		return nil, false
	}
	arg := gp[i-1]
	if arg == nil {
		return parser.DNull, true
	}

	// A type switch to handle a few explicit types with special semantics.
	switch t := arg.(type) {
	// Datums are passed along as is.
	case parser.Datum:
		return t, true
	// Time datatypes get special representation in the database.
	case time.Time:
		return parser.DTimestamp{Time: t}, true
	case time.Duration:
		return parser.DInterval{Duration: t}, true
	case *inf.Dec:
		dd := &parser.DDecimal{}
		dd.Set(t)
		return dd, true
	}

	// Handle all types which have an underlying type that can be stored in the
	// database.
	// Note: if this reflection becomes a performance concern in the future,
	// commonly used types could be added explicitly into the type switch above
	// for a performance gain.
	val := reflect.ValueOf(arg)
	switch val.Kind() {
	case reflect.Bool:
		return parser.DBool(val.Bool()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return parser.DInt(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return parser.DInt(val.Uint()), true
	case reflect.Float32, reflect.Float64:
		return parser.DFloat(val.Float()), true
	case reflect.String:
		return parser.DString(val.String()), true
	case reflect.Slice:
		// Handle byte slices.
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return parser.DBytes(val.Bytes()), true
		}
	}

	panic(fmt.Sprintf("unexpected type %T", arg))
}

func checkResultDatum(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}

	switch datum.(type) {
	case parser.DBool:
	case parser.DInt:
	case parser.DFloat:
	case *parser.DDecimal:
	case parser.DBytes:
	case parser.DString:
	case parser.DDate:
	case parser.DTimestamp:
	case parser.DInterval:
	case parser.DValArg:
		return fmt.Errorf("could not determine data type of %s %s", datum.Type(), datum)
	default:
		return util.Errorf("unsupported result type: %s", datum.Type())
	}
	return nil
}
