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

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

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

// Request is an SQL request to cockroach. A transaction can consist of multiple
// requests.
type Request struct {
	// User is the originating user.
	User string
	// Session settings that were returned in the last response that
	// contained them, being reflected back to the server.
	// This Session will be updated as part of executing this request.
	Session *Session
	// SQL statement(s) to be serially executed by the server. Multiple
	// statements are passed as a single string separated by semicolons.
	SQL string
	// Parameters referred to in the above SQL statement(s) using "?".
	Params []parser.Datum
}

// Response is the reply to an SQL request to cockroach.
type Response struct {
	// Settings that should be reflected back in all subsequent requests.
	// Immutable.
	Session *Session
	Results StatementResults
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

	TestingMocker ExecutorTestingMocker
}

// ExecutorTestingMocker is a part of the context used to control parts of the system.
type ExecutorTestingMocker struct {
	// WaitForGossipUpdate causes metadata-mutating operations to wait
	// for the new metadata to back-propagate through gossip.
	WaitForGossipUpdate bool

	// CheckStmtStringChange causes Executor.execStmtsInCurrentTxn to verify
	// that executed statements are not modified during execution.
	CheckStmtStringChange bool
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

// initializeTxn initialize a Txn using the session defaults.
func (e *Executor) initializeTxn(txn *client.Txn, s *Session) {
	txn.Proto.Isolation = s.DefaultIsolationLevel
	if s.TransactionTimeout != nil {
		txn.Proto.HeartbeatInterval = s.TransactionTimeout
	}
}

// newTxn creates a new Txn and initializes it using the session defaults.
func (e *Executor) newTxn(s *Session) *client.Txn {
	txn := client.NewTxn(*e.ctx.DB)
	e.initializeTxn(txn, s)
	return txn
}

// Prepare returns the result types of the given statement. Args may be a
// partially populated val args map. Prepare will populate the missing val
// args. The column result types are returned (or nil if there are no results).
func (e *Executor) Prepare(user string, query string, session *Session, args parser.MapArgs) (
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
		},
		leaseMgr:      e.ctx.LeaseManager,
		systemConfig:  cfg,
		databaseCache: cache,
		session:       session,
	}

	timestamp := time.Now()
	txn := e.newTxn(session)
	planMaker.setTxn(txn, timestamp)
	planMaker.evalCtx.StmtTimestamp = parser.DTimestamp{Time: timestamp}
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

// txnState contains state associated with an ongoing SQL txn.
// There may or may not be an open KV txn associated with the SQL txn.
// For interactive transactions (open across batches of SQL commands sent by a
// user, txnState is intended to be serialized/deserialized as part of a user
// Session.
// The state is as follows:
// - if aborted is set, the sql txn is in a mode where it rejects every
// statement until a COMMIT/ROLLBACK, after which there will be no more
// transaction to speak of.
// In the future there might be a difference between txn being nil or not,
// signaling whether the user can retry the transaction.
// - if aborted is not set and txn is nil, it means that the SQL transaction is
// done (COMMIT/ROLLBACK).
// - if aborted is not set and txn != nil, the SQL transaction is open, as is
// the KV txn.
type txnState struct {
	txn *client.Txn
	// true if we were inside a txn at some point and that txn was aborted. We
	// need to reject every subsequent statement.
	aborted bool
	// Timestamp to be used by SQL (transaction_timestamp()) in the above
	// transaction.
	txnTimestamp time.Time
	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection
}

type transactionState int

const (
	noTransaction transactionState = iota
	openTransaction
	abortedTransaction // waiting for COMMIT/ROLLBACK
)

func (s *txnState) state() transactionState {
	if s.aborted {
		return abortedTransaction
	}
	if s.txn == nil {
		return noTransaction
	}
	return openTransaction
}

// ExecuteStatements executes the given statement(s) and returns a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) ExecuteStatements(
	user string, session *Session, stmts string,
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
	}

	// Move the transaction state from the session to curTxnState, a struct
	// that only lives for the duration of this request.
	// If a pending transaction is present, it will be resumed.
	curTxnState := txnState{
		txn:          nil,
		aborted:      false,
		txnTimestamp: time.Time{},
	}
	txnProto := session.Txn.Txn
	curTxnState.aborted = session.Txn.TxnAborted
	if txnProto != nil {
		curTxnState.txn = e.newTxn(session)
		curTxnState.txn.Proto = *txnProto
		curTxnState.txn.UserPriority = session.Txn.UserPriority
		if session.Txn.MutatesSystemConfig {
			curTxnState.txn.SetSystemConfigTrigger()
		}
		curTxnState.txnTimestamp = session.Txn.TxnTimestamp.GoTime()
	}
	session.Txn = Session_Transaction{}

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	planMaker.params = parameters(params)
	res := e.execRequest(&curTxnState, stmts, planMaker)

	// Send back the session state even if there were application-level errors.
	// Add transaction to session state.
	if curTxnState.txn != nil {
		// TODO(pmattis): Need to associate the leases used by a transaction with
		// the session state.
		planMaker.releaseLeases()
		session.Txn = Session_Transaction{
			Txn:          &curTxnState.txn.Proto,
			TxnTimestamp: Timestamp(curTxnState.txnTimestamp),
			UserPriority: curTxnState.txn.UserPriority,
		}
		session.Txn.MutatesSystemConfig = curTxnState.txn.SystemConfigTrigger()
		session.Txn.TxnAborted = curTxnState.aborted
	} else {
		session.Txn.Txn = nil
		session.Txn.TxnAborted = curTxnState.aborted
		session.Txn.MutatesSystemConfig = false
	}

	return res
}

// Execute the statement(s) in the given request and returns a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) Execute(args Request) (Response, int, error) {
	defer func(start time.Time) {
		e.latency.RecordValue(time.Now().Sub(start).Nanoseconds())
	}(time.Now())
	results := e.ExecuteStatements(
		args.User, args.Session, args.SQL, args.Params)
	return Response{Results: results, Session: args.Session}, 0, nil
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
	txnState *txnState, sql string, planMaker *planner) StatementResults {
	var res StatementResults
	stmts, err := planMaker.parser.Parse(sql, parser.Syntax(planMaker.session.Syntax))
	if err != nil {
		pErr := roachpb.NewError(err)
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.txn != nil {
			// Rollback the txn.
			txnState.txn.Cleanup(pErr)
			txnState.aborted = true
			txnState.txn = nil
		}
		res.ResultList = append(res.ResultList, Result{PErr: pErr})
		return res
	}
	if len(stmts) == 0 {
		res.Empty = true
		return res
	}

	if e.ctx.TestingMocker.WaitForGossipUpdate {
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

		inTxn := txnState.state() != noTransaction
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
			txnState.txn = e.newTxn(planMaker.session)
			execOpt.AutoRetry = true
			txnState.txnTimestamp = time.Now()
			txnState.txn.SetDebugName(fmt.Sprintf("sql implicit: %t", execOpt.AutoCommit), 0)
		}
		if txnState.state() == noTransaction {
			panic("we failed to initialize a txn")
		}
		// Now actually run some statements.
		var remainingStmts parser.StatementList
		var results []Result
		origAborted := txnState.state() == abortedTransaction

		txnClosure := func(txn *client.Txn, opt *client.TxnExecOptions) *roachpb.Error {
			return runTxnAttempt(e, planMaker, origAborted, txnState, txn, opt, stmtsToExec,
				&results, &remainingStmts)
		}
		// This is where the magic happens - we ask db to run a KV txn and possibly retry it.
		pErr := txnState.txn.Exec(execOpt, txnClosure)
		res.ResultList = append(res.ResultList, results...)
		// Now make sense of the state we got into and update txnState.
		if pErr != nil {
			// If we got an error, the txn has been aborted (or it might be already
			// done if the error was encountered when executing the COMMIT/ROLLBACK.
			// There's nothing we can use it for any more.
			// TODO(andrei): once txn.Exec() doesn't abort retriable txns any more,
			// we need to be more nuanced here.
			txnState.txn = nil
			e.txnAbortCount.Inc(1)
		}
		if execOpt.AutoCommit {
			// If execOpt.AutoCommit was set, then the txn no longer exists at this point.
			txnState.txn = nil
			txnState.aborted = false
		}
		// If the txn is in a final state (committed, rolled back or aborted), exec
		// the schema changes.
		if txnState.state() != openTransaction {
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
	if e.ctx.TestingMocker.WaitForGossipUpdate {
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
	e *Executor, planMaker *planner, origAborted bool, txnState *txnState,
	txn *client.Txn, opt *client.TxnExecOptions,
	stmts parser.StatementList,
	// return values
	results *[]Result, remainingStmts *parser.StatementList) *roachpb.Error {

	if txnState.txn != txn {
		panic("runTxnAttempt wasn't called in the txn we set up for it")
	}
	// Ignore the abort status that might have been set by a previous try
	// of this closure.
	txnState.aborted = origAborted
	*results = nil
	// (re)init the schemaChangers.
	// TODO(andrei): figure out how to persist schema changers across
	// different batches of statements in the same txn.
	txnState.schemaChangers = schemaChangerCollection{}
	planMaker.schemaChangeCallback = txnState.schemaChangers.queueSchemaChanger

	planMaker.setTxn(txn, txnState.txnTimestamp)
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

// execStmtsInCurrentTransaction consumes a prefix of stmts, namely the
// statements belonging to a single SQL transaction. It executes in the
// planner's transaction, which is assumed to exist.
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
	if planMaker.txn == nil && txnState.state() != abortedTransaction {
		panic("execStmtsInCurrentTransaction called outside of a txn")
	}
	for i, stmt := range stmts {
		if log.V(2) {
			log.Infof("about to execute sql statement (%d/%d): %s", i+1, len(stmts), stmt)
		}
		txnState.schemaChangers.curStatementIdx = i
		// For implicit transactions, the transaction timestamp is also
		// used as the statement_transaction() too.
		stmtTimestamp := planMaker.evalCtx.TxnTimestamp
		if !implicitTxn {
			stmtTimestamp = parser.DTimestamp{Time: time.Now()}
		}
		var stmtStrBefore string
		if e.ctx.TestingMocker.CheckStmtStringChange {
			stmtStrBefore = stmt.String()
		}
		var res Result
		var pErr *roachpb.Error
		if txnState.state() == abortedTransaction {
			res, pErr = e.execStmtInAbortedTxn(stmt, txnState)
		} else {
			res, pErr = e.execStmtInOpenTxn(
				stmt, planMaker, implicitTxn, txnBeginning && (i == 0), /* firstInTxn */
				stmtTimestamp, txnState)
		}
		if e.ctx.TestingMocker.CheckStmtStringChange {
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
		if txnState.state() == noTransaction {
			// If the transaction is done, return the remaining statements to
			// be executed as a different group.
			return results, stmts[i+1:], nil
		}
	}
	// If we got here, we've managed to consume all statements and we're still in a txn.
	return results, nil, nil
}

// execStmtInAbortedMode executes a statement in a txn that's in aborted mode.
// Everything but COMMIT/ROLLBACK causes errors.
func (e *Executor) execStmtInAbortedTxn(
	stmt parser.Statement, txnState *txnState) (Result, *roachpb.Error) {
	if txnState.state() != abortedTransaction {
		panic("execStmtInAbortedTxn called outside of an aborted txn")
	}
	switch stmt.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		// Reset the state to allow new transactions to start.
		// Note: postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
		txnState.aborted = false
		txnState.txn = nil
		return result, nil
	default:
		pErr := roachpb.NewError(&roachpb.SqlTransactionAbortedError{})
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
//
// Args:
// abortedMode: if set, we're in a transaction that has encountered errors, so we
//  must reject the statement unless it's a COMMIT/ROLLBACK.
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
	stmtTimestamp parser.DTimestamp,
	txnState *txnState) (Result, *roachpb.Error) {
	if txnState.state() != openTransaction {
		panic("execStmtInOpenTxn called outside of an open txn")
	}
	if planMaker.txn == nil {
		panic("execStmtInOpenTxn called with the a txn not set on the planner")
	}

	planMaker.evalCtx.StmtTimestamp = stmtTimestamp

	// TODO(cdo): Figure out how to not double count on retries.
	e.updateStmtCounts(stmt)
	switch stmt.(type) {
	case *parser.BeginTransaction:
		if !firstInTxn {
			txnState.aborted = true
			pErr := roachpb.NewError(errTransactionInProgress)
			return Result{PErr: pErr}, pErr
		}
	case *parser.CommitTransaction, *parser.RollbackTransaction, *parser.SetTransaction:
		if implicitTxn {
			txnState.aborted = true
			pErr := roachpb.NewError(errNoTransactionInProgress)
			return Result{PErr: pErr}, pErr
		}
	}

	// Bind all the placeholder variables in the stmt to actual values.
	stmt, err := parser.FillArgs(stmt, &planMaker.params)
	if err != nil {
		txnState.aborted = true
		pErr := roachpb.NewError(err)
		return Result{PErr: pErr}, pErr
	}

	result, pErr := e.execStmt(stmt, planMaker, time.Now(),
		implicitTxn /* autoCommit */)
	txnDone := planMaker.txn == nil
	if pErr != nil {
		result = Result{PErr: pErr}
		txnState.aborted = true
	}
	if txnDone {
		txnState.aborted = false
		txnState.txn = nil
	}
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
