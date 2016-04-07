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
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
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
	defer e.systemConfigMu.Unlock()
	e.systemConfig = cfg
	// The database cache gets reset whenever the system config changes.
	e.databaseCache = &databaseCache{
		databases: map[string]ID{},
	}
	e.systemConfigCond.Broadcast()
}

// getSystemConfig returns a copy of the latest system config.
func (e *Executor) getSystemConfig() (config.SystemConfig, *databaseCache) {
	e.systemConfigMu.RLock()
	defer e.systemConfigMu.RUnlock()
	cfg, cache := e.systemConfig, e.databaseCache
	return cfg, cache
}

// Prepare returns the result types of the given statement. Args may be a
// partially populated val args map. Prepare will populate the missing val
// args. The column result types are returned (or nil if there are no results).
func (e *Executor) Prepare(ctx context.Context, query string, session *Session, args parser.MapArgs) (
	[]ResultColumn, *roachpb.Error) {
	stmt, err := parser.ParseOne(query, parser.Syntax(session.Syntax))
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	session.planner.resetForBatch(e)
	session.planner.evalCtx.Args = args
	session.planner.evalCtx.PrepareOnly = true

	// TODO(andrei): does the prepare phase really need a Txn?
	txn := client.NewTxn(ctx, *e.ctx.DB)
	txn.Proto.Isolation = session.DefaultIsolationLevel
	session.planner.setTxn(txn)
	defer session.planner.setTxn(nil)

	plan, pErr := session.planner.prepare(stmt)
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

// ExecuteStatements executes the given statement(s) and returns a response.
func (e *Executor) ExecuteStatements(
	ctx context.Context, session *Session, stmts string,
	params []parser.Datum) StatementResults {

	session.planner.resetForBatch(e)
	session.planner.params = parameters(params)

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	return e.execRequest(ctx, session, stmts)
}

// blockConfigUpdates blocks any gossip updates to the system config
// until the unlock function returned is called. Useful in tests.
func (e *Executor) blockConfigUpdates() func() {
	e.systemConfigCond.L.Lock()
	return func() {
		e.systemConfigCond.L.Unlock()
	}
}

// waitForConfigUpdate blocks the caller until a new SystemConfig is received
// via gossip. This can only be called after blockConfigUpdates().
func (e *Executor) waitForConfigUpdate() {
	e.systemConfigCond.Wait()
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
func (e *Executor) execRequest(ctx context.Context, session *Session, sql string) StatementResults {
	var res StatementResults
	txnState := &session.TxnState
	planMaker := &session.planner
	stmts, err := planMaker.parser.Parse(sql, parser.Syntax(session.Syntax))
	if err != nil {
		pErr := roachpb.NewError(err)
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.txn != nil {
			// Rollback the txn.
			txnState.updateStateAndCleanupOnErr(pErr, e)
		}
		res.ResultList = append(res.ResultList, Result{PErr: pErr})
		return res
	}
	if len(stmts) == 0 {
		res.Empty = true
		return res
	}

	// If the planMaker wants config updates to be blocked, then block them.
	defer planMaker.blockConfigUpdatesMaybe(e)()

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
				stmtsToExec = stmtsToExec[:1]
			}
			txnState.reset(ctx, e, session)
			txnState.State = Open
			txnState.autoRetry = true
			execOpt.MinInitialTimestamp = e.ctx.Clock.Now()
			txnState.sqlTimestamp = e.ctx.Clock.PhysicalTime()
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

			var pErr *roachpb.Error
			results, remainingStmts, pErr = runTxnAttempt(e, planMaker, origState, txnState, opt, stmtsToExec)
			return pErr
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
		stmtsExecuted := stmts[:len(stmtsToExec)-len(remainingStmts)]
		if txnState.State != Open {
			planMaker.releaseLeases()
			// Exec the schema changers (if the txn rolled back, the schema changers
			// will short-circuit because the corresponding descriptor mutation is not
			// found).
			txnState.schemaChangers.execSchemaChanges(e, planMaker, res.ResultList)
			planMaker.checkTestingVerifyMetadataInitialOrDie(e, stmts)
			planMaker.checkTestingVerifyMetadataOrDie(e, stmtsExecuted)
		} else {
			// We're still in a txn, so we only check that the verifyMetadata callback
			// fails the first time it's run. The gossip update that will make the
			// callback succeed only happens when the txn is done.
			planMaker.checkTestingVerifyMetadataInitialOrDie(e, stmtsExecuted)
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

// runTxnAttempt is used in the closure we pass to txn.Exec(). It
// will be called possibly multiple times (if opt.AutoRetry is set).
// It sets up a planner and delegates execution of statements to
// execStmtsInCurrentTxn().
func runTxnAttempt(
	e *Executor,
	planMaker *planner,
	origState TxnStateEnum,
	txnState *txnState,
	opt *client.TxnExecOptions,
	stmts parser.StatementList,
) ([]Result, parser.StatementList, *roachpb.Error) {

	// Ignore the state that might have been set by a previous try
	// of this closure.
	txnState.State = origState
	txnState.commitSeen = false

	planMaker.setTxn(txnState.txn)
	results, remainingStmts, pErr := e.execStmtsInCurrentTxn(
		stmts, planMaker, txnState,
		opt.AutoCommit /* implicitTxn */, opt.AutoRetry /* txnBeginning */)
	if opt.AutoCommit && len(remainingStmts) > 0 {
		panic("implicit txn failed to execute all stmts")
	}
	planMaker.resetTxn()
	return results, remainingStmts, pErr
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
// statements. Further note that errTransactionAborted is no exception -
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
	stmts parser.StatementList,
	planMaker *planner,
	txnState *txnState,
	implicitTxn bool,
	txnBeginning bool,
) ([]Result, parser.StatementList, *roachpb.Error) {
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
				txnState)
		case Aborted, RestartWait:
			res, pErr = e.execStmtInAbortedTxn(stmt, txnState, planMaker)
		case CommitWait:
			res, pErr = e.execStmtInCommitWaitTxn(stmt, txnState)
		default:
			panic(fmt.Sprintf("unexpected txn state: %s", txnState.State))
		}
		if e.ctx.TestingKnobs.CheckStmtStringChange {
			if after := stmt.String(); after != stmtStrBefore {
				panic(fmt.Sprintf("statement changed after exec; before:\n    %s\nafter:\n    %s",
					stmtStrBefore, after))
			}
		}
		res.PErr = convertToErrWithPGCode(res.PErr)
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
	stmt parser.Statement, txnState *txnState, planMaker *planner,
) (Result, *roachpb.Error) {

	if txnState.State != Aborted && txnState.State != RestartWait {
		panic("execStmtInAbortedTxn called outside of an aborted txn")
	}
	// TODO(andrei/cuongdo): Figure out what statements to count here.
	switch s := stmt.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		if txnState.State == RestartWait {
			return rollbackSQLTransaction(txnState, planMaker), nil
		}
		// Reset the state to allow new transactions to start.
		// The KV txn has already been rolled back when we entered the Aborted state.
		// Note: postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
		txnState.resetStateAndTxn(NoTxn)
		return result, nil
	case *parser.RollbackToSavepoint:
		if pErr := parser.ValidateRestartCheckpoint(s.Savepoint); pErr != nil {
			return Result{PErr: pErr}, pErr
		}
		if txnState.State == RestartWait {
			// Reset the state. Txn is Open again.
			txnState.State = Open
			txnState.retrying = true
			// TODO(andrei/cdo): add a counter for user-directed retries.
			return Result{}, nil
		}
		pErr := sqlErrToPErr(&errTransactionAborted{
			CustomMsg: fmt.Sprintf(
				"SAVEPOINT %s has not been used or a non-retriable error was encountered.",
				parser.RestartSavepointName)})
		return Result{PErr: pErr}, pErr
	default:
		pErr := sqlErrToPErr(&errTransactionAborted{})
		return Result{PErr: pErr}, pErr
	}
}

// execStmtInCommitWaitTxn executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (e *Executor) execStmtInCommitWaitTxn(
	stmt parser.Statement, txnState *txnState,
) (Result, *roachpb.Error) {
	if txnState.State != CommitWait {
		panic("execStmtInCommitWaitTxn called outside of an aborted txn")
	}
	e.updateStmtCounts(stmt)
	switch stmt.(type) {
	case *parser.CommitTransaction, *parser.RollbackTransaction:
		// Reset the state to allow new transactions to start.
		result := Result{PGTag: (*parser.CommitTransaction)(nil).StatementTag()}
		txnState.resetStateAndTxn(NoTxn)
		return result, nil
	default:
		pErr := sqlErrToPErr(&errTransactionCommitted{})
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
	stmt parser.Statement,
	planMaker *planner,
	implicitTxn bool,
	firstInTxn bool,
	txnState *txnState,
) (Result, *roachpb.Error) {
	if txnState.State != Open {
		panic("execStmtInOpenTxn called outside of an open txn")
	}
	if planMaker.txn == nil {
		panic("execStmtInOpenTxn called with the a txn not set on the planner")
	}

	planMaker.evalCtx.SetTxnTimestamp(txnState.sqlTimestamp)
	planMaker.evalCtx.SetStmtTimestamp(e.ctx.Clock.PhysicalTime())

	// TODO(cdo): Figure out how to not double count on retries.
	e.updateStmtCounts(stmt)
	switch s := stmt.(type) {
	case *parser.BeginTransaction:
		if !firstInTxn {
			pErr := roachpb.NewError(errTransactionInProgress)
			txnState.updateStateAndCleanupOnErr(pErr, e)
			return Result{PErr: pErr}, pErr
		}
	case *parser.CommitTransaction:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		// CommitTransaction is executed fully here; there's no planNode for it
		// and the planner is not involved at all.
		return commitSQLTransaction(txnState, planMaker, commit, e)
	case *parser.ReleaseSavepoint:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		if pErr := parser.ValidateRestartCheckpoint(s.Savepoint); pErr != nil {
			return Result{PErr: pErr}, pErr
		}
		// ReleaseSavepoint is executed fully here; there's no planNode for it
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
	case *parser.Savepoint:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		if pErr := parser.ValidateRestartCheckpoint(s.Name); pErr != nil {
			return Result{PErr: pErr}, pErr
		}
		// We check if the transaction has "started" already by looking inside the txn proto.
		// The executor should not be doing that. But it's also what the planner does for
		// SET TRANSACTION ISOLATION ... It feels ever more wrong here.
		// TODO(andrei): find a better way to track this running state.
		// TODO(andrei): the check for retrying is a hack - we erroneously allow
		// SAVEPOINT to be issued at any time during a retry, not just in the
		// beginning. We should figure out how to track whether we started using the
		// transaction during a retry.
		if txnState.txn.Proto.IsInitialized() && !txnState.retrying {
			pErr := roachpb.NewError(util.Errorf(
				"SAVEPOINT %s needs to be the first statement in a transaction",
				parser.RestartSavepointName))
			txnState.updateStateAndCleanupOnErr(pErr, e)
			return Result{PErr: pErr}, pErr
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		txnState.retryIntent = true
		return Result{}, nil
	case *parser.RollbackToSavepoint:
		pErr := parser.ValidateRestartCheckpoint(s.Savepoint)
		if pErr == nil {
			// Can't restart if we didn't get an error first, which would've put the
			// txn in a different state.
			pErr = roachpb.NewError(errNotRetriable)
		}
		txnState.updateStateAndCleanupOnErr(pErr, e)
		return Result{PErr: pErr}, pErr
	}

	// Bind all the placeholder variables in the stmt to actual values.
	stmt, err := parser.FillArgs(stmt, &planMaker.params)
	if err != nil {
		pErr := roachpb.NewError(err)
		txnState.updateStateAndCleanupOnErr(pErr, e)
		return Result{PErr: pErr}, pErr
	}

	if txnState.tr != nil {
		txnState.tr.LazyLog(stmt, true /* sensitive */)
	}
	result, pErr := e.execStmt(stmt, planMaker, implicitTxn /* autoCommit */)
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

// Clean up after trying to execute a transactional statement while not in a SQL
// transaction.
func (e *Executor) noTransactionHelper(txnState *txnState) (Result, *roachpb.Error) {
	pErr := roachpb.NewError(errNoTransactionInProgress)
	// Clean up the KV txn and set the SQL state to Aborted.
	txnState.updateStateAndCleanupOnErr(pErr, e)
	return Result{PErr: pErr}, pErr
}

// rollbackSQLTransaction rolls back a transaction. All errors are swallowed.
func rollbackSQLTransaction(txnState *txnState, p *planner) Result {
	if p.txn != txnState.txn {
		panic("rollbackSQLTransaction called on a different txn than the planner's")
	}
	if txnState.State != Open && txnState.State != RestartWait {
		panic(fmt.Sprintf("rollbackSQLTransaction called on txn in wrong state: %s (txn: %s)",
			txnState.State, txnState.txn.Proto))
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
func commitSQLTransaction(
	txnState *txnState, p *planner, commitType commitType, e *Executor,
) (Result, *roachpb.Error) {

	if p.txn != txnState.txn {
		panic("commitSQLTransaction called on a different txn than the planner's")
	}
	if txnState.State != Open {
		panic(fmt.Sprintf("commitSqlTransaction called on non-open txn: %+v", txnState.txn))
	}
	if commitType == commit {
		txnState.commitSeen = true
	}
	pErr := txnState.txn.Commit()
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
	stmt parser.Statement, planMaker *planner, autoCommit bool,
) (Result, *roachpb.Error) {
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
		return parser.DInterval{Duration: duration.Duration{Nanos: t.Nanoseconds()}}, true
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
