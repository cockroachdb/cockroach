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
	"fmt"
	"reflect"
	"sync"
	"time"

	"golang.org/x/net/context"
	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/distsql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")
var errNotRetriable = errors.New("the transaction is not in a retriable state")

const sqlTxnName string = "sql txn"
const sqlImplicitTxnName string = "sql txn implicit"

// TODO(radu): experimental code for testing distSQL flows.
//    0 : disabled
//    1 : enabled, sync mode
//    2 : enabled, async mode
const testDistSQL int = 0

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
	Err error
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
	systemConfigMu syncutil.RWMutex
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
	Context      context.Context
	DB           *client.DB
	Gossip       *gossip.Gossip
	LeaseManager *LeaseManager
	Clock        *hlc.Clock
	DistSQLSrv   *distsql.ServerImpl

	TestingKnobs *ExecutorTestingKnobs
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// TestingSchemaChangerCollection is an exported (for testing) version of
// schemaChangerCollection.
// TODO(andrei): get rid of this type once we can have tests internal to the sql
// package (as of April 2016 we can't because sql can't import server).
type TestingSchemaChangerCollection struct {
	scc *schemaChangerCollection
}

// ClearSchemaChangers clears the schema changers from the collection.
// If this is called from a SyncSchemaChangersFilter, no schema changer will be
// run.
func (tscc TestingSchemaChangerCollection) ClearSchemaChangers() {
	tscc.scc.schemaChangers = tscc.scc.schemaChangers[:0]
}

// SyncSchemaChangersFilter is the type of a hook to be installed through the
// ExecutorContext for blocking or otherwise manipulating schema changers run
// through the sync schema changers path.
type SyncSchemaChangersFilter func(TestingSchemaChangerCollection)

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

	// SyncSchemaChangersFilter is called before running schema changers
	// synchronously (at the end of a txn). The function can be used to clear the
	// schema changers (if the test doesn't want them run using the synchronous
	// path) or to temporarily block execution.
	// Note that this has nothing to do with the async path for running schema
	// changers. To block that, see TestDisableAsyncSchemaChangeExec().
	SyncSchemaChangersFilter SyncSchemaChangersFilter

	// SchemaChangersStartBackfillNotification is called before applying the
	// backfill for a schema change operation. It returns error to stop the
	// backfill and return an error to the caller of the SchemaChanger.exec().
	SchemaChangersStartBackfillNotification func() error

	//SyncSchemaChangersRenameOldNameNotInUseNotification is called during a rename
	//schema change, after all leases on the version of the descriptor with the
	//old name are gone, and just before the mapping of the old name to the
	//descriptor id is about to be deleted.
	SyncSchemaChangersRenameOldNameNotInUseNotification func()
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

// NewDummyExecutor creates an empty Executor that is used for certain tests.
func NewDummyExecutor() *Executor {
	return &Executor{ctx: ExecutorContext{Context: context.Background()}}
}

// Ctx returns the Context associated with this Executor.
func (e *Executor) Ctx() context.Context {
	return e.ctx.Context
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
		databases: map[string]sqlbase.ID{},
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

// Prepare returns the result types of the given statement. pinfo may
// contain partial type information for placeholders. Prepare will
// populate the missing types. The column result types are returned (or
// nil if there are no results).
func (e *Executor) Prepare(
	query string,
	session *Session,
	pinfo parser.PlaceholderTypes,
) ([]ResultColumn, error) {
	if log.V(2) {
		log.Infof(session.Ctx(), "preparing: %s", query)
	} else if traceSQL {
		log.Tracef(session.Ctx(), "preparing: %s", query)
	}
	stmt, err := parser.ParseOne(query, parser.Syntax(session.Syntax))
	if err != nil {
		return nil, err
	}
	if err = pinfo.ProcessPlaceholderAnnotations(stmt); err != nil {
		return nil, err
	}
	protoTS, err := isAsOf(&session.planner, stmt, e.ctx.Clock.Now())
	if err != nil {
		return nil, err
	}

	session.planner.resetForBatch(e)
	session.planner.semaCtx.Placeholders.SetTypes(pinfo)
	session.planner.evalCtx.PrepareOnly = true

	// Prepare needs a transaction because it needs to retrieve db/table
	// descriptors for type checking.
	txn := client.NewTxn(session.Ctx(), *e.ctx.DB)
	txn.Proto.Isolation = session.DefaultIsolationLevel
	session.planner.setTxn(txn)
	defer session.planner.setTxn(nil)

	if protoTS != nil {
		session.planner.asOf = true
		defer func() {
			session.planner.asOf = false
		}()

		setTxnTimestamps(txn, *protoTS)
	}

	plan, err := session.planner.prepare(stmt)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	cols := plan.Columns()
	for _, c := range cols {
		if err := checkResultDatum(c.Typ); err != nil {
			return nil, err
		}
	}
	return cols, nil
}

// ExecuteStatements executes the given statement(s) and returns a response.
func (e *Executor) ExecuteStatements(
	session *Session, stmts string, pinfo *parser.PlaceholderInfo,
) StatementResults {

	session.planner.resetForBatch(e)
	session.planner.semaCtx.Placeholders.Assign(pinfo)

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	return e.execRequest(session, stmts)
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
func (e *Executor) execRequest(session *Session, sql string) StatementResults {
	var res StatementResults
	txnState := &session.TxnState
	planMaker := &session.planner
	stmts, err := planMaker.parser.Parse(sql, parser.Syntax(session.Syntax))
	if err != nil {
		// A parse error occurred: we can't determine if there were multiple
		// statements or only one, so just pretend there was one.
		if txnState.txn != nil {
			// Rollback the txn.
			txnState.updateStateAndCleanupOnErr(err, e)
		}
		res.ResultList = append(res.ResultList, Result{Err: err})
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
		// If protoTS is set, the transaction proto sets its Orig and Max timestamps
		// to it each retry.
		var protoTS *hlc.Timestamp
		// We can AutoRetry the next batch of statements if we're in a clean state
		// (i.e. the next statements we're going to see are the first statements in
		// a transaction).
		if !inTxn {
			execOpt.MinInitialTimestamp = e.ctx.Clock.Now()
			// Detect implicit transactions.
			if _, isBegin := stmts[0].(*parser.BeginTransaction); !isBegin {
				execOpt.AutoCommit = true
				stmtsToExec = stmtsToExec[:1]
				// Check for AS OF SYSTEM TIME. If it is present but not detected here,
				// it will raise an error later on.
				protoTS, err = isAsOf(planMaker, stmtsToExec[0], execOpt.MinInitialTimestamp)
				if err != nil {
					res.ResultList = append(res.ResultList, Result{Err: err})
					return res
				}
				if protoTS != nil {
					planMaker.asOf = true
					defer func() {
						planMaker.asOf = false
					}()
				}
			}
			txnState.reset(session.Ctx(), e, session)
			txnState.State = Open
			txnState.autoRetry = true
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

		txnClosure := func(txn *client.Txn, opt *client.TxnExecOptions) error {
			if txnState.State == Open && txnState.txn != txn {
				panic(fmt.Sprintf("closure wasn't called in the txn we set up for it."+
					"\ntxnState.txn:%+v\ntxn:%+v\ntxnState:%+v", txnState.txn, txn, txnState))
			}
			txnState.txn = txn

			if protoTS != nil {
				setTxnTimestamps(txnState.txn, *protoTS)
			}

			var err error
			results, remainingStmts, err = runTxnAttempt(e, planMaker, origState, txnState, opt, stmtsToExec)
			return err
		}
		// This is where the magic happens - we ask db to run a KV txn and possibly retry it.
		txn := txnState.txn // this might be nil if the txn was already aborted.
		err := txn.Exec(execOpt, txnClosure)

		// Update the Err field of the last result if the error was coming from
		// auto commit. The error was generated outside of the txn closure, so it was not
		// set in any result.
		if err != nil {
			lastResult := &results[len(results)-1]
			if aErr, ok := err.(*client.AutoCommitError); ok {
				// Until #7881 fixed.
				if txn == nil {
					log.Errorf(session.Ctx(), "AutoCommitError on nil txn: %+v, "+
						"txnState %+v, execOpt %+v, stmts %+v, remaining %+v",
						err, txnState, execOpt, stmts, remainingStmts)
				}
				lastResult.Err = aErr
				e.txnAbortCount.Inc(1)
				txn.CleanupOnError(err)
			}
			if lastResult.Err == nil {
				log.Fatalf(session.Ctx(),
					"error (%s) was returned, but it was not set in the last result (%v)",
					err, lastResult)
			}
		}

		res.ResultList = append(res.ResultList, results...)
		// Now make sense of the state we got into and update txnState.
		if (txnState.State == RestartWait || txnState.State == Aborted) &&
			txnState.commitSeen {
			// A COMMIT got an error (retryable or not). Too bad, this txn is toast.
			// After we return a result for COMMIT (with the COMMIT pgwire tag), the
			// user can't send any more commands.
			e.txnAbortCount.Inc(1)
			txn.CleanupOnError(err)
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
			planMaker.checkTestingVerifyMetadataInitialOrDie(e, stmts)
			planMaker.checkTestingVerifyMetadataOrDie(e, stmtsExecuted)
			// Exec the schema changers (if the txn rolled back, the schema changers
			// will short-circuit because the corresponding descriptor mutation is not
			// found).
			planMaker.releaseLeases()
			txnState.schemaChangers.execSchemaChanges(e, planMaker, res.ResultList)
		} else {
			// We're still in a txn, so we only check that the verifyMetadata callback
			// fails the first time it's run. The gossip update that will make the
			// callback succeed only happens when the txn is done.
			planMaker.checkTestingVerifyMetadataInitialOrDie(e, stmtsExecuted)
		}

		// Figure out what statements to run on the next iteration.
		if err != nil {
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

// If the plan has a fast path we attempt to query that,
// otherwise we fall back to counting via plan.Next().
func countRowsAffected(p planNode) (int, error) {
	if a, ok := p.(planNodeFastPath); ok {
		if count, res := a.FastPathResults(); res {
			return count, nil
		}
	}
	count := 0
	next, err := p.Next()
	for ; next; next, err = p.Next() {
		count++
	}
	return count, err
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
) ([]Result, parser.StatementList, error) {

	// Ignore the state that might have been set by a previous try
	// of this closure.
	txnState.State = origState
	txnState.commitSeen = false

	planMaker.setTxn(txnState.txn)
	results, remainingStmts, err := e.execStmtsInCurrentTxn(
		stmts, planMaker, txnState,
		opt.AutoCommit /* implicitTxn */, opt.AutoRetry /* txnBeginning */)
	if opt.AutoCommit {
		if len(remainingStmts) > 0 {
			panic("implicit txn failed to execute all stmts")
		}
	}
	planMaker.resetTxn()
	return results, remainingStmts, err
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
) ([]Result, parser.StatementList, error) {
	var results []Result
	if txnState.State == NoTxn {
		panic("execStmtsInCurrentTransaction called outside of a txn")
	}
	if txnState.State == Open && planMaker.txn == nil {
		panic(fmt.Sprintf("inconsistent planMaker txn state. txnState: %+v", txnState))
	}

	for i, stmt := range stmts {
		ctx := planMaker.session.Ctx()
		if log.V(2) {
			log.Infof(ctx, "executing %d/%d: %s", i+1, len(stmts), stmt)
		} else if traceSQL {
			log.Tracef(ctx, "executing %d/%d: %s", i+1, len(stmts), stmt)
		}
		txnState.schemaChangers.curStatementIdx = i

		var stmtStrBefore string
		// TODO(nvanbenschoten) Constant literals can change their representation (1.0000 -> 1) when type checking,
		// so we need to reconsider how this works.
		if e.ctx.TestingKnobs.CheckStmtStringChange && false {
			stmtStrBefore = stmt.String()
		}
		var res Result
		var err error
		switch txnState.State {
		case Open:
			res, err = e.execStmtInOpenTxn(
				stmt, planMaker, implicitTxn, txnBeginning && (i == 0), /* firstInTxn */
				txnState)
		case Aborted, RestartWait:
			res, err = e.execStmtInAbortedTxn(stmt, txnState, planMaker)
		case CommitWait:
			res, err = e.execStmtInCommitWaitTxn(stmt, txnState)
		default:
			panic(fmt.Sprintf("unexpected txn state: %s", txnState.State))
		}
		if e.ctx.TestingKnobs.CheckStmtStringChange && false {
			if after := stmt.String(); after != stmtStrBefore {
				panic(fmt.Sprintf("statement changed after exec; before:\n    %s\nafter:\n    %s",
					stmtStrBefore, after))
			}
		}
		res.Err = convertToErrWithPGCode(res.Err)
		results = append(results, res)
		if err != nil {
			// After an error happened, skip executing all the remaining statements
			// in this batch.  This is Postgres behavior, and it makes sense as the
			// protocol doesn't let you return results after an error.
			return results, nil, err
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
// Aborted or RestartWait. All statements cause errors except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (e *Executor) execStmtInAbortedTxn(
	stmt parser.Statement, txnState *txnState, planMaker *planner,
) (Result, error) {

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
	case *parser.RollbackToSavepoint, *parser.Savepoint:
		// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
		// "SAVEPOINT cockroach_restart" commands to indicate client intent to
		// retry a transaction in a RestartWait state.
		var spName string
		switch n := s.(type) {
		case *parser.RollbackToSavepoint:
			spName = n.Savepoint
		case *parser.Savepoint:
			spName = n.Name
		default:
			panic("unreachable")
		}
		if err := parser.ValidateRestartCheckpoint(spName); err != nil {
			return Result{Err: err}, err
		}
		if txnState.State == RestartWait {
			// Reset the state. Txn is Open again.
			txnState.State = Open
			txnState.retrying = true
			// TODO(andrei/cdo): add a counter for user-directed retries.
			return Result{}, nil
		}
		err := sqlbase.NewTransactionAbortedError(fmt.Sprintf(
			"SAVEPOINT %s has not been used or a non-retriable error was encountered.",
			parser.RestartSavepointName))
		return Result{Err: err}, err
	default:
		err := sqlbase.NewTransactionAbortedError("")
		return Result{Err: err}, err
	}
}

// execStmtInCommitWaitTxn executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (e *Executor) execStmtInCommitWaitTxn(
	stmt parser.Statement, txnState *txnState,
) (Result, error) {
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
		err := sqlbase.NewTransactionCommittedError()
		return Result{Err: err}, err
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
) (Result, error) {
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
			txnState.updateStateAndCleanupOnErr(errTransactionInProgress, e)
			return Result{Err: errTransactionInProgress}, errTransactionInProgress
		}
	case *parser.CommitTransaction:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		// CommitTransaction is executed fully here; there's no planNode for it
		// and the planner is not involved at all.
		res, err := commitSQLTransaction(txnState, planMaker, commit, e)
		return res, err
	case *parser.ReleaseSavepoint:
		if implicitTxn {
			return e.noTransactionHelper(txnState)
		}
		if err := parser.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return Result{Err: err}, err
		}
		// ReleaseSavepoint is executed fully here; there's no planNode for it
		// and the planner is not involved at all.
		res, err := commitSQLTransaction(txnState, planMaker, release, e)
		return res, err
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
		if err := parser.ValidateRestartCheckpoint(s.Name); err != nil {
			return Result{Err: err}, err
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running, but such enforcement is problematic in the
		// presence of transaction retries (since the transaction proto is
		// necessarily reused). To work around this, we keep track of the
		// transaction's retrying state and special-case SAVEPOINT when it is
		// set.
		//
		// TODO(andrei): the check for retrying is a hack - we erroneously
		// allow SAVEPOINT to be issued at any time during a retry, not just
		// in the beginning. We should figure out how to track whether we
		// started using the transaction during a retry.
		if txnState.txn.Proto.IsInitialized() && !txnState.retrying {
			err := fmt.Errorf("SAVEPOINT %s needs to be the first statement in a transaction",
				parser.RestartSavepointName)
			txnState.updateStateAndCleanupOnErr(err, e)
			return Result{Err: err}, err
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		txnState.retryIntent = true
		return Result{}, nil
	case *parser.RollbackToSavepoint:
		err := parser.ValidateRestartCheckpoint(s.Savepoint)
		if err == nil {
			// Can't restart if we didn't get an error first, which would've put the
			// txn in a different state.
			err = errNotRetriable
		}
		txnState.updateStateAndCleanupOnErr(err, e)
		return Result{Err: err}, err
	case *parser.Prepare:
		err := util.UnimplementedWithIssueErrorf(7568,
			"Prepared statements are supported only via the Postgres wire protocol")
		txnState.updateStateAndCleanupOnErr(err, e)
		return Result{Err: err}, err
	case *parser.Execute:
		err := util.UnimplementedWithIssueErrorf(7568,
			"Executing prepared statements is supported only via the Postgres wire protocol")
		txnState.updateStateAndCleanupOnErr(err, e)
		return Result{Err: err}, err
	case *parser.Deallocate:
		if s.Name == "" {
			planMaker.session.PreparedStatements.DeleteAll()
		} else {
			if found := planMaker.session.PreparedStatements.Delete(string(s.Name)); !found {
				err := fmt.Errorf("prepared statement %s does not exist", s.Name)
				txnState.updateStateAndCleanupOnErr(err, e)
				return Result{Err: err}, err
			}
		}
		return Result{PGTag: s.StatementTag()}, nil
	}

	if txnState.tr != nil {
		txnState.tr.LazyLog(stmt, true /* sensitive */)
	}

	result, err := e.execStmt(stmt, planMaker, implicitTxn /* autoCommit */)
	if err != nil {
		if traceSQL {
			log.Tracef(txnState.txn.Context, "ERROR: %v", err)
		}
		if txnState.tr != nil {
			txnState.tr.LazyPrintf("ERROR: %v", err)
		}
		txnState.updateStateAndCleanupOnErr(err, e)
		result = Result{Err: err}
	} else if txnState.tr != nil {
		tResult := &traceResult{tag: result.PGTag, count: -1}
		switch result.Type {
		case parser.RowsAffected:
			tResult.count = result.RowsAffected
		case parser.Rows:
			tResult.count = len(result.Rows)
		}
		txnState.tr.LazyLog(tResult, false)
		if traceSQL {
			log.Tracef(txnState.txn.Context, "%s done", tResult)
		}
	}
	return result, err
}

// Clean up after trying to execute a transactional statement while not in a SQL
// transaction.
func (e *Executor) noTransactionHelper(txnState *txnState) (Result, error) {
	// Clean up the KV txn and set the SQL state to Aborted.
	txnState.updateStateAndCleanupOnErr(errNoTransactionInProgress, e)
	return Result{Err: errNoTransactionInProgress}, errNoTransactionInProgress
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
	err := p.txn.Rollback()
	result := Result{PGTag: (*parser.RollbackTransaction)(nil).StatementTag()}
	if err != nil {
		log.Warningf(p.ctx(), "txn rollback failed. The error was swallowed: %s", err)
		result.Err = err
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
) (Result, error) {

	if p.txn != txnState.txn {
		panic("commitSQLTransaction called on a different txn than the planner's")
	}
	if txnState.State != Open {
		panic(fmt.Sprintf("commitSqlTransaction called on non-open txn: %+v", txnState.txn))
	}
	if commitType == commit {
		txnState.commitSeen = true
	}
	err := txnState.txn.Commit()
	result := Result{PGTag: (*parser.CommitTransaction)(nil).StatementTag()}
	if err != nil {
		// Errors on COMMIT need special handling, as COMMIT needs to finalize the
		// transaction (it can't leave it in Aborted or RestartWait). Except if it's
		// an auto-retry txn, in which case we do want to leave it in RestartWait
		// here. We ignore all of this here and do regular cleanup. A higher layer
		// handles closing the txn if the auto-retry doesn't get rid of the error.
		txnState.updateStateAndCleanupOnErr(err, e)
		result.Err = err
	} else {
		switch commitType {
		case release:
			// We'll now be waiting for a COMMIT.
			txnState.State = CommitWait
		case commit:
			// We're done with this txn.
			txnState.State = NoTxn
		}
		txnState.dumpTrace()
		txnState.txn = nil
	}
	// Reset transaction to prevent running further commands on this planner.
	p.resetTxn()
	return result, err
}

// the current transaction might have been committed/rolled back when this returns.
func (e *Executor) execStmt(
	stmt parser.Statement, planMaker *planner, autoCommit bool,
) (Result, error) {
	var result Result
	plan, err := planMaker.makePlan(stmt, autoCommit)
	if err != nil {
		return result, err
	}

	if testDistSQL != 0 {
		if err := hackPlanToUseDistSQL(plan, testDistSQL == 1); err != nil {
			return result, err
		}
	}

	if err := plan.Start(); err != nil {
		return result, err
	}

	result.PGTag = stmt.StatementTag()
	result.Type = stmt.StatementType()

	switch result.Type {
	case parser.RowsAffected:
		count, err := countRowsAffected(plan)
		if err != nil {
			return result, err
		}
		result.RowsAffected += count

	case parser.Rows:
		result.Columns = plan.Columns()
		for _, c := range result.Columns {
			if err := checkResultDatum(c.Typ); err != nil {
				return result, err
			}
		}

		// valuesAlloc is used to allocate the backing storage for the
		// ResultRow.Values slices in chunks.
		var valuesAlloc []parser.Datum
		const maxChunkSize = 64 // Arbitrary, could use tuning.
		chunkSize := 4          // Arbitrary as well.

		next, err := plan.Next()
		for ; next; next, err = plan.Next() {
			// The plan.Values DTuple needs to be copied on each iteration.
			values := plan.Values()

			n := len(values)
			if len(valuesAlloc) < n {
				valuesAlloc = make([]parser.Datum, len(result.Columns)*chunkSize)
				if chunkSize < maxChunkSize {
					chunkSize *= 2
				}
			}
			row := ResultRow{Values: valuesAlloc[:0:n]}
			valuesAlloc = valuesAlloc[n:]

			for _, val := range values {
				if err := checkResultDatum(val); err != nil {
					return result, err
				}
				row.Values = append(row.Values, val)
			}
			result.Rows = append(result.Rows, row)
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
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

// golangFillQueryArguments populates the placeholder map with
// types and values from an array of Go values.
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func golangFillQueryArguments(pinfo *parser.PlaceholderInfo, args []interface{}) {
	pinfo.Clear()

	for i, arg := range args {
		k := fmt.Sprint(i + 1)
		if arg == nil {
			pinfo.SetValue(k, parser.DNull)
			continue
		}

		// A type switch to handle a few explicit types with special semantics:
		// - Datums are passed along as is.
		// - Time datatypes get special representation in the database.
		var d parser.Datum
		switch t := arg.(type) {
		case parser.Datum:
			d = t
		case time.Time:
			d = parser.MakeDTimestamp(t, time.Microsecond)
		case time.Duration:
			d = &parser.DInterval{Duration: duration.Duration{Nanos: t.Nanoseconds()}}
		case *inf.Dec:
			dd := &parser.DDecimal{}
			dd.Set(t)
			d = dd
		}
		if d == nil {
			// Handle all types which have an underlying type that can be stored in the
			// database.
			// Note: if this reflection becomes a performance concern in the future,
			// commonly used types could be added explicitly into the type switch above
			// for a performance gain.
			val := reflect.ValueOf(arg)
			switch val.Kind() {
			case reflect.Bool:
				d = parser.MakeDBool(parser.DBool(val.Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				d = parser.NewDInt(parser.DInt(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				d = parser.NewDInt(parser.DInt(val.Uint()))
			case reflect.Float32, reflect.Float64:
				d = parser.NewDFloat(parser.DFloat(val.Float()))
			case reflect.String:
				d = parser.NewDString(val.String())
			case reflect.Slice:
				// Handle byte slices.
				if val.Type().Elem().Kind() == reflect.Uint8 {
					d = parser.NewDBytes(parser.DBytes(val.Bytes()))
				}
			}
			if d == nil {
				panic(fmt.Sprintf("unexpected type %T", arg))
			}
		}
		pinfo.SetValue(k, d)
	}
}

func checkResultDatum(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}

	switch datum.(type) {
	case *parser.DBool:
	case *parser.DInt:
	case *parser.DFloat:
	case *parser.DDecimal:
	case *parser.DBytes:
	case *parser.DString:
	case *parser.DDate:
	case *parser.DTimestamp:
	case *parser.DTimestampTZ:
	case *parser.DInterval:
	case *parser.DPlaceholder:
		return fmt.Errorf("could not determine data type of %s %s", datum.Type(), datum)
	default:
		return errors.Errorf("unsupported result type: %s", datum.Type())
	}
	return nil
}

// makeResultColumns converts sqlbase.ColumnDescriptors to ResultColumns.
func makeResultColumns(colDescs []sqlbase.ColumnDescriptor) []ResultColumn {
	cols := make([]ResultColumn, 0, len(colDescs))
	for _, colDesc := range colDescs {
		// Convert the sqlbase.ColumnDescriptor to ResultColumn.
		typ := colDesc.Type.ToDatumType()
		if typ == nil {
			panic(fmt.Sprintf("unsupported column type: %s", colDesc.Type.Kind))
		}

		hidden := colDesc.Hidden
		cols = append(cols, ResultColumn{Name: colDesc.Name, Typ: typ, hidden: hidden})
	}
	return cols
}

// isAsOf analyzes a select statement to bypass the logic in newPlan(),
// since that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction should
// be set.
func isAsOf(planMaker *planner, stmt parser.Statement, max hlc.Timestamp) (*hlc.Timestamp, error) {
	s, ok := stmt.(*parser.Select)
	if !ok {
		return nil, nil
	}
	sc, ok := s.Select.(*parser.SelectClause)
	if !ok {
		return nil, nil
	}
	if sc.From == nil || sc.From.AsOf.Expr == nil {
		return nil, nil
	}
	te, err := sc.From.AsOf.Expr.TypeCheck(nil, parser.TypeString)
	if err != nil {
		return nil, err
	}
	d, err := te.Eval(&planMaker.evalCtx)
	if err != nil {
		return nil, err
	}
	ds, ok := d.(*parser.DString)
	if !ok {
		return nil, fmt.Errorf("AS OF SYSTEM TIME expected string, got %s", ds.Type())
	}
	// Allow nanosecond precision because the timestamp is only used by the
	// system and won't be returned to the user over pgwire.
	dt, err := parser.ParseDTimestamp(string(*ds), planMaker.session.Location, time.Nanosecond)
	if err != nil {
		return nil, err
	}
	ts := hlc.Timestamp{
		WallTime: dt.Time.UnixNano(),
	}
	if max.Less(ts) {
		return nil, fmt.Errorf("cannot specify timestamp in the future")
	}
	return &ts, nil
}

// setTxnTimestamps sets the transaction's proto timestamps and deadline
// to ts. This is for use with AS OF queries, and should be called in the
// retry block (except in the case of prepare which doesn't use retry). The
// deadline-checking code checks that the `Timestamp` field of the proto
// hasn't exceeded the deadline. Since we set the Timestamp field each retry,
// it won't ever exceed the deadline, and thus setting the deadline here is
// not strictly needed. However, it doesn't do anything incorrect and it will
// possibly find problems if things change in the future, so it is left in.
func setTxnTimestamps(txn *client.Txn, ts hlc.Timestamp) {
	txn.Proto.Timestamp = ts
	txn.Proto.OrigTimestamp = ts
	txn.Proto.MaxTimestamp = ts
	txn.UpdateDeadlineMaybe(ts)
}
