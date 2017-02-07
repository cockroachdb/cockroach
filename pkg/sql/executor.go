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
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")
var errNotRetriable = errors.New("the transaction is not in a retriable state")

const sqlTxnName string = "sql txn"
const sqlImplicitTxnName string = "sql txn implicit"

// Fully-qualified names for metrics.
var (
	MetaLatency       = metric.Metadata{Name: "sql.latency"}
	MetaTxnBegin      = metric.Metadata{Name: "sql.txn.begin.count"}
	MetaTxnCommit     = metric.Metadata{Name: "sql.txn.commit.count"}
	MetaTxnAbort      = metric.Metadata{Name: "sql.txn.abort.count"}
	MetaTxnRollback   = metric.Metadata{Name: "sql.txn.rollback.count"}
	MetaSelect        = metric.Metadata{Name: "sql.select.count"}
	MetaDistSQLSelect = metric.Metadata{Name: "sql.distsql.select.count"}
	MetaUpdate        = metric.Metadata{Name: "sql.update.count"}
	MetaInsert        = metric.Metadata{Name: "sql.insert.count"}
	MetaDelete        = metric.Metadata{Name: "sql.delete.count"}
	MetaDdl           = metric.Metadata{Name: "sql.ddl.count"}
	MetaMisc          = metric.Metadata{Name: "sql.misc.count"}
	MetaQuery         = metric.Metadata{Name: "sql.query.count"}
)

// distSQLExecMode controls if and when the Executor uses DistSQL.
type distSQLExecMode int

const (
	// distSQLOff means that we never use distSQL.
	distSQLOff distSQLExecMode = iota
	// distSQLAuto means that we automatically decide on a case-by-case basis if
	// we use distSQL.
	distSQLAuto
	// distSQLOn means that we use distSQL for queries that are supported.
	distSQLOn
	// distSQLAlways means that we only use distSQL; unsupported queries fail.
	distSQLAlways
)

func distSQLExecModeFromString(val string) distSQLExecMode {
	switch strings.ToUpper(val) {
	case "OFF":
		return distSQLOff
	case "AUTO":
		return distSQLAuto
	case "ON":
		return distSQLOn
	case "ALWAYS":
		return distSQLAlways
	default:
		panic(fmt.Sprintf("unknown DistSQL mode %s", val))
	}
}

// defaultDistSQLMode controls the default DistSQL mode (see above). It can
// still be overridden per-session using `SET DIST_SQL = ...`.
var defaultDistSQLMode = distSQLExecModeFromString(
	envutil.EnvOrDefaultString("COCKROACH_DISTSQL_MODE", "OFF"),
)

// SetDefaultDistSQLMode changes the default DistSQL mode; returns a function
// that can be used to restore the previous mode.
func SetDefaultDistSQLMode(mode string) func() {
	prevMode := defaultDistSQLMode
	defaultDistSQLMode = distSQLExecModeFromString(mode)
	return func() {
		defaultDistSQLMode = prevMode
	}
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

// Close ensures that the resources claimed by the results are released.
func (s *StatementResults) Close() {
	s.ResultList.Close()
}

// Close ensures that the resources claimed by the results are released.
func (rl ResultList) Close() {
	for _, r := range rl {
		r.Close()
	}
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
	Columns ResultColumns
	// Rows will be populated if the statement type is "Rows". It will contain
	// the result set of the result.
	// TODO(nvanbenschoten): Can this be streamed from the planNode?
	Rows *RowContainer
}

// Close ensures that the resources claimed by the result are released.
func (r *Result) Close() {
	// The Rows pointer may be nil if the statement returned no rows or
	// if an error occurred.
	if r.Rows != nil {
		r.Rows.Close()
	}
}

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  parser.Type

	// If set, this is an implicit column; used internally.
	hidden bool

	// If set, a value won't be produced for this column; used internally.
	omitted bool
}

// ResultColumns is the type used throughout the sql module to
// describe the column types of a table.
type ResultColumns []ResultColumn

// An Executor executes SQL statements.
// Executor is thread-safe.
type Executor struct {
	cfg            ExecutorConfig
	stopper        *stop.Stopper
	reCache        *parser.RegexpCache
	virtualSchemas virtualSchemaHolder

	// Transient stats.
	Latency     *metric.Histogram
	SelectCount *metric.Counter
	// The subset of SELECTs that are processed through DistSQL.
	DistSQLSelectCount *metric.Counter
	TxnBeginCount      *metric.Counter

	// txnCommitCount counts the number of times a COMMIT was attempted.
	TxnCommitCount *metric.Counter

	TxnAbortCount    *metric.Counter
	TxnRollbackCount *metric.Counter
	UpdateCount      *metric.Counter
	InsertCount      *metric.Counter
	DeleteCount      *metric.Counter
	DdlCount         *metric.Counter
	MiscCount        *metric.Counter
	QueryCount       *metric.Counter

	// System Config and mutex.
	systemConfig   config.SystemConfig
	databaseCache  *databaseCache
	systemConfigMu syncutil.RWMutex
	// This uses systemConfigMu in RLocker mode to not block
	// execution of statements. So don't go on changing state after you've
	// Wait()ed on it.
	systemConfigCond *sync.Cond

	distSQLPlanner *distSQLPlanner
}

// An ExecutorConfig encompasses the auxiliary objects and configuration
// required to create an executor.
// All fields holding a pointer or an interface are required to create
// a Executor; the rest will have sane defaults set if omitted.
type ExecutorConfig struct {
	AmbientCtx   log.AmbientContext
	NodeID       *base.NodeIDContainer
	DB           *client.DB
	Gossip       *gossip.Gossip
	DistSender   *kv.DistSender
	RPCContext   *rpc.Context
	LeaseManager *LeaseManager
	Clock        *hlc.Clock
	DistSQLSrv   *distsqlrun.ServerImpl

	TestingKnobs              *ExecutorTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
	// HistogramWindowInterval is (server.Context).HistogramWindowInterval.
	HistogramWindowInterval time.Duration
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// StatementFilter is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
type StatementFilter func(context.Context, string, *Result)

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// WaitForGossipUpdate causes metadata-mutating operations to wait
	// for the new metadata to back-propagate through gossip.
	WaitForGossipUpdate bool

	// CheckStmtStringChange causes Executor.execStmtsInCurrentTxn to verify
	// that executed statements are not modified during execution.
	CheckStmtStringChange bool

	// StatementFilter can be used to trap execution of SQL statements and
	// optionally change their results. The filter function is invoked after each
	// statement has been executed.
	StatementFilter StatementFilter

	// DisableAutoCommit, if set, disables the auto-commit functionality of some
	// SQL statements. That functionality allows some statements to commit
	// directly when they're executed in an implicit SQL txn, without waiting for
	// the Executor to commit the implicit txn.
	// This has to be set in tests that need to abort such statements using a
	// StatementFilter; otherwise, the statement commits immediately after
	// execution so there'll be nothing left to abort by the time the filter runs.
	DisableAutoCommit bool
}

// NewExecutor creates an Executor and registers a callback on the
// system config.
func NewExecutor(cfg ExecutorConfig, stopper *stop.Stopper) *Executor {
	return &Executor{
		cfg:     cfg,
		stopper: stopper,
		reCache: parser.NewRegexpCache(512),

		Latency:            metric.NewLatency(MetaLatency, cfg.HistogramWindowInterval),
		TxnBeginCount:      metric.NewCounter(MetaTxnBegin),
		TxnCommitCount:     metric.NewCounter(MetaTxnCommit),
		TxnAbortCount:      metric.NewCounter(MetaTxnAbort),
		TxnRollbackCount:   metric.NewCounter(MetaTxnRollback),
		SelectCount:        metric.NewCounter(MetaSelect),
		DistSQLSelectCount: metric.NewCounter(MetaDistSQLSelect),
		UpdateCount:        metric.NewCounter(MetaUpdate),
		InsertCount:        metric.NewCounter(MetaInsert),
		DeleteCount:        metric.NewCounter(MetaDelete),
		DdlCount:           metric.NewCounter(MetaDdl),
		MiscCount:          metric.NewCounter(MetaMisc),
		QueryCount:         metric.NewCounter(MetaQuery),
	}
}

// Start starts workers for the executor and initializes the distSQLPlanner.
func (e *Executor) Start(
	ctx context.Context, startupMemMetrics *MemoryMetrics, nodeDesc roachpb.NodeDescriptor,
) {
	ctx = e.AnnotateCtx(ctx)
	log.Infof(ctx, "creating distSQLPlanner with address %s", nodeDesc.Address)
	e.distSQLPlanner = newDistSQLPlanner(
		nodeDesc, e.cfg.RPCContext, e.cfg.DistSQLSrv, e.cfg.DistSender, e.cfg.Gossip,
	)

	e.systemConfigCond = sync.NewCond(e.systemConfigMu.RLocker())

	gossipUpdateC := e.cfg.Gossip.RegisterSystemConfigChannel()
	e.stopper.RunWorker(func() {
		for {
			select {
			case <-gossipUpdateC:
				sysCfg, _ := e.cfg.Gossip.GetSystemConfig()
				e.updateSystemConfig(sysCfg)
			case <-e.stopper.ShouldStop():
				return
			}
		}
	})

	ctx = log.WithLogTag(ctx, "startup", nil)
	startupSession := NewSession(ctx, SessionArgs{}, e, nil, startupMemMetrics)
	if err := e.virtualSchemas.init(&startupSession.planner); err != nil {
		log.Fatal(ctx, err)
	}
	startupSession.Finish(e)
}

// SetDistSQLSpanResolver changes the SpanResolver used for DistSQL. It is the
// caller's responsibility to make sure no queries are being run with DistSQL at
// the same time.
func (e *Executor) SetDistSQLSpanResolver(spanResolver distsqlplan.SpanResolver) {
	e.distSQLPlanner.setSpanResolver(spanResolver)
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (e *Executor) AnnotateCtx(ctx context.Context) context.Context {
	return e.cfg.AmbientCtx.AnnotateCtx(ctx)
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
	query string, session *Session, pinfo parser.PlaceholderTypes,
) (ResultColumns, error) {
	log.VEventf(session.Ctx(), 2, "preparing: %s", query)
	var p parser.Parser
	stmts, err := p.Parse(query, parser.Syntax(session.Syntax))
	if err != nil {
		return nil, err
	}
	switch len(stmts) {
	case 0:
		return nil, nil
	case 1:
		// ignore
	default:
		return nil, errors.Errorf("expected 1 statement, but found %d", len(stmts))
	}
	stmt := stmts[0]
	if err = pinfo.ProcessPlaceholderAnnotations(stmt); err != nil {
		return nil, err
	}
	protoTS, err := isAsOf(&session.planner, stmt, e.cfg.Clock.Now())
	if err != nil {
		return nil, err
	}

	session.planner.resetForBatch(e)
	session.planner.semaCtx.Placeholders.SetTypes(pinfo)
	session.planner.evalCtx.PrepareOnly = true

	// Prepare needs a transaction because it needs to retrieve db/table
	// descriptors for type checking.
	// TODO(andrei): is this OK? If we're preparing as part of a SQL txn, how do
	// we check that they're reading descriptors consistent with the txn in which
	// they'll be used?
	txn := client.NewTxn(session.Ctx(), *e.cfg.DB)
	txn.Proto.Isolation = session.DefaultIsolationLevel
	session.planner.setTxn(txn)
	defer session.planner.setTxn(nil)

	if protoTS != nil {
		session.planner.avoidCachedDescriptors = true
		defer func() {
			session.planner.avoidCachedDescriptors = false
		}()

		SetTxnTimestamps(txn, *protoTS)
	}

	plan, err := session.planner.prepare(stmt)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	defer plan.Close()
	cols := plan.Columns()
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
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

	defer func() {
		if r := recover(); r != nil {
			// On a panic, prepend the executed SQL.
			panic(fmt.Errorf("%s: %s", stmts, r))
		}
	}()

	// Send the Request for SQL execution and set the application-level error
	// for each result in the reply.
	return e.execRequest(session, stmts, copyMsgNone)
}

// CopyData adds data to the COPY buffer and executes if there are enough rows.
func (e *Executor) CopyData(session *Session, data string) StatementResults {
	return e.execRequest(session, data, copyMsgData)
}

// CopyDone executes the buffered COPY data.
func (e *Executor) CopyDone(session *Session) StatementResults {
	return e.execRequest(session, "", copyMsgDone)
}

// CopyEnd ends the COPY mode. Any buffered data is discarded.
func (session *Session) CopyEnd() {
	session.planner.copyFrom.Close()
	session.planner.copyFrom = nil
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
// KV transactions and automatically retries them when possible, and executes
// the (synchronous attempt of) schema changes.
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
func (e *Executor) execRequest(session *Session, sql string, copymsg copyMsg) StatementResults {
	var res StatementResults
	txnState := &session.TxnState
	planMaker := &session.planner
	var stmts parser.StatementList
	var err error

	if log.V(2) {
		log.Infof(session.Ctx(), "execRequest: %s", sql)
	}

	if session.planner.copyFrom != nil {
		stmts, err = session.planner.ProcessCopyData(sql, copymsg)
	} else if copymsg != copyMsgNone {
		err = fmt.Errorf("unexpected copy command")
	} else {
		stmts, err = planMaker.parser.Parse(sql, parser.Syntax(session.Syntax))
	}
	if err != nil {
		if log.V(2) {
			log.Infof(session.Ctx(), "execRequest: error: %v", err)
		}
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
		execOpt := client.TxnExecOptions{
			Clock: e.cfg.Clock,
		}
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
			// Detect implicit transactions.
			if _, isBegin := stmts[0].(*parser.BeginTransaction); !isBegin {
				execOpt.AutoCommit = true
				stmtsToExec = stmtsToExec[:1]
				// Check for AS OF SYSTEM TIME. If it is present but not detected here,
				// it will raise an error later on.
				protoTS, err = isAsOf(planMaker, stmtsToExec[0], e.cfg.Clock.Now())
				if err != nil {
					res.ResultList = append(res.ResultList, Result{Err: err})
					return res
				}
				if protoTS != nil {
					planMaker.avoidCachedDescriptors = true
					defer func() {
						planMaker.avoidCachedDescriptors = false
					}()
				}
			}
			txnState.resetForNewSQLTxn(e, session)
			txnState.autoRetry = true
			txnState.sqlTimestamp = e.cfg.Clock.PhysicalTime()
			if execOpt.AutoCommit {
				txnState.txn.SetDebugName(sqlImplicitTxnName)
			} else {
				txnState.txn.SetDebugName(sqlTxnName)
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

		// Track if we are retrying this query, so that we do not double count.
		isAutomaticRetry := false
		txnClosure := func(txn *client.Txn, opt *client.TxnExecOptions) error {
			defer func() { isAutomaticRetry = true }()
			if txnState.State == Open && txnState.txn != txn {
				panic(fmt.Sprintf("closure wasn't called in the txn we set up for it."+
					"\ntxnState.txn:%+v\ntxn:%+v\ntxnState:%+v", txnState.txn, txn, txnState))
			}
			txnState.txn = txn

			if protoTS != nil {
				SetTxnTimestamps(txnState.txn, *protoTS)
			}

			var err error
			if results != nil {
				// Some results were produced by a previous attempt. Discard them.
				ResultList(results).Close()
			}
			results, remainingStmts, err = runTxnAttempt(e, planMaker, origState, txnState, opt, stmtsToExec, isAutomaticRetry)

			// TODO(andrei): Until #7881 fixed.
			if err == nil && txnState.State == Aborted {
				doWarn := true
				if len(stmtsToExec) > 0 {
					if _, ok := stmtsToExec[0].(*parser.ShowTransactionStatus); ok {
						doWarn = false
					}
				}
				if doWarn {
					log.Errorf(session.Ctx(),
						"7881: txnState is Aborted without an error propagating. stmtsToExec: %s, "+
							"results: %+v, remainingStmts: %s, txnState: %+v", stmtsToExec, results,
						remainingStmts, txnState)
				}
			}

			return err
		}
		// This is where the magic happens - we ask db to run a KV txn and possibly retry it.
		txn := txnState.txn // this might be nil if the txn was already aborted.
		err := txn.Exec(execOpt, txnClosure)
		if err != nil && len(results) > 0 {
			// Set or override the error in the last result, if any.
			// The error might have come from auto-commit, in which case it wasn't
			// captured in a result. Or, we might have had a RetryableTxnError that
			// got converted to a non-retryable error when the txn closure was done.
			lastRes := &results[len(results)-1]
			lastRes.Err = convertToErrWithPGCode(err)
		}

		if err != nil && log.V(2) {
			log.Infof(session.Ctx(), "execRequest: error: %v", err)
		}

		// Update the Err field of the last result if the error was coming from
		// auto commit. The error was generated outside of the txn closure, so it was not
		// set in any result.
		if err != nil {
			if aErr, ok := err.(*client.AutoCommitError); ok {
				// TODO(andrei): Until #7881 fixed.
				{
					log.Eventf(session.Ctx(), "executor got AutoCommitError: %s\n"+
						"txn: %+v\nexecOpt.AutoRetry %t, execOpt.AutoCommit:%t, stmts %+v, remaining %+v",
						aErr, txnState.txn.Proto, execOpt.AutoRetry, execOpt.AutoCommit, stmts,
						remainingStmts)
					if txnState.txn == nil {
						log.Errorf(session.Ctx(), "7881: AutoCommitError on nil txn: %s, "+
							"txnState %+v, execOpt %+v, stmts %+v, remaining %+v",
							aErr, txnState, execOpt, stmts, remainingStmts)
						txnState.sp.SetBaggageItem(keyFor7881Sample, "sample me please")
					}
				}
				e.TxnAbortCount.Inc(1)
				txn.CleanupOnError(err)
			}
		}

		// Sanity check about not leaving KV txns open on errors.
		if err != nil && txnState.txn != nil && !txnState.txn.IsFinalized() {
			if _, retryable := err.(*roachpb.RetryableTxnError); !retryable {
				log.Fatalf(session.Ctx(), "got a non-retryable error but the KV "+
					"transaction is not finalized. TxnState: %s, err: %s\n"+
					"err:%+v\n\ntxn: %s", txnState.State, err, err, txnState.txn.Proto)
			}
		}

		res.ResultList = append(res.ResultList, results...)
		// Now make sense of the state we got into and update txnState.
		if (txnState.State == RestartWait || txnState.State == Aborted) &&
			txnState.commitSeen {
			// A COMMIT got an error (retryable or not). Too bad, this txn is toast.
			// After we return a result for COMMIT (with the COMMIT pgwire tag), the
			// user can't send any more commands.
			e.TxnAbortCount.Inc(1)
			txn.CleanupOnError(err)
			txnState.resetStateAndTxn(NoTxn)
		}

		if execOpt.AutoCommit {
			// If execOpt.AutoCommit was set, then the txn no longer exists at this point.
			txnState.resetStateAndTxn(NoTxn)
		}

		// If we're no longer in a transaction, finish the trace.
		if txnState.State == NoTxn {
			txnState.finishSQLTxn(session.context)
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
	isAutomaticRetry bool,
) ([]Result, parser.StatementList, error) {

	// Ignore the state that might have been set by a previous try
	// of this closure.
	txnState.State = origState
	txnState.commitSeen = false

	planMaker.setTxn(txnState.txn)
	results, remainingStmts, err := e.execStmtsInCurrentTxn(
		stmts, planMaker, txnState,
		opt.AutoCommit /* implicitTxn */, opt.AutoRetry /* txnBeginning */, isAutomaticRetry)
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
//    occurred, it corresponds to the last result returned. Subsequent statements
//    have not been executed. Note that usually the error is not reflected in
//    this last result; the caller is responsible copying it into the result
//    after converting it adequately.
func (e *Executor) execStmtsInCurrentTxn(
	stmts parser.StatementList,
	planMaker *planner,
	txnState *txnState,
	implicitTxn bool,
	txnBeginning bool,
	isAutomaticRetry bool,
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
		log.VEventf(ctx, 2, "executing %d/%d: %s", i+1, len(stmts), stmt)
		txnState.schemaChangers.curStatementIdx = i

		var stmtStrBefore string
		// TODO(nvanbenschoten) Constant literals can change their representation (1.0000 -> 1) when type checking,
		// so we need to reconsider how this works.
		if (e.cfg.TestingKnobs.CheckStmtStringChange && false) ||
			(e.cfg.TestingKnobs.StatementFilter != nil) {
			// We do "statement string change" if a StatementFilter is installed,
			// because the StatementFilter relies on the textual representation of
			// statements to not change from what the client sends.
			stmtStrBefore = stmt.String()
		}

		var res Result
		var err error
		// Run SHOW TRANSACTION STATUS in a separate code path so it is
		// always guaranteed to execute regardless of the current transaction state.
		if _, ok := stmt.(*parser.ShowTransactionStatus); ok {
			res, err = planMaker.runShowTransactionState(txnState, implicitTxn)
		} else {
			switch txnState.State {
			case Open:
				res, err = e.execStmtInOpenTxn(
					stmt, planMaker, implicitTxn, txnBeginning && (i == 0), /* firstInTxn */
					txnState, isAutomaticRetry)
			case Aborted, RestartWait:
				res, err = e.execStmtInAbortedTxn(stmt, txnState, planMaker)
			case CommitWait:
				res, err = e.execStmtInCommitWaitTxn(stmt, txnState)
			default:
				panic(fmt.Sprintf("unexpected txn state: %s", txnState.State))
			}
			if (e.cfg.TestingKnobs.CheckStmtStringChange && false) ||
				(e.cfg.TestingKnobs.StatementFilter != nil) {
				if after := stmt.String(); after != stmtStrBefore {
					panic(fmt.Sprintf("statement changed after exec; before:\n    %s\nafter:\n    %s",
						stmtStrBefore, after))
				}
			}
		}
		if filter := e.cfg.TestingKnobs.StatementFilter; filter != nil {
			filter(ctx, stmt.String(), &res)
		}
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
			return Result{}, err
		}
		if txnState.State == RestartWait {
			// Reset the state. Txn is Open again.
			txnState.State = Open
			txnState.retrying = true
			// TODO(andrei/cdo): add a counter for user-directed retries.
			return Result{}, nil
		}
		err := sqlbase.NewTransactionAbortedError(fmt.Sprintf(
			"SAVEPOINT %s has not been used or a non-retriable error was encountered",
			parser.RestartSavepointName))
		return Result{}, err
	default:
		err := sqlbase.NewTransactionAbortedError("")
		return Result{}, err
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
		return Result{}, err
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
// isAutomaticRetry: A boolean that is set for retries so that we don't double
// count in metrics.
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
	isAutomaticRetry bool,
) (Result, error) {
	if txnState.State != Open {
		panic("execStmtInOpenTxn called outside of an open txn")
	}
	if planMaker.txn == nil {
		panic("execStmtInOpenTxn called with a txn not set on the planner")
	}

	planMaker.evalCtx.SetTxnTimestamp(txnState.sqlTimestamp)
	planMaker.evalCtx.SetStmtTimestamp(e.cfg.Clock.PhysicalTime())

	session := planMaker.session
	log.Eventf(session.context, "%s", stmt)

	// Do not double count automatically retried transactions.
	if !isAutomaticRetry {
		e.updateStmtCounts(stmt)
	}
	switch s := stmt.(type) {
	case *parser.BeginTransaction:
		if !firstInTxn {
			txnState.updateStateAndCleanupOnErr(errTransactionInProgress, e)
			return Result{}, errTransactionInProgress
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
			return Result{}, err
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
			return Result{}, err
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
			return Result{}, err
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
		return Result{}, err
	case *parser.Prepare:
		err := util.UnimplementedWithIssueErrorf(7568,
			"Prepared statements are supported only via the Postgres wire protocol")
		txnState.updateStateAndCleanupOnErr(err, e)
		return Result{}, err
	case *parser.Execute:
		err := util.UnimplementedWithIssueErrorf(7568,
			"Executing prepared statements is supported only via the Postgres wire protocol")
		txnState.updateStateAndCleanupOnErr(err, e)
		return Result{}, err
	case *parser.Deallocate:
		if s.Name == "" {
			planMaker.session.PreparedStatements.DeleteAll()
		} else {
			if found := planMaker.session.PreparedStatements.Delete(string(s.Name)); !found {
				err := fmt.Errorf("prepared statement %s does not exist", s.Name)
				txnState.updateStateAndCleanupOnErr(err, e)
				return Result{}, err
			}
		}
		return Result{PGTag: s.StatementTag()}, nil
	}

	autoCommit := implicitTxn && !e.cfg.TestingKnobs.DisableAutoCommit
	result, err := e.execStmt(stmt, planMaker, autoCommit, isAutomaticRetry)
	if err != nil {
		if result.Rows != nil {
			result.Rows.Close()
			result.Rows = nil
		}
		if traceSQL {
			log.ErrEventf(txnState.txn.Context, "ERROR: %v", err)
		}
		log.ErrEventf(session.context, "ERROR: %v", err)
		txnState.updateStateAndCleanupOnErr(err, e)
		return Result{}, err
	}

	tResult := &traceResult{tag: result.PGTag, count: -1}
	switch result.Type {
	case parser.RowsAffected:
		tResult.count = result.RowsAffected
	case parser.Rows:
		tResult.count = result.Rows.Len()
	}
	if traceSQL {
		log.Eventf(txnState.txn.Context, "%s done", tResult)
	}
	log.Eventf(session.context, "%s done", tResult)
	return result, nil
}

// Clean up after trying to execute a transactional statement while not in a SQL
// transaction.
func (e *Executor) noTransactionHelper(txnState *txnState) (Result, error) {
	// Clean up the KV txn and set the SQL state to Aborted.
	txnState.updateStateAndCleanupOnErr(errNoTransactionInProgress, e)
	return Result{}, errNoTransactionInProgress
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
	} else {
		switch commitType {
		case release:
			// We'll now be waiting for a COMMIT.
			txnState.resetStateAndTxn(CommitWait)
		case commit:
			// We're done with this txn.
			txnState.resetStateAndTxn(NoTxn)
		}
	}
	// Reset transaction to prevent running further commands on this planner.
	p.resetTxn()
	return result, err
}

// exectDistSQL converts a classic plan to a distributed SQL physical plan and
// runs it.
func (e *Executor) execDistSQL(planMaker *planner, tree planNode, result *Result) error {
	// Note: if we just want the row count, result.Rows is nil here.
	recv := distSQLReceiver{rows: result.Rows}
	err := e.distSQLPlanner.PlanAndRun(planMaker.session.Ctx(), planMaker.txn, tree, &recv)
	if err != nil {
		return err
	}
	if recv.err != nil {
		return recv.err
	}
	if result.Type == parser.RowsAffected {
		result.RowsAffected = int(recv.numRows)
	}
	return nil
}

// execClassic runs a plan using the classic (non-distributed) SQL
// implementation.
func (e *Executor) execClassic(planMaker *planner, plan planNode, result *Result) error {
	if err := planMaker.startPlan(plan); err != nil {
		return err
	}

	switch result.Type {
	case parser.RowsAffected:
		count, err := countRowsAffected(plan)
		if err != nil {
			return err
		}
		result.RowsAffected += count

	case parser.Rows:
		next, err := plan.Next()
		for ; next; next, err = plan.Next() {
			// The plan.Values Datums needs to be copied on each iteration.
			values := plan.Values()

			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			if _, err := result.Rows.AddRow(values); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// shouldUseDistSQL determines whether we should use DistSQL for a plan, based
// on the session settings.
func (e *Executor) shouldUseDistSQL(planMaker *planner, plan planNode) (bool, error) {
	distSQLMode := defaultDistSQLMode
	if planMaker.session.DistSQLMode != distSQLOff {
		distSQLMode = planMaker.session.DistSQLMode
	}

	if distSQLMode == distSQLOff {
		return false, nil
	}
	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*emptyNode); ok {
		return false, nil
	}

	var err error
	var distribute bool

	// Temporary workaround for #13376: if the transaction modified something,
	// TxnCoordSender will freak out if it sees scans in this txn from other
	// nodes. We detect this by checking if the transaction's "anchor" key is set.
	if planMaker.txn.Proto.TxnMeta.Key != nil {
		err = errors.New("writing txn")
	} else {
		distribute, err = e.distSQLPlanner.CheckSupport(plan)
	}

	if err != nil {
		// If the distSQLMode is ALWAYS, any unsupported statement is an error.
		if distSQLMode == distSQLAlways {
			return false, err
		}
		// Don't use distSQL for this request.
		log.VEventf(planMaker.session.Ctx(), 1, "query not supported for distSQL: %s", err)
		return false, nil
	}

	if distSQLMode == distSQLAuto && !distribute {
		log.VEventf(planMaker.session.Ctx(), 1, "not distributing query")
		return false, nil
	}

	// In ON or ALWAYS mode, all supported queries are distributed.
	return true, nil
}

// The current transaction might have been committed/rolled back when this returns.
// The caller closes result.Rows (even in error cases).
func (e *Executor) execStmt(
	stmt parser.Statement, planMaker *planner, autoCommit bool, isAutomaticRetry bool,
) (Result, error) {
	plan, err := planMaker.makePlan(stmt, autoCommit)
	if err != nil {
		return Result{}, err
	}

	defer plan.Close()

	result := Result{
		PGTag: stmt.StatementTag(),
		Type:  stmt.StatementType(),
	}
	if result.Type == parser.Rows {
		result.Columns = plan.Columns()
		for _, c := range result.Columns {
			if err := checkResultType(c.Typ); err != nil {
				return result, err
			}
		}
		result.Rows = NewRowContainer(planMaker.session.makeBoundAccount(), result.Columns, 0)
	}

	useDistSQL, err := e.shouldUseDistSQL(planMaker, plan)
	if err != nil {
		return result, err
	}
	if useDistSQL && !isAutomaticRetry {
		switch stmt.(type) {
		case *parser.Select:
			e.DistSQLSelectCount.Inc(1)
		}
		err = e.execDistSQL(planMaker, plan, &result)
	} else {
		err = e.execClassic(planMaker, plan, &result)
	}
	return result, err
}

// updateStmtCounts updates metrics for the number of times the different types of SQL
// statements have been received by this node.
func (e *Executor) updateStmtCounts(stmt parser.Statement) {
	e.QueryCount.Inc(1)
	switch stmt.(type) {
	case *parser.BeginTransaction:
		e.TxnBeginCount.Inc(1)
	case *parser.Select:
		e.SelectCount.Inc(1)
	case *parser.Update:
		e.UpdateCount.Inc(1)
	case *parser.Insert:
		e.InsertCount.Inc(1)
	case *parser.Delete:
		e.DeleteCount.Inc(1)
	case *parser.CommitTransaction:
		e.TxnCommitCount.Inc(1)
	case *parser.RollbackTransaction:
		e.TxnRollbackCount.Inc(1)
	default:
		if stmt.StatementType() == parser.DDL {
			e.DdlCount.Inc(1)
		} else {
			e.MiscCount.Inc(1)
		}
	}
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

func checkResultType(typ parser.Type) error {
	// Compare all types that can rely on == equality.
	switch parser.UnwrapType(typ) {
	case parser.TypeNull:
	case parser.TypeBool:
	case parser.TypeInt:
	case parser.TypeFloat:
	case parser.TypeDecimal:
	case parser.TypeBytes:
	case parser.TypeString:
	case parser.TypeDate:
	case parser.TypeTimestamp:
	case parser.TypeTimestampTZ:
	case parser.TypeInterval:
	case parser.TypeStringArray:
	case parser.TypeNameArray:
	case parser.TypeIntArray:
	default:
		// Compare all types that cannot rely on == equality.
		istype := typ.FamilyEqual
		switch {
		case istype(parser.TypeCollatedString):
		case istype(parser.TypeTuple):
		case istype(parser.TypePlaceholder):
			return errors.Errorf("could not determine data type of %s", typ)
		default:
			return errors.Errorf("unsupported result type: %s", typ)
		}
	}
	return nil
}

// makeResultColumns converts sqlbase.ColumnDescriptors to ResultColumns.
func makeResultColumns(colDescs []sqlbase.ColumnDescriptor) ResultColumns {
	cols := make(ResultColumns, 0, len(colDescs))
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

// EvalAsOfTimestamp evaluates and returns the timestamp from an AS OF SYSTEM
// TIME clause.
func EvalAsOfTimestamp(
	evalCtx *parser.EvalContext, asOf parser.AsOfClause, max hlc.Timestamp,
) (hlc.Timestamp, error) {
	var ts hlc.Timestamp
	te, err := asOf.Expr.TypeCheck(nil, parser.TypeString)
	if err != nil {
		return hlc.ZeroTimestamp, err
	}
	d, err := te.Eval(evalCtx)
	if err != nil {
		return hlc.ZeroTimestamp, err
	}
	switch d := d.(type) {
	case *parser.DString:
		// Allow nanosecond precision because the timestamp is only used by the
		// system and won't be returned to the user over pgwire.
		dt, err := parser.ParseDTimestamp(string(*d), time.Nanosecond)
		if err != nil {
			return hlc.ZeroTimestamp, err
		}
		ts.WallTime = dt.Time.UnixNano()
	case *parser.DInt:
		ts.WallTime = int64(*d)
	case *parser.DDecimal:
		// Format the decimal into a string and split on `.` to extract the nanosecond
		// walltime and logical tick parts.
		s := d.String()
		parts := strings.SplitN(s, ".", 2)
		nanos, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return hlc.ZeroTimestamp, errors.Wrap(err, "parse AS OF SYSTEM TIME argument")
		}
		var logical int64
		if len(parts) > 1 {
			// logicalLength is the number of decimal digits expected in the
			// logical part to the right of the decimal. See the implementation of
			// cluster_logical_timestamp().
			const logicalLength = 10
			p := parts[1]
			if lp := len(p); lp > logicalLength {
				return hlc.ZeroTimestamp, errors.Errorf("bad AS OF SYSTEM TIME argument: logical part has too many digits")
			} else if lp < logicalLength {
				p += strings.Repeat("0", logicalLength-lp)
			}
			logical, err = strconv.ParseInt(p, 10, 32)
			if err != nil {
				return hlc.ZeroTimestamp, errors.Wrap(err, "parse AS OF SYSTEM TIME argument")
			}
		}
		ts.WallTime = nanos
		ts.Logical = int32(logical)
	default:
		return hlc.ZeroTimestamp, fmt.Errorf("unexpected AS OF SYSTEM TIME argument: %s (%T)", d.ResolvedType(), d)
	}
	if max.Less(ts) {
		return hlc.ZeroTimestamp, fmt.Errorf("cannot specify timestamp in the future")
	}
	return ts, nil
}

// isAsOf analyzes a select statement to bypass the logic in newPlan(),
// since that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction should
// be set.
//
// max is a lower bound on what the transaction's timestamp will be. Used to
// check that the user didn't specify a timestamp in the future.
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

	ts, err := EvalAsOfTimestamp(&planMaker.evalCtx, sc.From.AsOf, max)
	return &ts, err
}

// SetTxnTimestamps sets the transaction's proto timestamps and deadline
// to ts. This is for use with AS OF queries, and should be called in the
// retry block (except in the case of prepare which doesn't use retry). The
// deadline-checking code checks that the `Timestamp` field of the proto
// hasn't exceeded the deadline. Since we set the Timestamp field each retry,
// it won't ever exceed the deadline, and thus setting the deadline here is
// not strictly needed. However, it doesn't do anything incorrect and it will
// possibly find problems if things change in the future, so it is left in.
func SetTxnTimestamps(txn *client.Txn, ts hlc.Timestamp) {
	txn.Proto.Timestamp = ts
	txn.Proto.OrigTimestamp = ts
	txn.Proto.MaxTimestamp = ts
	txn.UpdateDeadlineMaybe(ts)
}

// convertToErrWithPGCode recognizes errs that should have SQL error codes to be
// reported to the client and converts err to them. If this doesn't apply, err
// is returned.
// Note that this returns a new proto, and no fields from the original one are
// copied. So only use it in contexts where the only thing that matters in the
// response is the error detail.
// TODO(andrei): sqlbase.ConvertBatchError() seems to serve similar purposes, but
// it's called from more specialized contexts. Consider unifying the two.
func convertToErrWithPGCode(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*roachpb.RetryableTxnError); ok {
		return sqlbase.NewRetryError(err)
	}
	return err
}
