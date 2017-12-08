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
	"io"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/pkg/errors"
)

// A connExecutor is in charge of executing queries received on a given client
// connection. The connExecutor implements a state machine (dictated by the
// Postgres/pgwire session semantics). The state machine is supposed to run
// asynchronously wrt the client connection: it receives input statements
// through a stmtBuf and produces results through a clientComm interface. The
// connExecutor maintains a cursor over the statementBuffer and executes
// statements / produces results for one statement at a time. The cursor points
// at all times to the statement that the connExecutor is currently executing.
// Results for statements before the cursor have already been produced (but not
// necessarily delivered to the client). Statements after the cursor are queued
// for future execution. Keeping already executed statements in the buffer is
// useful in case of automatic retries (in which case statements from the
// retried transaction have to be executed again); the connExecutor is in charge
// of removing old statements that are no longer needed for retries from the
// (head of the) buffer. Separately, the implementer of the clientComm interface
// (e.g. the pgwire module) is in charge of keeping track of what results have
// been delivered to the client and what results haven't (yet).
//
// The connExecutor has two main responsibilities: to dispatch queries to the
// execution engine(s) and relay their results to the clientComm, and to
// implement the state machine maintaining the various aspects of a connection's
// state. The state machine implementation is further divided into two aspects:
// maintaining the transaction status of the connection (outside of a txn,
// inside a txn, in an aborted txn, in a txn awaiting client restart, etc.) and
// maintaining the cursor position (i.e. correctly jumping to whatever the
// "next" statement to execute is in various situations).
//
// The cursor normally advances one statement at a time, but it can also skip
// some statements (remaining statements in a query string are skipped once an
// error is encountered) and it can sometimes be rewound when performing
// automatic retries. Rewinding can only be done if results for the rewound
// statements have not actually been delivered to the client; see below.
//
//                                                   +---------------------+
//                                                   |connExecutor         |
//                                                   |                     |
//                                                   +->execution+--------------+
//                                                   ||  +                 |    |
//                                                   ||  |fsm.Event        |    |
//                                                   ||  |                 |    |
//                                                   ||  v                 |    |
//                                                   ||  fsm.Machine(TxnStateTransitions)
//                                                   ||  +  +--------+     |    |
//      +--------------------+                       ||  |  |txnState|     |    |
//      |stmtBuf             |                       ||  |  +--------+     |    |
//      |                    | statements are read   ||  |                 |    |
//      | +-+-+ +-+-+ +-+-+  +------------------------+  |                 |    |
//      | | | | | | | | | |  |                       |   |   +-------------+    |
//  +---> +-+-+ +++-+ +-+-+  |                       |   |   |session data |    |
//  |   |        ^           |                       |   |   +-------------+    |
//  |   |        |   +-----------------------------------+                 |    |
//  |   |        +   v       | cursor is advanced    |  advanceInfo        |    |
//  |   |       cursor       |                       |                     |    |
//  |   +--------------------+                       +---------------------+    |
//  |                                                                           |
//  |                                                                           |
//  +-------------+                                                             |
//                +--------+                                                    |
//                | parser |                                                    |
//                +--------+                                                    |
//                |                                                             |
//                |                                                             |
//                |                                   +----------------+        |
//        +-------+------+                            |execution engine<--------+
//        | pgwire conn  |               +------------+(local/DistSQL) |
//        |              |               |            +----------------+
//        |   +----------+               |
//        |   |clientComm<---------------+
//        |   +----------+           results are produced
//        |              |
//        +-------^------+
//                |
//                |
//        +-------+------+
//        | SQL client   |
//        +--------------+
//
// The connExecutor is disconnected from client communication (i.e. generally
// network communication - pgwire); the module doing client communication is
// responsible for pushing statements into the buffer and for providing an
// implementation of the clientConn interface (and thus consume results and send
// them to the client). The connExecutor does not control when results are
// delivered to the client, but still it does have some influence over that;
// this is because of the fact that the possibility of doing automatic retries
// goes away the moment results for the transaction in question are delivered to
// the client. The communication module has full freedom in sending results
// whenever it sees fit; however the connExecutor influences communication in
// the following ways:
//
// a) The connExecutor calls clientComm.flush(), informing the implementer that
// all the previous results can be delivered to the client at will.
//
// b) When deciding whether an automatic retry can be performed for a
// transaction, the connExecutor needs to:
//
//   1) query the communication status to check that no results for the txn have
//   been delivered to the client and, if this check passes:
//   2) lock the communication so that no further results are delivered to the
//   client, and, eventually:
//   3) rewind the clientComm to a certain position corresponding to the start
//   of the transaction, thereby discarding all the results that had been
//   accumulated for the previous attempt to run the transaction in question.
//
// These steps are all orchestrated through clientComm.lockCommunication() and
// rewindCapability{}.
//
// Implementation notes:
//
// --- Error handling ---
//
// The key to understanding how the connExecutor handles errors is understanding
// the fact that there's two distinct categories of errors to speak of. There
// are "query execution errors" and there are the rest. Most things fall in the
// former category: invalid queries, queries that fail constraints at runtime,
// data unavailability errors, retriable errors (i.e. serializability
// violations) "internal errors" (e.g. connection problems in the cluster). This
// category of errors don't represent dramatic events as far as the connExecutor
// is concerned: they produce "results" for the query to be passed to the client
// just like more successful queries do and they produce Events for the
// state machine just like the successful queries (the events in question
// are generally event{non}RetriableErr and they generally cause the
// state machine to move to the Aborted state, but the connExecutor doesn't
// concern itself with this). The way the connExecutor reacts to these errors is
// the same as how it reacts to a successful query completing: it moves the
// cursor over the incoming statements as instructed by the state machine and
// continues running statements.
//
// And then there's other errors that don't have anything to do with a
// particular query, but with the connExecutor itself. In other languages, these
// would perhaps be modeled as Exceptions: we want them to unwind the stack
// significantly. These errors cause the connExecutor.run() to break out of its
// loop and return an error. Example of such errors include errors in
// communication with the client (e.g. the network connection is broken) or the
// connection's context being canceled.
//
// All of connExecutor's methods only return errors for the 2nd category. Query
// execution errors are written to a StatementResult. Low-level methods don't
// operate on a StatementResult directly; instead they operate on a wrapper
// (resultWithStoredErr), which provides access to the query error for purposes
// of building the correct state machine event.
//
// --- Context management ---
//
// At the highest level, there's connExecutor.run() that takes a context. That
// context is supposed to represent "the connection's context": its lifetime is
// the client connection's lifetime and it is assigned to connEx.connCtx.
// Below that, every SQL transaction has its own derived context because that's
// the level at which we trace operations. The lifetime of SQL transactions is
// determined by the txnState: the state machine decides when transactions start
// and end in txnState.performStateTransition(). When we're inside a SQL
// transaction, most operations are considered to happen in the context of that
// txn. When there's no SQL transaction (i.e. stateNoTxn), everything happens in
// the connection's context.
// TODO(andrei): mention sessionEventf().
//
// High-level code in connExecutor is agnostic of whether it currently is inside
// a txn or not. To deal with both cases, such methods don't explicitly take a
// context; instead they use connEx.Ctx(), which returns the appropriate ctx
// based on the current state.
// Lower-level code (everything below connEx.execStmt() which dispatches based
// on the current state) knows what state its running in, and so the usual
// pattern of explicitly taking a context as an argument is used.

// Server is the top level singleton for handling SQL connections. It creates
// connExecutor's to server every incoming connection.
type Server struct {
	noCopy util.NoCopy

	cfg *ExecutorConfig

	// sqlStats tracks per-application statistics for all applications on each
	// node.
	sqlStats *sqlStats

	reCache *tree.RegexpCache

	// pool is the parent monitor for all session monitors
	// WIP(andrei): this needs to be pgwire.Server.sqlMemoryPool.
	pool *mon.BytesMonitor

	// memMetrics track memory usage by SQL execution.
	memMetrics *MemoryMetrics

	// EngineMetrics is exported as required by the metrics.Struct magic we use
	// for metrics registration.
	EngineMetrics sqlEngineMetrics

	// dbCache is a cache for database descriptors. It is updated in response to
	// system config updates. connExecutors get a reference to one version of the
	// cache and periodically refresh that reference.
	// TODO(andrei): I don't understand the benefits of this scheme very well -
	// databaseCache instance are shared; but the top-level databaseCacheHolder is
	// not. It has been ported over from who knows when. Rethink it.
	dbCache *databaseCacheHolder
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig) *Server {
	// WIP(andrei): take the stopper and listen to Gossip like the Executor does.
	return &Server{
		cfg: cfg,
		EngineMetrics: sqlEngineMetrics{
			DistSQLSelectCount: metric.NewCounter(MetaDistSQLSelect),
			// TODO(mrtracy): See HistogramWindowInterval in server/config.go for the 6x factor.
			DistSQLExecLatency: metric.NewLatency(MetaDistSQLExecLatency,
				6*metricsSampleInterval),
			SQLExecLatency: metric.NewLatency(MetaSQLExecLatency,
				6*metricsSampleInterval),
			DistSQLServiceLatency: metric.NewLatency(MetaDistSQLServiceLatency,
				6*metricsSampleInterval),
			SQLServiceLatency: metric.NewLatency(MetaSQLServiceLatency,
				6*metricsSampleInterval),
		},
		// dbCache will be updated on Start().
		dbCache: newDatabaseCacheHolder(newDatabaseCache(config.SystemConfig{})),
	}
}

// Start starts the Server's background processing.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	var wg sync.WaitGroup
	wg.Add(1)

	gossipUpdateC := s.cfg.Gossip.RegisterSystemConfigChannel()
	stopper.RunWorker(ctx, func(ctx context.Context) {
		var first bool
		for {
			select {
			case <-gossipUpdateC:
				sysCfg, _ := s.cfg.Gossip.GetSystemConfig()
				s.dbCache.updateSystemConfig(sysCfg)
				if first {
					first = false
					wg.Done()
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	// Wait for the databaseCache to be initialized. RegisterSystemConfigChannel()
	// fires immediately.
	wg.Wait()
}

// newConnExecutor creates a connExecutor tied to this Server.
//
// Args:
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
// 	 produce results for the client.
// reserved: An amount on memory reserved for the connection. The connExecutor
// 	 takes ownership of this memory.
// memMetrics: The metrics that statements executed on this connection will
//   contribute to.
func (s *Server) newConnExecutor(
	ctx context.Context,
	args SessionArgs,
	stmtBuf *StmtBuf,
	clientComm clientComm,
	reserved mon.BoundAccount,
	memMetrics *MemoryMetrics,
) *connExecutor {

	// Create the various monitors. They are Start()ed later.
	//
	// Note: we pass `reserved` to sessionRootMon where it causes it to act as a
	// buffer. This is not done for sessionMon nor state.mon: these monitors don't
	// start with any buffer, so they'll need to ask their "parent" for memory as
	// soon as the first allocation. This is acceptable because the session is
	// single threaded, and the point of buffering is just to avoid contention.
	sessionRootMon := mon.MakeMonitor("session root",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		-1, math.MaxInt64)
	sessionRootMon.Start(ctx, s.pool, reserved)
	sessionMon := mon.MakeMonitor("session",
		mon.MemoryResource,
		s.memMetrics.SessionCurBytesCount,
		s.memMetrics.SessionMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes)
	sessionMon.Start(ctx, &sessionRootMon, mon.BoundAccount{})

	// We merely prepare the txn monitor here. It is started in
	// txnState.resetForNewSQLTxn().
	txnMon := mon.MakeMonitor("txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes)

	settings := &s.cfg.Settings.SV
	distSQLMode := sessiondata.DistSQLExecMode(DistSQLClusterExecMode.Get(settings))

	ex := connExecutor{
		server:     s,
		stmtBuf:    stmtBuf,
		clientComm: clientComm,
		sessionData: sessiondata.SessionData{
			Database:      args.Database,
			DistSQLMode:   distSQLMode,
			SearchPath:    sqlbase.DefaultSearchPath,
			Location:      time.UTC,
			User:          args.User,
			SequenceState: sessiondata.NewSequenceState(),
		},
		dataMutator: sessionDataMutator{},
		state: txnState2{
			mon: &txnMon,
		},
		transitionCtx: transitionCtx{
			db:     s.cfg.DB,
			nodeID: s.cfg.NodeID.Get(),
			clock:  s.cfg.Clock,
			// The transaction's monitor inherits from sessionRootMon.
			connMon: &sessionRootMon,
			tracer:  s.cfg.AmbientCtx.Tracer,
		},
		parallelizeQueue: MakeParallelizeQueue(NewSpanBasedDependencyAnalyzer()),
		memMetrics:       memMetrics,
		extraTxnState: transactionState{
			tables: TableCollection{
				leaseMgr:      s.cfg.LeaseManager,
				databaseCache: s.dbCache.getDatabaseCache(),
			},
			schemaChangers: schemaChangerCollection{},
		},
	}
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.dataMutator = sessionDataMutator{
		data: &ex.sessionData,
		defaults: sessionDefaults{
			applicationName: args.ApplicationName,
			database:        args.Database,
		},
		settings:       s.cfg.Settings,
		curTxnReadOnly: &ex.state.readOnly,
		sessionTracing: &SessionTracing{},
		applicationNameChanged: func(newName string) {
			if ex.server.sqlStats != nil {
				ex.appStats = ex.server.sqlStats.getStatsForApplication(newName)
			}
		},
	}

	return &ex
}

// WIP(andrei)
var _ = Server{}

type connExecutor struct {
	noCopy util.NoCopy

	// The server to which this connExecutor is attached. The reference is used
	// for getting access to configuration settings and metrics.
	server *Server

	// The buffer with incoming statements to execute.
	stmtBuf *StmtBuf
	// The interface for communicating statement results to the client.
	clientComm clientComm
	// Finity "the machine" Automaton is the state machine controlling the state
	// below.
	machine fsm.Machine
	// state encapsulates fields related to the ongoing SQL txn. It is mutated as
	// the machine's ExtendedState.
	state txnState2

	// extraTxnState groups fields scoped to a SQL txn that are not handled by
	// ex.state, above. The rule of thumb is that, if the state influences state
	// transitions, it should live in state, otherwise it can live here.
	// This is only used in the Open state. extraTxnState is reset whenever a
	// transaction finishes or gets retried.
	extraTxnState transactionState

	// txnStartPos is the position within stmtBuf at which the current SQL
	// transaction started. If we're going to perform an automatic txn retry, this
	// is the position from which we'll start executing again.
	// The value is only defined while the connection is in the stateOpen.
	//
	// Set via setTxnStartPos().
	txnStartPos cmdPos

	// sessionData contains the user-configurable connection variables.
	sessionData sessiondata.SessionData
	dataMutator sessionDataMutator

	// connCtx is the root context in which all statements from the connection are
	// running. This generally should not be used directly, but through the Ctx()
	// method; if we're inside a transaction, Ctx() is going to return a derived
	// context. See the Context Management comments at the top of the file.
	connCtx context.Context

	// planner is the "default planner" on a session, to save planner allocations
	// during serial execution. Since planners are not threadsafe, this is only
	// safe to use when a statement is not being parallelized. It must be reset
	// before using.
	planner planner

	// parallelizeQueue is a queue managing all parallelized SQL statements
	// running on this connection.
	parallelizeQueue ParallelizeQueue

	// mon tracks memory usage for SQL activity within this session. It
	// is not directly used, but rather indirectly used via sessionMon
	// and state.mon. sessionMon tracks session-bound objects like prepared
	// statements and result sets.
	//
	// The reason why state.mon and mon are split is to enable
	// separate reporting of statistics per transaction and per
	// session. This is because the "interesting" behavior w.r.t memory
	// is typically caused by transactions, not sessions. The reason why
	// sessionMon and mon are split is to enable separate reporting of
	// statistics for result sets (which escape transactions).
	mon        mon.BytesMonitor
	sessionMon mon.BytesMonitor
	// memMetrics contains the metrics that statements executed on this connection
	// will contribute to.
	memMetrics *MemoryMetrics

	transitionCtx transitionCtx

	// appStats tracks per-application SQL usage statistics. It is maintained to
	// represent statistrics for the application currently identified by
	// sessiondata.ApplicationName.
	appStats *appStats

	//
	// Per-session statistics.
	//

	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		//
		// Session parameters, user-configurable.
		//

		// ApplicationName is the name of the application running the current
		// session. This can be used for logging and per-application statistics.
		// Change via resetApplicationName().
		ApplicationName string

		//
		// State structures for the logical SQL session.
		//

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[uint128.Uint128]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement
	}
}

// transactionState groups state that's specific to a SQL transaction. They all
// need to be reset if that transaction is committed, rolled-back or retried.
type transactionState struct {
	// tables collects descriptors used by the current transaction.
	// TODO(andrei, vivek): I think this TableCollection guy should go away, or at
	// least only contain tables *mutated* in the current txn. I don't understand
	// what purpose it serves beyond that.
	tables TableCollection

	// schemaChangers accumulate schema changes staged for execution. Staging
	// happens when executing DDL statements. The staged changes are executed once
	// the transaction that staged them commits (which is once the DDL statement
	// is done if the statement was executed in an implicit txn).
	schemaChangers schemaChangerCollection

	// autoRetryCounter keeps track of the which iteration of a transaction
	// auto-retry we're currently in. It's 0 whenever the transaction state is not
	// stateOpen.
	autoRetryCounter int
}

// reset wipes the transactionState.
func (ts *transactionState) reset(ctx context.Context, ev txnEvent, dbCache *databaseCache) {
	ts.schemaChangers.reset()

	var opt releaseOpt
	if ev == txnCommit {
		opt = blockForDBCacheUpdate
	} else {
		opt = dontBlockForDBCacheUpdate
	}
	ts.tables.releaseTables(ctx, opt)
	ts.tables.databaseCache = dbCache

	ts.autoRetryCounter = 0
}

func (ex *connExecutor) Ctx() context.Context {
	if _, ok := ex.machine.CurState().(stateNoTxn); ok {
		return ex.connCtx
	}
	return ex.state.Ctx
}

// run implements the run loop for a connExecutor. Commands are read one by one
// from the input buffer; they are executed and the resulting state transitions
// are performed.
//
// run returns when either the stmtBuf is closed by someone else or when an
// error is propagated from query execution. Note that query errors are not
// propagated as errors to this layer; only things that are supposed to
// terminate the session are (e.g. client communication errors and ctx
// cancelations).
// run() is expected to react on ctx cancelation, but the caller needs to also
// close the stmtBuf at the same time as canceling the ctx. If cancelation
// happens in the middle of a query execution, that's expected to interrupt the
// execution and generate an error. run() is then supposed to return because the
// buffer is closed and no further commands can be read.
func (ex *connExecutor) run(ctx context.Context) error {
	defer ex.extraTxnState.reset(ctx, txnAborted, nil /* dbCache */)

	ex.connCtx = ctx
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		cmd, pos, err := ex.stmtBuf.curCmd(ex.Ctx())
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		exec, ok := cmd.(ExecStmt)
		if !ok {
			panic("not implemented")
		}

		// Execute the statement.
		res := ex.clientComm.createStatementResult(exec.Stmt, pos)
		ev, payload, err := ex.execStmt(Statement{AST: exec.Stmt}, res, nil /* pinfo */, pos)
		if err != nil {
			return err
		}

		var advInfo advanceInfo

		// If an event was generated, feed it to the state machine.
		if ev != nil {
			var err error
			advInfo, err = ex.txnStateTransitionsApplyWrapper(ctx, ev, payload, res)
			if err != nil {
				return err
			}
		} else {
			// If no event was generated synthesize an advance code.
			_, inOpen := ex.machine.CurState().(stateOpen)
			advInfo = advanceInfo{
				code:  advanceOne,
				flush: !inOpen,
			}
		}

		// Close the result. In case of an execution error, the result might have
		// its error set already or it might not.
		pe, ok := payload.(payloadWithError)
		if res.Err() == nil && ok {
			if advInfo.code == stayInPlace {
				return errors.Errorf("unexpected stayInPlace code with err: %s", pe.errorCause())
			}
			if res.Err() == nil {
				res.CloseWithErr(pe.errorCause())
			}
		} else {
			res.Close()
		}

		if _, inOpen := ex.machine.CurState().(stateOpen); !inOpen && !advInfo.flush {
			return errors.Errorf("expected flush to be set when in state: %#v", ex.machine.CurState())
		}

		// Move the cursor according to what the state transition told us to do.
		switch advInfo.code {
		case advanceOne:
			ex.stmtBuf.advanceOne(ex.Ctx())
		case skipBatch:
			if err := ex.stmtBuf.seekToNextBatch(ex.Ctx()); err != nil {
				return err
			}
		case rewind:
			advInfo.rewCap.rewindAndUnlock(ex.Ctx())
		case stayInPlace:
			// Nothing to do. The same statement will be executed again.
		default:
			log.Fatalf(ex.Ctx(), "unexpected advance code: %s", advInfo.code)
		}

		// If flush is set, we reset the point to which future rewinds will refer.
		if advInfo.flush {
			if err := ex.clientComm.flush(); err != nil {
				return err
			}
			// Future rewinds will refer to the next position; the flushed statement
			// cannot be rewound to.
			ex.setTxnStartPos(ex.Ctx(), pos+1)
		}
	}
}

type descriptorsPolicy int

const (
	useCachedDescs descriptorsPolicy = iota
	disallowCachedDescs
)

// execStmt executes one statement by dispatching according to the current
// state. Returns an Event to be passed to the state machine, or nil if no
// transition is needed. If nil is returned, then the cursor is supposed to
// advance to the next statement.
//
// If an error is returned, the session is supposed to be considered done. Query
// execution errors are not returned explicitly and they're also not
// communicated to the client. Instead they're incorporated in the returned
// event (the returned payload will implement payloadWithError). It is the
// caller's responsibility to deliver execution errors to the client.
//
// Args:
// stmt: The statement to execute.
// res: Used to produce query results.
// pinfo: The placeholders to use in the statements.
// pos: The position of stmt.
func (ex *connExecutor) execStmt(
	stmt Statement, res RestrictedCommandResult, pinfo *tree.PlaceholderInfo, pos cmdPos,
) (fsm.Event, fsm.EventPayload, error) {
	// Run SHOW TRANSACTION STATUS in a separate code path; it's execution does
	// not depend on the current transaction state.
	if _, ok := stmt.AST.(*tree.ShowTransactionStatus); ok {
		err := ex.runShowTransactionState(res)
		return nil, nil, err
	}

	queryID := ex.generateQueryID()
	stmt.queryID = queryID

	// Dispatch the statement for execution based on the current state.
	var ev fsm.Event
	var payload fsm.EventPayload
	var err error
	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		ev, payload = ex.execStmtInNoTxnState(ex.Ctx(), stmt)
	case stateOpen:
		ev, payload, err = ex.execStmtInOpenState(ex.Ctx(), stmt, pinfo, res)
		// If the execution generated an event, we short-circuit here. Otherwise,
		// if we're told that nothing exceptional happened, we have further loose
		// ends to tie.
		if err != nil || ev != nil {
			break
		}
		var autoCommitErr error
		if ex.machine.CurState().(stateOpen).ImplicitTxn.Get() {
			autoCommitErr = ex.handleAutoCommit(stmt.AST)
		}
		if autoCommitErr != nil {
			ev, payload = ex.handleSerializablePushMaybe(ex.Ctx(), stmt.AST)
		}
		if ev == nil {
			if payload != nil {
				panic("nil event but payload")
			}
			ev = eventTxnFinish{}
			payload = eventTxnFinishPayload{commit: true}
		}
		return ev, payload, nil

	case stateAborted, stateRestartWait:
		ev, payload = ex.execStmtInAbortedState(stmt, res)
	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	if filter := ex.server.cfg.TestingKnobs.StatementFilter2; filter != nil {
		if err := filter(ex.Ctx(), stmt.String(), ev); err != nil {
			return nil, nil, err
		}
	}
	return ev, payload, err
}

// runShowTransactionState returns the state of current transaction.
// res is closed.
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(res RestrictedCommandResult) error {
	res.SetColumns(sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

	// WIP(andrei): figure out what to print here
	state := fmt.Sprintf("%s", ex.machine.CurState())
	return res.AddRow(ex.Ctx(), tree.Datums{tree.NewDString(state)})
}

// generateQueryID generates a unique ID for a query based on the node's
// ID and its current HLC timestamp.
func (ex *connExecutor) generateQueryID() uint128.Uint128 {
	timestamp := ex.server.cfg.Clock.Now()

	loInt := (uint64)(ex.server.cfg.NodeID.Get())
	loInt = loInt | ((uint64)(timestamp.Logical) << 32)

	return uint128.FromInts((uint64)(timestamp.WallTime), loInt)
}

// addActiveQuery adds a running query to the list of running queries.
//
// It returns a cleanup function that needs to be run when the query is no
// longer "running": i.e. after it has finished execution and its results have
// been delivered to the client.
func (ex *connExecutor) addActiveQuery(
	queryID uint128.Uint128, stmt tree.Statement, cancelFun context.CancelFunc,
) func() {

	_, hidden := stmt.(tree.HiddenFromShowQueries)
	qm := queryMeta{
		start:         ex.phaseTimes[sessionEndParse],
		stmt:          stmt,
		phase:         preparing,
		isDistributed: false,
		ctxCancel:     cancelFun,
		hidden:        hidden,
	}
	ex.mu.Lock()
	ex.mu.ActiveQueries[queryID] = &qm
	ex.mu.Unlock()
	return func() {
		ex.mu.Lock()
		_, ok := ex.mu.ActiveQueries[queryID]
		if !ok {
			ex.mu.Unlock()
			panic(fmt.Sprintf("query %d missing from ActiveQueries", queryID))
		}
		delete(ex.mu.ActiveQueries, queryID)
		ex.mu.Unlock()
	}
}

// setTxnStartPos updates the position to which future rewinds will refer.
//
// All statements with lower position in stmtBuf (if any) are removed, as we
// won't ever need them again.
func (ex *connExecutor) setTxnStartPos(ctx context.Context, pos cmdPos) {
	if pos <= ex.txnStartPos {
		log.Fatalf(ctx, "can only move the  txnStartpos forward."+
			"Was: %d; new value: %d", ex.txnStartPos, pos)
	}
	ex.txnStartPos = pos
	ex.stmtBuf.ltrim(ctx, pos)
}

// getRewindTxnCapability checks whether rewinding to the position previously
// set through setTxnStartPos() is possible and, if it is, returns a
// rewindCapability bound to that position. The returned bool is true if the
// rewind is possible. If it is, client communication is blocked until the
// rewindCapability is exercised.
func (ex *connExecutor) getRewindTxnCapability() (rewindCapability, bool) {
	cl := ex.clientComm.lockCommunication()

	// If we already delivered results at or past the start position, we can't
	// rewind.
	if cl.clientPos() >= ex.txnStartPos {
		cl.Close()
		return rewindCapability{}, false
	}
	return rewindCapability{
		cl:        cl,
		buf:       ex.stmtBuf,
		rewindPos: ex.txnStartPos,
	}, true
}

// isCommit returns true if stmt is a "COMMIT" statement.
func isCommit(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.CommitTransaction)
	return ok
}

// makeErrEvent takes an error and returns either an eventRetriableErr or an
// eventNonRetriableErr, depending on the error type.
func (ex *connExecutor) makeErrEvent(err error, stmt tree.Statement) (fsm.Event, fsm.EventPayload) {
	retErr, retriable := err.(*roachpb.HandledRetryableTxnError)
	if retriable {
		if _, inOpen := ex.machine.CurState().(stateOpen); !inOpen {
			log.Fatalf(ex.Ctx(), "retriable error in unexpected state: %#v",
				ex.machine.CurState())
		}
	}
	if retriable && ex.state.mu.txn.IsRetryableErrMeantForTxn(*retErr) {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		ev := eventRetriableErr{
			IsCommit:     fsm.FromBool(isCommit(stmt)),
			CanAutoRetry: fsm.FromBool(canAutoRetry),
		}
		payload := eventRetriableErrPayload{
			err:    err,
			rewCap: rc,
		}
		return ev, payload
	}
	ev := eventNonRetriableErr{
		IsCommit: fsm.FromBool(isCommit(stmt)),
	}
	payload := eventNonRetriableErrPayload{err: err}
	return ev, payload
}

// handleAutoCommit commits the KV transaction if it hasn't been committed
// already.
//
// It's possible that the statement constituting the implicit txn has already
// committed it (in case it tried to run as a 1PC). This method detects that
// case.
// NOTE(andrei): It bothers me some that we're peeking at txn to figure out
// whether we committed or not, where SQL could already know that - individual
// statements could report this back through the Event.
//
// Args:
// stmt: The statement that we just ran.
func (ex *connExecutor) handleAutoCommit(stmt tree.Statement) error {
	_, inOpen := ex.machine.CurState().(stateOpen)
	if !inOpen {
		log.Fatalf(ex.Ctx(), "handleAutoCommit called in state: %#v",
			ex.machine.CurState())
	}
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		return nil
	}
	var skipCommit bool
	var err error
	if knob := ex.server.cfg.TestingKnobs.BeforeAutoCommit; knob != nil {
		err = knob(ex.Ctx(), stmt.String())
		skipCommit = err != nil
	}
	if !skipCommit {
		err = txn.Commit(ex.Ctx())
	}
	log.Eventf(ex.Ctx(), "AutoCommit. err: %v", err)
	if err != nil {
		return err
	}
	return nil
}

// handleSerializablePushMaybe takes action in some cases when a serializable
// transaction is detected to have been pushed - it generates a
// eventRetriableErr. Otherwise it returns nil.
//
// Generally, if the transaction is serializable and it has been pushed, we
// don't deal with the push until commit time (when we return a retriable error
// to the client). This is because we want the transaction to continue and lay
// down all the intents it wants, so that a future attempt is more likely to
// succeed. However, there are two situations when we do want to detect the push
// and restart early:
// 1. If we can still automatically retry the txn, we go ahead and retry now -
// if we'd execute further statements, we probably wouldn't be allowed to retry
// automatically any more.
// 2. If the client is not following the client-directed retries protocol, then
// technically there won't be any retries, so the client would not benefit from
// letting the transaction run more statements.
//
// This is only supposed to be called if we're in the Open state and we're not
// already transitioning away from that state for some other reason.
func (ex *connExecutor) handleSerializablePushMaybe(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	os, ok := ex.machine.CurState().(stateOpen)
	if !ok {
		log.Fatalf(ex.Ctx(),
			"handleSerializablePushMaybe called in state: %#v", ex.machine.CurState())
	}

	// rewCap ownership will be passed to the returned event.
	var rewCap *rewindCapability
	rc, canAutoRetry := ex.getRewindTxnCapability()
	if !(canAutoRetry || !os.RetryIntent.Get()) {
		return nil, nil
	}
	rewCap = &rc
	defer func() {
		if rewCap != nil {
			rewCap.close()
		}
	}()

	// Flush the parallel execution queue.
	// If we just ran a statement synchronously, then the parallel queue would
	// have been synchronized first, so this would be a no-op.  However, if we
	// just ran a statement asynchronously, then the queue could still contain
	// statements executing. So if we're in a "serializable restart" state, we
	// synchronize to drain the rest of the stmts and clear the parallel batch's
	// error-set before restarting. If we did not drain the parallel stmts then
	// the txn.Proto().Restart() call below might cause them to write at the wrong
	// epoch.
	//
	// TODO(nvanbenschoten): like elsewhere, we should try to actively cancel
	// these parallel queries instead of just waiting for them to finish.
	if parErr := ex.synchronizeParallelStmts(ex.Ctx()); parErr != nil {
		// If synchronizing results in a non-retriable error, it takes priority.
		if _, ok := parErr.(*roachpb.HandledRetryableTxnError); !ok {
			return ex.makeErrEvent(parErr, stmt)
		}
		log.VEventf(ex.Ctx(), 2, "parallel execution queue returned retriable error, "+
			"which will be swallowed because the transaction has been pushed anyway: %s",
			parErr)

	}

	ex.state.mu.txn.Proto().Restart(
		0 /* userPriority */, 0 /* upgradePriority */, ex.server.cfg.Clock.Now())
	injectedErr := roachpb.NewHandledRetryableTxnError(
		"serializable transaction timestamp pushed (detected by connExecutor)",
		ex.state.mu.txn.ID(),
		// No updated transaction required; we've already manually updated our
		// client.Txn.
		roachpb.Transaction{},
	)
	ev := eventRetriableErr{
		CanAutoRetry: fsm.True,
	}
	payload := eventRetriableErrPayload{
		err: injectedErr,
		// We're passing the rewCap to the caller.
		rewCap: *rewCap,
	}
	rewCap = nil
	return ev, payload
}

// synchronizeParallelStmts waits for all statements in the parallelizeQueue to
// finish. If errors are seen in the parallel batch, we attempt to turn these
// errors into a single error we can send to the client. We do this by prioritizing
// non-retryable errors over retryable errors.
// Note that the returned error is to always be considered a "query execution
// error". This means that it should never interrupt the connection.
func (ex *connExecutor) synchronizeParallelStmts(ctx context.Context) error {
	if errs := ex.parallelizeQueue.Wait(); len(errs) > 0 {
		ex.state.mu.Lock()
		defer ex.state.mu.Unlock()

		// Check that all errors are retryable. If any are not, return the
		// first non-retryable error.
		var retryErr *roachpb.HandledRetryableTxnError
		for _, err := range errs {
			switch t := err.(type) {
			case *roachpb.HandledRetryableTxnError:
				// Ignore retryable errors to previous incarnations of this transaction.
				curTxn := ex.state.mu.txn.Proto()
				errTxn := t.Transaction
				if errTxn.ID == curTxn.ID && errTxn.Epoch == curTxn.Epoch {
					retryErr = t
				}
			case *roachpb.TxnPrevAttemptError:
				// Symptom of concurrent retry, ignore.
			default:
				return err
			}
		}

		if retryErr == nil {
			log.Fatalf(ctx, "found symptoms of a concurrent retry, but did "+
				"not find the final retry error: %v", errs)
		}

		// If all errors are retryable, we return the one meant for the current
		// incarnation of this transaction. Before doing so though, we need to bump
		// the transaction epoch to invalidate any writes performed by any workers
		// after the retry updated the txn's proto but before we synchronized (some
		// of these writes might have been performed at the wrong epoch). Note
		// that we don't need to lock the client.Txn because we're synchronized.
		// See #17197.
		ex.state.mu.txn.Proto().BumpEpoch()
		return retryErr
	}
	return nil
}

// setTransactionModes implements the txnModesSetter interface.
func (ex *connExecutor) setTransactionModes(modes tree.TransactionModes) error {
	// Transform the transaction options into the types needed by the state
	// machine.
	pri, err := priorityToProto(modes.UserPriority)
	if err != nil {
		return err
	}
	iso, err := ex.isolationToProto(modes.Isolation)
	if err != nil {
		return err
	}

	// We cheat here and we manipulate ex.state directly, not through an event.
	// The alternative would be to create a special event, but it's unclear how
	// that'd work given that this method is called while executing a statement.
	if err := ex.state.setPriority(pri); err != nil {
		return err
	}
	if err := ex.state.setIsolationLevel(iso); err != nil {
		return err
	}
	return ex.state.setReadOnlyMode(modes.ReadWriteMode)
}

func (ex *connExecutor) isolationToProto(mode tree.IsolationLevel) (enginepb.IsolationType, error) {
	var iso enginepb.IsolationType
	switch mode {
	case tree.UnspecifiedIsolation:
		iso = ex.sessionData.DefaultIsolationLevel
	case tree.SnapshotIsolation:
		iso = enginepb.SNAPSHOT
	case tree.SerializableIsolation:
		iso = enginepb.SERIALIZABLE
	default:
		return enginepb.IsolationType(0), errors.Errorf("unknown isolation level: %s", mode)
	}
	return iso, nil
}

func priorityToProto(mode tree.UserPriority) (roachpb.UserPriority, error) {
	var pri roachpb.UserPriority
	switch mode {
	case tree.UnspecifiedUserPriority:
		pri = roachpb.NormalUserPriority
	case tree.Low:
		pri = roachpb.MinUserPriority
	case tree.Normal:
		pri = roachpb.NormalUserPriority
	case tree.High:
		pri = roachpb.MaxUserPriority
	default:
		return roachpb.UserPriority(0), errors.Errorf("unknown user priority: %s", mode)
	}
	return pri, nil
}

func (ex *connExecutor) readWriteModeWithSessionDefault(
	mode tree.ReadWriteMode,
) tree.ReadWriteMode {
	if mode == tree.UnspecifiedReadWriteMode {
		if ex.sessionData.DefaultReadOnly {
			return tree.ReadOnly
		}
		return tree.ReadWrite
	}
	return mode
}

// execStmtInNoTxnState "executes" a statement when no transaction is in scope.
// For anything but BEGIN, this method doesn't actually execute the statement;
// it just returns an Event that will generate a transaction. The statement will
// then be executed again, but this time in the Open state (implicit txn).
//
// Note that eventTxnStart, which is generally returned by this method, causes
// the state to change and previous results to be flushed, but for implicit txns
// the cursor is not advanced. This means that the statement will run again in
// stateOpen, at each point its results will also be flushed.
func (ex *connExecutor) execStmtInNoTxnState(
	ctx context.Context, stmt Statement,
) (fsm.Event, fsm.EventPayload) {
	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		iso, err := ex.isolationToProto(s.Modes.Isolation)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		pri, err := priorityToProto(s.Modes.UserPriority)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}

		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				iso, pri,
				ex.server.cfg.Clock.PhysicalTime(),
				ex.transitionCtx,
				ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode))
	case *tree.CommitTransaction:
		goto statementNeedsExplicitTxn
	case *tree.ReleaseSavepoint:
		goto statementNeedsExplicitTxn
	case *tree.RollbackTransaction:
		goto statementNeedsExplicitTxn
	case *tree.SetTransaction:
		goto statementNeedsExplicitTxn
	case *tree.Savepoint:
		goto statementNeedsExplicitTxn
	default:
		mode := tree.ReadWrite
		if ex.sessionData.DefaultReadOnly {
			mode = tree.ReadOnly
		}
		return eventTxnStart{ImplicitTxn: fsm.True},
			makeEventTxnStartPayload(
				ex.sessionData.DefaultIsolationLevel,
				roachpb.NormalUserPriority,
				ex.server.cfg.Clock.PhysicalTime(),
				ex.transitionCtx,
				mode)
	}
statementNeedsExplicitTxn:
	return ex.makeErrEvent(errNoTransactionInProgress, stmt.AST)
}

// execStmtInOpenState executes one statement in the context of the session's
// current transaction.
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// directly and delegates everything else to the execution engines.
// It binds placeholders.
// Results and query execution errors are written to res.
//
// If an error is returned, the connection is supposed to be consider done.
// Query execution errors are not returned explicitly; they're incorporated in
// the returned Event.
//
// The caller is in charge of Close()ing res.
func (ex *connExecutor) execStmtInOpenState(
	ctx context.Context,
	stmt Statement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload, error) {
	os := ex.machine.CurState().(stateOpen)

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, stmt.AST)
		return ev, payload, nil
	}

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancellation function here.
	unregisterFun := ex.addActiveQuery(stmt.queryID, stmt.AST, ex.state.cancel)
	// We pass responsibility for unregistering to results delivery. We might take
	// the responsibility back later if it turns out that we're going to execute
	// this query through the parallelize queue.
	res.SetResultsDeliveredCallback(unregisterFun)

	// Check if the statement is parallelized or is independent from parallel
	// execution. If neither of these cases are true, we need to synchronize
	// parallel execution by letting it drain before we can begin executing stmt.
	parallelize := IsStmtParallelized(stmt)
	_, independentFromParallelStmts := stmt.AST.(tree.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := ex.synchronizeParallelStmts(ctx); err != nil {
			return makeErrEvent(err)
		}
	}

	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		// BEGIN is always an error when in the Open state. It's legitimate only in
		// the NoTxn state.
		return makeErrEvent(errTransactionInProgress)

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, stmt.AST)
		return ev, payload, nil

	case *tree.ReleaseSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return makeErrEvent(err)
		}
		// ReleaseSavepoint is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, stmt.AST)
		return ev, payload, nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.rollbackSQLTransaction(ctx)
		return ev, payload, nil

	case *tree.Savepoint:
		if err := tree.ValidateRestartCheckpoint(s.Name); err != nil {
			return makeErrEvent(err)
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction.
		if ex.state.mu.txn.GetTxnCoordMeta().CommandCount > 0 {
			err := fmt.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", tree.RestartSavepointName)
			return makeErrEvent(err)
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		return eventRetryIntentSet{}, nil, nil

	case *tree.RollbackToSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return makeErrEvent(err)
		}
		if !os.RetryIntent.Get() {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			return makeErrEvent(err)
		}

		res.ResetStmtType((*tree.Savepoint)(nil))
		return eventTxnRestart{}, nil, nil

		// !!! prepared statements are not supported yet
		// case *tree.Prepare:
		//   // This must be handled here instead of the common path below
		//   // because we need to use the Executor reference.
		//   var dummy_session *Session = nil // !!!
		//   if err := PrepareStmt(
		//     dummy_session, s, ex.server.dbCache, &ex.server.cfg, ex.server.reCache,
		//   ); err != nil {
		//     return makeErrEventAndCloseRes(err)
		//   }
		//   return makeNoTopLevelTransitionEvent(retryIntentUnknown, dontFlushResult), nil
		//
		// case *tree.Execute:
		//   // Substitute the placeholder information and actual statement with that of
		//   // the saved prepared statement and pass control back to the ordinary
		//   // execute path.
		//   ps, newPInfo, err := getPreparedStatementForExecute(session, s)
		//   if err != nil {
		//     return err
		//   }
		//   pinfo = newPInfo
		//   stmt.AST = ps.Statement
		//   stmt.ExpectedTypes = ps.Columns
		//   stmt.AnonymizedStr = ps.AnonymizedStr

	}

	var p *planner
	stmtTs := ex.server.cfg.Clock.PhysicalTime()
	// Only run statements asynchronously through the parallelize queue if the
	// statements are parallelized and we're in a transaction. Parallelized
	// statements outside of a transaction are run synchronously with mocked
	// results, which has the same effect as running asynchronously but
	// immediately blocking.
	runInParallel := parallelize && !os.ImplicitTxn.Get()
	if runInParallel {
		// Create a new planner since we're executing in parallel.
		p = ex.newPlanner(ex.state.mu.txn, stmtTs)
	} else {
		// We're not executing in parallel; we'll use the cached planner.
		p = &ex.planner
		ex.resetPlanner(p, ex.state.mu.txn, stmtTs)
	}

	if os.ImplicitTxn.Get() {
		ts, err := isAsOf(stmt.AST, p.EvalContext(), ex.server.cfg.Clock.Now())
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			p.asOfSystemTime = true
			p.avoidCachedDescriptors = true
			ex.state.mu.txn.SetFixedTimestamp(ctx, *ts)
		}
	}

	p.semaCtx.Placeholders.Assign(pinfo)
	ex.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt

	// TODO(andrei): Ideally we'd like to fork off a context for each individual
	// statement. But the heartbeat loop in TxnCoordSender currently assumes that
	// the context of the first operation in a txn batch lasts at least as long as
	// the transaction itself. Once that sender is able to distinguish between
	// statement and transaction contexts, we should move to per-statement
	// contexts.
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	// constantMemAcc accounts for all constant folded values that are computed
	// prior to any rows being computed.
	constantMemAcc := p.EvalContext().Mon.MakeBoundAccount()
	p.EvalContext().ActiveMemAcc = &constantMemAcc
	defer constantMemAcc.Close(ctx)

	if runInParallel {
		// We're passing unregisterFun to the parallel execution.
		res.SetResultsDeliveredCallback(nil)
		cols, err := ex.execStmtInParallel(ex.Ctx(), stmt, p, unregisterFun)
		if err != nil {
			return makeErrEvent(err)
		}
		// Produce mocked out results for the query - the "zero value" of the
		// statement's result type:
		// - tree.Rows -> an empty set of rows
		// - tree.RowsAffected -> zero rows affected
		if err := ex.initStatementResult(res, stmt, cols); err != nil {
			return makeErrEvent(err)
		}
	} else {
		p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit
		if err := ex.dispatchToExecutionEngine(ctx, stmt, p, res); err != nil {
			return nil, nil, err
		}
		if err := res.Err(); err != nil {
			ev, payload := ex.makeErrEvent(err, stmt.AST)
			return ev, payload, nil
		}
	}

	return nil, nil, nil
}

// commitSQLTransaction executes a COMMIT or RELEASE SAVEPOINT statement. The
// transaction is committed and the statement result is written to res. res is
// closed.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	if _, inOpen := ex.machine.CurState().(stateOpen); !inOpen {
		log.Fatalf(ctx, "commitSQLTransaction called in state: %#v", ex.machine.CurState())
	}

	commitType := commit
	if _, ok := stmt.(*tree.ReleaseSavepoint); ok {
		commitType = release
	}

	if err := ex.state.mu.txn.Commit(ex.Ctx()); err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	switch commitType {
	case commit:
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	case release:
		return eventTxnReleased{}, nil
	}
	panic("unreached")
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(ctx context.Context) (fsm.Event, fsm.EventPayload) {
	_, inOpen := ex.machine.CurState().(stateOpen)
	_, inRestartWait := ex.machine.CurState().(stateRestartWait)
	if !inOpen && !inRestartWait {
		log.Fatalf(ctx,
			"rollbackSQLTransaction called on txn in wrong state: %#v", ex.machine.CurState())
	}
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
}

// evalCtx creates an extendedEvalCtx corresponding to the current state of the
// session.
//
// p is the planner that the EvalCtx will link to. Note that the planner also
// needs to have a reference to an evalCtx, so the caller will have to set that
// up.
// stmtTs is the timestamp that the statement_timestamp() SQL builtin will
// return for statements executed with this evalCtx. Since generally each
// statement is supposed to have a different timestamp, the evalCtx generally
// shouldn't be reused across statements.
func (ex *connExecutor) evalCtx(p *planner, stmtTs time.Time) extendedEvalContext {
	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Planner:       p,
			StmtTimestamp: stmtTs,

			Txn:              ex.state.mu.txn,
			SessionData:      &ex.sessionData,
			ApplicationName:  ex.dataMutator.ApplicationName(),
			TxnState:         ex.getTransactionState(),
			TxnReadOnly:      ex.state.readOnly,
			TxnImplicit:      ex.implicitTxn(),
			Settings:         ex.server.cfg.Settings,
			CtxProvider:      ex,
			Mon:              ex.state.mon,
			TestingKnobs:     ex.server.cfg.EvalContextTestingKnobs,
			TxnTimestamp:     ex.state.sqlTimestamp,
			ClusterTimestamp: ex.state.mu.txn.OrigTimestamp(),
			ClusterID:        ex.server.cfg.ClusterID(),
			NodeID:           ex.server.cfg.NodeID.Get(),
			ReCache:          ex.server.reCache,
		},
		SessionMutator: ex.dataMutator,
		VirtualSchemas: ex.server.cfg.VirtualSchemas,
		Tracing:        &ex.state.tracing,
		StatusServer:   ex.server.cfg.StatusServer,
		MemMetrics:     ex.memMetrics,
		Tables:         &ex.extraTxnState.tables,
		ExecCfg:        ex.server.cfg,
		DistSQLPlanner: ex.server.cfg.DistSQLPlanner,
		TxnModesSetter: ex,
		SchemaChangers: &ex.extraTxnState.schemaChangers,
	}
}

// getTransactionState retrieves a text representation of the given state.
func (ex *connExecutor) getTransactionState() string {
	state := ex.machine.CurState()
	if os, ok := state.(stateOpen); ok {
		if os.ImplicitTxn.Get() {
			// If the statement reading the state is in an implicit transaction, then
			// we want to tell NoTxn to the client.
			state = stateNoTxn{}
		}
	}
	return state.(fmt.Stringer).String()
}
func (ex *connExecutor) implicitTxn() bool {
	state := ex.machine.CurState()
	os, ok := state.(stateOpen)
	return ok && os.ImplicitTxn.Get()
}

// newPlanner creates a planner inside the scope of the given Session. The
// statement executed by the planner will be executed in txn. The planner
// should only be used to execute one statement.
//
// txn can be nil.
func (ex *connExecutor) newPlanner(txn *client.Txn, stmtTs time.Time) *planner {
	p := &planner{}
	ex.resetPlanner(p, txn, stmtTs)
	return p
}

func (ex *connExecutor) resetPlanner(p *planner, txn *client.Txn, stmtTs time.Time) {
	p.statsCollector = ex.newStatsCollector()
	p.txn = txn
	p.stmt = nil
	p.cancelChecker = sqlbase.NewCancelChecker(ex.Ctx())

	p.semaCtx = tree.MakeSemaContext(ex.sessionData.User == security.RootUser)
	p.semaCtx.Location = &ex.sessionData.Location
	p.semaCtx.SearchPath = ex.sessionData.SearchPath

	p.extendedEvalCtx = ex.evalCtx(p, stmtTs)
	p.extendedEvalCtx.ClusterID = ex.server.cfg.ClusterID()
	p.extendedEvalCtx.NodeID = ex.server.cfg.NodeID.Get()
	p.extendedEvalCtx.ReCache = ex.server.reCache

	p.sessionDataMutator = ex.dataMutator
	// !!! p.preparedStatements = &s.PreparedStatements
	p.autoCommit = false
}

// execStmtInParallel executes a query asynchronously: the query will wait for
// all other currently executing async queries which are not independent, and
// then it will run.
// Note that planning needs to be done synchronously because it's needed by the
// query dependency analysis.
//
// A list of columns is returned for purposes of initializing the statement
// results. This will be nil if the query's result is of type "RowsAffected".
// If this method returns an error, the error is to be treated as a query
// execution error (in other words, it should be sent to the clients as part of
// the query's result, and the connection should be allowed to proceed with
// other queries).
//
// Args:
// unregisterQuery: A cleanup function to be called when the execution is done.
//
// TODO(nvanbenschoten): We do not currently support parallelizing distributed SQL
// queries, so this method can only be used with classical SQL.
func (ex *connExecutor) execStmtInParallel(
	ctx context.Context, stmt Statement, planner *planner, unregisterQuery func(),
) (sqlbase.ResultColumns, error) {
	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: planner.ExtendedEvalContext(),
		p:               planner,
	}

	if err := planner.makePlan(ctx, stmt); err != nil {
		return nil, err
	}

	// Prepare the result set, and determine the execution parameters.
	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = false
	ex.mu.Unlock()

	if err := ex.parallelizeQueue.Add(params, func() error {
		defer unregisterQuery()

		// TODO(andrei): this should really be a result writer implementation that
		// does nothing.
		bufferedWriter := newBufferedWriter(ex.sessionMon.MakeBoundAccount())
		res := bufferedWriter.NewCommandResult()
		if err := ex.initStatementResult(res, stmt, cols); err != nil {
			return err
		}

		if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
			ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), true /* isParallel */)
		}

		planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()
		err := ex.execWithLocalEngine(ctx, planner, stmt.AST.StatementType(), res)
		planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()
		recordStatementSummary(
			planner, stmt, false /* distSQLUsed*/, ex.extraTxnState.autoRetryCounter,
			bufferedWriter.RowsAffected(), err, &ex.server.EngineMetrics)
		if ex.server.cfg.TestingKnobs.AfterExecute != nil {
			ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), err)
		}

		bufferedWriter.Close()
		if err != nil {
			log.Warningf(ctx, "Connection error from the parallel queue. How can that "+
				"be when using the BufferedWriter? err: %s", err)
			// I think this can't happen; if it does, it's unclear how to react when a
			// BufferedWriter "connection" is toast.
			res.CloseWithErr(err)
			return err
		}
		bufferedWriter.Close()
		return res.Err()
	}); err != nil {
		return nil, err
	}

	return cols, nil
}

// dispatchToExecutionEngine executes the statement, writes the result to res
// and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned; it is
// expected that the caller will inspect res and react to query errors by
// producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, stmt Statement, planner *planner, res RestrictedCommandResult,
) error {

	planner.statsCollector.PhaseTimes()[plannerStartLogicalPlan] = timeutil.Now()
	err := planner.makePlan(ctx, stmt)
	planner.statsCollector.PhaseTimes()[plannerEndLogicalPlan] = timeutil.Now()
	if err != nil {
		res.SetError(err)
		return nil
	}
	defer planner.curPlan.close(ctx)

	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}
	if err := ex.initStatementResult(res, stmt, cols); err != nil {
		res.SetError(err)
		return nil
	}

	useDistSQL, err := shouldUseDistSQL(
		ctx, ex.sessionData.DistSQLMode, ex.server.cfg.DistSQLPlanner, planner, planner.curPlan.plan)
	if err != nil {
		res.SetError(err)
		return nil
	}

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = useDistSQL
	ex.mu.Unlock()

	if useDistSQL {
		err = ex.execWithDistSQLEngine(ctx, planner, stmt.AST.StatementType(), res)
	} else {
		err = ex.execWithLocalEngine(ctx, planner, stmt.AST.StatementType(), res)
	}
	planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()
	recordStatementSummary(
		planner, stmt, useDistSQL, ex.extraTxnState.autoRetryCounter,
		res.RowsAffected(), err, &ex.server.EngineMetrics,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), err)
	}

	if err != nil {
		res.SetError(err)
	}
	return nil
}

type terminalErr struct {
	error
}

// execWithLocalEngine runs a plan using the local (non-distributed) SQL
// engine.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
func (ex *connExecutor) execWithLocalEngine(
	ctx context.Context, planner *planner, stmtType tree.StatementType, res RestrictedCommandResult,
) error {
	// Create a BoundAccount to track the memory usage of each row.
	rowAcc := planner.extendedEvalCtx.Mon.MakeBoundAccount()
	planner.extendedEvalCtx.ActiveMemAcc = &rowAcc
	defer rowAcc.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &planner.extendedEvalCtx,
		p:               planner,
	}

	if err := startPlan(params, planner.curPlan.plan); err != nil {
		res.SetError(err)
		return nil
	}

	switch stmtType {
	case tree.RowsAffected:
		count, err := countRowsAffected(params, planner.curPlan.plan)
		if err != nil {
			res.SetError(err)
			return nil
		}
		res.IncrementRowsAffected(count)
		return nil
	case tree.Rows:
		return ex.forEachRow(params, planner.curPlan.plan, func(values tree.Datums) error {
			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					res.SetError(err)
					return nil
				}
			}
			return res.AddRow(ctx, values)
		})
	default:
		// WIP(andrei): The Executor is ignoring other stmt types. Was calling
		// startPlan() sufficient for them?
		panic(fmt.Sprintf("unsupported statement type: %s", stmtType))
	}
}

// exectWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned. If res has
// not had its error set, it is the caller's responsibility to close it.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context, planner *planner, stmtType tree.StatementType, res RestrictedCommandResult,
) error {
	recv := makeDistSQLReceiver(
		ctx, res, stmtType,
		ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			_ = ex.server.cfg.Clock.Update(ts)
		},
	)
	ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, planner.txn, planner.curPlan.plan, recv, planner.ExtendedEvalContext(),
	)
	return recv.commErr
}

// forEachRow calls the provided closure for each successful call to
// planNode.Next with planNode.Values, making sure to properly track memory
// usage.
// Errors generated by this method other than the errors coming from the closure
// are to be considered query errors; they need to be communicated to the client
// as query results. The contract for errors coming from the closure
// (independent of this method) is that if they are terminalErr's, then the
// connection is to be interrupted. Otherwise, they are also to be considered
// query errors.
func (ex *connExecutor) forEachRow(params runParams, p planNode, f func(tree.Datums) error) error {
	next, err := p.Next(params)
	for ; next; next, err = p.Next(params) {
		// If we're tracking memory, clear the previous row's memory account.
		if params.extendedEvalCtx.ActiveMemAcc != nil {
			params.extendedEvalCtx.ActiveMemAcc.Clear(params.ctx)
		}

		if err := f(p.Values()); err != nil {
			return err
		}
	}
	return err
}

func (ex *connExecutor) execStmtInAbortedState(
	stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	panic("!!! unimplemented")
}

func (ex *connExecutor) execStmtInCommitWaitState(
	stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	panic("!!! unimplemented")
}

// txnStateTransitionsApplyWrapper is a wrapper on top of Machine built with the
// TxnStateTransitions above. Its point is to detect when we go in and out of
// transaction and update some state.
//
// Any returned error indicates an unrecoverable error for the session;
// execution on this connection should be interrupted.
func (ex *connExecutor) txnStateTransitionsApplyWrapper(
	ctx context.Context, ev fsm.Event, payload fsm.EventPayload, res CommandResult,
) (advanceInfo, error) {

	implicitTxn := ex.machine.CurState().(stateOpen).ImplicitTxn.Get()

	err := ex.machine.ApplyWithPayload(ctx, ev, payload)
	// TODO(andrei): panic on missing state transition errors
	if err != nil {
		return advanceInfo{}, err
	}

	advInfo := ex.state.consumeAdvanceInfo()

	if advInfo.code == rewind {
		ex.extraTxnState.autoRetryCounter++
	}

	// Handle transaction events which cause updates to txnState.
	switch advInfo.txnEvent {
	case noEvent:
		break
	case txnCommit:
		// We got a schema change error. We'll return it to the client as the result
		// of the current statement - which is either the DDL statement or a COMMIT
		// statement if the DDL was part of an explicit transaction. In the explicit
		// transaction case, we return a funky error code to the client to seed fear
		// about what happened to the transaction. The reality is that the
		// transaction committed, but at least some of the staged schema changes
		// failed. We don't have a good way to indicate this.
		//
		// TODO(andrei): figure out how session tracing should interact with schema
		// changes.
		if schemaChangeErr := ex.extraTxnState.schemaChangers.execSchemaChanges(
			ctx, ex.server.cfg,
		); schemaChangeErr != nil {
			if implicitTxn {
				res.SetError(schemaChangeErr)
				return advInfo, nil
			}
			res.SetError(sqlbase.NewStatementCompletionUnknownError(schemaChangeErr))
		}
		fallthrough
	case txnRestart:
		fallthrough
	case txnAborted:
		ex.extraTxnState.reset(ex.Ctx(), advInfo.txnEvent, ex.server.dbCache.getDatabaseCache())
	default:
		return advanceInfo{}, errors.Errorf("unexpected event: %v", advInfo.txnEvent)
	}

	return advInfo, nil
}

// initStatementResult initializes res according to a query.
//
// cols represents the columns of the result rows. Should be nil if
// stmt.AST.StatementType() != tree.Rows.
//
// If an error is returned, it is to be considered a query execution error.
func (ex *connExecutor) initStatementResult(
	res RestrictedCommandResult, stmt Statement, cols sqlbase.ResultColumns,
) error {
	res.SetColumns(cols)
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			return err
		}
	}
	return nil
}

// newStatsCollector returns an sqlStatsCollector that will record stats in the
// session's stats containers.
func (ex *connExecutor) newStatsCollector() sqlStatsCollector {
	return newSQLStatsCollectorImpl(ex.server.sqlStats, ex.appStats, ex.phaseTimes)
}
