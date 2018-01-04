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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
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
// transaction, most of all operations are considered to happen in the context
// of that txn. When there's no SQL transaction (i.e. stateNoTxn), everything
// happens in the connection's context.
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

	cfg ExecutorConfig

	distSQLPlanner *DistSQLPlanner

	// sqlStats tracks per-application statistics for all
	// applications on each node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics.
	appStats *appStats

	reCache *tree.RegexpCache
	dbCache *databaseCache

	// pool is the parent monitor for all session monitors
	// WIP(andrei): this needs to be pgwire.Server.sqlMemoryPool.
	pool *mon.BytesMonitor

	// memMetrics track memory usage by SQL execution.
	memMetrics *MemoryMetrics

	// EngineMetrics is exported as required by the metrics.Struct magic we use
	// for metrics registration.
	EngineMetrics sqlEngineMetrics
}

// NewServer creates a new Server.
func NewServer(cfg ExecutorConfig) *Server {
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
	}
}

// newConnExecutor creates a connExecutor tied to this Server.
//
// Args:
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
// 	 produce results for the client.
// reserved: An amount on memory reserved for the connection. The connExecutor
// 	 takes ownership of this memory.
func (s *Server) newConnExecutor(
	ctx context.Context, stmtBuf *stmtBuf, clientComm clientComm, reserved mon.BoundAccount,
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
		s.memMetrics.CurBytesCount,
		s.memMetrics.MaxBytesHist,
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
		s.memMetrics.TxnCurBytesCount,
		s.memMetrics.TxnMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes)

	ex := connExecutor{
		server:     s,
		stmtBuf:    stmtBuf,
		clientComm: clientComm,
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
		distSQLPlanner:   s.distSQLPlanner,
	}
	ex.transitionCtx.defaultIsolationLevel = &ex.sessionData.DefaultIsolationLevel
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)
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
	stmtBuf *stmtBuf
	// The interface for communicating statement results to the client.
	clientComm clientComm
	// Finity "the machine" Automaton is the state machine controlling the state
	// below.
	machine fsm.Machine
	// state encapsulates fields related to the ongoing SQL txn. It is mutated as
	// the machine's ExtendedState.
	state txnState2

	// txnStartPos is the position within stmtBuf at which the current SQL
	// transaction started. If we are to perform an automatic txn retry, that's
	// the position from which we need to start executing again.
	// The value is only defined while the connection is in the stateOpen.
	//
	// Set via setTxnStartPos().
	txnStartPos cursorPosition
	// expBeginPos is the position of an expected BEGIN statement. BEGIN
	// statements are allowed to execute in the Open state only at this position.
	// This is set when executing a BEGIN in state NoTxn and is used to
	// differentiate between the BEGIN that's executed again in the Open state
	// and subsequent BEGINs that need to be rejected.
	expBeginPos cursorPosition

	// sessionData contains the user-configurable connection variables.
	sessionData SessionData

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

	// distSQLPlanner plans and executes queries when using the DistSQL execution
	// engine.
	distSQLPlanner *DistSQLPlanner

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

	transitionCtx transitionCtx

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

		// ApplicationName is the name of the application running the
		// current session. This can be used for logging and per-application
		// statistics. Change via resetApplicationName().
		ApplicationName string

		//
		// State structures for the logical SQL session.
		//

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[uint128.Uint128]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement

		// sequenceState stores state related to calls to sequence
		// builtins currval(), nextval(), and lastval().
		SequenceState sequenceState
	}

	//
	// Testing state.
	//

	// If set, called after the Session is done executing the current SQL statement.
	// It can be used to verify assumptions about how metadata will be asynchronously
	// updated. Note that this can overwrite a previous callback that was waiting to be
	// verified, which is not ideal.
	// WIP(andrei): this is not used right now.
	testingVerifyMetadataFn func(config.SystemConfig) error
	verifyFnCheckedOnce     bool
}

func (ex *connExecutor) Ctx() context.Context {
	if _, ok := ex.machine.GetCurState().(stateNoTxn); ok {
		return ex.connCtx
	}
	return ex.state.Ctx
}

// run implements the run loop for a connExecutor. Statements are read one by
// one from the input buffer, executed and the resulting state transitions are
// performed.
//
// run returns when either the stmtBuf is closed by someone else or when an
// error is propagated from query execution. Note that query errors are not
// propagated as errors to this layer; only things that are supposed to
// terminate the session are (e.g. client communication errors and ctx
// cancelations).
// If ctx is canceled while a statement is being executed, that's expected to
// interrupt the statement execution; this method will then supposed to detect the
// cancelation and return an error.
func (ex *connExecutor) run(ctx context.Context) error {
	ex.connCtx = ctx
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		queryOrPrepared, pos, err := ex.stmtBuf.curStmt(ex.Ctx())
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		stmt, pinfo := queryOrPreparedToStatement(queryOrPrepared)

		// Execute the statement.
		res := ex.clientComm.createStatementResult(stmt.AST, pos)
		// execStmt takes responsibility for closing res.
		ev, baggage, err := ex.execStmt(stmt, res, pinfo, pos)
		if err != nil {
			return err
		}
		// If an event was generated, feed it to the state machine.
		if _, justFlush := ev.(eventFlush); ev != nil && !justFlush {
			err := txnStateTransitionsApplyWrapper(ctx, &ex.machine, &ex.state, ev, baggage)
			// WIP(andrei): panic on missing transition errors
			if err != nil {
				return err
			}
		} else {
			_, inOpen := ex.machine.GetCurState().(stateOpen)
			ex.state.adv = advanceInfo{
				code:  advanceOne,
				flush: !inOpen || justFlush,
			}
		}
		if _, inOpen := ex.machine.GetCurState().(stateOpen); !inOpen && !ex.state.adv.flush {
			log.Fatalf(ex.Ctx(), "Expected flush to be set when in state: %#v", ex.machine.GetCurState())
		}
		// Read the output of the transition.
		advInfo := ex.state.adv

		// Move the cursor according to what the state transition told us to do.
		switch advInfo.code {
		case advanceOne:
			ex.stmtBuf.advanceOne(ex.Ctx())
		case skipQueryStr:
			ex.stmtBuf.seekToNextQueryStr()
		case rewind:
			advInfo.rewCap.rewindAndUnlock(ex.Ctx())
		case stayInPlace:
			// Nothing to do. The same statement will be executed again.
		default:
			log.Fatalf(ex.Ctx(), "unexpected advance code: %s", advInfo.code)
		}

		// If flush is set, we reset the point to which future rewinds will refer.
		// For rewind purposes, we could only act if we're in stateOpen, but
		// flushing and trimming the stmtBuf has to be done regardless of state.
		if advInfo.flush {
			if err := ex.clientComm.flush(); err != nil {
				return err
			}
			// The position passed is the one of the statement following the
			// statement that caused this flush. The flushed statement cannot be
			// rewound to.
			nextPos, err := ex.stmtBuf.nextPos(pos)
			if err != nil {
				return err
			}
			ex.setTxnStartPos(ex.Ctx(), nextPos)
		}
	}
}

type stmtPositionInTxn int

const (
	firstInTxn stmtPositionInTxn = iota
	notFirstInTxn
)

type descriptorsPolicy int

const (
	useCachedDescs descriptorsPolicy = iota
	disallowCachedDescs
)

// resultWithStoreErr is a wrapper on top of StatementResult providing a way to
// send a query execution error to the client and hold on to it too. This is
// useful for keeping track of whether there was a query execution error between
// layers of code.
// Code that employs a resultWithStoredErr should not close the wrapped
// StatementResult directly; it should only use the SetError method.
type resultWithStoredErr struct {
	// TODO(andrei): This should be a more restricted interface, without
	// CloseResult() for example.
	StatementResult
	err error
}

// SetError passes and error to the wrapped resultWithStoredErr and hold on to
// it.
func (r *resultWithStoredErr) SetError(err error) error {
	r.err = err
	return r.CloseResultWithError(err)
}

// execStmt executes one statement by dispatching according to the current
// state. Returns an Event to be passed to the state machine, or nil if no
// transition is needed. If nil is returned, then the cursor is supposed to
// advance to the next statement.
//
// If an error is returned, the session is supposed to be considered done. Query
// execution errors are not returned explicitly; they're incorporated in the
// returned event and they have also already been communicated to the client
// through res. This function is responsible for closing res.
//
// Args:
// stmt: The statement to execute.
// res: Used to produce query results. This method will close the result.
// pinfo: The placeholders to use in the statements.
// pos: The position of stmt.
func (ex *connExecutor) execStmt(
	stmt Statement, res StatementResult, pinfo *tree.PlaceholderInfo, pos cursorPosition,
) (fsm.Event, fsm.EventBaggage, error) {
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
	var baggage fsm.EventBaggage
	var err error
	switch ex.machine.GetCurState().(type) {
	case stateNoTxn:
		ev, baggage = ex.execStmtInNoTxnState(ex.Ctx(), stmt, pos)
		if bwe, ok := baggage.(baggageWithError); ok {
			err = res.CloseResultWithError(bwe.errorCause())
		}
	case stateOpen:
		ev, baggage, err = ex.execStmtInOpenState(ex.Ctx(), stmt, pinfo, pos, res)
		// If the execution generated an event, we short-circuit here. Otherwise,
		// if we're told that nothing exceptional happened, we have further loose
		// ends to tie.
		// Note that if the execution resulted in an error, res has been already
		// closed.
		if err != nil || ev != nil {
			break
		}
		if ex.machine.GetCurState().(stateOpen).ImplicitTxn.Get() {
			ev, baggage = ex.handleAutoCommit(stmt.AST)
		}
		if ev == nil {
			ev, baggage = ex.handleSerializablePushMaybe(ex.Ctx(), stmt.AST)
		}
		// Close the result with the error from auto-commit/push detection (if
		// any).
		if bwe, ok := baggage.(baggageWithError); ok {
			err = res.CloseResultWithError(bwe.errorCause())
		}
	case stateAborted, stateRestartWait:
		// !!! take care of closing res
		ev, baggage = ex.execStmtInAbortedState(stmt, res)
	case stateCommitWait:
		// !!! take care of closing res
		ev, baggage = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.GetCurState()))
	}

	if filter := ex.server.cfg.TestingKnobs.StatementFilter2; filter != nil {
		if err := filter(ex.Ctx(), stmt.String(), ev); err != nil {
			return nil, nil, err
		}
	}
	return ev, baggage, err
}

// runShowTransactionState returns the state of current transaction.
// res is closed.
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(res StatementResult) error {
	res.BeginResult((*tree.ShowTransactionStatus)(nil))
	res.SetColumns(sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

	// WIP(andrei): figure out what to print here
	state := fmt.Sprintf("%s", ex.machine.GetCurState())
	if err := res.AddRow(ex.Ctx(), tree.Datums{tree.NewDString(state)}); err != nil {
		return err
	}
	return res.CloseResultWithError(nil)
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
// It also removes all the previous statements from the statementBuffer.
func (ex *connExecutor) setTxnStartPos(ctx context.Context, pos cursorPosition) {
	if pos.compare(ex.txnStartPos) == -1 {
		log.Fatalf(ctx, "attempting to move txnStartPos backwards. "+
			"Was: %s; new value: %s", ex.txnStartPos, pos)
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
	defer cl.Close()

	// If we already delivered results at or past the start position, we can't
	// rewind.
	if cl.clientPos().compare(ex.txnStartPos) != -1 {
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
func (ex *connExecutor) makeErrEvent(err error, stmt tree.Statement) (fsm.Event, fsm.EventBaggage) {
	retErr, retriable := err.(*roachpb.HandledRetryableTxnError)
	if retriable {
		if _, inOpen := ex.machine.GetCurState().(stateOpen); !inOpen {
			log.Fatalf(ex.Ctx(), "retriable error in unexpected state: %#v",
				ex.machine.GetCurState())
		}
	}
	if retriable && ex.state.mu.txn.IsRetryableErrMeantForTxn(*retErr) {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		ev := eventRetriableErr{
			IsCommit:     fsm.FromBool(isCommit(stmt)),
			CanAutoRetry: fsm.FromBool(canAutoRetry),
		}
		baggage := baggageEventRetriableErr{
			err:    err,
			rewCap: rc,
		}
		return ev, baggage
	}
	ev := eventNonRetriableErr{
		IsCommit: fsm.FromBool(isCommit(stmt)),
	}
	baggage := baggageEventNonRetriableErr{err: err}
	return ev, baggage
}

// handleAutoCommit commits the KV transaction if it hasn't been committed
// already. It also returns an Event that finishes the SQL txn. This is to be
// called after an "implicit txn" has run.
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
func (ex *connExecutor) handleAutoCommit(stmt tree.Statement) (fsm.Event, fsm.EventBaggage) {
	_, inOpen := ex.machine.GetCurState().(stateOpen)
	if !inOpen {
		log.Fatalf(ex.Ctx(), "handleAutoCommit called in state: %#v",
			ex.machine.GetCurState())
	}
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		return eventTxnFinish{}, nil
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
		return ex.makeErrEvent(err, stmt)
	}
	return eventTxnFinish{}, nil
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
) (fsm.Event, fsm.EventBaggage) {
	os, ok := ex.machine.GetCurState().(stateOpen)
	if !ok {
		log.Fatalf(ex.Ctx(),
			"handleSerializablePushMaybe called in state: %#v", ex.machine.GetCurState())
	}

	if !ex.isSerializableRestart() {
		return nil, nil
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
	baggage := baggageEventRetriableErr{
		err: injectedErr,
		// We're passing the rewCap to the caller.
		rewCap: *rewCap,
	}
	rewCap = nil
	return ev, baggage
}

// isSerializableRestart returns true if the KV transaction is serializable and
// its timestamp has been pushed. Used to detect whether the SQL txn will be
// allowed to commit.
//
// Note that this method allows for false negatives: sometimes the gaterway only
// figures out that it's been pushed when it sends an EndTransaction - i.e. it's
// possible for the txn to have been pushed asynchoronously by some other
// operation (usually, but not exclusively, by a high-priority txn with
// conflicting writes).
func (ex *connExecutor) isSerializableRestart() bool {
	txn := ex.state.mu.txn
	if txn == nil {
		return false
	}
	return txn.IsSerializableRestart()
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
			case *roachpb.UntrackedTxnError:
				// Symptom of concurrent retry, ignore.
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

// execStmtInNoTxnState "executes" a statement when no transaction is in scope.
// This method doesn't actually execute the statement; it just returns an Event
// that will generate a transaction. The statement will then be executed again,
// but this time in the Open state. This is true for both explicit and implicit
// transactions: in the case of an explicit transaction, there's still a plan to
// run for the BEGIN statement and it will run when the statement is executed
// again.
//
// Note that eventTxnStart{}, which is generally returned by this method, causes
// the state to change and previous results to be flushed, but the cursor is not
// advanced. This means that the statement will run again in stateOpen, and its
// results will also be flushed).
//
// TODO(andrei): It's surprising that there's a plan for BEGIN. We should
// probably just execute it all here.
func (ex *connExecutor) execStmtInNoTxnState(
	ctx context.Context, stmt Statement, stmtPos cursorPosition,
) (fsm.Event, fsm.EventBaggage) {
	switch stmt.AST.(type) {
	case *tree.BeginTransaction:
		ex.expBeginPos = stmtPos
		return eventTxnStart{ImplicitTxn: fsm.False}, baggageEventTxnStart{tranCtx: ex.transitionCtx}
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
		return eventTxnStart{ImplicitTxn: fsm.True}, baggageEventTxnStart{tranCtx: ex.transitionCtx}
	}
statementNeedsExplicitTxn:
	return ex.makeErrEvent(errNoTransactionInProgress, stmt.AST)
}

// execStmtInOpenState executes one statement in the context of the session's
// current transaction (which is assumed to exist).
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// and delegates everything else to the execution engines.
// It binds placeholders.
// Results and query execution errors are written to res.
//
// If an error is returned, the connection is supposed to be consider done.
// Query execution errors are not returned explicitly; they're incorporated in
// the returned Event. The result is Close()d if the returned event is anything
// but a noTopLevelTransition event; in such cases, the caller doesn't need to
// do anything further with the result. However, in case the returned event is
// of type noTopLevelTransition, then it is the caller's responsibility to
// Close() it. This is because in cases where the query execution is uneventful,
// the caller may want to contribute to the result by performing an auto-commit
// or such.
//
// Args:
// stmt: The statement to execute.
// pinfo: The placeholders to use in the statement.
// firstInTxn: Set for the first statement in a transaction. Used
//  so that nested BEGIN statements are caught.
// avoidCachedDescriptors: Set if the statement execution should avoid
//  using cached descriptors.
// res: The writer to send results to. res is sometimes Close()d and sometimes
// 	not when this returns; see explanations of the contract above.
func (ex *connExecutor) execStmtInOpenState(
	ctx context.Context,
	stmt Statement,
	pinfo *tree.PlaceholderInfo,
	stmtPos cursorPosition,
	res StatementResult,
) (fsm.Event, fsm.EventBaggage, error) {
	os := ex.machine.GetCurState().(stateOpen)

	makeErrEventAndCloseRes := func(err error) (fsm.Event, fsm.EventBaggage, error) {
		if err := res.CloseResultWithError(err); err != nil {
			return nil, nil, err
		}
		ev, baggage := ex.makeErrEvent(err, stmt.AST)
		return ev, baggage, nil
	}

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancellation function here.
	unregisterFun := ex.addActiveQuery(stmt.queryID, stmt.AST, ex.state.cancel)
	// We pass responsibility for  unregistering to results delivery. We might
	// take the responsibility back later if it turns out that we're going to
	// execute this query through the parallelize queue.
	res.SetResultsDeliveredCallback(unregisterFun)

	// Check if the statement is parallelized or is independent from parallel
	// execution. If neither of these cases are true, we need to synchronize
	// parallel execution by letting it drain before we can begin executing stmt.
	parallelize := IsStmtParallelized(stmt)
	_, independentFromParallelStmts := stmt.AST.(tree.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := ex.synchronizeParallelStmts(ctx); err != nil {
			return makeErrEventAndCloseRes(err)
		}
	}

	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		// Verify that this BEGIN is the one that started the txn.
		if stmtPos.compare(ex.expBeginPos) != 0 {
			return makeErrEventAndCloseRes(errTransactionInProgress)
		}
		return eventFlush{}, nil, nil

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		return ex.commitSQLTransaction(ctx, stmt.AST, res)

	case *tree.ReleaseSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return makeErrEventAndCloseRes(err)
		}
		// ReleaseSavepoint is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		return ex.commitSQLTransaction(ctx, stmt.AST, res)

	case *tree.RollbackTransaction:
		// Turn off test verification of metadata changes made by the
		// transaction.
		ex.testingVerifyMetadataFn = nil
		// RollbackTransaction is executed fully here; there's no planNode for it
		// and a planner is not involved at all.
		return ex.rollbackSQLTransaction(ctx, res)

	case *tree.Savepoint:
		if err := tree.ValidateRestartCheckpoint(s.Name); err != nil {
			return makeErrEventAndCloseRes(err)
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction.
		if ex.state.mu.txn.CommandCount() > 0 {
			err := fmt.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", tree.RestartSavepointName)
			return makeErrEventAndCloseRes(err)
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		res.BeginResult((*tree.Savepoint)(nil))
		return eventRetryIntentSet{}, nil, nil

	case *tree.RollbackToSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return makeErrEventAndCloseRes(err)
		}
		if !os.RetryIntent.Get() {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			return makeErrEventAndCloseRes(err)
		}

		res.BeginResult((*tree.Savepoint)(nil))

		// If commands have already been sent through the transaction,
		// restart the client txn's proto to increment the epoch.
		if ex.state.mu.txn.CommandCount() > 0 {
			// TODO(andrei): Should the timestamp below be ex.server.cfg.Clock.Now(), so that
			// the transaction gets a new timestamp?
			ex.state.mu.txn.Proto().Restart(
				0 /* userPriority */, 0 /* upgradePriority */, hlc.Timestamp{})
		}
		// We stay in Open. We can flush everything; this is the point to which
		// we'll rewind on auto-retries.
		return eventFlush{}, nil, nil

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
		//   res.BeginResult((*tree.Prepare)(nil))
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
	// Only run statements asynchronously through the parallelize queue if the
	// statements are parallelized and we're in a transaction. Parallelized
	// statements outside of a transaction are run synchronously with mocked
	// results, which has the same effect as running asynchronously but
	// immediately blocking.
	runInParallel := parallelize && !os.ImplicitTxn.Get()
	if runInParallel {
		// Create a new planner since we're executing in parallel.
		var dummySession *Session // WIP(andrei)
		p = ex.newPlanner(dummySession, ex.state.mu.txn)
	} else {
		// We're not executing in parallel. We can use the cached planner on the
		// session.
		p = &ex.planner
		var dummySession *Session // WIP(andrei)
		ex.resetPlanner(p, dummySession, ex.state.mu.txn)
	}

	if os.ImplicitTxn.Get() {
		ts, err := isAsOf(stmt.AST, &p.evalCtx, ex.server.cfg.Clock.Now())
		if err != nil {
			return makeErrEventAndCloseRes(err)
		}
		if ts != nil {
			p.asOfSystemTime = true
			p.avoidCachedDescriptors = true
			ex.state.mu.txn.SetFixedTimestamp(ctx, *ts)
		}
	}

	p.evalCtx.SetTxnTimestamp(ex.state.sqlTimestamp)
	p.evalCtx.SetStmtTimestamp(ex.server.cfg.Clock.PhysicalTime())
	p.semaCtx.Placeholders.Assign(pinfo)
	p.phaseTimes[plannerStartExecStmt] = timeutil.Now()
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
	constantMemAcc := p.evalCtx.Mon.MakeBoundAccount()
	p.evalCtx.ActiveMemAcc = &constantMemAcc
	defer constantMemAcc.Close(ctx)

	if runInParallel {
		// We're passing unregisterFun to the parallel execution.
		res.SetResultsDeliveredCallback(nil)
		cols, err := ex.execStmtInParallel(ex.Ctx(), stmt, p, unregisterFun)
		if err != nil {
			return makeErrEventAndCloseRes(err)
		}
		// Produce mocked out results for the query - the "zero value" of the
		// statement's result type:
		// - tree.Rows -> an empty set of rows
		// - tree.RowsAffected -> zero rows affected
		if err := initStatementResult(res, stmt, cols); err != nil {
			return makeErrEventAndCloseRes(err)
		}
	} else {
		p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit
		resOrErr := resultWithStoredErr{StatementResult: res}
		if err := ex.dispatchToExecutionEngine(ctx, stmt, p, &resOrErr); err != nil {
			return nil, nil, err
		}
		if resOrErr.err != nil {
			// res has already been closed.
			ev, baggage := ex.makeErrEvent(resOrErr.err, stmt.AST)
			return ev, baggage, nil
		}
	}

	// res has not been closed; the caller will have to do it.
	return nil, nil, nil
}

// commitSQLTransaction executes a COMMIT or RELEASE SAVEPOINT statement. The
// transaction is committed and the statement result is written to res. res is
// closed.
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, stmt tree.Statement, res StatementResult,
) (fsm.Event, fsm.EventBaggage, error) {
	if _, inOpen := ex.machine.GetCurState().(stateOpen); !inOpen {
		log.Fatalf(ctx, "commitSQLTransaction called in state: %#v", ex.machine.GetCurState())
	}

	commitType := commit
	if _, ok := stmt.(*tree.ReleaseSavepoint); ok {
		commitType = release
	}

	if err := ex.state.mu.txn.Commit(ex.Ctx()); err != nil {
		if err := res.CloseResultWithError(err); err != nil {
			return nil, nil, err
		}
		ev, baggage := ex.makeErrEvent(err, stmt)
		return ev, baggage, nil
	}

	res.BeginResult(stmt)

	switch commitType {
	case commit:
		if err := res.CloseResultWithError(nil); err != nil {
			return nil, nil, err
		}
		return eventTxnFinish{}, nil, nil
	case release:
		if err := res.CloseResultWithError(nil); err != nil {
			return nil, nil, err
		}
		return eventTxnReleased{}, nil, nil
	}
	panic("unreached")
}

// rollbackSQLTransaction executes a ROLLBACK statement. The transaction is
// rolled-back and the statement result is written to res. res is closed.
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) rollbackSQLTransaction(
	ctx context.Context, res StatementResult,
) (fsm.Event, fsm.EventBaggage, error) {
	_, inOpen := ex.machine.GetCurState().(stateOpen)
	_, inRestartWait := ex.machine.GetCurState().(stateRestartWait)
	if !inOpen && !inRestartWait {
		log.Fatalf(ctx,
			"rollbackSQLTransaction called on txn in wrong state: %#v", ex.machine.GetCurState())
	}
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	res.BeginResult((*tree.RollbackTransaction)(nil))
	if err := res.CloseResultWithError(nil); err != nil {
		return nil, nil, err
	}
	return eventTxnFinish{}, nil, nil
}

// evalCtx creates a tree.EvalContext from the current configuration.
func (ex *connExecutor) evalCtx() tree.EvalContext {
	return tree.EvalContext{
		Location:    &ex.sessionData.Location,
		Database:    ex.sessionData.Database,
		User:        ex.sessionData.User,
		SearchPath:  ex.sessionData.SearchPath,
		CtxProvider: ex,
		Mon:         ex.state.mon,
	}
}

// newPlanner creates a planner inside the scope of the given Session. The
// statement executed by the planner will be executed in txn. The planner
// should only be used to execute one statement.
//
// txn can be nil.
//
// WIP(andrei): the Session arg needs to go away.
func (ex *connExecutor) newPlanner(session *Session, txn *client.Txn) *planner {
	p := &planner{}
	ex.resetPlanner(p, session, txn)
	return p
}

func (ex *connExecutor) resetPlanner(p *planner, session *Session, txn *client.Txn) {
	resetPlanner(
		p, session, txn, ex.evalCtx(),
		ex.server.cfg.ClusterID(), ex.server.cfg.NodeID.Get(), ex.server.reCache,
	)
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
		ctx:     ctx,
		evalCtx: &planner.evalCtx,
		p:       planner,
	}

	plan, err := planner.makePlan(ctx, stmt)
	if err != nil {
		return nil, err
	}
	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(plan)
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

	if err := ex.parallelizeQueue.Add(params, plan, func(plan planNode) error {
		defer unregisterQuery()

		// TODO(andrei): this should really be a result writer implementation that
		// does nothing.
		bufferedWriter := newBufferedWriter(ex.sessionMon.MakeBoundAccount())
		if err := initStatementResult(bufferedWriter, stmt, cols); err != nil {
			return err
		}

		if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
			ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), true /* isParallel */)
		}

		planner.phaseTimes[plannerStartExecStmt] = timeutil.Now()
		clientRes := bufferedWriter.NewStatementResult()
		res := resultWithStoredErr{StatementResult: clientRes}
		err = ex.execWithLocalEngine(ctx, planner, plan, &res)
		planner.phaseTimes[plannerEndExecStmt] = timeutil.Now()
		recordStatementSummary(
			planner, stmt, false /* distSQLUsed*/, ex.state.autoRetryCounter,
			bufferedWriter, err, &ex.server.EngineMetrics)
		if ex.server.cfg.TestingKnobs.AfterExecute != nil {
			ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), bufferedWriter, err)
		}

		if err != nil {
			log.Warningf(ctx, "Connection error from the parallel queue. How can that "+
				"be when using the BufferedWriter? err: %s", err)
			// I think this can't happen and it's unclear how to react when a
			// BufferedWriter "connection" is toast.
			_ = res.CloseResultWithError(err)
			bufferedWriter.Close()
			return err
		}
		if res.err == nil {
			_ = clientRes.CloseResultWithError(nil)
		}
		bufferedWriter.Close()
		return res.err
	}); err != nil {
		return nil, err
	}

	return cols, nil
}

// dispatchToExecutionEngine executes the statement synchronously, writes the
// result to res and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned. If res has
// not had its error set, it is the caller's responsibility to close it.
// It is expected that the caller will inspect res and react to query
// errors by producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, stmt Statement, planner *planner, res *resultWithStoredErr,
) error {

	planner.phaseTimes[plannerStartLogicalPlan] = timeutil.Now()
	plan, err := planner.makePlan(ctx, stmt)
	planner.phaseTimes[plannerEndLogicalPlan] = timeutil.Now()
	if err != nil {
		return res.SetError(err)
	}
	defer plan.Close(ctx)

	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(plan)
	}
	if err := initStatementResult(res, stmt, cols); err != nil {
		return res.SetError(err)
	}

	useDistSQL, err := shouldUseDistSQL(
		ctx, ex.sessionData.DistSQLMode, ex.distSQLPlanner, planner, plan)
	if err != nil {
		return res.SetError(err)
	}

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.phaseTimes[plannerStartExecStmt] = timeutil.Now()

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
		err = ex.execWithDistSQLEngine(ctx, planner, plan, res)
	} else {
		err = ex.execWithLocalEngine(ctx, planner, plan, res)
	}
	planner.phaseTimes[plannerEndExecStmt] = timeutil.Now()
	recordStatementSummary(
		planner, stmt, useDistSQL, ex.state.autoRetryCounter, res, err, &ex.server.EngineMetrics,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res, err)
	}

	if err != nil {
		return res.SetError(err)
	}
	return nil
}

type terminalErr struct {
	error
}

// execWithLocalEngine runs a plan using the local (non-distributed) SQL
// engine.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned. If res has
// not had its error set, it is the caller's responsibility to close it.
func (ex *connExecutor) execWithLocalEngine(
	ctx context.Context, planner *planner, plan planNode, res *resultWithStoredErr,
) error {
	// Create a BoundAccount to track the memory usage of each row.
	rowAcc := planner.evalCtx.Mon.MakeBoundAccount()
	planner.evalCtx.ActiveMemAcc = &rowAcc
	defer rowAcc.Close(ctx)

	params := runParams{
		ctx:     ctx,
		evalCtx: &planner.evalCtx,
		p:       planner,
	}

	if err := startPlan(params, plan); err != nil {
		return err
	}

	switch res.StatementType() {
	case tree.RowsAffected:
		count, err := countRowsAffected(params, plan)
		if err != nil {
			return err
		}
		res.IncrementRowsAffected(count)
		return nil
	case tree.Rows:
		err := ex.forEachRow(params, plan, func(values tree.Datums) error {
			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			if err := res.AddRow(ctx, values); err != nil {
				// If res.AddRow() returned an error, it is a terminal communication
				// error.
				return terminalErr{err}
			}
			return nil
		})
		if err != nil {
			if _, ok := err.(terminalErr); ok {
				return err
			}
			return res.SetError(err)
		}
		return nil
	default:
		// WIP(andrei): The Executor is ignoring other stmt types. Was calling
		// startPlan() sufficient for them? Shouls I just close the result?
		panic("!!!")
	}
}

// exectWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned. If res has
// not had its error set, it is the caller's responsibility to close it.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context, planner *planner, tree planNode, res *resultWithStoredErr,
) error {
	recv := makeDistSQLReceiver(
		ctx, res,
		ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			_ = ex.server.cfg.Clock.Update(ts)
		},
	)
	if err := ex.distSQLPlanner.PlanAndRun(
		ctx, planner.txn, tree, &recv, planner.evalCtx,
	); err != nil {
		return res.SetError(err)
	}
	// WIP(andrei): !!! this is wrong. recv needs to learn to separate connection
	// errors from query execution errors.
	if recv.err != nil {
		return res.SetError(recv.err)
	}
	return nil
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
		if params.evalCtx.ActiveMemAcc != nil {
			params.evalCtx.ActiveMemAcc.Clear(params.ctx)
		}

		if err := f(p.Values()); err != nil {
			return err
		}
	}
	return err
}

func (ex *connExecutor) execStmtInAbortedState(
	stmt Statement, res StatementResult,
) (fsm.Event, fsm.EventBaggage) {
	panic("!!! unimplemented")
}

func (ex *connExecutor) execStmtInCommitWaitState(
	stmt Statement, res StatementResult,
) (fsm.Event, fsm.EventBaggage) {
	panic("!!! unimplemented")
}

// eventFlush is a dummy event type that is never passed to the state machine.
// It is used by statement execution to indicate that a statement doesn't want
// any state transitions, but it wants results to be flushed and the transaction
// start point to be moved forward. Only statements executed in stateOpen need
// to generate this dummy event if they want to stay in stateOpen but also want
// flushing; for other states, results are implicitly flushed when no event is
// produced.
type eventFlush struct{}

// Event implements the fsm.Event interface.
func (eventFlush) Event() {}

// eventFlush implements the fsm.Event interface.
var _ fsm.Event = eventFlush{}
