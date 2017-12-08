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
	"strconv"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
// network communication - i.e. pgwire.conn); the module doing client
// communication is responsible for pushing statements into the buffer and for
// providing an implementation of the clientConn interface (and thus sending
// results to the client). The connExecutor does not control when
// results are delivered to the client, but still it does have some influence
// over that; this is because of the fact that the possibility of doing
// automatic retries goes away the moment results for the transaction in
// question are delivered to the client. The communication module has full
// freedom in sending results whenever it sees fit; however the connExecutor
// influences communication in the following ways:
//
// a) When deciding whether an automatic retry can be performed for a
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
// b) The connExecutor sometimes ask the clientComm to deliver everything
// (most commonly in response to a Sync command).
//
// As of Feb 2018, the pgwire.conn delivers results synchronously to the client
// when its internal buffer overflows. In priciple, delivery of result could be
// done asynchronously wrt the processing of commands (e.g. we could have a
// timing policy in addition to the buffer size). The first implementation of
// that showed a performance impact of involving a channel communication in the
// Sync processing path.
//
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
// execution errors are written to a CommandResult. Low-level methods don't
// operate on a CommandResult directly; instead they operate on a wrapper
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
	sqlStats sqlStats

	reCache *tree.RegexpCache

	// pool is the parent monitor for all session monitors.
	pool *mon.BytesMonitor

	// EngineMetrics is exported as required by the metrics.Struct magic we use
	// for metrics registration.
	EngineMetrics SQLEngineMetrics

	// StatementCounters contains metrics.
	StatementCounters StatementCounters

	// dbCache is a cache for database descriptors, maintained through Gossip
	// updates.
	dbCache *databaseCacheHolder

	// Attempts to use unimplemented features.
	unimplementedErrors struct {
		syncutil.Mutex
		counts map[string]int64
	}
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig, pool *mon.BytesMonitor) *Server {
	return &Server{
		cfg: cfg,
		EngineMetrics: SQLEngineMetrics{
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
		StatementCounters: makeStatementCounters(),
		// dbCache will be updated on Start().
		dbCache:  newDatabaseCacheHolder(newDatabaseCache(config.SystemConfig{})),
		pool:     pool,
		sqlStats: sqlStats{st: cfg.Settings, apps: make(map[string]*appStats)},
		reCache:  tree.NewRegexpCache(512),
	}
}

// Start starts the Server's background processing.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	gossipUpdateC := s.cfg.Gossip.RegisterSystemConfigChannel()
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-gossipUpdateC:
				sysCfg, _ := s.cfg.Gossip.GetSystemConfig()
				s.dbCache.updateSystemConfig(sysCfg)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

func (s *Server) recordUnimplementedFeature(feature string) {
	if feature == "" {
		return
	}
	s.unimplementedErrors.Lock()
	if s.unimplementedErrors.counts == nil {
		s.unimplementedErrors.counts = make(map[string]int64)
	}
	s.unimplementedErrors.counts[feature]++
	s.unimplementedErrors.Unlock()
}

// FillUnimplementedErrorCounts fills the passed map with the server's current
// counts of how often individual unimplemented features have been encountered.
func (s *Server) FillUnimplementedErrorCounts(fill map[string]int64) {
	s.unimplementedErrors.Lock()
	for k, v := range s.unimplementedErrors.counts {
		fill[k] = v
	}
	s.unimplementedErrors.Unlock()
}

// ResetUnimplementedCounts resets counting of unimplemented errors.
func (s *Server) ResetUnimplementedCounts() {
	s.unimplementedErrors.Lock()
	s.unimplementedErrors.counts = make(map[string]int64, len(s.unimplementedErrors.counts))
	s.unimplementedErrors.Unlock()
}

// ResetStatementStats resets the executor's collected statement statistics.
func (s *Server) ResetStatementStats(ctx context.Context) {
	s.sqlStats.resetStats(ctx)
}

// GetScrubbedStmtStats returns the statement statistics by app, with the
// queries scrubbed of their identifiers. Any statements which cannot be
// scrubbed will be omitted from the returned map.
func (s *Server) GetScrubbedStmtStats() []roachpb.CollectedStatementStatistics {
	return s.sqlStats.getScrubbedStmtStats(s.cfg.VirtualSchemas)
}

// ServeConn creates a connExecutor and serves a client connection by reading
// commands from stmtBuf.
//
// Args:
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
// 	 produce results for the client.
// reserved: An amount on memory reserved for the connection. The connExecutor
// 	 takes ownership of this memory.
// memMetrics: The metrics that statements executed on this connection will
//   contribute to.
func (s *Server) ServeConn(
	ctx context.Context,
	args SessionArgs,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	reserved mon.BoundAccount,
	memMetrics *MemoryMetrics,
) error {
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
		-1, math.MaxInt64, s.cfg.Settings)
	sessionRootMon.Start(ctx, s.pool, reserved)
	sessionMon := mon.MakeMonitor("session",
		mon.MemoryResource,
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings)
	sessionMon.Start(ctx, &sessionRootMon, mon.BoundAccount{})

	// We merely prepare the txn monitor here. It is started in
	// txnState.resetForNewSQLTxn().
	txnMon := mon.MakeMonitor("txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings)

	settings := &s.cfg.Settings.SV
	distSQLMode := sessiondata.DistSQLExecMode(DistSQLClusterExecMode.Get(settings))

	ex := connExecutor{
		server:     s,
		stmtBuf:    stmtBuf,
		clientComm: clientComm,
		mon:        sessionRootMon,
		sessionMon: sessionMon,
		sessionData: sessiondata.SessionData{
			Database:      args.Database,
			DistSQLMode:   distSQLMode,
			SearchPath:    sqlbase.DefaultSearchPath,
			Location:      time.UTC,
			User:          args.User,
			SequenceState: sessiondata.NewSequenceState(),
			RemoteAddr:    args.RemoteAddr,
		},
		prepStmtsNamespace: prepStmtNamespace{
			prepStmts: make(map[string]prepStmtEntry),
			portals:   make(map[string]portalEntry),
		},
		state: txnState2{
			mon:           &txnMon,
			txnAbortCount: s.StatementCounters.TxnAbortCount,
		},
		transitionCtx: transitionCtx{
			db:     s.cfg.DB,
			nodeID: s.cfg.NodeID.Get(),
			clock:  s.cfg.Clock,
			// Future transaction's monitors will inherits from sessionRootMon.
			connMon: &sessionRootMon,
			tracer:  s.cfg.AmbientCtx.Tracer,
		},
		parallelizeQueue: MakeParallelizeQueue(NewSpanBasedDependencyAnalyzer()),
		memMetrics:       memMetrics,
		extraTxnState: transactionState{
			tables: TableCollection{
				leaseMgr:          s.cfg.LeaseManager,
				databaseCache:     s.dbCache.getDatabaseCache(),
				dbCacheSubscriber: s.dbCache,
			},
			schemaChangers: schemaChangerCollection{},
		},
		txnStartPos: -1,
		appStats:    s.sqlStats.getStatsForApplication(args.ApplicationName),
		planner:     planner{execCfg: s.cfg},
	}
	ex.mu.ActiveQueries = make(map[uint128.Uint128]*queryMeta)
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.dataMutator = sessionDataMutator{
		data: &ex.sessionData,
		defaults: sessionDefaults{
			applicationName: args.ApplicationName,
			database:        args.Database,
		},
		settings:       s.cfg.Settings,
		curTxnReadOnly: &ex.state.readOnly,
		sessionTracing: &ex.state.tracing,
		applicationNameChanged: func(newName string) {
			ex.appStats = ex.server.sqlStats.getStatsForApplication(newName)
		},
	}
	ex.dataMutator.SetApplicationName(args.ApplicationName)

	defer func() {
		if r := recover(); r != nil {
			ex.closeWrapper(ctx, r)
		} else {
			ex.closeWrapper(ctx, nil)
		}
	}()
	return ex.run(ctx)
}

type closeType bool

const (
	normalClose closeType = true
	panicClose  closeType = false
)

func (ex *connExecutor) closeWrapper(ctx context.Context, recovered interface{}) {
	if recovered != nil {
		// A warning header guaranteed to go to stderr. This is unanonymized.
		cutStmt := ex.curStmt.String()
		if len(cutStmt) > panicLogOutputCutoffChars {
			cutStmt = cutStmt[:panicLogOutputCutoffChars] + " [...]"
		}

		log.Shout(ctx, log.Severity_ERROR,
			fmt.Sprintf("a SQL panic has occurred while executing %q: %s", cutStmt, recovered))

		ex.close(ctx, panicClose)

		safeErr := AnonymizeStatementsForReporting("executing", ex.curStmt.String(), recovered)

		log.ReportPanic(ctx, &ex.server.cfg.Settings.SV, safeErr, 1 /* depth */)

		// Propagate the (sanitized) panic further.
		// NOTE(andrei): It used to be that we sanitized the panic and then a higher
		// layer was in charge of doing the log.ReportPanic() call. Now that the
		// call is above, it's unclear whether we should propagate the original
		// panic or safeErr. I'm propagating safeErr to be on the safe side.
		panic(safeErr)
	}
	ex.close(ctx, normalClose)
}

func (ex *connExecutor) close(ctx context.Context, closeType closeType) {
	log.VEvent(ctx, 2, "finishing connExecutor")

	// Make sure that no statements remain in the ParallelizeQueue. If no statements
	// are in the queue, this will be a no-op. If there are statements in the
	// queue, they would have eventually drained on their own, but if we don't
	// wait here, we risk alarming the MemoryMonitor. We ignore the error because
	// it will only ever be non-nil if there are statements in the queue, meaning
	// that the Session was abandoned in the middle of a transaction, in which
	// case the error doesn't matter.
	//
	// TODO(nvanbenschoten): Once we have better support for canceling ongoing
	// statement execution by the infrastructure added to support CancelRequest,
	// we should try to actively drain this queue instead of passively waiting
	// for it to drain. (andrei, 2017/09) - We now have support for statement
	// cancellation. Now what?
	_ = ex.synchronizeParallelStmts(ctx)

	// We'll cleanup the SQL txn by creating a non-retriable (commit:true) event.
	// This event is guaranteed to be accepted in every state.
	ev := eventNonRetriableErr{IsCommit: fsm.FromBool(true)}
	payload := eventNonRetriableErrPayload{err: fmt.Errorf("connExecutor closing")}
	if err := ex.machine.ApplyWithPayload(ctx, ev, payload); err != nil {
		log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
	}

	ex.extraTxnState.reset(ctx, txnAborted, ex.server.dbCache)

	if closeType == normalClose {
		// Close all statements and prepared portals by first unifying the namespaces
		// and the closing what remains.
		ex.commitPrepStmtNamespace(ctx)
		ex.prepStmtsNamespace.resetTo(ctx, &prepStmtNamespace{})
	}

	if ex.state.tracing.Enabled() {
		if err := ex.dataMutator.StopSessionTracing(); err != nil {
			log.Warningf(ctx, "error stopping tracing: %s", err)
		}
	}

	ex.server.cfg.SessionRegistry.deregister(ex)

	if closeType == normalClose {
		ex.state.mon.Stop(ctx)
		ex.sessionMon.Stop(ctx)
		ex.mon.Stop(ctx)
	} else {
		ex.state.mon.EmergencyStop(ctx)
		ex.sessionMon.EmergencyStop(ctx)
		ex.mon.EmergencyStop(ctx)
	}
}

type connExecutor struct {
	noCopy util.NoCopy

	// The server to which this connExecutor is attached. The reference is used
	// for getting access to configuration settings and metrics.
	server *Server

	// The buffer with incoming statements to execute.
	stmtBuf *StmtBuf
	// The interface for communicating statement results to the client.
	clientComm ClientComm
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
	txnStartPos CmdPos

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

	// prepStmtNamespace contains the prepared statements and portals that the
	// session currently has access to.
	// prepStmtsNamespaceAtTxnStartPos is a snapshot of the prep stmts/portals
	// before processing the command at position txnStartPos. Here's the deal:
	// prepared statements are not transactional, but they do need to interact
	// properly with automatic retries (i.e. rewinding the command buffer). When
	// doing a rewind, we need to be able to restore the prep stmts as they were.
	// We do this by taking a snapshot every time txnStartPos is advanced.
	// Prepared statements are shared between the two collections, but these
	// collections are periodically reconciled.
	prepStmtsNamespace              prepStmtNamespace
	prepStmtsNamespaceAtTxnStartPos prepStmtNamespace

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

	// curStmt is the statement that's currently being prepared or executed, if
	// any. This is printed by high-level panic recovery.
	curStmt tree.Statement
}

type prepStmtNamespace struct {
	// prepStmts contains the prepared statements currently available on the
	// session.
	prepStmts map[string]prepStmtEntry
	// portals contains the portals currently available on the session.
	portals map[string]portalEntry
}

type prepStmtEntry struct {
	*PreparedStatement
	portals map[string]struct{}
}

func (pe *prepStmtEntry) copy() prepStmtEntry {
	cpy := prepStmtEntry{}
	cpy.PreparedStatement = pe.PreparedStatement
	cpy.portals = make(map[string]struct{})
	for pname, _ := range pe.portals {
		cpy.portals[pname] = struct{}{}
	}
	return cpy
}

type portalEntry struct {
	*PreparedPortal
	psName string
}

// resetTo resets a namespace to equate another one (`to`). Prep stmts and portals
// that are present in ns but not in to are deallocated.
//
// A (pointer to) empty `to` can be passed in to deallocate everything.
func (ns *prepStmtNamespace) resetTo(ctx context.Context, to *prepStmtNamespace) {
	for name, ps := range ns.prepStmts {
		bps, ok := to.prepStmts[name]
		// If the prepared statement didn't exist before (including if a statement
		// with the same name existed, but it was different), close it.
		if !ok || bps.PreparedStatement != ps.PreparedStatement {
			ps.close(ctx)
		}
	}
	for name, p := range ns.portals {
		bp, ok := to.portals[name]
		// If the prepared statement didn't exist before (including if a statement
		// with the same name existed, but it was different), close it.
		if !ok || bp.PreparedPortal != p.PreparedPortal {
			p.close(ctx)
		}
	}
	*ns = to.copy()
}

func (ns *prepStmtNamespace) copy() prepStmtNamespace {
	var cpy prepStmtNamespace
	cpy.prepStmts = make(map[string]prepStmtEntry)
	for name, psEntry := range ns.prepStmts {
		cpy.prepStmts[name] = psEntry.copy()
	}
	cpy.portals = make(map[string]portalEntry)
	for name, p := range ns.portals {
		cpy.portals[name] = p
	}
	return cpy
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
func (ts *transactionState) reset(ctx context.Context, ev txnEvent, dbCacheHolder *databaseCacheHolder) {
	ts.schemaChangers.reset()

	var opt releaseOpt
	if ev == txnCommit {
		opt = blockForDBCacheUpdate
	} else {
		opt = dontBlockForDBCacheUpdate
	}
	ts.tables.releaseTables(ctx, opt)
	ts.tables.databaseCache = dbCacheHolder.getDatabaseCache()

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
//
// When this returns, the connection to the client needs to be terminated. If it
// returns with an error, that error may represent a communication error (in
// which case the connection might already also have an error from the reading
// side), or some other unexpected failure. Returned errors have not been
// communicated to the client: it's up to the caller to do that if it wants.
func (ex *connExecutor) run(ctx context.Context) error {
	defer ex.extraTxnState.reset(ctx, txnAborted, ex.server.dbCache)

	ex.server.cfg.SessionRegistry.register(ex)
	defer ex.server.cfg.SessionRegistry.deregister(ex)

	ex.connCtx = ctx
	var draining bool
	for {
		ex.curStmt = nil
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

		var ev fsm.Event
		var payload fsm.EventPayload
		var res ResultBase

		switch tcmd := cmd.(type) {
		case ExecPortal:
			// ExecPortal is handled like ExecStmt, except that the placeholder info
			// is taken from the portal.

			portal, ok := ex.prepStmtsNamespace.portals[tcmd.Name]
			if !ok {
				err := pgerror.NewErrorf(
					pgerror.CodeInvalidCursorNameError, "unknown portal %q", tcmd.Name)
				ev = eventNonRetriableErr{IsCommit: fsm.False}
				payload = eventNonRetriableErrPayload{err: err}
				res = ex.clientComm.CreateErrorResult(pos)
				break
			}
			ex.curStmt = portal.Stmt.Statement

			pinfo := &tree.PlaceholderInfo{
				TypeHints: portal.Stmt.TypeHints,
				Types:     portal.Stmt.Types,
				Values:    portal.Qargs,
			}

			// No parsing is taking place, but we need to set the parsing phase time
			// because the service latency is measured from
			// phaseTimes[sessionStartParse].
			now := timeutil.Now()
			ex.phaseTimes[sessionStartParse] = now
			ex.phaseTimes[sessionEndParse] = now

			if portal.Stmt.Statement == nil {
				res = ex.clientComm.CreateEmptyQueryResult(pos)
				break
			}

			stmtRes := ex.clientComm.CreateStatementResult(
				portal.Stmt.Statement,
				// The client is using the extended protocol, so no row description is
				// needed.
				DontNeedRowDesc,
				pos, portal.OutFormats, ex.sessionData.Location)
			stmtRes.SetLimit(tcmd.Limit)
			res = stmtRes
			curStmt := Statement{
				AST:           portal.Stmt.Statement,
				ExpectedTypes: portal.Stmt.Columns,
				AnonymizedStr: portal.Stmt.AnonymizedStr,
			}
			ev, payload, err = ex.execStmt(ex.Ctx(), curStmt, stmtRes, pinfo, pos)
			if err != nil {
				return err
			}
		case ExecStmt:
			if tcmd.Stmt == nil {
				res = ex.clientComm.CreateEmptyQueryResult(pos)
				break
			}
			ex.curStmt = tcmd.Stmt

			stmtRes := ex.clientComm.CreateStatementResult(
				tcmd.Stmt, NeedRowDesc, pos, nil /* formatCodes */, ex.sessionData.Location)
			res = stmtRes
			curStmt := Statement{AST: tcmd.Stmt}
			ev, payload, err = ex.execStmt(ex.Ctx(), curStmt, stmtRes, nil /* pinfo */, pos)
			if err != nil {
				return err
			}
		case PrepareStmt:
			ex.curStmt = tcmd.Stmt
			ev, payload = ex.execPrepare(ex.Ctx(), tcmd)
			res = ex.clientComm.CreateParseResult(pos)
		case DescribeStmt:
			descRes := ex.clientComm.CreateDescribeResult(pos)
			res = descRes
			ev, payload = ex.execDescribe(ex.Ctx(), tcmd, descRes)
		case BindStmt:
			ev, payload = ex.execBind(ex.Ctx(), tcmd)
			res = ex.clientComm.CreateBindResult(pos)
		case DeletePreparedStmt:
			ev, payload = ex.execDelPrepStmt(ex.Ctx(), tcmd)
			res = ex.clientComm.CreateDeleteResult(pos)
		case SendError:
			ev = eventNonRetriableErr{IsCommit: fsm.False}
			payload = eventNonRetriableErrPayload{err: tcmd.Err}
			res = ex.clientComm.CreateErrorResult(pos)
		case Sync:
			// Note that the Sync result will flush results to the network connection.
			res = ex.clientComm.CreateSyncResult(pos)
			if draining {
				// If we're draining, check whether this is a good time to finish the
				// connection.
				if snt, ok := ex.machine.CurState().(stateNoTxn); ok {
					res.Close(stateToTxnStatusIndicator(snt))
					return nil
				}
			}
		case CopyIn:
			res = ex.clientComm.CreateCopyInResult(pos)
			var err error
			ev, payload, err = ex.execCopyIn(ex.Ctx(), tcmd)
			if err != nil {
				return err
			}
		case DrainRequest:
			// We received a drain request. We terminate immediately if we're not in a
			// transaction. If we are in a transaction, we'll finish as soon as a Sync
			// command (i.e. the end of a batch) is processed outside of a
			// transaction.
			draining = true
			if _, ok := ex.machine.CurState().(stateNoTxn); ok {
				return nil
			}
		case Flush:
			// Closing the res will flush the connection's buffer.
			res = ex.clientComm.CreateFlushResult(pos)
		default:
			panic(fmt.Sprintf("unsupported command type: %T", cmd))
		}

		var advInfo advanceInfo

		// If an event was generated, feed it to the state machine.
		if ev != nil {
			var err error
			advInfo, err = ex.txnStateTransitionsApplyWrapper(ctx, ev, payload, res, pos)
			if err != nil {
				return err
			}
		} else {
			// If no event was generated synthesize an advance code.
			advInfo = advanceInfo{
				code:  advanceOne,
				flush: false,
			}
		}

		// Decide if we need to close the result or not. We don't need to do it if
		// we're staying in place or rewinding - the statement will be executed
		// again.
		if advInfo.code != stayInPlace && advInfo.code != rewind {
			// Close the result. In case of an execution error, the result might have
			// its error set already or it might not.
			pe, ok := payload.(payloadWithError)
			if ok {
				ex.recordUnimplementedErrorMaybe(pe.errorCause())
			}
			if res.Err() == nil && ok {
				if advInfo.code == stayInPlace {
					return errors.Errorf("unexpected stayInPlace code with err: %s", pe.errorCause())
				}
				// Depending on whether the result has the error already or not, we have
				// to call either Close or CloseWithErr.
				if res.Err() == nil {
					res.CloseWithErr(pe.errorCause())
				} else {
					res.Close(stateToTxnStatusIndicator(ex.machine.CurState()))
				}
			} else {
				res.Close(stateToTxnStatusIndicator(ex.machine.CurState()))
			}
		} else {
			res.Discard()
		}

		// Move the cursor according to what the state transition told us to do.
		switch advInfo.code {
		case advanceOne:
			ex.stmtBuf.advanceOne(ex.Ctx())
		case skipBatch:
			// We'll flush whatever results we have to the network. The last one must
			// be an error. This flush may seem unnecessary, as we generally only
			// flush when the client requests it through a Sync or a Flush but without
			// it the Node.js driver isn't happy. That driver likes to send "flush"
			// command and only sends Syncs once it received some data. But we ignore
			// flush commands (just like we ignore any other commands) when skipping
			// to the next batch.
			if err := ex.clientComm.Flush(pos); err != nil {
				return err
			}
			if err := ex.stmtBuf.seekToNextBatch(ex.Ctx()); err != nil {
				return err
			}
		case rewind:
			ex.rewindPrepStmtNamespace(ex.Ctx())
			advInfo.rewCap.rewindAndUnlock(ex.Ctx())
		case stayInPlace:
			// Nothing to do. The same statement will be executed again.
		default:
			log.Fatalf(ex.Ctx(), "unexpected advance code: %s", advInfo.code)
		}

		if err := ex.updateTxnStartPosMaybe(ex.Ctx(), cmd, pos, advInfo); err != nil {
			return err
		}
	}
}

// We handle the CopyFrom statement by creating a copyMachine and handing it
// control over the connection until the copying is done. The contract is that,
// when this is called, the pgwire.conn is not reading from the network
// connection any more until this returns. The copyMachine will to the reading
// and writing up to the CommandComplete message.
func (ex *connExecutor) execCopyIn(
	ctx context.Context, cmd CopyIn,
) (fsm.Event, fsm.EventPayload, error) {

	// When we're done, unblock the network connection.
	defer cmd.CopyDone.Done()

	state := ex.machine.CurState()
	_, isNoTxn := state.(stateNoTxn)
	_, isOpen := state.(stateOpen)
	if !isNoTxn && !isOpen {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */)}
		return ev, payload, nil
	}

	// If we're in an explicit txn, then the copying will be done within that
	// txn. Otherwise, we tell the copyMachine to manage its own transactions.
	var txnOpt copyTxnOpt
	if isOpen {
		txnOpt = copyTxnOpt{
			txn:           ex.state.mu.txn,
			txnTimestamp:  ex.state.sqlTimestamp,
			stmtTimestamp: ex.server.cfg.Clock.PhysicalTime(),
		}
	}

	var monToStop *mon.BytesMonitor
	defer func() {
		if monToStop != nil {
			monToStop.Stop(ctx)
		}
	}()
	if isNoTxn {
		// HACK: We're reaching inside ex.state and starting the monitor. Normally
		// that's driven by the state machine, but we're bypassing the state machine
		// here.
		ex.state.mon.Start(ctx, &ex.sessionMon, mon.BoundAccount{} /* reserved */)
		monToStop = ex.state.mon
	}
	cm, err := newCopyMachine(
		ctx, cmd.Conn, cmd.Stmt, txnOpt, ex.server.cfg,
		// resetPlanner
		func(p *planner, txn *client.Txn, txnTS time.Time, stmtTS time.Time) {
			// HACK: We're reaching inside ex.state and changing sqlTimestamp by hand.
			// It is used by resetPlanner. Normally sqlTimestamp is updated by the
			// state machine, but the copyMachine manages its own transactions without
			// going through the state machine.
			ex.state.sqlTimestamp = txnTS
			ex.resetPlanner(p, txn, stmtTS)
		},
	)
	if err != nil {
		return nil, nil, err
	}
	if err := cm.run(ctx); err != nil {
		// TODO(andrei): We don't have a retriable error story for the copy machine.
		// When running outside of a txn, the copyMachine should probably do retries
		// internally. When not, it's unclear what we should do. For now, we abort
		// the txn (if any).
		// We also don't have a story for distinguishing communication errors (which
		// should terminate the connection) from query errors. For now, we treat all
		// errors as query errors.
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload, nil
	}
	return nil, nil, nil
}

// updateTxnStartPosMaybe checks whether the ex.txnStartPos should be advanced,
// based on the advInfo produced by running cmd at position pos.
func (ex *connExecutor) updateTxnStartPosMaybe(
	ctx context.Context, cmd Command, pos CmdPos, advInfo advanceInfo,
) error {
	// txnStartPos is only maintained while in stateOpen.
	if _, ok := ex.machine.CurState().(stateOpen); !ok {
		return nil
	}
	if advInfo.txnEvent == txnStart || advInfo.txnEvent == txnRestart {
		var nextPos CmdPos
		switch advInfo.code {
		case stayInPlace:
			nextPos = pos
		case advanceOne:
			// Future rewinds will refer to the next position; the statement that
			// started the transaction (i.e. BEGIN) will not be itself be executed
			// again.
			nextPos = pos + 1
		case rewind:
			if advInfo.rewCap.rewindPos != ex.txnStartPos {
				return errors.Errorf("unexpected rewind position: %s when txn start is: %d",
					advInfo.rewCap.rewindPos, ex.txnStartPos)
			}
			// txnStartPos stays unchanged.
			return nil
		default:
			return errors.Errorf("unexpected advance code when starting a txn: %s", advInfo.code)
		}
		ex.setTxnStartPos(ex.Ctx(), nextPos)
	} else {
		// See if we can advance the rewind point even if this is not the point
		// where the transaction started. We can do that after running a special
		// statement (e.g. SET TRANSACTION or SAVEPOINT) or when we just ran a Sync
		// command. The idea with the Sync command is that we don't want the
		// following sequence to disable retries for what comes after the sequence:
		// 1: PrepareStmt BEGIN
		// 2: BindStmt
		// 3: ExecutePortal
		// 4: Sync
		//
		// We can blindly advance the txnStartPos for most commands that don't run
		// statements.

		if advInfo.code != advanceOne {
			panic(fmt.Sprintf("unexpected advanceCode: %s", advInfo.code))
		}

		var canAdvance bool
		_, inOpen := ex.machine.CurState().(stateOpen)
		if inOpen && (ex.txnStartPos == pos) {
			switch tcmd := cmd.(type) {
			case ExecStmt:
				canAdvance = ex.stmtDoesntNeedRetry(tcmd.Stmt)
			case ExecPortal:
				portal := ex.prepStmtsNamespace.portals[tcmd.Name]
				canAdvance = ex.stmtDoesntNeedRetry(portal.Stmt.Statement)
			case CopyIn:
			case PrepareStmt:
				canAdvance = true
			case DescribeStmt:
				canAdvance = true
			case BindStmt:
				canAdvance = true
			case DeletePreparedStmt:
				canAdvance = true
			case SendError:
				canAdvance = true
			case Sync:
				canAdvance = true
			case DrainRequest:
				ex.setTxnStartPos(ex.Ctx(), pos+1)
			default:
				panic(fmt.Sprintf("unsupported cmd: %T", cmd))
			}
			if canAdvance {
				ex.setTxnStartPos(ex.Ctx(), pos+1)
			}
		}
	}
	return nil
}

// stmtDoesntNeedRetry returns true if the given statement does not need to be
// retried when performing automatic retries. This means that the results of the
// statement do not change with retries.
func (ex *connExecutor) stmtDoesntNeedRetry(stmt tree.Statement) bool {
	wrap := Statement{AST: stmt}
	return isSavepoint(wrap) || isSetTransaction(wrap)
}

func stateToTxnStatusIndicator(s fsm.State) TransactionStatusIndicator {
	switch s.(type) {
	case stateOpen:
		return InTxnBlock
	case stateAborted:
		return InFailedTxnBlock
	case stateRestartWait:
		return InTxnBlock
	case stateNoTxn:
		return IdleTxnBlock
	case stateCommitWait:
		return InTxnBlock
	default:
		panic(fmt.Sprintf("unknown state: %T", s))
	}
}

type descriptorsPolicy int

const (
	useCachedDescs descriptorsPolicy = iota
	disallowCachedDescs
)

func (ex *connExecutor) execPrepare(
	ctx context.Context, parseCmd PrepareStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	// The anonymous statement can be overwritter.
	if parseCmd.Name != "" {
		if _, ok := ex.prepStmtsNamespace.prepStmts[parseCmd.Name]; ok {
			err := pgerror.NewErrorf(
				pgerror.CodeDuplicatePreparedStatementError,
				"prepared statement %q already exists", parseCmd.Name,
			)
			return retErr(err)
		}
	} else {
		// Deallocate the unnamed statement, if it exists.
		ex.deletePreparedStmt(ctx, "")
	}

	ps, err := ex.addPreparedStmt(
		ctx, parseCmd.Name, Statement{AST: parseCmd.Stmt}, parseCmd.TypeHints,
	)
	if err != nil {
		return retErr(err)
	}

	// Convert the inferred SQL types back to an array of pgwire Oids.
	inTypes := make([]oid.Oid, 0, len(ps.TypeHints))
	if len(ps.TypeHints) > pgwirebase.MaxPreparedStatementArgs {
		return retErr(
			pgwirebase.NewProtocolViolationErrorf(
				"more than %d arguments to prepared statement: %d",
				pgwirebase.MaxPreparedStatementArgs, len(ps.TypeHints)))
	}
	for k, t := range ps.TypeHints {
		i, err := strconv.Atoi(k)
		if err != nil || i < 1 {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeUndefinedParameterError, "invalid placeholder name: $%s", k))
		}
		// Placeholder names are 1-indexed; the arrays in the protocol are
		// 0-indexed.
		i--
		// Grow inTypes to be at least as large as i. Prepopulate all
		// slots with the hints provided, if any.
		for j := len(inTypes); j <= i; j++ {
			inTypes = append(inTypes, 0)
			if j < len(parseCmd.RawTypeHints) {
				inTypes[j] = parseCmd.RawTypeHints[j]
			}
		}
		// OID to Datum is not a 1-1 mapping (for example, int4 and int8
		// both map to TypeInt), so we need to maintain the types sent by
		// the client.
		if inTypes[i] != 0 {
			continue
		}
		inTypes[i] = t.Oid()
	}
	for i, t := range inTypes {
		if t == 0 {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeIndeterminateDatatypeError,
				"could not determine data type of placeholder $%d", i+1))
		}
	}
	// Remember the inferred placeholder types so they can be reported on
	// Describe.
	ps.InTypes = inTypes
	return nil, nil
}

func (ex *connExecutor) execBind(
	ctx context.Context, bindCmd BindStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	portalName := bindCmd.PortalName
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := ex.prepStmtsNamespace.portals[portalName]; ok {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeDuplicateCursorError, "portal %q already exists", portalName))
		}
	} else {
		// Deallocate the unnamed portal, if it exists.
		ex.deletePortal(ctx, "")
	}

	ps, ok := ex.prepStmtsNamespace.prepStmts[bindCmd.PreparedStatementName]
	if !ok {
		return retErr(pgerror.NewErrorf(
			pgerror.CodeInvalidSQLStatementNameError,
			"unknown prepared statement %q", bindCmd.PreparedStatementName))
	}

	numQArgs := uint16(len(ps.InTypes))
	qArgFormatCodes := bindCmd.ArgFormatCodes

	if len(qArgFormatCodes) != 1 && len(qArgFormatCodes) != int(numQArgs) {
		return retErr(pgwirebase.NewProtocolViolationErrorf(
			"wrong number of format codes specified: %d for %d arguments",
			len(qArgFormatCodes), numQArgs))
	}
	// If a single format code was specified, it applies to all the arguments.
	if len(qArgFormatCodes) == 1 {
		fmtCode := qArgFormatCodes[0]
		qArgFormatCodes = make([]pgwirebase.FormatCode, numQArgs)
		for i := range qArgFormatCodes {
			qArgFormatCodes[i] = fmtCode
		}
	}

	if len(bindCmd.Args) != int(numQArgs) {
		return retErr(
			pgwirebase.NewProtocolViolationErrorf(
				"expected %d arguments, got %d", numQArgs, len(bindCmd.Args)))
	}
	qargs := tree.QueryArguments{}
	for i, arg := range bindCmd.Args {
		k := strconv.Itoa(i + 1)
		t := ps.InTypes[i]
		if arg == nil {
			// nil indicates a NULL argument value.
			qargs[k] = tree.DNull
		} else {
			d, err := pgwirebase.DecodeOidDatum(t, qArgFormatCodes[i], arg)
			if err != nil {
				if _, ok := err.(*pgerror.Error); ok {
					return retErr(err)
				} else {
					return retErr(pgwirebase.NewProtocolViolationErrorf(
						"error in argument for $%d: %s", i+1, err.Error()))
				}
			}
			qargs[k] = d
		}
	}

	numCols := len(ps.Columns)
	if numCols != 0 && (len(bindCmd.OutFormats) > 1) && (len(bindCmd.OutFormats) != numCols) {
		return retErr(pgwirebase.NewProtocolViolationErrorf(
			"expected 0, 1, or %d for number of format codes, got %d",
			numCols, len(bindCmd.OutFormats)))
	}

	columnFormatCodes := bindCmd.OutFormats
	if len(bindCmd.OutFormats) == 1 {
		// Apply the format code to every column.
		columnFormatCodes = make([]pgwirebase.FormatCode, numCols)
		for i := 0; i < numCols; i++ {
			columnFormatCodes[i] = bindCmd.OutFormats[0]
		}
	}

	// Create the new PreparedPortal.
	if err := ex.addPortal(
		ctx, portalName, bindCmd.PreparedStatementName, ps.PreparedStatement, qargs, columnFormatCodes,
	); err != nil {
		return retErr(err)
	}

	if log.V(2) {
		log.Infof(ctx, "portal: %q for %q, args %q, formats %q",
			portalName, ps.Statement, qargs, columnFormatCodes)
	}

	return nil, nil
}

// addPreparedStmt creates a new PreparedStatement with the provided name using
// the given query. The new prepared statement is added to the connExecutor and
// also returned. It is illegal to call this when a statement with that name
// already exists (even for anonymous prepared statements).
//
// placeholderHints are used to assist in inferring placeholder types.
func (ex *connExecutor) addPreparedStmt(
	ctx context.Context, name string, stmt Statement, placeholderHints tree.PlaceholderTypes,
) (*PreparedStatement, error) {
	if _, ok := ex.prepStmtsNamespace.prepStmts[name]; ok {
		panic(fmt.Sprintf("prepared statement already exists: %q", name))
	}

	// Prepare the query. This completes the typing of placeholders.
	prepared, err := ex.Prepare(ctx, stmt, placeholderHints)
	if err != nil {
		return nil, err
	}

	prepared.memAcc.Grow(ctx, int64(len(name)))
	ex.prepStmtsNamespace.prepStmts[name] = prepStmtEntry{
		PreparedStatement: prepared,
		portals:           make(map[string]struct{}),
	}
	return prepared, nil
}

// addPortal creates a new PreparedPortal on the connExecutor.
//
// It is illegal to call this when a portal with that name already exists (even
// for anonymous portals).
func (ex *connExecutor) addPortal(
	ctx context.Context,
	portalName string,
	psName string,
	stmt *PreparedStatement,
	qargs tree.QueryArguments,
	outFormats []pgwirebase.FormatCode,
) error {
	if _, ok := ex.prepStmtsNamespace.portals[portalName]; ok {
		panic(fmt.Sprintf("portal already exists: %q"))
	}

	portal, err := ex.newPreparedPortal(ctx, portalName, stmt, qargs, outFormats)
	if err != nil {
		return err
	}

	ex.prepStmtsNamespace.portals[portalName] = portalEntry{
		PreparedPortal: portal,
		psName:         psName,
	}
	ex.prepStmtsNamespace.prepStmts[psName].portals[portalName] = struct{}{}
	return nil
}

func (ex *connExecutor) deletePreparedStmt(ctx context.Context, name string) {
	psEntry, ok := ex.prepStmtsNamespace.prepStmts[name]
	if !ok {
		return
	}
	// If the prepared statement only exists in prepStmtsNamespace, it's up to us
	// to close it.
	baseP, inBase := ex.prepStmtsNamespaceAtTxnStartPos.prepStmts[name]
	if !inBase || (baseP.PreparedStatement != psEntry.PreparedStatement) {
		psEntry.close(ctx)
	}
	for portalName, _ := range psEntry.portals {
		ex.deletePortal(ctx, portalName)
	}
	delete(ex.prepStmtsNamespace.prepStmts, name)
}

func (ex *connExecutor) deletePortal(ctx context.Context, name string) {
	portalEntry, ok := ex.prepStmtsNamespace.portals[name]
	if !ok {
		return
	}
	// If the portal only exists in prepStmtsNamespace, it's up to us to close it.
	baseP, inBase := ex.prepStmtsNamespaceAtTxnStartPos.portals[name]
	if !inBase || (baseP.PreparedPortal != portalEntry.PreparedPortal) {
		portalEntry.close(ctx)
	}
	delete(ex.prepStmtsNamespace.portals, name)
	delete(ex.prepStmtsNamespace.prepStmts[portalEntry.psName].portals, name)
}

func (ex *connExecutor) execDelPrepStmt(
	ctx context.Context, delCmd DeletePreparedStmt,
) (fsm.Event, fsm.EventPayload) {
	switch delCmd.Type {
	case pgwirebase.PrepareStatement:
		_, ok := ex.prepStmtsNamespace.prepStmts[delCmd.Name]
		if !ok {
			// The spec says "It is not an error to issue Close against a nonexistent
			// statement or portal name.". See
			// https://www.postgresql.org/docs/current/static/protocol-flow.html.
			break
		}

		ex.deletePreparedStmt(ctx, delCmd.Name)
	case pgwirebase.PreparePortal:
		_, ok := ex.prepStmtsNamespace.portals[delCmd.Name]
		if !ok {
			break
		}
		ex.deletePortal(ctx, delCmd.Name)
	default:
		panic(fmt.Sprintf("unknown del type: %s", delCmd.Type))
	}
	return nil, nil
}

func (ex *connExecutor) execDescribe(
	ctx context.Context, descCmd DescribeStmt, res DescribeResult,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	switch descCmd.Type {
	case pgwirebase.PrepareStatement:
		ps, ok := ex.prepStmtsNamespace.prepStmts[descCmd.Name]
		if !ok {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeInvalidSQLStatementNameError,
				"unknown prepared statement %q", descCmd.Name))
		}

		res.SetInTypes(ps.InTypes)

		if stmtHasNoData(ps.Statement) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPrepStmtOutput(ctx, ps.Columns)
		}
	case pgwirebase.PreparePortal:
		portal, ok := ex.prepStmtsNamespace.portals[descCmd.Name]
		if !ok {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeInvalidCursorNameError, "unknown portal %q", descCmd.Name))
		}

		if stmtHasNoData(portal.Stmt.Statement) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPortalOutput(ctx, portal.Stmt.Columns, portal.OutFormats)
		}
	default:
		return retErr(errors.Errorf("unknown describe type: %s", descCmd.Type))
	}
	return nil, nil
}

// stmtHasNoData returns true if describing a result of the input statement
// type should return NoData.
func stmtHasNoData(stmt tree.Statement) bool {
	return stmt == nil || stmt.StatementType() != tree.Rows
}

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
// pinfo: The values to use for the statement's placeholders. If nil is passed,
// 	 then the statement cannot have any placeholder.
// pos: The position of stmt.
func (ex *connExecutor) execStmt(
	ctx context.Context,
	stmt Statement,
	res RestrictedCommandResult,
	pinfo *tree.PlaceholderInfo,
	pos CmdPos,
) (fsm.Event, fsm.EventPayload, error) {
	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := stmt.AST.(tree.ObserverStatement); ok {
		err := ex.runObserverStatement(ctx, stmt, res)
		return nil, nil, err
	}

	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", stmt, ex.machine.CurState())
	}

	queryID := ex.generateQueryID()
	stmt.queryID = queryID

	// Dispatch the statement for execution based on the current state.
	var ev fsm.Event
	var payload fsm.EventPayload
	var err error
	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		ev, payload = ex.execStmtInNoTxnState(ctx, stmt)
	case stateOpen:
		ev, payload, err = ex.execStmtInOpenState(ctx, stmt, pinfo, res)

		if filter := ex.server.cfg.TestingKnobs.StatementFilter; err == nil && filter != nil {
			var execErr error
			if perr, ok := payload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, stmt.String(), execErr)
		}

		// If the execution generated an event, we short-circuit here. Otherwise,
		// if we're told that nothing exceptional happened, we have further loose
		// ends to tie.
		if err != nil || ev != nil {
			break
		}
		autoCommit := ex.machine.CurState().(stateOpen).ImplicitTxn.Get()
		if autoCommit {
			autoCommitErr := ex.handleAutoCommit(ctx, stmt.AST)
			if autoCommitErr != nil {
				ev, payload = ex.makeErrEvent(autoCommitErr, stmt.AST)
			}
		}
		if ev == nil && autoCommit {
			if payload != nil {
				panic("nil event but payload")
			}
			ev = eventTxnFinish{}
			payload = eventTxnFinishPayload{commit: true}
		}
		return ev, payload, nil

	case stateAborted, stateRestartWait:
		ev, payload = ex.execStmtInAbortedState(ctx, stmt, res)
	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	return ev, payload, err
}

// runObserverStatement executes the given observer statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runObserverStatement(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) error {
	switch sqlStmt := stmt.AST.(type) {
	case *tree.ShowTransactionStatus:
		return ex.runShowTransactionState(ctx, res)
	case *tree.ShowSyntax:
		return ex.runShowSyntax(ctx, sqlStmt.Statement, res)
	default:
		res.SetError(pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: unrecognized observer statement type %T", stmt.AST))
		return nil
	}
}

// runShowTransactionState executes a SHOW SYNTAX <stmt> query.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSyntax(
	ctx context.Context, stmt string, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{
		{Name: "field", Typ: types.String},
		{Name: "message", Typ: types.String},
	})
	var commErr error
	if err := runShowSyntax(ctx, stmt,
		func(ctx context.Context, field, msg string) error {
			commErr = res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
			return nil
		}); err != nil {
		res.SetError(err)
	}
	return commErr
}

// runShowTransactionState executes a SHOW TRANSACTION STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

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
		ex.mu.LastActiveQuery = qm.stmt

		ex.mu.Unlock()
	}
}

// setTxnStartPos updates the position to which future rewinds will refer.
//
// All statements with lower position in stmtBuf (if any) are removed, as we
// won't ever need them again.
func (ex *connExecutor) setTxnStartPos(ctx context.Context, pos CmdPos) {
	if pos <= ex.txnStartPos {
		log.Fatalf(ctx, "can only move the  txnStartPos forward."+
			"Was: %d; new value: %d", ex.txnStartPos, pos)
	}
	ex.txnStartPos = pos
	ex.stmtBuf.ltrim(ctx, pos)
	ex.commitPrepStmtNamespace(ctx)
}

// commitPrepStmtNamespace deallocates everything in
// prepStmtsNamespaceAtTxnStartPos that's not part of prepStmtsNamespace.
func (ex *connExecutor) commitPrepStmtNamespace(ctx context.Context) {
	ex.prepStmtsNamespaceAtTxnStartPos.resetTo(ctx, &ex.prepStmtsNamespace)
}

// commitPrepStmtNamespace deallocates everything in prepStmtsNamespace that's
// not part of prepStmtsNamespaceAtTxnStartPos.
func (ex *connExecutor) rewindPrepStmtNamespace(ctx context.Context) {
	ex.prepStmtsNamespace.resetTo(ctx, &ex.prepStmtsNamespaceAtTxnStartPos)
}

// getRewindTxnCapability checks whether rewinding to the position previously
// set through setTxnStartPos() is possible and, if it is, returns a
// rewindCapability bound to that position. The returned bool is true if the
// rewind is possible. If it is, client communication is blocked until the
// rewindCapability is exercised.
func (ex *connExecutor) getRewindTxnCapability() (rewindCapability, bool) {
	cl := ex.clientComm.LockCommunication()

	// If we already delivered results at or past the start position, we can't
	// rewind.
	if cl.ClientPos() >= ex.txnStartPos {
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
	_, retriable := err.(*roachpb.HandledRetryableTxnError)
	if retriable {
		if _, inOpen := ex.machine.CurState().(stateOpen); !inOpen {
			log.Fatalf(ex.Ctx(), "retriable error in unexpected state: %#v",
				ex.machine.CurState())
		}
	}
	if retriable {
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
func (ex *connExecutor) handleAutoCommit(ctx context.Context, stmt tree.Statement) error {
	_, inOpen := ex.machine.CurState().(stateOpen)
	if !inOpen {
		log.Fatalf(ctx, "handleAutoCommit called in state: %#v", ex.machine.CurState())
	}
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		return nil
	}
	var skipCommit bool
	var err error
	if knob := ex.server.cfg.TestingKnobs.BeforeAutoCommit; knob != nil {
		err = knob(ctx, stmt.String())
		skipCommit = err != nil
	}
	if !skipCommit {
		err = txn.Commit(ctx)
	}
	log.VEventf(ctx, 2, "AutoCommit. err: %v", err)
	if err != nil {
		return err
	}
	return nil
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
	// This method cheats and manipulates ex.state directly, not through an event.
	// The alternative would be to create a special event, but it's unclear how
	// that'd work given that this method is called while executing a statement.

	// Transform the transaction options into the types needed by the state
	// machine.
	if modes.UserPriority != tree.UnspecifiedUserPriority {
		pri, err := priorityToProto(modes.UserPriority)
		if err != nil {
			return err
		}
		if err := ex.state.setPriority(pri); err != nil {
			return err
		}
	}
	if modes.Isolation != tree.UnspecifiedIsolation {
		iso, err := ex.isolationToProto(modes.Isolation)
		if err != nil {
			return err
		}
		if err := ex.state.setIsolationLevel(iso); err != nil {
			return err
		}
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
		ex.server.StatementCounters.incrementCount(stmt.AST)
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
				iso, pri, ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode),
				ex.server.cfg.Clock.PhysicalTime(),
				ex.transitionCtx)
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
				mode,
				ex.server.cfg.Clock.PhysicalTime(),
				ex.transitionCtx)
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
// The returned event can be nil if no state transition is required.
func (ex *connExecutor) execStmtInOpenState(
	ctx context.Context,
	stmt Statement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload, error) {
	ex.server.StatementCounters.incrementCount(stmt.AST)

	os := ex.machine.CurState().(stateOpen)

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, stmt.AST)
		return ev, payload, nil
	}

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancellation function here.
	unregisterFun := ex.addActiveQuery(stmt.queryID, stmt.AST, ex.state.cancel)
	// We tie unregistering to closing the result. We might take the
	// responsibility back later if it turns out that we're going to execute this
	// query through the parallelize queue.
	res.SetFinishedCallback(unregisterFun)

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
		res.ResetStmtType((*tree.CommitTransaction)(nil))
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

	case *tree.Prepare:
		name := s.Name.String()
		if _, ok := ex.prepStmtsNamespace.prepStmts[name]; ok {
			err := pgerror.NewErrorf(
				pgerror.CodeDuplicatePreparedStatementError,
				"prepared statement %q already exists", name,
			)
			return makeErrEvent(err)
		}
		typeHints := make(tree.PlaceholderTypes, len(s.Types))
		for i, t := range s.Types {
			typeHints[strconv.Itoa(i+1)] = coltypes.CastTargetToDatumType(t)
		}
		if _, err := ex.addPreparedStmt(ctx, name, Statement{AST: s.Statement}, typeHints); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil

	case *tree.Execute:
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution below.
		name := s.Name.String()
		ps, ok := ex.prepStmtsNamespace.prepStmts[name]
		if !ok {
			err := pgerror.NewErrorf(
				pgerror.CodeInvalidSQLStatementNameError,
				"prepared statement %q does not exist", name,
			)
			return makeErrEvent(err)
		}
		var err error
		pinfo, err = fillInPlaceholders(ps.PreparedStatement, name, s.Params, ex.sessionData.SearchPath)
		if err != nil {
			return makeErrEvent(err)
		}

		stmt.AST = ps.Statement
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		res.ResetStmtType(ps.Statement)
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
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
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
		res.SetFinishedCallback(nil)
		cols, err := ex.execStmtInParallel(ex.Ctx(), stmt, p, unregisterFun)
		if err != nil {
			return makeErrEvent(err)
		}
		// Produce mocked out results for the query - the "zero value" of the
		// statement's result type:
		// - tree.Rows -> an empty set of rows
		// - tree.RowsAffected -> zero rows affected
		if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
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
	txn := ex.state.mu.txn
	var clusterTimestamp hlc.Timestamp
	// txn can be nil if we're setting this context up for preparing a statement
	// and we are outside of a transaction,
	if txn != nil {
		clusterTimestamp = ex.state.mu.txn.OrigTimestamp()
	}

	scInterface := newSchemaInterface(&ex.extraTxnState.tables, ex.server.cfg.VirtualSchemas)

	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Planner:       p,
			Sequence:      p,
			StmtTimestamp: stmtTs,

			Txn:              txn,
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
			ClusterTimestamp: clusterTimestamp,
			ClusterID:        ex.server.cfg.ClusterID(),
			NodeID:           ex.server.cfg.NodeID.Get(),
			ReCache:          ex.server.reCache,
		},
		SessionMutator:  ex.dataMutator,
		VirtualSchemas:  ex.server.cfg.VirtualSchemas,
		Tracing:         &ex.state.tracing,
		StatusServer:    ex.server.cfg.StatusServer,
		MemMetrics:      ex.memMetrics,
		Tables:          &ex.extraTxnState.tables,
		ExecCfg:         ex.server.cfg,
		DistSQLPlanner:  ex.server.cfg.DistSQLPlanner,
		TxnModesSetter:  ex,
		SchemaChangers:  &ex.extraTxnState.schemaChangers,
		schemaAccessors: scInterface,
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
	p := &planner{execCfg: ex.server.cfg}
	ex.resetPlanner(p, txn, stmtTs)
	return p
}

// resetPlanner re-initializes a planner so it can can be used for planning a
// query in the context of this session.
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
	p.preparedStatements = ex.getPrepStmtsAccessor()
	p.autoCommit = false
	p.isPreparing = false
	p.asOfSystemTime = false
}

// connExPrepStmtsAccessor is an implementation of preparedStatementsAccessor
// that gives access to a connExecutor's prepared statements.
type connExPrepStmtsAccessor struct {
	ex *connExecutor
}

var _ preparedStatementsAccessor = connExPrepStmtsAccessor{}

// Get is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Get(name string) (*PreparedStatement, bool) {
	s, ok := ps.ex.prepStmtsNamespace.prepStmts[name]
	return s.PreparedStatement, ok
}

// Delete is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Delete(ctx context.Context, name string) bool {
	_, ok := ps.Get(name)
	if !ok {
		return false
	}
	ps.ex.deletePreparedStmt(ctx, name)
	return true
}

// DeleteAll is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) DeleteAll(ctx context.Context) {
	ps.ex.prepStmtsNamespace = prepStmtNamespace{
		prepStmts: make(map[string]prepStmtEntry),
		portals:   make(map[string]portalEntry),
	}
}

func (ex *connExecutor) getPrepStmtsAccessor() preparedStatementsAccessor {
	return connExPrepStmtsAccessor{
		ex: ex,
	}
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
// queries, so this method can only be used with local SQL.
func (ex *connExecutor) execStmtInParallel(
	ctx context.Context, stmt Statement, planner *planner, unregisterQuery func(),
) (sqlbase.ResultColumns, error) {
	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: planner.ExtendedEvalContext(),
		p:               planner,
	}

	if err := planner.makePlan(ctx, stmt); err != nil {
		planner.maybeLogStatement(ctx, "par-prepare" /* lbl */, 0 /* rows */, err)
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

		res := &errOnlyRestrictedCommandResult{}

		defer func() {
			planner.maybeLogStatement(ctx, "par-exec" /* lbl */, res.RowsAffected(), res.Err())
		}()

		if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
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
			res.RowsAffected(), err, &ex.server.EngineMetrics)
		if ex.server.cfg.TestingKnobs.AfterExecute != nil {
			ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
		}

		if err != nil {
			// I think this can't happen; if it does, it's unclear how to react when a
			// this "connection" is toast.
			log.Warningf(ctx, "Connection error from the parallel queue. How can that "+
				"be?", err)
			res.SetError(err)
			return err
		}
		return res.Err()
	}); err != nil {
		planner.maybeLogStatement(ctx, "par-queue" /* lbl */, 0 /* rows */, err)
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
	useOptimizer := shouldUseOptimizer(ex.sessionData.OptimizerMode, stmt)
	var err error
	if useOptimizer {
		// Experimental path (disabled by default).
		err = planner.makeOptimizerPlan(ctx, stmt)
	} else {
		err = planner.makePlan(ctx, stmt)
	}

	defer func() { planner.maybeLogStatement(ctx, "exec", res.RowsAffected(), res.Err()) }()

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
	if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
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
	if err != nil {
		res.SetError(err)
		return err
	}
	recordStatementSummary(
		planner, stmt, useDistSQL, ex.extraTxnState.autoRetryCounter,
		res.RowsAffected(), res.Err(), &ex.server.EngineMetrics,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return nil
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

	if err := planner.curPlan.start(params); err != nil {
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
		var commErr error
		queryErr := ex.forEachRow(params, planner.curPlan.plan, func(values tree.Datums) error {
			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			commErr = res.AddRow(ctx, values)
			return commErr
		})
		if commErr != nil {
			return commErr
		}
		if queryErr != nil {
			res.SetError(queryErr)
		}
		return nil
	default:
		// Calling StartPlan is sufficient for other statement types.
		return nil
	}
}

// exectWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
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
//
// f is not allowed to hold on to the row slice. It needs to make a copy if it
// want to use the memory later.
//
// Errors returned by this method are to be considered query errors. If the
// caller wants to handle some errors within the callback differently, it has to
// capture those itself.
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

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (ex *connExecutor) execStmtInAbortedState(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	_, inRestartWait := ex.machine.CurState().(stateRestartWait)

	// TODO(andrei/cuongdo): Figure out what statements to count here.
	switch s := stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		if inRestartWait {
			ev, payload := ex.rollbackSQLTransaction(ctx)
			return ev, payload
		}

		// Note: Postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		res.ResetStmtType((*tree.RollbackTransaction)(nil))

		return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
	case *tree.RollbackToSavepoint, *tree.Savepoint:
		// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
		// "SAVEPOINT cockroach_restart" commands to indicate client intent to
		// retry a transaction in a RestartWait state.
		var spName string
		switch n := s.(type) {
		case *tree.RollbackToSavepoint:
			spName = n.Savepoint
		case *tree.Savepoint:
			spName = n.Name
		default:
			panic("unreachable")
		}
		if err := tree.ValidateRestartCheckpoint(spName); err != nil {
			ev := eventNonRetriableErr{IsCommit: fsm.False}
			payload := eventNonRetriableErrPayload{
				err: err,
			}
			return ev, payload
		}

		if !(inRestartWait || ex.machine.CurState().(stateAborted).RetryIntent.Get()) {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			ev := eventNonRetriableErr{IsCommit: fsm.False}
			payload := eventNonRetriableErrPayload{
				err: err,
			}
			return ev, payload
		}

		res.ResetStmtType((*tree.RollbackTransaction)(nil))

		if inRestartWait {
			return eventTxnRestart{}, nil
		}
		// We accept ROLLBACK TO SAVEPOINT even after non-retryable errors to make
		// it easy for client libraries that want to indiscriminately issue
		// ROLLBACK TO SAVEPOINT after every error and possibly follow it with a
		// ROLLBACK and also because we accept ROLLBACK TO SAVEPOINT in the Open
		// state, so this is consistent.
		// We start a new txn with the same sql timestamp and isolation as the
		// current one.

		ev := eventTxnStart{
			ImplicitTxn: fsm.False,
		}
		rwMode := tree.ReadWrite
		if ex.state.readOnly {
			rwMode = tree.ReadOnly
		}
		payload := makeEventTxnStartPayload(
			ex.state.isolation, ex.state.priority,
			rwMode, ex.state.sqlTimestamp,
			ex.transitionCtx)
		return ev, payload
	default:
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		if inRestartWait {
			payload := eventNonRetriableErrPayload{
				err: sqlbase.NewTransactionAbortedError(
					"Expected \"ROLLBACK TO SAVEPOINT COCKROACH_RESTART\"" /* customMsg */),
			}
			return ev, payload
		}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */),
		}
		return ev, payload
	}
}

// execStmtInCommitWaitState executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (ex *connExecutor) execStmtInCommitWaitState(
	stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	if _, ok := ex.machine.CurState().(stateCommitWait); !ok {
		panic(fmt.Sprintf("execStmtInCommitWaitState called in state: %s",
			ex.machine.CurState()))
	}
	ex.server.StatementCounters.incrementCount(stmt.AST)
	switch stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		// Reply to a rollback with the COMMIT tag, by analogy to what we do when we
		// get a COMMIT in state Aborted.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	default:
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionCommittedError(),
		}
		return ev, payload
	}
}

// txnStateTransitionsApplyWrapper is a wrapper on top of Machine built with the
// TxnStateTransitions above. Its point is to detect when we go in and out of
// transactions and update some state.
//
// Any returned error indicates an unrecoverable error for the session;
// execution on this connection should be interrupted.
func (ex *connExecutor) txnStateTransitionsApplyWrapper(
	ctx context.Context, ev fsm.Event, payload fsm.EventPayload, res ResultBase, pos CmdPos,
) (advanceInfo, error) {

	var implicitTxn bool
	if so, ok := ex.machine.CurState().(stateOpen); ok {
		implicitTxn = so.ImplicitTxn.Get()
	}

	err := ex.machine.ApplyWithPayload(ctx, ev, payload)
	if err != nil {
		if _, ok := err.(fsm.TransitionNotFoundError); ok {
			panic(err)
		}
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
	case txnStart:
		break
	case txnCommit:
		// If we have schema changers to run, release leases early so that schema
		// changers can run.
		if len(ex.extraTxnState.schemaChangers.schemaChangers) > 0 {
			ex.extraTxnState.tables.releaseLeases(ex.Ctx())
		}
		// TODO(andrei): figure out how session tracing should interact with schema
		// changes.
		if schemaChangeErr := ex.extraTxnState.schemaChangers.execSchemaChanges(
			ctx, ex.server.cfg,
		); schemaChangeErr != nil {
			// We got a schema change error. We'll return it to the client as the
			// result of the current statement - which is either the DDL statement or
			// a COMMIT statement if the DDL was part of an explicit transaction. In
			// the explicit transaction case, we return a funky error code to the
			// client to seed fear about what happened to the transaction. The reality
			// is that the transaction committed, but at least some of the staged
			// schema changes failed. We don't have a good way to indicate this.
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
		ex.extraTxnState.reset(ex.Ctx(), advInfo.txnEvent, ex.server.dbCache)
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
	ctx context.Context,
	res RestrictedCommandResult,
	stmt Statement,
	cols sqlbase.ResultColumns,
) error {
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			return err
		}
	}
	if stmt.AST.StatementType() == tree.Rows {
		// Note that this call is necessary even if cols is nil.
		res.SetColumns(ctx, cols)
	}
	return nil
}

// newStatsCollector returns an sqlStatsCollector that will record stats in the
// session's stats containers.
func (ex *connExecutor) newStatsCollector() sqlStatsCollector {
	return newSQLStatsCollectorImpl(&ex.server.sqlStats, ex.appStats, ex.phaseTimes)
}

// Prepare prepares the given statement.
//
// placeholderHints may contain partial type information for placeholders.
// Prepare will populate the missing types.
//
// The PreparedStatement is returned (or nil if there are no results). The
// returned PreparedStatement needs to be close()d once its no longer in use.
func (ex *connExecutor) Prepare(
	ctx context.Context,
	stmt Statement,
	placeholderHints tree.PlaceholderTypes,
) (*PreparedStatement, error) {
	prepared := &PreparedStatement{
		TypeHints: placeholderHints,
		memAcc:    ex.sessionMon.MakeBoundAccount(),
	}
	// NB: if we start caching the plan, we'll want to keep around the memory
	// account used for the plan, rather than clearing it.
	defer prepared.memAcc.Clear(ctx)

	if stmt.AST == nil {
		return prepared, nil
	}
	prepared.Str = stmt.String()

	prepared.Statement = stmt.AST
	prepared.AnonymizedStr = anonymizeStmt(stmt)

	if err := placeholderHints.ProcessPlaceholderAnnotations(stmt.AST); err != nil {
		return nil, err
	}
	// Preparing needs a transaction because it needs to retrieve db/table
	// descriptors for type checking.
	txn := client.NewTxn(ex.server.cfg.DB, ex.server.cfg.NodeID.Get(), client.RootTxn)

	// Create a plan for the statement to figure out the typing, then close the
	// plan.
	if err := func() error {
		p := &ex.planner
		ex.resetPlanner(p, txn, ex.server.cfg.Clock.PhysicalTime() /* stmtTimestamp */)
		p.semaCtx.Placeholders.SetTypeHints(placeholderHints)
		p.extendedEvalCtx.PrepareOnly = true
		p.extendedEvalCtx.ActiveMemAcc = &prepared.memAcc
		// constantMemAcc accounts for all constant folded values that are computed
		// prior to any rows being computed.
		constantMemAcc := p.extendedEvalCtx.Mon.MakeBoundAccount()
		p.extendedEvalCtx.ActiveMemAcc = &constantMemAcc
		defer constantMemAcc.Close(ctx)

		protoTS, err := isAsOf(stmt.AST, p.EvalContext(), ex.server.cfg.Clock.Now() /* max */)
		if err != nil {
			return err
		}
		if protoTS != nil {
			p.asOfSystemTime = true
			// We can't use cached descriptors anywhere in this query, because
			// we want the descriptors at the timestamp given, not the latest
			// known to the cache.
			p.avoidCachedDescriptors = true
			txn.SetFixedTimestamp(ctx, *protoTS)
		}

		if err := p.prepare(ctx, stmt.AST); err != nil {
			return err
		}
		if p.curPlan.plan == nil {
			// The statement cannot be prepared. Nothing to do.
			return nil
		}
		defer p.curPlan.close(ctx)

		prepared.Columns = p.curPlan.columns()
		for _, c := range prepared.Columns {
			if err := checkResultType(c.Typ); err != nil {
				return err
			}
		}
		prepared.Types = p.semaCtx.Placeholders.Types
		return nil
	}(); err != nil {
		return nil, err
	}

	// Account for the memory used by this prepared statement: for now we are just
	// counting the size of the query string (we'll account for the statement name
	// at a higher layer). When we start storing the prepared query plan during
	// prepare, this should be tallied up to the monitor as well.
	if err := prepared.memAcc.Grow(ctx,
		int64(len(prepared.Str)+int(unsafe.Sizeof(*prepared))),
	); err != nil {
		return nil, err
	}

	return prepared, nil
}

// cancelQuery is part of the registrySession interface.
func (ex *connExecutor) cancelQuery(queryID uint128.Uint128) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if queryMeta, exists := ex.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancel()
		return true
	}
	return false
}

// user is part of the registrySession interface.
func (ex *connExecutor) user() string {
	return ex.sessionData.User
}

// serialize is part of the registrySession interface.
func (ex *connExecutor) serialize() serverpb.Session {
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	ex.state.mu.RLock()
	defer ex.state.mu.RUnlock()

	var kvTxnID *uuid.UUID
	txn := ex.state.mu.txn
	if txn != nil {
		id := txn.ID()
		kvTxnID = &id
	}

	activeQueries := make([]serverpb.ActiveQuery, 0, len(ex.mu.ActiveQueries))
	truncateSQL := func(sql string) string {
		if len(sql) > MaxSQLBytes {
			sql = sql[:MaxSQLBytes-utf8.RuneLen('…')]
			// Ensure the resulting string is valid utf8.
			for {
				if r, _ := utf8.DecodeLastRuneInString(sql); r != utf8.RuneError {
					break
				}
				sql = sql[:len(sql)-1]
			}
			sql += "…"
		}
		return sql
	}

	for id, query := range ex.mu.ActiveQueries {
		if query.hidden {
			continue
		}
		sql := truncateSQL(query.stmt.String())
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			ID:            id.String(),
			Start:         query.start.UTC(),
			Sql:           sql,
			IsDistributed: query.isDistributed,
			Phase:         (serverpb.ActiveQuery_Phase)(query.phase),
		})
	}
	lastActiveQuery := ""
	if ex.mu.LastActiveQuery != nil {
		lastActiveQuery = truncateSQL(ex.mu.LastActiveQuery.String())
	}

	remoteStr := "<admin>"
	if ex.sessionData.RemoteAddr != nil {
		remoteStr = ex.sessionData.RemoteAddr.String()
	}

	return serverpb.Session{
		Username:        ex.sessionData.User,
		ClientAddress:   remoteStr,
		ApplicationName: ex.sessionData.ApplicationName(),
		Start:           ex.phaseTimes[sessionInit].UTC(),
		ActiveQueries:   activeQueries,
		KvTxnID:         kvTxnID,
		LastActiveQuery: lastActiveQuery,
	}
}

// recordUnimplementedErrorMaybe takes an error and increments the
// "unimplemented" metric if the error has the corresponding pg code.
func (ex *connExecutor) recordUnimplementedErrorMaybe(err error) {
	if err == nil {
		return
	}
	if pgErr, ok := pgerror.GetPGCause(err); ok {
		if pgErr.Code == pgerror.CodeFeatureNotSupportedError {
			ex.server.recordUnimplementedFeature(pgErr.InternalCommand)
		}
	}
}

type StatementCounters struct {
	SelectCount   *metric.Counter
	TxnBeginCount *metric.Counter

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
}

func makeStatementCounters() StatementCounters {
	return StatementCounters{
		TxnBeginCount:    metric.NewCounter(MetaTxnBegin),
		TxnCommitCount:   metric.NewCounter(MetaTxnCommit),
		TxnAbortCount:    metric.NewCounter(MetaTxnAbort),
		TxnRollbackCount: metric.NewCounter(MetaTxnRollback),
		SelectCount:      metric.NewCounter(MetaSelect),
		UpdateCount:      metric.NewCounter(MetaUpdate),
		InsertCount:      metric.NewCounter(MetaInsert),
		DeleteCount:      metric.NewCounter(MetaDelete),
		DdlCount:         metric.NewCounter(MetaDdl),
		MiscCount:        metric.NewCounter(MetaMisc),
		QueryCount:       metric.NewCounter(MetaQuery),
	}
}

func (sc *StatementCounters) incrementCount(stmt tree.Statement) {
	sc.QueryCount.Inc(1)
	switch stmt.(type) {
	case *tree.BeginTransaction:
		sc.TxnBeginCount.Inc(1)
	case *tree.Select:
		sc.SelectCount.Inc(1)
	case *tree.Update:
		sc.UpdateCount.Inc(1)
	case *tree.Insert:
		sc.InsertCount.Inc(1)
	case *tree.Delete:
		sc.DeleteCount.Inc(1)
	case *tree.CommitTransaction:
		sc.TxnCommitCount.Inc(1)
	case *tree.RollbackTransaction:
		sc.TxnRollbackCount.Inc(1)
	default:
		if tree.CanModifySchema(stmt) {
			sc.DdlCount.Inc(1)
		} else {
			sc.MiscCount.Inc(1)
		}
	}
}
