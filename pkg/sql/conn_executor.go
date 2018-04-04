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
	"sort"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
// when its internal buffer overflows. In principle, delivery of result could be
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
// category of errors doesn't represent dramatic events as far as the connExecutor
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
// the client connection's lifetime and it is assigned to
// connEx.ctxHolder.connCtx. Below that, every SQL transaction has its own
// derived context because that's the level at which we trace operations. The
// lifetime of SQL transactions is determined by the txnState: the state machine
// decides when transactions start and end in txnState.performStateTransition().
// When we're inside a SQL transaction, most operations are considered to happen
// in the context of that txn. When there's no SQL transaction (i.e.
// stateNoTxn), everything happens in the connection's context.
//
// High-level code in connExecutor is agnostic of whether it currently is inside
// a txn or not. To deal with both cases, such methods don't explicitly take a
// context; instead they use connEx.Ctx(), which returns the appropriate ctx
// based on the current state.
// Lower-level code (everything from connEx.execStmt() and below which runs in
// between state transitions) knows what state its running in, and so the usual
// pattern of explicitly taking a context as an argument is used.

// Server is the top level singleton for handling SQL connections. It creates
// connExecutors to server every incoming connection.
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
	EngineMetrics EngineMetrics

	// StatementCounters contains metrics.
	StatementCounters StatementCounters

	// dbCache is a cache for database descriptors, maintained through Gossip
	// updates.
	dbCache *databaseCacheHolder

	errorCounts struct {
		syncutil.Mutex
		// Error returned by code.
		codes map[string]int64
		// Attempts to use unimplemented features.
		unimplemented map[string]int64
	}
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig, pool *mon.BytesMonitor) *Server {
	return &Server{
		cfg: cfg,
		EngineMetrics: EngineMetrics{
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

// recordError takes an error and increments the corresponding count for its
// error code, and, if it is an unimplemented error, the count for that feature.
func (s *Server) recordError(err error) {
	if err == nil {
		return
	}
	s.errorCounts.Lock()
	if s.errorCounts.codes == nil {
		s.errorCounts.codes = make(map[string]int64)
	}

	if pgErr, ok := pgerror.GetPGCause(err); ok {
		s.errorCounts.codes[pgErr.Code]++

		if pgErr.Code == pgerror.CodeFeatureNotSupportedError {
			if feature := pgErr.InternalCommand; feature != "" {
				if s.errorCounts.unimplemented == nil {
					s.errorCounts.unimplemented = make(map[string]int64)
				}
				s.errorCounts.unimplemented[feature]++
			}
		}
	} else {
		typ := util.ErrorSource(err)
		if typ == "" {
			typ = "unknown"
		}
		s.errorCounts.codes[typ]++
	}
	s.errorCounts.Unlock()
}

// FillErrorCounts fills the passed map with the server's current
// counts of how often individual unimplemented features have been encountered.
func (s *Server) FillErrorCounts(codes, unimplemented map[string]int64) {
	s.errorCounts.Lock()
	for k, v := range s.errorCounts.codes {
		codes[k] = v
	}
	for k, v := range s.errorCounts.unimplemented {
		unimplemented[k] = v
	}
	s.errorCounts.Unlock()
}

// ResetErrorCounts resets the counts of error types seen.
func (s *Server) ResetErrorCounts() {
	s.errorCounts.Lock()
	s.errorCounts.codes = make(map[string]int64, len(s.errorCounts.codes))
	s.errorCounts.unimplemented = make(map[string]int64, len(s.errorCounts.unimplemented))
	s.errorCounts.Unlock()
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
	cancel context.CancelFunc,
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
		mon:        &sessionRootMon,
		sessionMon: &sessionMon,
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
			connCtx:       ctx,
			txnAbortCount: s.StatementCounters.TxnAbortCount,
		},
		transitionCtx: transitionCtx{
			db:     s.cfg.DB,
			nodeID: s.cfg.NodeID.Get(),
			clock:  s.cfg.Clock,
			// Future transaction's monitors will inherits from sessionRootMon.
			connMon:  &sessionRootMon,
			tracer:   s.cfg.AmbientCtx.Tracer,
			settings: s.cfg.Settings,
		},
		parallelizeQueue: MakeParallelizeQueue(NewSpanBasedDependencyAnalyzer()),
		memMetrics:       memMetrics,
		appStats:         s.sqlStats.getStatsForApplication(args.ApplicationName),
		planner:          planner{execCfg: s.cfg},
	}
	ex.phaseTimes[sessionInit] = timeutil.Now()
	ex.extraTxnState.tables = TableCollection{
		leaseMgr:          s.cfg.LeaseManager,
		databaseCache:     s.dbCache.getDatabaseCache(),
		dbCacheSubscriber: s.dbCache,
	}
	ex.extraTxnState.txnRewindPos = -1

	ex.mu.ActiveQueries = make(map[ClusterWideID]*queryMeta)
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.dataMutator = sessionDataMutator{
		data: &ex.sessionData,
		defaults: sessionDefaults{
			applicationName: args.ApplicationName,
			database:        args.Database,
		},
		settings:       s.cfg.Settings,
		curTxnReadOnly: &ex.state.readOnly,
		applicationNameChanged: func(newName string) {
			ex.appStats = ex.server.sqlStats.getStatsForApplication(newName)
		},
	}
	ex.dataMutator.SetApplicationName(args.ApplicationName)
	ex.dataMutator.sessionTracing.ex = &ex
	ex.transitionCtx.sessionTracing = &ex.dataMutator.sessionTracing

	if traceSessionEventLogEnabled.Get(&s.cfg.Settings.SV) {
		remoteStr := "<admin>"
		if ex.sessionData.RemoteAddr != nil {
			remoteStr = ex.sessionData.RemoteAddr.String()
		}
		ex.eventLog = trace.NewEventLog(fmt.Sprintf("sql session [%s]", args.User), remoteStr)
	}

	defer func() {
		r := recover()
		ex.closeWrapper(ctx, r)
	}()
	return ex.run(ctx, cancel)
}

type closeType bool

const (
	normalClose closeType = true
	panicClose  closeType = false
)

func (ex *connExecutor) closeWrapper(ctx context.Context, recovered interface{}) {
	if recovered != nil {
		// A warning header guaranteed to go to stderr. This is unanonymized.
		var cutStmt string
		var stmt string
		if ex.curStmt != nil {
			stmt = ex.curStmt.String()
			cutStmt = stmt
		}
		if len(cutStmt) > panicLogOutputCutoffChars {
			cutStmt = cutStmt[:panicLogOutputCutoffChars] + " [...]"
		}

		log.Shout(ctx, log.Severity_ERROR,
			fmt.Sprintf("a SQL panic has occurred while executing %q: %s", cutStmt, recovered))

		ex.close(ctx, panicClose)

		safeErr := AnonymizeStatementsForReporting("executing", stmt, recovered)

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
	ex.sessionEventf(ctx, "finishing connExecutor")

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

	if err := ex.resetExtraTxnState(ctx, txnAborted, ex.server.dbCache); err != nil {
		log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
	}

	if closeType == normalClose {
		// Close all statements and prepared portals by first unifying the namespaces
		// and the closing what remains.
		ex.commitPrepStmtNamespace(ctx)
		ex.prepStmtsNamespace.resetTo(ctx, &prepStmtNamespace{})
	}

	if ex.dataMutator.sessionTracing.Enabled() {
		if err := ex.dataMutator.StopSessionTracing(); err != nil {
			log.Warningf(ctx, "error stopping tracing: %s", err)
		}
	}

	if ex.eventLog != nil {
		ex.eventLog.Finish()
		ex.eventLog = nil
	}

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
	mon        *mon.BytesMonitor
	sessionMon *mon.BytesMonitor
	// memMetrics contains the metrics that statements executed on this connection
	// will contribute to.
	memMetrics *MemoryMetrics

	// The buffer with incoming statements to execute.
	stmtBuf *StmtBuf
	// The interface for communicating statement results to the client.
	clientComm ClientComm
	// Finity "the machine" Automaton is the state machine controlling the state
	// below.
	machine fsm.Machine
	// state encapsulates fields related to the ongoing SQL txn. It is mutated as
	// the machine's ExtendedState.
	state         txnState2
	transitionCtx transitionCtx

	// eventLog for SQL statements and other important session events. Will be set
	// if traceSessionEventLogEnabled; it is used by ex.sessionEventf()
	eventLog trace.EventLog

	// extraTxnState groups fields scoped to a SQL txn that are not handled by
	// ex.state, above. The rule of thumb is that, if the state influences state
	// transitions, it should live in state, otherwise it can live here.
	// This is only used in the Open state. extraTxnState is reset whenever a
	// transaction finishes or gets retried.
	extraTxnState struct {
		// tables collects descriptors used by the current transaction.
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

		// txnRewindPos is the position within stmtBuf to which we'll rewind when
		// performing automatic retries. This is more or less the position where the
		// current transaction started.
		// This field is only defined while in stateOpen.
		//
		// Set via setTxnRewindPos().
		txnRewindPos CmdPos

		// prepStmtsNamespaceAtTxnRewindPos is a snapshot of the prep stmts/portals
		// (ex.prepStmtsNamespace) before processing the command at position
		// txnRewindPos.
		// Here's the deal: prepared statements are not transactional, but they do
		// need to interact properly with automatic retries (i.e. rewinding the
		// command buffer). When doing a rewind, we need to be able to restore the
		// prep stmts as they were. We do this by taking a snapshot every time
		// txnRewindPos is advanced. Prepared statements are shared between the two
		// collections, but these collections are periodically reconciled.
		prepStmtsNamespaceAtTxnRewindPos prepStmtNamespace
	}

	// sessionData contains the user-configurable connection variables.
	sessionData sessiondata.SessionData
	dataMutator sessionDataMutator
	// appStats tracks per-application SQL usage statistics. It is maintained to
	// represent statistrics for the application currently identified by
	// sessiondata.ApplicationName.
	appStats *appStats

	// ctxHolder contains the connection's context in which all command executed
	// on the connection are running. This generally should not be used directly,
	// but through the Ctx() method; if we're inside a transaction, Ctx() is going
	// to return a derived context. See the Context Management comments at the top
	// of the file.
	ctxHolder ctxHolder

	// planner is the "default planner" on a session, to save planner allocations
	// during serial execution. Since planners are not threadsafe, this is only
	// safe to use when a statement is not being parallelized. It must be reset
	// before using.
	planner planner
	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes

	// parallelizeQueue is a queue managing all parallelized SQL statements
	// running on this connection.
	parallelizeQueue ParallelizeQueue

	// prepStmtNamespace contains the prepared statements and portals that the
	// session currently has access to.
	prepStmtsNamespace prepStmtNamespace

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[ClusterWideID]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement
	}

	// curStmt is the statement that's currently being prepared or executed, if
	// any. This is printed by high-level panic recovery.
	curStmt tree.Statement

	sessionID ClusterWideID
}

// ctxHolder contains a connection's context and, while session tracing is
// enabled, a derived context with a recording span. The connExecutor should use
// the latter while session tracing is active, or the former otherwise; that's
// what the ctx() method returns.
type ctxHolder struct {
	connCtx           context.Context
	sessionTracingCtx context.Context
	cancel            context.CancelFunc
}

func (ch *ctxHolder) ctx() context.Context {
	if ch.sessionTracingCtx != nil {
		return ch.sessionTracingCtx
	}
	return ch.connCtx
}

func (ch *ctxHolder) hijack(sessionTracingCtx context.Context) {
	if ch.sessionTracingCtx != nil {
		panic("hijack already in effect")
	}
	ch.sessionTracingCtx = sessionTracingCtx
}

func (ch *ctxHolder) unhijack() {
	if ch.sessionTracingCtx == nil {
		panic("hijack not in effect")
	}
	ch.sessionTracingCtx = nil
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
	for pname := range pe.portals {
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

func (ex *connExecutor) resetExtraTxnState(
	ctx context.Context, ev txnEvent, dbCacheHolder *databaseCacheHolder,
) error {
	ex.extraTxnState.schemaChangers.reset()

	var opt releaseOpt
	if ev == txnCommit {
		opt = blockForDBCacheUpdate
	} else {
		opt = dontBlockForDBCacheUpdate
	}
	if err := ex.extraTxnState.tables.releaseTables(ctx, opt); err != nil {
		return err
	}
	ex.extraTxnState.tables.databaseCache = dbCacheHolder.getDatabaseCache()

	ex.extraTxnState.autoRetryCounter = 0
	return nil
}

// Ctx returns the transaction's ctx, if we're inside a transaction, or the
// session's context otherwise.
func (ex *connExecutor) Ctx() context.Context {
	if _, ok := ex.machine.CurState().(stateNoTxn); ok {
		return ex.ctxHolder.ctx()
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
// When this returns, ex.close() needs to be called and  the connection to the
// client needs to be terminated. If it returns with an error, that error may
// represent a communication error (in which case the connection might already
// also have an error from the reading side), or some other unexpected failure.
// Returned errors have not been communicated to the client: it's up to the
// caller to do that if it wants.
func (ex *connExecutor) run(ctx context.Context, cancel context.CancelFunc) error {
	ex.ctxHolder.connCtx = ctx
	ex.ctxHolder.cancel = cancel

	ex.sessionID = ex.generateID()
	ex.server.cfg.SessionRegistry.register(ex.sessionID, ex)
	defer ex.server.cfg.SessionRegistry.deregister(ex.sessionID)

	var draining bool
	for {
		ex.curStmt = nil
		if err := ctx.Err(); err != nil {
			return err
		}

		cmd, pos, err := ex.stmtBuf.curCmd()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		ex.sessionEventf(ex.Ctx(), "[%s pos:%d] executing %s",
			ex.machine.CurState(), pos, cmd)

		var ev fsm.Event
		var payload fsm.EventPayload
		var res ResultBase

		switch tcmd := cmd.(type) {
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

			ex.phaseTimes[sessionQueryReceived] = tcmd.TimeReceived
			ex.phaseTimes[sessionStartParse] = tcmd.ParseStart
			ex.phaseTimes[sessionEndParse] = tcmd.ParseEnd

			ctx := withStatement(ex.Ctx(), ex.curStmt)
			ev, payload, err = ex.execStmt(ctx, curStmt, stmtRes, nil /* pinfo */, pos)
			if err != nil {
				return err
			}
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
			log.VEventf(ex.Ctx(), 2, "portal resolved to: %s", portal.Stmt.Str)
			ex.curStmt = portal.Stmt.Statement

			pinfo := &tree.PlaceholderInfo{
				TypeHints: portal.Stmt.TypeHints,
				Types:     portal.Stmt.Types,
				Values:    portal.Qargs,
			}

			ex.phaseTimes[sessionQueryReceived] = tcmd.TimeReceived
			// When parsing has been done earlier, via a separate parse
			// message, it is not any more part of the statistics collected
			// for this execution. In that case, we simply report that
			// parsing took no time.
			ex.phaseTimes[sessionStartParse] = time.Time{}
			ex.phaseTimes[sessionEndParse] = time.Time{}

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
			ctx := withStatement(ex.Ctx(), ex.curStmt)
			ev, payload, err = ex.execStmt(ctx, curStmt, stmtRes, pinfo, pos)
			if err != nil {
				return err
			}
		case PrepareStmt:
			ex.curStmt = tcmd.Stmt
			res = ex.clientComm.CreatePrepareResult(pos)
			ctx := withStatement(ex.Ctx(), ex.curStmt)
			ev, payload = ex.execPrepare(ctx, tcmd)
		case DescribeStmt:
			descRes := ex.clientComm.CreateDescribeResult(pos)
			res = descRes
			ev, payload = ex.execDescribe(ex.Ctx(), tcmd, descRes)
		case BindStmt:
			res = ex.clientComm.CreateBindResult(pos)
			ev, payload = ex.execBind(ex.Ctx(), tcmd)
		case DeletePreparedStmt:
			res = ex.clientComm.CreateDeleteResult(pos)
			ev, payload = ex.execDelPrepStmt(ex.Ctx(), tcmd)
		case SendError:
			res = ex.clientComm.CreateErrorResult(pos)
			ev = eventNonRetriableErr{IsCommit: fsm.False}
			payload = eventNonRetriableErrPayload{err: tcmd.Err}
		case Sync:
			// Note that the Sync result will flush results to the network connection.
			res = ex.clientComm.CreateSyncResult(pos)
			if draining {
				// If we're draining, check whether this is a good time to finish the
				// connection. If we're not inside a transaction, we stop processing
				// now. If we are inside a transaction, we'll check again the next time
				// a Sync is processed.
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
			res = ex.clientComm.CreateDrainResult(pos)
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
			advInfo, err = ex.txnStateTransitionsApplyWrapper(ev, payload, res, pos)
			if err != nil {
				return err
			}
		} else {
			// If no event was generated synthesize an advance code.
			advInfo = advanceInfo{
				code: advanceOne,
			}
		}

		// Decide if we need to close the result or not. We don't need to do it if
		// we're staying in place or rewinding - the statement will be executed
		// again.
		if advInfo.code != stayInPlace && advInfo.code != rewind {
			// Close the result. In case of an execution error, the result might have
			// its error set already or it might not.
			resErr := res.Err()

			pe, ok := payload.(payloadWithError)
			if ok {
				ex.sessionEventf(ex.Ctx(), "execution error: %s", pe.errorCause())
			}
			if resErr == nil && ok {
				ex.server.recordError(pe.errorCause())
				// Depending on whether the result has the error already or not, we have
				// to call either Close or CloseWithErr.
				res.CloseWithErr(pe.errorCause())
			} else {
				ex.server.recordError(resErr)
				res.Close(stateToTxnStatusIndicator(ex.machine.CurState()))
			}
		} else {
			res.Discard()
		}

		// Move the cursor according to what the state transition told us to do.
		switch advInfo.code {
		case advanceOne:
			ex.stmtBuf.advanceOne()
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
			if err := ex.stmtBuf.seekToNextBatch(); err != nil {
				return err
			}
		case rewind:
			ex.rewindPrepStmtNamespace(ex.Ctx())
			advInfo.rewCap.rewindAndUnlock(ex.Ctx())
		case stayInPlace:
			// Nothing to do. The same statement will be executed again.
		default:
			panic(fmt.Sprintf("unexpected advance code: %s", advInfo.code))
		}

		if err := ex.updateTxnRewindPosMaybe(ex.Ctx(), cmd, pos, advInfo); err != nil {
			return err
		}
	}
}

// updateTxnRewindPosMaybe checks whether the ex.extraTxnState.txnRewindPos
// should be advanced, based on the advInfo produced by running cmd at position
// pos.
func (ex *connExecutor) updateTxnRewindPosMaybe(
	ctx context.Context, cmd Command, pos CmdPos, advInfo advanceInfo,
) error {
	// txnRewindPos is only maintained while in stateOpen.
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
			if advInfo.rewCap.rewindPos != ex.extraTxnState.txnRewindPos {
				return errors.Errorf("unexpected rewind position: %d when txn start is: %d",
					advInfo.rewCap.rewindPos, ex.extraTxnState.txnRewindPos)
			}
			// txnRewindPos stays unchanged.
			return nil
		default:
			return errors.Errorf("unexpected advance code when starting a txn: %s", advInfo.code)
		}
		ex.setTxnRewindPos(ctx, nextPos)
	} else {
		// See if we can advance the rewind point even if this is not the point
		// where the transaction started. We can do that after running a special
		// statement (e.g. SET TRANSACTION or SAVEPOINT) or after most commands that
		// don't execute statements.
		// The idea is that, for example, we don't want the following sequence to
		// disable retries for what comes after the sequence:
		// 1: PrepareStmt BEGIN
		// 2: BindStmt
		// 3: ExecutePortal
		// 4: Sync

		// Note that the current command cannot influence the rewind point if
		// if the rewind point is not current set to the command's position
		// (i.e. we don't do anything if txnRewindPos != pos).

		if advInfo.code != advanceOne {
			panic(fmt.Sprintf("unexpected advanceCode: %s", advInfo.code))
		}

		var canAdvance bool
		_, inOpen := ex.machine.CurState().(stateOpen)
		if inOpen && (ex.extraTxnState.txnRewindPos == pos) {
			switch tcmd := cmd.(type) {
			case ExecStmt:
				canAdvance = ex.stmtDoesntNeedRetry(tcmd.Stmt)
			case ExecPortal:
				portal := ex.prepStmtsNamespace.portals[tcmd.Name]
				canAdvance = ex.stmtDoesntNeedRetry(portal.Stmt.Statement)
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
			case CopyIn:
				// Can't advance.
			case DrainRequest:
				canAdvance = true
			case Flush:
				canAdvance = true
			default:
				panic(fmt.Sprintf("unsupported cmd: %T", cmd))
			}
			if canAdvance {
				ex.setTxnRewindPos(ctx, pos+1)
			}
		}
	}
	return nil
}

// setTxnRewindPos updates the position to which future rewinds will refer.
//
// All statements with lower position in stmtBuf (if any) are removed, as we
// won't ever need them again.
func (ex *connExecutor) setTxnRewindPos(ctx context.Context, pos CmdPos) {
	if pos <= ex.extraTxnState.txnRewindPos {
		panic(fmt.Sprintf("can only move the  txnRewindPos forward. "+
			"Was: %d; new value: %d", ex.extraTxnState.txnRewindPos, pos))
	}
	ex.extraTxnState.txnRewindPos = pos
	ex.stmtBuf.ltrim(ctx, pos)
	ex.commitPrepStmtNamespace(ctx)
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
		ex.state.mon.Start(ctx, ex.sessionMon, mon.BoundAccount{} /* reserved */)
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
			ex.resetPlanner(ctx, p, txn, stmtTS)
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

// stmtHasNoData returns true if describing a result of the input statement
// type should return NoData.
func stmtHasNoData(stmt tree.Statement) bool {
	return stmt == nil || stmt.StatementType() != tree.Rows
}

// generateID generates a unique ID based on the node's ID and its current HLC
// timestamp. These IDs are either scoped at the query level or at the session
// level.
func (ex *connExecutor) generateID() ClusterWideID {
	return GenerateClusterWideID(ex.server.cfg.Clock.Now(), ex.server.cfg.NodeID.Get())
}

// commitPrepStmtNamespace deallocates everything in
// prepStmtsNamespaceAtTxnRewindPos that's not part of prepStmtsNamespace.
func (ex *connExecutor) commitPrepStmtNamespace(ctx context.Context) {
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(
		ctx, &ex.prepStmtsNamespace)
}

// commitPrepStmtNamespace deallocates everything in prepStmtsNamespace that's
// not part of prepStmtsNamespaceAtTxnRewindPos.
func (ex *connExecutor) rewindPrepStmtNamespace(ctx context.Context) {
	ex.prepStmtsNamespace.resetTo(
		ctx, &ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos)
}

// getRewindTxnCapability checks whether rewinding to the position previously
// set through setTxnRewindPos() is possible and, if it is, returns a
// rewindCapability bound to that position. The returned bool is true if the
// rewind is possible. If it is, client communication is blocked until the
// rewindCapability is exercised.
func (ex *connExecutor) getRewindTxnCapability() (rewindCapability, bool) {
	cl := ex.clientComm.LockCommunication()

	// If we already delivered results at or past the start position, we can't
	// rewind.
	if cl.ClientPos() >= ex.extraTxnState.txnRewindPos {
		cl.Close()
		return rewindCapability{}, false
	}
	return rewindCapability{
		cl:        cl,
		buf:       ex.stmtBuf,
		rewindPos: ex.extraTxnState.txnRewindPos,
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
			panic(fmt.Sprintf("retriable error in unexpected state: %#v",
				ex.machine.CurState()))
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

		// Sort the errors according to their importance.
		curTxn := ex.state.mu.txn.Proto()
		sort.Slice(errs, func(i, j int) bool {
			errPriority := func(err error) int {
				switch t := err.(type) {
				case *roachpb.HandledRetryableTxnError:
					errTxn := t.Transaction
					if errTxn.ID == curTxn.ID && errTxn.Epoch == curTxn.Epoch {
						// A retryable error for the current transaction
						// incarnation is given the highest priority.
						return 1
					}
					return 2
				case *roachpb.TxnPrevAttemptError:
					// Symptom of concurrent retry.
					return 3
				default:
					// Any other error. We sort these behind retryable errors
					// and errors we know to be their symptoms because it is
					// impossible to conclusively determine in all cases whether
					// one of these errors is a symptom of a concurrent retry or
					// not. If the error is a symptom then we want to ignore it.
					// If it is not, we expect to see the same error during a
					// transaction retry.
					return 4
				}
			}
			return errPriority(errs[i]) < errPriority(errs[j])
		})

		// Return the "best" error.
		bestErr := errs[0]
		switch bestErr.(type) {
		case *roachpb.HandledRetryableTxnError:
			// If any of the errors are retryable, we need to bump the transaction
			// epoch to invalidate any writes performed by any workers after the
			// retry updated the txn's proto but before we synchronized (some of
			// these writes might have been performed at the wrong epoch). Note
			// that we don't need to lock the client.Txn because we're synchronized.
			// See #17197.
			ex.state.mu.txn.Proto().BumpEpoch()
		case *roachpb.TxnPrevAttemptError:
			log.Fatalf(ctx, "found symptoms of a concurrent retry, but did "+
				"not find the final retry error: %v", errs)
		}
		return bestErr
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

// evalCtx creates an extendedEvalCtx corresponding to the current state of the
// session.
//
// p is the planner that the EvalCtx will link to. Note that the planner also
// needs to have a reference to an evalCtx, so the caller will have to set that
// up.
// stmtTS is the timestamp that the statement_timestamp() SQL builtin will
// return for statements executed with this evalCtx. Since generally each
// statement is supposed to have a different timestamp, the evalCtx generally
// shouldn't be reused across statements.
func (ex *connExecutor) evalCtx(p *planner, stmtTS time.Time) extendedEvalContext {
	txn := ex.state.mu.txn

	scInterface := newSchemaInterface(&ex.extraTxnState.tables, ex.server.cfg.VirtualSchemas)

	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Planner:       p,
			Sequence:      p,
			StmtTimestamp: stmtTS,

			Txn:             txn,
			SessionData:     &ex.sessionData,
			ApplicationName: ex.dataMutator.ApplicationName(),
			TxnState:        ex.getTransactionState(),
			TxnReadOnly:     ex.state.readOnly,
			TxnImplicit:     ex.implicitTxn(),
			Settings:        ex.server.cfg.Settings,
			CtxProvider:     ex,
			Mon:             ex.state.mon,
			TestingKnobs:    ex.server.cfg.EvalContextTestingKnobs,
			TxnTimestamp:    ex.state.sqlTimestamp,
			ClusterID:       ex.server.cfg.ClusterID(),
			NodeID:          ex.server.cfg.NodeID.Get(),
			ReCache:         ex.server.reCache,
		},
		SessionMutator:  &ex.dataMutator,
		VirtualSchemas:  ex.server.cfg.VirtualSchemas,
		Tracing:         &ex.dataMutator.sessionTracing,
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
	if ex.implicitTxn() {
		// If the statement reading the state is in an implicit transaction, then we
		// want to tell NoTxn to the client.
		state = stateNoTxn{}
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
func (ex *connExecutor) newPlanner(
	ctx context.Context, txn *client.Txn, stmtTS time.Time,
) *planner {
	p := &planner{execCfg: ex.server.cfg}
	ex.resetPlanner(ctx, p, txn, stmtTS)
	return p
}

// resetPlanner re-initializes a planner so it can can be used for planning a
// query in the context of this session.
func (ex *connExecutor) resetPlanner(
	ctx context.Context, p *planner, txn *client.Txn, stmtTS time.Time,
) {
	p.statsCollector = ex.newStatsCollector()
	p.txn = txn
	p.stmt = nil
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	p.semaCtx = tree.MakeSemaContext(ex.sessionData.User == security.RootUser)
	p.semaCtx.Location = &ex.sessionData.Location
	p.semaCtx.SearchPath = ex.sessionData.SearchPath

	p.extendedEvalCtx = ex.evalCtx(p, stmtTS)
	p.extendedEvalCtx.ClusterID = ex.server.cfg.ClusterID()
	p.extendedEvalCtx.NodeID = ex.server.cfg.NodeID.Get()
	p.extendedEvalCtx.ReCache = ex.server.reCache

	p.sessionDataMutator = &ex.dataMutator
	p.preparedStatements = ex.getPrepStmtsAccessor()
	p.autoCommit = false
	p.isPreparing = false
	p.asOfSystemTime = false
}

// txnStateTransitionsApplyWrapper is a wrapper on top of Machine built with the
// TxnStateTransitions above. Its point is to detect when we go in and out of
// transactions and update some state.
//
// Any returned error indicates an unrecoverable error for the session;
// execution on this connection should be interrupted.
func (ex *connExecutor) txnStateTransitionsApplyWrapper(
	ev fsm.Event, payload fsm.EventPayload, res ResultBase, pos CmdPos,
) (advanceInfo, error) {

	var implicitTxn bool
	if os, ok := ex.machine.CurState().(stateOpen); ok {
		implicitTxn = os.ImplicitTxn.Get()
	}

	ctx := withStatement(ex.Ctx(), ex.curStmt)
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
	case txnStart:
	case txnCommit:
		// If we have schema changers to run, release leases early so that schema
		// changers can run.
		if len(ex.extraTxnState.schemaChangers.schemaChangers) > 0 {
			ex.extraTxnState.tables.releaseLeases(ex.Ctx())
		}
		// TODO(andrei): figure out how session tracing should interact with schema
		// changes.
		if schemaChangeErr := ex.extraTxnState.schemaChangers.execSchemaChanges(
			ex.Ctx(), ex.server.cfg,
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
	case txnRestart, txnAborted:
		if err := ex.resetExtraTxnState(ex.Ctx(), advInfo.txnEvent, ex.server.dbCache); err != nil {
			return advanceInfo{}, err
		}
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
	ctx context.Context, res RestrictedCommandResult, stmt Statement, cols sqlbase.ResultColumns,
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

// cancelQuery is part of the registrySession interface.
func (ex *connExecutor) cancelQuery(queryID ClusterWideID) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if queryMeta, exists := ex.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancel()
		return true
	}
	return false
}

// cancelSession is part of the registrySession interface.
func (ex *connExecutor) cancelSession() {
	// TODO(abhimadan): figure out how to send a nice error message to the client.
	ex.ctxHolder.cancel()
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
		ID:              ex.sessionID.GetBytes(),
	}
}

func (ex *connExecutor) getPrepStmtsAccessor() preparedStatementsAccessor {
	return connExPrepStmtsAccessor{
		ex: ex,
	}
}

// sessionEventf logs a message to the session event log (if any).
func (ex *connExecutor) sessionEventf(ctx context.Context, format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	log.VEventfDepth(ex.Ctx(), 1 /* depth */, 2 /* level */, str)
	if ex.eventLog != nil {
		ex.eventLog.Printf("%s", str)
	}
}

// StatementCounters groups metrics for counting different types of statements.
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

// contextStatementKey is an empty type for the handle associated with the
// statement value (see context.Value).
type contextStatementKey struct{}

// withStatement adds a SQL statement to the provided context. The statement
// will then be included in crash reports which use that context.
func withStatement(ctx context.Context, stmt tree.Statement) context.Context {
	return context.WithValue(ctx, contextStatementKey{}, stmt)
}

// statementFromCtx returns the statement value from a context, or nil if unset.
func statementFromCtx(ctx context.Context) tree.Statement {
	stmt := ctx.Value(contextStatementKey{})
	if stmt == nil {
		return nil
	}
	return stmt.(tree.Statement)
}

func init() {
	// Register a function to include the anonymized statement in crash reports.
	log.RegisterTagFn("statement", func(ctx context.Context) string {
		stmt := statementFromCtx(ctx)
		if stmt == nil {
			return ""
		}
		// Anonymize the statement for reporting.
		return anonymizeStmtAndConstants(stmt)
	})
}
