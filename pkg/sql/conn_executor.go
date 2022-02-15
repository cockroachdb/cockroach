// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/txnidcache"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/net/trace"
)

// noteworthyMemoryUsageBytes is the minimum size tracked by a
// transaction or session monitor before the monitor starts explicitly
// logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SESSION_MEMORY_USAGE", 1024*1024)

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
	_ util.NoCopy

	cfg *ExecutorConfig

	// sqlStats tracks per-application statistics for all applications on each
	// node. Newly collected statistics flow into sqlStats.
	sqlStats *persistedsqlstats.PersistedSQLStats

	// sqlStatsController is the control-plane interface for sqlStats.
	sqlStatsController *persistedsqlstats.Controller

	// indexUsageStatsController is the control-plane interface for
	// indexUsageStats.
	indexUsageStatsController *idxusage.Controller

	// reportedStats is a pool of stats that is held for reporting, and is
	// cleared on a lower interval than sqlStats. Stats from sqlStats flow
	// into reported stats when sqlStats is cleared.
	reportedStats sqlstats.Provider

	// reportedStatsController is the control-plane interface for
	// reportedStatsController.
	reportedStatsController *sslocal.Controller

	reCache *tree.RegexpCache

	// pool is the parent monitor for all session monitors.
	pool *mon.BytesMonitor

	// indexUsageStats tracks the index usage statistics queries that use current
	// node as gateway node.
	indexUsageStats *idxusage.LocalIndexUsageStats

	// txnIDCache stores the mapping from transaction ID to transaction
	// fingerprint IDs for all recently executed transactions.
	txnIDCache *txnidcache.Cache

	// Metrics is used to account normal queries.
	Metrics Metrics

	// InternalMetrics is used to account internal queries.
	InternalMetrics Metrics

	// ServerMetrics is used to account for Server activities that are unrelated to
	// query planning and execution.
	ServerMetrics ServerMetrics

	// TelemetryLoggingMetrics is used to track metrics for logging to the telemetry channel.
	TelemetryLoggingMetrics *TelemetryLoggingMetrics

	mu struct {
		syncutil.Mutex
		connectionCount int64
	}
}

// Metrics collects timeseries data about SQL activity.
type Metrics struct {
	// EngineMetrics is exported as required by the metrics.Struct magic we use
	// for metrics registration.
	EngineMetrics EngineMetrics

	// StartedStatementCounters contains metrics for statements initiated by
	// users. These metrics count user-initiated operations, regardless of
	// success (in particular, TxnCommitCount is the number of COMMIT statements
	// attempted, not the number of transactions that successfully commit).
	StartedStatementCounters StatementCounters

	// ExecutedStatementCounters contains metrics for successfully executed
	// statements.
	ExecutedStatementCounters StatementCounters

	// GuardrailMetrics contains metrics related to different guardrails in the
	// SQL layer.
	GuardrailMetrics GuardrailMetrics
}

// ServerMetrics collects timeseries data about Server activities that are
// unrelated to SQL planning and execution.
type ServerMetrics struct {
	// StatsMetrics contains metrics for SQL statistics collection.
	StatsMetrics StatsMetrics

	// ContentionSubsystemMetrics contains metrics related to contention
	// subsystem.
	ContentionSubsystemMetrics txnidcache.Metrics
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig, pool *mon.BytesMonitor) *Server {
	metrics := makeMetrics(false /* internal */)
	serverMetrics := makeServerMetrics(cfg)
	reportedSQLStats := sslocal.New(
		cfg.Settings,
		sqlstats.MaxMemReportedSQLStatsStmtFingerprints,
		sqlstats.MaxMemReportedSQLStatsTxnFingerprints,
		serverMetrics.StatsMetrics.ReportedSQLStatsMemoryCurBytesCount,
		serverMetrics.StatsMetrics.ReportedSQLStatsMemoryMaxBytesHist,
		pool,
		nil, /* reportedProvider */
		cfg.SQLStatsTestingKnobs,
	)
	reportedSQLStatsController :=
		reportedSQLStats.GetController(cfg.SQLStatusServer, cfg.DB, cfg.InternalExecutor)
	memSQLStats := sslocal.New(
		cfg.Settings,
		sqlstats.MaxMemSQLStatsStmtFingerprints,
		sqlstats.MaxMemSQLStatsTxnFingerprints,
		serverMetrics.StatsMetrics.SQLStatsMemoryCurBytesCount,
		serverMetrics.StatsMetrics.SQLStatsMemoryMaxBytesHist,
		pool,
		reportedSQLStats,
		cfg.SQLStatsTestingKnobs,
	)
	s := &Server{
		cfg:                     cfg,
		Metrics:                 metrics,
		InternalMetrics:         makeMetrics(true /* internal */),
		ServerMetrics:           serverMetrics,
		pool:                    pool,
		reportedStats:           reportedSQLStats,
		reportedStatsController: reportedSQLStatsController,
		reCache:                 tree.NewRegexpCache(512),
		indexUsageStats: idxusage.NewLocalIndexUsageStats(&idxusage.Config{
			ChannelSize: idxusage.DefaultChannelSize,
			Setting:     cfg.Settings,
		}),
		txnIDCache: txnidcache.NewTxnIDCache(
			cfg.Settings,
			&serverMetrics.ContentionSubsystemMetrics),
	}

	telemetryLoggingMetrics := &TelemetryLoggingMetrics{}

	telemetryLoggingMetrics.Knobs = cfg.TelemetryLoggingTestingKnobs
	s.TelemetryLoggingMetrics = telemetryLoggingMetrics

	sqlStatsInternalExecutor := MakeInternalExecutor(context.Background(), s, MemoryMetrics{}, cfg.Settings)
	persistedSQLStats := persistedsqlstats.New(&persistedsqlstats.Config{
		Settings:         s.cfg.Settings,
		InternalExecutor: &sqlStatsInternalExecutor,
		KvDB:             cfg.DB,
		SQLIDContainer:   cfg.NodeID,
		JobRegistry:      s.cfg.JobRegistry,
		Knobs:            cfg.SQLStatsTestingKnobs,
		FlushCounter:     serverMetrics.StatsMetrics.SQLStatsFlushStarted,
		FailureCounter:   serverMetrics.StatsMetrics.SQLStatsFlushFailure,
		FlushDuration:    serverMetrics.StatsMetrics.SQLStatsFlushDuration,
	}, memSQLStats)

	s.sqlStats = persistedSQLStats
	s.sqlStatsController = persistedSQLStats.GetController(cfg.SQLStatusServer)
	s.indexUsageStatsController = idxusage.NewController(cfg.SQLStatusServer)
	return s
}

func makeMetrics(internal bool) Metrics {
	return Metrics{
		EngineMetrics: EngineMetrics{
			DistSQLSelectCount:    metric.NewCounter(getMetricMeta(MetaDistSQLSelect, internal)),
			SQLOptFallbackCount:   metric.NewCounter(getMetricMeta(MetaSQLOptFallback, internal)),
			SQLOptPlanCacheHits:   metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheHits, internal)),
			SQLOptPlanCacheMisses: metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheMisses, internal)),
			// TODO(mrtracy): See HistogramWindowInterval in server/config.go for the 6x factor.
			DistSQLExecLatency: metric.NewLatency(getMetricMeta(MetaDistSQLExecLatency, internal),
				6*metricsSampleInterval),
			SQLExecLatency: metric.NewLatency(getMetricMeta(MetaSQLExecLatency, internal),
				6*metricsSampleInterval),
			DistSQLServiceLatency: metric.NewLatency(getMetricMeta(MetaDistSQLServiceLatency, internal),
				6*metricsSampleInterval),
			SQLServiceLatency: metric.NewLatency(getMetricMeta(MetaSQLServiceLatency, internal),
				6*metricsSampleInterval),
			SQLTxnLatency: metric.NewLatency(getMetricMeta(MetaSQLTxnLatency, internal),
				6*metricsSampleInterval),
			SQLTxnsOpen:         metric.NewGauge(getMetricMeta(MetaSQLTxnsOpen, internal)),
			SQLActiveStatements: metric.NewGauge(getMetricMeta(MetaSQLActiveQueries, internal)),

			TxnAbortCount:                     metric.NewCounter(getMetricMeta(MetaTxnAbort, internal)),
			FailureCount:                      metric.NewCounter(getMetricMeta(MetaFailure, internal)),
			FullTableOrIndexScanCount:         metric.NewCounter(getMetricMeta(MetaFullTableOrIndexScan, internal)),
			FullTableOrIndexScanRejectedCount: metric.NewCounter(getMetricMeta(MetaFullTableOrIndexScanRejected, internal)),
		},
		StartedStatementCounters:  makeStartedStatementCounters(internal),
		ExecutedStatementCounters: makeExecutedStatementCounters(internal),
		GuardrailMetrics: GuardrailMetrics{
			TxnRowsWrittenLogCount: metric.NewCounter(getMetricMeta(MetaTxnRowsWrittenLog, internal)),
			TxnRowsWrittenErrCount: metric.NewCounter(getMetricMeta(MetaTxnRowsWrittenErr, internal)),
			TxnRowsReadLogCount:    metric.NewCounter(getMetricMeta(MetaTxnRowsReadLog, internal)),
			TxnRowsReadErrCount:    metric.NewCounter(getMetricMeta(MetaTxnRowsReadErr, internal)),
		},
	}
}

func makeServerMetrics(cfg *ExecutorConfig) ServerMetrics {
	return ServerMetrics{
		StatsMetrics: StatsMetrics{
			SQLStatsMemoryMaxBytesHist: metric.NewHistogram(
				MetaSQLStatsMemMaxBytes,
				cfg.HistogramWindowInterval,
				log10int64times1000,
				3, /* sigFigs */
			),
			SQLStatsMemoryCurBytesCount: metric.NewGauge(MetaSQLStatsMemCurBytes),
			ReportedSQLStatsMemoryMaxBytesHist: metric.NewHistogram(
				MetaReportedSQLStatsMemMaxBytes,
				cfg.HistogramWindowInterval,
				log10int64times1000,
				3, /* sigFigs */
			),
			ReportedSQLStatsMemoryCurBytesCount: metric.NewGauge(MetaReportedSQLStatsMemCurBytes),
			DiscardedStatsCount:                 metric.NewCounter(MetaDiscardedSQLStats),
			SQLStatsFlushStarted:                metric.NewCounter(MetaSQLStatsFlushStarted),
			SQLStatsFlushFailure:                metric.NewCounter(MetaSQLStatsFlushFailure),
			SQLStatsFlushDuration: metric.NewLatency(
				MetaSQLStatsFlushDuration, 6*metricsSampleInterval,
			),
			SQLStatsRemovedRows: metric.NewCounter(MetaSQLStatsRemovedRows),
			SQLTxnStatsCollectionOverhead: metric.NewLatency(
				MetaSQLTxnStatsCollectionOverhead, 6*metricsSampleInterval,
			),
		},
		ContentionSubsystemMetrics: txnidcache.NewMetrics(),
	}
}

// Start starts the Server's background processing.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	s.sqlStats.Start(ctx, stopper)

	// reportedStats is periodically cleared to prevent too many SQL Stats
	// accumulated in the reporter when the telemetry server fails.
	// Usually it is telemetry's reporter's job to clear the reporting SQL Stats.
	s.reportedStats.Start(ctx, stopper)

	s.txnIDCache.Start(ctx, stopper)
}

// GetSQLStatsController returns the persistedsqlstats.Controller for current
// sql.Server's SQL Stats.
func (s *Server) GetSQLStatsController() *persistedsqlstats.Controller {
	return s.sqlStatsController
}

// GetIndexUsageStatsController returns the idxusage.Controller for current
// sql.Server's index usage stats.
func (s *Server) GetIndexUsageStatsController() *idxusage.Controller {
	return s.indexUsageStatsController
}

// GetSQLStatsProvider returns the provider for the sqlstats subsystem.
func (s *Server) GetSQLStatsProvider() sqlstats.Provider {
	return s.sqlStats
}

// GetReportedSQLStatsController returns the sqlstats.Controller for the current
// sql.Server's reported SQL Stats.
func (s *Server) GetReportedSQLStatsController() *sslocal.Controller {
	return s.reportedStatsController
}

// GetTxnIDCache returns the txnidcache.Cache for the current sql.Server.
func (s *Server) GetTxnIDCache() *txnidcache.Cache {
	return s.txnIDCache
}

// GetScrubbedStmtStats returns the statement statistics by app, with the
// queries scrubbed of their identifiers. Any statements which cannot be
// scrubbed will be omitted from the returned map.
func (s *Server) GetScrubbedStmtStats(
	ctx context.Context,
) ([]roachpb.CollectedStatementStatistics, error) {
	return s.getScrubbedStmtStats(ctx, s.sqlStats.GetLocalMemProvider())
}

// Avoid lint errors.
var _ = (*Server).GetScrubbedStmtStats

// GetUnscrubbedStmtStats returns the same thing as GetScrubbedStmtStats, except
// identifiers (e.g. table and column names) aren't scrubbed from the statements.
func (s *Server) GetUnscrubbedStmtStats(
	ctx context.Context,
) ([]roachpb.CollectedStatementStatistics, error) {
	var stmtStats []roachpb.CollectedStatementStatistics
	stmtStatsVisitor := func(_ context.Context, stat *roachpb.CollectedStatementStatistics) error {
		stmtStats = append(stmtStats, *stat)
		return nil
	}
	err :=
		s.sqlStats.GetLocalMemProvider().IterateStatementStats(ctx, &sqlstats.IteratorOptions{}, stmtStatsVisitor)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch statement stats")
	}

	return stmtStats, nil
}

// GetUnscrubbedTxnStats returns the same transaction statistics by app.
// Identifiers (e.g. table and column names) aren't scrubbed from the statements.
func (s *Server) GetUnscrubbedTxnStats(
	ctx context.Context,
) ([]roachpb.CollectedTransactionStatistics, error) {
	var txnStats []roachpb.CollectedTransactionStatistics
	txnStatsVisitor := func(_ context.Context, stat *roachpb.CollectedTransactionStatistics) error {
		txnStats = append(txnStats, *stat)
		return nil
	}
	err :=
		s.sqlStats.GetLocalMemProvider().IterateTransactionStats(ctx, &sqlstats.IteratorOptions{}, txnStatsVisitor)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch statement stats")
	}

	return txnStats, nil
}

// GetScrubbedReportingStats does the same thing as GetScrubbedStmtStats but
// returns statistics from the reported stats pool.
func (s *Server) GetScrubbedReportingStats(
	ctx context.Context,
) ([]roachpb.CollectedStatementStatistics, error) {
	return s.getScrubbedStmtStats(ctx, s.reportedStats)
}

func (s *Server) getScrubbedStmtStats(
	ctx context.Context, statsProvider sqlstats.Provider,
) ([]roachpb.CollectedStatementStatistics, error) {
	salt := ClusterSecret.Get(&s.cfg.Settings.SV)

	var scrubbedStats []roachpb.CollectedStatementStatistics
	stmtStatsVisitor := func(_ context.Context, stat *roachpb.CollectedStatementStatistics) error {
		// Scrub the statement itself.
		scrubbedQueryStr, ok := scrubStmtStatKey(s.cfg.VirtualSchemas, stat.Key.Query)

		// We don't want to report this stats if scrubbing has failed. We also don't
		// wish to abort here because we want to try our best to report all the
		// stats.
		if !ok {
			return nil
		}

		stat.Key.Query = scrubbedQueryStr

		// Possibly scrub the app name.
		if !strings.HasPrefix(stat.Key.App, catconstants.ReportableAppNamePrefix) {
			stat.Key.App = HashForReporting(salt, stat.Key.App)
		}

		// Quantize the counts to avoid leaking information that way.
		quantizeCounts(&stat.Stats)
		stat.Stats.SensitiveInfo = stat.Stats.SensitiveInfo.GetScrubbedCopy()

		scrubbedStats = append(scrubbedStats, *stat)
		return nil
	}

	err :=
		statsProvider.IterateStatementStats(ctx, &sqlstats.IteratorOptions{}, stmtStatsVisitor)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch scrubbed statement stats")
	}

	return scrubbedStats, nil
}

// GetStmtStatsLastReset returns the time at which the statement statistics were
// last cleared.
func (s *Server) GetStmtStatsLastReset() time.Time {
	return s.sqlStats.GetLastReset()
}

// GetExecutorConfig returns this server's executor config.
func (s *Server) GetExecutorConfig() *ExecutorConfig {
	return s.cfg
}

// SetupConn creates a connExecutor for the client connection.
//
// When this method returns there are no resources allocated yet that
// need to be close()d.
//
// Args:
// args: The initial session parameters. They are validated by SetupConn
//   and an error is returned if this validation fails.
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
//   produce results for the client.
// memMetrics: The metrics that statements executed on this connection will
//   contribute to.
func (s *Server) SetupConn(
	ctx context.Context,
	args SessionArgs,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
	onDefaultIntSizeChange func(newSize int32),
) (ConnectionHandler, error) {
	sd := s.newSessionData(args)
	sds := sessiondata.NewStack(sd)
	// Set the SessionData from args.SessionDefaults. This also validates the
	// respective values.
	sdMutIterator := s.makeSessionDataMutatorIterator(sds, args.SessionDefaults)
	sdMutIterator.onDefaultIntSizeChange = onDefaultIntSizeChange
	if err := sdMutIterator.applyOnEachMutatorError(func(m sessionDataMutator) error {
		return resetSessionVars(ctx, m)
	}); err != nil {
		log.Errorf(ctx, "error setting up client session: %s", err)
		return ConnectionHandler{}, err
	}

	ex := s.newConnExecutor(
		ctx, sdMutIterator, stmtBuf, clientComm, memMetrics, &s.Metrics,
		s.sqlStats.GetApplicationStats(sd.ApplicationName),
	)
	return ConnectionHandler{ex}, nil
}

// IncrementConnectionCount increases connectionCount by 1.
func (s *Server) IncrementConnectionCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.connectionCount++
}

// DecrementConnectionCount decreases connectionCount by 1.
func (s *Server) DecrementConnectionCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.connectionCount--
}

// IncrementConnectionCountIfLessThan increases connectionCount by and returns true if allowedConnectionCount < max,
// otherwise it does nothing and returns false.
func (s *Server) IncrementConnectionCountIfLessThan(max int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	lt := s.mu.connectionCount < max
	if lt {
		s.mu.connectionCount++
	}
	return lt
}

// GetConnectionCount returns the current number of connections.
func (s *Server) GetConnectionCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.connectionCount
}

// ConnectionHandler is the interface between the result of SetupConn
// and the ServeConn below. It encapsulates the connExecutor and hides
// it away from other packages.
type ConnectionHandler struct {
	ex *connExecutor
}

// GetParamStatus retrieves the configured value of the session
// variable identified by varName. This is used for the initial
// message sent to a client during a session set-up.
func (h ConnectionHandler) GetParamStatus(ctx context.Context, varName string) string {
	name := strings.ToLower(varName)
	v, ok := varGen[name]
	if !ok {
		log.Fatalf(ctx, "programming error: status param %q must be defined session var", varName)
		return ""
	}
	hasDefault, defVal := getSessionVarDefaultString(name, v, h.ex.dataMutatorIterator.sessionDataMutatorBase)
	if !hasDefault {
		log.Fatalf(ctx, "programming error: status param %q must have a default value", varName)
		return ""
	}
	return defVal
}

// GetQueryCancelKey returns the per-session identifier that can be used to
// cancel a query using the pgwire cancel protocol.
func (h ConnectionHandler) GetQueryCancelKey() pgwirecancel.BackendKeyData {
	return h.ex.queryCancelKey
}

// ServeConn serves a client connection by reading commands from the stmtBuf
// embedded in the ConnHandler.
//
// If not nil, reserved represents memory reserved for the connection. The
// connExecutor takes ownership of this memory.
func (s *Server) ServeConn(
	ctx context.Context, h ConnectionHandler, reserved mon.BoundAccount, cancel context.CancelFunc,
) error {
	defer func() {
		r := recover()
		h.ex.closeWrapper(ctx, r)
	}()
	return h.ex.run(ctx, s.pool, reserved, cancel)
}

// GetLocalIndexStatistics returns a idxusage.LocalIndexUsageStats.
func (s *Server) GetLocalIndexStatistics() *idxusage.LocalIndexUsageStats {
	return s.indexUsageStats
}

// newSessionData a SessionData that can be passed to newConnExecutor.
func (s *Server) newSessionData(args SessionArgs) *sessiondata.SessionData {
	sd := &sessiondata.SessionData{
		SessionData: sessiondatapb.SessionData{
			UserProto: args.User.EncodeProto(),
		},
		LocalUnmigratableSessionData: sessiondata.LocalUnmigratableSessionData{
			RemoteAddr: args.RemoteAddr,
		},
		LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
			ResultsBufferSize: args.ConnResultsBufferSize,
			IsSuperuser:       args.IsSuperuser,
		},
	}
	if len(args.CustomOptionSessionDefaults) > 0 {
		sd.CustomOptions = make(map[string]string)
		for k, v := range args.CustomOptionSessionDefaults {
			sd.CustomOptions[k] = v
		}
	}
	s.populateMinimalSessionData(sd)
	return sd
}

func (s *Server) makeSessionDataMutatorIterator(
	sds *sessiondata.Stack, defaults SessionDefaults,
) *sessionDataMutatorIterator {
	return &sessionDataMutatorIterator{
		sds: sds,
		sessionDataMutatorBase: sessionDataMutatorBase{
			defaults: defaults,
			settings: s.cfg.Settings,
		},
		sessionDataMutatorCallbacks: sessionDataMutatorCallbacks{},
	}
}

// populateMinimalSessionData populates sd with some minimal values needed for
// not crashing. Fields of sd that are already set are not overwritten.
func (s *Server) populateMinimalSessionData(sd *sessiondata.SessionData) {
	if sd.SequenceState == nil {
		sd.SequenceState = sessiondata.NewSequenceState()
	}
	if sd.Location == nil {
		sd.Location = time.UTC
	}
	if len(sd.SearchPath.GetPathArray()) == 0 {
		sd.SearchPath = sessiondata.DefaultSearchPathForUser(sd.User())
	}
}

// newConnExecutor creates a new connExecutor.
//
// sd is expected to be fully initialized with the values of all the session
// vars.
// sdDefaults controls what the session vars will be reset to through
// RESET statements.
func (s *Server) newConnExecutor(
	ctx context.Context,
	sdMutIterator *sessionDataMutatorIterator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
	applicationStats sqlstats.ApplicationStats,
) *connExecutor {
	// Create the various monitors.
	// The session monitors are started in activate().
	sessionRootMon := mon.NewMonitor(
		"session root",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		-1 /* increment */, math.MaxInt64, s.cfg.Settings,
	)
	sessionMon := mon.NewMonitor(
		"session",
		mon.MemoryResource,
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings,
	)
	// The txn monitor is started in txnState.resetForNewSQLTxn().
	txnMon := mon.NewMonitor(
		"txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings,
	)

	nodeIDOrZero, _ := s.cfg.NodeID.OptionalNodeID()
	ex := &connExecutor{
		server:              s,
		metrics:             srvMetrics,
		stmtBuf:             stmtBuf,
		clientComm:          clientComm,
		mon:                 sessionRootMon,
		sessionMon:          sessionMon,
		sessionDataStack:    sdMutIterator.sds,
		dataMutatorIterator: sdMutIterator,
		state: txnState{
			mon:                          txnMon,
			connCtx:                      ctx,
			testingForceRealTracingSpans: s.cfg.TestingKnobs.ForceRealTracingSpans,
		},
		transitionCtx: transitionCtx{
			db:           s.cfg.DB,
			nodeIDOrZero: nodeIDOrZero,
			clock:        s.cfg.Clock,
			// Future transaction's monitors will inherits from sessionRootMon.
			connMon:          sessionRootMon,
			tracer:           s.cfg.AmbientCtx.Tracer,
			settings:         s.cfg.Settings,
			execTestingKnobs: s.GetExecutorConfig().TestingKnobs,
		},
		memMetrics: memMetrics,
		planner:    planner{execCfg: s.cfg, alloc: &tree.DatumAlloc{}},

		// ctxHolder will be reset at the start of run(). We only define
		// it here so that an early call to close() doesn't panic.
		ctxHolder:                 ctxHolder{connCtx: ctx},
		phaseTimes:                sessionphase.NewTimes(),
		rng:                       rand.New(rand.NewSource(timeutil.Now().UnixNano())),
		executorType:              executorTypeExec,
		hasCreatedTemporarySchema: false,
		stmtDiagnosticsRecorder:   s.cfg.StmtDiagnosticsRecorder,
		indexUsageStats:           s.indexUsageStats,
		txnIDCacheWriter:          s.txnIDCache,
	}

	ex.state.txnAbortCount = ex.metrics.EngineMetrics.TxnAbortCount

	// The transaction_read_only variable is special; its updates need to be
	// hooked-up to the executor.
	ex.dataMutatorIterator.setCurTxnReadOnly = func(val bool) {
		ex.state.readOnly = val
	}
	ex.dataMutatorIterator.onTempSchemaCreation = func() {
		ex.hasCreatedTemporarySchema = true
	}

	ex.applicationName.Store(ex.sessionData().ApplicationName)
	ex.applicationStats = applicationStats
	ex.statsCollector = sslocal.NewStatsCollector(
		s.cfg.Settings,
		applicationStats,
		ex.phaseTimes,
		s.cfg.SQLStatsTestingKnobs,
	)
	ex.dataMutatorIterator.onApplicationNameChange = func(newName string) {
		ex.applicationName.Store(newName)
		ex.applicationStats = ex.server.sqlStats.GetApplicationStats(newName)
	}

	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionInit, timeutil.Now())

	ex.extraTxnState.prepStmtsNamespace = prepStmtNamespace{
		prepStmts: make(map[string]*PreparedStatement),
		portals:   make(map[string]PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos = prepStmtNamespace{
		prepStmts: make(map[string]*PreparedStatement),
		portals:   make(map[string]PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceMemAcc = ex.sessionMon.MakeBoundAccount()
	ex.extraTxnState.descCollection = s.cfg.CollectionFactory.MakeCollection(ctx, descs.NewTemporarySchemaProvider(sdMutIterator.sds))
	ex.extraTxnState.txnRewindPos = -1
	ex.extraTxnState.schemaChangeJobRecords = make(map[descpb.ID]*jobs.Record)
	ex.queryCancelKey = pgwirecancel.MakeBackendKeyData(ex.rng, ex.server.cfg.NodeID.SQLInstanceID())
	ex.mu.ActiveQueries = make(map[ClusterWideID]*queryMeta)
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.sessionTracing.ex = ex
	ex.transitionCtx.sessionTracing = &ex.sessionTracing

	ex.extraTxnState.hasAdminRoleCache = HasAdminRoleCache{}

	ex.extraTxnState.atomicAutoRetryCounter = new(int32)

	ex.initPlanner(ctx, &ex.planner)

	return ex
}

// newConnExecutorWithTxn creates a connExecutor that will execute statements
// under a higher-level txn. This connExecutor runs with a different state
// machine, much reduced from the regular one. It cannot initiate or end
// transactions (so, no BEGIN, COMMIT, ROLLBACK, no auto-commit, no automatic
// retries).
//
// If there is no error, this function also activate()s the returned
// executor, so the caller does not need to run the
// activation. However this means that run() or close() must be called
// to release resources.
func (s *Server) newConnExecutorWithTxn(
	ctx context.Context,
	sdMutIterator *sessionDataMutatorIterator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	parentMon *mon.BytesMonitor,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
	txn *kv.Txn,
	syntheticDescs []catalog.Descriptor,
	applicationStats sqlstats.ApplicationStats,
) *connExecutor {
	ex := s.newConnExecutor(
		ctx,
		sdMutIterator,
		stmtBuf,
		clientComm,
		memMetrics,
		srvMetrics,
		applicationStats,
	)
	if txn.Type() == kv.LeafTxn {
		// If the txn is a leaf txn it is not allowed to perform mutations. For
		// sanity, set read only on the session.
		ex.dataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
			m.SetReadOnly(true)
		})
	}

	// The new transaction stuff below requires active monitors and traces, so
	// we need to activate the executor now.
	ex.activate(ctx, parentMon, mon.BoundAccount{})

	// Perform some surgery on the executor - replace its state machine and
	// initialize the state.
	ex.machine = fsm.MakeMachine(
		BoundTxnStateTransitions,
		stateOpen{ImplicitTxn: fsm.False},
		&ex.state,
	)
	ex.state.resetForNewSQLTxn(
		ctx,
		explicitTxn,
		txn.ReadTimestamp().GoTime(),
		nil, /* historicalTimestamp */
		roachpb.UnspecifiedUserPriority,
		tree.ReadWrite,
		txn,
		ex.transitionCtx,
		ex.QualityOfService())

	// Modify the Collection to match the parent executor's Collection.
	// This allows the InternalExecutor to see schema changes made by the
	// parent executor.
	ex.extraTxnState.descCollection.SetSyntheticDescriptors(syntheticDescs)
	return ex
}

type closeType int

const (
	normalClose closeType = iota
	panicClose
	// externalTxnClose means that the connExecutor has been used within a
	// higher-level txn (through the InternalExecutor).
	externalTxnClose
)

func (ex *connExecutor) closeWrapper(ctx context.Context, recovered interface{}) {
	if recovered != nil {
		panicErr := logcrash.PanicAsError(1, recovered)

		// If there's a statement currently being executed, we'll report
		// on it.
		if ex.curStmtAST != nil {
			// A warning header guaranteed to go to stderr.
			log.SqlExec.Shoutf(ctx, severity.ERROR,
				"a SQL panic has occurred while executing the following statement:\n%s",
				// For the log message, the statement is not anonymized.
				truncateStatementStringForTelemetry(ex.curStmtAST.String()))

			// Embed the statement in the error object for the telemetry
			// report below. The statement gets anonymized.
			vt := ex.planner.extendedEvalCtx.VirtualSchemas
			panicErr = WithAnonymizedStatement(panicErr, ex.curStmtAST, vt)
		}

		// Report the panic to telemetry in any case.
		logcrash.ReportPanic(ctx, &ex.server.cfg.Settings.SV, panicErr, 1 /* depth */)

		// Close the executor before propagating the panic further.
		ex.close(ctx, panicClose)

		// Propagate - this may be meant to stop the process.
		panic(panicErr)
	}
	// Closing is not cancelable.
	closeCtx := ex.server.cfg.AmbientCtx.AnnotateCtx(context.Background())
	// AddTags and not WithTags, so that we combine the tags with those
	// filled by AnnotateCtx.
	closeCtx = logtags.AddTags(closeCtx, logtags.FromContext(ctx))
	ex.close(closeCtx, normalClose)
}

func (ex *connExecutor) close(ctx context.Context, closeType closeType) {
	ex.sessionEventf(ctx, "finishing connExecutor")

	txnEvType := noEvent
	if _, noTxn := ex.machine.CurState().(stateNoTxn); !noTxn {
		txnEvType = txnRollback
	}

	if closeType == normalClose {
		// We'll cleanup the SQL txn by creating a non-retriable (commit:true) event.
		// This event is guaranteed to be accepted in every state.
		ev := eventNonRetriableErr{IsCommit: fsm.True}
		payload := eventNonRetriableErrPayload{err: pgerror.Newf(pgcode.AdminShutdown,
			"connExecutor closing")}
		if err := ex.machine.ApplyWithPayload(ctx, ev, payload); err != nil {
			log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
		}
		switch t := ex.machine.CurState().(type) {
		case stateNoTxn:
			// No txn to finish.
		case stateAborted:
			// A non-retriable error with IsCommit set to true causes the transaction
			// to be cleaned up.
		case stateCommitWait:
			ex.state.finishSQLTxn()
		default:
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("unexpected state in conn executor after ApplyWithPayload %T", t))
			}
		}
		if buildutil.CrdbTestBuild && ex.state.Ctx != nil {
			panic(errors.AssertionFailedf("txn span not closed in state %s", ex.machine.CurState()))
		}
	} else if closeType == externalTxnClose {
		ex.state.finishExternalTxn()
	}

	if err := ex.resetExtraTxnState(ctx, txnEvent{eventType: txnEvType}); err != nil {
		log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
	}

	if ex.hasCreatedTemporarySchema && !ex.server.cfg.TestingKnobs.DisableTempObjectsCleanupOnSessionExit {
		ie := MakeInternalExecutor(ctx, ex.server, MemoryMetrics{}, ex.server.cfg.Settings)
		err := cleanupSessionTempObjects(
			ctx,
			ex.server.cfg.Settings,
			ex.server.cfg.CollectionFactory,
			ex.server.cfg.DB,
			ex.server.cfg.Codec,
			&ie,
			ex.sessionID,
		)
		if err != nil {
			log.Errorf(
				ctx,
				"error deleting temporary objects at session close, "+
					"the temp tables deletion job will retry periodically: %s",
				err,
			)
		}
	}

	if closeType != panicClose {
		// Close all statements, prepared portals, and cursors.
		ex.extraTxnState.prepStmtsNamespace.resetToEmpty(
			ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetToEmpty(
			ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.prepStmtsNamespaceMemAcc.Close(ctx)
		ex.extraTxnState.sqlCursors.closeAll()
	}

	if ex.sessionTracing.Enabled() {
		if err := ex.sessionTracing.StopTracing(); err != nil {
			log.Warningf(ctx, "error stopping tracing: %s", err)
		}
	}

	if ex.eventLog != nil {
		ex.eventLog.Finish()
		ex.eventLog = nil
	}

	// Stop idle timer if the connExecutor is closed to ensure cancel session
	// is not called.
	ex.mu.IdleInSessionTimeout.Stop()
	ex.mu.IdleInTransactionSessionTimeout.Stop()

	if closeType != panicClose {
		ex.state.mon.Stop(ctx)
		ex.sessionMon.Stop(ctx)
		ex.mon.Stop(ctx)
	} else {
		ex.state.mon.EmergencyStop(ctx)
		ex.sessionMon.EmergencyStop(ctx)
		ex.mon.EmergencyStop(ctx)
	}
}

// HasAdminRoleCache is stored in extraTxnState and used to cache if the
// user has admin role throughout a transaction.
// This is used for admin audit logging to check if a transaction is being
// executed with admin privileges.
// HasAdminRoleCache does not have to be reset when a transaction restarts
// or roles back as the user's admin status will not change throughout the
// lifecycle of a single transaction.
type HasAdminRoleCache struct {
	HasAdminRole bool

	// IsSet is used to determine if the value for caching is set or not.
	IsSet bool
}

type connExecutor struct {
	_ util.NoCopy

	// The server to which this connExecutor is attached. The reference is used
	// for getting access to configuration settings.
	// Note: do not use server.Metrics directly. Use metrics below instead.
	server *Server

	// The metrics to which the statement metrics should be accounted.
	// This is different whether the executor is for regular client
	// queries or for "internal" queries.
	metrics *Metrics

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
	memMetrics MemoryMetrics

	// The buffer with incoming statements to execute.
	stmtBuf *StmtBuf
	// The interface for communicating statement results to the client.
	clientComm ClientComm
	// Finity "the machine" Automaton is the state machine controlling the state
	// below.
	machine fsm.Machine
	// state encapsulates fields related to the ongoing SQL txn. It is mutated as
	// the machine's ExtendedState.
	state          txnState
	transitionCtx  transitionCtx
	sessionTracing SessionTracing

	// eventLog for SQL statements and other important session events. Will be set
	// if traceSessionEventLogEnabled; it is used by ex.sessionEventf()
	eventLog trace.EventLog

	// extraTxnState groups fields scoped to a SQL txn that are not handled by
	// ex.state, above. The rule of thumb is that, if the state influences state
	// transitions, it should live in state, otherwise it can live here.
	// This is only used in the Open state. extraTxnState is reset whenever a
	// transaction finishes or gets retried.
	extraTxnState struct {
		// descCollection collects descriptors used by the current transaction.
		descCollection descs.Collection

		// jobs accumulates jobs staged for execution inside the transaction.
		// Staging happens when executing statements that are implemented with a
		// job. The jobs are staged via the function QueueJob in
		// pkg/sql/planner.go. The staged jobs are executed once the transaction
		// that staged them commits.
		jobs jobsCollection

		// schemaChangeJobRecords is a map of descriptor IDs to job Records.
		// Used in createOrUpdateSchemaChangeJob so we can check if a job has been
		// queued up for the given ID. The cache remains valid only for the current
		// transaction and it is cleared after the transaction is committed.
		schemaChangeJobRecords map[descpb.ID]*jobs.Record

		// atomicAutoRetryCounter keeps track of the which iteration of a transaction
		// auto-retry we're currently in. It's 0 whenever the transaction state is not
		// stateOpen.
		atomicAutoRetryCounter *int32

		// autoRetryReason records the error causing an auto-retryable error event if
		// the current transaction is being automatically retried. This is used in
		// statement traces to give more information in statement diagnostic bundles.
		autoRetryReason error

		// numDDL keeps track of how many DDL statements have been
		// executed so far.
		numDDL int

		// numRows keeps track of the number of rows that have been observed by this
		// transaction. This is simply the summation of number of rows observed by
		// comprising statements.
		numRows int

		// txnCounter keeps track of how many SQL txns have been open since
		// the start of the session. This is used for logging, to
		// distinguish statements that belong to separate SQL transactions.
		// A txn unique key can be obtained by grouping this counter with
		// the session ID.
		//
		// Note that a single SQL txn can use multiple KV txns under the
		// hood with multiple KV txn UUIDs, so the KV UUID is not a good
		// txn identifier for SQL logging.
		txnCounter int

		// txnRewindPos is the position within stmtBuf to which we'll rewind when
		// performing automatic retries. This is more or less the position where the
		// current transaction started.
		// This field is only defined while in stateOpen.
		//
		// Set via setTxnRewindPos().
		txnRewindPos CmdPos

		// prepStmtNamespace contains the prepared statements and portals that the
		// session currently has access to.
		// Portals are bound to a transaction and they're all destroyed once the
		// transaction finishes.
		// Prepared statements are not transactional and so it's a bit weird that
		// they're part of extraTxnState, but it's convenient to put them here
		// because they need the same kind of "snapshoting" as the portals (see
		// prepStmtsNamespaceAtTxnRewindPos).
		prepStmtsNamespace prepStmtNamespace

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

		// prepStmtsNamespaceMemAcc is the memory account that is shared
		// between prepStmtsNamespace and prepStmtsNamespaceAtTxnRewindPos. It
		// tracks the memory usage of portals and should be closed upon
		// connExecutor's closure.
		prepStmtsNamespaceMemAcc mon.BoundAccount

		// sqlCursors contains the list of SQL CURSORs the session currently has
		// access to.
		// Cursors are bound to an explicit transaction and they're all destroyed
		// once the transaction finishes.
		sqlCursors cursorMap

		// shouldExecuteOnTxnFinish indicates that ex.onTxnFinish will be called
		// when txn is finished (either committed or aborted). It is true when
		// txn is started but can remain false when txn is executed within
		// another higher-level txn.
		shouldExecuteOnTxnFinish bool

		// txnFinishClosure contains fields that ex.onTxnFinish uses to execute.
		txnFinishClosure struct {
			// txnStartTime is the time that the transaction started.
			txnStartTime time.Time
			// implicit is whether or not the transaction was implicit.
			implicit bool
		}

		// shouldExecuteOnTxnRestart indicates that ex.onTxnRestart will be
		// called when txn is being retried. It is true when txn is started but
		// can remain false when txn is executed within another higher-level
		// txn.
		shouldExecuteOnTxnRestart bool

		// savepoints maintains the stack of savepoints currently open.
		savepoints savepointStack
		// rewindPosSnapshot is a snapshot of the savepoints and sessionData stack
		// before processing the command at position txnRewindPos. When rewinding,
		// we're going to restore this snapshot.
		rewindPosSnapshot struct {
			savepoints       savepointStack
			sessionDataStack *sessiondata.Stack
		}

		// transactionStatementFingerprintIDs tracks all statement IDs that make up the current
		// transaction. It's length is bound by the TxnStatsNumStmtFingerprintIDsToRecord
		// cluster setting.
		transactionStatementFingerprintIDs []roachpb.StmtFingerprintID

		// transactionStatementsHash is the hashed accumulation of all statementFingerprintIDs
		// that comprise the transaction. It is used to construct the key when
		// recording transaction statistics. It's important to accumulate this hash
		// as we go along in addition to the transactionStatementFingerprintIDs as
		// transactionStatementFingerprintIDs are capped to prevent unbound expansion, but we
		// still need the statementFingerprintID hash to disambiguate beyond the capped
		// statements.
		transactionStatementsHash util.FNV64

		schemaChangerState SchemaChangerState

		// shouldCollectTxnExecutionStats specifies whether the statements in
		// this transaction should collect execution stats.
		shouldCollectTxnExecutionStats bool
		// accumulatedStats are the accumulated stats of all statements.
		accumulatedStats execstats.QueryLevelStats
		// rowsRead and bytesRead are separate from QueryLevelStats because they are
		// accumulated independently since they are always collected, as opposed to
		// QueryLevelStats which are sampled.
		rowsRead  int64
		bytesRead int64

		// rowsWritten tracks the number of rows written (modified) by all
		// statements in this txn so far.
		rowsWritten int64

		// rowsWrittenLogged and rowsReadLogged indicates whether we have
		// already logged an event about reaching written/read rows setting,
		// respectively.
		rowsWrittenLogged bool
		rowsReadLogged    bool

		// hasAdminRole is used to cache if the user running the transaction
		// has admin privilege. hasAdminRoleCache is set for the first statement
		// in a transaction.
		hasAdminRoleCache HasAdminRoleCache
	}

	// sessionDataStack contains the user-configurable connection variables.
	sessionDataStack *sessiondata.Stack
	// dataMutatorIterator is nil for session-bound internal executors; we
	// shouldn't issue statements that manipulate session state to an internal
	// executor.
	dataMutatorIterator *sessionDataMutatorIterator

	// applicationStats records per-application SQL usage statistics. It is
	// maintained to represent statistics for the application currently identified
	// by sessiondata.ApplicationName.
	applicationStats sqlstats.ApplicationStats

	// statsCollector is used to collect statistics about SQL statements and
	// transactions.
	statsCollector sqlstats.StatsCollector

	// applicationName is the same as sessionData.ApplicationName. It's copied
	// here as an atomic so that it can be read concurrently by serialize().
	applicationName atomic.Value

	// ctxHolder contains the connection's context in which all command executed
	// on the connection are running. This generally should not be used directly,
	// but through the Ctx() method; if we're inside a transaction, Ctx() is going
	// to return a derived context. See the Context Management comments at the top
	// of the file.
	ctxHolder ctxHolder

	// onCancelSession is called when the SessionRegistry is cancels this session.
	// For pgwire connections, this is hooked up to canceling the connection's
	// context.
	// If nil, canceling this session will be a no-op.
	onCancelSession context.CancelFunc

	// planner is the "default planner" on a session, to save planner allocations
	// during serial execution. Since planners are not threadsafe, this is only
	// safe to use when a statement is not being parallelized. It must be reset
	// before using.
	planner planner

	// phaseTimes tracks session- and transaction-level phase times. It is
	// copied-by-value when resetting statsCollector before executing each
	// statement.
	phaseTimes *sessionphase.Times

	// rng is used to generate random numbers. An example usage is to determine
	// whether to sample execution stats.
	rng *rand.Rand

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[ClusterWideID]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement

		// IdleInSessionTimeout is returned by the AfterFunc call that cancels the
		// session if the idle time exceeds the idle_in_session_timeout.
		IdleInSessionTimeout timeout

		// IdleInTransactionSessionTimeout is returned by the AfterFunc call that
		// cancels the session if the idle time in a transaction exceeds the
		// idle_in_transaction_session_timeout.
		IdleInTransactionSessionTimeout timeout
	}

	// curStmtAST is the statement that's currently being prepared or executed, if
	// any. This is printed by high-level panic recovery.
	curStmtAST tree.Statement

	// queryCancelKey is a 64-bit identifier for the session used by the
	// pgwire cancellation protocol.
	queryCancelKey pgwirecancel.BackendKeyData

	sessionID ClusterWideID

	// activated determines whether activate() was called already.
	// When this is set, close() must be called to release resources.
	activated bool

	// draining is set if we've received a DrainRequest. Once this is set, we're
	// going to find a suitable time to close the connection.
	draining bool

	// executorType is set to whether this executor is an ordinary executor which
	// responds to user queries or an internal one.
	executorType executorType

	// hasCreatedTemporarySchema is set if the executor has created a
	// temporary schema, which requires special cleanup on close.
	hasCreatedTemporarySchema bool

	// stmtDiagnosticsRecorder is used to track which queries need to have
	// information collected.
	stmtDiagnosticsRecorder *stmtdiagnostics.Registry

	// indexUsageStats is used to track index usage stats.
	indexUsageStats *idxusage.LocalIndexUsageStats

	// txnIDCacheWriter is used to write txnidcache.ResolvedTxnID to the
	// Transaction ID Cache.
	txnIDCacheWriter txnidcache.Writer
}

// ctxHolder contains a connection's context and, while session tracing is
// enabled, a derived context with a recording span. The connExecutor should use
// the latter while session tracing is active, or the former otherwise; that's
// what the ctx() method returns.
type ctxHolder struct {
	connCtx           context.Context
	sessionTracingCtx context.Context
}

// timeout wraps a Timer returned by time.AfterFunc. This interface
// allows us to call Stop() on the Timer without having to check if
// the timer is nil.
type timeout struct {
	timeout *time.Timer
}

func (t timeout) Stop() {
	if t.timeout != nil {
		t.timeout.Stop()
	}
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
	prepStmts map[string]*PreparedStatement
	// portals contains the portals currently available on the session. Note
	// that PreparedPortal.accountForCopy needs to be called if a copy of a
	// PreparedPortal is retained.
	portals map[string]PreparedPortal
}

// HasActivePortals returns true if there are portals in the session.
func (ns prepStmtNamespace) HasActivePortals() bool {
	return len(ns.portals) > 0
}

// MigratablePreparedStatements returns a mapping of all prepared statements.
func (ns prepStmtNamespace) MigratablePreparedStatements() []sessiondatapb.MigratableSession_PreparedStatement {
	ret := make([]sessiondatapb.MigratableSession_PreparedStatement, 0, len(ns.prepStmts))
	for name, stmt := range ns.prepStmts {
		ret = append(
			ret,
			sessiondatapb.MigratableSession_PreparedStatement{
				Name:                 name,
				PlaceholderTypeHints: stmt.InferredTypes,
				SQL:                  stmt.SQL,
			},
		)
	}
	return ret
}

func (ns prepStmtNamespace) String() string {
	var sb strings.Builder
	sb.WriteString("Prep stmts: ")
	for name := range ns.prepStmts {
		sb.WriteString(name + " ")
	}
	sb.WriteString("Portals: ")
	for name := range ns.portals {
		sb.WriteString(name + " ")
	}
	return sb.String()
}

// resetToEmpty deallocates the prepStmtNamespace.
func (ns *prepStmtNamespace) resetToEmpty(
	ctx context.Context, prepStmtsNamespaceMemAcc *mon.BoundAccount,
) {
	// No errors could occur since we're releasing the resources.
	_ = ns.resetTo(ctx, prepStmtNamespace{}, prepStmtsNamespaceMemAcc)
}

// resetTo resets a namespace to equate another one (`to`). All the receiver's
// references are released and all the to's references are duplicated.
//
// An empty `to` can be passed in to deallocate everything.
//
// It can only return an error if we've reached the memory limit and had to make
// a copy of portals.
func (ns *prepStmtNamespace) resetTo(
	ctx context.Context, to prepStmtNamespace, prepStmtsNamespaceMemAcc *mon.BoundAccount,
) error {
	for name, p := range ns.prepStmts {
		p.decRef(ctx)
		delete(ns.prepStmts, name)
	}
	for name, p := range ns.portals {
		p.close(ctx, prepStmtsNamespaceMemAcc, name)
		delete(ns.portals, name)
	}

	for name, ps := range to.prepStmts {
		ps.incRef(ctx)
		ns.prepStmts[name] = ps
	}
	for name, p := range to.portals {
		if err := p.accountForCopy(ctx, prepStmtsNamespaceMemAcc, name); err != nil {
			return err
		}
		ns.portals[name] = p
	}
	return nil
}

// resetExtraTxnState resets the fields of ex.extraTxnState when a transaction
// finishes execution (either commits, rollbacks or restarts). Based on the
// transaction event, resetExtraTxnState invokes corresponding callbacks
// (e.g. onTxnFinish() and onTxnRestart()).
func (ex *connExecutor) resetExtraTxnState(ctx context.Context, ev txnEvent) error {
	ex.extraTxnState.jobs = nil
	ex.extraTxnState.hasAdminRoleCache = HasAdminRoleCache{}
	ex.extraTxnState.schemaChangerState = SchemaChangerState{
		mode: ex.sessionData().NewSchemaChangerMode,
	}

	for k := range ex.extraTxnState.schemaChangeJobRecords {
		delete(ex.extraTxnState.schemaChangeJobRecords, k)
	}

	ex.extraTxnState.descCollection.ReleaseAll(ctx)

	// Close all portals.
	for name, p := range ex.extraTxnState.prepStmtsNamespace.portals {
		p.close(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
		delete(ex.extraTxnState.prepStmtsNamespace.portals, name)
	}

	// Close all cursors.
	ex.extraTxnState.sqlCursors.closeAll()

	switch ev.eventType {
	case txnCommit, txnRollback:
		for name, p := range ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.portals {
			p.close(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
			delete(ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.portals, name)
		}
		ex.extraTxnState.savepoints.clear()
		ex.onTxnFinish(ctx, ev)
	case txnRestart:
		ex.onTxnRestart(ctx)
		ex.state.mu.Lock()
		defer ex.state.mu.Unlock()
		ex.state.mu.stmtCount = 0
	}
	// NOTE: on txnRestart we don't need to muck with the savepoints stack. It's either a
	// a ROLLBACK TO SAVEPOINT that generated the event, and that statement deals with the
	// savepoints, or it's a rewind which also deals with them.

	return nil
}

// Ctx returns the transaction's ctx, if we're inside a transaction, or the
// session's context otherwise.
func (ex *connExecutor) Ctx() context.Context {
	ctx := ex.state.Ctx
	if _, ok := ex.machine.CurState().(stateNoTxn); ok {
		ctx = ex.ctxHolder.ctx()
	}
	// stateInternalError is used by the InternalExecutor.
	if _, ok := ex.machine.CurState().(stateInternalError); ok {
		ctx = ex.ctxHolder.ctx()
	}
	return ctx
}

// sessionData returns the top SessionData in the executor's sessionDataStack.
// This should be how callers should reference SessionData objects, as it
// will always contain the "latest" SessionData object in the transaction.
func (ex *connExecutor) sessionData() *sessiondata.SessionData {
	if ex.sessionDataStack == nil {
		return nil
	}
	return ex.sessionDataStack.Top()
}

// activate engages the use of resources that must be cleaned up
// afterwards. after activate() completes, the close() method must be
// called.
//
// Args:
// parentMon: The root monitor.
// reserved: Memory reserved for the connection. The connExecutor takes
//   ownership of this memory.
func (ex *connExecutor) activate(
	ctx context.Context, parentMon *mon.BytesMonitor, reserved mon.BoundAccount,
) {
	// Note: we pass `reserved` to sessionRootMon where it causes it to act as a
	// buffer. This is not done for sessionMon nor state.mon: these monitors don't
	// start with any buffer, so they'll need to ask their "parent" for memory as
	// soon as the first allocation. This is acceptable because the session is
	// single threaded, and the point of buffering is just to avoid contention.
	ex.mon.Start(ctx, parentMon, reserved)
	ex.sessionMon.Start(ctx, ex.mon, mon.BoundAccount{})

	// Enable the trace if configured.
	if traceSessionEventLogEnabled.Get(&ex.server.cfg.Settings.SV) {
		remoteStr := "<admin>"
		if ex.sessionData().RemoteAddr != nil {
			remoteStr = ex.sessionData().RemoteAddr.String()
		}
		ex.eventLog = trace.NewEventLog(
			fmt.Sprintf("sql session [%s]", ex.sessionData().User()), remoteStr)
	}

	ex.activated = true
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
//
// If not nil, reserved represents Memory reserved for the connection. The
// connExecutor takes ownership of this memory.
//
// onCancel, if not nil, will be called when the SessionRegistry cancels the
// session. TODO(andrei): This is hooked up to canceling the pgwire connection's
// context (of which ctx is also a child). It seems uncouth for the connExecutor
// to cancel a higher-level task. A better design would probably be for pgwire
// to own the SessionRegistry, instead of it being owned by the sql.Server -
// then pgwire would directly cancel its own tasks; the sessions also more
// naturally belong there. There is a problem, however, as query cancelation (as
// opposed to session cancelation) is done through the SessionRegistry and that
// does belong with the connExecutor. Introducing a query registry, separate
// from the session registry, might be too costly - the way query cancelation
// works is that every session is asked to cancel a given query until the right
// one is found. That seems like a good performance trade-off.
func (ex *connExecutor) run(
	ctx context.Context,
	parentMon *mon.BytesMonitor,
	reserved mon.BoundAccount,
	onCancel context.CancelFunc,
) error {
	if !ex.activated {
		ex.activate(ctx, parentMon, reserved)
	}
	ex.ctxHolder.connCtx = ctx
	ex.onCancelSession = onCancel

	ex.sessionID = ex.generateID()
	ex.server.cfg.SessionRegistry.register(ex.sessionID, ex.queryCancelKey, ex)
	ex.planner.extendedEvalCtx.setSessionID(ex.sessionID)
	defer ex.server.cfg.SessionRegistry.deregister(ex.sessionID, ex.queryCancelKey)
	for {
		ex.curStmtAST = nil
		if err := ctx.Err(); err != nil {
			return err
		}

		var err error
		if err = ex.execCmd(); err != nil {
			if errors.IsAny(err, io.EOF, errDrainingComplete) {
				return nil
			}
			return err
		}
	}
}

// errDrainingComplete is returned by execCmd when the connExecutor previously got
// a DrainRequest and the time is ripe to finish this session (i.e. we're no
// longer in a transaction).
var errDrainingComplete = fmt.Errorf("draining done. this is a good time to finish this session")

// execCmd reads the current command from the stmtBuf and executes it. The
// transaction state is modified accordingly, and the stmtBuf is advanced or
// rewinded accordingly.
//
// Returns an error if communication of results to the client has failed and the
// session should be terminated. Returns io.EOF if the stmtBuf has been closed.
// Returns drainingComplete if the session should finish because draining is
// complete (i.e. we received a DrainRequest - possibly previously - and the
// connection is found to be idle).
func (ex *connExecutor) execCmd() error {
	ctx := ex.Ctx()
	cmd, pos, err := ex.stmtBuf.CurCmd()
	if err != nil {
		return err // err could be io.EOF
	}

	if log.ExpensiveLogEnabled(ctx, 2) || ex.eventLog != nil {
		ex.sessionEventf(ctx, "[%s pos:%d] executing %s",
			ex.machine.CurState(), pos, cmd)
	}

	var ev fsm.Event
	var payload fsm.EventPayload
	var res ResultBase
	switch tcmd := cmd.(type) {
	case ExecStmt:
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionQueryReceived, tcmd.TimeReceived)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartParse, tcmd.ParseStart)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndParse, tcmd.ParseEnd)

		// We use a closure for the body of the execution so as to
		// guarantee that the full service time is captured below.
		err := func() error {
			if tcmd.AST == nil {
				res = ex.clientComm.CreateEmptyQueryResult(pos)
				return nil
			}
			ex.curStmtAST = tcmd.AST

			stmtRes := ex.clientComm.CreateStatementResult(
				tcmd.AST,
				NeedRowDesc,
				pos,
				nil, /* formatCodes */
				ex.sessionData().DataConversionConfig,
				ex.sessionData().GetLocation(),
				0,  /* limit */
				"", /* portalName */
				ex.implicitTxn(),
			)
			res = stmtRes

			canAutoCommit := ex.implicitTxn()
			ev, payload, err = ex.execStmt(
				ctx, tcmd.Statement, nil /* prepared */, nil /* pinfo */, stmtRes, canAutoCommit,
			)
			return err
		}()
		// Note: we write to ex.statsCollector.PhaseTimes, instead of ex.phaseTimes,
		// because:
		// - stats use ex.statsCollector, not ex.phaseTimes.
		// - ex.statsCollector merely contains a copy of the times, that
		//   was created when the statement started executing (via the
		//   Reset() method).
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionQueryServiced, timeutil.Now())
		if err != nil {
			return err
		}

	case ExecPortal:
		// ExecPortal is handled like ExecStmt, except that the placeholder info
		// is taken from the portal.
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionQueryReceived, tcmd.TimeReceived)
		// When parsing has been done earlier, via a separate parse
		// message, it is not any more part of the statistics collected
		// for this execution. In that case, we simply report that
		// parsing took no time.
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartParse, time.Time{})
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndParse, time.Time{})
		// We use a closure for the body of the execution so as to
		// guarantee that the full service time is captured below.
		err := func() error {
			portalName := tcmd.Name
			portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]
			if !ok {
				err := pgerror.Newf(
					pgcode.InvalidCursorName, "unknown portal %q", portalName)
				ev = eventNonRetriableErr{IsCommit: fsm.False}
				payload = eventNonRetriableErrPayload{err: err}
				res = ex.clientComm.CreateErrorResult(pos)
				return nil
			}
			if portal.Stmt.AST == nil {
				res = ex.clientComm.CreateEmptyQueryResult(pos)
				return nil
			}

			if log.ExpensiveLogEnabled(ctx, 2) {
				log.VEventf(ctx, 2, "portal resolved to: %s", portal.Stmt.AST.String())
			}
			ex.curStmtAST = portal.Stmt.AST

			pinfo := &tree.PlaceholderInfo{
				PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
					TypeHints: portal.Stmt.TypeHints,
					Types:     portal.Stmt.Types,
				},
				Values: portal.Qargs,
			}

			stmtRes := ex.clientComm.CreateStatementResult(
				portal.Stmt.AST,
				// The client is using the extended protocol, so no row description is
				// needed.
				DontNeedRowDesc,
				pos, portal.OutFormats,
				ex.sessionData().DataConversionConfig,
				ex.sessionData().GetLocation(),
				tcmd.Limit,
				portalName,
				ex.implicitTxn(),
			)
			res = stmtRes

			// In the extended protocol, autocommit is not always allowed. The postgres
			// docs say that commands in the extended protocol are all treated as an
			// implicit transaction that does not get committed until a Sync message is
			// received. However, if we are executing a statement that is immediately
			// followed by Sync (which is the common case), then we still can auto-commit,
			// which allows the 1PC txn fast path to be used.
			canAutoCommit := ex.implicitTxn() && tcmd.FollowedBySync
			ev, payload, err = ex.execPortal(ctx, portal, portalName, stmtRes, pinfo, canAutoCommit)
			return err
		}()
		// Note: we write to ex.statsCollector.phaseTimes, instead of ex.phaseTimes,
		// because:
		// - stats use ex.statsCollector, not ex.phasetimes.
		// - ex.statsCollector merely contains a copy of the times, that
		//   was created when the statement started executing (via the
		//   reset() method).
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionQueryServiced, timeutil.Now())
		if err != nil {
			return err
		}
		// Update the cmd and pos in the stmtBuf as limitedCommandResult will have
		// advanced the position if the the portal is repeatedly executed with a limit
		cmd, pos, err = ex.stmtBuf.CurCmd()
		if err != nil {
			return err
		}

	case PrepareStmt:
		ex.curStmtAST = tcmd.AST
		res = ex.clientComm.CreatePrepareResult(pos)
		stmtCtx := withStatement(ctx, ex.curStmtAST)
		ev, payload = ex.execPrepare(stmtCtx, tcmd)
	case DescribeStmt:
		descRes := ex.clientComm.CreateDescribeResult(pos)
		res = descRes
		ev, payload = ex.execDescribe(ctx, tcmd, descRes)
	case BindStmt:
		res = ex.clientComm.CreateBindResult(pos)
		ev, payload = ex.execBind(ctx, tcmd)
	case DeletePreparedStmt:
		res = ex.clientComm.CreateDeleteResult(pos)
		ev, payload = ex.execDelPrepStmt(ctx, tcmd)
	case SendError:
		res = ex.clientComm.CreateErrorResult(pos)
		ev = eventNonRetriableErr{IsCommit: fsm.False}
		payload = eventNonRetriableErrPayload{err: tcmd.Err}
	case Sync:
		// The Postgres docs say: "At completion of each series of extended-query
		// messages, the frontend should issue a Sync message. This parameterless
		// message causes the backend to close the current transaction if it's not
		// inside a BEGIN/COMMIT transaction block (“close” meaning to commit if no
		// error, or roll back if error)."
		// In other words, Sync is treated as commit for implicit transactions.
		if ex.implicitTxn() {
			// Note that the handling of ev in the case of Sync is massaged a bit
			// later - Sync is special in that, if it encounters an error, that does
			// *not *cause the session to ignore all commands until the next Sync.
			ev, payload = ex.handleAutoCommit(ctx, &tree.CommitTransaction{})
		}
		// Note that the Sync result will flush results to the network connection.
		res = ex.clientComm.CreateSyncResult(pos)
		if ex.draining {
			// If we're draining, check whether this is a good time to finish the
			// connection. If we're not inside a transaction, we stop processing
			// now. If we are inside a transaction, we'll check again the next time
			// a Sync is processed.
			if ex.idleConn() {
				// If we're about to close the connection, close res in order to flush
				// now, as we won't have an opportunity to do it later.
				res.Close(ctx, stateToTxnStatusIndicator(ex.machine.CurState()))
				return errDrainingComplete
			}
		}
	case CopyIn:
		res = ex.clientComm.CreateCopyInResult(pos)
		var err error
		ev, payload, err = ex.execCopyIn(ctx, tcmd)
		if err != nil {
			return err
		}
	case DrainRequest:
		// We received a drain request. We terminate immediately if we're not in a
		// transaction. If we are in a transaction, we'll finish as soon as a Sync
		// command (i.e. the end of a batch) is processed outside of a
		// transaction.
		ex.draining = true
		res = ex.clientComm.CreateDrainResult(pos)
		if ex.idleConn() {
			return errDrainingComplete
		}
	case Flush:
		// Closing the res will flush the connection's buffer.
		res = ex.clientComm.CreateFlushResult(pos)
	default:
		panic(errors.AssertionFailedf("unsupported command type: %T", cmd))
	}

	var advInfo advanceInfo

	// If an event was generated, feed it to the state machine.
	if ev != nil {
		var err error
		advInfo, err = ex.txnStateTransitionsApplyWrapper(ev, payload, res, pos)
		if err != nil {
			return err
		}

		// Massage the advancing for Sync, which is special.
		if _, ok := cmd.(Sync); ok {
			switch advInfo.code {
			case skipBatch:
				// An error on Sync doesn't cause us to skip commands (and errors are
				// possible because Sync can trigger a commit). We generate the
				// ErrorResponse and the ReadyForQuery responses, and we continue with
				// the next command. From the Postgres docs:
				// """
				// Note that no skipping occurs if an error is detected while processing
				// Sync — this ensures that there is one and only one ReadyForQuery sent for
				// each Sync.
				// """
				advInfo = advanceInfo{code: advanceOne}
			case advanceOne:
			case rewind:
			case stayInPlace:
				return errors.AssertionFailedf("unexpected advance code stayInPlace when processing Sync")
			}
		}

		// If a txn just started, we henceforth want to run in the context of the
		// transaction. Similarly, if a txn just ended, we don't want to run in its
		// context any more.
		ctx = ex.Ctx()
	} else {
		// If no event was generated synthesize an advance code.
		advInfo = advanceInfo{code: advanceOne}
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
			ex.sessionEventf(ctx, "execution error: %s", pe.errorCause())
			if resErr == nil {
				res.SetError(pe.errorCause())
			}
		}
		res.Close(ctx, stateToTxnStatusIndicator(ex.machine.CurState()))
	} else {
		res.Discard()
	}

	// Move the cursor according to what the state transition told us to do.
	switch advInfo.code {
	case advanceOne:
		ex.stmtBuf.AdvanceOne()
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
		if err := ex.rewindPrepStmtNamespace(ctx); err != nil {
			return err
		}
		ex.extraTxnState.savepoints = ex.extraTxnState.rewindPosSnapshot.savepoints
		// Note we use the Replace function instead of reassigning, as there are
		// copies of the ex.sessionDataStack in the iterators and extendedEvalContext.
		ex.sessionDataStack.Replace(ex.extraTxnState.rewindPosSnapshot.sessionDataStack)
		advInfo.rewCap.rewindAndUnlock(ctx)
	case stayInPlace:
		// Nothing to do. The same statement will be executed again.
	default:
		panic(errors.AssertionFailedf("unexpected advance code: %s", advInfo.code))
	}

	if err := ex.updateTxnRewindPosMaybe(ctx, cmd, pos, advInfo); err != nil {
		return err
	}

	if rewindCapability, canRewind := ex.getRewindTxnCapability(); !canRewind {
		// Trim statements that cannot be retried to reclaim memory.
		ex.stmtBuf.Ltrim(ctx, pos)
	} else {
		rewindCapability.close()
	}

	if ex.server.cfg.TestingKnobs.AfterExecCmd != nil {
		ex.server.cfg.TestingKnobs.AfterExecCmd(ctx, cmd, ex.stmtBuf)
	}

	return nil
}

func (ex *connExecutor) idleConn() bool {
	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		return true
	case stateInternalError:
		return true
	default:
		return false
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
	if advInfo.txnEvent.eventType == txnStart || advInfo.txnEvent.eventType == txnRestart {
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
				return errors.AssertionFailedf(
					"unexpected rewind position: %d when txn start is: %d",
					errors.Safe(advInfo.rewCap.rewindPos),
					errors.Safe(ex.extraTxnState.txnRewindPos))
			}
			// txnRewindPos stays unchanged.
			return nil
		default:
			return errors.AssertionFailedf(
				"unexpected advance code when starting a txn: %s",
				errors.Safe(advInfo.code))
		}
		if err := ex.setTxnRewindPos(ctx, nextPos); err != nil {
			return err
		}
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
			panic(errors.AssertionFailedf("unexpected advanceCode: %s", advInfo.code))
		}

		var canAdvance bool
		_, inOpen := ex.machine.CurState().(stateOpen)
		if inOpen && (ex.extraTxnState.txnRewindPos == pos) {
			switch tcmd := cmd.(type) {
			case ExecStmt:
				canAdvance = ex.stmtDoesntNeedRetry(tcmd.AST)
			case ExecPortal:
				canAdvance = true
				// The portal might have been deleted if DEALLOCATE was executed.
				portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[tcmd.Name]
				if ok {
					canAdvance = ex.stmtDoesntNeedRetry(portal.Stmt.AST)
				}
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
				panic(errors.AssertionFailedf("unsupported cmd: %T", cmd))
			}
			if canAdvance {
				if err := ex.setTxnRewindPos(ctx, pos+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// setTxnRewindPos updates the position to which future rewinds will refer.
//
// All statements with lower position in stmtBuf (if any) are removed, as we
// won't ever need them again.
func (ex *connExecutor) setTxnRewindPos(ctx context.Context, pos CmdPos) error {
	if pos <= ex.extraTxnState.txnRewindPos {
		panic(errors.AssertionFailedf("can only move the  txnRewindPos forward. "+
			"Was: %d; new value: %d", ex.extraTxnState.txnRewindPos, pos))
	}
	ex.extraTxnState.txnRewindPos = pos
	ex.stmtBuf.Ltrim(ctx, pos)
	ex.extraTxnState.rewindPosSnapshot.savepoints = ex.extraTxnState.savepoints.clone()
	ex.extraTxnState.rewindPosSnapshot.sessionDataStack = ex.sessionDataStack.Clone()
	return ex.commitPrepStmtNamespace(ctx)
}

// stmtDoesntNeedRetry returns true if the given statement does not need to be
// retried when performing automatic retries. This means that the results of the
// statement do not change with retries.
func (ex *connExecutor) stmtDoesntNeedRetry(ast tree.Statement) bool {
	return isSavepoint(ast) || isSetTransaction(ast)
}

func stateToTxnStatusIndicator(s fsm.State) TransactionStatusIndicator {
	switch s.(type) {
	case stateOpen:
		return InTxnBlock
	case stateAborted:
		return InFailedTxnBlock
	case stateNoTxn:
		return IdleTxnBlock
	case stateCommitWait:
		return InTxnBlock
	case stateInternalError:
		return InTxnBlock
	default:
		panic(errors.AssertionFailedf("unknown state: %T", s))
	}
}

// isCopyToExternalStorage returns true if the CopyIn command is writing to an
// ExternalStorage such as nodelocal or userfile. It does so by checking the
// target table/schema names against the sentinel, internal table/schema names
// used by these commands.
func isCopyToExternalStorage(cmd CopyIn) bool {
	stmt := cmd.Stmt
	return (stmt.Table.Table() == NodelocalFileUploadTable ||
		stmt.Table.Table() == UserFileUploadTable) && stmt.Table.SchemaName == CrdbInternalName
}

// We handle the CopyFrom statement by creating a copyMachine and handing it
// control over the connection until the copying is done. The contract is that,
// when this is called, the pgwire.conn is not reading from the network
// connection any more until this returns. The copyMachine will to the reading
// and writing up to the CommandComplete message.
func (ex *connExecutor) execCopyIn(
	ctx context.Context, cmd CopyIn,
) (_ fsm.Event, retPayload fsm.EventPayload, retErr error) {
	ex.incrementStartedStmtCounter(cmd.Stmt)
	defer func() {
		if retErr == nil && !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(cmd.Stmt)
		}
	}()

	// When we're done, unblock the network connection.
	defer cmd.CopyDone.Done()

	state := ex.machine.CurState()
	_, isNoTxn := state.(stateNoTxn)
	_, isOpen := state.(stateOpen)
	if !isNoTxn && !isOpen {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlerrors.NewTransactionAbortedError("" /* customMsg */)}
		return ev, payload, nil
	}

	// If we're in an explicit txn, then the copying will be done within that
	// txn. Otherwise, we tell the copyMachine to manage its own transactions
	// and give it a closure to reset the accumulated extraTxnState.
	var txnOpt copyTxnOpt
	if isOpen {
		txnOpt = copyTxnOpt{
			txn:           ex.state.mu.txn,
			txnTimestamp:  ex.state.sqlTimestamp,
			stmtTimestamp: ex.server.cfg.Clock.PhysicalTime(),
		}
	} else {
		txnOpt = copyTxnOpt{
			resetExtraTxnState: func(ctx context.Context) error {
				return ex.resetExtraTxnState(ctx, txnEvent{eventType: noEvent})
			},
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
	txnOpt.resetPlanner = func(ctx context.Context, p *planner, txn *kv.Txn, txnTS time.Time, stmtTS time.Time) {
		// HACK: We're reaching inside ex.state and changing sqlTimestamp by hand.
		// It is used by resetPlanner. Normally sqlTimestamp is updated by the
		// state machine, but the copyMachine manages its own transactions without
		// going through the state machine.
		ex.state.sqlTimestamp = txnTS
		ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
		ex.initPlanner(ctx, p)
		ex.resetPlanner(ctx, p, txn, stmtTS)
	}
	var cm copyMachineInterface
	var err error
	if isCopyToExternalStorage(cmd) {
		cm, err = newFileUploadMachine(ctx, cmd.Conn, cmd.Stmt, txnOpt, ex.server.cfg)
	} else {
		cm, err = newCopyMachine(
			ctx, cmd.Conn, cmd.Stmt, txnOpt, ex.server.cfg,
			// execInsertPlan
			func(ctx context.Context, p *planner, res RestrictedCommandResult) error {
				_, err := ex.execWithDistSQLEngine(ctx, p, tree.RowsAffected, res, DistributionTypeNone, nil /* progressAtomic */)
				return err
			},
		)
	}
	if err != nil {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload, nil
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
	return stmt == nil || stmt.StatementReturnType() != tree.Rows
}

// generateID generates a unique ID based on the SQL instance ID and its current
// HLC timestamp. These IDs are either scoped at the query level or at the
// session level.
func (ex *connExecutor) generateID() ClusterWideID {
	return GenerateClusterWideID(ex.server.cfg.Clock.Now(), ex.server.cfg.NodeID.SQLInstanceID())
}

// commitPrepStmtNamespace deallocates everything in
// prepStmtsNamespaceAtTxnRewindPos that's not part of prepStmtsNamespace.
func (ex *connExecutor) commitPrepStmtNamespace(ctx context.Context) error {
	return ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(
		ctx, ex.extraTxnState.prepStmtsNamespace, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
}

// commitPrepStmtNamespace deallocates everything in prepStmtsNamespace that's
// not part of prepStmtsNamespaceAtTxnRewindPos.
func (ex *connExecutor) rewindPrepStmtNamespace(ctx context.Context) error {
	return ex.extraTxnState.prepStmtsNamespace.resetTo(
		ctx, ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
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

var retriableMinTimestampBoundUnsatisfiableError = errors.Newf(
	"retriable MinTimestampBoundUnsatisfiableError",
)

func errIsRetriable(err error) bool {
	return errors.HasType(err, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) ||
		scerrors.ConcurrentSchemaChangeDescID(err) != descpb.InvalidID ||
		errors.Is(err, retriableMinTimestampBoundUnsatisfiableError)
}

// makeErrEvent takes an error and returns either an eventRetriableErr or an
// eventNonRetriableErr, depending on the error type.
func (ex *connExecutor) makeErrEvent(err error, stmt tree.Statement) (fsm.Event, fsm.EventPayload) {
	// Check for MinTimestampBoundUnsatisfiableError errors.
	// If this is detected, it means we are potentially able to retry with a lower
	// MaxTimestampBound set if our MinTimestampBound was bumped up from the
	// original AS OF SYSTEM TIME timestamp set due to a schema bumping the
	// timestamp to a higher value.
	if minTSErr := (*roachpb.MinTimestampBoundUnsatisfiableError)(nil); errors.As(err, &minTSErr) {
		aost := ex.planner.EvalContext().AsOfSystemTime
		if aost != nil && aost.BoundedStaleness {
			if !aost.MaxTimestampBound.IsEmpty() && aost.MaxTimestampBound.LessEq(minTSErr.MinTimestampBound) {
				// If this occurs, we have a strange logic bug where we resolved
				// a minimum timestamp during a bounded staleness read to be greater
				// than or equal to the maximum staleness bound we put up.
				err = errors.CombineErrors(
					errors.AssertionFailedf(
						"unexpected MaxTimestampBound >= txn MinTimestampBound: %s >= %s",
						aost.MaxTimestampBound,
						minTSErr.MinTimestampBound,
					),
					err,
				)
			}
			if aost.Timestamp.Less(minTSErr.MinTimestampBound) {
				err = errors.Mark(err, retriableMinTimestampBoundUnsatisfiableError)
			}
		}
	}

	retriable := errIsRetriable(err)
	if retriable {
		var rc rewindCapability
		var canAutoRetry bool
		if ex.implicitTxn() || !ex.sessionData().InjectRetryErrorsEnabled {
			rc, canAutoRetry = ex.getRewindTxnCapability()
		}
		if canAutoRetry {
			ex.extraTxnState.autoRetryReason = err
		}

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

// setTransactionModes implements the txnModesSetter interface.
func (ex *connExecutor) setTransactionModes(
	ctx context.Context, modes tree.TransactionModes, asOfTs hlc.Timestamp,
) error {
	// This method cheats and manipulates ex.state directly, not through an event.
	// The alternative would be to create a special event, but it's unclear how
	// that'd work given that this method is called while executing a statement.

	// Transform the transaction options into the types needed by the state
	// machine.
	if modes.UserPriority != tree.UnspecifiedUserPriority {
		pri := txnPriorityToProto(modes.UserPriority)
		if err := ex.state.setPriority(pri); err != nil {
			return err
		}
	}
	if modes.Isolation != tree.UnspecifiedIsolation && modes.Isolation != tree.SerializableIsolation {
		return errors.AssertionFailedf(
			"unknown isolation level: %s", errors.Safe(modes.Isolation))
	}
	rwMode := modes.ReadWriteMode
	if modes.AsOf.Expr != nil && asOfTs.IsEmpty() {
		return errors.AssertionFailedf("expected an evaluated AS OF timestamp")
	}
	if !asOfTs.IsEmpty() {
		if err := ex.state.setHistoricalTimestamp(ctx, asOfTs); err != nil {
			return err
		}
		if rwMode == tree.UnspecifiedReadWriteMode {
			rwMode = tree.ReadOnly
		}
	}
	return ex.state.setReadOnlyMode(rwMode)
}

func txnPriorityToProto(mode tree.UserPriority) roachpb.UserPriority {
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
		log.Fatalf(context.Background(), "unknown user priority: %s", mode)
	}
	return pri
}

func (ex *connExecutor) txnPriorityWithSessionDefault(mode tree.UserPriority) roachpb.UserPriority {
	if mode == tree.UnspecifiedUserPriority {
		mode = tree.UserPriority(ex.sessionData().DefaultTxnPriority)
	}
	return txnPriorityToProto(mode)
}

// QualityOfService returns the QoSLevel session setting if the session
// settings are populated, otherwise the default QoSLevel.
func (ex *connExecutor) QualityOfService() sessiondatapb.QoSLevel {
	if ex.sessionData() == nil {
		return sessiondatapb.Normal
	}
	return ex.sessionData().DefaultTxnQualityOfService
}

func (ex *connExecutor) readWriteModeWithSessionDefault(
	mode tree.ReadWriteMode,
) tree.ReadWriteMode {
	if mode == tree.UnspecifiedReadWriteMode {
		if ex.sessionData().DefaultTxnReadOnly {
			return tree.ReadOnly
		}
		return tree.ReadWrite
	}
	return mode
}

// followerReadTimestampExpr is the function which can be used with AOST clauses
// to generate a timestamp likely to be safe for follower reads.
//
// NOTE: this cannot live in pkg/sql/sem/tree due to the call to WrapFunction,
// which fails before the pkg/sql/sem/builtins package has been initialized.
var followerReadTimestampExpr = &tree.FuncExpr{
	Func: tree.WrapFunction(tree.FollowerReadTimestampFunctionName),
}

func (ex *connExecutor) asOfClauseWithSessionDefault(expr tree.AsOfClause) tree.AsOfClause {
	if expr.Expr == nil {
		if ex.sessionData().DefaultTxnUseFollowerReads {
			return tree.AsOfClause{Expr: followerReadTimestampExpr}
		}
		return tree.AsOfClause{}
	}
	return expr
}

// initEvalCtx initializes the fields of an extendedEvalContext that stay the
// same across multiple statements. resetEvalCtx must also be called before each
// statement, to reinitialize other fields.
func (ex *connExecutor) initEvalCtx(ctx context.Context, evalCtx *extendedEvalContext, p *planner) {
	*evalCtx = extendedEvalContext{
		EvalContext: tree.EvalContext{
			Planner:                   p,
			PrivilegedAccessor:        p,
			SessionAccessor:           p,
			JobExecContext:            p,
			ClientNoticeSender:        p,
			Sequence:                  p,
			Tenant:                    p,
			Regions:                   p,
			JoinTokenCreator:          p,
			PreparedStatementState:    &ex.extraTxnState.prepStmtsNamespace,
			SessionDataStack:          ex.sessionDataStack,
			ReCache:                   ex.server.reCache,
			SQLStatsController:        ex.server.sqlStatsController,
			IndexUsageStatsController: ex.server.indexUsageStatsController,
		},
		Tracing:                &ex.sessionTracing,
		MemMetrics:             &ex.memMetrics,
		Descs:                  &ex.extraTxnState.descCollection,
		TxnModesSetter:         ex,
		Jobs:                   &ex.extraTxnState.jobs,
		SchemaChangeJobRecords: ex.extraTxnState.schemaChangeJobRecords,
		statsProvider:          ex.server.sqlStats,
		indexUsageStats:        ex.indexUsageStats,
		statementPreparer:      ex,
	}
	evalCtx.copyFromExecCfg(ex.server.cfg)
}

// resetEvalCtx initializes the fields of evalCtx that can change
// during a session (i.e. the fields not set by initEvalCtx).
//
// stmtTS is the timestamp that the statement_timestamp() SQL builtin will
// return for statements executed with this evalCtx. Since generally each
// statement is supposed to have a different timestamp, the evalCtx generally
// shouldn't be reused across statements.
func (ex *connExecutor) resetEvalCtx(evalCtx *extendedEvalContext, txn *kv.Txn, stmtTS time.Time) {
	newTxn := txn == nil || evalCtx.Txn != txn
	evalCtx.TxnState = ex.getTransactionState()
	evalCtx.TxnReadOnly = ex.state.readOnly
	evalCtx.TxnImplicit = ex.implicitTxn()
	if newTxn || !ex.implicitTxn() {
		// Only update the stmt timestamp if in a new txn or an explicit txn. This is because this gets
		// called multiple times during an extended protocol implicit txn, but we
		// want all those stages to share the same stmtTS.
		evalCtx.StmtTimestamp = stmtTS
	}
	evalCtx.TxnTimestamp = ex.state.sqlTimestamp
	evalCtx.Placeholders = nil
	evalCtx.Annotations = nil
	evalCtx.IVarContainer = nil
	evalCtx.Context = ex.Ctx()
	evalCtx.Txn = txn
	evalCtx.Mon = ex.state.mon
	evalCtx.PrepareOnly = false
	evalCtx.SkipNormalize = false
	evalCtx.SchemaChangerState = &ex.extraTxnState.schemaChangerState

	// If we are retrying due to an unsatisfiable timestamp bound which is
	// retriable, it means we were unable to serve the previous minimum timestamp
	// as there was a schema update in between. When retrying, we want to keep the
	// same minimum timestamp for the AOST read, but set the maximum timestamp
	// to the point just before our failed read to ensure we don't try to read
	// data which may be after the schema change when we retry.
	var minTSErr *roachpb.MinTimestampBoundUnsatisfiableError
	if err := ex.extraTxnState.autoRetryReason; err != nil && errors.As(err, &minTSErr) {
		nextMax := minTSErr.MinTimestampBound
		ex.extraTxnState.descCollection.SetMaxTimestampBound(nextMax)
		evalCtx.AsOfSystemTime.MaxTimestampBound = nextMax
	} else if newTxn {
		// Otherwise, only change the historical timestamps if this is a new txn.
		// This is because resetPlanner can be called multiple times for the same
		// txn during the extended protocol.
		ex.extraTxnState.descCollection.ResetMaxTimestampBound()
		evalCtx.AsOfSystemTime = nil
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

// initPlanner initializes a planner so it can can be used for planning a
// query in the context of this session.
func (ex *connExecutor) initPlanner(ctx context.Context, p *planner) {
	p.cancelChecker.Reset(ctx)

	ex.initEvalCtx(ctx, &p.extendedEvalCtx, p)

	p.sessionDataMutatorIterator = ex.dataMutatorIterator
	p.noticeSender = nil
	p.preparedStatements = ex.getPrepStmtsAccessor()
	p.sqlCursors = ex.getCursorAccessor()

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)
}

func (ex *connExecutor) resetPlanner(
	ctx context.Context, p *planner, txn *kv.Txn, stmtTS time.Time,
) {
	p.txn = txn
	p.stmt = Statement{}
	p.instrumentation = instrumentationHelper{}

	p.cancelChecker.Reset(ctx)

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.SearchPath = ex.sessionData().SearchPath
	p.semaCtx.IntervalStyleEnabled = ex.sessionData().IntervalStyleEnabled
	p.semaCtx.DateStyleEnabled = ex.sessionData().DateStyleEnabled
	p.semaCtx.Annotations = nil
	p.semaCtx.TypeResolver = p
	p.semaCtx.TableNameResolver = p
	p.semaCtx.DateStyle = ex.sessionData().GetDateStyle()
	p.semaCtx.IntervalStyle = ex.sessionData().GetIntervalStyle()

	ex.resetEvalCtx(&p.extendedEvalCtx, txn, stmtTS)

	p.autoCommit = false
	p.isPreparing = false
	p.avoidLeasedDescriptors = false
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
	txnIsOpen := false
	if os, ok := ex.machine.CurState().(stateOpen); ok {
		implicitTxn = os.ImplicitTxn.Get()
		txnIsOpen = true
	}

	ex.mu.Lock()
	err := ex.machine.ApplyWithPayload(withStatement(ex.Ctx(), ex.curStmtAST), ev, payload)
	ex.mu.Unlock()
	if err != nil {
		if errors.HasType(err, (*fsm.TransitionNotFoundError)(nil)) {
			panic(err)
		}
		return advanceInfo{}, err
	}

	advInfo := ex.state.consumeAdvanceInfo()
	if advInfo.code == rewind {
		atomic.AddInt32(ex.extraTxnState.atomicAutoRetryCounter, 1)
	}

	// If we had an error from DDL statement execution due to the presence of
	// other concurrent schema changes when attempting a schema change, wait for
	// the completion of those schema changes first.
	if p, ok := payload.(payloadWithError); ok {
		if descID := scerrors.ConcurrentSchemaChangeDescID(p.errorCause()); descID != descpb.InvalidID {
			if err := ex.handleWaitingForConcurrentSchemaChanges(ex.Ctx(), descID); err != nil {
				return advanceInfo{}, err
			}
		}
	}

	// Handle transaction events which cause updates to txnState.
	switch advInfo.txnEvent.eventType {
	case noEvent:
		_, nextStateIsAborted := ex.machine.CurState().(stateAborted)
		// Update the deadline on the transaction based on the collections,
		// if the transaction is currently open. If the next state is aborted
		// then the collection will get reset once we retry or rollback the
		// transaction, and no deadline needs to be picked up here.
		if txnIsOpen && !nextStateIsAborted {
			err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ex.Ctx(), ex.state.mu.txn)
			if err != nil {
				return advanceInfo{}, err
			}
		}
	case txnStart:
		atomic.StoreInt32(ex.extraTxnState.atomicAutoRetryCounter, 0)
		ex.extraTxnState.autoRetryReason = nil
		ex.recordTransactionStart(advInfo.txnEvent.txnID)
		// Bump the txn counter for logging.
		ex.extraTxnState.txnCounter++
		if !ex.server.cfg.Codec.ForSystemTenant() {
			// Update the leased descriptor collection with the current sqlliveness.Session.
			// This is required in the multi-tenant environment to update the transaction
			// deadline to either the session expiry or the leased descriptor deadline,
			// whichever is sooner. We need this to ensure that transactions initiated
			// by ephemeral SQL pods in multi-tenant environments are committed before the
			// session expires.
			session, err := ex.server.cfg.SQLLiveness.Session(ex.Ctx())
			if err != nil {
				return advanceInfo{}, err
			}
			ex.extraTxnState.descCollection.SetSession(session)
		}
	case txnCommit:
		if res.Err() != nil {
			// See https://github.com/cockroachdb/errors/issues/86.
			// nolint:errwrap
			err := errorutil.UnexpectedWithIssueErrorf(
				26687,
				"programming error: non-error event %s generated even though res.Err() has been set to: %s",
				errors.Safe(advInfo.txnEvent.eventType.String()),
				res.Err())
			log.Errorf(ex.Ctx(), "%v", err)
			errorutil.SendReport(ex.Ctx(), &ex.server.cfg.Settings.SV, err)
			return advanceInfo{}, err
		}

		handleErr := func(err error) {
			if implicitTxn {
				// The schema change/job failed but it was also the only
				// operation in the transaction. In this case, the transaction's
				// error is the schema change error.
				// TODO (lucy): I'm not sure the above is true. What about DROP TABLE
				// with multiple tables?
				res.SetError(err)
			} else {
				// The schema change/job failed but everything else in the
				// transaction was actually committed successfully already. At
				// this point, it is too late to cancel the transaction. In
				// effect, we have violated the "A" of ACID.
				//
				// This situation is sufficiently serious that we cannot let the
				// error that caused the schema change to fail flow back to the
				// client as-is. We replace it by a custom code dedicated to
				// this situation. Replacement occurs because this error code is
				// a "serious error" and the code computation logic will give it
				// a higher priority.
				//
				// We also print out the original error code as prefix of the
				// error message, in case it was a serious error.
				newErr := pgerror.Wrapf(err,
					pgcode.TransactionCommittedWithSchemaChangeFailure,
					"transaction committed but schema change aborted with error: (%s)",
					pgerror.GetPGCode(err))
				newErr = errors.WithHint(newErr,
					"Some of the non-DDL statements may have committed successfully, "+
						"but some of the DDL statement(s) failed.\nManual inspection may be "+
						"required to determine the actual state of the database.")
				newErr = errors.WithIssueLink(newErr,
					errors.IssueLink{IssueURL: "https://github.com/cockroachdb/cockroach/issues/42061"})
				res.SetError(newErr)
			}
		}
		ex.notifyStatsRefresherOfNewTables(ex.Ctx())

		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionStartPostCommitJob, timeutil.Now())
		if err := ex.server.cfg.JobRegistry.Run(
			ex.ctxHolder.connCtx,
			ex.server.cfg.InternalExecutor,
			ex.extraTxnState.jobs,
		); err != nil {
			handleErr(err)
		}
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionEndPostCommitJob, timeutil.Now())

		fallthrough
	case txnRestart, txnRollback:
		if err := ex.resetExtraTxnState(ex.Ctx(), advInfo.txnEvent); err != nil {
			return advanceInfo{}, err
		}
	default:
		return advanceInfo{}, errors.AssertionFailedf(
			"unexpected event: %v", errors.Safe(advInfo.txnEvent))
	}
	return advInfo, nil
}

func (ex *connExecutor) handleWaitingForConcurrentSchemaChanges(
	ctx context.Context, descID descpb.ID,
) error {
	if err := ex.planner.WaitForDescriptorSchemaChanges(
		ctx, descID, ex.extraTxnState.schemaChangerState,
	); err != nil {
		return err
	}
	// Restart the transaction at a higher timestamp after waiting.
	ex.state.mu.Lock()
	defer ex.state.mu.Unlock()
	userPriority := ex.state.mu.txn.UserPriority()
	ex.state.mu.txn = kv.NewTxnWithSteppingEnabled(ctx, ex.transitionCtx.db,
		ex.transitionCtx.nodeIDOrZero, ex.QualityOfService())
	return ex.state.mu.txn.SetUserPriority(userPriority)
}

// initStatementResult initializes res according to a query.
//
// cols represents the columns of the result rows. Should be nil if
// stmt.AST.StatementReturnType() != tree.Rows.
//
// If an error is returned, it is to be considered a query execution error.
func (ex *connExecutor) initStatementResult(
	ctx context.Context, res RestrictedCommandResult, ast tree.Statement, cols colinfo.ResultColumns,
) error {
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			return err
		}
	}
	if ast.StatementReturnType() == tree.Rows {
		// Note that this call is necessary even if cols is nil.
		res.SetColumns(ctx, cols)
	}
	return nil
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

// cancelCurrentQueries is part of the registrySession interface.
func (ex *connExecutor) cancelCurrentQueries() bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	canceled := false
	for _, queryMeta := range ex.mu.ActiveQueries {
		queryMeta.cancel()
		canceled = true
	}
	return canceled
}

// cancelSession is part of the registrySession interface.
func (ex *connExecutor) cancelSession() {
	if ex.onCancelSession == nil {
		return
	}
	// TODO(abhimadan): figure out how to send a nice error message to the client.
	ex.onCancelSession()
}

// user is part of the registrySession interface.
func (ex *connExecutor) user() security.SQLUsername {
	return ex.sessionData().User()
}

// serialize is part of the registrySession interface.
func (ex *connExecutor) serialize() serverpb.Session {
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	ex.state.mu.RLock()
	defer ex.state.mu.RUnlock()

	var activeTxnInfo *serverpb.TxnInfo
	txn := ex.state.mu.txn
	if txn != nil {
		id := txn.ID()
		activeTxnInfo = &serverpb.TxnInfo{
			ID:                    id,
			Start:                 ex.state.mu.txnStart,
			NumStatementsExecuted: int32(ex.state.mu.stmtCount),
			NumRetries:            int32(txn.Epoch()),
			NumAutoRetries:        atomic.LoadInt32(ex.extraTxnState.atomicAutoRetryCounter),
			TxnDescription:        txn.String(),
			Implicit:              ex.implicitTxn(),
			AllocBytes:            ex.state.mon.AllocBytes(),
			MaxAllocBytes:         ex.state.mon.MaximumBytes(),
			IsHistorical:          ex.state.isHistorical,
			ReadOnly:              ex.state.readOnly,
			Priority:              ex.state.priority.String(),
			QualityOfService:      sessiondatapb.ToQoSLevelString(txn.AdmissionHeader().Priority),
		}
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
		ast, err := query.getStatement()
		if err != nil {
			continue
		}
		sqlNoConstants := truncateSQL(formatStatementHideConstants(ast))
		sql := truncateSQL(ast.String())
		progress := math.Float64frombits(atomic.LoadUint64(&query.progressAtomic))
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			TxnID:          query.txnID,
			ID:             id.String(),
			Start:          query.start.UTC(),
			Sql:            sql,
			SqlNoConstants: sqlNoConstants,
			SqlSummary:     formatStatementSummary(ast),
			IsDistributed:  query.isDistributed,
			Phase:          (serverpb.ActiveQuery_Phase)(query.phase),
			Progress:       float32(progress),
		})
	}
	lastActiveQuery := ""
	lastActiveQueryNoConstants := ""
	if ex.mu.LastActiveQuery != nil {
		lastActiveQuery = truncateSQL(ex.mu.LastActiveQuery.String())
		lastActiveQueryNoConstants = truncateSQL(formatStatementHideConstants(ex.mu.LastActiveQuery))
	}

	// We always use base here as the fields from the SessionData should always
	// be that of the root session.
	sd := ex.sessionDataStack.Base()

	remoteStr := "<admin>"
	if sd.RemoteAddr != nil {
		remoteStr = sd.RemoteAddr.String()
	}

	return serverpb.Session{
		Username:                   sd.SessionUser().Normalized(),
		ClientAddress:              remoteStr,
		ApplicationName:            ex.applicationName.Load().(string),
		Start:                      ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionInit).UTC(),
		ActiveQueries:              activeQueries,
		ActiveTxn:                  activeTxnInfo,
		LastActiveQuery:            lastActiveQuery,
		ID:                         ex.sessionID.GetBytes(),
		AllocBytes:                 ex.mon.AllocBytes(),
		MaxAllocBytes:              ex.mon.MaximumBytes(),
		LastActiveQueryNoConstants: lastActiveQueryNoConstants,
	}
}

func (ex *connExecutor) getPrepStmtsAccessor() preparedStatementsAccessor {
	return connExPrepStmtsAccessor{
		ex: ex,
	}
}

func (ex *connExecutor) getCursorAccessor() sqlCursors {
	return connExCursorAccessor{
		ex: ex,
	}
}

// sessionEventf logs a message to the session event log (if any).
func (ex *connExecutor) sessionEventf(ctx context.Context, format string, args ...interface{}) {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventfDepth(ctx, 1 /* depth */, 2 /* level */, format, args...)
	}
	if ex.eventLog != nil {
		ex.eventLog.Printf(format, args...)
	}
}

// notifyStatsRefresherOfNewTables is called on txn commit to inform
// the stats refresher that new tables exist and should have their stats
// collected now.
func (ex *connExecutor) notifyStatsRefresherOfNewTables(ctx context.Context) {
	for _, desc := range ex.extraTxnState.descCollection.GetUncommittedTables() {
		// The CREATE STATISTICS run for an async CTAS query is initiated by the
		// SchemaChanger, so we don't do it here.
		if desc.IsTable() && !desc.IsAs() && desc.GetVersion() == 1 {
			// Initiate a run of CREATE STATISTICS. We use a large number
			// for rowsAffected because we want to make sure that stats always get
			// created/refreshed here.
			ex.planner.execCfg.StatsRefresher.NotifyMutation(desc, math.MaxInt32 /* rowsAffected */)
		}
	}
}

// runPreCommitStages is part of the new schema changer infrastructure to
// mutate descriptors prior to committing a SQL transaction.
func (ex *connExecutor) runPreCommitStages(ctx context.Context) error {
	scs := &ex.extraTxnState.schemaChangerState
	deps := newSchemaChangerTxnRunDependencies(
		ex.planner.SessionData(),
		ex.planner.User(),
		ex.server.cfg,
		ex.planner.txn,
		&ex.extraTxnState.descCollection,
		ex.planner.EvalContext(),
		ex.planner.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		scs.jobID,
		scs.stmts,
	)

	after, jobID, err := scrun.RunPreCommitPhase(
		ctx, ex.server.cfg.DeclarativeSchemaChangerTestingKnobs, deps, scs.state,
	)
	if err != nil {
		return err
	}
	scs.state = after
	scs.jobID = jobID
	if jobID != jobspb.InvalidJobID {
		ex.extraTxnState.jobs.add(jobID)
		log.Infof(ctx, "queued new schema change job %d using the new schema changer", jobID)
	}
	return nil
}

// StatementCounters groups metrics for counting different types of
// statements.
type StatementCounters struct {
	// QueryCount includes all statements and it is therefore the sum of
	// all the below metrics.
	QueryCount telemetry.CounterWithMetric

	// Basic CRUD statements.
	SelectCount telemetry.CounterWithMetric
	UpdateCount telemetry.CounterWithMetric
	InsertCount telemetry.CounterWithMetric
	DeleteCount telemetry.CounterWithMetric

	// Transaction operations.
	TxnBeginCount    telemetry.CounterWithMetric
	TxnCommitCount   telemetry.CounterWithMetric
	TxnRollbackCount telemetry.CounterWithMetric

	// Savepoint operations. SavepointCount is for real SQL savepoints;
	// the RestartSavepoint variants are for the
	// cockroach-specific client-side retry protocol.
	SavepointCount                  telemetry.CounterWithMetric
	ReleaseSavepointCount           telemetry.CounterWithMetric
	RollbackToSavepointCount        telemetry.CounterWithMetric
	RestartSavepointCount           telemetry.CounterWithMetric
	ReleaseRestartSavepointCount    telemetry.CounterWithMetric
	RollbackToRestartSavepointCount telemetry.CounterWithMetric

	// CopyCount counts all COPY statements.
	CopyCount telemetry.CounterWithMetric

	// DdlCount counts all statements whose StatementReturnType is DDL.
	DdlCount telemetry.CounterWithMetric

	// MiscCount counts all statements not covered by a more specific stat above.
	MiscCount telemetry.CounterWithMetric
}

func makeStartedStatementCounters(internal bool) StatementCounters {
	return StatementCounters{
		TxnBeginCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnBeginStarted, internal)),
		TxnCommitCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnCommitStarted, internal)),
		TxnRollbackCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnRollbackStarted, internal)),
		RestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRestartSavepointStarted, internal)),
		ReleaseRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseRestartSavepointStarted, internal)),
		RollbackToRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToRestartSavepointStarted, internal)),
		SavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSavepointStarted, internal)),
		ReleaseSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseSavepointStarted, internal)),
		RollbackToSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToSavepointStarted, internal)),
		SelectCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSelectStarted, internal)),
		UpdateCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaUpdateStarted, internal)),
		InsertCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaInsertStarted, internal)),
		DeleteCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDeleteStarted, internal)),
		DdlCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDdlStarted, internal)),
		CopyCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCopyStarted, internal)),
		MiscCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaMiscStarted, internal)),
		QueryCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaQueryStarted, internal)),
	}
}

func makeExecutedStatementCounters(internal bool) StatementCounters {
	return StatementCounters{
		TxnBeginCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnBeginExecuted, internal)),
		TxnCommitCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnCommitExecuted, internal)),
		TxnRollbackCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnRollbackExecuted, internal)),
		RestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRestartSavepointExecuted, internal)),
		ReleaseRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseRestartSavepointExecuted, internal)),
		RollbackToRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToRestartSavepointExecuted, internal)),
		SavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSavepointExecuted, internal)),
		ReleaseSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseSavepointExecuted, internal)),
		RollbackToSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToSavepointExecuted, internal)),
		SelectCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSelectExecuted, internal)),
		UpdateCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaUpdateExecuted, internal)),
		InsertCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaInsertExecuted, internal)),
		DeleteCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDeleteExecuted, internal)),
		DdlCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDdlExecuted, internal)),
		CopyCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCopyExecuted, internal)),
		MiscCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaMiscExecuted, internal)),
		QueryCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaQueryExecuted, internal)),
	}
}

func (sc *StatementCounters) incrementCount(ex *connExecutor, stmt tree.Statement) {
	sc.QueryCount.Inc()
	switch t := stmt.(type) {
	case *tree.BeginTransaction:
		sc.TxnBeginCount.Inc()
	case *tree.Select:
		sc.SelectCount.Inc()
	case *tree.Update:
		sc.UpdateCount.Inc()
	case *tree.Insert:
		sc.InsertCount.Inc()
	case *tree.Delete:
		sc.DeleteCount.Inc()
	case *tree.CommitTransaction:
		sc.TxnCommitCount.Inc()
	case *tree.RollbackTransaction:
		// The CommitWait state means that the transaction has already committed
		// after a specially handled `RELEASE SAVEPOINT cockroach_restart` command.
		if ex.getTransactionState() == CommitWaitStateStr {
			sc.TxnCommitCount.Inc()
		} else {
			sc.TxnRollbackCount.Inc()
		}
	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(t.Name) {
			sc.RestartSavepointCount.Inc()
		} else {
			sc.SavepointCount.Inc()
		}
	case *tree.ReleaseSavepoint:
		if ex.isCommitOnReleaseSavepoint(t.Savepoint) {
			sc.ReleaseRestartSavepointCount.Inc()
		} else {
			sc.ReleaseSavepointCount.Inc()
		}
	case *tree.RollbackToSavepoint:
		if ex.isCommitOnReleaseSavepoint(t.Savepoint) {
			sc.RollbackToRestartSavepointCount.Inc()
		} else {
			sc.RollbackToSavepointCount.Inc()
		}
	case *tree.CopyFrom:
		sc.CopyCount.Inc()
	default:
		if tree.CanModifySchema(stmt) {
			sc.DdlCount.Inc()
		} else {
			sc.MiscCount.Inc()
		}
	}
}

// connExPrepStmtsAccessor is an implementation of preparedStatementsAccessor
// that gives access to a connExecutor's prepared statements.
type connExPrepStmtsAccessor struct {
	ex *connExecutor
}

var _ preparedStatementsAccessor = connExPrepStmtsAccessor{}

// List is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) List() map[string]*PreparedStatement {
	// Return a copy of the data, to prevent modification of the map.
	stmts := ps.ex.extraTxnState.prepStmtsNamespace.prepStmts
	ret := make(map[string]*PreparedStatement, len(stmts))
	for key, stmt := range stmts {
		ret[key] = stmt
	}
	return ret
}

// Get is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Get(name string) (*PreparedStatement, bool) {
	s, ok := ps.ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
	return s, ok
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
	ps.ex.extraTxnState.prepStmtsNamespace.resetToEmpty(
		ctx, &ps.ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
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
	logcrash.RegisterTagFn("statement", func(ctx context.Context) string {
		stmt := statementFromCtx(ctx)
		if stmt == nil {
			return ""
		}
		// Anonymize the statement for reporting.
		return anonymizeStmtAndConstants(stmt, nil /* VirtualTabler */)
	})
}
