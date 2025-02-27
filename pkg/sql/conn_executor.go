// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantcpu"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/txnidcache"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/idxrecommendations"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/ctxlog"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/sentryutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tochar"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/petermattis/goid"
)

var maxNumNonAdminConnections = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.max_connections_per_gateway",
	"the maximum number of SQL connections per gateway allowed at a given time "+
		"(note: this will only limit future connection attempts and will not affect already established connections). "+
		"Negative values result in unlimited number of connections. Superusers are not affected by this limit.",
	-1, // Postgres defaults to 100, but we default to -1 to match our previous behavior of unlimited.
	settings.WithPublic)

// Note(alyshan): This setting is not public. It is intended to be used by Cockroach Cloud to limit
// connections to serverless clusters while still being able to connect from the Cockroach Cloud control plane.
// This setting may be extended one day to include an arbitrary list of users to exclude from connection limiting.
// This setting may be removed one day.
var maxNumNonRootConnections = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.cockroach_cloud.max_client_connections_per_gateway",
	"this setting is intended to be used by Cockroach Cloud for limiting connections to serverless clusters. "+
		"The maximum number of SQL connections per gateway allowed at a given time "+
		"(note: this will only limit future connection attempts and will not affect already established connections). "+
		"Negative values result in unlimited number of connections. Cockroach Cloud internal users (including root user) "+
		"are not affected by this limit.",
	-1,
)

var maxOpenTransactions = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.max_open_transactions_per_gateway",
	"the maximum number of open SQL transactions per gateway allowed at a given time. "+
		"Negative values result in unlimited number of connections. Superusers are not affected by this limit.",
	-1,
	settings.WithPublic)

// maxNumNonRootConnectionsReason is used to supplement the error message for connections that denied due to
// server.cockroach_cloud.max_client_connections_per_gateway.
// Note(alyshan): This setting is not public. It is intended to be used by Cockroach Cloud when limiting
// connections to serverless clusters.
// This setting may be removed one day.
var maxNumNonRootConnectionsReason = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"server.cockroach_cloud.max_client_connections_per_gateway_reason",
	"a reason to provide in the error message for connections that are denied due to "+
		"server.cockroach_cloud.max_client_connections_per_gateway",
	"cluster connections are limited",
)

// detailedLatencyMetrics enables separate per-statement-fingerprint latency histograms. The utility of this extra
// detail is expected to exceed its costs in most workloads.
var detailedLatencyMetrics = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.detailed_latency_metrics.enabled",
	"label latency metrics with the statement fingerprint. Workloads with tens of thousands of "+
		"distinct query fingerprints should leave this setting false. "+
		"(experimental, affects performance for workloads with high "+
		"fingerprint cardinality)",
	false,
	settings.WithPublic,
)

// The metric label name we'll use to facet latency metrics by statement fingerprint.
var detailedLatencyMetricLabel = "fingerprint"

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

	// sqlStats provides the mechanisms for writing sql stats to system tables.
	sqlStats *persistedsqlstats.PersistedSQLStats

	// localSqlStats tracks per-application statistics for all applications on each
	// node. Newly collected statistics flow into localSqlStats.
	localSqlStats *sslocal.SQLStats

	// sqlStatsController is the control-plane interface for sqlStats.
	sqlStatsController *persistedsqlstats.Controller

	// schemaTelemetryController is the control-plane interface for schema
	// telemetry.
	schemaTelemetryController *schematelemetrycontroller.Controller

	// indexUsageStatsController is the control-plane interface for
	// indexUsageStats.
	indexUsageStatsController *idxusage.Controller

	// reportedStats is a pool of stats that is held for reporting, and is
	// cleared on a lower interval than sqlStats. Stats from sqlStats flow
	// into reported stats when sqlStats is cleared.
	reportedStats *sslocal.SQLStats

	// reportedStatsController is the control-plane interface for
	// reportedStatsController.
	reportedStatsController *sslocal.Controller

	insights *insights.Provider

	reCache           *tree.RegexpCache
	toCharFormatCache *tochar.FormatCache

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
	TelemetryLoggingMetrics *telemetryLoggingMetrics

	idxRecommendationsCache *idxrecommendations.IndexRecCache

	mu struct {
		syncutil.Mutex
		connectionCount     int64
		rootConnectionCount int64
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

	// InsightsMetrics contains metrics related to outlier detection.
	InsightsMetrics insights.Metrics
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig, pool *mon.BytesMonitor) *Server {
	metrics := makeMetrics(false /* internal */, &cfg.Settings.SV)
	serverMetrics := makeServerMetrics(cfg)
	insightsProvider := insights.New(cfg.Settings, serverMetrics.InsightsMetrics, cfg.InsightsTestingKnobs)
	// TODO(117690): Unify StmtStatsEnable and TxnStatsEnable into a single cluster setting.
	sqlstats.TxnStatsEnable.SetOnChange(&cfg.Settings.SV, func(_ context.Context) {
		if !sqlstats.TxnStatsEnable.Get(&cfg.Settings.SV) {
			insightsProvider.Writer().Clear()
		}
	})
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
	reportedSQLStatsController := reportedSQLStats.GetController(cfg.SQLStatusServer)
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
		InternalMetrics:         makeMetrics(true /* internal */, &cfg.Settings.SV),
		ServerMetrics:           serverMetrics,
		pool:                    pool,
		localSqlStats:           memSQLStats,
		reportedStats:           reportedSQLStats,
		reportedStatsController: reportedSQLStatsController,
		insights:                insightsProvider,
		reCache:                 tree.NewRegexpCache(512),
		toCharFormatCache:       tochar.NewFormatCache(512),
		indexUsageStats: idxusage.NewLocalIndexUsageStats(&idxusage.Config{
			ChannelSize: idxusage.DefaultChannelSize,
			Setting:     cfg.Settings,
		}),
		txnIDCache: txnidcache.NewTxnIDCache(
			cfg.Settings,
			&serverMetrics.ContentionSubsystemMetrics),
		idxRecommendationsCache: idxrecommendations.NewIndexRecommendationsCache(cfg.Settings),
	}

	telemetryLoggingMetrics := newTelemetryLoggingMetrics(cfg.TelemetryLoggingTestingKnobs, cfg.Settings)
	s.TelemetryLoggingMetrics = telemetryLoggingMetrics

	sqlStatsInternalExecutorMonitor := MakeInternalExecutorMemMonitor(MemoryMetrics{}, s.GetExecutorConfig().Settings)
	sqlStatsInternalExecutorMonitor.StartNoReserved(context.Background(), s.GetBytesMonitor())
	persistedSQLStats := persistedsqlstats.New(&persistedsqlstats.Config{
		Settings:                s.cfg.Settings,
		InternalExecutorMonitor: sqlStatsInternalExecutorMonitor,
		DB: NewInternalDB(
			s, MemoryMetrics{}, sqlStatsInternalExecutorMonitor,
		),
		ClusterID:               s.cfg.NodeInfo.LogicalClusterID,
		SQLIDContainer:          cfg.NodeInfo.NodeID,
		JobRegistry:             s.cfg.JobRegistry,
		Knobs:                   cfg.SQLStatsTestingKnobs,
		FlushesSuccessful:       serverMetrics.StatsMetrics.SQLStatsFlushesSuccessful,
		FlushDoneSignalsIgnored: serverMetrics.StatsMetrics.SQLStatsFlushDoneSignalsIgnored,
		FlushedFingerprintCount: serverMetrics.StatsMetrics.SQLStatsFlushFingerprintCount,
		FlushesFailed:           serverMetrics.StatsMetrics.SQLStatsFlushesFailed,
		FlushLatency:            serverMetrics.StatsMetrics.SQLStatsFlushLatency,
	}, memSQLStats)

	s.sqlStats = persistedSQLStats
	s.sqlStatsController = persistedSQLStats.GetController(cfg.SQLStatusServer)
	schemaTelemetryIEMonitor := MakeInternalExecutorMemMonitor(MemoryMetrics{}, s.GetExecutorConfig().Settings)
	schemaTelemetryIEMonitor.StartNoReserved(context.Background(), s.GetBytesMonitor())
	s.schemaTelemetryController = schematelemetrycontroller.NewController(
		NewInternalDB(
			s, MemoryMetrics{}, schemaTelemetryIEMonitor,
		),
		schemaTelemetryIEMonitor,
		s.cfg.Settings, s.cfg.JobRegistry,
		s.cfg.NodeInfo.LogicalClusterID,
	)
	s.indexUsageStatsController = idxusage.NewController(cfg.SQLStatusServer)
	return s
}

func makeMetrics(internal bool, sv *settings.Values) Metrics {
	// For internal metrics, don't facet the latency metrics on the fingerprint
	facetLabels := make([]string, 0, 1)
	if !internal {
		facetLabels = append(facetLabels, detailedLatencyMetricLabel)
	}

	sqlExecLatencyDetail := metric.NewExportedHistogramVec(
		getMetricMeta(MetaSQLExecLatencyDetail, internal),
		metric.IOLatencyBuckets,
		facetLabels,
	)

	// Clear the latency metrics when we toggle the fingerprint detail setting on or off
	detailedLatencyMetrics.SetOnChange(sv, func(ctx context.Context) {
		sqlExecLatencyDetail.Clear()
	})

	return Metrics{
		EngineMetrics: EngineMetrics{
			DistSQLSelectCount:            metric.NewCounter(getMetricMeta(MetaDistSQLSelect, internal)),
			DistSQLSelectDistributedCount: metric.NewCounter(getMetricMeta(MetaDistSQLSelectDistributed, internal)),
			SQLOptFallbackCount:           metric.NewCounter(getMetricMeta(MetaSQLOptFallback, internal)),
			SQLOptPlanCacheHits:           metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheHits, internal)),
			SQLOptPlanCacheMisses:         metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheMisses, internal)),
			StatementFingerprintCount:     metric.NewUniqueCounter(getMetricMeta(MetaUniqueStatementCount, internal)),
			SQLExecLatencyDetail:          sqlExecLatencyDetail,
			// TODO(mrtracy): See HistogramWindowInterval in server/config.go for the 6x factor.
			DistSQLExecLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     getMetricMeta(MetaDistSQLExecLatency, internal),
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			SQLExecLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     getMetricMeta(MetaSQLExecLatency, internal),
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			DistSQLServiceLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     getMetricMeta(MetaDistSQLServiceLatency, internal),
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			SQLServiceLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     getMetricMeta(MetaSQLServiceLatency, internal),
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			SQLTxnLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     getMetricMeta(MetaSQLTxnLatency, internal),
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
			SQLTxnsOpen:         metric.NewGauge(getMetricMeta(MetaSQLTxnsOpen, internal)),
			SQLActiveStatements: metric.NewGauge(getMetricMeta(MetaSQLActiveQueries, internal)),
			SQLContendedTxns:    metric.NewCounter(getMetricMeta(MetaSQLTxnContended, internal)),

			TxnAbortCount:                     metric.NewCounter(getMetricMeta(MetaTxnAbort, internal)),
			FailureCount:                      metric.NewCounter(getMetricMeta(MetaFailure, internal)),
			StatementTimeoutCount:             metric.NewCounter(getMetricMeta(MetaStatementTimeout, internal)),
			TransactionTimeoutCount:           metric.NewCounter(getMetricMeta(MetaTransactionTimeout, internal)),
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
			SQLStatsMemoryMaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
				Metadata:     MetaSQLStatsMemMaxBytes,
				Duration:     cfg.HistogramWindowInterval,
				MaxVal:       log10int64times1000,
				SigFigs:      3,
				BucketConfig: metric.MemoryUsage64MBBuckets,
			}),
			SQLStatsMemoryCurBytesCount: metric.NewGauge(MetaSQLStatsMemCurBytes),
			ReportedSQLStatsMemoryMaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
				Metadata:     MetaReportedSQLStatsMemMaxBytes,
				Duration:     cfg.HistogramWindowInterval,
				MaxVal:       log10int64times1000,
				SigFigs:      3,
				BucketConfig: metric.MemoryUsage64MBBuckets,
			}),
			ReportedSQLStatsMemoryCurBytesCount: metric.NewGauge(MetaReportedSQLStatsMemCurBytes),
			DiscardedStatsCount:                 metric.NewCounter(MetaDiscardedSQLStats),
			SQLStatsFlushesSuccessful:           metric.NewCounter(MetaSQLStatsFlushesSuccessful),
			SQLStatsFlushDoneSignalsIgnored:     metric.NewCounter(MetaSQLStatsFlushDoneSignalsIgnored),
			SQLStatsFlushFingerprintCount:       metric.NewCounter(MetaSQLStatsFlushFingerprintCount),

			SQLStatsFlushesFailed: metric.NewCounter(MetaSQLStatsFlushesFailed),
			SQLStatsFlushLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     MetaSQLStatsFlushLatency,
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.BatchProcessLatencyBuckets,
			}),
			SQLStatsRemovedRows: metric.NewCounter(MetaSQLStatsRemovedRows),
			SQLTxnStatsCollectionOverhead: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePreferHdrLatency,
				Metadata:     MetaSQLTxnStatsCollectionOverhead,
				Duration:     6 * metricsSampleInterval,
				BucketConfig: metric.IOLatencyBuckets,
			}),
		},
		ContentionSubsystemMetrics: txnidcache.NewMetrics(),
		InsightsMetrics:            insights.NewMetrics(),
	}
}

// Start starts the Server's background processing.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	// Exclude SQL background processing from cost accounting and limiting.
	// NOTE: Only exclude background processing that is not under user control.
	// If a user can opt in/out of some aspect of background processing, then it
	// should be accounted for in their costs.
	ctx = multitenant.WithTenantCostControlExemption(ctx)

	s.insights.Start(ctx, stopper)
	s.sqlStats.Start(ctx, stopper)

	s.schemaTelemetryController.Start(ctx, stopper)

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

// GetSchemaTelemetryController returns the schematelemetryschedule.Controller
// for current sql.Server's schema telemetry.
func (s *Server) GetSchemaTelemetryController() *schematelemetrycontroller.Controller {
	return s.schemaTelemetryController
}

// GetIndexUsageStatsController returns the idxusage.Controller for current
// sql.Server's index usage stats.
func (s *Server) GetIndexUsageStatsController() *idxusage.Controller {
	return s.indexUsageStatsController
}

// GetInsightsReader returns the insights store for the current sql.Server's
// detected execution insights.
func (s *Server) GetInsightsReader() *insights.LockingStore {
	return s.insights.Store()
}

// GetSQLStatsProvider returns the provider for the sqlstats subsystem.
func (s *Server) GetSQLStatsProvider() *persistedsqlstats.PersistedSQLStats {
	return s.sqlStats
}

// GetLocalSQLStatsProvider returns the in-memory provider for the sqlstats subsystem.
func (s *Server) GetLocalSQLStatsProvider() *sslocal.SQLStats {
	return s.localSqlStats
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
) ([]appstatspb.CollectedStatementStatistics, error) {
	return s.getScrubbedStmtStats(ctx, s.localSqlStats, math.MaxInt32, true)
}

// Avoid lint errors.
var _ = (*Server).GetScrubbedStmtStats

// GetUnscrubbedStmtStats returns the same thing as GetScrubbedStmtStats, except
// identifiers (e.g. table and column names) aren't scrubbed from the statements.
func (s *Server) GetUnscrubbedStmtStats(
	ctx context.Context,
) ([]appstatspb.CollectedStatementStatistics, error) {
	var stmtStats []appstatspb.CollectedStatementStatistics
	stmtStatsVisitor := func(_ context.Context, stat *appstatspb.CollectedStatementStatistics) error {
		stmtStats = append(stmtStats, *stat)
		return nil
	}
	err := s.localSqlStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{}, stmtStatsVisitor)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch statement stats")
	}

	return stmtStats, nil
}

// GetUnscrubbedTxnStats returns the same transaction statistics by app.
// Identifiers (e.g. table and column names) aren't scrubbed from the statements.
func (s *Server) GetUnscrubbedTxnStats(
	ctx context.Context,
) ([]appstatspb.CollectedTransactionStatistics, error) {
	var txnStats []appstatspb.CollectedTransactionStatistics
	txnStatsVisitor := func(_ context.Context, stat *appstatspb.CollectedTransactionStatistics) error {
		txnStats = append(txnStats, *stat)
		return nil
	}
	err := s.localSqlStats.IterateTransactionStats(ctx, sqlstats.IteratorOptions{}, txnStatsVisitor)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch statement stats")
	}

	return txnStats, nil
}

// GetScrubbedReportingStats does the same thing as GetScrubbedStmtStats but
// returns statistics from the reported stats pool.
func (s *Server) GetScrubbedReportingStats(
	ctx context.Context, limit int, includeInternal bool,
) ([]appstatspb.CollectedStatementStatistics, error) {
	return s.getScrubbedStmtStats(ctx, s.reportedStats, limit, includeInternal)
}

func (s *Server) getScrubbedStmtStats(
	ctx context.Context, statsProvider *sslocal.SQLStats, limit int, includeInternal bool,
) ([]appstatspb.CollectedStatementStatistics, error) {
	salt := ClusterSecret.Get(&s.cfg.Settings.SV)

	var scrubbedStats []appstatspb.CollectedStatementStatistics
	stmtStatsVisitor := func(_ context.Context, stat *appstatspb.CollectedStatementStatistics) error {
		if limit <= (len(scrubbedStats)) {
			return nil
		}

		if !includeInternal && strings.HasPrefix(stat.Key.App, catconstants.InternalAppNamePrefix) {
			return nil
		}

		// Scrub the statement itself.
		scrubbedQueryStr, ok := scrubStmtStatKey(s.cfg.VirtualSchemas, stat.Key.Query, nil)

		// We don't want to report this stats if scrubbing has failed. We also don't
		// wish to abort here because we want to try our best to report all the
		// stats.
		if !ok {
			return nil
		}

		stat.Key.Query = scrubbedQueryStr
		stat.Key.App = MaybeHashAppName(stat.Key.App, salt)

		// Quantize the counts to avoid leaking information that way.
		quantizeCounts(&stat.Stats)
		stat.Stats.SensitiveInfo = stat.Stats.SensitiveInfo.GetScrubbedCopy()

		scrubbedStats = append(scrubbedStats, *stat)
		return nil
	}

	err := statsProvider.IterateStatementStats(ctx, sqlstats.IteratorOptions{}, stmtStatsVisitor)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch scrubbed statement stats")
	}

	return scrubbedStats, nil
}

// GetStmtStatsLastReset returns the time at which the statement statistics were
// last cleared.
func (s *Server) GetStmtStatsLastReset() time.Time {
	return s.localSqlStats.GetLastReset()
}

// GetExecutorConfig returns this server's executor config.
func (s *Server) GetExecutorConfig() *ExecutorConfig {
	return s.cfg
}

// GetBytesMonitor returns this server's BytesMonitor.
func (s *Server) GetBytesMonitor() *mon.BytesMonitor {
	return s.pool
}

// SetupConn creates a connExecutor for the client connection.
//
// When this method returns there are no resources allocated yet that
// need to be close()d.
//
// Args:
// args: The initial session parameters. They are validated by SetupConn
// and an error is returned if this validation fails.
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
// produce results for the client.
// memMetrics: The metrics that statements executed on this connection will
// contribute to.
func (s *Server) SetupConn(
	ctx context.Context,
	args SessionArgs,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
	onDefaultIntSizeChange func(newSize int32),
	sessionID clusterunique.ID,
) (ConnectionHandler, error) {
	sd := newSessionData(args)
	sds := sessiondata.NewStack(sd)
	// Set the SessionData from args.SessionDefaults. This also validates the
	// respective values.
	sdMutIterator := makeSessionDataMutatorIterator(sds, args.SessionDefaults, s.cfg.Settings)
	sdMutIterator.onDefaultIntSizeChange = onDefaultIntSizeChange
	if err := sdMutIterator.applyOnEachMutatorError(func(m sessionDataMutator) error {
		for varName, v := range varGen {
			if v.Set != nil {
				hasDefault, defVal := getSessionVarDefaultString(varName, v, m.sessionDataMutatorBase)
				if hasDefault {
					if err := v.Set(ctx, m, defVal); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		log.Errorf(ctx, "error setting up client session: %s", err)
		return ConnectionHandler{}, err
	}

	ex := s.newConnExecutor(
		ctx,
		executorTypeExec,
		sdMutIterator,
		stmtBuf,
		clientComm,
		memMetrics,
		&s.Metrics,
		s.localSqlStats.GetApplicationStats(sd.ApplicationName),
		sessionID,
		false, /* underOuterTxn */
		nil,   /* postSetupFn */
	)
	return ConnectionHandler{ex}, nil
}

// IncrementConnectionCount increases connectionCount by 1 if possible and
// rootConnectionCount by 1 if applicable.
//
// decrementConnectionCount must be called if err is nil.
func (s *Server) IncrementConnectionCount(
	sessionArgs SessionArgs,
) (decrementConnectionCount func(), _ error) {
	sv := &s.cfg.Settings.SV
	maxNumNonRootConnectionsValue := maxNumNonRootConnections.Get(sv)
	maxNumConnectionsValue := maxNumNonAdminConnections.Get(sv)
	maxNumNonRootConnectionsReasonValue := maxNumNonRootConnectionsReason.Get(sv)
	var maxNumNonRootConnectionsExceeded, maxNumConnectionsExceeded bool
	// This lock blocks other connections from being made so minimize the amount
	// of work done inside lock.
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		// Root user is not affected by connection limits.
		if sessionArgs.User.IsRootUser() {
			s.mu.connectionCount++
			s.mu.rootConnectionCount++
			decrementConnectionCount = func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.mu.connectionCount--
				s.mu.rootConnectionCount--
			}
			return
		}
		connectionCount := s.mu.connectionCount
		nonRootConnectionCount := connectionCount - s.mu.rootConnectionCount
		maxNumNonRootConnectionsExceeded = maxNumNonRootConnectionsValue >= 0 && nonRootConnectionCount >= maxNumNonRootConnectionsValue
		if maxNumNonRootConnectionsExceeded {
			return
		}
		maxNumConnectionsExceeded = !sessionArgs.IsSuperuser && maxNumConnectionsValue >= 0 && connectionCount >= maxNumConnectionsValue
		if maxNumConnectionsExceeded {
			return
		}
		s.mu.connectionCount++
		decrementConnectionCount = func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.mu.connectionCount--
		}
	}()
	if maxNumNonRootConnectionsExceeded {
		return nil, errors.WithHintf(
			pgerror.Newf(pgcode.TooManyConnections, "%s", redact.SafeString(maxNumNonRootConnectionsReasonValue)),
			"the maximum number of allowed connections is %d",
			maxNumNonRootConnectionsValue,
		)
	}
	if maxNumConnectionsExceeded {
		return nil, errors.WithHintf(
			pgerror.New(pgcode.TooManyConnections, "sorry, too many clients already"),
			"the maximum number of allowed connections is %d and can be modified using the %s config key",
			maxNumConnectionsValue,
			maxNumNonAdminConnections.Name(),
		)
	}
	return decrementConnectionCount, nil
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
// connExecutor takes ownership of this memory and will close the account before
// exiting.
func (s *Server) ServeConn(
	ctx context.Context, h ConnectionHandler, reserved *mon.BoundAccount, cancel context.CancelFunc,
) error {
	// Make sure to clear the reserved account even if closeWrapper below
	// panics: so we do it in a defer that is guaranteed to execute. We also
	// cannot clear it before closeWrapper since we need to close the internal
	// monitors of the connExecutor first.
	defer reserved.Clear(ctx)
	defer func(ctx context.Context, h ConnectionHandler) {
		r := recover()
		h.ex.closeWrapper(ctx, r)
	}(ctx, h)
	return h.ex.run(ctx, s.pool, reserved, cancel)
}

// GetLocalIndexStatistics returns a idxusage.LocalIndexUsageStats.
func (s *Server) GetLocalIndexStatistics() *idxusage.LocalIndexUsageStats {
	return s.indexUsageStats
}

// newSessionData a SessionData that can be passed to newConnExecutor.
func newSessionData(args SessionArgs) *sessiondata.SessionData {
	sd := &sessiondata.SessionData{
		SessionData: sessiondatapb.SessionData{
			UserProto: args.User.EncodeProto(),
		},
		LocalUnmigratableSessionData: sessiondata.LocalUnmigratableSessionData{
			RemoteAddr:           args.RemoteAddr,
			IsSSL:                args.IsSSL,
			AuthenticationMethod: args.AuthenticationMethod,
		},
		LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
			ResultsBufferSize:   args.ConnResultsBufferSize,
			IsSuperuser:         args.IsSuperuser,
			SystemIdentityProto: args.SystemIdentity,
		},
	}
	if len(args.CustomOptionSessionDefaults) > 0 {
		sd.CustomOptions = make(map[string]string)
		for k, v := range args.CustomOptionSessionDefaults {
			sd.CustomOptions[k] = v
		}
	}
	sd.SearchPath = sessiondata.DefaultSearchPathForUser(sd.User())
	populateMinimalSessionData(sd)
	return sd
}

// populateMinimalSessionData populates sd with some minimal values needed for
// not crashing. Fields of sd that are already set are not overwritten.
func populateMinimalSessionData(sd *sessiondata.SessionData) {
	if sd.SequenceState == nil {
		sd.SequenceState = sessiondata.NewSequenceState()
	}
	if sd.Location == nil {
		sd.Location = time.UTC
	}
}

// newConnExecutor creates a new connExecutor.
//
// - underOuterTxn indicates whether the conn executor is constructed when there
// is already an outstanding "outer" txn. This is the case with an internal
// executor with non-nil txn.
func (s *Server) newConnExecutor(
	ctx context.Context,
	executorType executorType,
	sdMutIterator *sessionDataMutatorIterator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
	applicationStats *ssmemstorage.Container,
	sessionID clusterunique.ID,
	underOuterTxn bool,
	postSetupFn func(ex *connExecutor),
) *connExecutor {
	// Create the various monitors.
	// The session monitors are started in activate().
	sessionRootMon := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("session root"),
		CurCount: memMetrics.CurBytesCount,
		MaxHist:  memMetrics.MaxBytesHist,
		Settings: s.cfg.Settings,
	})
	sessionMon := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("session"),
		CurCount: memMetrics.SessionCurBytesCount,
		MaxHist:  memMetrics.SessionMaxBytesHist,
		Settings: s.cfg.Settings,
	})
	sessionPreparedMon := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("session prepared statements"),
		CurCount:  memMetrics.SessionPreparedCurBytesCount,
		MaxHist:   memMetrics.SessionPreparedMaxBytesHist,
		Increment: 1024,
		Settings:  s.cfg.Settings,
	})
	// The txn monitor is started in txnState.resetForNewSQLTxn().
	txnMon := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("txn"),
		CurCount: memMetrics.TxnCurBytesCount,
		MaxHist:  memMetrics.TxnMaxBytesHist,
		Settings: s.cfg.Settings,
	})
	txnFingerprintIDCacheAcc := sessionMon.MakeBoundAccount()

	nodeIDOrZero, _ := s.cfg.NodeInfo.NodeID.OptionalNodeID()
	ex := &connExecutor{
		server:              s,
		metrics:             srvMetrics,
		stmtBuf:             stmtBuf,
		clientComm:          clientComm,
		mon:                 sessionRootMon,
		sessionMon:          sessionMon,
		sessionPreparedMon:  sessionPreparedMon,
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
		planner: planner{
			execCfg:    s.cfg,
			datumAlloc: &tree.DatumAlloc{},
		},

		// ctxHolder will be reset at the start of run(). We only define
		// it here so that an early call to close() doesn't panic.
		ctxHolder:                 ctxHolder{connCtx: ctx, goroutineID: goid.Get()},
		phaseTimes:                sessionphase.NewTimes(),
		executorType:              executorType,
		hasCreatedTemporarySchema: false,
		stmtDiagnosticsRecorder:   s.cfg.StmtDiagnosticsRecorder,
		indexUsageStats:           s.indexUsageStats,
		txnIDCacheWriter:          s.txnIDCache,
		totalActiveTimeStopWatch:  timeutil.NewStopWatch(),
		txnFingerprintIDCache:     NewTxnFingerprintIDCache(ctx, s.cfg.Settings, &txnFingerprintIDCacheAcc),
		txnFingerprintIDAcc:       &txnFingerprintIDCacheAcc,
	}
	ex.rng.internal = rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	ex.state.txnAbortCount = ex.metrics.EngineMetrics.TxnAbortCount

	// The transaction_read_only variable is special; its updates need to be
	// hooked-up to the executor.
	ex.dataMutatorIterator.setCurTxnReadOnly = func(readOnly bool) error {
		mode := tree.ReadWrite
		if readOnly {
			mode = tree.ReadOnly
		}
		return ex.state.setReadOnlyMode(mode)
	}
	// kv_transaction_buffered_writes_enabled is special since it won't affect
	// the current explicit txn, so we want to let the user know.
	ex.dataMutatorIterator.setBufferedWritesEnabled = func(enabled bool) {
		if ex.state.mu.txn.BufferedWritesEnabled() == enabled {
			return
		}
		if !ex.implicitTxn() {
			action := "enabling"
			if !enabled {
				action = "disabling"
			}
			ex.planner.BufferClientNotice(ctx, pgnotice.Newf("%s buffered writes will apply after the current txn commits", action))
		}
	}
	ex.dataMutatorIterator.onTempSchemaCreation = func() {
		ex.hasCreatedTemporarySchema = true
	}

	ex.dataMutatorIterator.upgradedIsolationLevel = func(
		ctx context.Context, upgradedFrom tree.IsolationLevel, requiresNotice bool,
	) {
		telemetry.Inc(sqltelemetry.IsolationLevelUpgradedCounter(ctx, upgradedFrom))
		ex.metrics.ExecutedStatementCounters.TxnUpgradedCount.Inc(1)
		if requiresNotice {
			const msgFmt = "%s isolation level is not allowed without an enterprise license; upgrading to SERIALIZABLE"
			displayLevel := upgradedFrom
			if upgradedFrom == tree.ReadUncommittedIsolation {
				displayLevel = tree.ReadCommittedIsolation
			} else if upgradedFrom == tree.SnapshotIsolation {
				displayLevel = tree.RepeatableReadIsolation
			}
			if logIsolationLevelLimiter.ShouldLog() {
				log.Warningf(ctx, msgFmt, displayLevel)
			}
			ex.planner.BufferClientNotice(ctx, pgnotice.Newf(msgFmt, displayLevel))
		}
	}

	ex.applicationName.Store(ex.sessionData().ApplicationName)

	ex.applicationStats = applicationStats
	// We ignore statements and transactions run by the internal executor by
	// passing a nil writer.
	var writer *insights.ConcurrentBufferIngester
	if !ex.sessionData().Internal {
		writer = ex.server.insights.Writer()
	}
	ex.statsCollector = sslocal.NewStatsCollector(
		s.cfg.Settings,
		applicationStats,
		writer,
		ex.phaseTimes,
		s.localSqlStats.GetCounters(),
		underOuterTxn,
		s.cfg.SQLStatsTestingKnobs,
	)
	ex.dataMutatorIterator.onApplicationNameChange = func(newName string) {
		ex.applicationName.Store(newName)
		ex.applicationStats = ex.server.localSqlStats.GetApplicationStats(newName)
	}

	ex.extraTxnState.underOuterTxn = underOuterTxn
	ex.extraTxnState.prepStmtsNamespace = prepStmtNamespace{
		prepStmts:    make(map[string]*PreparedStatement),
		prepStmtsLRU: make(map[string]struct{ prev, next string }),
		portals:      make(map[string]PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos = prepStmtNamespace{
		prepStmts:    make(map[string]*PreparedStatement),
		prepStmtsLRU: make(map[string]struct{ prev, next string }),
		portals:      make(map[string]PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceMemAcc = ex.sessionMon.MakeBoundAccount()
	dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(sdMutIterator.sds)
	ex.extraTxnState.descCollection = s.cfg.CollectionFactory.NewCollection(
		ctx, descs.WithDescriptorSessionDataProvider(dsdp), descs.WithMonitor(ex.sessionMon),
	)
	ex.extraTxnState.jobs = newTxnJobsCollection()
	ex.extraTxnState.txnRewindPos = -1
	ex.extraTxnState.schemaChangerState = &SchemaChangerState{
		mode:   ex.sessionData().NewSchemaChangerMode,
		memAcc: ex.sessionMon.MakeBoundAccount(),
	}
	ex.queryCancelKey = pgwirecancel.MakeBackendKeyData(ex.rng.internal, ex.server.cfg.NodeInfo.NodeID.SQLInstanceID())
	ex.mu.ActiveQueries = make(map[clusterunique.ID]*queryMeta)
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.sessionTracing.ex = ex
	ex.transitionCtx.sessionTracing = &ex.sessionTracing

	ex.extraTxnState.hasAdminRoleCache = HasAdminRoleCache{}

	if lm := ex.server.cfg.LeaseManager; executorType == executorTypeExec && lm != nil {
		if desc, err := lm.Acquire(ctx, ex.server.cfg.Clock.Now(), keys.SystemDatabaseID); err != nil {
			log.Infof(ctx, "unable to lease system database to determine if PCR reader is in use: %s", err)
		} else {
			defer desc.Release(ctx)
			// The system database ReplicatedPCRVersion is set during reader tenant bootstrap,
			// which guarantees that all user tenant sql connections to the reader tenant will
			// correctly set this
			ex.isPCRReaderCatalog = desc.Underlying().(catalog.DatabaseDescriptor).GetReplicatedPCRVersion() != 0
		}
	}

	if postSetupFn != nil {
		postSetupFn(ex)
	}

	ex.initPlanner(ctx, &ex.planner)
	ex.planner.extendedEvalCtx.SessionID = sessionID

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
			panicErr = WithAnonymizedStatement(panicErr, ex.curStmtAST, vt, nil)
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

var connExecutorNormalCloseErr = pgerror.Newf(pgcode.AdminShutdown, "connExecutor closing")

func (ex *connExecutor) close(ctx context.Context, closeType closeType) {
	ex.sessionEventf(ctx, "finishing connExecutor")

	txnEvType := noEvent
	if _, noTxn := ex.machine.CurState().(stateNoTxn); !noTxn {
		txnEvType = txnRollback
	}

	// Close all portals and cursors, otherwise there will be leftover bytes.
	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(
		ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.closeAllPortals(
		ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
	if err := ex.extraTxnState.sqlCursors.closeAll(&ex.planner, cursorCloseForExplicitClose); err != nil {
		log.Warningf(ctx, "error closing cursors: %v", err)
	}

	// Free any memory used by the stats collector.
	ex.statsCollector.Close(ctx, ex.planner.extendedEvalCtx.SessionID)

	var payloadErr error
	if closeType == normalClose {
		// We'll cleanup the SQL txn by creating a non-retriable (commit:true) event.
		// This event is guaranteed to be accepted in every state.
		ev := eventNonRetriableErr{IsCommit: fsm.True}
		payloadErr = connExecutorNormalCloseErr
		payload := eventNonRetriableErrPayload{err: payloadErr}
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

	ex.resetExtraTxnState(ctx, txnEvent{eventType: txnEvType}, payloadErr)
	if ex.hasCreatedTemporarySchema && !ex.server.cfg.TestingKnobs.DisableTempObjectsCleanupOnSessionExit {
		err := cleanupSessionTempObjects(
			ctx,
			ex.server.cfg.InternalDB,
			ex.server.cfg.Codec,
			ex.planner.extendedEvalCtx.SessionID,
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
		// Close all statements and prepared portals. The cursors have already been
		// closed.
		ex.extraTxnState.prepStmtsNamespace.resetToEmpty(
			ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetToEmpty(
			ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.prepStmtsNamespaceMemAcc.Close(ctx)
	}

	if ex.sessionTracing.Enabled() {
		if err := ex.sessionTracing.StopTracing(); err != nil {
			log.Warningf(ctx, "error stopping tracing: %s", err)
		}
	}

	// Stop idle timer if the connExecutor is closed to ensure cancel session
	// is not called.
	ex.mu.IdleInSessionTimeout.Stop()
	ex.mu.IdleInTransactionSessionTimeout.Stop()
	ex.mu.TransactionTimeout.Stop()

	ex.txnFingerprintIDAcc.Close(ctx)
	if closeType != panicClose {
		ex.state.mon.Stop(ctx)
		ex.sessionPreparedMon.Stop(ctx)
		ex.sessionMon.Stop(ctx)
		ex.mon.Stop(ctx)
	} else {
		ex.state.mon.EmergencyStop(ctx)
		ex.sessionPreparedMon.EmergencyStop(ctx)
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

	// sessionPreparedMon tracks memory usage by prepared statements.
	sessionPreparedMon *mon.BytesMonitor
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

	// extraTxnState groups fields scoped to a SQL txn that are not handled by
	// ex.state, above. The rule of thumb is that, if the state influences state
	// transitions, it should live in state, otherwise it can live here.
	// This is only used in the Open state. extraTxnState is reset whenever a
	// transaction finishes or gets retried. Additionally, if the field is
	// accessed outside the connExecutor's goroutine, it should
	// be added to the mu struct in connExecutor's txnState. Notably if
	// the field is accessed in connExecutor's serialize function, it should be
	// added to txnState behind the mutex.
	extraTxnState struct {
		// underOuterTxn indicates whether the conn executor is used by an
		// internal executor with an outer txn.
		underOuterTxn bool
		// skipResettingSchemaObjects should be set true when underOuterTxn is
		// set AND the conn executor should **not** release the leases of
		// descriptor collections or delete schema change job records upon
		// closing (the caller of the internal executor is responsible for
		// that).
		skipResettingSchemaObjects bool

		// shouldResetSyntheticDescriptors should be set to true only if
		// skipResettingSchemaObjects is set to true, and, upon finishing the
		// statement, the synthetic descriptors should be reset. This exists to
		// support injecting synthetic descriptors via the InternalExecutor
		// methods like WithSyntheticDescriptors. Note that we'll never use
		// those methods when injecting synthetic descriptors during execution
		// by the declarative schema changer.
		shouldResetSyntheticDescriptors bool

		// descCollection collects descriptors used by the current transaction.
		descCollection *descs.Collection

		jobs *txnJobsCollection

		// firstStmtExecuted indicates that the first statement inside this
		// transaction has been executed.
		firstStmtExecuted bool

		// upgradedToSerializable indicates that the transaction has been implicitly
		// upgraded to the SERIALIZABLE isolation level.
		upgradedToSerializable bool

		// numDDL keeps track of how many DDL statements have been
		// executed so far.
		numDDL int

		// numRows keeps track of the number of rows that have been observed by this
		// transaction. This is simply the summation of number of rows observed by
		// comprising statements.
		numRows int

		// validateDbZoneConfig should the DB zone config on commit.
		validateDbZoneConfig bool

		// txnCounter keeps track of how many SQL txns have been open since
		// the start of the session. This is used for logging, to
		// distinguish statements that belong to separate SQL transactions.
		// A txn unique key can be obtained by grouping this counter with
		// the session ID.
		//
		// Note that a single SQL txn can use multiple KV txns under the
		// hood with multiple KV txn UUIDs, so the KV UUID is not a good
		// txn identifier for SQL logging.
		txnCounter atomic.Int32

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

		// sqlCursors contains the list of SQL cursors the session currently has
		// access to.
		//
		// Cursors declared WITH HOLD belong to the session, and can outlive their
		// parent transaction. Otherwise, cursors are bound to their transaction,
		// and are destroyed when the transaction finishes.
		sqlCursors cursorMap

		// shouldExecuteOnTxnFinish indicates that ex.onTxnFinish will be called
		// when txn is finished (either committed or aborted). It is true when
		// txn is started but can remain false when txn is executed within
		// another higher-level txn.
		shouldExecuteOnTxnFinish bool

		// txnFinishClosure contains fields that ex.onTxnFinish uses to execute.
		txnFinishClosure struct {
			// txnStartTime is the time that the transaction started.
			txnStartTime crtime.Mono
			// implicit is whether the transaction was implicit.
			implicit bool
		}

		// storedProcTxnState contains fields that are used for explicit transaction
		// management in stored procedures. Both fields are set during execution of
		// a stored procedure through the planner. Both fields are reset by the
		// connExecutor in execCmd, depending on the execution status (e.g. success,
		// error, retry).
		storedProcTxnState struct {
			// txnOp, if not set to StoredProcTxnNoOp, indicates that the current txn
			// should be committed or aborted. The connExecutor uses it in
			// execStmtInOpenState to decide whether to commit or rollback the current
			// transaction.
			txnOp tree.StoredProcTxnOp

			// txnModes allows for SET TRANSACTION statements to apply to the new txn
			// that starts after the COMMIT/ROLLBACK.
			txnModes *tree.TransactionModes

			// resumeProc, if non-nil, contains the plan for a CALL statement that
			// will continue execution of a stored procedure that previously suspended
			// execution in order to commit or abort its transaction. The planner
			// checks for resumeProc in buildExecMemo, and executes it as the next
			// statement if it is non-nil.
			resumeProc *memo.Memo
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
		transactionStatementFingerprintIDs []appstatspb.StmtFingerprintID

		// transactionStatementsHash is the hashed accumulation of all statementFingerprintIDs
		// that comprise the transaction. It is used to construct the key when
		// recording transaction statistics. It's important to accumulate this hash
		// as we go along in addition to the transactionStatementFingerprintIDs as
		// transactionStatementFingerprintIDs are capped to prevent unbound expansion, but we
		// still need the statementFingerprintID hash to disambiguate beyond the capped
		// statements.
		transactionStatementsHash util.FNV64

		// schemaChangerState captures the state of the ongoing declarative schema
		// in the transaction.
		schemaChangerState *SchemaChangerState

		// shouldCollectTxnExecutionStats specifies whether the statements in
		// this transaction should collect execution stats.
		shouldCollectTxnExecutionStats bool
		// accumulatedStats are the accumulated stats of all statements.
		accumulatedStats execstats.QueryLevelStats

		// idleLatency is the cumulative amount of time spent waiting for the
		// client to send statements while holding the transaction open.
		idleLatency time.Duration

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

		// shouldAcceptReleaseCockroachRestartInCommitWait is set to true if the
		// transaction had SAVEPOINT cockroach_restart installed at the time that
		// SHOW COMMIT TIMESTAMP was executed to commit the transaction. If so, the
		// connExecutor will permit one invocation of RELEASE SAVEPOINT
		// cockroach_restart while in the CommitWait state.
		shouldAcceptReleaseCockroachRestartInCommitWait bool

		// hasAdminRole is used to cache if the user running the transaction
		// has admin privilege. hasAdminRoleCache is set for the first statement
		// in a transaction.
		hasAdminRoleCache HasAdminRoleCache

		// createdSequences keeps track of sequences created in the current transaction.
		// The map key is the sequence descpb.ID.
		createdSequences map[descpb.ID]struct{}

		// shouldLogToTelemetry indicates if the current transaction should be
		// logged to telemetry. It is used in telemetry transaction sampling
		// mode to emit all statement events for a particular transaction.
		// Note that the number of statement events emitted per transaction is
		// capped by the cluster setting telemetryStatementsPerTransactionMax.
		shouldLogToTelemetry bool

		// telemetrySkippedTxns contains the number of transactions skipped by
		// telemetry logging prior to this one.
		telemetrySkippedTxns uint64
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
	applicationStats *ssmemstorage.Container

	// statsCollector is used to collect statistics about SQL statements and
	// transactions.
	statsCollector *sslocal.StatsCollector

	// cpuStatsCollector is used to estimate RU consumption due to CPU usage for
	// tenants.
	cpuStatsCollector multitenantcpu.CPUUsageHelper

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

	// rng contains random number generators for this session.
	rng struct {
		// internal is used for internal operations like determining the query
		// cancel key and whether sampling execution stats should be performed.
		internal *rand.Rand
		// external is used to power random() builtin. It is important to store
		// this field by value so that the same RNG is reused throughout the
		// whole session.
		external eval.RNGFactory
		// ulidEntropy is used to power gen_random_ulid builtin. It is important
		// to store this field by value so that the same source is reused
		// throughout the whole session. Under the hood it'll use 'external' RNG
		// as the entropy source.
		ulidEntropy eval.ULIDEntropyFactory
	}

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[clusterunique.ID]*queryMeta

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

		// TransactionTimeout is configured if the transaction_timeout is set and
		// the connExecutor is in an explicit transaction. It fires if we are
		// waiting for the next statement while the timeout is exceeded.
		TransactionTimeout timeout
	}

	// curStmtAST is the statement that's currently being prepared or executed, if
	// any. This is printed by high-level panic recovery.
	curStmtAST tree.Statement

	// queryCancelKey is a 64-bit identifier for the session used by the
	// pgwire cancellation protocol.
	queryCancelKey pgwirecancel.BackendKeyData

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

	// txnFingerprintIDCache is used to track the most recent
	// txnFingerprintIDs executed in this session.
	txnFingerprintIDCache *TxnFingerprintIDCache
	txnFingerprintIDAcc   *mon.BoundAccount

	// totalActiveTimeStopWatch tracks the total active time of the session.
	// This is defined as the time spent executing transactions and statements.
	totalActiveTimeStopWatch *timeutil.StopWatch

	// previousTransactionCommitTimestamp is the timestamp of the previous
	// transaction which committed. It is zero-valued when there is a transaction
	// open or the previous transaction did not successfully commit.
	previousTransactionCommitTimestamp hlc.Timestamp

	// isPCRReader catalog indicates this connection executor is for
	// PCR reader catalog, which is done by checking for the ReplicatedPCRVersion
	// field on the system database (which is set during tenant bootstrap).
	isPCRReaderCatalog bool
}

// ctxHolder contains a connection's context and, while session tracing is
// enabled, a derived context with a recording span. The connExecutor should use
// the latter while session tracing is active, or the former otherwise; that's
// what the ctx() method returns.
type ctxHolder struct {
	connCtx           context.Context
	sessionTracingCtx context.Context
	goroutineID       int64
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
	// prepStmtsLRU is a circular doubly-linked list containing the prepared
	// statement names ordered by most recent access (needed to determine
	// evictions when prepared_statements_cache_size is set). There is a special
	// entry for the empty string which is both the head and tail of the
	// list. (Consequently, if it exists, the actual prepared statement for the
	// empty string does not have an entry in this list and cannot be evicted.)
	prepStmtsLRU map[string]struct{ prev, next string }
	// prepStmtsLRUAlloc is the total amount of memory allocated for prepared
	// statements in prepStmtsLRU. This will sometimes be less than
	// ex.sessionPreparedMon.AllocBytes() because refcounting causes us to hold
	// onto more PreparedStatements than are currently in the LRU list.
	prepStmtsLRUAlloc int64
	// portals contains the portals currently available on the session. Note
	// that PreparedPortal.accountForCopy needs to be called if a copy of a
	// PreparedPortal is retained.
	portals map[string]PreparedPortal
}

// HasActivePortals returns true if there are portals in the session.
func (ns *prepStmtNamespace) HasActivePortals() bool {
	return len(ns.portals) > 0
}

// HasPortal returns true if there exists a given named portal in the session.
func (ns *prepStmtNamespace) HasPortal(s string) bool {
	_, ok := ns.portals[s]
	return ok
}

const prepStmtsLRUHead = ""
const prepStmtsLRUTail = ""

// addLRUEntry adds a new prepared statement name to the LRU list. It is an
// error to re-add an existing name to the LRU list.
func (ns *prepStmtNamespace) addLRUEntry(name string, alloc int64) {
	if name == prepStmtsLRUHead {
		return
	}
	if _, ok := ns.prepStmtsLRU[name]; ok {
		// Assert that we're not re-adding an existing name to the LRU list.
		panic(errors.AssertionFailedf(
			"prepStmtsLRU unexpected existing entry (%s): %v", name, ns.prepStmtsLRU,
		))
	}
	var this struct{ prev, next string }
	this.prev = prepStmtsLRUHead
	// Note: must do this serially in case head and next are the same entry.
	head := ns.prepStmtsLRU[this.prev]
	this.next = head.next
	head.next = name
	ns.prepStmtsLRU[prepStmtsLRUHead] = head
	next, ok := ns.prepStmtsLRU[this.next]
	if !ok || next.prev != prepStmtsLRUHead {
		// Assert that the chain isn't broken before we modify it.
		panic(errors.AssertionFailedf(
			"prepStmtsLRU head entry not correct (%s): %v", this.next, ns.prepStmtsLRU,
		))
	}
	next.prev = name
	ns.prepStmtsLRU[this.next] = next
	ns.prepStmtsLRU[name] = this
	ns.prepStmtsLRUAlloc += alloc
}

// delLRUEntry removes a prepared statement name from the LRU list. (It is not an
// error to remove a non-existent prepared statement.)
func (ns *prepStmtNamespace) delLRUEntry(name string, alloc int64) {
	if name == prepStmtsLRUHead {
		return
	}
	this, ok := ns.prepStmtsLRU[name]
	if !ok {
		// Not an error to remove a non-existent prepared statement.
		return
	}
	// Note: must do this serially in case prev and next are the same entry.
	prev, ok := ns.prepStmtsLRU[this.prev]
	if !ok || prev.next != name {
		// Assert that the chain isn't broken before we modify it.
		panic(errors.AssertionFailedf(
			"prepStmtsLRU prev entry not correct (%s): %v", this.prev, ns.prepStmtsLRU,
		))
	}
	prev.next = this.next
	ns.prepStmtsLRU[this.prev] = prev
	next, ok := ns.prepStmtsLRU[this.next]
	if !ok || next.prev != name {
		// Assert that the chain isn't broken before we modify it.
		panic(errors.AssertionFailedf(
			"prepStmtsLRU next entry not correct (%s): %v", this.next, ns.prepStmtsLRU,
		))
	}
	next.prev = this.prev
	ns.prepStmtsLRU[this.next] = next
	delete(ns.prepStmtsLRU, name)
	ns.prepStmtsLRUAlloc -= alloc
}

// touchLRUEntry moves an existing prepared statement to the front of the LRU
// list.
func (ns *prepStmtNamespace) touchLRUEntry(name string) {
	if name == prepStmtsLRUHead {
		return
	}
	if ns.prepStmtsLRU[prepStmtsLRUHead].next == name {
		// Already at the front of the list.
		return
	}
	ns.delLRUEntry(name, 0)
	ns.addLRUEntry(name, 0)
}

func (ns *prepStmtNamespace) closeAllPortals(
	ctx context.Context, prepStmtsNamespaceMemAcc *mon.BoundAccount,
) {
	for name, p := range ns.portals {
		p.close(ctx, prepStmtsNamespaceMemAcc, name)
		delete(ns.portals, name)
	}
}

func (ns *prepStmtNamespace) closeAllPausablePortals(
	ctx context.Context, prepStmtsNamespaceMemAcc *mon.BoundAccount,
) {
	for name, p := range ns.portals {
		if p.pauseInfo != nil {
			p.close(ctx, prepStmtsNamespaceMemAcc, name)
			delete(ns.portals, name)
		}
	}
}

// MigratablePreparedStatements returns a mapping of all prepared statements.
func (ns *prepStmtNamespace) MigratablePreparedStatements() []sessiondatapb.MigratableSession_PreparedStatement {
	ret := make([]sessiondatapb.MigratableSession_PreparedStatement, 0, len(ns.prepStmts))

	// Serialize prepared statements from least-recently used to most-recently
	// used, so that we build the LRU list correctly when deserializing.
	for e, ok := ns.prepStmtsLRU[prepStmtsLRUTail]; ok && e.prev != prepStmtsLRUHead; e, ok = ns.prepStmtsLRU[e.prev] {
		name := e.prev
		stmt := ns.prepStmts[name]
		ret = append(
			ret,
			sessiondatapb.MigratableSession_PreparedStatement{
				Name:                 name,
				PlaceholderTypeHints: stmt.InferredTypes,
				SQL:                  stmt.SQL,
			},
		)
	}
	// Finally, serialize the anonymous prepared statement (if it exists).
	if stmt, ok := ns.prepStmts[""]; ok {
		ret = append(
			ret,
			sessiondatapb.MigratableSession_PreparedStatement{
				Name:                 "",
				PlaceholderTypeHints: stmt.InferredTypes,
				SQL:                  stmt.SQL,
			},
		)
	}

	return ret
}

func (ns *prepStmtNamespace) String() string {
	var sb strings.Builder
	sb.WriteString("Prep stmts: ")
	// Put the anonymous prepared statement first (if it exists).
	if _, ok := ns.prepStmts[""]; ok {
		sb.WriteString("\"\" ")
	}
	for e, ok := ns.prepStmtsLRU[prepStmtsLRUHead]; ok && e.next != prepStmtsLRUTail; e, ok = ns.prepStmtsLRU[e.next] {
		sb.WriteString(e.next + " ")
	}
	fmt.Fprintf(&sb, "LRU alloc: %d ", ns.prepStmtsLRUAlloc)
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
	_ = ns.resetTo(ctx, &prepStmtNamespace{}, prepStmtsNamespaceMemAcc)
}

// resetTo resets a namespace to equate another one (`to`). All the receiver's
// references are released and all the to's references are duplicated.
//
// An empty `to` can be passed in to deallocate everything.
//
// It can only return an error if we've reached the memory limit and had to make
// a copy of portals.
func (ns *prepStmtNamespace) resetTo(
	ctx context.Context, to *prepStmtNamespace, prepStmtsNamespaceMemAcc *mon.BoundAccount,
) error {
	// Reset prepStmts.
	if !maps.Equal(ns.prepStmts, to.prepStmts) {
		for name, ps := range ns.prepStmts {
			ps.decRef(ctx)
			delete(ns.prepStmts, name)
		}
		for name, ps := range to.prepStmts {
			ps.incRef(ctx)
			ns.prepStmts[name] = ps
		}
	}

	// Reset prepStmtsLRU.
	if !maps.Equal(ns.prepStmtsLRU, to.prepStmtsLRU) {
		clear(ns.prepStmtsLRU)
		maps.Copy(ns.prepStmtsLRU, to.prepStmtsLRU)
	}

	// Reset prepStmtsLRUAlloc.
	ns.prepStmtsLRUAlloc = to.prepStmtsLRUAlloc

	// Reset portals.
	ns.closeAllPortals(ctx, prepStmtsNamespaceMemAcc)
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
// transaction event, resetExtraTxnState invokes corresponding callbacks.
// The payload error is included for statistics recording.
// (e.g. onTxnFinish() and onTxnRestart()).
func (ex *connExecutor) resetExtraTxnState(ctx context.Context, ev txnEvent, payloadErr error) {
	ex.extraTxnState.numDDL = 0
	ex.extraTxnState.firstStmtExecuted = false
	ex.extraTxnState.upgradedToSerializable = false
	ex.extraTxnState.hasAdminRoleCache = HasAdminRoleCache{}
	ex.extraTxnState.createdSequences = nil

	if ex.extraTxnState.skipResettingSchemaObjects {
		if ex.extraTxnState.shouldResetSyntheticDescriptors {
			ex.extraTxnState.descCollection.ResetSyntheticDescriptors()
		}
	} else {
		ex.extraTxnState.descCollection.ReleaseAll(ctx)
		ex.extraTxnState.jobs.reset()
		ex.extraTxnState.validateDbZoneConfig = false
		ex.extraTxnState.schemaChangerState.memAcc.Clear(ctx)
		ex.extraTxnState.schemaChangerState = &SchemaChangerState{
			mode:   ex.sessionData().NewSchemaChangerMode,
			memAcc: ex.sessionMon.MakeBoundAccount(),
		}
	}

	// Close all portals.
	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(
		ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)

	// Close all (non HOLD) cursors.
	var closeReason cursorCloseReason
	switch ev.eventType {
	case txnCommit:
		closeReason = cursorCloseForTxnCommit
	case txnPrepare:
		closeReason = cursorCloseForTxnPrepare
	default:
		closeReason = cursorCloseForTxnRollback
	}
	if err := ex.extraTxnState.sqlCursors.closeAll(&ex.planner, closeReason); err != nil {
		log.Warningf(ctx, "error closing cursors: %v", err)
	}

	switch ev.eventType {
	case txnCommit, txnRollback, txnPrepare:
		ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.closeAllPortals(
			ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.savepoints.clear()
		ex.onTxnFinish(ctx, ev, payloadErr)
	case txnRestart:
		ex.onTxnRestart(ctx)
		ex.state.mu.Lock()
		defer ex.state.mu.Unlock()
		ex.state.mu.stmtCount = 0
	}

	// NOTE: on txnRestart we don't need to muck with the savepoints stack. It's either a
	// a ROLLBACK TO SAVEPOINT that generated the event, and that statement deals with the
	// savepoints, or it's a rewind which also deals with them.
}

// Ctx returns the transaction's ctx, if we're inside a transaction, or the
// session's context otherwise.
func (ex *connExecutor) Ctx() context.Context {
	ctx := ex.state.Ctx
	if _, ok := ex.machine.CurState().(stateNoTxn); ok {
		ctx = ex.ctxHolder.ctx()
	}
	// stateInternalError is used by the Executor.
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
// ownership of this memory.
func (ex *connExecutor) activate(
	ctx context.Context, parentMon *mon.BytesMonitor, reserved *mon.BoundAccount,
) {
	// Note: we pass `reserved` to sessionRootMon where it causes it to act as a
	// buffer. This is not done for sessionMon nor state.mon: these monitors don't
	// start with any buffer, so they'll need to ask their "parent" for memory as
	// soon as the first allocation. This is acceptable because the session is
	// single threaded, and the point of buffering is just to avoid contention.
	ex.mon.Start(ctx, parentMon, reserved)
	ex.sessionMon.StartNoReserved(ctx, ex.mon)
	ex.sessionPreparedMon.StartNoReserved(ctx, ex.sessionMon)

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
	reserved *mon.BoundAccount,
	onCancel context.CancelFunc,
) (err error) {
	if !ex.activated {
		ex.activate(ctx, parentMon, reserved)
	}
	ex.ctxHolder.connCtx = ctx
	ex.onCancelSession = onCancel

	sessionID := ex.planner.extendedEvalCtx.SessionID
	ex.server.cfg.SessionRegistry.register(sessionID, ex.queryCancelKey, ex)

	defer func() {
		ex.server.cfg.SessionRegistry.deregister(sessionID, ex.queryCancelKey)
		addToClosedSessionCache := ex.executorType == executorTypeExec
		if ex.executorType == executorTypeInternal {
			rate := closedSessionCacheInternalSamplingProbability.Get(&ex.server.cfg.Settings.SV)
			addToClosedSessionCache = ex.rng.internal.Float64() < rate
		}
		if addToClosedSessionCache {
			addErr := ex.server.cfg.ClosedSessionCache.add(ctx, sessionID, ex.serialize())
			if addErr != nil {
				err = errors.CombineErrors(err, addErr)
			}
		}
	}()

	for {
		ex.curStmtAST = nil
		if err := ctx.Err(); err != nil {
			return err
		}

		var err error
		if err = ex.execCmd(); err != nil {
			// Both of these errors are normal ways for the connExecutor to exit.
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
func (ex *connExecutor) execCmd() (retErr error) {
	ctx := ex.Ctx()
	cmd, pos, err := ex.stmtBuf.CurCmd()
	if err != nil {
		return err // err could be io.EOF
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
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
				PortalPausabilityDisabled, /* portalPausability */
			)
			res = stmtRes

			// In the simple protocol, autocommit only when this is the last statement
			// in the batch. This matches the Postgres behavior. See
			// "Multiple Statements in a Single Query" at
			// https://www.postgresql.org/docs/14/protocol-flow.html.
			// The behavior is configurable, in case users want to preserve the
			// behavior from v21.2 and earlier.
			implicitTxnForBatch := ex.sessionData().EnableImplicitTransactionForBatchStatements
			canAutoCommit := ex.implicitTxn() &&
				(tcmd.LastInBatchBeforeShowCommitTimestamp ||
					tcmd.LastInBatch || !implicitTxnForBatch)
			ev, payload, err = ex.execStmt(
				ctx, tcmd.Statement, nil /* portal */, nil /* pinfo */, stmtRes, canAutoCommit,
			)

			return err
		}()
		// Note: we write to ex.statsCollector.PhaseTimes, instead of ex.phaseTimes,
		// because:
		// - stats use ex.statsCollector, not ex.phaseTimes.
		// - ex.statsCollector merely contains a copy of the times, that
		//   was created when the statement started executing (via the
		//   Reset() method).
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionQueryServiced, crtime.NowMono())
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
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartParse, 0)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndParse, 0)
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
			if copyTo, ok := ex.curStmtAST.(*tree.CopyTo); ok {
				// The execution uses the same logic as if it were a simple query. This
				// is because we no-op preparing of a COPY statement, in order to match
				// Postgres behavior. Since there is no plan, we use the connExecutor
				// to execute a prepared COPY.
				copyCmd := CopyOut{
					ParsedStmt:   portal.Stmt.Statement,
					Stmt:         copyTo,
					TimeReceived: tcmd.TimeReceived,
				}
				copyRes := ex.clientComm.CreateCopyOutResult(copyCmd, pos)
				res = copyRes
				ev, payload = ex.execCopyOut(ctx, copyCmd, copyRes)
				return nil
			}

			pinfo := &tree.PlaceholderInfo{
				PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
					TypeHints: portal.Stmt.TypeHints,
					Types:     portal.Stmt.Types,
				},
				Values: portal.Qargs,
			}

			if tcmd.Limit != 0 && tree.ReturnsAtMostOneRow(portal.Stmt.AST) {
				// When a statement returns at most one row, the result row
				// limit doesn't matter. We set it to 0 to fetch all rows, which
				// allows us to clean up resources sooner if using a pausable
				// portal.
				tcmd.Limit = 0
			}

			// If this is the first-time execution of a portal without a limit set,
			// it means all rows will be exhausted, so no need to pause this portal.
			if tcmd.Limit == 0 && portal.pauseInfo != nil && portal.pauseInfo.curRes == nil {
				portal.pauseInfo = nil
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
				portal.portalPausablity,
			)
			if portal.pauseInfo != nil {
				portal.pauseInfo.curRes = stmtRes
			}
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
		// TODO(sql-sessions): fix the phase time for pausable portals.
		// https://github.com/cockroachdb/cockroach/issues/99410
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionQueryServiced, crtime.NowMono())
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
		ev, payload = ex.execPrepare(ctx, tcmd)
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
			// If we're draining, then after handing the Sync connExecutor state
			// transition, check whether this is a good time to finish the
			// connection. If we're not inside a transaction, we stop processing
			// now. If we are inside a transaction, we'll check again the next time
			// a Sync is processed.
			defer func() {
				if ex.idleConn() {
					retErr = errDrainingComplete
				}
			}()
		}
	case CopyIn:
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionQueryReceived, tcmd.TimeReceived)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartParse, tcmd.ParseStart)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndParse, tcmd.ParseEnd)

		copyRes := ex.clientComm.CreateCopyInResult(tcmd, pos)
		res = copyRes
		ev, payload = ex.execCopyIn(ctx, tcmd, copyRes)

		// Note: we write to ex.statsCollector.phaseTimes, instead of ex.phaseTimes,
		// because:
		// - stats use ex.statsCollector, not ex.phasetimes.
		// - ex.statsCollector merely contains a copy of the times, that
		//   was created when the statement started executing (via the
		//   reset() method).
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionQueryServiced, crtime.NowMono())
	case CopyOut:
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionQueryReceived, tcmd.TimeReceived)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartParse, tcmd.ParseStart)
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndParse, tcmd.ParseEnd)
		copyRes := ex.clientComm.CreateCopyOutResult(tcmd, pos)
		res = copyRes
		ev, payload = ex.execCopyOut(ctx, tcmd, copyRes)

		// Note: we write to ex.statsCollector.phaseTimes, instead of ex.phaseTimes,
		// because:
		// - stats use ex.statsCollector, not ex.phasetimes.
		// - ex.statsCollector merely contains a copy of the times, that
		//   was created when the statement started executing (via the
		//   reset() method).
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionQueryServiced, crtime.NowMono())
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

	// We close all pausable portals and cursors when we encounter err payload,
	// otherwise there will be leftover bytes.
	shouldClosePausablePortalsAndCursors := func(payload fsm.EventPayload) bool {
		switch payload.(type) {
		case eventNonRetriableErrPayload, eventRetriableErrPayload:
			return true
		default:
			return false
		}
	}

	// If an event was generated, feed it to the state machine.
	if ev != nil {
		var err error
		if shouldClosePausablePortalsAndCursors(payload) {
			// We need this as otherwise, there'll be leftover bytes when
			// txnState.finishSQLTxn() is being called, as the underlying resources of
			// pausable portals hasn't been cleared yet.
			ex.extraTxnState.prepStmtsNamespace.closeAllPausablePortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)
			if err := ex.extraTxnState.sqlCursors.closeAll(&ex.planner, cursorCloseForTxnRollback); err != nil {
				log.Warningf(ctx, "error closing cursors: %v", err)
			}
		}
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
		// For a pausable portal, we don't log the affected rows until we close the
		// portal. However, we update the result for each execution. Thus, we need
		// to accumulate the number of affected rows before closing the result.
		if tcmd, ok := cmd.(*ExecPortal); ok {
			if portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[tcmd.Name]; ok {
				if portal.pauseInfo != nil {
					portal.pauseInfo.dispatchToExecutionEngine.rowsAffected += res.(RestrictedCommandResult).RowsAffected()
				}
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
		requireSyncFromClient := cmd.isExtendedProtocolCmd()
		if err := ex.stmtBuf.seekToNextBatch(requireSyncFromClient); err != nil {
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

	// Special handling for COMMIT/ROLLBACK in PL/pgSQL stored procedures. We
	// unconditionally reset the StoredProcTxnOp because it has either
	// successfully set in motion the commit/rollback of the current transaction,
	// or execution failed and the PL/pgSQL command should be ignored.
	ex.extraTxnState.storedProcTxnState.txnOp = tree.StoredProcTxnNoOp
	switch advInfo.code {
	case stayInPlace, rewind:
		// Do not reset the "resume" plan. This will allow a sub-transaction
		// within a stored procedure to be retried individually from any previous or
		// following transactions within the stored procedure.
		//
		// NOTE: we will eventually reach the default case below when the retries
		// terminate.
	default:
		// Finish cleaning up the stored proc state by resetting the "resume" plan
		// and transaction modes. The stored procedure has finished execution,
		// either successfully or with an error.
		ex.extraTxnState.storedProcTxnState.resumeProc = nil
		ex.extraTxnState.storedProcTxnState.txnModes = nil
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
	if advInfo.txnEvent.eventType == txnStart ||
		advInfo.txnEvent.eventType == txnRestart {
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
			case CopyOut:
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
	if pos < ex.extraTxnState.txnRewindPos {
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

func (ex *connExecutor) execCopyOut(
	ctx context.Context, cmd CopyOut, res CopyOutResult,
) (retEv fsm.Event, retPayload fsm.EventPayload) {
	// First handle connExecutor state transitions.
	if _, isNoTxn := ex.machine.CurState().(stateNoTxn); isNoTxn {
		return ex.beginImplicitTxn(ctx, cmd.ParsedStmt.AST, ex.copyQualityOfService())
	} else if _, isAbortedTxn := ex.machine.CurState().(stateAborted); isAbortedTxn {
		return ex.makeErrEvent(sqlerrors.NewTransactionAbortedError("" /* customMsg */), cmd.ParsedStmt.AST)
	}

	ex.incrementStartedStmtCounter(cmd.Stmt)
	var numOutputRows int
	var cancelQuery context.CancelFunc
	ctx, cancelQuery = ctxlog.WithCancel(ctx)
	queryID := ex.server.cfg.GenerateID()
	ex.addActiveQuery(cmd.ParsedStmt, nil /* placeholders */, queryID, cancelQuery)
	ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1)

	defer func() {
		ex.removeActiveQuery(queryID, cmd.Stmt)
		cancelQuery()
		ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1)
		if !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(cmd.Stmt)
		}
		var copyErr error
		if p, ok := retPayload.(payloadWithError); ok {
			copyErr = p.errorCause()
			log.SqlExec.Errorf(ctx, "error executing %s: %+v", cmd, copyErr)
		}

		// Log the query for sampling.
		ex.planner.maybeLogStatement(
			ctx,
			ex.executorType,
			int(ex.state.mu.autoRetryCounter),
			int(ex.extraTxnState.txnCounter.Load()),
			numOutputRows,
			ex.state.mu.stmtCount,
			0, /* bulkJobId */
			copyErr,
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
			ex.implicitTxn(),
			ex.statsCollector,
			ex.extraTxnState.shouldLogToTelemetry)
	}()

	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	ex.resetPlanner(ctx, &ex.planner, ex.state.mu.txn, stmtTS)
	ex.setCopyLoggingFields(cmd.ParsedStmt)

	var queryTimeoutTicker *time.Timer
	var txnTimeoutTicker *time.Timer
	queryTimedOut := false
	txnTimedOut := false

	// queryDoneAfterFunc and txnDoneAfterFunc will be allocated only when
	// queryTimeoutTicker or txnTimeoutTicker is non-nil.
	var queryDoneAfterFunc chan struct{}
	var txnDoneAfterFunc chan struct{}

	defer func(ctx context.Context) {
		if queryTimeoutTicker != nil {
			if !queryTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-queryDoneAfterFunc
			}
		}
		if txnTimeoutTicker != nil {
			if !txnTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// txnTimedOut.
				<-txnDoneAfterFunc
			}
		}

		// Detect context cancelation and overwrite whatever error might have been
		// set on the result before. The idea is that once the query's context is
		// canceled, all sorts of actors can detect the cancelation and set all
		// sorts of errors on the result. Rather than trying to impose discipline
		// in that jungle, we just overwrite them all here with an error that's
		// nicer to look at for the client.
		if ctx.Err() != nil {
			// Even in the cases where the error is a retryable error, we want to
			// intercept the event and payload returned here to ensure that the query
			// is not retried.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(false),
			}
			retPayload = eventNonRetriableErrPayload{err: cancelchecker.QueryCanceledError}
		}

		// If the query timed out, we intercept the error, payload, and event here
		// for the same reasons we intercept them for canceled queries above.
		// Overriding queries with a QueryTimedOut error needs to happen after
		// we've checked for canceled queries as some queries may be canceled
		// because of a timeout, in which case the appropriate error to return to
		// the client is one that indicates the timeout, rather than the more general
		// query canceled error. It's important to note that a timed out query may
		// not have been canceled (eg. We never even start executing a query
		// because the timeout has already expired), and therefore this check needs
		// to happen outside the canceled query check above.
		if queryTimedOut {
			// A timed out query should never produce retryable errors/events/payloads
			// so we intercept and overwrite them all here.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(false),
			}
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.QueryTimeoutError}
		} else if txnTimedOut {
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(false),
			}
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.TxnTimeoutError}
		}
	}(ctx)

	if ex.sessionData().StmtTimeout > 0 {
		timerDuration :=
			ex.sessionData().StmtTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
		// There's no need to proceed with execution if the timer has already expired.
		if timerDuration < 0 {
			queryTimedOut = true
			return ex.makeErrEvent(sqlerrors.QueryTimeoutError, cmd.Stmt)
		}
		queryDoneAfterFunc = make(chan struct{}, 1)
		queryTimeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				cancelQuery()
				queryTimedOut = true
				queryDoneAfterFunc <- struct{}{}
			})
	}
	if ex.sessionData().TransactionTimeout > 0 && !ex.implicitTxn() {
		timerDuration :=
			ex.sessionData().TransactionTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted).Elapsed()

		// If the timer already expired, but the transaction is not yet aborted,
		// we should error immediately without executing. If the timer
		// expired but the transaction already is aborted, then we should still
		// proceed with executing the statement in order to get a
		// TransactionAbortedError.
		_, txnAborted := ex.machine.CurState().(stateAborted)

		if timerDuration < 0 && !txnAborted {
			txnTimedOut = true
			return ex.makeErrEvent(sqlerrors.TxnTimeoutError, cmd.Stmt)
		}

		if timerDuration > 0 {
			txnDoneAfterFunc = make(chan struct{}, 1)
			txnTimeoutTicker = time.AfterFunc(
				timerDuration,
				func() {
					cancelQuery()
					txnTimedOut = true
					txnDoneAfterFunc <- struct{}{}
				})
		}
	}

	if copyErr := ex.execWithProfiling(ctx, cmd.Stmt, nil, func(ctx context.Context) error {
		if err := func() error {
			ex.mu.Lock()
			defer ex.mu.Unlock()
			queryMeta, ok := ex.mu.ActiveQueries[queryID]
			if !ok {
				return errors.AssertionFailedf("query %d not in registry", queryID)
			}
			queryMeta.phase = executing
			return nil
		}(); err != nil {
			return err
		}

		// We'll always have a txn on the planner since we called resetPlanner
		// above.
		txn := ex.planner.Txn()
		var err error
		if numOutputRows, err = runCopyTo(ctx, &ex.planner, txn, cmd, res); err != nil {
			return err
		}

		return nil
	}); copyErr != nil {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: copyErr}
		return ev, payload
	}
	return nil, nil
}

func (ex *connExecutor) setCopyLoggingFields(stmt statements.Statement[tree.Statement]) {
	// These fields need to be set for logging purposes.
	ex.planner.stmt = Statement{
		Statement: stmt,
	}
	ann := tree.MakeAnnotations(stmt.NumAnnotations)
	ex.planner.extendedEvalCtx.Context.Annotations = &ann
	ex.planner.extendedEvalCtx.Context.Placeholders = &tree.PlaceholderInfo{}
	ex.planner.curPlan.init(&ex.planner.stmt, &ex.planner.instrumentation)
}

// We handle the CopyFrom statement by creating a copyMachine and handing it
// control over the connection until the copying is done. The contract is that,
// when this is called, the pgwire.conn is not reading from the network
// connection any more until this returns. The copyMachine will do the reading
// and writing up to the CommandComplete message.
func (ex *connExecutor) execCopyIn(
	ctx context.Context, cmd CopyIn, res CopyInResult,
) (retEv fsm.Event, retPayload fsm.EventPayload) {
	// First handle connExecutor state transitions.
	if _, isNoTxn := ex.machine.CurState().(stateNoTxn); isNoTxn {
		return ex.beginImplicitTxn(ctx, cmd.ParsedStmt.AST, ex.copyQualityOfService())
	} else if _, isAbortedTxn := ex.machine.CurState().(stateAborted); isAbortedTxn {
		return ex.makeErrEvent(sqlerrors.NewTransactionAbortedError("" /* customMsg */), cmd.ParsedStmt.AST)
	}

	ex.incrementStartedStmtCounter(cmd.Stmt)
	var cancelQuery context.CancelFunc
	ctx, cancelQuery = ctxlog.WithCancel(ctx)
	queryID := ex.server.cfg.GenerateID()
	ex.addActiveQuery(cmd.ParsedStmt, nil /* placeholders */, queryID, cancelQuery)
	ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1)

	defer func() {
		ex.removeActiveQuery(queryID, cmd.Stmt)
		cancelQuery()
		ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1)
		if !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(cmd.Stmt)
		}
		if p, ok := retPayload.(payloadWithError); ok {
			log.SqlExec.Errorf(ctx, "error executing %s: %+v", cmd, p.errorCause())
		}
	}()

	// When we're done, unblock the network connection.
	defer func() {
		// We've seen cases where this deferred function is executed multiple
		// times for the same CopyIn command (#112095), so we protect the wait
		// group to be decremented exactly once via sync.Once.
		cmd.CopyDone.Once.Do(cmd.CopyDone.WaitGroup.Done)
	}()

	// The connExecutor state machine has already set us up with a txn at this
	// point.
	//
	// Disable the buffered writes for COPY since there is no benefit in this
	// ability here.
	ex.state.mu.txn.SetBufferedWritesEnabled(false /* enabled */)
	txnOpt := copyTxnOpt{
		txn:           ex.state.mu.txn,
		txnTimestamp:  ex.state.sqlTimestamp,
		stmtTimestamp: ex.server.cfg.Clock.PhysicalTime(),
		initPlanner:   ex.initPlanner,
		resetPlanner: func(ctx context.Context, p *planner, txn *kv.Txn, txnTS time.Time, stmtTS time.Time) {
			ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
			ex.resetPlanner(ctx, p, txn, stmtTS)
			ex.setCopyLoggingFields(cmd.ParsedStmt)
		},
	}

	if ex.implicitTxn() && !ex.planner.SessionData().CopyWritePipeliningEnabled {
		if err := txnOpt.txn.DisablePipelining(); err != nil {
			return ex.makeErrEvent(err, cmd.ParsedStmt.AST)
		}
	}

	ex.setCopyLoggingFields(cmd.ParsedStmt)

	var cm copyMachineInterface
	// Log the query for sampling.
	defer func() {
		var copyErr error
		if p, ok := retPayload.(payloadWithError); ok {
			copyErr = p.errorCause()
		}
		var numInsertedRows int
		if cm != nil {
			numInsertedRows = cm.numInsertedRows()
			res.SetRowsAffected(ctx, numInsertedRows)
		}
		ex.planner.maybeLogStatement(ctx, ex.executorType,
			int(ex.state.mu.autoRetryCounter), int(ex.extraTxnState.txnCounter.Load()),
			numInsertedRows, ex.state.mu.stmtCount,
			0, /* bulkJobId */
			copyErr,
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
			ex.implicitTxn(),
			ex.statsCollector,
			ex.extraTxnState.shouldLogToTelemetry)
	}()

	var copyErr error
	if isCopyToExternalStorage(cmd) {
		cm, copyErr = newFileUploadMachine(ctx, cmd.Conn, cmd.Stmt, txnOpt, &ex.planner, ex.state.mon)
	} else {
		// The planner will be prepared before use.
		p := ex.planner
		cm, copyErr = newCopyMachine(
			ctx, cmd.Conn, cmd.Stmt, &p, txnOpt, ex.state.mon, ex.implicitTxn(),
			// execInsertPlan
			func(ctx context.Context, p *planner, res RestrictedCommandResult) error {
				defer p.curPlan.close(ctx)
				_, err := ex.execWithDistSQLEngine(
					ctx, p, tree.RowsAffected, res, LocalDistribution,
					nil /* progressAtomic */, nil, /* distSQLProhibitedErr */
				)
				return err
			},
		)
	}
	if copyErr != nil {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: copyErr}
		return ev, payload
	}

	var queryTimeoutTicker *time.Timer
	var txnTimeoutTicker *time.Timer
	queryTimedOut := false
	txnTimedOut := false

	// queryDoneAfterFunc and txnDoneAfterFunc will be allocated only when
	// queryTimeoutTicker or txnTimeoutTicker is non-nil.
	var queryDoneAfterFunc chan struct{}
	var txnDoneAfterFunc chan struct{}

	defer func(ctx context.Context) {
		if queryTimeoutTicker != nil {
			if !queryTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-queryDoneAfterFunc
			}
		}
		if txnTimeoutTicker != nil {
			if !txnTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// txnTimedOut.
				<-txnDoneAfterFunc
			}
		}

		// Detect context cancelation and overwrite whatever error might have been
		// set on the result before. The idea is that once the query's context is
		// canceled, all sorts of actors can detect the cancelation and set all
		// sorts of errors on the result. Rather than trying to impose discipline
		// in that jungle, we just overwrite them all here with an error that's
		// nicer to look at for the client.
		if ctx.Err() != nil {
			// Even in the cases where the error is a retryable error, we want to
			// intercept the event and payload returned here to ensure that the query
			// is not retried.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(false),
			}
			retPayload = eventNonRetriableErrPayload{err: cancelchecker.QueryCanceledError}
		}

		cm.Close(ctx)

		// If the query timed out, we intercept the error, payload, and event here
		// for the same reasons we intercept them for canceled queries above.
		// Overriding queries with a QueryTimedOut error needs to happen after
		// we've checked for canceled queries as some queries may be canceled
		// because of a timeout, in which case the appropriate error to return to
		// the client is one that indicates the timeout, rather than the more general
		// query canceled error. It's important to note that a timed out query may
		// not have been canceled (eg. We never even start executing a query
		// because the timeout has already expired), and therefore this check needs
		// to happen outside the canceled query check above.
		if queryTimedOut {
			// A timed out query should never produce retryable errors/events/payloads
			// so we intercept and overwrite them all here.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(false),
			}
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.QueryTimeoutError}
		} else if txnTimedOut {
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(false),
			}
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.TxnTimeoutError}
		}
	}(ctx)

	if ex.sessionData().StmtTimeout > 0 {
		timerDuration :=
			ex.sessionData().StmtTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
		// There's no need to proceed with execution if the timer has already expired.
		if timerDuration < 0 {
			queryTimedOut = true
			return ex.makeErrEvent(sqlerrors.QueryTimeoutError, cmd.Stmt)
		}
		queryDoneAfterFunc = make(chan struct{}, 1)
		queryTimeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				cancelQuery()
				queryTimedOut = true
				queryDoneAfterFunc <- struct{}{}
			})
	}
	if ex.sessionData().TransactionTimeout > 0 && !ex.implicitTxn() {
		timerDuration :=
			ex.sessionData().TransactionTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted).Elapsed()

		// If the timer already expired, but the transaction is not yet aborted,
		// we should error immediately without executing. If the timer
		// expired but the transaction already is aborted, then we should still
		// proceed with executing the statement in order to get a
		// TransactionAbortedError.
		_, txnAborted := ex.machine.CurState().(stateAborted)

		if timerDuration < 0 && !txnAborted {
			txnTimedOut = true
			return ex.makeErrEvent(sqlerrors.TxnTimeoutError, cmd.Stmt)
		}

		if timerDuration > 0 {
			txnDoneAfterFunc = make(chan struct{}, 1)
			txnTimeoutTicker = time.AfterFunc(
				timerDuration,
				func() {
					cancelQuery()
					txnTimedOut = true
					txnDoneAfterFunc <- struct{}{}
				})
		}
	}

	if copyErr = ex.execWithProfiling(ctx, cmd.Stmt, nil, func(ctx context.Context) error {
		if err := func() error {
			ex.mu.Lock()
			defer ex.mu.Unlock()
			queryMeta, ok := ex.mu.ActiveQueries[queryID]
			if !ok {
				return errors.AssertionFailedf("query %d not in registry", queryID)
			}
			queryMeta.phase = executing
			return nil
		}(); err != nil {
			return err
		}
		return cm.run(ctx)
	}); copyErr != nil {
		// TODO(andrei): We don't have a full retriable error story for the copy machine.
		// When running outside of a txn, the copyMachine should probably do retries
		// internally - this is partially done, see `copyMachine.insertRows`.
		// When not, it's unclear what we should do. For now, we abort
		// the txn (if any).
		// We also don't have a story for distinguishing communication errors (which
		// should terminate the connection) from query errors. For now, we treat all
		// errors as query errors.
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: copyErr}
		return ev, payload
	}
	return nil, nil
}

// stmtHasNoData returns true if describing a result of the input statement
// type should return NoData.
func stmtHasNoData(stmt tree.Statement) bool {
	return stmt == nil || stmt.StatementReturnType() != tree.Rows
}

// commitPrepStmtNamespace deallocates everything in
// prepStmtsNamespaceAtTxnRewindPos that's not part of prepStmtsNamespace.
func (ex *connExecutor) commitPrepStmtNamespace(ctx context.Context) error {
	return ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(
		ctx, &ex.extraTxnState.prepStmtsNamespace, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
}

// commitPrepStmtNamespace deallocates everything in prepStmtsNamespace that's
// not part of prepStmtsNamespaceAtTxnRewindPos.
func (ex *connExecutor) rewindPrepStmtNamespace(ctx context.Context) error {
	return ex.extraTxnState.prepStmtsNamespace.resetTo(
		ctx, &ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
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

// errIsRetriable is true if the error is a client-visible retry error
// or the error is a special error that is handled internally and retried.
func errIsRetriable(err error) bool {
	return errors.HasInterface(err, (*pgerror.ClientVisibleRetryError)(nil)) ||
		errors.Is(err, retriableMinTimestampBoundUnsatisfiableError) ||
		descs.IsTwoVersionInvariantViolationError(err)
}

// convertRetriableErrorIntoUserVisibleError converts internal retriable
// errors into external, so that the client goes and retries this
// transaction. One example of this is two version invariant errors, which
// happens when a schema change is waiting for a schema change transition to
// propagate. When this happens, we either need to retry externally or internally,
// depending on if we are in an explicit transaction.
func (ex *connExecutor) convertRetriableErrorIntoUserVisibleError(
	ctx context.Context, origErr error,
) (modifiedErr error, err error) {
	if descs.IsTwoVersionInvariantViolationError(origErr) {
		if resetErr := ex.resetTransactionOnSchemaChangeRetry(ctx); resetErr != nil {
			return nil, resetErr
		}
		// Generating a forced retry error here, right after resetting the
		// transaction is not exactly necessary, but it's a sound way to
		// generate the only type of ClientVisibleRetryError we have.
		return ex.state.mu.txn.GenerateForcedRetryableErr(ctx, redact.Sprint(origErr)), nil
	}
	// Return the original error, this error will not be surfaced to the user.
	return origErr, nil
}

// makeErrEvent takes an error and returns either an eventRetriableErr or an
// eventNonRetriableErr, depending on the error type.
func (ex *connExecutor) makeErrEvent(err error, stmt tree.Statement) (fsm.Event, fsm.EventPayload) {
	// Check for MinTimestampBoundUnsatisfiableError errors.
	// If this is detected, it means we are potentially able to retry with a lower
	// MaxTimestampBound set if our MinTimestampBound was bumped up from the
	// original AS OF SYSTEM TIME timestamp set due to a schema bumping the
	// timestamp to a higher value.
	if minTSErr := (*kvpb.MinTimestampBoundUnsatisfiableError)(nil); errors.As(err, &minTSErr) {
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
	if retriable && execinfra.IsDynamicQueryHasNoHomeRegionError(err) {
		// Retry only # of remote regions times if the retry is due to the
		// enforce_home_region setting.
		retriable = int(ex.state.mu.autoRetryCounter) < len(ex.planner.EvalContext().RemoteRegions)
		if !retriable {
			err = execinfra.MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(err)
		}
	}
	if retriable {
		var rc rewindCapability
		var canAutoRetry bool
		if ex.implicitTxn() || !ex.sessionData().InjectRetryErrorsEnabled {
			rc, canAutoRetry = ex.getRewindTxnCapability()
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
	if modes.Isolation != tree.UnspecifiedIsolation {
		level := ex.txnIsolationLevelToKV(ctx, modes.Isolation)
		if err := ex.state.setIsolationLevel(level); err != nil {
			return pgerror.WithCandidateCode(err, pgcode.ActiveSQLTransaction)
		}
	}
	rwMode := modes.ReadWriteMode
	if modes.AsOf.Expr != nil && asOfTs.IsEmpty() {
		return errors.AssertionFailedf("expected an evaluated AS OF timestamp")
	}
	if !asOfTs.IsEmpty() {
		if err := ex.state.checkReadsAndWrites(); err != nil {
			return err
		}
		if err := ex.state.setHistoricalTimestamp(ctx, asOfTs); err != nil {
			return err
		}
		if rwMode == tree.UnspecifiedReadWriteMode {
			rwMode = tree.ReadOnly
		}
	}
	return ex.state.setReadOnlyMode(rwMode)
}

var allowReadCommittedIsolation = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.txn.read_committed_isolation.enabled",
	"set to true to allow transactions to use the READ COMMITTED isolation "+
		"level if specified by BEGIN/SET commands",
	true,
	settings.WithPublic,
)

var allowRepeatableReadIsolation = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.txn.snapshot_isolation.enabled",
	"set to true to allow transactions to use the REPEATABLE READ isolation "+
		"level if specified by BEGIN/SET commands",
	false,
	settings.WithName("sql.txn.repeatable_read_isolation.enabled"),
	settings.WithPublic,
)

var logIsolationLevelLimiter = log.Every(10 * time.Second)

// Bitmask for enabling various query fingerprint formatting styles.
// We don't publish this setting because most users should not need
// to tweak the fingerprint generation.
var queryFormattingForFingerprintsMask = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.statement_fingerprint.format_mask",
	"enables setting additional fmt flags for statement fingerprint formatting. "+
		"Flags set here will be applied in addition to FmtHideConstants",
	int64(tree.FmtCollapseLists|tree.FmtConstantsAsUnderscores),
	settings.WithValidateInt(func(i int64) error {
		if i == 0 || int64(tree.FmtCollapseLists|tree.FmtConstantsAsUnderscores)&i == i {
			return nil
		}
		return errors.Newf("invalid value %d", i)
	}),
)

func (ex *connExecutor) txnIsolationLevelToKV(
	ctx context.Context, level tree.IsolationLevel,
) isolation.Level {
	if level == tree.UnspecifiedIsolation {
		level = tree.IsolationLevel(ex.sessionData().DefaultTxnIsolationLevel)
	}
	originalLevel := level
	allowReadCommitted := allowReadCommittedIsolation.Get(&ex.server.cfg.Settings.SV)
	allowRepeatableRead := allowRepeatableReadIsolation.Get(&ex.server.cfg.Settings.SV)
	hasLicense := base.CCLDistributionAndEnterpriseEnabled(ex.server.cfg.Settings)
	level, upgraded, upgradedDueToLicense := level.UpgradeToEnabledLevel(
		allowReadCommitted, allowRepeatableRead, hasLicense)
	if f := ex.dataMutatorIterator.upgradedIsolationLevel; upgraded && f != nil {
		f(ctx, originalLevel, upgradedDueToLicense)
	}

	ret := level.ToKVIsoLevel()
	if ret != isolation.Serializable {
		telemetry.Inc(sqltelemetry.IsolationLevelCounter(ctx, ret))
	}
	return ret
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

// copyQualityOfService returns the QoSLevel session setting for COPY if the
// session settings are populated, otherwise the background QoSLevel.
func (ex *connExecutor) copyQualityOfService() sessiondatapb.QoSLevel {
	// TODO(yuzefovich): investigate whether we need this check here and above.
	if ex.sessionData() == nil {
		return sessiondatapb.UserLow
	}
	return ex.sessionData().CopyTxnQualityOfService
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
	Func: tree.WrapFunction(asof.FollowerReadTimestampFunctionName),
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

// omitInRangefeeds returns a bool representing whether the KV writes
// originating from this session should be omitted in rangefeeds.
// The primary use case for this function is to set the OmitInRangefeeds
// transaction attribute on roachpb.Transaction.
//
// Currently, the only way to exclude writes from rangefeeds is through
// the CDC session-based filtering feature, which is configured with the
// disable_changefeed_replication session variable.
func (ex *connExecutor) omitInRangefeeds() bool {
	if ex.sessionData() == nil {
		return false
	}
	return ex.sessionData().DisableChangefeedReplication
}

func (ex *connExecutor) bufferedWritesEnabled(ctx context.Context) bool {
	if ex.sessionData() == nil {
		return false
	}
	return ex.sessionData().BufferedWritesEnabled && ex.server.cfg.Settings.Version.IsActive(ctx, clusterversion.V25_2)
}

// initEvalCtx initializes the fields of an extendedEvalContext that stay the
// same across multiple statements. resetEvalCtx must also be called before each
// statement, to reinitialize other fields.
func (ex *connExecutor) initEvalCtx(ctx context.Context, evalCtx *extendedEvalContext, p *planner) {
	*evalCtx = extendedEvalContext{
		Context: eval.Context{
			Planner:                        p,
			StreamManagerFactory:           p,
			PrivilegedAccessor:             p,
			SessionAccessor:                p,
			JobExecContext:                 p,
			ClientNoticeSender:             p,
			Sequence:                       p,
			Tenant:                         p,
			Regions:                        p,
			Gossip:                         p,
			PreparedStatementState:         &ex.extraTxnState.prepStmtsNamespace,
			SessionDataStack:               ex.sessionDataStack,
			ReCache:                        ex.server.reCache,
			ToCharFormatCache:              ex.server.toCharFormatCache,
			SQLStatsController:             ex.server.sqlStatsController,
			SchemaTelemetryController:      ex.server.schemaTelemetryController,
			IndexUsageStatsController:      ex.server.indexUsageStatsController,
			ConsistencyChecker:             p.execCfg.ConsistencyChecker,
			RangeProber:                    p.execCfg.RangeProber,
			StmtDiagnosticsRequestInserter: ex.server.cfg.StmtDiagnosticsRecorder.InsertRequest,
			CatalogBuiltins:                &p.evalCatalogBuiltins,
			QueryCancelKey:                 ex.queryCancelKey,
			DescIDGenerator:                ex.getDescIDGenerator(),
			RangeStatsFetcher:              p.execCfg.RangeStatsFetcher,
			JobsProfiler:                   p,
			RNGFactory:                     &ex.rng.external,
			ULIDEntropyFactory:             &ex.rng.ulidEntropy,
			CidrLookup:                     p.execCfg.CidrLookup,
		},
		Tracing:              &ex.sessionTracing,
		MemMetrics:           &ex.memMetrics,
		Descs:                ex.extraTxnState.descCollection,
		TxnModesSetter:       ex,
		jobs:                 ex.extraTxnState.jobs,
		validateDbZoneConfig: &ex.extraTxnState.validateDbZoneConfig,
		statsProvider:        ex.server.sqlStats,
		localStatsProvider:   ex.server.localSqlStats,
		indexUsageStats:      ex.indexUsageStats,
		statementPreparer:    ex,
	}
	evalCtx.copyFromExecCfg(ex.server.cfg)
}

// GetPCRReaderTimestamp if the system database is setup as PCR
// catalog reader, then this function will return an non-zero timestamp
// to use for all read operations.
func (ex *connExecutor) GetPCRReaderTimestamp() hlc.Timestamp {
	if ex.isPCRReaderCatalog && !ex.sessionData().BypassPCRReaderCatalogAOST {
		return ex.server.cfg.LeaseManager.GetSafeReplicationTS()
	}
	return hlc.Timestamp{}
}

// resetEvalCtx initializes the fields of evalCtx that can change
// during a session (i.e. the fields not set by initEvalCtx).
//
// stmtTS is the timestamp that the statement_timestamp() SQL builtin will
// return for statements executed with this evalCtx. Since generally each
// statement is supposed to have a different timestamp, the evalCtx generally
// shouldn't be reused across statements.
//
// Safe for concurrent use.
func (ex *connExecutor) resetEvalCtx(evalCtx *extendedEvalContext, txn *kv.Txn, stmtTS time.Time) {
	newTxn := txn == nil || evalCtx.Txn != txn
	evalCtx.TxnState = ex.getTransactionState()
	evalCtx.TxnReadOnly = ex.state.readOnly.Load()
	evalCtx.TxnImplicit = ex.implicitTxn()
	evalCtx.TxnIsSingleStmt = false
	func() {
		ex.state.mu.Lock()
		defer ex.state.mu.Unlock()
		evalCtx.TxnIsoLevel = ex.state.mu.isolationLevel
	}()
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
	evalCtx.Txn = txn
	evalCtx.PrepareOnly = false
	evalCtx.SkipNormalize = false
	evalCtx.SchemaChangerState = ex.extraTxnState.schemaChangerState
	evalCtx.DescIDGenerator = ex.getDescIDGenerator()

	// See resetPlanner for more context on setting the maximum timestamp for
	// AOST read retries.
	var minTSErr *kvpb.MinTimestampBoundUnsatisfiableError
	if err := ex.state.mu.autoRetryReason; err != nil && errors.As(err, &minTSErr) {
		evalCtx.AsOfSystemTime.MaxTimestampBound = ex.extraTxnState.descCollection.GetMaxTimestampBound()
	} else if newTxn {
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

// initPlanner initializes a planner so it can be used for planning a
// query in the context of this session.
func (ex *connExecutor) initPlanner(ctx context.Context, p *planner) {
	p.cancelChecker.Reset(ctx)

	ex.initEvalCtx(ctx, &p.extendedEvalCtx, p)

	p.sessionDataMutatorIterator = ex.dataMutatorIterator
	p.noticeSender = nil
	p.preparedStatements = ex.getPrepStmtsAccessor()
	p.sqlCursors = ex.getCursorAccessor()
	p.storedProcTxnState = ex.getStoredProcTxnStateAccessor()
	p.createdSequences = ex.getCreatedSequencesAccessor()

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)
	p.schemaResolver.sessionDataStack = p.EvalContext().SessionDataStack
	p.schemaResolver.descCollection = p.Descriptors()
	p.schemaResolver.authAccessor = p
	p.reducedAuditConfig = &auditlogging.ReducedAuditConfig{}
	p.datumAlloc = &tree.DatumAlloc{}
}

// maybeAdjustMaxTimestampBound checks
func (ex *connExecutor) maybeAdjustMaxTimestampBound(p *planner, txn *kv.Txn) {
	if autoRetryReason := ex.state.mu.autoRetryReason; autoRetryReason != nil {
		// If we are retrying due to an unsatisfiable timestamp bound which is
		// retriable, it means we were unable to serve the previous minimum
		// timestamp as there was a schema update in between. When retrying, we
		// want to keep the same minimum timestamp for the AOST read, but set
		// the maximum timestamp to the point just before our failed read to
		// ensure we don't try to read data which may be after the schema change
		// when we retry.
		var minTSErr *kvpb.MinTimestampBoundUnsatisfiableError
		if errors.As(autoRetryReason, &minTSErr) {
			nextMax := minTSErr.MinTimestampBound
			ex.extraTxnState.descCollection.SetMaxTimestampBound(nextMax)
			return
		}
	}
	// Otherwise, only change the historical timestamps if this is a new txn.
	// This is because resetPlanner can be called multiple times for the same
	// txn during the extended protocol.
	if newTxn := txn == nil || p.extendedEvalCtx.Txn != txn; newTxn {
		ex.extraTxnState.descCollection.ResetMaxTimestampBound()
	}
}

func (ex *connExecutor) resetPlanner(
	ctx context.Context, p *planner, txn *kv.Txn, stmtTS time.Time,
) {
	p.resetPlanner(ctx, txn, ex.sessionData(), ex.state.mon, ex.sessionMon)
	ex.maybeAdjustMaxTimestampBound(p, txn)
	// Make sure the default locality specifies the actual gateway region at the
	// start of query compilation. It could have been overridden to a remote
	// region when the enforce_home_region session setting is true.
	p.EvalContext().Locality = p.EvalContext().OriginalLocality
	if execinfra.IsDynamicQueryHasNoHomeRegionError(ex.state.mu.autoRetryReason) {
		if int(ex.state.mu.autoRetryCounter) <= len(p.EvalContext().RemoteRegions) {
			// Set a fake gateway region for use by the optimizer to inform its
			// decision on which region to access first in locality-optimized
			// scan and join operations. This setting does not affect the
			// distsql planner, and local plans will continue to be run from the
			// actual gateway region.
			p.EvalContext().Locality = p.EvalContext().Locality.CopyReplaceKeyValue(
				"region" /* key */, string(p.EvalContext().RemoteRegions[ex.state.mu.autoRetryCounter-1]),
			)
		}
	}
	ex.resetEvalCtx(&p.extendedEvalCtx, txn, stmtTS)
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

	err := func() error {
		ex.mu.Lock()
		defer ex.mu.Unlock()
		return ex.machine.ApplyWithPayload(ex.Ctx(), ev, payload)
	}()
	if err != nil {
		if errors.HasType(err, (*fsm.TransitionNotFoundError)(nil)) {
			panic(err)
		}
		return advanceInfo{}, err
	}

	advInfo := ex.state.consumeAdvanceInfo()

	var payloadErr error
	// If we had an error from DDL statement execution due to the presence of
	// other concurrent schema changes when attempting a schema change, wait for
	// the completion of those schema changes. The state machine transition above
	// (ApplyWithPayload) may have moved the state to NoTxn, so we also need to
	// check that the txn is open.
	if p, ok := payload.(payloadWithError); ok {
		payloadErr = p.errorCause()
		if _, isOpen := ex.machine.CurState().(stateOpen); isOpen {
			if descID := scerrors.ConcurrentSchemaChangeDescID(payloadErr); descID != descpb.InvalidID {
				if err := ex.handleWaitingForConcurrentSchemaChanges(ex.Ctx(), descID); err != nil {
					return advanceInfo{}, err
				}
			}
		}
	}

	// Handle transaction events which cause updates to txnState.
	switch advInfo.txnEvent.eventType {
	case noEvent, txnUpgradeToExplicit:
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
			if advInfo.txnEvent.eventType == txnUpgradeToExplicit {
				ex.extraTxnState.txnFinishClosure.implicit = false
			}
		}
	case txnStart:
		ex.recordTransactionStart(advInfo.txnEvent.txnID)

		// Session is considered active when executing a transaction.
		ex.totalActiveTimeStopWatch.Start()

		if err := ex.maybeSetSQLLivenessSessionAndGeneration(); err != nil {
			return advanceInfo{}, err
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
			sentryutil.SendReport(ex.Ctx(), &ex.server.cfg.Settings.SV, err)
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
					errors.IssueLink{IssueURL: build.MakeIssueURL(42061)})
				res.SetError(newErr)
			}
		}
		ex.notifyStatsRefresherOfNewTables(ex.Ctx())

		// If there is any descriptor has new version. We want to make sure there is
		// only one version of the descriptor in all nodes. In schema changer jobs,
		// `WaitForOneVersion` has been called for the descriptors included in jobs.
		// So we just need to do this for descriptors not in jobs.
		// We need to get descriptor IDs in jobs before jobs are run because we have
		// operations in declarative schema changer removing descriptor IDs from job
		// payload as it's done with the descriptors.
		descIDsInJobs, err := ex.descIDsInSchemaChangeJobs()
		if err != nil {
			return advanceInfo{}, err
		}
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionStartPostCommitJob, crtime.NowMono())
		if err := ex.waitForTxnJobs(); err != nil {
			handleErr(err)
		}
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.SessionEndPostCommitJob, crtime.NowMono())
		// If descriptors are either modified or created wait then we may have to
		// wait for one version (if no job exists) or the initial version to be
		// acquired.
		if ex.extraTxnState.descCollection.HasUncommittedDescriptors() {
			cachedRegions, err := regions.NewCachedDatabaseRegions(ex.Ctx(), ex.server.cfg.DB, ex.server.cfg.LeaseManager)
			if err != nil {
				return advanceInfo{}, err
			}
			if err := ex.waitOneVersionForNewVersionDescriptorsWithoutJobs(descIDsInJobs, cachedRegions); err != nil {
				return advanceInfo{}, err
			}
			if err := ex.waitForInitialVersionForNewDescriptors(cachedRegions); err != nil {
				return advanceInfo{}, err
			}
		}
		if ex.extraTxnState.descCollection.HasUncommittedNewOrDroppedDescriptors() {
			execCfg := ex.planner.ExecCfg()
			if err := UpdateDescriptorCount(ex.Ctx(), execCfg, execCfg.SchemaChangerMetrics); err != nil {
				log.Warningf(ex.Ctx(), "failed to scan descriptor table: %v", err)
			}
		}
		fallthrough
	case txnRollback, txnPrepare:
		ex.resetExtraTxnState(ex.Ctx(), advInfo.txnEvent, payloadErr)
		// Since we're finalizing the SQL transaction (commit, rollback, prepare),
		// there's no need to keep the prepared stmts for a txn rewind.
		ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.closeAllPortals(
			ex.Ctx(), &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.resetPlanner(ex.Ctx(), &ex.planner, nil, ex.server.cfg.Clock.PhysicalTime())
	case txnRestart:
		// In addition to resetting the extraTxnState, the restart event may
		// also need to reset the sqlliveness.Session.
		ex.resetExtraTxnState(ex.Ctx(), advInfo.txnEvent, payloadErr)
		if err := ex.maybeSetSQLLivenessSessionAndGeneration(); err != nil {
			return advanceInfo{}, err
		}
	default:
		return advanceInfo{}, errors.AssertionFailedf(
			"unexpected event: %v", errors.Safe(advInfo.txnEvent))
	}
	return advInfo, nil
}

// waitForTxnJobs waits for any jobs created inside this txn
// and respects the statement timeout for implicit transactions.
func (ex *connExecutor) waitForTxnJobs() error {
	var retErr error
	if len(ex.extraTxnState.jobs.created) == 0 {
		return nil
	}
	ex.mu.IdleInSessionTimeout.Stop()
	defer ex.startIdleInSessionTimeout()
	ex.server.cfg.JobRegistry.NotifyToResume(
		ex.ctxHolder.connCtx, ex.extraTxnState.jobs.created...,
	)
	// Set up a context for waiting for the jobs, which can be cancelled if
	// a statement timeout exists.
	jobWaitCtx := ex.ctxHolder.ctx()
	var queryTimedout atomic.Bool
	if ex.sessionData().StmtTimeout > 0 {
		timePassed := ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
		if timePassed > ex.sessionData().StmtTimeout {
			queryTimedout.Store(true)
		} else {
			var cancelFn context.CancelFunc
			jobWaitCtx, cancelFn = context.WithCancel(jobWaitCtx)
			queryTimeTicker := time.AfterFunc(ex.sessionData().StmtTimeout-timePassed, func() {
				cancelFn()
				queryTimedout.Store(true)
			})
			defer cancelFn()
			defer queryTimeTicker.Stop()
		}
	}
	if !queryTimedout.Load() && len(ex.extraTxnState.jobs.created) > 0 {
		if err := ex.server.cfg.JobRegistry.WaitForJobs(jobWaitCtx,
			ex.extraTxnState.jobs.created); err != nil {
			if errors.Is(err, context.Canceled) && queryTimedout.Load() {
				retErr = sqlerrors.QueryTimeoutError
				err = nil
			} else {
				return err
			}
		}
	}
	// If the query timed out indicate that there are jobs left behind.
	if queryTimedout.Load() {
		jobList := strings.Builder{}
		for i, j := range ex.extraTxnState.jobs.created {
			if i > 0 {
				jobList.WriteString(",")
			}
			jobList.WriteString(j.String())
		}
		if err := ex.planner.noticeSender.SendNotice(
			ex.ctxHolder.connCtx,
			pgnotice.Newf(
				"The statement has timed out, but the following "+
					"background jobs have been created and will continue running: %s.",
				jobList.String()),
			false, /* immediateFlush */
		); err != nil {
			return err
		}
	}
	return retErr
}

func (ex *connExecutor) maybeSetSQLLivenessSessionAndGeneration() error {
	if !ex.server.cfg.Codec.ForSystemTenant() ||
		ex.server.cfg.TestingKnobs.ForceSQLLivenessSession {
		// Update the leased descriptor collection with the current sqlliveness.Session.
		// This is required in the multi-tenant environment to update the transaction
		// deadline to either the session expiry or the leased descriptor deadline,
		// whichever is sooner. We need this to ensure that transactions initiated
		// by ephemeral SQL pods in multi-tenant environments are committed before the
		// session expires.
		session, err := ex.server.cfg.SQLLiveness.Session(ex.Ctx())
		if err != nil {
			return err
		}
		ex.extraTxnState.descCollection.SetSession(session)
	}
	// Reset the lease generation at the same time.
	ex.extraTxnState.descCollection.ResetLeaseGeneration()
	return nil
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
	for i, c := range cols {
		fmtCode, err := res.GetFormatCode(i)
		if err != nil {
			return err
		}
		if err = checkResultType(c.Typ, fmtCode); err != nil {
			return err
		}
	}
	// If the output mode has been modified by instrumentation (e.g. EXPLAIN
	// ANALYZE), then the columns will be set later.
	if ex.planner.instrumentation.outputMode == unmodifiedOutput &&
		ast.StatementReturnType() == tree.Rows {
		// Note that this call is necessary even if cols is nil.
		res.SetColumns(ctx, cols)
	}
	return nil
}

// hasQuery is part of the RegistrySession interface.
func (ex *connExecutor) hasQuery(queryID clusterunique.ID) bool {
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	_, exists := ex.mu.ActiveQueries[queryID]
	return exists
}

// CancelQuery is part of the RegistrySession interface.
func (ex *connExecutor) CancelQuery(queryID clusterunique.ID) bool {
	// RLock can be used because map deletion happens in
	// connExecutor.removeActiveQuery.
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	if queryMeta, exists := ex.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancelQuery()
		return true
	}
	return false
}

// CancelActiveQueries is part of the RegistrySession interface.
func (ex *connExecutor) CancelActiveQueries() bool {
	// RLock can be used because map deletion happens in
	// connExecutor.removeActiveQuery.
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	canceled := false
	for _, queryMeta := range ex.mu.ActiveQueries {
		queryMeta.cancelQuery()
		canceled = true
	}
	return canceled
}

// CancelSession is part of the RegistrySession interface.
func (ex *connExecutor) CancelSession() {
	if ex.onCancelSession == nil {
		return
	}
	// TODO(abhimadan): figure out how to send a nice error message to the client.
	ex.onCancelSession()
}

// SessionUser is part of the RegistrySession interface.
func (ex *connExecutor) SessionUser() username.SQLUsername {
	// SessionUser is the same for all elements in the stack so use Base()
	// to avoid needing a lock and race conditions.
	return ex.sessionDataStack.Base().SessionUser()
}

// serialize is part of the RegistrySession interface.
func (ex *connExecutor) serialize() serverpb.Session {
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	ex.state.mu.RLock()
	defer ex.state.mu.RUnlock()

	var activeTxnInfo *serverpb.TxnInfo
	txn := ex.state.mu.txn

	var autoRetryReasonStr string

	if ex.state.mu.autoRetryReason != nil {
		autoRetryReasonStr = ex.state.mu.autoRetryReason.Error()
	}

	timeNow := timeutil.Now()

	if txn != nil {
		id := txn.ID()
		elapsedTime := crtime.MonoFromTime(timeNow).Sub(ex.state.mu.txnStart)
		activeTxnInfo = &serverpb.TxnInfo{
			ID:                    id,
			Start:                 timeNow.Add(-elapsedTime),
			ElapsedTime:           elapsedTime,
			NumStatementsExecuted: int32(ex.state.mu.stmtCount),
			NumRetries:            int32(txn.Epoch()),
			NumAutoRetries:        ex.state.mu.autoRetryCounter,
			TxnDescription:        txn.String(),
			// TODO(yuzefovich): this seems like not a concurrency safe call.
			Implicit:            ex.implicitTxn(),
			AllocBytes:          ex.state.mon.AllocBytes(),
			MaxAllocBytes:       ex.state.mon.MaximumBytes(),
			IsHistorical:        ex.state.isHistorical.Load(),
			ReadOnly:            ex.state.readOnly.Load(),
			Priority:            ex.state.mu.priority.String(),
			QualityOfService:    sessiondatapb.ToQoSLevelString(txn.AdmissionHeader().Priority),
			LastAutoRetryReason: autoRetryReasonStr,
			IsolationLevel:      tree.FromKVIsoLevel(ex.state.mu.isolationLevel).String(),
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
		// Note: while it may seem tempting to just use query.stmt.AST instead of
		// re-parsing the original SQL, it's unfortunately NOT SAFE to do so because
		// the AST is currently not immutable - doing so will produce data races.
		// See issue https://github.com/cockroachdb/cockroach/issues/90965 for the
		// last time this was hit.
		// This can go away if we resolve https://github.com/cockroachdb/cockroach/issues/22847.
		parsed, err := parser.ParseOne(query.stmt.SQL)
		if err != nil {
			// This shouldn't happen, but might as well not completely give up if we
			// fail to parse a parseable sql for some reason. We unfortunately can't
			// just log the SQL either as it could contain sensitive information.
			log.Warningf(ex.Ctx(), "failed to re-parse sql during session "+
				"serialization")
			continue
		}
		sqlNoConstants := truncateSQL(formatStatementHideConstants(parsed.AST))
		nPlaceholders := 0
		if query.placeholders != nil {
			nPlaceholders = len(query.placeholders.Values)
		}
		placeholders := make([]string, nPlaceholders)
		for i := range placeholders {
			placeholders[i] = tree.AsStringWithFlags(query.placeholders.Values[i], tree.FmtSimple)
		}
		sql := truncateSQL(query.stmt.SQL)
		progress := math.Float64frombits(atomic.LoadUint64(&query.progressAtomic))
		elapsedTime := crtime.MonoFromTime(timeNow).Sub(query.start)
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			TxnID:          query.txnID,
			ID:             id.String(),
			Start:          timeNow.Add(-elapsedTime),
			ElapsedTime:    elapsedTime,
			Sql:            sql,
			SqlNoConstants: sqlNoConstants,
			SqlSummary:     formatStatementSummary(parsed.AST),
			Placeholders:   placeholders,
			IsDistributed:  query.isDistributed,
			Phase:          (serverpb.ActiveQuery_Phase)(query.phase),
			Progress:       float32(progress),
			IsFullScan:     query.isFullScan,
			PlanGist:       query.planGist,
			Database:       query.database,
		})
	}
	lastActiveQuery := ""
	lastActiveQueryNoConstants := ""
	if ex.mu.LastActiveQuery != nil {
		lastActiveQuery = truncateSQL(ex.mu.LastActiveQuery.String())
		lastActiveQueryNoConstants = truncateSQL(formatStatementHideConstants(ex.mu.LastActiveQuery))
	}
	status := serverpb.Session_IDLE
	if len(activeQueries) > 0 {
		status = serverpb.Session_ACTIVE
	}

	// We always use base here as the fields from the SessionData should always
	// be that of the root session.
	sd := ex.sessionDataStack.Base()

	remoteStr := "<admin>"
	if sd.RemoteAddr != nil {
		remoteStr = sd.RemoteAddr.String()
	}

	txnFingerprintIDs := ex.txnFingerprintIDCache.GetAllTxnFingerprintIDs()
	sessionActiveTime := ex.totalActiveTimeStopWatch.Elapsed()
	if startedAt, started := ex.totalActiveTimeStopWatch.LastStartedAt(); started {
		sessionActiveTime = time.Duration(sessionActiveTime.Nanoseconds() + timeutil.Since(startedAt).Nanoseconds())
	}

	return serverpb.Session{
		Username:          sd.SessionUser().Normalized(),
		ClientAddress:     remoteStr,
		ApplicationName:   ex.applicationName.Load().(string),
		Start:             ex.phaseTimes.InitTime(),
		ActiveQueries:     activeQueries,
		ActiveTxn:         activeTxnInfo,
		NumTxnsExecuted:   ex.extraTxnState.txnCounter.Load(),
		TxnFingerprintIDs: txnFingerprintIDs,
		LastActiveQuery:   lastActiveQuery,
		// TODO(yuzefovich): this seems like not a concurrency safe call.
		ID:                         ex.planner.extendedEvalCtx.SessionID.GetBytes(),
		AllocBytes:                 ex.mon.AllocBytes(),
		MaxAllocBytes:              ex.mon.MaximumBytes(),
		LastActiveQueryNoConstants: lastActiveQueryNoConstants,
		Status:                     status,
		TotalActiveTime:            sessionActiveTime,
		PGBackendPID:               ex.planner.extendedEvalCtx.QueryCancelKey.GetPGBackendPID(),
		TraceID:                    uint64(ex.planner.extendedEvalCtx.Tracing.connSpan.TraceID()),
		GoroutineID:                ex.ctxHolder.goroutineID,
		AuthenticationMethod:       sd.AuthenticationMethod,
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

func (ex *connExecutor) getStoredProcTxnStateAccessor() storedProcTxnStateAccessor {
	return storedProcTxnStateAccessor{
		ex: ex,
	}
}

func (ex *connExecutor) getCreatedSequencesAccessor() createdSequences {
	return connExCreatedSequencesAccessor{
		ex: ex,
	}
}

// sessionEventf logs a message to the session event log (if any).
func (ex *connExecutor) sessionEventf(ctx context.Context, format string, args ...interface{}) {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventfDepth(ctx, 1 /* depth */, 2 /* level */, format, args...)
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

func (ex *connExecutor) getDescIDGenerator() eval.DescIDGenerator {
	if ex.server.cfg.TestingKnobs.UseTransactionalDescIDGenerator &&
		ex.state.mu.txn != nil {
		return descidgen.NewTransactionalGenerator(
			ex.server.cfg.Settings, ex.server.cfg.Codec, ex.state.mu.txn,
		)
	}
	return ex.server.cfg.DescIDGenerator
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
	// CRUDQueryCount includes all 4 CRUD statements above.
	CRUDQueryCount telemetry.CounterWithMetric

	// Transaction operations.
	TxnBeginCount    telemetry.CounterWithMetric
	TxnCommitCount   telemetry.CounterWithMetric
	TxnRollbackCount telemetry.CounterWithMetric
	TxnUpgradedCount *metric.Counter

	// Transaction XA two-phase commit operations.
	TxnPrepareCount          telemetry.CounterWithMetric
	TxnCommitPreparedCount   telemetry.CounterWithMetric
	TxnRollbackPreparedCount telemetry.CounterWithMetric

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

	// CopyNonAtomicCount counts all non-atomic COPY statements (prior to 22.2
	// non-atomic was the default, in 22.2 COPY became atomic by default
	// but we want to know if there's customers using the
	// 'CopyFromAtomicEnabled' session variable to go back to non-atomic
	// COPYs.
	CopyNonAtomicCount telemetry.CounterWithMetric

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
		TxnUpgradedCount: metric.NewCounter(
			getMetricMeta(MetaTxnUpgradedFromWeakIsolation, internal)),
		TxnPrepareCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnPrepareStarted, internal)),
		TxnCommitPreparedCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnCommitPreparedStarted, internal)),
		TxnRollbackPreparedCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnRollbackPreparedStarted, internal)),
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
		CRUDQueryCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCRUDStarted, internal)),
		DdlCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDdlStarted, internal)),
		CopyCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCopyStarted, internal)),
		CopyNonAtomicCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCopyNonAtomicStarted, internal)),
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
		TxnUpgradedCount: metric.NewCounter(
			getMetricMeta(MetaTxnUpgradedFromWeakIsolation, internal)),
		TxnPrepareCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnPrepareExecuted, internal)),
		TxnCommitPreparedCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnCommitPreparedExecuted, internal)),
		TxnRollbackPreparedCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnRollbackPreparedExecuted, internal)),
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
		CRUDQueryCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCRUDExecuted, internal)),
		DdlCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDdlExecuted, internal)),
		CopyCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCopyExecuted, internal)),
		CopyNonAtomicCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaCopyNonAtomicExecuted, internal)),
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
		sc.CRUDQueryCount.Inc()
	case *tree.Update:
		sc.UpdateCount.Inc()
		sc.CRUDQueryCount.Inc()
	case *tree.Insert:
		sc.InsertCount.Inc()
		sc.CRUDQueryCount.Inc()
	case *tree.Delete:
		sc.DeleteCount.Inc()
		sc.CRUDQueryCount.Inc()
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
	case *tree.PrepareTransaction:
		sc.TxnPrepareCount.Inc()
	case *tree.CommitPrepared:
		sc.TxnCommitPreparedCount.Inc()
	case *tree.RollbackPrepared:
		sc.TxnRollbackPreparedCount.Inc()
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
		if !ex.sessionData().CopyFromAtomicEnabled {
			sc.CopyNonAtomicCount.Inc()
		}
	default:
		if stmt.StatementReturnType() == tree.DDL || stmt.StatementType() == tree.TypeDDL {
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
func (ps connExPrepStmtsAccessor) Get(name string, touchLRU bool) (*PreparedStatement, bool) {
	s, ok := ps.ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
	if ok && touchLRU {
		ps.ex.extraTxnState.prepStmtsNamespace.touchLRUEntry(name)
	}
	return s, ok
}

// Delete is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Delete(ctx context.Context, name string) bool {
	_, ok := ps.Get(name, false /* touchLRU */)
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

var contextPlanGistKey = ctxutil.RegisterFastValueKey()

func withPlanGist(ctx context.Context, gist string) context.Context {
	if gist == "" {
		return ctx
	}
	return ctxutil.WithFastValue(ctx, contextPlanGistKey, gist)
}

func planGistFromCtx(ctx context.Context) string {
	val := ctxutil.FastValue(ctx, contextPlanGistKey)
	if val != nil {
		return val.(string)
	}
	return ""
}

func init() {
	// Register a function to include the plan gist in crash reports.
	logcrash.RegisterTagFn("gist", func(ctx context.Context) string {
		return planGistFromCtx(ctx)
	})
}
