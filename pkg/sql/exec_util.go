// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// ClusterOrganization is the organization name.
var ClusterOrganization = settings.RegisterStringSetting(
	"cluster.organization",
	"organization name",
	"",
)

// ClusterSecret is a cluster specific secret. This setting is hidden.
var ClusterSecret = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"cluster.secret",
		"cluster specific secret",
		"",
	)
	s.SetConfidential()
	return s
}()

// defaultIntSize controls how a "naked" INT type will be parsed.
// TODO(bob): Change this to 4 in v2.3; https://github.com/cockroachdb/cockroach/issues/32534
// TODO(bob): Remove or n-op this in v2.4: https://github.com/cockroachdb/cockroach/issues/32844
var defaultIntSize = settings.RegisterValidatedIntSetting(
	"sql.defaults.default_int_size",
	"the size, in bytes, of an INT type", 8, func(i int64) error {
		if i != 4 && i != 8 {
			return errors.New("only 4 or 8 are valid values")
		}
		return nil
	})

// traceTxnThreshold can be used to log SQL transactions that take
// longer than duration to complete. For example, traceTxnThreshold=1s
// will log the trace for any transaction that takes 1s or longer. To
// log traces for all transactions use traceTxnThreshold=1ns. Note
// that any positive duration will enable tracing and will slow down
// all execution because traces are gathered for all transactions even
// if they are not output.
var traceTxnThreshold = settings.RegisterDurationSetting(
	"sql.trace.txn.enable_threshold",
	"duration beyond which all transactions are traced (set to 0 to disable)", 0,
)

// traceSessionEventLogEnabled can be used to enable the event log
// that is normally kept for every SQL connection. The event log has a
// non-trivial performance impact and also reveals SQL statements
// which may be a privacy concern.
var traceSessionEventLogEnabled = settings.RegisterBoolSetting(
	"sql.trace.session_eventlog.enabled",
	"set to true to enable session tracing", false,
)

// ReorderJoinsLimitClusterSettingName is the name of the cluster setting for
// the maximum number of joins to reorder.
const ReorderJoinsLimitClusterSettingName = "sql.defaults.reorder_joins_limit"

// ReorderJoinsLimitClusterValue controls the cluster default for the maximum
// number of joins reordered.
var ReorderJoinsLimitClusterValue = settings.RegisterValidatedIntSetting(
	ReorderJoinsLimitClusterSettingName,
	"default number of joins to reorder",
	opt.DefaultJoinOrderLimit,
	func(v int64) error {
		if v < 0 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"cannot set sql.defaults.reorder_joins_limit to a negative value: %d", v)
		}
		return nil
	},
)

var optDrivenFKClusterMode = settings.RegisterBoolSetting(
	"sql.defaults.experimental_optimizer_foreign_keys.enabled",
	"enables optimizer-driven foreign key checks by default",
	false,
)

// VectorizeClusterMode controls the cluster default for when automatic
// vectorization is enabled.
var VectorizeClusterMode = settings.RegisterEnumSetting(
	"sql.defaults.vectorize",
	"default vectorize mode",
	"auto",
	map[int64]string{
		int64(sessiondata.VectorizeOff):            "off",
		int64(sessiondata.VectorizeAuto):           "auto",
		int64(sessiondata.VectorizeExperimentalOn): "experimental_on",
	},
)

// VectorizeRowCountThresholdClusterValue controls the cluster default for the
// vectorize row count threshold. When it is met, the vectorized execution
// engine will be used if possible.
var VectorizeRowCountThresholdClusterValue = settings.RegisterValidatedIntSetting(
	"sql.defaults.vectorize_row_count_threshold",
	"default vectorize row count threshold",
	colexec.DefaultVectorizeRowCountThreshold,
	func(v int64) error {
		if v < 0 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"cannot set sql.defaults.vectorize_row_count_threshold to a negative value: %d", v)
		}
		return nil
	},
)

// DistSQLClusterExecMode controls the cluster default for when DistSQL is used.
var DistSQLClusterExecMode = settings.RegisterEnumSetting(
	"sql.defaults.distsql",
	"default distributed SQL execution mode",
	"auto",
	map[int64]string{
		int64(sessiondata.DistSQLOff):  "off",
		int64(sessiondata.DistSQLAuto): "auto",
		int64(sessiondata.DistSQLOn):   "on",
	},
)

// SerialNormalizationMode controls how the SERIAL type is interpreted in table
// definitions.
var SerialNormalizationMode = settings.RegisterEnumSetting(
	"sql.defaults.serial_normalization",
	"default handling of SERIAL in table definitions",
	"rowid",
	map[int64]string{
		int64(sessiondata.SerialUsesRowID):            "rowid",
		int64(sessiondata.SerialUsesVirtualSequences): "virtual_sequence",
		int64(sessiondata.SerialUsesSQLSequences):     "sql_sequence",
	},
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

const sqlTxnName string = "sql txn"
const metricsSampleInterval = 10 * time.Second

// Fully-qualified names for metrics.
var (
	MetaSQLExecLatency = metric.Metadata{
		Name:        "sql.exec.latency",
		Help:        "Latency of SQL statement execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLServiceLatency = metric.Metadata{
		Name:        "sql.service.latency",
		Help:        "Latency of SQL request execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLOpt = metric.Metadata{
		Name:        "sql.optimizer.count",
		Help:        "Number of statements which ran with the cost-based optimizer",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLOptFallback = metric.Metadata{
		Name:        "sql.optimizer.fallback.count",
		Help:        "Number of statements which the cost-based optimizer was unable to plan",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLOptPlanCacheHits = metric.Metadata{
		Name:        "sql.optimizer.plan_cache.hits",
		Help:        "Number of non-prepared statements for which a cached plan was used",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLOptPlanCacheMisses = metric.Metadata{
		Name:        "sql.optimizer.plan_cache.misses",
		Help:        "Number of non-prepared statements for which a cached plan was not used",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDistSQLSelect = metric.Metadata{
		Name:        "sql.distsql.select.count",
		Help:        "Number of DistSQL SELECT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDistSQLExecLatency = metric.Metadata{
		Name:        "sql.distsql.exec.latency",
		Help:        "Latency of DistSQL statement execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaDistSQLServiceLatency = metric.Metadata{
		Name:        "sql.distsql.service.latency",
		Help:        "Latency of DistSQL request execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaTxnAbort = metric.Metadata{
		Name:        "sql.txn.abort.count",
		Help:        "Number of SQL transaction abort errors",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaFailure = metric.Metadata{
		Name:        "sql.failure.count",
		Help:        "Number of statements resulting in a planning or runtime error",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLTxnLatency = metric.Metadata{
		Name:        "sql.txn.latency",
		Help:        "Latency of SQL transactions",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// Below are the metadata for the statement started counters.

	MetaQueryStarted = metric.Metadata{
		Name:        "sql.query.started.count",
		Help:        "Number of SQL queries started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnBeginStarted = metric.Metadata{
		Name:        "sql.txn.begin.started.count",
		Help:        "Number of SQL transaction BEGIN statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnCommitStarted = metric.Metadata{
		Name:        "sql.txn.commit.started.count",
		Help:        "Number of SQL transaction COMMIT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRollbackStarted = metric.Metadata{
		Name:        "sql.txn.rollback.started.count",
		Help:        "Number of SQL transaction ROLLBACK statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSelectStarted = metric.Metadata{
		Name:        "sql.select.started.count",
		Help:        "Number of SQL SELECT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaUpdateStarted = metric.Metadata{
		Name:        "sql.update.started.count",
		Help:        "Number of SQL UPDATE statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaInsertStarted = metric.Metadata{
		Name:        "sql.insert.started.count",
		Help:        "Number of SQL INSERT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDeleteStarted = metric.Metadata{
		Name:        "sql.delete.started.count",
		Help:        "Number of SQL DELETE statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSavepointStarted = metric.Metadata{
		Name:        "sql.savepoint.started.count",
		Help:        "Number of SQL SAVEPOINT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRestartSavepointStarted = metric.Metadata{
		Name:        "sql.restart_savepoint.started.count",
		Help:        "Number of `SAVEPOINT cockroach_restart` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaReleaseRestartSavepointStarted = metric.Metadata{
		Name:        "sql.restart_savepoint.release.started.count",
		Help:        "Number of `RELEASE SAVEPOINT cockroach_restart` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRollbackToRestartSavepointStarted = metric.Metadata{
		Name:        "sql.restart_savepoint.rollback.started.count",
		Help:        "Number of `ROLLBACK TO SAVEPOINT cockroach_restart` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDdlStarted = metric.Metadata{
		Name:        "sql.ddl.started.count",
		Help:        "Number of SQL DDL statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaMiscStarted = metric.Metadata{
		Name:        "sql.misc.started.count",
		Help:        "Number of other SQL statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}

	// Below are the metadata for the statement executed counters.
	MetaQueryExecuted = metric.Metadata{
		Name:        "sql.query.count",
		Help:        "Number of SQL queries executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnBeginExecuted = metric.Metadata{
		Name:        "sql.txn.begin.count",
		Help:        "Number of SQL transaction BEGIN statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnCommitExecuted = metric.Metadata{
		Name:        "sql.txn.commit.count",
		Help:        "Number of SQL transaction COMMIT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRollbackExecuted = metric.Metadata{
		Name:        "sql.txn.rollback.count",
		Help:        "Number of SQL transaction ROLLBACK statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSelectExecuted = metric.Metadata{
		Name:        "sql.select.count",
		Help:        "Number of SQL SELECT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaUpdateExecuted = metric.Metadata{
		Name:        "sql.update.count",
		Help:        "Number of SQL UPDATE statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaInsertExecuted = metric.Metadata{
		Name:        "sql.insert.count",
		Help:        "Number of SQL INSERT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDeleteExecuted = metric.Metadata{
		Name:        "sql.delete.count",
		Help:        "Number of SQL DELETE statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSavepointExecuted = metric.Metadata{
		Name:        "sql.savepoint.count",
		Help:        "Number of SQL SAVEPOINT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRestartSavepointExecuted = metric.Metadata{
		Name:        "sql.restart_savepoint.count",
		Help:        "Number of `SAVEPOINT cockroach_restart` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaReleaseRestartSavepointExecuted = metric.Metadata{
		Name:        "sql.restart_savepoint.release.count",
		Help:        "Number of `RELEASE SAVEPOINT cockroach_restart` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRollbackToRestartSavepointExecuted = metric.Metadata{
		Name:        "sql.restart_savepoint.rollback.count",
		Help:        "Number of `ROLLBACK TO SAVEPOINT cockroach_restart` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDdlExecuted = metric.Metadata{
		Name:        "sql.ddl.count",
		Help:        "Number of SQL DDL statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaMiscExecuted = metric.Metadata{
		Name:        "sql.misc.count",
		Help:        "Number of other SQL statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
)

func getMetricMeta(meta metric.Metadata, internal bool) metric.Metadata {
	if internal {
		meta.Name += ".internal"
		meta.Help += " (internal queries)"
		meta.Measurement = "SQL Internal Statements"
	}
	return meta
}

// NodeInfo contains metadata about the executing node and cluster.
type NodeInfo struct {
	ClusterID func() uuid.UUID
	NodeID    *base.NodeIDContainer
	AdminURL  func() *url.URL
	PGURL     func(*url.Userinfo) (*url.URL, error)
}

// nodeStatusGenerator is a limited portion of the status.MetricsRecorder
// struct, to avoid having to import all of status in sql.
type nodeStatusGenerator interface {
	GenerateNodeStatus(ctx context.Context) *statuspb.NodeStatus
}

// An ExecutorConfig encompasses the auxiliary objects and configuration
// required to create an executor.
// All fields holding a pointer or an interface are required to create
// a Executor; the rest will have sane defaults set if omitted.
type ExecutorConfig struct {
	Settings *cluster.Settings
	NodeInfo
	DefaultZoneConfig *zonepb.ZoneConfig
	Locality          roachpb.Locality
	AmbientCtx        log.AmbientContext
	DB                *client.DB
	Gossip            *gossip.Gossip
	DistSender        *kv.DistSender
	RPCContext        *rpc.Context
	LeaseManager      *LeaseManager
	Clock             *hlc.Clock
	DistSQLSrv        *distsql.ServerImpl
	StatusServer      serverpb.StatusServer
	MetricsRecorder   nodeStatusGenerator
	SessionRegistry   *SessionRegistry
	JobRegistry       *jobs.Registry
	VirtualSchemas    *VirtualSchemaHolder
	DistSQLPlanner    *DistSQLPlanner
	TableStatsCache   *stats.TableStatisticsCache
	StatsRefresher    *stats.Refresher
	ExecLogger        *log.SecondaryLogger
	AuditLogger       *log.SecondaryLogger
	InternalExecutor  *InternalExecutor
	QueryCache        *querycache.C

	TestingKnobs              ExecutorTestingKnobs
	PGWireTestingKnobs        *PGWireTestingKnobs
	SchemaChangerTestingKnobs *SchemaChangerTestingKnobs
	DistSQLRunTestingKnobs    *execinfra.TestingKnobs
	EvalContextTestingKnobs   tree.EvalContextTestingKnobs
	// HistogramWindowInterval is (server.Config).HistogramWindowInterval.
	HistogramWindowInterval time.Duration

	// Caches updated by DistSQL.
	RangeDescriptorCache *kv.RangeDescriptorCache
	LeaseHolderCache     *kv.LeaseHolderCache
}

// Organization returns the value of cluster.organization.
func (ec *ExecutorConfig) Organization() string {
	return ClusterOrganization.Get(&ec.Settings.SV)
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// StatementFilter is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
type StatementFilter func(context.Context, string, error)

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// StatementFilter can be used to trap execution of SQL statements and
	// optionally change their results. The filter function is invoked after each
	// statement has been executed.
	StatementFilter StatementFilter

	// BeforeExecute is called by the Executor before plan execution. It is useful
	// for synchronizing statement execution.
	BeforeExecute func(ctx context.Context, stmt string)

	// AfterExecute is like StatementFilter, but it runs in the same goroutine of the
	// statement.
	AfterExecute func(ctx context.Context, stmt string, err error)

	// DisableAutoCommit, if set, disables the auto-commit functionality of some
	// SQL statements. That functionality allows some statements to commit
	// directly when they're executed in an implicit SQL txn, without waiting for
	// the Executor to commit the implicit txn.
	// This has to be set in tests that need to abort such statements using a
	// StatementFilter; otherwise, the statement commits immediately after
	// execution so there'll be nothing left to abort by the time the filter runs.
	DisableAutoCommit bool

	// BeforeAutoCommit is called when the Executor is about to commit the KV
	// transaction after running a statement in an implicit transaction, allowing
	// tests to inject errors into that commit.
	// If an error is returned, that error will be considered the result of
	// txn.Commit(), and the txn.Commit() call will not actually be
	// made. If no error is returned, txn.Commit() is called normally.
	//
	// Note that this is not called if the SQL statement representing the implicit
	// transaction has committed the KV txn itself (e.g. if it used the 1-PC
	// optimization). This is only called when the Executor is the one doing the
	// committing.
	BeforeAutoCommit func(ctx context.Context, stmt string) error
}

// PGWireTestingKnobs contains knobs for the pgwire module.
type PGWireTestingKnobs struct {
	// CatchPanics causes the pgwire.conn to recover from panics in its execution
	// thread and return them as errors to the client, closing the connection
	// afterward.
	CatchPanics bool

	// AuthHook is used to override the normal authentication handling on new
	// connections.
	AuthHook func(context.Context) error
}

var _ base.ModuleTestingKnobs = &PGWireTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*PGWireTestingKnobs) ModuleTestingKnobs() {}

// databaseCacheHolder is a thread-safe container for a *databaseCache.
// It also allows clients to block until the cache is updated to a desired
// state.
//
// NOTE(andrei): The way in which we handle the database cache is funky: there's
// this top-level holder, which gets updated on gossip updates. Then, each
// session gets its *databaseCache, which is updated from the holder after every
// transaction - the SystemConfig is updated and the lazily computer map of db
// names to ids is wiped. So many session are sharing and contending on a
// mutable cache, but nobody's sharing this holder. We should make up our mind
// about whether we like the sharing or not and, if we do, share the holder too.
// Also, we could use the SystemConfigDeltaFilter to limit the updates to
// databases that chaged. One of the problems with the existing architecture
// is if a transaction is completed on a session and the session remains dormant
// for a long time, the next transaction will see a rather old database cache.
type databaseCacheHolder struct {
	mu struct {
		syncutil.Mutex
		c  *databaseCache
		cv *sync.Cond
	}
}

func newDatabaseCacheHolder(c *databaseCache) *databaseCacheHolder {
	dc := &databaseCacheHolder{}
	dc.mu.c = c
	dc.mu.cv = sync.NewCond(&dc.mu.Mutex)
	return dc
}

func (dc *databaseCacheHolder) getDatabaseCache() *databaseCache {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.mu.c
}

// waitForCacheState implements the dbCacheSubscriber interface.
func (dc *databaseCacheHolder) waitForCacheState(cond func(*databaseCache) bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for done := cond(dc.mu.c); !done; done = cond(dc.mu.c) {
		dc.mu.cv.Wait()
	}
}

// databaseCacheHolder implements the dbCacheSubscriber interface.
var _ dbCacheSubscriber = &databaseCacheHolder{}

// updateSystemConfig is called whenever a new system config gossip entry is
// received.
func (dc *databaseCacheHolder) updateSystemConfig(cfg *config.SystemConfig) {
	dc.mu.Lock()
	dc.mu.c.databases = sync.Map{}
	dc.mu.c.systemConfig = cfg
	dc.mu.cv.Broadcast()
	dc.mu.Unlock()
}

func shouldDistributeGivenRecAndMode(
	rec distRecommendation, mode sessiondata.DistSQLExecMode,
) bool {
	switch mode {
	case sessiondata.DistSQLOff:
		return false
	case sessiondata.DistSQLAuto:
		return rec == shouldDistribute
	case sessiondata.DistSQLOn, sessiondata.DistSQLAlways:
		return rec != cannotDistribute
	}
	panic(fmt.Sprintf("unhandled distsql mode %v", mode))
}

// shouldDistributePlan determines whether we should distribute the
// given logical plan, based on the session settings.
func shouldDistributePlan(
	ctx context.Context, distSQLMode sessiondata.DistSQLExecMode, dp *DistSQLPlanner, plan planNode,
) bool {
	if distSQLMode == sessiondata.DistSQLOff {
		return false
	}

	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.(*zeroNode); ok {
		return false
	}

	rec, err := dp.checkSupportForNode(plan)
	if err != nil {
		// Don't use distSQL for this request.
		log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
		return false
	}

	return shouldDistributeGivenRecAndMode(rec, distSQLMode)
}

// golangFillQueryArguments transforms Go values into datums.
// Some of the args can be datums (in which case the transformation is a no-op).
//
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func golangFillQueryArguments(args ...interface{}) tree.Datums {
	res := make(tree.Datums, len(args))
	for i, arg := range args {
		if arg == nil {
			res[i] = tree.DNull
			continue
		}

		// A type switch to handle a few explicit types with special semantics:
		// - Datums are passed along as is.
		// - Time datatypes get special representation in the database.
		var d tree.Datum
		switch t := arg.(type) {
		case tree.Datum:
			d = t
		case time.Time:
			d = tree.MakeDTimestamp(t, time.Microsecond)
		case time.Duration:
			d = &tree.DInterval{Duration: duration.MakeDuration(t.Nanoseconds(), 0, 0)}
		case bitarray.BitArray:
			d = &tree.DBitArray{BitArray: t}
		case *apd.Decimal:
			dd := &tree.DDecimal{}
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
				d = tree.MakeDBool(tree.DBool(val.Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				d = tree.NewDInt(tree.DInt(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				d = tree.NewDInt(tree.DInt(val.Uint()))
			case reflect.Float32, reflect.Float64:
				d = tree.NewDFloat(tree.DFloat(val.Float()))
			case reflect.String:
				d = tree.NewDString(val.String())
			case reflect.Slice:
				// Handle byte slices.
				if val.Type().Elem().Kind() == reflect.Uint8 {
					d = tree.NewDBytes(tree.DBytes(val.Bytes()))
				}
			}
			if d == nil {
				panic(fmt.Sprintf("unexpected type %T", arg))
			}
		}
		res[i] = d
	}
	return res
}

// checkResultType verifies that a table result can be returned to the
// client.
func checkResultType(typ *types.T) error {
	// Compare all types that can rely on == equality.
	switch typ.Family() {
	case types.UnknownFamily:
	case types.BitFamily:
	case types.BoolFamily:
	case types.IntFamily:
	case types.FloatFamily:
	case types.DecimalFamily:
	case types.BytesFamily:
	case types.StringFamily:
	case types.CollatedStringFamily:
	case types.DateFamily:
	case types.TimestampFamily:
	case types.TimeFamily:
	case types.TimestampTZFamily:
	case types.IntervalFamily:
	case types.JsonFamily:
	case types.UuidFamily:
	case types.INetFamily:
	case types.OidFamily:
	case types.TupleFamily:
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.ArrayFamily {
			// Technically we could probably return arrays of arrays to a
			// client (the encoding exists) but we don't want to give
			// mixed signals -- that nested arrays appear to be supported
			// in this case, and not in other cases (eg. CREATE). So we
			// reject them in every case instead.
			return unimplemented.NewWithIssueDetail(32552,
				"result", "arrays cannot have arrays as element type")
		}
	case types.AnyFamily:
		// Placeholder case.
		return errors.Errorf("could not determine data type of %s", typ)
	default:
		return errors.Errorf("unsupported result type: %s", typ)
	}
	return nil
}

// EvalAsOfTimestamp evaluates and returns the timestamp from an AS OF SYSTEM
// TIME clause.
func (p *planner) EvalAsOfTimestamp(asOf tree.AsOfClause) (_ hlc.Timestamp, err error) {
	ts, err := tree.EvalAsOfTimestamp(asOf, &p.semaCtx, p.EvalContext())
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if now := p.execCfg.Clock.Now(); now.Less(ts) {
		return hlc.Timestamp{}, errors.Errorf(
			"AS OF SYSTEM TIME: cannot specify timestamp in the future (%s > %s)", ts, now)
	}
	return ts, nil
}

// ParseHLC parses a string representation of an `hlc.Timestamp`.
func ParseHLC(s string) (hlc.Timestamp, error) {
	dec, _, err := apd.NewFromString(s)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return tree.DecimalToHLC(dec)
}

// isAsOf analyzes a statement to bypass the logic in newPlan(), since
// that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction
// should be set. The statements that will be checked are Select,
// ShowTrace (of a Select statement), Scrub, Export, and CreateStats.
func (p *planner) isAsOf(stmt tree.Statement) (*hlc.Timestamp, error) {
	var asOf tree.AsOfClause
	switch s := stmt.(type) {
	case *tree.Select:
		selStmt := s.Select
		var parenSel *tree.ParenSelect
		var ok bool
		for parenSel, ok = selStmt.(*tree.ParenSelect); ok; parenSel, ok = selStmt.(*tree.ParenSelect) {
			selStmt = parenSel.Select.Select
		}

		sc, ok := selStmt.(*tree.SelectClause)
		if !ok {
			return nil, nil
		}
		if sc.From.AsOf.Expr == nil {
			return nil, nil
		}

		asOf = sc.From.AsOf
	case *tree.Scrub:
		if s.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.AsOf
	case *tree.Export:
		return p.isAsOf(s.Query)
	case *tree.CreateStats:
		if s.Options.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.Options.AsOf
	default:
		return nil, nil
	}
	ts, err := p.EvalAsOfTimestamp(asOf)
	return &ts, err
}

// isSavepoint returns true if stmt is a SAVEPOINT statement.
func isSavepoint(stmt Statement) bool {
	_, isSavepoint := stmt.AST.(*tree.Savepoint)
	return isSavepoint
}

// isSetTransaction returns true if stmt is a "SET TRANSACTION ..." statement.
func isSetTransaction(stmt Statement) bool {
	_, isSet := stmt.AST.(*tree.SetTransaction)
	return isSet
}

// queryPhase represents a phase during a query's execution.
type queryPhase int

const (
	// The phase before start of execution (includes parsing, building a plan).
	preparing queryPhase = 0

	// Execution phase.
	executing queryPhase = 1
)

// queryMeta stores metadata about a query. Stored as reference in
// session.mu.ActiveQueries.
type queryMeta struct {
	// The timestamp when this query began execution.
	start time.Time

	// AST of the SQL statement - converted to query string only when necessary.
	stmt tree.Statement

	// States whether this query is distributed. Note that all queries,
	// including those that are distributed, have this field set to false until
	// start of execution; only at that point can we can actually determine whether
	// this query will be distributed. Use the phase variable below
	// to determine whether this query has entered execution yet.
	isDistributed bool

	// Current phase of execution of query.
	phase queryPhase

	// Cancellation function for the context associated with this query's transaction.
	ctxCancel context.CancelFunc

	// If set, this query will not be reported as part of SHOW QUERIES. This is
	// set based on the statement implementing tree.HiddenFromShowQueries.
	hidden bool
}

// cancel cancels the query associated with this queryMeta, by closing the associated
// txn context.
func (q *queryMeta) cancel() {
	q.ctxCancel()
}

// SessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT (or RESET ...) statements.
type SessionDefaults map[string]string

// SessionArgs contains arguments for serving a client connection.
type SessionArgs struct {
	User            string
	SessionDefaults SessionDefaults
	// RemoteAddr is the client's address. This is nil iff this is an internal
	// client.
	RemoteAddr            net.Addr
	ConnResultsBufferSize int64
}

// isDefined returns true iff the SessionArgs is well-defined.
// This method exists because SessionArgs is passed by value but it
// matters to the functions using it whether the value was explicitly
// specified or left empty.
func (s SessionArgs) isDefined() bool { return len(s.User) != 0 }

// SessionRegistry stores a set of all sessions on this node.
// Use register() and deregister() to modify this registry.
type SessionRegistry struct {
	syncutil.Mutex
	sessions map[ClusterWideID]registrySession
}

// NewSessionRegistry creates a new SessionRegistry with an empty set
// of sessions.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{sessions: make(map[ClusterWideID]registrySession)}
}

func (r *SessionRegistry) register(id ClusterWideID, s registrySession) {
	r.Lock()
	r.sessions[id] = s
	r.Unlock()
}

func (r *SessionRegistry) deregister(id ClusterWideID) {
	r.Lock()
	delete(r.sessions, id)
	r.Unlock()
}

type registrySession interface {
	user() string
	cancelQuery(queryID ClusterWideID) bool
	cancelSession()
	// serialize serializes a Session into a serverpb.Session
	// that can be served over RPC.
	serialize() serverpb.Session
}

// CancelQuery looks up the associated query in the session registry and cancels it.
func (r *SessionRegistry) CancelQuery(queryIDStr string, username string) (bool, error) {
	queryID, err := StringToClusterWideID(queryIDStr)
	if err != nil {
		return false, fmt.Errorf("query ID %s malformed: %s", queryID, err)
	}

	r.Lock()
	defer r.Unlock()

	for _, session := range r.sessions {
		if !(username == security.RootUser || username == session.user()) {
			// Skip this session.
			continue
		}

		if session.cancelQuery(queryID) {
			return true, nil
		}
	}

	return false, fmt.Errorf("query ID %s not found", queryID)
}

// CancelSession looks up the specified session in the session registry and cancels it.
func (r *SessionRegistry) CancelSession(sessionIDBytes []byte, username string) (bool, error) {
	sessionID := BytesToClusterWideID(sessionIDBytes)

	r.Lock()
	defer r.Unlock()

	for id, session := range r.sessions {
		if !(username == security.RootUser || username == session.user()) {
			// Skip this session.
			continue
		}

		if id == sessionID {
			session.cancelSession()
			return true, nil
		}
	}

	return false, fmt.Errorf("session ID %s not found", sessionID)
}

// SerializeAll returns a slice of all sessions in the registry, converted to serverpb.Sessions.
func (r *SessionRegistry) SerializeAll() []serverpb.Session {
	r.Lock()
	defer r.Unlock()

	response := make([]serverpb.Session, 0, len(r.sessions))

	for _, s := range r.sessions {
		response = append(response, s.serialize())
	}

	return response
}

func newSchemaInterface(tables *TableCollection, vt VirtualTabler) *schemaInterface {
	sc := &schemaInterface{
		physical: &CachedPhysicalAccessor{
			SchemaAccessor: UncachedPhysicalAccessor{},
			tc:             tables,
		},
	}
	sc.logical = &LogicalSchemaAccessor{
		SchemaAccessor: sc.physical,
		vt:             vt,
	}
	return sc
}

// MaxSQLBytes is the maximum length in bytes of SQL statements serialized
// into a serverpb.Session. Exported for testing.
const MaxSQLBytes = 1000

type schemaChangerCollection struct {
	schemaChangers []SchemaChanger
}

func (scc *schemaChangerCollection) queueSchemaChanger(schemaChanger SchemaChanger) {
	scc.schemaChangers = append(scc.schemaChangers, schemaChanger)
}

func (scc *schemaChangerCollection) reset() {
	scc.schemaChangers = nil
}

// execSchemaChanges releases schema leases and runs the queued
// schema changers. This needs to be run after the transaction
// scheduling the schema change has finished.
//
// The list of closures is cleared after (attempting) execution.
func (scc *schemaChangerCollection) execSchemaChanges(
	ctx context.Context,
	cfg *ExecutorConfig,
	tracing *SessionTracing,
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) error {
	if len(scc.schemaChangers) == 0 {
		return nil
	}
	if fn := cfg.SchemaChangerTestingKnobs.SyncFilter; fn != nil {
		fn(TestingSchemaChangerCollection{scc})
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	var firstError error
	for _, sc := range scc.schemaChangers {
		sc.db = cfg.DB
		sc.testingKnobs = cfg.SchemaChangerTestingKnobs
		sc.distSQLPlanner = cfg.DistSQLPlanner
		sc.settings = cfg.Settings
		sc.ieFactory = ieFactory
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			evalCtx := createSchemaChangeEvalCtx(ctx, cfg.Clock.Now(), tracing, ieFactory)
			if err := sc.exec(ctx, true /* inSession */, &evalCtx); err != nil {
				if onError := cfg.SchemaChangerTestingKnobs.OnError; onError != nil {
					onError(err)
				}
				if shouldLogSchemaChangeError(err) {
					log.Warningf(ctx, "error executing schema change: %s", err)
				}

				if err == sqlbase.ErrDescriptorNotFound || err == ctx.Err() {
					// 1. If the descriptor is dropped while the schema change
					// is executing, the schema change is considered completed.
					// 2. If the context is canceled the schema changer quits here
					// letting the asynchronous code path complete the schema
					// change.
				} else if isPermanentSchemaChangeError(err) {
					// All constraint violations can be reported; we report it as the result
					// corresponding to the statement that enqueued this changer.
					// There's some sketchiness here: we assume there's a single result
					// per statement and we clobber the result/error of the corresponding
					// statement.
					if firstError == nil {
						firstError = err
					}
				} else {
					// retryable error.
					continue
				}
			}
			break
		}
	}
	scc.schemaChangers = nil
	return firstError
}

const panicLogOutputCutoffChars = 10000

func anonymizeStmtAndConstants(stmt tree.Statement) string {
	return tree.AsStringWithFlags(stmt, tree.FmtAnonymize|tree.FmtHideConstants)
}

// AnonymizeStatementsForReporting transforms an action, SQL statements, and a value
// (usually a recovered panic) into an error that will be useful when passed to
// our error reporting as it exposes a scrubbed version of the statements.
func AnonymizeStatementsForReporting(action, sqlStmts string, r interface{}) error {
	var anonymized []string
	{
		stmts, err := parser.Parse(sqlStmts)
		if err == nil {
			for i := range stmts {
				anonymized = append(anonymized, anonymizeStmtAndConstants(stmts[i].AST))
			}
		}
	}
	anonStmtsStr := strings.Join(anonymized, "; ")
	if len(anonStmtsStr) > panicLogOutputCutoffChars {
		anonStmtsStr = anonStmtsStr[:panicLogOutputCutoffChars] + " [...]"
	}

	return log.Safe(
		fmt.Sprintf("panic while %s %d statements: %s", action, len(anonymized), anonStmtsStr),
	).WithCause(r)
}

// SessionTracing holds the state used by SET TRACING {ON,OFF,LOCAL} statements in
// the context of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// SessionTracing and its interactions with the connExecutor are thread-safe;
// tracing can be turned on at any time.
type SessionTracing struct {
	// enabled is set at times when "session enabled" is active - i.e. when
	// transactions are being recorded.
	enabled bool

	// kvTracingEnabled is set at times when KV tracing is active. When
	// KV tracning is enabled, the SQL/KV interface logs individual K/V
	// operators to the current context.
	kvTracingEnabled bool

	// showResults, when set, indicates that the result rows produced by
	// the execution statement must be reported in the
	// trace. showResults can be set manually by SET TRACING = ...,
	// results
	showResults bool

	// If recording==true, recordingType indicates the type of the current
	// recording.
	recordingType tracing.RecordingType

	// ex is the connExecutor to which this SessionTracing is tied.
	ex *connExecutor

	// firstTxnSpan is the span of the first txn that was active when session
	// tracing was enabled.
	firstTxnSpan opentracing.Span

	// connSpan is the connection's span. This is recording.
	connSpan opentracing.Span

	// lastRecording will collect the recording when stopping tracing.
	lastRecording []traceRow
}

// getSessionTrace returns the session trace. If we're not currently tracing,
// this will be the last recorded trace. If we are currently tracing, we'll
// return whatever was recorded so far.
func (st *SessionTracing) getSessionTrace() ([]traceRow, error) {
	if !st.enabled {
		return st.lastRecording, nil
	}

	return generateSessionTraceVTable(st.getRecording())
}

// getRecording returns the recorded spans of the current trace.
func (st *SessionTracing) getRecording() []tracing.RecordedSpan {
	var spans []tracing.RecordedSpan
	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
	}
	return append(spans, tracing.GetRecording(st.connSpan)...)
}

// StartTracing starts "session tracing". From this moment on, everything
// happening on both the connection's context and the current txn's context (if
// any) will be traced.
// StopTracing() needs to be called to finish this trace.
//
// There's two contexts on which we must record:
// 1) If we're inside a txn, we start recording on the txn's span. We assume
// that the txn's ctx has a recordable span on it.
// 2) Regardless of whether we're in a txn or not, we need to record the
// connection's context. This context generally does not have a span, so we
// "hijack" it with one that does. Whatever happens on that context, plus
// whatever happens in future derived txn contexts, will be recorded.
//
// Args:
// kvTracingEnabled: If set, the traces will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the messages
//   are per-row.
// showResults: If set, result rows are reported in the trace.
func (st *SessionTracing) StartTracing(
	recType tracing.RecordingType, kvTracingEnabled, showResults bool,
) error {
	if st.enabled {
		// We're already tracing. Only treat as no-op if the same options
		// are requested.
		if kvTracingEnabled != st.kvTracingEnabled ||
			showResults != st.showResults ||
			recType != st.recordingType {
			var desiredOptions bytes.Buffer
			comma := ""
			if kvTracingEnabled {
				desiredOptions.WriteString("kv")
				comma = ", "
			}
			if showResults {
				fmt.Fprintf(&desiredOptions, "%sresults", comma)
				comma = ", "
			}
			recOption := "cluster"
			if recType == tracing.SingleNodeRecording {
				recOption = "local"
			}
			fmt.Fprintf(&desiredOptions, "%s%s", comma, recOption)

			err := pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"tracing is already started with different options")
			return errors.WithHintf(err,
				"reset with SET tracing = off; SET tracing = %s", desiredOptions.String())
		}

		return nil
	}

	// If we're inside a transaction, start recording on the txn span.
	if _, ok := st.ex.machine.CurState().(stateNoTxn); !ok {
		sp := opentracing.SpanFromContext(st.ex.state.Ctx)
		if sp == nil {
			return errors.Errorf("no txn span for SessionTracing")
		}
		tracing.StartRecording(sp, recType)
		st.firstTxnSpan = sp
	}

	st.enabled = true
	st.kvTracingEnabled = kvTracingEnabled
	st.showResults = showResults
	st.recordingType = recType

	// Now hijack the conn's ctx with one that has a recording span.

	opName := "session recording"
	var sp opentracing.Span
	connCtx := st.ex.ctxHolder.connCtx

	// TODO(andrei): use tracing.EnsureChildSpan() or something more efficient
	// than StartSpan(). The problem is that the current interface doesn't allow
	// the Recordable option to be passed.
	if parentSp := opentracing.SpanFromContext(connCtx); parentSp != nil {
		// Create a child span while recording.
		sp = parentSp.Tracer().StartSpan(
			opName,
			opentracing.ChildOf(parentSp.Context()), tracing.Recordable,
			tracing.LogTagsFromCtx(connCtx),
		)
	} else {
		// Create a root span while recording.
		sp = st.ex.server.cfg.AmbientCtx.Tracer.StartSpan(
			opName, tracing.Recordable,
			tracing.LogTagsFromCtx(connCtx),
		)
	}
	tracing.StartRecording(sp, recType)
	st.connSpan = sp

	// Hijack the connections context.
	newConnCtx := opentracing.ContextWithSpan(st.ex.ctxHolder.connCtx, sp)
	st.ex.ctxHolder.hijack(newConnCtx)

	return nil
}

// StopTracing stops the trace that was started with StartTracing().
// An error is returned if tracing was not active.
func (st *SessionTracing) StopTracing() error {
	if !st.enabled {
		// We're not currently tracing. No-op.
		return nil
	}
	st.enabled = false
	st.kvTracingEnabled = false
	st.showResults = false
	st.recordingType = tracing.NoRecording

	var spans []tracing.RecordedSpan

	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
		tracing.StopRecording(st.firstTxnSpan)
	}
	st.connSpan.Finish()
	spans = append(spans, tracing.GetRecording(st.connSpan)...)
	// NOTE: We're stopping recording on the connection's ctx only; the stopping
	// is not inherited by children. If we are inside of a txn, that span will
	// continue recording, even though nobody will collect its recording again.
	tracing.StopRecording(st.connSpan)
	st.ex.ctxHolder.unhijack()

	var err error
	st.lastRecording, err = generateSessionTraceVTable(spans)
	return err
}

// RecordingType returns which type of tracing is currently being done.
func (st *SessionTracing) RecordingType() tracing.RecordingType {
	return st.recordingType
}

// KVTracingEnabled checks whether KV tracing is currently enabled.
func (st *SessionTracing) KVTracingEnabled() bool {
	return st.kvTracingEnabled
}

// Enabled checks whether session tracing is currently enabled.
func (st *SessionTracing) Enabled() bool {
	return st.enabled
}

// TracePlanStart conditionally emits a trace message at the moment
// logical planning starts.
func (st *SessionTracing) TracePlanStart(ctx context.Context, stmtTag string) {
	if st.enabled {
		log.VEventf(ctx, 2, "planning starts: %s", stmtTag)
	}
}

// TracePlanEnd conditionally emits a trace message at the moment
// logical planning ends.
func (st *SessionTracing) TracePlanEnd(ctx context.Context, err error) {
	log.VEventfDepth(ctx, 2, 1, "planning ends")
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "planning error: %v", err)
	}
}

// TracePlanCheckStart conditionally emits a trace message at the
// moment the test of which execution engine to use starts.
func (st *SessionTracing) TracePlanCheckStart(ctx context.Context) {
	log.VEventfDepth(ctx, 2, 1, "checking distributability")
}

// TracePlanCheckEnd conditionally emits a trace message at the moment
// the engine check ends.
func (st *SessionTracing) TracePlanCheckEnd(ctx context.Context, err error, dist bool) {
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "distributability check error: %v", err)
	} else {
		log.VEventfDepth(ctx, 2, 1, "will distribute plan: %v", dist)
	}
}

// TraceExecStart conditionally emits a trace message at the moment
// plan execution starts.
func (st *SessionTracing) TraceExecStart(ctx context.Context, engine string) {
	log.VEventfDepth(ctx, 2, 1, "execution starts: %s engine", engine)
}

// TraceExecConsume creates a context for TraceExecRowsResult below.
func (st *SessionTracing) TraceExecConsume(ctx context.Context) (context.Context, func()) {
	if st.enabled {
		consumeCtx, sp := tracing.ChildSpan(ctx, "consuming rows")
		return consumeCtx, sp.Finish
	}
	return ctx, func() {}
}

// TraceExecRowsResult conditionally emits a trace message for a single output row.
func (st *SessionTracing) TraceExecRowsResult(ctx context.Context, values tree.Datums) {
	if st.showResults {
		log.VEventfDepth(ctx, 2, 1, "output row: %s", values)
	}
}

// TraceExecEnd conditionally emits a trace message at the moment
// plan execution completes.
func (st *SessionTracing) TraceExecEnd(ctx context.Context, err error, count int) {
	log.VEventfDepth(ctx, 2, 1, "execution ends")
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "execution failed after %d rows: %v", count, err)
	} else {
		log.VEventfDepth(ctx, 2, 1, "rows affected: %d", count)
	}
}

// extractMsgFromRecord extracts the message of the event, which is either in an
// "event" or "error" field.
func extractMsgFromRecord(rec tracing.RecordedSpan_LogRecord) string {
	for _, f := range rec.Fields {
		key := f.Key
		if key == "event" {
			return f.Value
		}
		if key == "error" {
			return fmt.Sprint("error:", f.Value)
		}
	}
	return "<event missing in trace message>"
}

const (
	// span_idx    INT NOT NULL,        -- The span's index.
	traceSpanIdxCol = iota
	// message_idx INT NOT NULL,        -- The message's index within its span.
	_
	// timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
	traceTimestampCol
	// duration    INTERVAL,            -- The span's duration.
	//                                  -- NULL if the span was not finished at the time
	//                                  -- the trace has been collected.
	traceDurationCol
	// operation   STRING NULL,         -- The span's operation.
	traceOpCol
	// loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
	traceLocCol
	// tag         STRING NOT NULL,     -- The logging tag, if any.
	traceTagCol
	// message     STRING NOT NULL,     -- The logged message.
	traceMsgCol
	// age         INTERVAL NOT NULL    -- The age of the message.
	traceAgeCol
	// traceNumCols must be the last item in the enumeration.
	traceNumCols
)

// traceRow is the type of a single row in the session_trace vtable.
type traceRow [traceNumCols]tree.Datum

// A regular expression to split log messages.
// It has three parts:
// - the (optional) code location, with at least one forward slash and a period
//   in the file name:
//   ((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?)
// - the (optional) tag: ((?:\[(?:[^][]|\[[^]]*\])*\])?)
// - the message itself: the rest.
var logMessageRE = regexp.MustCompile(
	`(?s:^((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?) *((?:\[(?:[^][]|\[[^]]*\])*\])?) *(.*))`)

// generateSessionTraceVTable generates the rows of said table by using the log
// messages from the session's trace (i.e. the ongoing trace, if any, or the
// last one recorded).
//
// All the log messages from the current recording are returned, in
// the order in which they should be presented in the crdb_internal.session_info
// virtual table. Messages from child spans are inserted as a block in between
// messages from the parent span. Messages from sibling spans are not
// interleaved.
//
// Here's a drawing showing the order in which messages from different spans
// will be interleaved. Each box is a span; inner-boxes are child spans. The
// numbers indicate the order in which the log messages will appear in the
// virtual table.
//
// +-----------------------+
// |           1           |
// | +-------------------+ |
// | |         2         | |
// | |  +----+           | |
// | |  |    | +----+    | |
// | |  | 3  | | 4  |    | |
// | |  |    | |    |  5 | |
// | |  |    | |    | ++ | |
// | |  |    | |    |    | |
// | |  +----+ |    |    | |
// | |         +----+    | |
// | |                   | |
// | |          6        | |
// | +-------------------+ |
// |            7          |
// +-----------------------+
//
// Note that what's described above is not the order in which SHOW TRACE FOR SESSION
// displays the information: SHOW TRACE will sort by the age column.
func generateSessionTraceVTable(spans []tracing.RecordedSpan) ([]traceRow, error) {
	// Get all the log messages, in the right order.
	var allLogs []logRecordRow

	// NOTE: The spans are recorded in the order in which they are started.
	seenSpans := make(map[uint64]struct{})
	for spanIdx, span := range spans {
		if _, ok := seenSpans[span.SpanID]; ok {
			continue
		}
		spanWithIndex := spanWithIndex{
			RecordedSpan: &spans[spanIdx],
			index:        spanIdx,
		}
		msgs, err := getMessagesForSubtrace(spanWithIndex, spans, seenSpans)
		if err != nil {
			return nil, err
		}
		allLogs = append(allLogs, msgs...)
	}

	// Transform the log messages into table rows.
	// We need to populate "operation" later because it is only
	// set for the first row in each span.
	opMap := make(map[tree.DInt]*tree.DString)
	durMap := make(map[tree.DInt]*tree.DInterval)
	var res []traceRow
	var minTimestamp, zeroTime time.Time
	for _, lrr := range allLogs {
		// The "operation" column is only set for the first row in span.
		// We'll populate the rest below.
		if lrr.index == 0 {
			spanIdx := tree.DInt(lrr.span.index)
			opMap[spanIdx] = tree.NewDString(lrr.span.Operation)
			if lrr.span.Duration != 0 {
				durMap[spanIdx] = &tree.DInterval{
					Duration: duration.MakeDuration(lrr.span.Duration.Nanoseconds(), 0, 0),
				}
			}
		}

		// We'll need the lowest timestamp to compute ages below.
		if minTimestamp == zeroTime || lrr.timestamp.Before(minTimestamp) {
			minTimestamp = lrr.timestamp
		}

		// Split the message into component parts.
		//
		// The result of FindStringSubmatchIndex is a 1D array of pairs
		// [start, end) of positions in the input string.  The first pair
		// identifies the entire match; the 2nd pair corresponds to the
		// 1st parenthetized expression in the regexp, and so on.
		loc := logMessageRE.FindStringSubmatchIndex(lrr.msg)
		if loc == nil {
			return nil, fmt.Errorf("unable to split trace message: %q", lrr.msg)
		}

		row := traceRow{
			tree.NewDInt(tree.DInt(lrr.span.index)),               // span_idx
			tree.NewDInt(tree.DInt(lrr.index)),                    // message_idx
			tree.MakeDTimestampTZ(lrr.timestamp, time.Nanosecond), // timestamp
			tree.DNull,                              // duration, will be populated below
			tree.DNull,                              // operation, will be populated below
			tree.NewDString(lrr.msg[loc[2]:loc[3]]), // location
			tree.NewDString(lrr.msg[loc[4]:loc[5]]), // tag
			tree.NewDString(lrr.msg[loc[6]:loc[7]]), // message
			tree.DNull,                              // age, will be populated below
		}
		res = append(res, row)
	}

	if len(res) == 0 {
		// Nothing to do below. Shortcut.
		return res, nil
	}

	// Populate the operation and age columns.
	for i := range res {
		spanIdx := res[i][traceSpanIdxCol]

		if opStr, ok := opMap[*(spanIdx.(*tree.DInt))]; ok {
			res[i][traceOpCol] = opStr
		}

		if dur, ok := durMap[*(spanIdx.(*tree.DInt))]; ok {
			res[i][traceDurationCol] = dur
		}

		ts := res[i][traceTimestampCol].(*tree.DTimestampTZ)
		res[i][traceAgeCol] = &tree.DInterval{
			Duration: duration.MakeDuration(ts.Sub(minTimestamp).Nanoseconds(), 0, 0),
		}
	}

	return res, nil
}

// getOrderedChildSpans returns all the spans in allSpans that are children of
// spanID. It assumes the input is ordered by start time, in which case the
// output is also ordered.
func getOrderedChildSpans(spanID uint64, allSpans []tracing.RecordedSpan) []spanWithIndex {
	children := make([]spanWithIndex, 0)
	for i := range allSpans {
		if allSpans[i].ParentSpanID == spanID {
			children = append(
				children,
				spanWithIndex{
					RecordedSpan: &allSpans[i],
					index:        i,
				})
		}
	}
	return children
}

// getMessagesForSubtrace takes a span and interleaves its log messages with
// those from its children (recursively). The order is the one defined in the
// comment on generateSessionTraceVTable().
//
// seenSpans is modified to record all the spans that are part of the subtrace
// rooted at span.
func getMessagesForSubtrace(
	span spanWithIndex, allSpans []tracing.RecordedSpan, seenSpans map[uint64]struct{},
) ([]logRecordRow, error) {
	if _, ok := seenSpans[span.SpanID]; ok {
		return nil, errors.Errorf("duplicate span %d", span.SpanID)
	}
	var allLogs []logRecordRow
	const spanStartMsgTemplate = "=== SPAN START: %s ==="

	// spanStartMsgs are metadata about the span, e.g. the operation name and tags
	// contained in the span. They are added as one log message.
	spanStartMsgs := make([]string, 0, len(span.Tags)+1)

	spanStartMsgs = append(spanStartMsgs, fmt.Sprintf(spanStartMsgTemplate, span.Operation))

	// Add recognized tags to the output.
	for name, value := range span.Tags {
		if !strings.HasPrefix(name, tracing.TagPrefix) {
			// Not a tag to be output.
			continue
		}
		spanStartMsgs = append(spanStartMsgs, fmt.Sprintf("%s: %s", name, value))
	}
	sort.Strings(spanStartMsgs[1:])

	// This message holds all the spanStartMsgs and marks the beginning of the
	// span, to indicate the start time and duration of the span.
	allLogs = append(
		allLogs,
		logRecordRow{
			timestamp: span.StartTime,
			msg:       strings.Join(spanStartMsgs, "\n"),
			span:      span,
			index:     0,
		},
	)

	seenSpans[span.SpanID] = struct{}{}
	childSpans := getOrderedChildSpans(span.SpanID, allSpans)
	var i, j int
	// Sentinel value - year 6000.
	maxTime := time.Date(6000, 0, 0, 0, 0, 0, 0, time.UTC)
	// Merge the logs with the child spans.
	for i < len(span.Logs) || j < len(childSpans) {
		logTime := maxTime
		childTime := maxTime
		if i < len(span.Logs) {
			logTime = span.Logs[i].Time
		}
		if j < len(childSpans) {
			childTime = childSpans[j].StartTime
		}

		if logTime.Before(childTime) {
			allLogs = append(allLogs,
				logRecordRow{
					timestamp: logTime,
					msg:       extractMsgFromRecord(span.Logs[i]),
					span:      span,
					// Add 1 to the index to account for the first dummy message in a
					// span.
					index: i + 1,
				})
			i++
		} else {
			// Recursively append messages from the trace rooted at the child.
			childMsgs, err := getMessagesForSubtrace(childSpans[j], allSpans, seenSpans)
			if err != nil {
				return nil, err
			}
			allLogs = append(allLogs, childMsgs...)
			j++
		}
	}
	return allLogs, nil
}

// logRecordRow is used to temporarily hold on to log messages and their
// metadata while flattening a trace.
type logRecordRow struct {
	timestamp time.Time
	msg       string
	span      spanWithIndex
	// index of the log message within its span.
	index int
}

type spanWithIndex struct {
	*tracing.RecordedSpan
	index int
}

// sessionDataMutator is the interface used by sessionVars to change the session
// state. It mostly mutates the Session's SessionData, but not exclusively (e.g.
// see curTxnReadOnly).
type sessionDataMutator struct {
	data     *sessiondata.SessionData
	defaults SessionDefaults
	settings *cluster.Settings
	// setCurTxnReadOnly is called when we execute SET transaction_read_only = ...
	setCurTxnReadOnly func(val bool)
	// applicationNamedChanged, if set, is called when the "application name"
	// variable is updated.
	applicationNameChanged func(newName string)
}

// SetApplicationName sets the application name.
func (m *sessionDataMutator) SetApplicationName(appName string) {
	m.data.ApplicationName = appName
	if m.applicationNameChanged != nil {
		m.applicationNameChanged(appName)
	}
}

func (m *sessionDataMutator) SetBytesEncodeFormat(val sessiondata.BytesEncodeFormat) {
	m.data.DataConversion.BytesEncodeFormat = val
}

func (m *sessionDataMutator) SetExtraFloatDigits(val int) {
	m.data.DataConversion.ExtraFloatDigits = val
}

func (m *sessionDataMutator) SetDatabase(dbName string) {
	m.data.Database = dbName
}

func (m *sessionDataMutator) SetDefaultIntSize(size int) {
	m.data.DefaultIntSize = size
}

func (m *sessionDataMutator) SetDefaultReadOnly(val bool) {
	m.data.DefaultReadOnly = val
}

func (m *sessionDataMutator) SetDistSQLMode(val sessiondata.DistSQLExecMode) {
	m.data.DistSQLMode = val
}

func (m *sessionDataMutator) SetForceSavepointRestart(val bool) {
	m.data.ForceSavepointRestart = val
}

func (m *sessionDataMutator) SetForceSplitAt(val bool) {
	m.data.ForceSplitAt = val
}

func (m *sessionDataMutator) SetZigzagJoinEnabled(val bool) {
	m.data.ZigzagJoinEnabled = val
}

func (m *sessionDataMutator) SetReorderJoinsLimit(val int) {
	m.data.ReorderJoinsLimit = val
}

func (m *sessionDataMutator) SetVectorize(val sessiondata.VectorizeExecMode) {
	m.data.VectorizeMode = val
}

func (m *sessionDataMutator) SetVectorizeRowCountThreshold(val uint64) {
	m.data.VectorizeRowCountThreshold = val
}

func (m *sessionDataMutator) SetOptimizerFKs(val bool) {
	m.data.OptimizerFKs = val
}

func (m *sessionDataMutator) SetSerialNormalizationMode(val sessiondata.SerialNormalizationMode) {
	m.data.SerialNormalizationMode = val
}

func (m *sessionDataMutator) SetSafeUpdates(val bool) {
	m.data.SafeUpdates = val
}

func (m *sessionDataMutator) SetSearchPath(val sessiondata.SearchPath) {
	m.data.SearchPath = val
}

func (m *sessionDataMutator) SetLocation(loc *time.Location) {
	m.data.DataConversion.Location = loc
}

func (m *sessionDataMutator) SetReadOnly(val bool) {
	m.setCurTxnReadOnly(val)
}

func (m *sessionDataMutator) SetStmtTimeout(timeout time.Duration) {
	m.data.StmtTimeout = timeout
}

func (m *sessionDataMutator) SetAllowPrepareAsOptPlan(val bool) {
	m.data.AllowPrepareAsOptPlan = val
}

func (m *sessionDataMutator) SetSaveTablesPrefix(prefix string) {
	m.data.SaveTablesPrefix = prefix
}

func (m *sessionDataMutator) SetTempTablesEnabled(val bool) {
	m.data.TempTablesEnabled = val
}

// RecordLatestSequenceValue records that value to which the session incremented
// a sequence.
func (m *sessionDataMutator) RecordLatestSequenceVal(seqID uint32, val int64) {
	m.data.SequenceState.RecordValue(seqID, val)
}

type sqlStatsCollector struct {
	// sqlStats tracks per-application statistics for all applications on each
	// node.
	sqlStats *sqlStats
	// appStats track per-application SQL usage statistics. This is a pointer
	// into sqlStats set as the session's current app.
	appStats *appStats
	// phaseTimes tracks session-level phase times.
	phaseTimes phaseTimes
}

// newSQLStatsCollector creates an instance of sqlStatsCollector. Note that
// phaseTimes is an array, not a slice, so this performs a copy-by-value.
func newSQLStatsCollector(
	sqlStats *sqlStats, appStats *appStats, phaseTimes *phaseTimes,
) *sqlStatsCollector {
	return &sqlStatsCollector{
		sqlStats:   sqlStats,
		appStats:   appStats,
		phaseTimes: *phaseTimes,
	}
}

// recordStatement records stats for one statement. samplePlanDescription can
// be nil, as these are only sampled periodically per unique fingerprint.
func (s *sqlStatsCollector) recordStatement(
	stmt *Statement,
	samplePlanDescription *roachpb.ExplainTreePlanNode,
	distSQLUsed bool,
	optUsed bool,
	implicitTxn bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
	bytesRead, rowsRead int64,
) {
	s.appStats.recordStatement(
		stmt, samplePlanDescription, distSQLUsed, optUsed, implicitTxn, automaticRetryCount, numRows, err,
		parseLat, planLat, runLat, svcLat, ovhLat, bytesRead, rowsRead)
}

// recordTransaction records stats for one transaction.
func (s *sqlStatsCollector) recordTransaction(txnTimeSec float64, ev txnEvent, implicit bool) {
	s.appStats.recordTransaction(txnTimeSec, ev, implicit)
}

func (s *sqlStatsCollector) reset(sqlStats *sqlStats, appStats *appStats, phaseTimes *phaseTimes) {
	*s = sqlStatsCollector{
		sqlStats:   sqlStats,
		appStats:   appStats,
		phaseTimes: *phaseTimes,
	}
}
