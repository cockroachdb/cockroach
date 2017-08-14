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

package cluster

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/time/rate"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// ValidateEnterpriseLicense is the validator for the enterprise license cluster
// setting. It is overridden on import by the `licenseccl` package. For pure OSS
// builds, returns an error for all nonempty strings.
var ValidateEnterpriseLicense = func(s string) error {
	if s != "" {
		return errors.New("not available in the pure OpenSource version of CockroachDB")
	}
	return nil
}

// BulkIOWriteLimiterBurst is the burst for the BulkIOWriteLimiter cluster setting.
const BulkIOWriteLimiterBurst = 2 * 1024 * 1024 // 2MB

// DebugRemoteMode controls who can access /debug/requests.
type DebugRemoteMode string

const (
	// DebugRemoteOff disallows access to /debug/requests.
	DebugRemoteOff DebugRemoteMode = "off"
	// DebugRemoteLocal allows only host-local access to /debug/requests.
	DebugRemoteLocal DebugRemoteMode = "local"
	// DebugRemoteAny allows all access to /debug/requests.
	DebugRemoteAny DebugRemoteMode = "any"
)

// DistSQLExecMode controls if and when the Executor uses DistSQL.
type DistSQLExecMode int64

const (
	// DistSQLOff means that we never use distSQL.
	DistSQLOff DistSQLExecMode = iota
	// DistSQLAuto means that we automatically decide on a case-by-case basis if
	// we use distSQL.
	DistSQLAuto
	// DistSQLOn means that we use distSQL for queries that are supported.
	DistSQLOn
	// DistSQLAlways means that we only use distSQL; unsupported queries fail.
	DistSQLAlways
)

func (m DistSQLExecMode) String() string {
	switch m {
	case DistSQLOff:
		return "off"
	case DistSQLAuto:
		return "auto"
	case DistSQLOn:
		return "on"
	case DistSQLAlways:
		return "always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// DistSQLExecModeFromString converts a string into a DistSQLExecMode
func DistSQLExecModeFromString(val string) DistSQLExecMode {
	switch strings.ToUpper(val) {
	case "OFF":
		return DistSQLOff
	case "AUTO":
		return DistSQLAuto
	case "ON":
		return DistSQLOn
	case "ALWAYS":
		return DistSQLAlways
	default:
		panic(fmt.Sprintf("unknown DistSQL mode %s", val))
	}
}

// TracingSettings is the subset of ClusterSettings affecting tracing.
type TracingSettings struct {
	EnableNetTrace  *settings.BoolSetting
	LightstepToken  *settings.StringSetting
	ZipkinCollector *settings.StringSetting

	Tracer *tracing.Tracer
}

type tracingReconfigurationOptions struct {
	ts TracingSettings
}

var _ tracing.ReconfigurationOptions = tracingReconfigurationOptions{}

func (t tracingReconfigurationOptions) EnableNetTrace() bool {
	return t.ts.EnableNetTrace.Get()
}

func (t tracingReconfigurationOptions) LightstepToken() string {
	return t.ts.LightstepToken.Get()
}

func (t tracingReconfigurationOptions) ZipkinAddr() string {
	return t.ts.ZipkinCollector.Get()
}

// ReportingSettings is the subset of ClusterSettings affecting crash and
// diagnostics reporting.
type ReportingSettings struct {
	DiagnosticsReportingEnabled *settings.BoolSetting
	CrashReports                *settings.BoolSetting
	DiagnosticsMetricsEnabled   *settings.BoolSetting

	// TODO(dt): this should be split from the report interval.
	// statsResetFrequency = settings.RegisterDurationSetting(
	// 	"sql.metrics.statement_details.reset_interval",
	// 	"interval at which the collected statement statistics should be reset",
	// 	time.Hour,
	// )
	DiagnosticReportFrequency *settings.DurationSetting
}

// HasDiagnosticsReportingEnabled returns true when the underlying cluster setting is true.
func (rs ReportingSettings) HasDiagnosticsReportingEnabled() bool {
	return rs.DiagnosticsReportingEnabled.Get()
}

// HasCrashReportsEnabled returns true when the underlying cluster setting is
// true.
func (rs ReportingSettings) HasCrashReportsEnabled() bool {
	return rs.CrashReports.Get()
}

// DistSQLSettings is the subset of ClusterSettings affecting DistSQL.
type DistSQLSettings struct {
	DistSQLUseTempStorage      *settings.BoolSetting
	DistSQLUseTempStorageSorts *settings.BoolSetting
	DistSQLUseTempStorageJoins *settings.BoolSetting
	DistributeIndexJoin        *settings.BoolSetting
	PlanMergeJoins             *settings.BoolSetting
}

// SQLStatsSettings is the subset of ClusterSettings affecting SQL statistics
// collection.
type SQLStatsSettings struct {
	StmtStatsEnable                    *settings.BoolSetting
	SQLStatsCollectionLatencyThreshold *settings.DurationSetting
	DumpStmtStatsToLogBeforeReset      *settings.BoolSetting
}

// SQLSessionSettings is the subset of ClusterSettings affecting SQL
// sessions.
type SQLSessionSettings struct {
	TraceTxnThreshold           *settings.DurationSetting
	TraceSessionEventLogEnabled *settings.BoolSetting
	LogStatementsExecuteEnabled *settings.BoolSetting
	DistSQLClusterExecMode      *settings.EnumSetting
}

// RocksDBSettings is the subset of ClusterSettings affecting RocksDB
// instances.
type RocksDBSettings struct {
	MinWALSyncInterval *settings.DurationSetting
}

// RebalancingSettings is the subset of ClusterSettings affecting
// rebalancing.
type RebalancingSettings struct {
	EnableLoadBasedLeaseRebalancing *settings.BoolSetting
	LeaseRebalancingAggressiveness  *settings.FloatSetting
	EnableStatsBasedRebalancing     *settings.BoolSetting
	StatRebalanceThreshold          *settings.FloatSetting
	RangeRebalanceThreshold         *settings.FloatSetting

	TimeUntilStoreDead *settings.DurationSetting
}

// StorageSettings is the subset of ClusterSettings affecting the storage
// layer.
type StorageSettings struct {
	SyncRaftLog    *settings.BoolSetting
	MaxCommandSize *settings.ByteSizeSetting
	GCBatchSize    *settings.IntSetting

	BulkIOWriteLimit   *settings.ByteSizeSetting
	BulkIOWriteLimiter *rate.Limiter

	RebalanceSnapshotRate       *settings.ByteSizeSetting
	RecoverySnapshotRate        *settings.ByteSizeSetting
	DeclinedReservationsTimeout *settings.DurationSetting
	FailedReservationsTimeout   *settings.DurationSetting
	ImportBatchSize             *settings.ByteSizeSetting
	AddSSTableEnabled           *settings.BoolSetting
	MaxIntents                  *settings.IntSetting
}

// UISettings is the subset of ClusterSettings affecting the UI.
type UISettings struct {
	WebSessionTimeout *settings.DurationSetting
	DebugRemote       *settings.StringSetting
}

// CCLSettings is the subset of ClusterSettings affecting
// enterprise-related functionality.
type CCLSettings struct {
	EnterpriseLicense   *settings.StringSetting
	ClusterOrganization *settings.StringSetting
}

// Settings is the collection of cluster settings. For a running CockroachDB
// node, there is a single instance of ClusterSetting which is shared across all
// of its components.
//
// The Version setting deserves an individual explanantion. During the node
// startup sequence, an initial version (persisted to the engines) is read and
// passed to InitializeVersion(). It is only after that that the Version field
// of this struct is ready for use (i.e. Version() and IsActive() can be
// called). In turn, the node usually registers itself as a callback to be
// notified of any further updates to the setting, which are then persisted.
//
// This dance is necessary because we cannot determine a safe default value for
// the version setting without looking at what's been persisted: The setting
// specifies the minimum binary version we have to expect to be in a mixed
// cluster with. We can't assume this binary's MinimumSupportedVersion as we
// could've started up earlier and enabled features that are not actually
// compatible with that version; we can't assume it's our binary's ServerVersion
// as that would enable features that may trip up older versions running in the
// same cluster. Hence, only once we get word of the "safe" version to use can
// we allow moving parts that actually need to know what's going on.
//
// Additionally, whenever the version changes, we want to persist that update to
// wherever the caller to InitializeVersion() got the initial version from
// (typically a collection of `engine.Engine`s), which the caller will do by
// registering itself via `(*Setting).Version.OnChange()`, which is invoked
// *before* exposing the new version to callers of `IsActive()` and `Version()`.
//
// For testing or one-off situations in which a ClusterSetting is needed, but
// cluster settings don't play a crucial role, MakeTestingClusterSetting() is
// provided; it is pre-initialized to the binary's ServerVersion.
type Settings struct {
	// Manual defaults to false. If set, lets this ClusterSetting's MakeUpdater
	// method return a dummy updater that simply throws away all values. This is
	// for use in tests for which manual control is desired.
	Manual atomic.Value // bool
	// A Registry populated with all of the individual cluster settings.
	settings.Registry

	TracingSettings
	ReportingSettings
	RocksDBSettings
	RebalancingSettings
	StorageSettings
	SQLStatsSettings
	SQLSessionSettings
	DistSQLSettings
	UISettings
	CCLSettings

	Version ExposedClusterVersion
}

const keyVersionSetting = "version"

// InitializeVersion initializes the Version field of this setting. Before this
// method has been called, usage of the Version field is illegal and leads to a
// fatal error.
func (s *Settings) InitializeVersion(cv ClusterVersion) error {
	b, err := cv.Marshal()
	if err != nil {
		return err
	}
	// Note that we don't call `updater.ResetRemaining()`.
	updater := settings.NewUpdater(s.Registry)
	if err := updater.Set(keyVersionSetting, string(b), s.Version.version.Typ()); err != nil {
		return err
	}
	s.Version.baseVersion.Store(&cv)
	return nil
}

// An ExposedClusterVersion exposes a cluster-wide minimum version which is
// assumed to be supported by all nodes. This in turn allows features which are
// incompatible with older versions to be used safely.
type ExposedClusterVersion struct {
	baseVersion atomic.Value // stores *ClusterVersion
	version     *settings.StateMachineSetting
	cb          func(ClusterVersion)
}

// OnChange registers (a single) callback that will be invoked whenever the
// cluster version changes. The new cluster version will only become "visible"
// after the callback has returned.
//
// The callback can be set at most once.
func (ecv *ExposedClusterVersion) OnChange(f func(cv ClusterVersion)) {
	if ecv.cb != nil {
		log.Fatal(context.TODO(), "cannot set callback twice")
	}
	ecv.cb = f
}

// Version returns the minimum cluster version the caller may assume is in
// effect. It must not be called until the setting has been initialized.
func (ecv *ExposedClusterVersion) Version() ClusterVersion {
	v := *ecv.baseVersion.Load().(*ClusterVersion)
	if (v == ClusterVersion{}) {
		log.Fatal(context.Background(), "Version() was called before having been initialized")
	}
	return v
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (ecv *ExposedClusterVersion) IsActive(v roachpb.Version) bool {
	return !ecv.Version().UseVersion.Less(v)
}

// MakeTestingClusterSettings returns a Settings object that has had its version
// initialized to BootstrapVersion().
func MakeTestingClusterSettings() *Settings {
	st := MakeClusterSettings()
	// Initialize with all features enabled.
	if err := st.InitializeVersion(BootstrapVersion()); err != nil {
		log.Fatalf(context.TODO(), "unable to initialize version: %s", err)
	}
	return st
}

// MakeClusterSettings makes a new ClusterSettings object. Note that by default,
// the Manual field is false, that is, an Updater made from the ClusterSetting
// is a NoopUpdater. For a "real" non-testing server, this field must be set to
// true or the settings won't be updated when the persisted settings table is
// updated.
func MakeClusterSettings() *Settings {
	var s Settings
	r := settings.NewRegistry()
	s.Registry = r

	// Initialize the setting. Note that it starts out with the zero cluster
	// version, for which the transformer accepts any new version. After that,
	// it'll only accept "valid bumps". We use this to initialize the variable
	// lazily, after we have read the current version from the engines. After
	// that, updates come from Gossip and need to be compatible with the engine
	// version.
	s.Version.baseVersion.Store(&ClusterVersion{})
	s.Version.version = r.RegisterStateMachineSetting(keyVersionSetting,
		"set the active cluster version in the format '<major>.<minor>'.", // hide optional `-<unstable>`
		versionTransformer(func() ClusterVersion {
			return *s.Version.baseVersion.Load().(*ClusterVersion)
		}),
	).OnChange(func() {
		_, obj, err := s.Version.version.Transformer([]byte(s.Version.version.Get()), nil)
		if err != nil {
			log.Fatal(context.Background(), err)
		}
		newV := *((*ClusterVersion)(obj.(*stringedVersion)))

		// Call callback before exposing the new version to callers of
		// IsActive() and Version(). Don't do this if the new version is
		// trivial, which is the case as the setting is initialized.
		if (newV != ClusterVersion{}) && s.Version.cb != nil {
			s.Version.cb(newV)
		}
		s.Version.baseVersion.Store(&newV)
	})

	s.Tracer = tracing.NewTracer()

	tracingOnChange := func() {
		s.Tracer.Reconfigure(tracingReconfigurationOptions{s.TracingSettings})
	}

	s.EnableNetTrace = r.RegisterBoolSetting(
		"trace.debug.enable",
		"if set, traces for recent requests can be seen in the /debug page",
		false,
	).OnChange(tracingOnChange)

	s.LightstepToken = r.RegisterStringSetting(
		"trace.lightstep.token",
		"if set, traces go to Lightstep using this token",
		envutil.EnvOrDefaultString("COCKROACH_TEST_LIGHTSTEP_TOKEN", ""),
	).OnChange(tracingOnChange)

	s.ZipkinCollector = r.RegisterStringSetting(
		"trace.zipkin.collector",
		"if set, traces go to the given Zipkin instance (example: '127.0.0.1:9411'); ignored if trace.lightstep.token is set.",
		envutil.EnvOrDefaultString("COCKROACH_TEST_ZIPKIN_COLLECTOR", ""),
	).OnChange(tracingOnChange)

	crashReportsOnChange := func() {
		f := log.ReportingSettings(s.ReportingSettings)
		log.ReportingSettingsSingleton.Store(&f)
	}

	// DiagnosticsReportingEnabled wraps "diagnostics.reporting.enabled".
	//
	// "diagnostics.reporting.enabled" enables reporting of metrics related to a
	// node's storage (number, size and health of ranges) back to CockroachDB.
	// Collecting this data from production clusters helps us understand and improve
	// how our storage systems behave in real-world use cases.
	//
	// Note: while the setting itself is actually defined with a default value of
	// `false`, it is usually automatically set to `true` when a cluster is created
	// (or is migrated from a earlier beta version). This can be prevented with the
	// env var COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING.
	//
	// Doing this, rather than just using a default of `true`, means that a node
	// will not errantly send a report using a default before loading settings.
	s.DiagnosticsReportingEnabled = r.RegisterBoolSetting(
		"diagnostics.reporting.enabled",
		"enable reporting diagnostic metrics to cockroach labs",
		false,
	).OnChange(crashReportsOnChange)

	s.CrashReports = r.RegisterBoolSetting(
		"diagnostics.reporting.send_crash_reports",
		"send crash and panic reports",
		true,
	).OnChange(crashReportsOnChange)

	// maxIntents is the limit for the number of intents that can be
	// written in a single transaction. All intents used by a transaction
	// must be included in the EndTransactionRequest, and processing a
	// large EndTransactionRequest currently consumes a larage amount of
	// memory. Limit the number of intents to keep this from causing the
	// server to run out of memory.
	s.MaxIntents = r.RegisterIntSetting(
		"kv.transaction.max_intents",
		"maximum number of write intents allowed for a KV transaction", 100000)

	s.MinWALSyncInterval = r.RegisterDurationSetting(
		"rocksdb.min_wal_sync_interval",
		"minimum duration between syncs of the RocksDB WAL",
		0*time.Millisecond)

	// EnableLoadBasedLeaseRebalancing controls whether lease rebalancing is done
	// via the new heuristic based on request load and latency or via the simpler
	// approach that purely seeks to balance the number of leases per node evenly.
	s.EnableLoadBasedLeaseRebalancing = r.RegisterBoolSetting(
		"kv.allocator.load_based_lease_rebalancing.enabled",
		"set to enable rebalancing of range leases based on load and latency",
		true)

	// LeaseRebalancingAggressiveness enables users to tweak how aggressive their
	// cluster is at moving leases towards the localities where the most requests
	// are coming from. Settings lower than 1.0 will make the system less
	// aggressive about moving leases toward requests than the default, while
	// settings greater than 1.0 will cause more aggressive placement.
	//
	// Setting this to 0 effectively disables load-based lease rebalancing, and
	// settings less than 0 are disallowed.
	s.LeaseRebalancingAggressiveness = r.RegisterNonNegativeFloatSetting(
		"kv.allocator.lease_rebalancing_aggressiveness",
		"set greater than 1.0 to rebalance leases toward load more aggressively, "+
			"or between 0 and 1.0 to be more conservative about rebalancing leases",
		1.0)

	// EnableStatsBasedRebalancing controls whether range rebalancing takes
	// additional variables such as write load and disk usage into account.
	// If disabled, rebalancing is done purely based on replica count.
	s.EnableStatsBasedRebalancing = r.RegisterBoolSetting(
		"kv.allocator.stat_based_rebalancing.enabled",
		"set to enable rebalancing of range replicas based on write load and disk usage",
		true)

	// rangeRebalanceThreshold is the minimum ratio of a store's range count to
	// the mean range count at which that store is considered overfull or underfull
	// of ranges.
	s.RangeRebalanceThreshold = r.RegisterNonNegativeFloatSetting(
		"kv.allocator.range_rebalance_threshold",
		"minimum fraction away from the mean a store's range count can be before it is considered overfull or underfull",
		0.05)

	// StatRebalanceThreshold is the the same as rangeRebalanceThreshold, but for
	// statistics other than range count. This should be larger than
	// rangeRebalanceThreshold because certain stats (like keys written per second)
	// are inherently less stable and thus we need to be a little more forgiving to
	// avoid thrashing.
	//
	// Note that there isn't a ton of science behind this number, but setting it
	// to .05 and .1 were shown to cause some instability in clusters without load
	// on them.
	//
	// TODO(a-robinson): Should disk usage be held to a higher standard than this?
	s.StatRebalanceThreshold = r.RegisterNonNegativeFloatSetting(
		"kv.allocator.stat_rebalance_threshold",
		"minimum fraction away from the mean a store's stats (like disk usage or writes per second) can be before it is considered overfull or underfull",
		0.20)

	s.SyncRaftLog = r.RegisterBoolSetting(
		"kv.raft_log.synchronize",
		"set to true to synchronize on Raft log writes to persistent storage",
		true)

	s.MaxCommandSize = r.RegisterByteSizeSetting(
		"kv.raft.command.max_size",
		"maximum size of a raft command",
		64<<20)

	// gcBatchSize controls the amount of work done in a single pass of
	// MVCC GC. Setting this too high may block the range for too long
	// (especially a risk in the system ranges), while setting it too low
	// may allow ranges to grow too large if we are unable to keep up with
	// the amount of garbage generated.
	s.GCBatchSize = r.RegisterIntSetting("kv.gc.batch_size",
		"maximum number of keys in a batch for MVCC garbage collection",
		100000)

	s.BulkIOWriteLimit = r.RegisterByteSizeSetting(
		"kv.bulk_io_write.max_rate",
		"the rate limit (bytes/sec) to use for writes to disk on behalf of bulk io ops",
		math.MaxInt64,
	)

	// TODO(dan): This limiting should be per-store and shared between any
	// operations that need lots of disk throughput.
	s.BulkIOWriteLimiter = rate.NewLimiter(rate.Limit(s.BulkIOWriteLimit.Get()), BulkIOWriteLimiterBurst)

	s.BulkIOWriteLimit.OnChange(func() {
		s.BulkIOWriteLimiter.SetLimit(rate.Limit(s.BulkIOWriteLimit.Get()))
	})

	s.RebalanceSnapshotRate = r.RegisterByteSizeSetting(
		"kv.snapshot_rebalance.max_rate",
		"the rate limit (bytes/sec) to use for rebalance snapshots",
		envutil.EnvOrDefaultBytes("COCKROACH_PREEMPTIVE_SNAPSHOT_RATE", 2<<20))
	s.RecoverySnapshotRate = r.RegisterByteSizeSetting(
		"kv.snapshot_recovery.max_rate",
		"the rate limit (bytes/sec) to use for recovery snapshots",
		envutil.EnvOrDefaultBytes("COCKROACH_RAFT_SNAPSHOT_RATE", 8<<20))

	// declinedReservationsTimeout needs to be non-zero to prevent useless retries
	// in the replicateQueue.process() retry loop.
	s.DeclinedReservationsTimeout = r.RegisterNonNegativeDurationSetting(
		"server.declined_reservation_timeout",
		"the amount of time to consider the store throttled for up-replication after a reservation was declined",
		1*time.Second,
	)

	s.FailedReservationsTimeout = r.RegisterNonNegativeDurationSetting(
		"server.failed_reservation_timeout",
		"the amount of time to consider the store throttled for up-replication after a failed reservation call",
		5*time.Second,
	)

	s.DistSQLUseTempStorage = r.RegisterBoolSetting(
		"sql.defaults.distsql.tempstorage",
		"set to true to enable use of disk for larger distributed sql queries",
		false,
	)

	s.DistSQLUseTempStorageSorts = r.RegisterBoolSetting(
		"sql.defaults.distsql.tempstorage.sorts",
		"set to true to enable use of disk for distributed sql sorts. sql.defaults.distsql.tempstorage must be true",
		true,
	)

	s.DistSQLUseTempStorageJoins = r.RegisterBoolSetting(
		"sql.defaults.distsql.tempstorage.joins",
		"set to true to enable use of disk for distributed sql joins. sql.defaults.distsql.tempstorage must be true",
		true,
	)

	// StmtStatsEnable determines whether to collect per-statement
	// statistics.
	s.StmtStatsEnable = r.RegisterBoolSetting(
		"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
	)

	// SQLStatsCollectionLatencyThreshold specifies the minimum amount of time
	// consumed by a SQL statement before it is collected for statistics reporting.
	s.SQLStatsCollectionLatencyThreshold = r.RegisterDurationSetting(
		"sql.metrics.statement_details.threshold",
		"minimum execution time to cause statistics to be collected",
		0,
	)

	s.DumpStmtStatsToLogBeforeReset = r.RegisterBoolSetting(
		"sql.metrics.statement_details.dump_to_logs",
		"dump collected statement statistics to node logs when periodically cleared",
		false,
	)

	// If true, for index joins  we instantiate a join reader on every node that
	// has a stream (usually from a table reader). If false, there is a single join
	// reader.
	s.DistributeIndexJoin = r.RegisterBoolSetting(
		"sql.distsql.distribute_index_joins",
		"if set, for index joins we instantiate a join reader on every node that has a "+
			"stream; if not set, we use a single join reader",
		true,
	)

	s.PlanMergeJoins = r.RegisterBoolSetting(
		"sql.distsql.merge_joins.enabled",
		"if set, we plan merge joins when possible",
		true,
	)

	// traceTxnThreshold can be used to log SQL transactions that take
	// longer than duration to complete. For example, traceTxnThreshold=1s
	// will log the trace for any transaction that takes 1s or longer. To
	// log traces for all transactions use traceTxnThreshold=1ns. Note
	// that any positive duration will enable tracing and will slow down
	// all execution because traces are gathered for all transactions even
	// if they are not output.
	s.TraceTxnThreshold = r.RegisterDurationSetting(
		"sql.trace.txn.enable_threshold",
		"duration beyond which all transactions are traced (set to 0 to disable)", 0)

	// traceSessionEventLogEnabled can be used to enable the event log
	// that is normally kept for every SQL connection. The event log has a
	// non-trivial performance impact and also reveals SQL statements
	// which may be a privacy concern.
	s.TraceSessionEventLogEnabled = r.RegisterBoolSetting(
		"sql.trace.session_eventlog.enabled",
		"set to true to enable session tracing", false)

	// logStatementsExecuteEnabled causes the Executor to log executed
	// statements and, if any, resulting errors.
	s.LogStatementsExecuteEnabled = r.RegisterBoolSetting(
		"sql.trace.log_statement_execute",
		"set to true to enable logging of executed statements", false)

	// DistSQLClusterExecMode controls the cluster default for when DistSQL is used.
	s.DistSQLClusterExecMode = r.RegisterEnumSetting(
		"sql.defaults.distsql",
		"Default distributed SQL execution mode",
		"Auto",
		map[int64]string{
			int64(DistSQLOff):  "Off",
			int64(DistSQLAuto): "Auto",
			int64(DistSQLOn):   "On",
		},
	)

	s.WebSessionTimeout = r.RegisterNonNegativeDurationSetting(
		"server.web_session_timeout",
		"the duration that a newly created web session will be valid",
		7*24*time.Hour)

	s.TimeUntilStoreDead = r.RegisterNonNegativeDurationSetting(
		"server.time_until_store_dead",
		"the time after which if there is no new gossiped information about a store, it is considered dead",
		5*time.Minute)

	s.DebugRemote = r.RegisterValidatedStringSetting(
		"server.remote_debugging.mode",
		"set to enable remote debugging, localhost-only or disable (any, local, off)",
		"local",
		func(s string) error {
			switch DebugRemoteMode(strings.ToLower(s)) {
			case DebugRemoteOff, DebugRemoteLocal, DebugRemoteAny:
				return nil
			default:
				return errors.Errorf("invalid mode: '%s'", s)
			}
		},
	)
	s.ClusterOrganization = r.RegisterStringSetting("cluster.organization", "organization name", "")

	// FIXME(tschottdorf): should be NonNegative?
	s.DiagnosticReportFrequency = r.RegisterDurationSetting(
		"diagnostics.reporting.interval",
		"interval at which diagnostics data should be reported",
		time.Hour,
	)

	s.DiagnosticsMetricsEnabled = r.RegisterBoolSetting(
		"diagnostics.reporting.report_metrics",
		"enable collection and reporting diagnostic metrics to cockroach labs",
		true,
	)

	s.ImportBatchSize = r.RegisterByteSizeSetting("kv.import.batch_size", "", 2<<20)
	s.ImportBatchSize.Hide()

	s.AddSSTableEnabled = r.RegisterBoolSetting(
		"kv.import.experimental_addsstable.enabled",
		"set to true to use the AddSSTable command in Import or false to use WriteBatch",
		true,
	)
	s.AddSSTableEnabled.Hide()

	s.EnterpriseLicense = r.RegisterValidatedStringSetting(
		"enterprise.license", "the encoded cluster license", "",
		ValidateEnterpriseLicense)
	s.EnterpriseLicense.Hide()
	return &s
}

// MakeUpdater returns a new Updater, pre-alloced to the registry size. Note
// that if the Setting has the Manual flag set, this Updater simply ignores all
// updates.
func (s *Settings) MakeUpdater() settings.Updater {
	if isManual, ok := s.Manual.Load().(bool); ok && isManual {
		return &settings.NoopUpdater{}
	}
	return settings.NewUpdater(s.Registry)
}

type stringedVersion ClusterVersion

func (sv *stringedVersion) String() string {
	if sv == nil {
		sv = &stringedVersion{}
	}
	return sv.MinimumVersion.String()
}

func versionTransformer(defaultVersion func() ClusterVersion) settings.TransformerFn {
	return func(curRawProto []byte, versionBump *string) (newRawProto []byte, versionStringer interface{}, _ error) {
		defer func() {
			if versionStringer != nil {
				versionStringer = (*stringedVersion)(versionStringer.(*ClusterVersion))
			}
		}()
		var oldV ClusterVersion

		// If no old value supplied, fill in the default.
		if curRawProto == nil {
			oldV = defaultVersion()
			var err error
			curRawProto, err = oldV.Marshal()
			if err != nil {
				return nil, nil, err
			}
		}

		if err := oldV.Unmarshal(curRawProto); err != nil {
			return nil, nil, err
		}
		if versionBump == nil {
			// Round-trip the existing value, but only if it passes sanity checks.
			b, err := oldV.Marshal()
			if err != nil {
				return nil, nil, err
			}
			return b, &oldV, err
		}

		// We have a new proposed update to the value, validate it.
		minVersion, err := roachpb.ParseVersion(*versionBump)
		if err != nil {
			return nil, nil, err
		}
		newV := oldV
		newV.UseVersion = minVersion
		newV.MinimumVersion = minVersion

		if minVersion.Less(oldV.MinimumVersion) {
			return nil, nil, errors.Errorf("cannot downgrade from %s to %s", oldV.MinimumVersion, minVersion)
		}

		if oldV != (ClusterVersion{}) && !oldV.MinimumVersion.CanBump(minVersion) {
			return nil, nil, errors.Errorf("cannot upgrade directly from %s to %s", oldV.MinimumVersion, minVersion)
		}

		if ServerVersion.Less(minVersion) {
			// TODO(tschottdorf): also ask gossip about other nodes.
			return nil, nil, errors.Errorf("cannot upgrade to %s: node running %s",
				minVersion, ServerVersion)
		}

		b, err := newV.Marshal()
		return b, &newV, err
	}
}
