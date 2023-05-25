// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/inspectz/inspectzpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatsconsumer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatskvaccessor"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/server/tracedumper"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfiglimiter"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsplitter"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesccache"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/consistencychecker"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob/gcjobnotifier"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/rangeprober"
	"github.com/cockroachdb/cockroach/pkg/sql/scheduledlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slprovider"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilegecache"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradecluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrademanager"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/collector"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/service"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/marusama/semaphore"
	"github.com/nightlyone/lockfile"
	"google.golang.org/grpc"
)

// SQLServer encapsulates the part of a CRDB server that is dedicated to SQL
// processing. All SQL commands are reduced to primitive operations on the
// lower-level KV layer. Multi-tenant installations of CRDB run zero or more
// standalone SQLServer instances per tenant (the KV layer is shared across all
// tenants).
type SQLServer struct {
	ambientCtx       log.AmbientContext
	stopper          *stop.Stopper
	stopTrigger      *stopTrigger
	sqlIDContainer   *base.SQLIDContainer
	pgServer         *pgwire.Server
	distSQLServer    *distsql.ServerImpl
	execCfg          *sql.ExecutorConfig
	cfg              *BaseConfig
	internalExecutor *sql.InternalExecutor
	internalDB       descs.DB
	leaseMgr         *lease.Manager
	tracingService   *service.Service
	tenantConnect    kvtenant.Connector
	// sessionRegistry can be queried for info on running SQL sessions. It is
	// shared between the sql.Server and the statusServer.
	sessionRegistry        *sql.SessionRegistry
	closedSessionCache     *sql.ClosedSessionCache
	jobRegistry            *jobs.Registry
	statsRefresher         *stats.Refresher
	temporaryObjectCleaner *sql.TemporaryObjectCleaner
	internalMemMetrics     sql.MemoryMetrics
	// sqlMemMetrics are used to track memory usage of sql sessions.
	sqlMemMetrics                  sql.MemoryMetrics
	stmtDiagnosticsRegistry        *stmtdiagnostics.Registry
	sqlLivenessSessionID           sqlliveness.SessionID
	sqlLivenessProvider            sqlliveness.Provider
	sqlInstanceReader              *instancestorage.Reader
	sqlInstanceStorage             *instancestorage.Storage
	metricsRegistry                *metric.Registry
	diagnosticsReporter            *diagnostics.Reporter
	spanconfigMgr                  *spanconfigmanager.Manager
	spanconfigSQLTranslatorFactory *spanconfigsqltranslator.Factory
	spanconfigSQLWatcher           *spanconfigsqlwatcher.SQLWatcher
	settingsWatcher                *settingswatcher.SettingsWatcher

	systemConfigWatcher *systemconfigwatcher.Cache

	isMeta1Leaseholder func(context.Context, hlc.ClockTimestamp) (bool, error)

	// isReady is the health status of the node. When true, the node is healthy;
	// load balancers and connection management tools treat the node as "ready".
	// When false, the node is unhealthy or "not ready", with load balancers and
	// connection management tools learning this status from health checks.
	// This is set to true when the server has started accepting client conns.
	isReady syncutil.AtomicBool

	// gracefulDrainComplete indicates when a graceful drain has
	// completed successfully. We use this to document cases where a
	// graceful drain did _not_ occur.
	gracefulDrainComplete syncutil.AtomicBool

	// internalDBMemMonitor is the memory monitor corresponding to the
	// InternalDB singleton. It only gets closed when
	// Server is closed. Every Executor created via the factory
	// uses this memory monitor.
	internalDBMemMonitor *mon.BytesMonitor

	// upgradeManager deals with cluster version upgrades on bootstrap and on
	// `set cluster setting version = <v>`.
	upgradeManager *upgrademanager.Manager

	// Tenant migration server for use in tenant tests.
	migrationServer *TenantMigrationServer
}

// sqlServerOptionalKVArgs are the arguments supplied to newSQLServer which are
// only available if the SQL server runs as part of a KV node.
//
// TODO(tbg): give all of these fields a wrapper that can signal whether the
// respective object is available. When it is not, return
// UnsupportedWithMultiTenancy.
type sqlServerOptionalKVArgs struct {
	// nodesStatusServer gives access to the NodesStatus service.
	nodesStatusServer serverpb.OptionalNodesStatusServer
	// Narrowed down version of *NodeLiveness. Used by jobs, DistSQLPlanner, and
	// upgrade manager.
	nodeLiveness optionalnodeliveness.Container
	// Gossip is relied upon by distSQLCfg (execinfra.ServerConfig), the executor
	// config, the DistSQL planner, the table statistics cache, the statements
	// diagnostics registry, and the lease manager.
	gossip gossip.OptionalGossip
	// To register blob and DistSQL servers.
	grpcServer *grpc.Server
	// For the temporaryObjectCleaner.
	isMeta1Leaseholder func(context.Context, hlc.ClockTimestamp) (bool, error)
	// DistSQL, lease management, and others want to know the node they're on.
	nodeIDContainer *base.SQLIDContainer

	// Used by backup/restore.
	externalStorage        cloud.ExternalStorageFactory
	externalStorageFromURI cloud.ExternalStorageFromURIFactory

	// The admission queue to use for SQLSQLResponseWork.
	sqlSQLResponseAdmissionQ *admission.WorkQueue

	// Used when creating and deleting tenant records.
	spanConfigKVAccessor spanconfig.KVAccessor
	// kvStores is used by crdb_internal builtins to access the stores on this
	// node.
	kvStoresIterator kvserverbase.StoresIterator

	// inspectzServer is used to power various crdb_internal vtables, exposing
	// the equivalent of /inspectz but through SQL.
	inspectzServer inspectzpb.InspectzServer
}

// sqlServerOptionalTenantArgs are the arguments supplied to newSQLServer which
// are only available if the SQL server runs as part of a standalone SQL node.
type sqlServerOptionalTenantArgs struct {
	tenantConnect      kvtenant.Connector
	spanLimiterFactory spanLimiterFactory

	promRuleExporter *metric.PrometheusRuleExporter
}

type sqlServerArgs struct {
	sqlServerOptionalKVArgs
	sqlServerOptionalTenantArgs

	*SQLConfig
	*BaseConfig

	stopper *stop.Stopper
	// stopTrigger is user by the sqlServer to signal requests to shut down the
	// server. The creator of the server is supposed to listen for such requests
	// and terminate the process.
	stopTrigger *stopTrigger

	// SQL uses the clock to assign timestamps to transactions, among many
	// other things.
	clock *hlc.Clock

	// The RuntimeStatSampler provides metrics data to the recorder.
	runtime *status.RuntimeStatSampler

	// DistSQL uses rpcContext to set up flows. Less centrally, the executor
	// also uses rpcContext in a number of places to learn whether the server
	// is running insecure, and to read the cluster name.
	rpcContext *rpc.Context

	// Used by DistSQLPlanner.
	nodeDescs kvcoord.NodeDescStore

	// Used by the executor config.
	systemConfigWatcher *systemconfigwatcher.Cache

	// Used by the span config reconciliation job.
	spanConfigAccessor spanconfig.KVAccessor

	// Used by the Key Visualizer job.
	keyVisServerAccessor *spanstatskvaccessor.SpanStatsKVAccessor

	// Used by DistSQLPlanner to dial KV nodes.
	nodeDialer *nodedialer.Dialer

	// Used by DistSQLPlanner to dial other pods in a multi-tenant environment.
	podNodeDialer *nodedialer.Dialer

	// SQL mostly uses the DistSender "wrapped" under a *kv.DB, but SQL also
	// uses range descriptors and leaseholders, which DistSender maintains,
	// for debugging and DistSQL planning purposes.
	distSender *kvcoord.DistSender

	// SQL uses KV, both for non-DistSQL and DistSQL execution.
	db *kv.DB

	// Various components want to register themselves with metrics.
	registry *metric.Registry

	// Recorder exposes metrics to the prometheus endpoint.
	recorder *status.MetricsRecorder

	// Used for SHOW/CANCEL QUERIE(S)/SESSION(S).
	sessionRegistry *sql.SessionRegistry

	// Used to store closed sessions.
	closedSessionCache *sql.ClosedSessionCache

	// Used to track the DistSQL flows currently running on this node but
	// initiated on behalf of other nodes.
	remoteFlowRunner *flowinfra.RemoteFlowRunner

	// KV depends on the internal executor, so we pass a pointer to an empty
	// struct in this configuration, which newSQLServer fills.
	//
	// TODO(tbg): make this less hacky.
	// TODO(ajwerner): Replace this entirely with the internalDB which follows.
	// it is no less hacky, but at least it removes some redundancy. In some ways
	// the internalDB is worse: the Executor() method cannot be used during server
	// startup while the internalDB is partially initialized.
	circularInternalExecutor *sql.InternalExecutor // empty initially

	// internalDB is to initialize an internal executor.
	internalDB *sql.InternalDB

	// Stores and deletes expired liveness sessions.
	sqlLivenessProvider sqlliveness.Provider

	// Stores and manages sql instance information.
	sqlInstanceReader *instancestorage.Reader
	// Low-level access to the system.sql_instances table, used for allocating
	// this server's instance ID.
	sqlInstanceStorage *instancestorage.Storage

	// The protected timestamps KV subsystem depends on this, so we pass a
	// pointer to an empty struct in this configuration, which newSQLServer
	// fills.
	circularJobRegistry *jobs.Registry

	// The executorConfig uses the provider.
	protectedtsProvider protectedts.Provider

	// Used to list activity (sessions, queries, contention, DistSQL flows) on
	// the node/cluster and cancel sessions/queries.
	sqlStatusServer serverpb.SQLStatusServer

	// Used to construct rangefeeds.
	rangeFeedFactory *rangefeed.Factory

	// Used to query status information useful for debugging on the server.
	tenantStatusServer serverpb.TenantStatusServer

	// Used for multi-tenant cost control (on the storage cluster side).
	tenantUsageServer multitenant.TenantUsageServer

	// Used for multi-tenant cost control (on the tenant side).
	costController multitenant.TenantSideCostController

	// monitorAndMetrics contains the return value of newRootSQLMemoryMonitor.
	monitorAndMetrics monitorAndMetrics

	// settingsStorage is an optional interface to drive storing of settings
	// data on disk to provide a fresh source of settings upon next startup.
	settingsStorage settingswatcher.Storage

	// grpc is the RPC service.
	grpc *grpcServer

	// eventsExporter communicates with the Observability Service.
	eventsExporter obs.EventsExporterInterface

	// externalStorageBuilder is the constructor for accesses to external
	// storage.
	externalStorageBuilder *externalStorageBuilder

	// admissionPacerFactory is used for elastic CPU control when performing
	// CPU intensive operations, such as CDC event encoding/decoding.
	admissionPacerFactory admission.PacerFactory

	// rangeDescIteratorFactory is used to construct iterators over range
	// descriptors.
	rangeDescIteratorFactory rangedesc.IteratorFactory

	// tenantTimeSeriesServer is used to make TSDB queries by the DB Console.
	tenantTimeSeriesServer *ts.TenantServer

	tenantCapabilitiesReader sql.SystemTenantOnly[tenantcapabilities.Reader]
}

type monitorAndMetrics struct {
	rootSQLMemoryMonitor *mon.BytesMonitor
	rootSQLMetrics       sql.BaseMemoryMetrics
}

type monitorAndMetricsOptions struct {
	memoryPoolSize          int64
	histogramWindowInterval time.Duration
	settings                *cluster.Settings
}

var vmoduleSetting = settings.RegisterStringSetting(
	settings.TenantWritable,
	"server.debug.default_vmodule",
	"vmodule string (ignored by any server with an explicit one provided at start)",
	"",
)

// newRootSQLMemoryMonitor returns a started BytesMonitor and corresponding
// metrics.
func newRootSQLMemoryMonitor(opts monitorAndMetricsOptions) monitorAndMetrics {
	rootSQLMetrics := sql.MakeBaseMemMetrics("root", opts.histogramWindowInterval)
	// We do not set memory monitors or a noteworthy limit because the children of
	// this monitor will be setting their own noteworthy limits.
	rootSQLMemoryMonitor := mon.NewMonitor(
		"root",
		mon.NewMemoryResourceWithErrorHint(
			"Consider increasing --max-sql-memory startup parameter.", /* hint */
		),
		rootSQLMetrics.CurBytesCount,
		rootSQLMetrics.MaxBytesHist,
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
		opts.settings,
	)
	// Set the limit to the memoryPoolSize. Note that this memory monitor also
	// serves as a parent for a memory monitor that accounts for memory used in
	// the KV layer at the same node.
	rootSQLMemoryMonitor.Start(
		context.Background(), nil, mon.NewStandaloneBudget(opts.memoryPoolSize))
	return monitorAndMetrics{
		rootSQLMemoryMonitor: rootSQLMemoryMonitor,
		rootSQLMetrics:       rootSQLMetrics,
	}
}

// stopperSessionEventListener implements slinstance.SessionEventListener and
// turns a session deletion event into a request to stop the server.
type stopperSessionEventListener struct {
	trigger *stopTrigger
}

var _ slinstance.SessionEventListener = &stopperSessionEventListener{}

// OnSessionDeleted implements the slinstance.SessionEventListener interface.
func (s *stopperSessionEventListener) OnSessionDeleted(
	ctx context.Context,
) (createAnotherSession bool) {
	s.trigger.signalStop(ctx,
		MakeShutdownRequest(ShutdownReasonFatalError, errors.New("sql liveness session deleted")))
	// Return false in order to prevent the sqlliveness loop from creating a new
	// session. We're shutting down the server and creating a new session would
	// only cause confusion.
	return false
}

type refreshInstanceSessionListener struct {
	cfg *sqlServerArgs
}

var _ slinstance.SessionEventListener = &stopperSessionEventListener{}

// OnSessionDeleted implements the slinstance.SessionEventListener interface.
func (r *refreshInstanceSessionListener) OnSessionDeleted(
	ctx context.Context,
) (createAnotherSession bool) {
	if err := r.cfg.stopper.RunAsyncTask(ctx, "refresh-instance-session", func(ctx context.Context) {
		for i := retry.StartWithCtx(ctx, retry.Options{MaxBackoff: time.Second * 5}); i.Next(); {
			select {
			case <-r.cfg.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			default:
			}
			nodeID, _ := r.cfg.nodeIDContainer.OptionalNodeID()
			s, err := r.cfg.sqlLivenessProvider.Session(ctx)
			if err != nil {
				log.Warningf(ctx, "failed to get new liveness session ID: %v", err)
				continue
			}
			if _, err := r.cfg.sqlInstanceStorage.CreateNodeInstance(
				ctx,
				s.ID(),
				s.Expiration(),
				r.cfg.AdvertiseAddr,
				r.cfg.SQLAdvertiseAddr,
				r.cfg.Locality,
				r.cfg.Settings.Version.BinaryVersion(),
				nodeID,
			); err != nil {
				log.Warningf(ctx, "failed to update instance with new session ID: %v", err)
				continue
			}
			return
		}
	}); err != nil {
		log.Errorf(ctx, "failed to run update of instance with new session ID: %v", err)
	}
	return true
}

// newSQLServer constructs a new SQLServer. The caller is responsible for
// listening to the server's ShutdownRequested() channel (which is the same as
// cfg.stopTrigger.C()) and stopping cfg.stopper when signaled.
func newSQLServer(ctx context.Context, cfg sqlServerArgs) (*SQLServer, error) {
	// NB: ValidateAddrs also fills in defaults.
	if err := cfg.Config.ValidateAddrs(ctx); err != nil {
		return nil, err
	}
	execCfg := &sql.ExecutorConfig{}
	codec := keys.MakeSQLCodec(cfg.SQLConfig.TenantID)
	if knobs := cfg.TestingKnobs.TenantTestingKnobs; knobs != nil {
		override := knobs.(*sql.TenantTestingKnobs).TenantIDCodecOverride
		if override != (roachpb.TenantID{}) {
			codec = keys.MakeSQLCodec(override)
		}
	}

	var jobAdoptionStopFile string
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory && spec.Path != "" {
			jobAdoptionStopFile = filepath.Join(spec.Path, jobs.PreventAdoptionFile)
			break
		}
	}

	if err := cfg.stopper.RunAsyncTask(ctx, "tracer-snapshots", func(context.Context) {
		cfg.Tracer.PeriodicSnapshotsLoop(&cfg.Settings.SV, cfg.stopper.ShouldQuiesce())
	}); err != nil {
		return nil, err
	}

	// Create trace service for inter-node sharing of inflight trace spans.
	tracingService := service.New(cfg.Tracer)
	tracingservicepb.RegisterTracingServer(cfg.grpcServer, tracingService)

	// If the node id is already populated, we only need to create a placeholder
	// instance provider without initializing the instance, since this is not a
	// SQL pod server.
	_, isMixedSQLAndKVNode := cfg.nodeIDContainer.OptionalNodeID()

	var settingsWatcher *settingswatcher.SettingsWatcher
	if codec.ForSystemTenant() {
		settingsWatcher = settingswatcher.New(
			cfg.clock, codec, cfg.Settings, cfg.rangeFeedFactory, cfg.stopper, cfg.settingsStorage,
		)
	} else {
		// Create the tenant settings watcher, using the tenant connector as the
		// overrides monitor.
		settingsWatcher = settingswatcher.NewWithOverrides(
			cfg.clock, codec, cfg.Settings, cfg.rangeFeedFactory, cfg.stopper, cfg.tenantConnect, cfg.settingsStorage,
		)
	}

	sqllivenessKnobs, _ := cfg.TestingKnobs.SQLLivenessKnobs.(*sqlliveness.TestingKnobs)
	var sessionEventsConsumer slinstance.SessionEventListener
	if !isMixedSQLAndKVNode {
		// For SQL pods, we want the process to shutdown when the session liveness
		// record is found to be deleted. This is because, if the session is
		// deleted, the instance ID used by this server may have been stolen by
		// another server, or it may be stolen in the future. This server shouldn't
		// use the instance ID anymore, and there's no mechanism for allocating a
		// new one after startup.
		sessionEventsConsumer = &stopperSessionEventListener{trigger: cfg.stopTrigger}
	} else {
		sessionEventsConsumer = &refreshInstanceSessionListener{cfg: &cfg}
	}
	cfg.sqlLivenessProvider = slprovider.New(
		cfg.AmbientCtx,
		cfg.stopper, cfg.clock, cfg.db, codec, cfg.Settings, settingsWatcher, sqllivenessKnobs, sessionEventsConsumer,
	)

	cfg.sqlInstanceStorage = instancestorage.NewStorage(
		cfg.db, codec, cfg.sqlLivenessProvider.CachedReader(), cfg.Settings,
		cfg.clock, cfg.rangeFeedFactory, settingsWatcher)

	cfg.sqlInstanceReader = instancestorage.NewReader(
		cfg.sqlInstanceStorage, cfg.sqlLivenessProvider.CachedReader(),
		cfg.stopper,
		cfg.db,
	)

	// We can't use the nodeDialer as the podNodeDialer unless we
	// are serving the system tenant despite the fact that we've
	// arranged for pod IDs and instance IDs to match since the
	// secondary tenant gRPC servers currently live on a different
	// port.
	canUseNodeDialerAsPodNodeDialer := isMixedSQLAndKVNode && codec.ForSystemTenant()
	if canUseNodeDialerAsPodNodeDialer {
		cfg.podNodeDialer = cfg.nodeDialer
	} else {
		// In a multi-tenant environment, use the sqlInstanceReader to resolve
		// SQL pod addresses.
		addressResolver := func(nodeID roachpb.NodeID) (net.Addr, error) {
			info, err := cfg.sqlInstanceReader.GetInstance(cfg.rpcContext.MasterCtx, base.SQLInstanceID(nodeID))
			if err != nil {
				return nil, errors.Wrapf(err, "unable to look up descriptor for n%d", nodeID)
			}
			return &util.UnresolvedAddr{AddressField: info.InstanceRPCAddr}, nil
		}
		cfg.podNodeDialer = nodedialer.New(cfg.rpcContext, addressResolver)
	}

	jobRegistry := cfg.circularJobRegistry
	{
		cfg.registry.AddMetricStruct(cfg.sqlLivenessProvider.Metrics())

		var jobsKnobs *jobs.TestingKnobs
		if cfg.TestingKnobs.JobsTestingKnobs != nil {
			jobsKnobs = cfg.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs)
		}

		td := tracedumper.NewTraceDumper(ctx, cfg.InflightTraceDirName, cfg.Settings)
		*jobRegistry = *jobs.MakeRegistry(
			ctx,
			cfg.AmbientCtx,
			cfg.stopper,
			cfg.clock,
			cfg.rpcContext.LogicalClusterID,
			cfg.nodeIDContainer,
			cfg.sqlLivenessProvider,
			cfg.Settings,
			cfg.HistogramWindowInterval(),
			func(ctx context.Context, opName string, user username.SQLUsername) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.MakeJobExecContext(ctx, opName, user, &sql.MemoryMetrics{}, execCfg)
			},
			jobAdoptionStopFile,
			td,
			jobsKnobs,
		)
	}
	cfg.registry.AddMetricStruct(jobRegistry.MetricsStruct())

	// Set up Lease Manager
	var lmKnobs lease.ManagerTestingKnobs
	if leaseManagerTestingKnobs := cfg.TestingKnobs.SQLLeaseManager; leaseManagerTestingKnobs != nil {
		lmKnobs = *leaseManagerTestingKnobs.(*lease.ManagerTestingKnobs)
	}

	leaseMgr := lease.NewLeaseManager(
		cfg.AmbientCtx,
		cfg.nodeIDContainer,
		cfg.internalDB,
		cfg.clock,
		cfg.Settings,
		settingsWatcher,
		codec,
		lmKnobs,
		cfg.stopper,
		cfg.rangeFeedFactory,
	)
	cfg.registry.AddMetricStruct(leaseMgr.MetricsStruct())

	rootSQLMetrics := cfg.monitorAndMetrics.rootSQLMetrics
	cfg.registry.AddMetricStruct(rootSQLMetrics)

	// Set up internal memory metrics for use by internal SQL executors.
	internalMemMetrics := sql.MakeMemMetrics("internal", cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(internalMemMetrics)

	rootSQLMemoryMonitor := cfg.monitorAndMetrics.rootSQLMemoryMonitor

	// bulkMemoryMonitor is the parent to all child SQL monitors tracking bulk
	// operations (IMPORT, index backfill). It is itself a child of the
	// ParentMemoryMonitor.
	bulkMemoryMonitor := mon.NewMonitorInheritWithLimit("bulk-mon", 0 /* limit */, rootSQLMemoryMonitor)
	bulkMetrics := bulk.MakeBulkMetrics(cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(bulkMetrics)
	bulkMemoryMonitor.SetMetrics(bulkMetrics.CurBytesCount, bulkMetrics.MaxBytesHist)
	bulkMemoryMonitor.StartNoReserved(context.Background(), rootSQLMemoryMonitor)

	backfillMemoryMonitor := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "backfill-mon")
	backupMemoryMonitor := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "backup-mon")

	serverCacheMemoryMonitor := mon.NewMonitorInheritWithLimit(
		"server-cache-mon", 0 /* limit */, rootSQLMemoryMonitor,
	)
	serverCacheMemoryMonitor.StartNoReserved(context.Background(), rootSQLMemoryMonitor)

	// Set up the DistSQL temp engine.

	useStoreSpec := cfg.TempStorageConfig.Spec
	tempEngine, tempFS, err := storage.NewTempEngine(ctx, cfg.TempStorageConfig, useStoreSpec)
	if err != nil {
		return nil, errors.Wrap(err, "creating temp storage")
	}
	cfg.stopper.AddCloser(tempEngine)
	// Remove temporary directory linked to tempEngine after closing
	// tempEngine.
	cfg.stopper.AddCloser(stop.CloserFn(func() {
		useStore := cfg.TempStorageConfig.Spec
		var err error
		if useStore.InMemory {
			// Used store is in-memory so we remove the temp
			// directory directly since there is no record file.
			err = os.RemoveAll(cfg.TempStorageConfig.Path)
		} else {
			// If record file exists, we invoke CleanupTempDirs to
			// also remove the record after the temp directory is
			// removed.
			recordPath := filepath.Join(useStore.Path, TempDirsRecordFilename)
			err = fs.CleanupTempDirs(recordPath)
		}
		if err != nil {
			log.Errorf(ctx, "could not remove temporary store directory: %v", err.Error())
		}
	}))

	distSQLMetrics := execinfra.MakeDistSQLMetrics(cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(distSQLMetrics)
	rowMetrics := sql.NewRowMetrics(false /* internal */)
	cfg.registry.AddMetricStruct(rowMetrics)
	internalRowMetrics := sql.NewRowMetrics(true /* internal */)
	cfg.registry.AddMetricStruct(internalRowMetrics)

	virtualSchemas, err := sql.NewVirtualSchemaHolder(ctx, cfg.Settings)
	if err != nil {
		return nil, errors.Wrap(err, "creating virtual schema holder")
	}

	hydratedDescCache := hydrateddesccache.NewCache(cfg.Settings)
	cfg.registry.AddMetricStruct(hydratedDescCache.Metrics())

	gcJobNotifier := gcjobnotifier.New(cfg.Settings, cfg.systemConfigWatcher, codec, cfg.stopper)

	spanConfig := struct {
		manager              *spanconfigmanager.Manager
		sqlTranslatorFactory *spanconfigsqltranslator.Factory
		sqlWatcher           *spanconfigsqlwatcher.SQLWatcher
		splitter             spanconfig.Splitter
		limiter              spanconfig.Limiter
	}{}

	spanConfigKnobs, _ := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs)
	if codec.ForSystemTenant() {
		spanConfig.splitter = spanconfigsplitter.NoopSplitter{}
	} else {
		spanConfig.splitter = spanconfigsplitter.New(codec, spanConfigKnobs)
	}

	if cfg.spanLimiterFactory == nil {
		spanConfig.limiter = spanconfiglimiter.NoopLimiter{}
	} else {
		spanConfig.limiter = cfg.spanLimiterFactory(
			cfg.circularInternalExecutor,
			cfg.Settings,
			spanConfigKnobs,
		)
	}

	collectionFactory := descs.NewCollectionFactory(
		ctx,
		cfg.Settings,
		leaseMgr,
		virtualSchemas,
		hydratedDescCache,
		spanConfig.splitter,
		spanConfig.limiter,
		catsessiondata.DefaultDescriptorSessionDataProvider,
	)

	clusterIDForSQL := cfg.rpcContext.LogicalClusterID

	bulkSenderLimiter := bulk.MakeAndRegisterConcurrencyLimiter(&cfg.Settings.SV)

	rangeStatsFetcher := rangestats.NewFetcher(cfg.db)

	// Set up the DistSQL server.
	distSQLCfg := execinfra.ServerConfig{
		AmbientContext:   cfg.AmbientCtx,
		Settings:         cfg.Settings,
		RuntimeStats:     cfg.runtime,
		LogicalClusterID: clusterIDForSQL,
		ClusterName:      cfg.ClusterName,
		NodeID:           cfg.nodeIDContainer,
		Locality:         cfg.Locality,
		Codec:            codec,
		DB:               cfg.internalDB,
		RPCContext:       cfg.rpcContext,
		Stopper:          cfg.stopper,

		TempStorage:     tempEngine,
		TempStoragePath: cfg.TempStorageConfig.Path,
		TempFS:          tempFS,
		// COCKROACH_VEC_MAX_OPEN_FDS specifies the maximum number of open file
		// descriptors that the vectorized execution engine may have open at any
		// one time. This limit is implemented as a weighted semaphore acquired
		// before opening files.
		VecFDSemaphore:    semaphore.New(envutil.EnvOrDefaultInt("COCKROACH_VEC_MAX_OPEN_FDS", colexec.VecMaxOpenFDsLimit)),
		ParentDiskMonitor: cfg.TempStorageConfig.Mon,
		BackfillerMonitor: backfillMemoryMonitor,
		BackupMonitor:     backupMemoryMonitor,
		BulkSenderLimiter: bulkSenderLimiter,

		ParentMemoryMonitor: rootSQLMemoryMonitor,
		BulkAdder: func(
			ctx context.Context, db *kv.DB, ts hlc.Timestamp, opts kvserverbase.BulkAdderOptions,
		) (kvserverbase.BulkAdder, error) {
			// Attach a child memory monitor to enable control over the BulkAdder's
			// memory usage.
			bulkMon := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "bulk-adder-monitor")
			return bulk.MakeBulkAdder(ctx, db, cfg.distSender.RangeDescriptorCache(), cfg.Settings, ts, opts, bulkMon, bulkSenderLimiter)
		},

		Metrics:            &distSQLMetrics,
		RowMetrics:         &rowMetrics,
		InternalRowMetrics: &internalRowMetrics,

		SQLLivenessReader: cfg.sqlLivenessProvider.CachedReader(),
		JobRegistry:       jobRegistry,
		Gossip:            cfg.gossip,
		PodNodeDialer:     cfg.podNodeDialer,
		LeaseManager:      leaseMgr,

		ExternalStorage:        cfg.externalStorage,
		ExternalStorageFromURI: cfg.externalStorageFromURI,

		DistSender:               cfg.distSender,
		RangeCache:               cfg.distSender.RangeDescriptorCache(),
		SQLSQLResponseAdmissionQ: cfg.sqlSQLResponseAdmissionQ,
		CollectionFactory:        collectionFactory,
		ExternalIORecorder:       cfg.costController,
		TenantCostController:     cfg.costController,
		RangeStatsFetcher:        rangeStatsFetcher,
		AdmissionPacerFactory:    cfg.admissionPacerFactory,
		ExecutorConfig:           execCfg,
		RootSQLMemoryPoolSize:    cfg.MemoryPoolSize,
	}
	cfg.TempStorageConfig.Mon.SetMetrics(distSQLMetrics.CurDiskBytesCount, distSQLMetrics.MaxDiskBytesHist)
	if distSQLTestingKnobs := cfg.TestingKnobs.DistSQL; distSQLTestingKnobs != nil {
		distSQLCfg.TestingKnobs = *distSQLTestingKnobs.(*execinfra.TestingKnobs)
	}
	if cfg.TestingKnobs.JobsTestingKnobs != nil {
		distSQLCfg.TestingKnobs.JobsTestingKnobs = cfg.TestingKnobs.JobsTestingKnobs
	}

	distSQLServer := distsql.NewServer(ctx, distSQLCfg, cfg.remoteFlowRunner)
	execinfrapb.RegisterDistSQLServer(cfg.grpcServer, distSQLServer)

	// Set up Executor

	var sqlExecutorTestingKnobs sql.ExecutorTestingKnobs
	if k := cfg.TestingKnobs.SQLExecutor; k != nil {
		sqlExecutorTestingKnobs = *k.(*sql.ExecutorTestingKnobs)
	} else {
		sqlExecutorTestingKnobs = sql.ExecutorTestingKnobs{}
	}

	nodeInfo := sql.NodeInfo{
		AdminURL: cfg.AdminURL,
		PGURL: func(user *url.Userinfo) (*pgurl.URL, error) {
			if cfg.Config.SQLAdvertiseAddr == "" {
				log.Fatal(ctx, "programming error: usage of advertised addr before listeners have started")
			}
			ccopts := clientsecopts.ClientSecurityOptions{
				Insecure: cfg.Config.Insecure,
				CertsDir: cfg.Config.SSLCertsDir,
			}
			sparams := clientsecopts.ServerParameters{
				ServerAddr:      cfg.Config.SQLAdvertiseAddr,
				DefaultPort:     base.DefaultPort,
				DefaultDatabase: catalogkeys.DefaultDatabaseName,
			}
			return clientsecopts.MakeURLForServer(ccopts, sparams, user)
		},
		LogicalClusterID: cfg.rpcContext.LogicalClusterID.Get,
		NodeID:           cfg.nodeIDContainer,
	}

	var isAvailable func(sqlInstanceID base.SQLInstanceID) bool
	nodeLiveness, hasNodeLiveness := cfg.nodeLiveness.Optional(47900)
	if hasNodeLiveness {
		// TODO(erikgrinaker): We may want to use IsAvailableNotDraining instead, to
		// avoid scheduling long-running flows (e.g. rangefeeds or backups) on nodes
		// that are being drained/decommissioned. However, these nodes can still be
		// leaseholders, and preventing processor scheduling on them can cause a
		// performance cliff for e.g. table reads that then hit the network.
		isAvailable = func(sqlInstanceID base.SQLInstanceID) bool {
			return nodeLiveness.GetNodeVitalityFromCache(roachpb.NodeID(sqlInstanceID)).IsAlive()
		}
	} else {
		// We're on a SQL tenant, so this is the only node DistSQL will ever
		// schedule on - always returning true is fine.
		isAvailable = func(sqlInstanceID base.SQLInstanceID) bool {
			return true
		}
	}

	// Setup the trace collector that is used to fetch inflight trace spans from
	// all nodes in the cluster.
	// The collector requires nodeliveness to get a list of all the nodes in the
	// cluster.
	var getNodes func(ctx context.Context) ([]roachpb.NodeID, error)
	if isMixedSQLAndKVNode {
		// TODO(dt): any reason not to just always use the instance reader? And just
		// pass it directly instead of making a new closure here?
		getNodes = func(ctx context.Context) ([]roachpb.NodeID, error) {
			var ns []roachpb.NodeID
			ls, err := nodeLiveness.ScanNodeVitalityFromKV(ctx)
			if err != nil {
				return nil, err
			}
			for _, l := range ls {
				if l.IsDecommissioned() {
					continue
				}
				ns = append(ns, l.Liveness.NodeID)
			}
			return ns, nil
		}
	} else {
		getNodes = func(ctx context.Context) ([]roachpb.NodeID, error) {
			instances, err := cfg.sqlInstanceReader.GetAllInstances(ctx)
			if err != nil {
				return nil, err
			}
			instanceIDs := make([]roachpb.NodeID, len(instances))
			for i, instance := range instances {
				instanceIDs[i] = roachpb.NodeID(instance.InstanceID)
			}
			return instanceIDs, err
		}
	}
	traceCollector := collector.New(cfg.Tracer, getNodes, cfg.podNodeDialer)
	contentionMetrics := contention.NewMetrics()
	cfg.registry.AddMetricStruct(contentionMetrics)

	contentionRegistry := contention.NewRegistry(
		cfg.Settings,
		cfg.sqlStatusServer.TxnIDResolution,
		&contentionMetrics,
	)

	storageEngineClient := kvserver.NewStorageEngineClient(cfg.nodeDialer)
	*execCfg = sql.ExecutorConfig{
		Settings:                cfg.Settings,
		NodeInfo:                nodeInfo,
		Codec:                   codec,
		DefaultZoneConfig:       &cfg.DefaultZoneConfig,
		Locality:                cfg.Locality,
		AmbientCtx:              cfg.AmbientCtx,
		DB:                      cfg.db,
		Gossip:                  cfg.gossip,
		NodeLiveness:            cfg.nodeLiveness,
		SystemConfig:            cfg.systemConfigWatcher,
		MetricsRecorder:         cfg.recorder,
		DistSender:              cfg.distSender,
		RPCContext:              cfg.rpcContext,
		LeaseManager:            leaseMgr,
		TenantStatusServer:      cfg.tenantStatusServer,
		Clock:                   cfg.clock,
		DistSQLSrv:              distSQLServer,
		NodesStatusServer:       cfg.nodesStatusServer,
		SQLStatusServer:         cfg.sqlStatusServer,
		SessionRegistry:         cfg.sessionRegistry,
		ClosedSessionCache:      cfg.closedSessionCache,
		ContentionRegistry:      contentionRegistry,
		SQLLiveness:             cfg.sqlLivenessProvider,
		JobRegistry:             jobRegistry,
		VirtualSchemas:          virtualSchemas,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		RangeDescriptorCache:    cfg.distSender.RangeDescriptorCache(),
		RoleMemberCache: sql.NewMembershipCache(
			serverCacheMemoryMonitor.MakeBoundAccount(), cfg.stopper,
		),
		SessionInitCache: sessioninit.NewCache(
			serverCacheMemoryMonitor.MakeBoundAccount(), cfg.stopper,
		),
		ClientCertExpirationCache: security.NewClientCertExpirationCache(
			ctx, cfg.Settings, cfg.stopper, &timeutil.DefaultTimeSource{}, rootSQLMemoryMonitor,
		),
		RootMemoryMonitor:         rootSQLMemoryMonitor,
		TestingKnobs:              sqlExecutorTestingKnobs,
		CompactEngineSpanFunc:     storageEngineClient.CompactEngineSpan,
		CompactionConcurrencyFunc: storageEngineClient.SetCompactionConcurrency,
		TraceCollector:            traceCollector,
		TenantUsageServer:         cfg.tenantUsageServer,
		KVStoresIterator:          cfg.kvStoresIterator,
		InspectzServer:            cfg.inspectzServer,
		RangeDescIteratorFactory:  cfg.rangeDescIteratorFactory,
		SyntheticPrivilegeCache: syntheticprivilegecache.New(
			cfg.Settings, cfg.stopper, cfg.db,
			serverCacheMemoryMonitor.MakeBoundAccount(),
			virtualSchemas, cfg.internalDB,
		),
		DistSQLPlanner: sql.NewDistSQLPlanner(
			ctx,
			execinfra.Version,
			cfg.Settings,
			cfg.nodeIDContainer.SQLInstanceID(),
			cfg.rpcContext,
			distSQLServer,
			cfg.distSender,
			cfg.nodeDescs,
			cfg.gossip,
			cfg.stopper,
			isAvailable,
			cfg.nodeDialer.ConnHealthTryDial,
			cfg.podNodeDialer,
			codec,
			cfg.sqlInstanceReader,
			cfg.clock,
		),

		TableStatsCache: stats.NewTableStatisticsCache(
			cfg.TableStatCacheSize,
			cfg.Settings,
			cfg.internalDB,
		),

		QueryCache:                 querycache.New(cfg.QueryCacheSize),
		RowMetrics:                 &rowMetrics,
		InternalRowMetrics:         &internalRowMetrics,
		ProtectedTimestampProvider: cfg.protectedtsProvider,
		ExternalIODirConfig:        cfg.ExternalIODirConfig,
		GCJobNotifier:              gcJobNotifier,
		RangeFeedFactory:           cfg.rangeFeedFactory,
		CollectionFactory:          collectionFactory,
		SystemTableIDResolver:      descs.MakeSystemTableIDResolver(collectionFactory, cfg.internalDB),
		ConsistencyChecker:         consistencychecker.NewConsistencyChecker(cfg.db),
		RangeProber:                rangeprober.NewRangeProber(cfg.db),
		DescIDGenerator:            descidgen.NewGenerator(cfg.Settings, codec, cfg.db),
		RangeStatsFetcher:          rangeStatsFetcher,
		EventsExporter:             cfg.eventsExporter,
		NodeDescs:                  cfg.nodeDescs,
		TenantCapabilitiesReader:   cfg.tenantCapabilitiesReader,
		AutoConfigProvider:         cfg.AutoConfigProvider,
	}

	if sqlSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLSchemaChanger; sqlSchemaChangerTestingKnobs != nil {
		execCfg.SchemaChangerTestingKnobs = sqlSchemaChangerTestingKnobs.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = new(sql.SchemaChangerTestingKnobs)
	}
	if declarativeSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLDeclarativeSchemaChanger; declarativeSchemaChangerTestingKnobs != nil {
		execCfg.DeclarativeSchemaChangerTestingKnobs = declarativeSchemaChangerTestingKnobs.(*scexec.TestingKnobs)
	} else {
		execCfg.DeclarativeSchemaChangerTestingKnobs = new(scexec.TestingKnobs)
	}
	if sqlTypeSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLTypeSchemaChanger; sqlTypeSchemaChangerTestingKnobs != nil {
		execCfg.TypeSchemaChangerTestingKnobs = sqlTypeSchemaChangerTestingKnobs.(*sql.TypeSchemaChangerTestingKnobs)
	} else {
		execCfg.TypeSchemaChangerTestingKnobs = new(sql.TypeSchemaChangerTestingKnobs)
	}
	execCfg.SchemaChangerMetrics = sql.NewSchemaChangerMetrics()
	cfg.registry.AddMetricStruct(execCfg.SchemaChangerMetrics)

	execCfg.FeatureFlagMetrics = featureflag.NewFeatureFlagMetrics()
	cfg.registry.AddMetricStruct(execCfg.FeatureFlagMetrics)

	if gcJobTestingKnobs := cfg.TestingKnobs.GCJob; gcJobTestingKnobs != nil {
		execCfg.GCJobTestingKnobs = gcJobTestingKnobs.(*sql.GCJobTestingKnobs)
	} else {
		execCfg.GCJobTestingKnobs = new(sql.GCJobTestingKnobs)
	}
	if distSQLRunTestingKnobs := cfg.TestingKnobs.DistSQL; distSQLRunTestingKnobs != nil {
		execCfg.DistSQLRunTestingKnobs = distSQLRunTestingKnobs.(*execinfra.TestingKnobs)
	} else {
		execCfg.DistSQLRunTestingKnobs = new(execinfra.TestingKnobs)
	}
	if sqlEvalContext := cfg.TestingKnobs.SQLEvalContext; sqlEvalContext != nil {
		execCfg.EvalContextTestingKnobs = *sqlEvalContext.(*eval.TestingKnobs)
	}
	if pgwireKnobs := cfg.TestingKnobs.PGWireTestingKnobs; pgwireKnobs != nil {
		execCfg.PGWireTestingKnobs = pgwireKnobs.(*sql.PGWireTestingKnobs)
	}
	if tenantKnobs := cfg.TestingKnobs.TenantTestingKnobs; tenantKnobs != nil {
		execCfg.TenantTestingKnobs = tenantKnobs.(*sql.TenantTestingKnobs)
	}
	if backupRestoreKnobs := cfg.TestingKnobs.BackupRestore; backupRestoreKnobs != nil {
		execCfg.BackupRestoreTestingKnobs = backupRestoreKnobs.(*sql.BackupRestoreTestingKnobs)
	}
	if streamTestingKnobs := cfg.TestingKnobs.Streaming; streamTestingKnobs != nil {
		execCfg.StreamingTestingKnobs = streamTestingKnobs.(*sql.StreamingTestingKnobs)
	}
	if ttlKnobs := cfg.TestingKnobs.TTL; ttlKnobs != nil {
		execCfg.TTLTestingKnobs = ttlKnobs.(*sql.TTLTestingKnobs)
	}
	if schemaTelemetryKnobs := cfg.TestingKnobs.SchemaTelemetry; schemaTelemetryKnobs != nil {
		execCfg.SchemaTelemetryTestingKnobs = schemaTelemetryKnobs.(*sql.SchemaTelemetryTestingKnobs)
	}
	if sqlStatsKnobs := cfg.TestingKnobs.SQLStatsKnobs; sqlStatsKnobs != nil {
		execCfg.SQLStatsTestingKnobs = sqlStatsKnobs.(*sqlstats.TestingKnobs)
	}
	if eventlogKnobs := cfg.TestingKnobs.EventLog; eventlogKnobs != nil {
		execCfg.EventLogTestingKnobs = eventlogKnobs.(*sql.EventLogTestingKnobs)
	}
	if telemetryLoggingKnobs := cfg.TestingKnobs.TelemetryLoggingKnobs; telemetryLoggingKnobs != nil {
		execCfg.TelemetryLoggingTestingKnobs = telemetryLoggingKnobs.(*sql.TelemetryLoggingTestingKnobs)
	}
	if spanConfigKnobs := cfg.TestingKnobs.SpanConfig; spanConfigKnobs != nil {
		execCfg.SpanConfigTestingKnobs = spanConfigKnobs.(*spanconfig.TestingKnobs)
	}
	if capturedIndexUsageStatsKnobs := cfg.TestingKnobs.CapturedIndexUsageStatsKnobs; capturedIndexUsageStatsKnobs != nil {
		execCfg.CaptureIndexUsageStatsKnobs = capturedIndexUsageStatsKnobs.(*scheduledlogging.CaptureIndexUsageStatsTestingKnobs)
	}

	if unusedIndexRecommendationsKnobs := cfg.TestingKnobs.UnusedIndexRecommendKnobs; unusedIndexRecommendationsKnobs != nil {
		execCfg.UnusedIndexRecommendationsKnobs = unusedIndexRecommendationsKnobs.(*idxusage.UnusedIndexRecommendationTestingKnobs)
	}
	if externalConnKnobs := cfg.TestingKnobs.ExternalConnection; externalConnKnobs != nil {
		execCfg.ExternalConnectionTestingKnobs = externalConnKnobs.(*externalconn.TestingKnobs)
	}
	if autoConfigKnobs := cfg.TestingKnobs.AutoConfig; autoConfigKnobs != nil {
		knobs := autoConfigKnobs.(*autoconfig.TestingKnobs)
		if knobs.Provider != nil {
			execCfg.AutoConfigProvider = knobs.Provider
		}
	}

	statsRefresher := stats.MakeRefresher(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.circularInternalExecutor,
		execCfg.TableStatsCache,
		stats.DefaultAsOfTime,
	)
	execCfg.StatsRefresher = statsRefresher

	// Set up internal memory metrics for use by internal SQL executors.
	// Don't add them to the registry now because it will be added as part of pgServer metrics.
	sqlMemMetrics := sql.MakeMemMetrics("sql", cfg.HistogramWindowInterval())

	// Initialize the pgwire server which handles connections
	// established via the pgPreServer.
	pgServer := pgwire.MakeServer(
		cfg.AmbientCtx,
		cfg.Config,
		cfg.Settings,
		sqlMemMetrics,
		rootSQLMemoryMonitor,
		cfg.HistogramWindowInterval(),
		execCfg,
	)

	distSQLServer.ServerConfig.SQLStatsController = pgServer.SQLServer.GetSQLStatsController()
	distSQLServer.ServerConfig.SchemaTelemetryController = pgServer.SQLServer.GetSchemaTelemetryController()
	distSQLServer.ServerConfig.IndexUsageStatsController = pgServer.SQLServer.GetIndexUsageStatsController()
	distSQLServer.ServerConfig.StatsRefresher = statsRefresher

	// We use one BytesMonitor for all Executor's created by the
	// internalDB.
	// Note that internalDBMonitor does not have to be closed, the parent
	// monitor comes from server. internalDBMonitor is a singleton attached
	// to server, if server is closed, we don't have to worry about
	// returning the memory allocated to internalDBMonitor since the
	// parent monitor is being closed anyway.
	internalDBMonitor := mon.NewMonitor(
		"internal sql executor",
		mon.MemoryResource,
		internalMemMetrics.CurBytesCount,
		internalMemMetrics.MaxBytesHist,
		-1,            /* use default increment */
		math.MaxInt64, /* noteworthy */
		cfg.Settings,
	)
	internalDBMonitor.StartNoReserved(ctx, pgServer.SQLServer.GetBytesMonitor())
	// Now that we have a pgwire.Server (which has a sql.Server), we can close a
	// circular dependency between the rowexec.Server and sql.Server and set
	// InternalDB. The same applies for setting a
	// SessionBoundInternalExecutor on the job registry.
	internalDB := sql.NewInternalDB(
		pgServer.SQLServer,
		internalMemMetrics,
		internalDBMonitor,
	)
	*cfg.internalDB = *internalDB
	execCfg.InternalDB = internalDB
	execCfg.IndexBackfiller = sql.NewIndexBackfiller(execCfg)
	execCfg.IndexSpanSplitter = sql.NewIndexSplitAndScatter(execCfg)
	execCfg.IndexMerger = sql.NewIndexBackfillerMergePlanner(execCfg)
	execCfg.ProtectedTimestampManager = jobsprotectedts.NewManager(
		execCfg.InternalDB,
		execCfg.Codec,
		execCfg.ProtectedTimestampProvider,
		execCfg.SystemConfig,
		execCfg.JobRegistry,
	)
	execCfg.Validator = scdeps.NewValidator(
		execCfg.DB,
		execCfg.Codec,
		execCfg.Settings,
		internalDB,
		execCfg.ProtectedTimestampManager,
		sql.ValidateForwardIndexes,
		sql.ValidateInvertedIndexes,
		sql.ValidateConstraint,
		sql.NewInternalSessionData,
	)

	jobsInternalDB := sql.NewInternalDBWithSessionDataOverrides(internalDB, func(sd *sessiondata.SessionData) {
		// All the internal SQL operations performed by the jobs subsystem
		// must have minimal interaction with other nodes. To achieve this,
		// we disable query distribution.
		// See https://github.com/cockroachdb/cockroach/issues/100578 for an example
		// bad thing that happens when not doing this.
		sd.DistSQLMode = sessiondatapb.DistSQLOff

		// Job internal operations use the node principal.
		sd.UserProto = username.NodeUserName().EncodeProto()
	})
	jobRegistry.SetInternalDB(jobsInternalDB)

	distSQLServer.ServerConfig.ProtectedTimestampProvider = execCfg.ProtectedTimestampProvider

	for _, m := range pgServer.Metrics() {
		cfg.registry.AddMetricStruct(m)
	}
	*cfg.circularInternalExecutor = sql.MakeInternalExecutor(pgServer.SQLServer, internalMemMetrics, internalDBMonitor)

	stmtDiagnosticsRegistry := stmtdiagnostics.NewRegistry(
		cfg.internalDB,
		cfg.Settings,
	)
	execCfg.StmtDiagnosticsRecorder = stmtDiagnosticsRegistry

	var upgradeMgr *upgrademanager.Manager
	{
		var c upgrade.Cluster
		var systemDeps upgrade.SystemDeps
		keyVisKnobs, _ := cfg.TestingKnobs.KeyVisualizer.(*keyvisualizer.TestingKnobs)
		sqlStatsKnobs, _ := cfg.TestingKnobs.SQLStatsKnobs.(*sqlstats.TestingKnobs)
		if codec.ForSystemTenant() {
			c = upgradecluster.New(upgradecluster.ClusterConfig{
				NodeLiveness:     nodeLiveness,
				Dialer:           cfg.nodeDialer,
				RangeDescScanner: rangedesc.NewScanner(cfg.db),
				DB:               cfg.db,
			})
		} else {
			c = upgradecluster.NewTenantCluster(
				upgradecluster.TenantClusterConfig{
					Dialer:         cfg.podNodeDialer,
					InstanceReader: cfg.sqlInstanceReader,
					DB:             cfg.db,
				})
		}
		systemDeps = upgrade.SystemDeps{
			Cluster:       c,
			DB:            cfg.internalDB,
			Settings:      cfg.Settings,
			JobRegistry:   jobRegistry,
			Stopper:       cfg.stopper,
			KeyVisKnobs:   keyVisKnobs,
			SQLStatsKnobs: sqlStatsKnobs,
		}

		knobs, _ := cfg.TestingKnobs.UpgradeManager.(*upgradebase.TestingKnobs)
		upgradeMgr = upgrademanager.NewManager(
			systemDeps, leaseMgr, cfg.circularInternalExecutor, jobRegistry, codec,
			cfg.Settings, clusterIDForSQL.Get(), knobs,
		)
		execCfg.UpgradeJobDeps = upgradeMgr
		execCfg.VersionUpgradeHook = upgradeMgr.Migrate
		execCfg.UpgradeTestingKnobs = knobs
	}

	if !codec.ForSystemTenant() || !cfg.SpanConfigsDisabled {
		// Instantiate a span config manager. If we're the host tenant we'll
		// only do it unless COCKROACH_DISABLE_SPAN_CONFIGS is set.
		spanConfigKnobs, _ := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs)
		spanConfig.sqlTranslatorFactory = spanconfigsqltranslator.NewFactory(
			execCfg.ProtectedTimestampProvider, codec, spanConfigKnobs,
		)
		spanConfig.sqlWatcher = spanconfigsqlwatcher.New(
			codec,
			cfg.Settings,
			cfg.rangeFeedFactory,
			1<<20, /* 1 MB bufferMemLimit */
			cfg.stopper,
			// TODO(irfansharif): What should this no-op cadence be?
			30*time.Second, /* checkpointNoopsEvery */
			spanConfigKnobs,
		)
		spanConfigReconciler := spanconfigreconciler.New(
			spanConfig.sqlWatcher,
			spanConfig.sqlTranslatorFactory,
			cfg.spanConfigAccessor,
			execCfg,
			codec,
			cfg.TenantID,
			cfg.Settings,
			spanConfigKnobs,
		)
		spanConfig.manager = spanconfigmanager.New(
			cfg.internalDB,
			jobRegistry,
			cfg.stopper,
			cfg.Settings,
			spanConfigReconciler,
			spanConfigKnobs,
		)

		execCfg.SpanConfigReconciler = spanConfigReconciler
	}
	execCfg.SpanConfigKVAccessor = cfg.spanConfigAccessor
	execCfg.SpanConfigLimiter = spanConfig.limiter
	execCfg.SpanConfigSplitter = spanConfig.splitter

	var waitForInstanceReaderStarted func(context.Context) error
	if cfg.sqlInstanceReader != nil {
		waitForInstanceReaderStarted = cfg.sqlInstanceReader.WaitForStarted
	}

	if codec.ForSystemTenant() {
		ri := kvcoord.MakeRangeIterator(cfg.distSender)
		spanStatsConsumer := spanstatsconsumer.New(
			cfg.keyVisServerAccessor,
			&ri,
			cfg.Settings,
			cfg.circularInternalExecutor,
		)
		execCfg.SpanStatsConsumer = spanStatsConsumer
	}

	temporaryObjectCleaner := sql.NewTemporaryObjectCleaner(
		cfg.Settings,
		cfg.internalDB,
		codec,
		cfg.registry,
		cfg.sqlStatusServer,
		cfg.isMeta1Leaseholder,
		sqlExecutorTestingKnobs,
		waitForInstanceReaderStarted,
	)

	reporter := &diagnostics.Reporter{
		StartTime:        timeutil.Now(),
		AmbientCtx:       &cfg.AmbientCtx,
		Config:           cfg.BaseConfig.Config,
		Settings:         cfg.Settings,
		StorageClusterID: cfg.rpcContext.StorageClusterID.Get,
		LogicalClusterID: clusterIDForSQL.Get,
		TenantID:         cfg.rpcContext.TenantID,
		SQLInstanceID:    cfg.nodeIDContainer.SQLInstanceID,
		SQLServer:        pgServer.SQLServer,
		InternalExec:     cfg.circularInternalExecutor,
		DB:               cfg.db,
		Recorder:         cfg.recorder,
		Locality:         cfg.Locality,
	}

	if cfg.TestingKnobs.Server != nil {
		reporter.TestingKnobs = &cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

	startedWithExplicitVModule := log.GetVModule() != ""
	fn := func(ctx context.Context) {
		if startedWithExplicitVModule {
			log.Infof(ctx, "ignoring vmodule cluster setting due to starting with explicit vmodule flag")
		} else {
			s := vmoduleSetting.Get(&cfg.Settings.SV)
			if log.GetVModule() != s {
				log.Infof(ctx, "updating vmodule from cluster setting to %s", s)
				if err := log.SetVModule(s); err != nil {
					log.Warningf(ctx, "failed to apply vmodule cluster setting: %v", err)
				}
			}
		}
	}
	vmoduleSetting.SetOnChange(&cfg.Settings.SV, fn)
	fn(ctx)

	auditlogging.UserAuditEnterpriseParamsHook = func(st *cluster.Settings, clusterID uuid.UUID) func() (*cluster.Settings, uuid.UUID, error) {
		return func() (*cluster.Settings, uuid.UUID, error) {
			return st, clusterID, nil
		}
	}(execCfg.Settings, cfg.ClusterIDContainer.Get())

	auditlogging.ConfigureRoleBasedAuditClusterSettings(ctx, execCfg.SessionInitCache.AuditConfig, execCfg.Settings, &execCfg.Settings.SV)

	return &SQLServer{
		ambientCtx:                     cfg.BaseConfig.AmbientCtx,
		stopper:                        cfg.stopper,
		stopTrigger:                    cfg.stopTrigger,
		sqlIDContainer:                 cfg.nodeIDContainer,
		pgServer:                       pgServer,
		distSQLServer:                  distSQLServer,
		execCfg:                        execCfg,
		internalExecutor:               cfg.circularInternalExecutor,
		internalDB:                     cfg.internalDB,
		leaseMgr:                       leaseMgr,
		tracingService:                 tracingService,
		tenantConnect:                  cfg.tenantConnect,
		sessionRegistry:                cfg.sessionRegistry,
		closedSessionCache:             cfg.closedSessionCache,
		jobRegistry:                    jobRegistry,
		statsRefresher:                 statsRefresher,
		temporaryObjectCleaner:         temporaryObjectCleaner,
		internalMemMetrics:             internalMemMetrics,
		sqlMemMetrics:                  sqlMemMetrics,
		stmtDiagnosticsRegistry:        stmtDiagnosticsRegistry,
		sqlLivenessProvider:            cfg.sqlLivenessProvider,
		sqlInstanceStorage:             cfg.sqlInstanceStorage,
		sqlInstanceReader:              cfg.sqlInstanceReader,
		metricsRegistry:                cfg.registry,
		diagnosticsReporter:            reporter,
		spanconfigMgr:                  spanConfig.manager,
		spanconfigSQLTranslatorFactory: spanConfig.sqlTranslatorFactory,
		spanconfigSQLWatcher:           spanConfig.sqlWatcher,
		settingsWatcher:                settingsWatcher,
		systemConfigWatcher:            cfg.systemConfigWatcher,
		isMeta1Leaseholder:             cfg.isMeta1Leaseholder,
		cfg:                            cfg.BaseConfig,
		internalDBMemMonitor:           internalDBMonitor,
		upgradeManager:                 upgradeMgr,
	}, nil
}

func (s *SQLServer) preStart(
	ctx context.Context,
	stopper *stop.Stopper,
	knobs base.TestingKnobs,
	orphanedLeasesTimeThresholdNanos int64,
) error {
	// If necessary, start the tenant proxy first, to ensure all other
	// components can properly route to KV nodes. The Start method will block
	// until a connection is established to the cluster and its ID has been
	// determined.
	if s.tenantConnect != nil {
		if err := s.tenantConnect.Start(ctx); err != nil {
			return err
		}
	}

	// Initialize the settings watcher early in sql server startup. Settings
	// values are meaningless before the watcher is initialized and most sub
	// systems depend on system settings.
	if err := s.settingsWatcher.Start(ctx); err != nil {
		return errors.Wrap(err, "initializing settings")
	}

	// Load the multi-region enum by reading the system database's descriptor.
	// This also serves as a simple check to see if a tenant exist (i.e. by
	// checking whether the system db has been bootstrapped).
	regionPhysicalRep, err := startup.RunIdempotentWithRetryEx(ctx,
		stopper.ShouldQuiesce(),
		"sql get locality",
		func(ctx context.Context) ([]byte, error) {
			res, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
				ctx, s.internalDB, keys.SystemDatabaseID, s.distSQLServer.Locality,
			)
			if errors.Is(err, sql.ErrNotMultiRegionDatabase) {
				err = nil
			}
			return res, err
		})
	if err != nil {
		return err
	}

	s.leaseMgr.SetRegionPrefix(regionPhysicalRep)

	s.execCfg.ContentionRegistry.Start(ctx, stopper)

	// Start the sql liveness subsystem. We'll need it to get a session.
	s.sqlLivenessProvider.Start(ctx, regionPhysicalRep)

	session, err := s.sqlLivenessProvider.Session(ctx)
	if err != nil {
		return err
	}
	s.sqlLivenessSessionID = session.ID()

	// Start instance ID reclaim loop.
	if err := s.sqlInstanceStorage.RunInstanceIDReclaimLoop(
		ctx, stopper, timeutil.DefaultTimeSource{}, s.internalDB, session.Expiration,
	); err != nil {
		return err
	}

	// If we have a nodeID, set our SQL instance ID to the node
	// ID. Otherwise, allow our SQL instance ID to be generated by
	// SQL.
	nodeID, hasNodeID := s.sqlIDContainer.OptionalNodeID()
	instance, err := startup.RunIdempotentWithRetryEx(ctx,
		stopper.ShouldQuiesce(),
		"sql create node instance row",
		func(ctx context.Context) (sqlinstance.InstanceInfo, error) {
			if hasNodeID {
				// Write/acquire our instance row.
				return s.sqlInstanceStorage.CreateNodeInstance(
					ctx,
					session.ID(),
					session.Expiration(),
					s.cfg.AdvertiseAddr,
					s.cfg.SQLAdvertiseAddr,
					s.distSQLServer.Locality,
					s.execCfg.Settings.Version.BinaryVersion(),
					nodeID,
				)
			}
			return s.sqlInstanceStorage.CreateInstance(
				ctx,
				session.ID(),
				session.Expiration(),
				s.cfg.AdvertiseAddr,
				s.cfg.SQLAdvertiseAddr,
				s.distSQLServer.Locality,
				s.execCfg.Settings.Version.BinaryVersion(),
			)
		})
	if err != nil {
		return err
	}

	// TODO(andrei): Release the instance ID on server shutdown. It is not trivial
	// to determine where/when exactly to do that, though. Doing it after stopper
	// quiescing doesn't work. Doing it too soon, for example as part of draining,
	// is potentially dangerous because the server will continue to use the
	// instance ID for a while.
	log.Infof(ctx, "bound sqlinstance: %v", instance)
	if err := s.sqlIDContainer.SetSQLInstanceID(ctx, instance.InstanceID); err != nil {
		return err
	}

	// TODO(ssd): The gateway instance ID on the DistSQL planner is not thread safe.
	//
	// Despite this function being call preStart, we have multiple
	// asyncronous processes that are already started and may
	// result in reads of the gateway ID, resulting in a race
	// detector violation.
	//
	// However, this is only true for the system tenant.
	if !s.execCfg.Codec.ForSystemTenant() {
		s.execCfg.DistSQLPlanner.SetGatewaySQLInstanceID(instance.InstanceID)
	}

	// Start the instance provider. This needs to come after we've allocated our
	// instance ID because the instances reader needs to see our own instance;
	// we might be the only SQL server available, especially when we have not
	// received data from the rangefeed yet, and if the reader doesn't see
	// it, we'd be unable to plan any queries.
	s.sqlInstanceReader.Start(ctx, instance)

	s.execCfg.GCJobNotifier.Start(ctx)
	s.temporaryObjectCleaner.Start(ctx, stopper)
	s.distSQLServer.Start()
	s.pgServer.Start(ctx, stopper)
	if err := s.statsRefresher.Start(ctx, stopper, stats.DefaultRefreshInterval); err != nil {
		return err
	}
	s.stmtDiagnosticsRegistry.Start(ctx, stopper)
	if err := s.execCfg.TableStatsCache.Start(ctx, s.execCfg.Codec, s.execCfg.RangeFeedFactory); err != nil {
		return err
	}

	s.leaseMgr.RefreshLeases(ctx, stopper, s.execCfg.DB)
	s.leaseMgr.PeriodicallyRefreshSomeLeases(ctx)

	ieMon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.execCfg.Settings)
	ieMon.StartNoReserved(ctx, s.pgServer.SQLServer.GetBytesMonitor())
	s.stopper.AddCloser(stop.CloserFn(func() { ieMon.Stop(ctx) }))

	if err := s.jobRegistry.Start(ctx, stopper); err != nil {
		return err
	}

	if s.spanconfigMgr != nil {
		if err := s.spanconfigMgr.Start(ctx); err != nil {
			return err
		}
	}

	var bootstrapVersion roachpb.Version
	if s.execCfg.Codec.ForSystemTenant() {
		if err := startup.RunIdempotentWithRetry(ctx,
			s.stopper.ShouldQuiesce(),
			"sql get cluster version", func(ctx context.Context) error {
				return s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return txn.GetProto(ctx, keys.BootstrapVersionKey, &bootstrapVersion)
				})
			}); err != nil {
			return err
		}
	} else {
		// We don't currently track the bootstrap version of each secondary tenant.
		// For this to be meaningful, we'd need to record the binary version of the
		// SQL gateway that processed the crdb_internal.create_tenant function which
		// created the tenant, as this is what dictates the MetadataSchema that was
		// in effect when the secondary tenant was constructed. This binary version
		// very well may differ from the cluster-wide bootstrap version at which the
		// system tenant was bootstrapped.
		//
		// Since we don't record this version anywhere, we do the next-best thing
		// and pass a lower-bound on the bootstrap version. We know that no tenants
		// could have been created before the start of the v20.2 dev cycle, so we
		// pass Start20_2. bootstrapVersion is only used to avoid performing
		// superfluous but necessarily idempotent SQL upgrades, so at worst, we're
		// doing more work than strictly necessary during the first time that the
		// upgrades are run.
		bootstrapVersion = roachpb.Version{Major: 20, Minor: 1, Internal: 1}
	}

	if err := s.systemConfigWatcher.Start(ctx, s.stopper); err != nil {
		return errors.Wrap(err, "initializing system config watcher")
	}

	clusterVersionMetrics := clusterversion.MakeMetricsAndRegisterOnVersionChangeCallback(&s.cfg.Settings.SV)
	s.metricsRegistry.AddMetricStruct(clusterVersionMetrics)

	// Run all the "permanent" upgrades that haven't already run in this cluster,
	// until the currently active version. Upgrades for higher versions, if any,
	// will be run in response to `SET CLUSTER SETTING version = <v>`, just like
	// non-permanent upgrade.
	//
	// NOTE: We're going to run the permanent upgrades up to the active version.
	// For mixed kv/sql nodes, I think we could use bootstrapVersion here instead.
	// If the active version has diverged from bootstrap version, then all
	// upgrades in between the two must have run when the cluster version
	// advanced. But for sql-only servers the bootstrap version is not
	// well-defined, so we use the active version.
	if err := s.upgradeManager.RunPermanentUpgrades(
		ctx,
		s.cfg.Settings.Version.ActiveVersion(ctx).Version, /* upToVersion */
	); err != nil {
		return err
	}

	log.Infof(ctx, "done ensuring all necessary startup migrations have run")

	// Prevent the server from starting if its binary version is too low
	// for the current tenant cluster version.
	// This check needs to run after the "version" setting is set in the
	// "system.settings" table of this tenant. This includes both system
	// and secondary tenants.
	var tenantActiveVersion clusterversion.ClusterVersion
	if err := startup.RunIdempotentWithRetry(ctx,
		s.stopper.ShouldQuiesce(),
		"sql get tenant version", func(ctx context.Context) error {
			return s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				tenantActiveVersion, err = s.settingsWatcher.GetClusterVersionFromStorage(ctx, txn)
				return err
			})
		}); err != nil {
		return err
	}
	if s.execCfg.Settings.Version.BinaryVersion().Less(tenantActiveVersion.Version) {
		return errors.WithHintf(errors.Newf("preventing SQL server from starting because its binary version "+
			"is too low for the tenant active version: server binary version = %v, tenant active version = %v",
			s.execCfg.Settings.Version.BinaryVersion(), tenantActiveVersion.Version),
			"use a tenant binary whose version is at least %v", tenantActiveVersion.Version)
	}

	// Delete all orphaned table leases created by a prior instance of this
	// node. This also uses SQL.
	s.leaseMgr.DeleteOrphanedLeases(ctx, orphanedLeasesTimeThresholdNanos)

	// Start scheduled jobs daemon.
	jobs.StartJobSchedulerDaemon(
		ctx,
		stopper,
		s.metricsRegistry,
		&scheduledjobs.JobExecutionConfig{
			Settings:     s.execCfg.Settings,
			DB:           s.execCfg.InternalDB,
			TestingKnobs: knobs.JobsTestingKnobs,
			PlanHookMaker: func(ctx context.Context, opName string, txn *kv.Txn, user username.SQLUsername) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.NewInternalPlanner(
					opName,
					txn,
					user,
					&sql.MemoryMetrics{},
					s.execCfg,
					sql.NewInternalSessionData(ctx, s.execCfg.Settings, opName),
				)
			},
		},
		scheduledjobs.ProdJobSchedulerEnv,
	)

	scheduledlogging.Start(
		ctx, stopper, s.execCfg.InternalDB, s.execCfg.Settings,
		s.execCfg.CaptureIndexUsageStatsKnobs,
	)
	s.execCfg.SyntheticPrivilegeCache.Start(ctx)

	// Report a warning if the server is being shut down via the stopper
	// before it was gracefully drained. This warning may be innocuous
	// in tests where there is no use of the test server/cluster after
	// shutdown; but may be a sign of a problem in production or for
	// tests that need to restart a server.
	stopper.AddCloser(stop.CloserFn(func() {
		var sk *TestingKnobs
		if knobs.Server != nil {
			sk, _ = knobs.Server.(*TestingKnobs)
		}

		if !s.gracefulDrainComplete.Get() {
			warnCtx := s.AnnotateCtx(context.Background())

			if sk != nil && sk.RequireGracefulDrain {
				log.Fatalf(warnCtx, "drain required but not performed")
			}

			log.Warningf(warnCtx, "server shutdown without a prior graceful drain")
		}

		if sk != nil && sk.DrainReportCh != nil {
			sk.DrainReportCh <- struct{}{}
		}
	}))

	return nil
}

// SQLInstanceID returns the ephemeral ID assigned to each SQL instance. The ID
// is guaranteed to be unique across all currently running instances, but may be
// reused once an instance is stopped.
func (s *SQLServer) SQLInstanceID() base.SQLInstanceID {
	return s.sqlIDContainer.SQLInstanceID()
}

// StartDiagnostics starts periodic diagnostics reporting.
// NOTE: This is not called in preStart so that it's disabled by default for
// testing.
func (s *SQLServer) StartDiagnostics(ctx context.Context) {
	s.diagnosticsReporter.PeriodicallyReportDiagnostics(ctx, s.stopper)
}

// AnnotateCtx annotates the given context with the server tracer and tags.
func (s *SQLServer) AnnotateCtx(ctx context.Context) context.Context {
	return s.ambientCtx.AnnotateCtx(ctx)
}

// startServeSQL starts accepting incoming SQL connections over TCP.
// It also starts listening on the Unix socket, if that was configured.
func startServeSQL(
	ctx context.Context,
	stopper *stop.Stopper,
	pgPreServer *pgwire.PreServeConnHandler,
	serveConn func(ctx context.Context, conn net.Conn, preServeStatus pgwire.PreServeStatus) error,
	pgL net.Listener,
	socketFileCfg *string,
) error {
	log.Ops.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	tcpKeepAlive := makeTCPKeepAliveManager()

	// The connManager is responsible for tearing down the net.Conn
	// objects when the stopper tells us to shut down.
	connManager := netutil.MakeTCPServer(ctx, stopper)

	_ = stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "pgwire-listener", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			err := connManager.ServeWith(ctx, pgL, func(ctx context.Context, conn net.Conn) {
				connCtx := pgPreServer.AnnotateCtxForIncomingConn(ctx, conn)
				tcpKeepAlive.configure(connCtx, conn)

				conn, status, err := pgPreServer.PreServe(connCtx, conn, pgwire.SocketTCP)
				if err != nil {
					log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
					return
				}

				if err := serveConn(connCtx, conn, status); err != nil {
					log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
				}
			})
			netutil.FatalIfUnexpected(err)
		})

	socketFile, socketLock, err := prepareUnixSocket(ctx, pgL, socketFileCfg)
	if err != nil {
		return err
	}

	// If a unix socket was requested, start serving there too.
	if len(socketFile) != 0 {
		log.Ops.Infof(ctx, "starting postgres server at unix: %s", socketFile)

		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", socketFile)
		if err != nil {
			err = errors.CombineErrors(err, socketLock.Unlock())
			return err
		}

		waitQuiesce := func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			if err := unixLn.Close(); err != nil {
				log.Ops.Warningf(ctx, "closing unix socket: %v", err)
			}
			if err := socketLock.Unlock(); err != nil {
				log.Ops.Warningf(ctx, "removing unix socket lock: %v", err)
			}
		}
		if err := stopper.RunAsyncTaskEx(ctx,
			stop.TaskOpts{TaskName: "unix-ln-close", SpanOpt: stop.SterileRootSpan},
			func(ctx context.Context) {
				waitQuiesce(ctx)
			}); err != nil {
			waitQuiesce(ctx)
			return err
		}

		if err := stopper.RunAsyncTaskEx(ctx,
			stop.TaskOpts{TaskName: "unix-listener", SpanOpt: stop.SterileRootSpan},
			func(ctx context.Context) {
				err := connManager.ServeWith(ctx, unixLn, func(ctx context.Context, conn net.Conn) {
					connCtx := pgPreServer.AnnotateCtxForIncomingConn(ctx, conn)

					conn, status, err := pgPreServer.PreServe(connCtx, conn, pgwire.SocketUnix)
					if err != nil {
						log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
						return
					}

					if err := serveConn(connCtx, conn, status); err != nil {
						log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
					}
				})
				netutil.FatalIfUnexpected(err)
			}); err != nil {
			return err
		}
	}

	return nil
}

const noLock lockfile.Lockfile = ""

func prepareUnixSocket(
	ctx context.Context, pgL net.Listener, socketFileCfg *string,
) (socketFile string, socketLock lockfile.Lockfile, err error) {
	socketFile = ""
	if socketFileCfg != nil {
		socketFile = *socketFileCfg
	}
	if len(socketFile) == 0 {
		// No socket configured. Nothing to do.
		return "", noLock, nil
	}

	if strings.HasSuffix(socketFile, ".0") {
		// Either a test explicitly set the SocketFile parameter to "xxx.0", or
		// the top-level 'start'  command was given a port number 0 to --listen-addr
		// (means: auto-allocate TCP port number).
		// In either case, this is an instruction for us to generate
		// a socket name after the TCP port automatically.
		tcpAddr := pgL.Addr()
		_, port, err := addr.SplitHostPort(tcpAddr.String(), "")
		if err != nil {
			return "", noLock, errors.Wrapf(err, "extracting port from SQL addr %q", tcpAddr)
		}
		socketFile = socketFile[:len(socketFile)-1] + port
		if socketFileCfg != nil {
			// Remember the computed value for reporting in the top-level
			// start command.
			*socketFileCfg = socketFile
		}
	}

	// Use a socket lock mechanism to ensure we reuse the socket only
	// if it's safe to do so (there's no owner any more).
	lockPath := socketFile + ".lock"
	// The lockfile package wants an absolute path.
	absLockPath, err := filepath.Abs(lockPath)
	if err != nil {
		return "", noLock, errors.Wrap(err, "computing absolute path for unix socket")
	}
	socketLock, err = lockfile.New(absLockPath)
	if err != nil {
		// This should only fail on non-absolute paths, but we
		// just made it absolute above.
		return "", noLock, errors.NewAssertionErrorWithWrappedErrf(err, "creating lock")
	}
	if err := socketLock.TryLock(); err != nil {
		if owner, ownerErr := socketLock.GetOwner(); ownerErr == nil {
			err = errors.WithHintf(err, "Socket appears locked by process %d.", owner.Pid)
		}
		return "", noLock, errors.Wrapf(err, "locking unix socket %q", socketFile)
	}

	// Now the lock has succeeded, we can delete the previous socket
	// if it exists.
	if _, err := os.Stat(socketFile); err != nil {
		if !oserror.IsNotExist(err) {
			// Socket exists but there's some file access error.
			// we probably can't remove it.
			return "", noLock, errors.CombineErrors(err, socketLock.Unlock())
		}
	} else {
		// Socket file existed already.
		if err := os.Remove(socketFile); err != nil {
			return "", noLock, errors.CombineErrors(
				errors.Wrap(err, "removing previous socket file"),
				socketLock.Unlock())
		}
	}

	return socketFile, socketLock, nil
}

// LogicalClusterID retrieves the logical (tenant-level) cluster ID
// for this server.
func (s *SQLServer) LogicalClusterID() uuid.UUID {
	return s.execCfg.NodeInfo.LogicalClusterID()
}

// ShutdownRequested returns a channel that is signaled when a subsystem wants
// the server to be shut down.
func (s *SQLServer) ShutdownRequested() <-chan ShutdownRequest {
	return s.stopTrigger.C()
}
