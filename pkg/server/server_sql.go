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
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/migration/migrationcluster"
	"github.com/cockroachdb/cockroach/pkg/migration/migrationmanager"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/server/tracedumper"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob/gcjobnotifier"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/scheduledlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instanceprovider"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slprovider"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/collector"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/service"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingservicepb"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
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
	sqlIDContainer   *base.SQLIDContainer
	pgServer         *pgwire.Server
	distSQLServer    *distsql.ServerImpl
	execCfg          *sql.ExecutorConfig
	cfg              *BaseConfig
	internalExecutor *sql.InternalExecutor
	leaseMgr         *lease.Manager
	blobService      *blobs.Service
	tracingService   *service.Service
	tenantConnect    kvtenant.Connector
	// sessionRegistry can be queried for info on running SQL sessions. It is
	// shared between the sql.Server and the statusServer.
	sessionRegistry        *sql.SessionRegistry
	jobRegistry            *jobs.Registry
	startupMigrationsMgr   *startupmigrations.Manager
	statsRefresher         *stats.Refresher
	temporaryObjectCleaner *sql.TemporaryObjectCleaner
	internalMemMetrics     sql.MemoryMetrics
	// sqlMemMetrics are used to track memory usage of sql sessions.
	sqlMemMetrics           sql.MemoryMetrics
	stmtDiagnosticsRegistry *stmtdiagnostics.Registry
	// sqlLivenessSessionID will be populated with a non-zero value for non-system
	// tenants.
	sqlLivenessSessionID    sqlliveness.SessionID
	sqlLivenessProvider     sqlliveness.Provider
	sqlInstanceProvider     sqlinstance.Provider
	metricsRegistry         *metric.Registry
	diagnosticsReporter     *diagnostics.Reporter
	spanconfigMgr           *spanconfigmanager.Manager
	spanconfigSQLTranslator *spanconfigsqltranslator.SQLTranslator
	spanconfigSQLWatcher    *spanconfigsqlwatcher.SQLWatcher
	settingsWatcher         *settingswatcher.SettingsWatcher

	systemConfigWatcher *systemconfigwatcher.Cache

	isMeta1Leaseholder func(context.Context, hlc.ClockTimestamp) (bool, error)

	// pgL is the shared RPC/SQL listener, opened when RPC was initialized.
	pgL net.Listener
	// connManager is the connection manager to use to set up additional
	// SQL listeners in AcceptClients().
	connManager netutil.Server

	// isReady is the health status of the node. When true, the node is healthy;
	// load balancers and connection management tools treat the node as "ready".
	// When false, the node is unhealthy or "not ready", with load balancers and
	// connection management tools learning this status from health checks.
	// This is set to true when the server has started accepting client conns.
	isReady syncutil.AtomicBool
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
	// migration manager.
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
}

// sqlServerOptionalTenantArgs are the arguments supplied to newSQLServer which
// are only available if the SQL server runs as part of a standalone SQL node.
type sqlServerOptionalTenantArgs struct {
	tenantConnect kvtenant.Connector

	// advertiseAddr stores the SQL address that is advertised to other servers
	// of the same tenant.
	advertiseAddr string
}

type sqlServerArgs struct {
	sqlServerOptionalKVArgs
	sqlServerOptionalTenantArgs

	*SQLConfig
	*BaseConfig

	stopper *stop.Stopper

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

	// Used to track the DistSQL flows scheduled on this node but initiated on
	// behalf of other nodes.
	flowScheduler *flowinfra.FlowScheduler

	// KV depends on the internal executor, so we pass a pointer to an empty
	// struct in this configuration, which newSQLServer fills.
	//
	// TODO(tbg): make this less hacky.
	circularInternalExecutor *sql.InternalExecutor // empty initially

	// Stores and deletes expired liveness sessions.
	sqlLivenessProvider sqlliveness.Provider

	// Stores and manages sql instance information.
	sqlInstanceProvider sqlinstance.Provider

	// The protected timestamps KV subsystem depends on this, so we pass a
	// pointer to an empty struct in this configuration, which newSQLServer
	// fills.
	circularJobRegistry *jobs.Registry
	jobAdoptionStopFile string

	// The executorConfig uses the provider.
	protectedtsProvider protectedts.Provider

	// Used to list activity (sessions, queries, contention, DistSQL flows) on
	// the node/cluster and cancel sessions/queries.
	sqlStatusServer serverpb.SQLStatusServer

	// Used to watch settings and descriptor changes.
	rangeFeedFactory *rangefeed.Factory

	// Used to query valid regions on the server.
	regionsServer serverpb.RegionsServer

	// Used to query status information useful for debugging on the server.
	tenantStatusServer serverpb.TenantStatusServer

	// Used for multi-tenant cost control (on the host cluster side).
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

// newRootSQLMemoryMonitor returns a started BytesMonitor and corresponding
// metrics.
func newRootSQLMemoryMonitor(opts monitorAndMetricsOptions) monitorAndMetrics {
	rootSQLMetrics := sql.MakeBaseMemMetrics("root", opts.histogramWindowInterval)
	// We do not set memory monitors or a noteworthy limit because the children of
	// this monitor will be setting their own noteworthy limits.
	rootSQLMemoryMonitor := mon.NewMonitor(
		"root",
		mon.MemoryResource,
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
		context.Background(), nil, mon.MakeStandaloneBudget(opts.memoryPoolSize))
	return monitorAndMetrics{
		rootSQLMemoryMonitor: rootSQLMemoryMonitor,
		rootSQLMetrics:       rootSQLMetrics,
	}
}

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
	// Create blob service for inter-node file sharing.
	blobService, err := blobs.NewBlobService(cfg.Settings.ExternalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating blob service")
	}
	blobspb.RegisterBlobServer(cfg.grpcServer, blobService)

	// Create trace service for inter-node sharing of inflight trace spans.
	tracingService := service.New(cfg.Tracer)
	tracingservicepb.RegisterTracingServer(cfg.grpcServer, tracingService)

	sqllivenessKnobs, _ := cfg.TestingKnobs.SQLLivenessKnobs.(*sqlliveness.TestingKnobs)
	cfg.sqlLivenessProvider = slprovider.New(
		cfg.AmbientCtx,
		cfg.stopper, cfg.clock, cfg.db, codec, cfg.Settings, sqllivenessKnobs,
	)
	cfg.sqlInstanceProvider = instanceprovider.New(
		cfg.stopper, cfg.db, codec, cfg.sqlLivenessProvider, cfg.advertiseAddr, cfg.rangeFeedFactory, cfg.clock,
	)

	if !codec.ForSystemTenant() {
		// In a multi-tenant environment, use the sqlInstanceProvider to resolve
		// SQL pod addresses.
		addressResolver := func(nodeID roachpb.NodeID) (net.Addr, error) {
			if cfg.sqlInstanceProvider == nil {
				return nil, errors.Errorf("no sqlInstanceProvider")
			}
			info, err := cfg.sqlInstanceProvider.GetInstance(cfg.rpcContext.MasterCtx, base.SQLInstanceID(nodeID))
			if err != nil {
				return nil, errors.Errorf("unable to look up descriptor for nsql%d", nodeID)
			}
			return &util.UnresolvedAddr{AddressField: info.InstanceAddr}, nil
		}
		cfg.podNodeDialer = nodedialer.New(cfg.rpcContext, addressResolver)
	} else {
		cfg.podNodeDialer = cfg.nodeDialer
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
			cfg.db,
			cfg.circularInternalExecutor,
			cfg.nodeIDContainer,
			cfg.sqlLivenessProvider,
			cfg.Settings,
			cfg.HistogramWindowInterval(),
			func(opName string, user security.SQLUsername) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.MakeJobExecContext(opName, user, &sql.MemoryMetrics{}, execCfg)
			},
			cfg.jobAdoptionStopFile,
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
		cfg.db,
		cfg.clock,
		cfg.circularInternalExecutor,
		cfg.Settings,
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
	bulkMemoryMonitor.Start(context.Background(), rootSQLMemoryMonitor, mon.BoundAccount{})

	backfillMemoryMonitor := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "backfill-mon")
	backupMemoryMonitor := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "backup-mon")

	serverCacheMemoryMonitor := mon.NewMonitorInheritWithLimit(
		"server-cache-mon", 0 /* limit */, rootSQLMemoryMonitor,
	)
	serverCacheMemoryMonitor.Start(context.Background(), rootSQLMemoryMonitor, mon.BoundAccount{})

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

	hydratedTablesCache := hydratedtables.NewCache(cfg.Settings)
	cfg.registry.AddMetricStruct(hydratedTablesCache.Metrics())

	gcJobNotifier := gcjobnotifier.New(cfg.Settings, cfg.systemConfigWatcher, codec, cfg.stopper)

	var compactEngineSpanFunc tree.CompactEngineSpanFunc
	if !codec.ForSystemTenant() {
		compactEngineSpanFunc = func(
			ctx context.Context, nodeID, storeID int32, startKey, endKey []byte,
		) error {
			return errorutil.UnsupportedWithMultiTenancy(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
		}
	} else {
		cli := kvserver.NewCompactEngineSpanClient(cfg.nodeDialer)
		compactEngineSpanFunc = cli.CompactEngineSpan
	}

	collectionFactory := descs.NewCollectionFactory(
		cfg.Settings,
		leaseMgr,
		virtualSchemas,
		hydratedTablesCache,
	)

	// Set up the DistSQL server.
	distSQLCfg := execinfra.ServerConfig{
		AmbientContext: cfg.AmbientCtx,
		Settings:       cfg.Settings,
		RuntimeStats:   cfg.runtime,
		ClusterID:      cfg.rpcContext.ClusterID,
		ClusterName:    cfg.ClusterName,
		NodeID:         cfg.nodeIDContainer,
		Locality:       cfg.Locality,
		Codec:          codec,
		DB:             cfg.db,
		Executor:       cfg.circularInternalExecutor,
		RPCContext:     cfg.rpcContext,
		Stopper:        cfg.stopper,

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

		ParentMemoryMonitor: rootSQLMemoryMonitor,
		BulkAdder: func(
			ctx context.Context, db *kv.DB, ts hlc.Timestamp, opts kvserverbase.BulkAdderOptions,
		) (kvserverbase.BulkAdder, error) {
			// Attach a child memory monitor to enable control over the BulkAdder's
			// memory usage.
			bulkMon := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "bulk-adder-monitor")
			return bulk.MakeBulkAdder(ctx, db, cfg.distSender.RangeDescriptorCache(), cfg.Settings, ts, opts, bulkMon)
		},

		Metrics:            &distSQLMetrics,
		RowMetrics:         &rowMetrics,
		InternalRowMetrics: &internalRowMetrics,

		SQLLivenessReader: cfg.sqlLivenessProvider,
		JobRegistry:       jobRegistry,
		Gossip:            cfg.gossip,
		NodeDialer:        cfg.nodeDialer,
		PodNodeDialer:     cfg.podNodeDialer,
		LeaseManager:      leaseMgr,

		ExternalStorage:        cfg.externalStorage,
		ExternalStorageFromURI: cfg.externalStorageFromURI,

		DistSender:               cfg.distSender,
		RangeCache:               cfg.distSender.RangeDescriptorCache(),
		SQLSQLResponseAdmissionQ: cfg.sqlSQLResponseAdmissionQ,
		CollectionFactory:        collectionFactory,
	}
	cfg.TempStorageConfig.Mon.SetMetrics(distSQLMetrics.CurDiskBytesCount, distSQLMetrics.MaxDiskBytesHist)
	if distSQLTestingKnobs := cfg.TestingKnobs.DistSQL; distSQLTestingKnobs != nil {
		distSQLCfg.TestingKnobs = *distSQLTestingKnobs.(*execinfra.TestingKnobs)
	}
	if cfg.TestingKnobs.JobsTestingKnobs != nil {
		distSQLCfg.TestingKnobs.JobsTestingKnobs = cfg.TestingKnobs.JobsTestingKnobs
	}

	distSQLServer := distsql.NewServer(ctx, distSQLCfg, cfg.flowScheduler)
	execinfrapb.RegisterDistSQLServer(cfg.grpcServer, distSQLServer)

	// Set up Executor

	var sqlExecutorTestingKnobs sql.ExecutorTestingKnobs
	if k := cfg.TestingKnobs.SQLExecutor; k != nil {
		sqlExecutorTestingKnobs = *k.(*sql.ExecutorTestingKnobs)
	} else {
		sqlExecutorTestingKnobs = sql.ExecutorTestingKnobs{}
	}

	nodeInfo := sql.NodeInfo{
		AdminURL:  cfg.AdminURL,
		PGURL:     cfg.rpcContext.PGURL,
		ClusterID: cfg.rpcContext.ClusterID.Get,
		NodeID:    cfg.nodeIDContainer,
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
			return nodeLiveness.IsAvailable(roachpb.NodeID(sqlInstanceID))
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
	var traceCollector *collector.TraceCollector
	if hasNodeLiveness {
		traceCollector = collector.New(cfg.nodeDialer, nodeLiveness, cfg.Tracer)
	}
	contentionRegistry := contention.NewRegistry(cfg.Settings, cfg.sqlStatusServer.TxnIDResolution)
	contentionRegistry.Start(ctx, cfg.stopper)

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
		RegionsServer:           cfg.regionsServer,
		SessionRegistry:         cfg.sessionRegistry,
		ContentionRegistry:      contentionRegistry,
		SQLLiveness:             cfg.sqlLivenessProvider,
		JobRegistry:             jobRegistry,
		VirtualSchemas:          virtualSchemas,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		RangeDescriptorCache:    cfg.distSender.RangeDescriptorCache(),
		RoleMemberCache:         sql.NewMembershipCache(serverCacheMemoryMonitor.MakeBoundAccount()),
		SessionInitCache:        sessioninit.NewCache(serverCacheMemoryMonitor.MakeBoundAccount()),
		RootMemoryMonitor:       rootSQLMemoryMonitor,
		TestingKnobs:            sqlExecutorTestingKnobs,
		CompactEngineSpanFunc:   compactEngineSpanFunc,
		TraceCollector:          traceCollector,
		TenantUsageServer:       cfg.tenantUsageServer,

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
			cfg.nodeDialer,
			cfg.podNodeDialer,
			codec,
			cfg.sqlInstanceProvider,
		),

		TableStatsCache: stats.NewTableStatisticsCache(
			ctx,
			cfg.TableStatCacheSize,
			cfg.db,
			cfg.circularInternalExecutor,
			codec,
			cfg.Settings,
			cfg.rangeFeedFactory,
			collectionFactory,
		),

		QueryCache:                 querycache.New(cfg.QueryCacheSize),
		RowMetrics:                 &rowMetrics,
		InternalRowMetrics:         &internalRowMetrics,
		ProtectedTimestampProvider: cfg.protectedtsProvider,
		ExternalIODirConfig:        cfg.ExternalIODirConfig,
		GCJobNotifier:              gcJobNotifier,
		RangeFeedFactory:           cfg.rangeFeedFactory,
		CollectionFactory:          collectionFactory,
		SystemTableIDResolver:      descs.MakeSystemTableIDResolver(collectionFactory, cfg.circularInternalExecutor, cfg.db),
	}

	if sqlSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLSchemaChanger; sqlSchemaChangerTestingKnobs != nil {
		execCfg.SchemaChangerTestingKnobs = sqlSchemaChangerTestingKnobs.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = new(sql.SchemaChangerTestingKnobs)
	}
	if declarativeSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLDeclarativeSchemaChanger; declarativeSchemaChangerTestingKnobs != nil {
		execCfg.DeclarativeSchemaChangerTestingKnobs = declarativeSchemaChangerTestingKnobs.(*scrun.TestingKnobs)
	} else {
		execCfg.DeclarativeSchemaChangerTestingKnobs = new(scrun.TestingKnobs)
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
		execCfg.EvalContextTestingKnobs = *sqlEvalContext.(*tree.EvalContextTestingKnobs)
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
	if ttlKnobs := cfg.TestingKnobs.TTL; ttlKnobs != nil {
		execCfg.TTLTestingKnobs = ttlKnobs.(*sql.TTLTestingKnobs)
	}
	if sqlStatsKnobs := cfg.TestingKnobs.SQLStatsKnobs; sqlStatsKnobs != nil {
		execCfg.SQLStatsTestingKnobs = sqlStatsKnobs.(*sqlstats.TestingKnobs)
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
	distSQLServer.ServerConfig.IndexUsageStatsController = pgServer.SQLServer.GetIndexUsageStatsController()

	// Now that we have a pgwire.Server (which has a sql.Server), we can close a
	// circular dependency between the rowexec.Server and sql.Server and set
	// SessionBoundInternalExecutorFactory. The same applies for setting a
	// SessionBoundInternalExecutor on the job registry.
	ieFactory := func(
		ctx context.Context, sessionData *sessiondata.SessionData,
	) sqlutil.InternalExecutor {
		ie := sql.MakeInternalExecutor(
			ctx,
			pgServer.SQLServer,
			internalMemMetrics,
			cfg.Settings,
		)
		ie.SetSessionData(sessionData)
		return &ie
	}

	distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory = ieFactory
	jobRegistry.SetSessionBoundInternalExecutorFactory(ieFactory)
	execCfg.IndexBackfiller = sql.NewIndexBackfiller(execCfg, ieFactory)
	execCfg.IndexValidator = scdeps.NewIndexValidator(
		execCfg.DB,
		execCfg.Codec,
		execCfg.Settings,
		ieFactory,
		sql.ValidateForwardIndexes,
		sql.ValidateInvertedIndexes,
		sql.NewFakeSessionData,
	)

	execCfg.DescMetadaUpdaterFactory = descmetadata.NewMetadataUpdaterFactory(
		ieFactory,
		collectionFactory,
		&execCfg.Settings.SV,
	)
	execCfg.InternalExecutorFactory = ieFactory

	distSQLServer.ServerConfig.ProtectedTimestampProvider = execCfg.ProtectedTimestampProvider

	for _, m := range pgServer.Metrics() {
		cfg.registry.AddMetricStruct(m)
	}
	*cfg.circularInternalExecutor = sql.MakeInternalExecutor(
		ctx, pgServer.SQLServer, internalMemMetrics, cfg.Settings,
	)
	execCfg.InternalExecutor = cfg.circularInternalExecutor
	stmtDiagnosticsRegistry := stmtdiagnostics.NewRegistry(
		cfg.circularInternalExecutor,
		cfg.db,
		cfg.gossip,
		cfg.Settings,
	)
	execCfg.StmtDiagnosticsRecorder = stmtDiagnosticsRegistry

	{
		// We only need to attach a version upgrade hook if we're the system
		// tenant. Regular tenants are disallowed from changing cluster
		// versions.
		var c migration.Cluster
		var systemDeps migration.SystemDeps
		if codec.ForSystemTenant() {
			c = migrationcluster.New(migrationcluster.ClusterConfig{
				NodeLiveness: nodeLiveness,
				Dialer:       cfg.nodeDialer,
				DB:           cfg.db,
			})
			systemDeps = migration.SystemDeps{
				Cluster:    c,
				DB:         cfg.db,
				DistSender: cfg.distSender,
				Stopper:    cfg.stopper,
			}
		} else {
			c = migrationcluster.NewTenantCluster(cfg.db)
			systemDeps = migration.SystemDeps{
				Cluster: c,
				DB:      cfg.db,
			}
		}

		knobs, _ := cfg.TestingKnobs.MigrationManager.(*migration.TestingKnobs)
		migrationMgr := migrationmanager.NewManager(
			systemDeps, leaseMgr, cfg.circularInternalExecutor, jobRegistry, codec,
			cfg.Settings, knobs,
		)
		execCfg.MigrationJobDeps = migrationMgr
		execCfg.VersionUpgradeHook = migrationMgr.Migrate
		execCfg.MigrationTestingKnobs = knobs
	}

	spanConfig := struct {
		manager       *spanconfigmanager.Manager
		sqlTranslator *spanconfigsqltranslator.SQLTranslator
		sqlWatcher    *spanconfigsqlwatcher.SQLWatcher
	}{}
	if !codec.ForSystemTenant() || !cfg.SpanConfigsDisabled {
		// Instantiate a span config manager. If we're the host tenant we'll
		// only do it unless COCKROACH_DISABLE_SPAN_CONFIGS is set.
		spanConfigKnobs, _ := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs)
		spanConfig.sqlTranslator = spanconfigsqltranslator.New(execCfg, codec, spanConfigKnobs)
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
			spanConfig.sqlTranslator,
			cfg.spanConfigAccessor,
			execCfg,
			codec,
			cfg.TenantID,
			spanConfigKnobs,
		)
		spanConfig.manager = spanconfigmanager.New(
			cfg.db,
			jobRegistry,
			cfg.circularInternalExecutor,
			cfg.stopper,
			cfg.Settings,
			spanConfigReconciler,
			spanConfigKnobs,
		)

		execCfg.SpanConfigReconciler = spanConfigReconciler
	}
	execCfg.SpanConfigKVAccessor = cfg.sqlServerOptionalKVArgs.spanConfigKVAccessor

	temporaryObjectCleaner := sql.NewTemporaryObjectCleaner(
		cfg.Settings,
		cfg.db,
		codec,
		cfg.registry,
		distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory,
		cfg.sqlStatusServer,
		cfg.isMeta1Leaseholder,
		sqlExecutorTestingKnobs,
		collectionFactory,
	)

	reporter := &diagnostics.Reporter{
		StartTime:     timeutil.Now(),
		AmbientCtx:    &cfg.AmbientCtx,
		Config:        cfg.BaseConfig.Config,
		Settings:      cfg.Settings,
		ClusterID:     cfg.rpcContext.ClusterID.Get,
		TenantID:      cfg.rpcContext.TenantID,
		SQLInstanceID: cfg.nodeIDContainer.SQLInstanceID,
		SQLServer:     pgServer.SQLServer,
		InternalExec:  cfg.circularInternalExecutor,
		DB:            cfg.db,
		Recorder:      cfg.recorder,
		Locality:      cfg.Locality,
	}

	if cfg.TestingKnobs.Server != nil {
		reporter.TestingKnobs = &cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

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

	return &SQLServer{
		ambientCtx:              cfg.BaseConfig.AmbientCtx,
		stopper:                 cfg.stopper,
		sqlIDContainer:          cfg.nodeIDContainer,
		pgServer:                pgServer,
		distSQLServer:           distSQLServer,
		execCfg:                 execCfg,
		internalExecutor:        cfg.circularInternalExecutor,
		leaseMgr:                leaseMgr,
		blobService:             blobService,
		tracingService:          tracingService,
		tenantConnect:           cfg.tenantConnect,
		sessionRegistry:         cfg.sessionRegistry,
		jobRegistry:             jobRegistry,
		statsRefresher:          statsRefresher,
		temporaryObjectCleaner:  temporaryObjectCleaner,
		internalMemMetrics:      internalMemMetrics,
		sqlMemMetrics:           sqlMemMetrics,
		stmtDiagnosticsRegistry: stmtDiagnosticsRegistry,
		sqlLivenessProvider:     cfg.sqlLivenessProvider,
		sqlInstanceProvider:     cfg.sqlInstanceProvider,
		metricsRegistry:         cfg.registry,
		diagnosticsReporter:     reporter,
		spanconfigMgr:           spanConfig.manager,
		spanconfigSQLTranslator: spanConfig.sqlTranslator,
		spanconfigSQLWatcher:    spanConfig.sqlWatcher,
		settingsWatcher:         settingsWatcher,
		systemConfigWatcher:     cfg.systemConfigWatcher,
		isMeta1Leaseholder:      cfg.isMeta1Leaseholder,
		cfg:                     cfg.BaseConfig,
	}, nil
}

// Checks if tenant exists. This function does a very superficial check to see if the system db
// has been bootstrapped for the tenant. This is not a complete check and is only sufficient
// to be used in the dev environment.
func maybeCheckTenantExists(ctx context.Context, codec keys.SQLCodec, db *kv.DB) error {
	if codec.ForSystemTenant() {
		// Skip check for system tenant and return early.
		return nil
	}
	key := catalogkeys.MakeDatabaseNameKey(codec, systemschema.SystemDatabaseName)
	result, err := db.Get(ctx, key)
	if err != nil {
		return err
	}
	if result.Value == nil || result.ValueInt() != keys.SystemDatabaseID {
		return errors.New("system DB uninitialized, check if tenant is non existent")
	}
	// Tenant has been confirmed to be bootstrapped successfully
	// as the system database, which is a part of the bootstrap data for
	// a tenant keyspace, exists in the namespace table.
	return nil
}

func (s *SQLServer) startSQLLivenessAndInstanceProviders(ctx context.Context) error {
	// If necessary, start the tenant proxy first, to ensure all other
	// components can properly route to KV nodes. The Start method will block
	// until a connection is established to the cluster and its ID has been
	// determined.
	if s.tenantConnect != nil {
		if err := s.tenantConnect.Start(ctx); err != nil {
			return err
		}
	}
	s.sqlLivenessProvider.Start(ctx)
	// sqlInstanceProvider must always be started after sqlLivenessProvider
	// as sqlInstanceProvider relies on the session initialized and maintained by
	// sqlLivenessProvider.
	if err := s.sqlInstanceProvider.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SQLServer) initInstanceID(ctx context.Context) error {
	if _, ok := s.sqlIDContainer.OptionalNodeID(); ok {
		// sqlIDContainer has already been initialized with a node ID,
		// we don't need to initialize a SQL instance ID in this case
		// as this is not a SQL pod server.
		return nil
	}
	instanceID, sessionID, err := s.sqlInstanceProvider.Instance(ctx)
	if err != nil {
		return err
	}
	err = s.sqlIDContainer.SetSQLInstanceID(ctx, instanceID)
	if err != nil {
		return err
	}
	s.sqlLivenessSessionID = sessionID
	s.execCfg.DistSQLPlanner.SetSQLInstanceInfo(roachpb.NodeDescriptor{NodeID: roachpb.NodeID(instanceID)})
	return nil
}

func (s *SQLServer) preStart(
	ctx context.Context,
	stopper *stop.Stopper,
	knobs base.TestingKnobs,
	connManager netutil.Server,
	pgL net.Listener,
	socketFile string,
	orphanedLeasesTimeThresholdNanos int64,
) error {
	// The sqlliveness and sqlinstance subsystem should be started first to ensure instance ID is
	// initialized prior to any other systems that need it.
	if err := s.startSQLLivenessAndInstanceProviders(ctx); err != nil {
		return err
	}
	// Confirm tenant exists prior to initialization. This is a sanity
	// check for the dev environment to ensure that a tenant has been
	// successfully created before attempting to initialize a SQL
	// server for it.
	if err := maybeCheckTenantExists(ctx, s.execCfg.Codec, s.execCfg.DB); err != nil {
		return err
	}
	if err := s.initInstanceID(ctx); err != nil {
		return err
	}
	s.connManager = connManager
	s.pgL = pgL
	s.execCfg.GCJobNotifier.Start(ctx)
	s.temporaryObjectCleaner.Start(ctx, stopper)
	s.distSQLServer.Start()
	s.pgServer.Start(ctx, stopper)
	if err := s.statsRefresher.Start(ctx, stopper, stats.DefaultRefreshInterval); err != nil {
		return err
	}
	s.stmtDiagnosticsRegistry.Start(ctx, stopper)

	// Before serving SQL requests, we have to make sure the database is
	// in an acceptable form for this version of the software.
	// We have to do this after actually starting up the server to be able to
	// seamlessly use the kv client against other nodes in the cluster.
	var mmKnobs startupmigrations.MigrationManagerTestingKnobs
	if migrationManagerTestingKnobs := knobs.StartupMigrationManager; migrationManagerTestingKnobs != nil {
		mmKnobs = *migrationManagerTestingKnobs.(*startupmigrations.MigrationManagerTestingKnobs)
	}

	s.leaseMgr.RefreshLeases(ctx, stopper, s.execCfg.DB)
	s.leaseMgr.PeriodicallyRefreshSomeLeases(ctx)

	migrationsExecutor := sql.MakeInternalExecutor(
		ctx, s.pgServer.SQLServer, s.internalMemMetrics, s.execCfg.Settings)
	migrationsExecutor.SetSessionData(
		&sessiondata.SessionData{
			LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
				// Migrations need an executor with query distribution turned off. This is
				// because the node crashes if migrations fail to execute, and query
				// distribution introduces more moving parts. Local execution is more
				// robust; for example, the DistSender has retries if it can't connect to
				// another node, but DistSQL doesn't. Also see #44101 for why DistSQL is
				// particularly fragile immediately after a node is started (i.e. the
				// present situation).
				DistSQLMode: sessiondatapb.DistSQLOff,
			},
		})
	startupMigrationsMgr := startupmigrations.NewManager(
		stopper,
		s.execCfg.DB,
		s.execCfg.Codec,
		&migrationsExecutor,
		s.execCfg.Clock,
		mmKnobs,
		s.execCfg.NodeID.SQLInstanceID().String(),
		s.execCfg.Settings,
		s.jobRegistry,
	)
	s.startupMigrationsMgr = startupMigrationsMgr // only for testing via TestServer

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
		if err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return txn.GetProto(ctx, keys.BootstrapVersionKey, &bootstrapVersion)
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
		// superfluous but necessarily idempotent SQL migrations, so at worst, we're
		// doing more work than strictly necessary during the first time that the
		// migrations are run.
		bootstrapVersion = roachpb.Version{Major: 20, Minor: 1, Internal: 1}
	}

	if err := s.settingsWatcher.Start(ctx); err != nil {
		return errors.Wrap(err, "initializing settings")
	}
	if err := s.systemConfigWatcher.Start(ctx, s.stopper); err != nil {
		return errors.Wrap(err, "initializing settings")
	}

	// Run startup migrations (note: these depend on jobs subsystem running).
	if err := startupMigrationsMgr.EnsureMigrations(ctx, bootstrapVersion); err != nil {
		return errors.Wrap(err, "ensuring SQL migrations")
	}

	log.Infof(ctx, "done ensuring all necessary startup migrations have run")

	// Delete all orphaned table leases created by a prior instance of this
	// node. This also uses SQL.
	s.leaseMgr.DeleteOrphanedLeases(ctx, orphanedLeasesTimeThresholdNanos)

	// Start scheduled jobs daemon.
	jobs.StartJobSchedulerDaemon(
		ctx,
		stopper,
		s.metricsRegistry,
		&scheduledjobs.JobExecutionConfig{
			Settings:         s.execCfg.Settings,
			InternalExecutor: s.internalExecutor,
			DB:               s.execCfg.DB,
			TestingKnobs:     knobs.JobsTestingKnobs,
			PlanHookMaker: func(opName string, txn *kv.Txn, user security.SQLUsername) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.NewInternalPlanner(
					opName,
					txn,
					user,
					&sql.MemoryMetrics{},
					s.execCfg,
					sessiondatapb.SessionData{},
				)
			},
			ShouldRunScheduler: func(ctx context.Context, ts hlc.ClockTimestamp) (bool, error) {
				if s.execCfg.Codec.ForSystemTenant() {
					return s.isMeta1Leaseholder(ctx, ts)
				}
				return true, nil
			},
		},
		scheduledjobs.ProdJobSchedulerEnv,
	)

	scheduledlogging.Start(ctx, stopper, s.execCfg.DB, s.execCfg.Settings, s.internalExecutor, s.execCfg.CaptureIndexUsageStatsKnobs)
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
func (s *SQLServer) startServeSQL(
	ctx context.Context,
	stopper *stop.Stopper,
	connManager netutil.Server,
	pgL net.Listener,
	socketFile string,
) error {
	log.Ops.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
	tcpKeepAlive := makeTCPKeepAliveManager()

	_ = stopper.RunAsyncTaskEx(pgCtx,
		stop.TaskOpts{TaskName: "pgwire-listener", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			err := connManager.ServeWith(ctx, stopper, pgL, func(ctx context.Context, conn net.Conn) {
				connCtx := s.pgServer.AnnotateCtxForIncomingConn(ctx, conn)
				tcpKeepAlive.configure(connCtx, conn)

				if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketTCP); err != nil {
					log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
				}
			})
			netutil.FatalIfUnexpected(err)
		})

	// If a unix socket was requested, start serving there too.
	if len(socketFile) != 0 {
		log.Ops.Infof(ctx, "starting postgres server at unix:%s", socketFile)

		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", socketFile)
		if err != nil {
			return err
		}

		waitQuiesce := func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			if err := unixLn.Close(); err != nil {
				log.Ops.Fatalf(ctx, "%v", err)
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

		if err := stopper.RunAsyncTaskEx(pgCtx,
			stop.TaskOpts{TaskName: "unix-listener", SpanOpt: stop.SterileRootSpan},
			func(ctx context.Context) {
				err := connManager.ServeWith(ctx, stopper, unixLn, func(ctx context.Context, conn net.Conn) {
					connCtx := s.pgServer.AnnotateCtxForIncomingConn(ctx, conn)
					if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketUnix); err != nil {
						log.Ops.Errorf(connCtx, "%v", err)
					}
				})
				netutil.FatalIfUnexpected(err)
			}); err != nil {
			return err
		}
	}

	s.isReady.Set(true)

	return nil
}
