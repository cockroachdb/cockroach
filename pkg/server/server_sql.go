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
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob/gcjobnotifier"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slprovider"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	stopper          *stop.Stopper
	sqlIDContainer   *base.SQLIDContainer
	pgServer         *pgwire.Server
	distSQLServer    *distsql.ServerImpl
	execCfg          *sql.ExecutorConfig
	internalExecutor *sql.InternalExecutor
	leaseMgr         *lease.Manager
	blobService      *blobs.Service
	tenantConnect    kvtenant.Connector
	// sessionRegistry can be queried for info on running SQL sessions. It is
	// shared between the sql.Server and the statusServer.
	sessionRegistry        *sql.SessionRegistry
	jobRegistry            *jobs.Registry
	migMgr                 *sqlmigrations.Manager
	statsRefresher         *stats.Refresher
	temporaryObjectCleaner *sql.TemporaryObjectCleaner
	internalMemMetrics     sql.MemoryMetrics
	// sqlMemMetrics are used to track memory usage of sql sessions.
	sqlMemMetrics           sql.MemoryMetrics
	stmtDiagnosticsRegistry *stmtdiagnostics.Registry
	sqlLivenessProvider     sqlliveness.Provider
	metricsRegistry         *metric.Registry
	diagnosticsReporter     *diagnostics.Reporter
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
	// Narrowed down version of *NodeLiveness. Used by jobs and DistSQLPlanner
	nodeLiveness optionalnodeliveness.Container
	// Gossip is relied upon by distSQLCfg (execinfra.ServerConfig), the executor
	// config, the DistSQL planner, the table statistics cache, the statements
	// diagnostics registry, and the lease manager.
	gossip gossip.OptionalGossip
	// To register blob and DistSQL servers.
	grpcServer *grpc.Server
	// For the temporaryObjectCleaner.
	isMeta1Leaseholder func(context.Context, hlc.Timestamp) (bool, error)
	// DistSQL, lease management, and others want to know the node they're on.
	nodeIDContainer *base.SQLIDContainer

	// Used by backup/restore.
	externalStorage        cloud.ExternalStorageFactory
	externalStorageFromURI cloud.ExternalStorageFromURIFactory
}

// sqlServerOptionalTenantArgs are the arguments supplied to newSQLServer which
// are only available if the SQL server runs as part of a standalone SQL node.
type sqlServerOptionalTenantArgs struct {
	tenantConnect kvtenant.Connector
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
	systemConfigProvider config.SystemConfigProvider

	// Used by DistSQLPlanner.
	nodeDialer *nodedialer.Dialer

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

	// KV depends on the internal executor, so we pass a pointer to an empty
	// struct in this configuration, which newSQLServer fills.
	//
	// TODO(tbg): make this less hacky.
	circularInternalExecutor *sql.InternalExecutor // empty initially

	// Stores and deletes expired liveness sessions.
	sqlLivenessProvider sqlliveness.Provider

	// The protected timestamps KV subsystem depends on this, so we pass a
	// pointer to an empty struct in this configuration, which newSQLServer
	// fills.
	circularJobRegistry *jobs.Registry
	jobAdoptionStopFile string

	// The executorConfig uses the provider.
	protectedtsProvider protectedts.Provider

	// Used to list sessions and cancel sessions/queries.
	sqlStatusServer serverpb.SQLStatusServer
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

	jobRegistry := cfg.circularJobRegistry

	{
		regLiveness := cfg.nodeLiveness
		if testingLiveness := cfg.TestingKnobs.RegistryLiveness; testingLiveness != nil {
			regLiveness = optionalnodeliveness.MakeContainer(testingLiveness.(*jobs.FakeNodeLiveness))
		}

		cfg.sqlLivenessProvider = slprovider.New(
			cfg.stopper, cfg.clock, cfg.db, cfg.circularInternalExecutor, cfg.Settings,
		)
		cfg.registry.AddMetricStruct(cfg.sqlLivenessProvider.Metrics())

		*jobRegistry = *jobs.MakeRegistry(
			cfg.AmbientCtx,
			cfg.stopper,
			cfg.clock,
			regLiveness,
			cfg.db,
			cfg.circularInternalExecutor,
			cfg.nodeIDContainer,
			cfg.sqlLivenessProvider,
			cfg.Settings,
			cfg.HistogramWindowInterval(),
			func(opName, user string) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.NewInternalPlanner(opName, nil, user, &sql.MemoryMetrics{}, execCfg)
			},
			cfg.jobAdoptionStopFile,
		)
	}
	cfg.registry.AddMetricStruct(jobRegistry.MetricsStruct())

	distSQLMetrics := execinfra.MakeDistSQLMetrics(cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(distSQLMetrics)

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
		cfg.LeaseManagerConfig,
	)

	rootSQLMetrics := sql.MakeBaseMemMetrics("root", cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(rootSQLMetrics)

	// Set up internal memory metrics for use by internal SQL executors.
	internalMemMetrics := sql.MakeMemMetrics("internal", cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(internalMemMetrics)

	// We do not set memory monitors or a noteworthy limit because the children of
	// this monitor will be setting their own noteworthy limits.
	rootSQLMemoryMonitor := mon.NewMonitor(
		"root",
		mon.MemoryResource,
		rootSQLMetrics.CurBytesCount,
		rootSQLMetrics.MaxBytesHist,
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
		cfg.Settings,
	)
	rootSQLMemoryMonitor.Start(context.Background(), nil, mon.MakeStandaloneBudget(cfg.MemoryPoolSize))

	// bulkMemoryMonitor is the parent to all child SQL monitors tracking bulk
	// operations (IMPORT, index backfill). It is itself a child of the
	// ParentMemoryMonitor.
	bulkMemoryMonitor := mon.NewMonitorInheritWithLimit("bulk-mon", 0 /* limit */, rootSQLMemoryMonitor)
	bulkMetrics := bulk.MakeBulkMetrics(cfg.HistogramWindowInterval())
	cfg.registry.AddMetricStruct(bulkMetrics)
	bulkMemoryMonitor.SetMetrics(bulkMetrics.CurBytesCount, bulkMetrics.MaxBytesHist)
	bulkMemoryMonitor.Start(context.Background(), rootSQLMemoryMonitor, mon.BoundAccount{})

	backfillMemoryMonitor := execinfra.NewMonitor(ctx, bulkMemoryMonitor, "backfill-mon")

	// Set up the DistSQL temp engine.

	useStoreSpec := cfg.TempStorageConfig.Spec
	tempEngine, tempFS, err := storage.NewTempEngine(ctx, cfg.StorageEngine, cfg.TempStorageConfig, useStoreSpec)
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
			err = storage.CleanupTempDirs(recordPath)
		}
		if err != nil {
			log.Errorf(ctx, "could not remove temporary store directory: %v", err.Error())
		}
	}))

	hydratedTablesCache := hydratedtables.NewCache(cfg.Settings)
	cfg.registry.AddMetricStruct(hydratedTablesCache.Metrics())

	gcJobNotifier := gcjobnotifier.New(cfg.Settings, cfg.systemConfigProvider, codec, cfg.stopper)

	// Set up the DistSQL server.
	distSQLCfg := execinfra.ServerConfig{
		AmbientContext: cfg.AmbientCtx,
		Settings:       cfg.Settings,
		RuntimeStats:   cfg.runtime,
		ClusterID:      &cfg.rpcContext.ClusterID,
		ClusterName:    cfg.ClusterName,
		NodeID:         cfg.nodeIDContainer,
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
		DiskMonitor:       cfg.TempStorageConfig.Mon,
		BackfillerMonitor: backfillMemoryMonitor,

		ParentMemoryMonitor: rootSQLMemoryMonitor,
		BulkAdder: func(
			ctx context.Context, db *kv.DB, ts hlc.Timestamp, opts kvserverbase.BulkAdderOptions,
		) (kvserverbase.BulkAdder, error) {
			// Attach a child memory monitor to enable control over the BulkAdder's
			// memory usage.
			bulkMon := execinfra.NewMonitor(ctx, bulkMemoryMonitor, fmt.Sprintf("bulk-adder-monitor"))
			return bulk.MakeBulkAdder(ctx, db, cfg.distSender.RangeDescriptorCache(), cfg.Settings, ts, opts, bulkMon)
		},

		Metrics: &distSQLMetrics,

		SQLLivenessReader: cfg.sqlLivenessProvider,
		JobRegistry:       jobRegistry,
		Gossip:            cfg.gossip,
		NodeDialer:        cfg.nodeDialer,
		LeaseManager:      leaseMgr,

		ExternalStorage:        cfg.externalStorage,
		ExternalStorageFromURI: cfg.externalStorageFromURI,

		RangeCache:     cfg.distSender.RangeDescriptorCache(),
		HydratedTables: hydratedTablesCache,
	}
	cfg.TempStorageConfig.Mon.SetMetrics(distSQLMetrics.CurDiskBytesCount, distSQLMetrics.MaxDiskBytesHist)
	if distSQLTestingKnobs := cfg.TestingKnobs.DistSQL; distSQLTestingKnobs != nil {
		distSQLCfg.TestingKnobs = *distSQLTestingKnobs.(*execinfra.TestingKnobs)
	}
	if cfg.TestingKnobs.JobsTestingKnobs != nil {
		distSQLCfg.TestingKnobs.JobsTestingKnobs = cfg.TestingKnobs.JobsTestingKnobs
	}
	distSQLServer := distsql.NewServer(ctx, distSQLCfg)
	execinfrapb.RegisterDistSQLServer(cfg.grpcServer, distSQLServer)

	virtualSchemas, err := sql.NewVirtualSchemaHolder(ctx, cfg.Settings)
	if err != nil {
		return nil, errors.Wrap(err, "creating virtual schema holder")
	}

	// Set up Executor

	var sqlExecutorTestingKnobs sql.ExecutorTestingKnobs
	if k := cfg.TestingKnobs.SQLExecutor; k != nil {
		sqlExecutorTestingKnobs = *k.(*sql.ExecutorTestingKnobs)
	} else {
		sqlExecutorTestingKnobs = sql.ExecutorTestingKnobs{}
	}

	loggerCtx, _ := cfg.stopper.WithCancelOnStop(ctx)

	nodeInfo := sql.NodeInfo{
		AdminURL:  cfg.AdminURL,
		PGURL:     cfg.rpcContext.PGURL,
		ClusterID: cfg.rpcContext.ClusterID.Get,
		NodeID:    cfg.nodeIDContainer,
	}

	var isLive func(roachpb.NodeID) (bool, error)
	if nl, ok := cfg.nodeLiveness.Optional(47900); ok {
		isLive = nl.IsLive
	} else {
		// We're on a SQL tenant, so this is the only node DistSQL will ever
		// schedule on - always returning true is fine.
		isLive = func(roachpb.NodeID) (bool, error) {
			return true, nil
		}
	}

	*execCfg = sql.ExecutorConfig{
		Settings:                cfg.Settings,
		NodeInfo:                nodeInfo,
		Codec:                   codec,
		DefaultZoneConfig:       &cfg.DefaultZoneConfig,
		Locality:                cfg.Locality,
		AmbientCtx:              cfg.AmbientCtx,
		DB:                      cfg.db,
		Gossip:                  cfg.gossip,
		SystemConfig:            cfg.systemConfigProvider,
		MetricsRecorder:         cfg.recorder,
		DistSender:              cfg.distSender,
		RPCContext:              cfg.rpcContext,
		LeaseManager:            leaseMgr,
		Clock:                   cfg.clock,
		DistSQLSrv:              distSQLServer,
		NodesStatusServer:       cfg.nodesStatusServer,
		SQLStatusServer:         cfg.sqlStatusServer,
		SessionRegistry:         cfg.sessionRegistry,
		SQLLivenessReader:       cfg.sqlLivenessProvider,
		JobRegistry:             jobRegistry,
		VirtualSchemas:          virtualSchemas,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		RangeDescriptorCache:    cfg.distSender.RangeDescriptorCache(),
		RoleMemberCache:         &sql.MembershipCache{},
		TestingKnobs:            sqlExecutorTestingKnobs,

		DistSQLPlanner: sql.NewDistSQLPlanner(
			ctx,
			execinfra.Version,
			cfg.Settings,
			roachpb.NodeID(cfg.nodeIDContainer.SQLInstanceID()),
			cfg.rpcContext,
			distSQLServer,
			cfg.distSender,
			cfg.nodeDescs,
			cfg.gossip,
			cfg.stopper,
			isLive,
			cfg.nodeDialer,
		),

		TableStatsCache: stats.NewTableStatisticsCache(
			cfg.TableStatCacheSize,
			cfg.gossip,
			cfg.db,
			cfg.circularInternalExecutor,
			codec,
			leaseMgr,
			cfg.Settings,
		),

		// Note: don't forget to add the secondary loggers as closers
		// on the Stopper, below.

		ExecLogger: log.NewSecondaryLogger(
			loggerCtx, nil /* dirName */, "sql-exec",
			true /* enableGc */, false /* forceSyncWrites */, true, /* enableMsgCount */
		),

		// Note: the auth logger uses sync writes because we don't want an
		// attacker to easily "erase their traces" after an attack by
		// crashing the server before it has a chance to write the last
		// few log lines to disk.
		//
		// TODO(knz): We could worry about disk I/O activity incurred by
		// logging here in case a malicious user spams the server with
		// (failing) connection attempts to cause a DoS failure; this
		// would be a good reason to invest into a syslog sink for logs.
		AuthLogger: log.NewSecondaryLogger(
			loggerCtx, nil /* dirName */, "auth",
			true /* enableGc */, true /* forceSyncWrites */, true, /* enableMsgCount */
		),

		// AuditLogger syncs to disk for the same reason as AuthLogger.
		AuditLogger: log.NewSecondaryLogger(
			loggerCtx, cfg.AuditLogDirName, "sql-audit",
			true /* enableGc */, true /* forceSyncWrites */, true, /* enableMsgCount */
		),

		SlowQueryLogger: log.NewSecondaryLogger(
			loggerCtx, nil, "sql-slow",
			true /* enableGc */, false /* forceSyncWrites */, true, /* enableMsgCount */
		),

		SlowInternalQueryLogger: log.NewSecondaryLogger(loggerCtx, nil, "sql-slow-internal-only",
			true /* enableGc */, false /* forceSyncWrites */, true /* enableMsgCount */),

		QueryCache:                 querycache.New(cfg.QueryCacheSize),
		ProtectedTimestampProvider: cfg.protectedtsProvider,
		ExternalIODirConfig:        cfg.ExternalIODirConfig,
		HydratedTables:             hydratedTablesCache,
		GCJobNotifier:              gcJobNotifier,
	}

	cfg.stopper.AddCloser(execCfg.ExecLogger)
	cfg.stopper.AddCloser(execCfg.AuditLogger)
	cfg.stopper.AddCloser(execCfg.SlowQueryLogger)
	cfg.stopper.AddCloser(execCfg.SlowInternalQueryLogger)
	cfg.stopper.AddCloser(execCfg.AuthLogger)

	if sqlSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLSchemaChanger; sqlSchemaChangerTestingKnobs != nil {
		execCfg.SchemaChangerTestingKnobs = sqlSchemaChangerTestingKnobs.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = new(sql.SchemaChangerTestingKnobs)
	}
	if sqlTypeSchemaChangerTestingKnobs := cfg.TestingKnobs.SQLTypeSchemaChanger; sqlTypeSchemaChangerTestingKnobs != nil {
		execCfg.TypeSchemaChangerTestingKnobs = sqlTypeSchemaChangerTestingKnobs.(*sql.TypeSchemaChangerTestingKnobs)
	} else {
		execCfg.TypeSchemaChangerTestingKnobs = new(sql.TypeSchemaChangerTestingKnobs)
	}
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

	statsRefresher := stats.MakeRefresher(
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

	// Now that we have a pgwire.Server (which has a sql.Server), we can close a
	// circular dependency between the rowexec.Server and sql.Server and set
	// SessionBoundInternalExecutorFactory. The same applies for setting a
	// SessionBoundInternalExecutor on the the job registry.
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

	temporaryObjectCleaner := sql.NewTemporaryObjectCleaner(
		cfg.Settings,
		cfg.db,
		codec,
		cfg.registry,
		distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory,
		cfg.sqlStatusServer,
		cfg.isMeta1Leaseholder,
		sqlExecutorTestingKnobs,
	)

	var reporter *diagnostics.Reporter
	if cfg.tenantConnect != nil {
		reporter = &diagnostics.Reporter{
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
	}

	return &SQLServer{
		stopper:                 cfg.stopper,
		sqlIDContainer:          cfg.nodeIDContainer,
		pgServer:                pgServer,
		distSQLServer:           distSQLServer,
		execCfg:                 execCfg,
		internalExecutor:        cfg.circularInternalExecutor,
		leaseMgr:                leaseMgr,
		blobService:             blobService,
		tenantConnect:           cfg.tenantConnect,
		sessionRegistry:         cfg.sessionRegistry,
		jobRegistry:             jobRegistry,
		statsRefresher:          statsRefresher,
		temporaryObjectCleaner:  temporaryObjectCleaner,
		internalMemMetrics:      internalMemMetrics,
		sqlMemMetrics:           sqlMemMetrics,
		stmtDiagnosticsRegistry: stmtDiagnosticsRegistry,
		sqlLivenessProvider:     cfg.sqlLivenessProvider,
		metricsRegistry:         cfg.registry,
		diagnosticsReporter:     reporter,
	}, nil
}

func (s *SQLServer) start(
	ctx context.Context,
	stopper *stop.Stopper,
	knobs base.TestingKnobs,
	connManager netutil.Server,
	pgL net.Listener,
	socketFile string,
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
	s.sqlLivenessProvider.Start(ctx)
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
	var mmKnobs sqlmigrations.MigrationManagerTestingKnobs
	if migrationManagerTestingKnobs := knobs.SQLMigrationManager; migrationManagerTestingKnobs != nil {
		mmKnobs = *migrationManagerTestingKnobs.(*sqlmigrations.MigrationManagerTestingKnobs)
	}

	s.leaseMgr.RefreshLeases(ctx, stopper, s.execCfg.DB, s.execCfg.Gossip)
	s.leaseMgr.PeriodicallyRefreshSomeLeases(ctx)

	migrationsExecutor := sql.MakeInternalExecutor(
		ctx, s.pgServer.SQLServer, s.internalMemMetrics, s.execCfg.Settings)
	migrationsExecutor.SetSessionData(
		&sessiondata.SessionData{
			// Migrations need an executor with query distribution turned off. This is
			// because the node crashes if migrations fail to execute, and query
			// distribution introduces more moving parts. Local execution is more
			// robust; for example, the DistSender has retries if it can't connect to
			// another node, but DistSQL doesn't. Also see #44101 for why DistSQL is
			// particularly fragile immediately after a node is started (i.e. the
			// present situation).
			DistSQLMode: sessiondata.DistSQLOff,
		})
	migMgr := sqlmigrations.NewManager(
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
	s.migMgr = migMgr // only for testing via TestServer

	if err := s.jobRegistry.Start(
		ctx, stopper, jobs.DefaultCancelInterval, jobs.DefaultAdoptInterval,
	); err != nil {
		return err
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
		// pass VersionStart20_2. bootstrapVersion is only used to avoid performing
		// superfluous but necessarily idempotent SQL migrations, so at worst, we're
		// doing more work than strictly necessary during the first time that the
		// migrations are run.
		bootstrapVersion = clusterversion.VersionByKey(clusterversion.VersionStart20_2)
	}

	// Run startup migrations (note: these depend on jobs subsystem running).
	if err := migMgr.EnsureMigrations(ctx, bootstrapVersion); err != nil {
		return errors.Wrap(err, "ensuring SQL migrations")
	}

	log.Infof(ctx, "done ensuring all necessary migrations have run")

	// Start serving SQL clients.
	if err := s.startServeSQL(ctx, stopper, connManager, pgL, socketFile); err != nil {
		return err
	}

	// Start the async migration to upgrade namespace entries from the old
	// namespace table (id 2) to the new one (id 30).
	if err := migMgr.StartSystemNamespaceMigration(ctx, bootstrapVersion); err != nil {
		return err
	}

	// Delete all orphaned table leases created by a prior instance of this
	// node. This also uses SQL.
	s.leaseMgr.DeleteOrphanedLeases(orphanedLeasesTimeThresholdNanos)

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
			PlanHookMaker: func(opName string, txn *kv.Txn, user string) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.NewInternalPlanner(opName, txn, user, &sql.MemoryMetrics{}, s.execCfg)
			},
		},
		scheduledjobs.ProdJobSchedulerEnv,
	)

	return nil
}

// SQLInstanceID returns the ephemeral ID assigned to each SQL instance. The ID
// is guaranteed to be unique across all currently running instances, but may be
// reused once an instance is stopped.
func (s *SQLServer) SQLInstanceID() base.SQLInstanceID {
	return s.sqlIDContainer.SQLInstanceID()
}

// StartDiagnostics starts periodic diagnostics reporting.
// NOTE: This is not called in start so that it's disabled by default for
// testing.
func (s *SQLServer) StartDiagnostics(ctx context.Context) {
	s.diagnosticsReporter.PeriodicallyReportDiagnostics(ctx, s.stopper)
}
