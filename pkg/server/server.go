// Copyright 2014 The Cockroach Authors.
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

package server

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"

	"github.com/elazarl/go-bindata-assetfs"
	raven "github.com/getsentry/raven-go"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// Allocation pool for gzipResponseWriters.
	gzipResponseWriterPool sync.Pool

	// GracefulDrainModes is the standard succession of drain modes entered
	// for a graceful shutdown.
	GracefulDrainModes = []serverpb.DrainMode{serverpb.DrainMode_CLIENT, serverpb.DrainMode_LEASES}

	queryWait = settings.RegisterDurationSetting(
		"server.shutdown.query_wait",
		"the server will wait for at least this amount of time for active queries to finish",
		10*time.Second,
	)

	drainWait = settings.RegisterDurationSetting(
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with the rest "+
			"of the shutdown process",
		0*time.Second,
	)

	forwardClockJumpCheckEnabled = settings.RegisterBoolSetting(
		"server.clock.forward_jump_check_enabled",
		"If enabled, forward clock jumps > max_offset/2 will cause a panic.",
		false,
	)

	persistHLCUpperBoundInterval = settings.RegisterDurationSetting(
		"server.clock.persist_upper_bound_interval",
		"the interval between persisting the wall time upper bound of the clock. The clock "+
			"does not generate a wall time greater than the persisted timestamp and will panic if "+
			"it sees a wall time greater than this value. When cockroach starts, it waits for the "+
			"wall time to catch-up till this persisted timestamp. This guarantees monotonic wall "+
			"time across server restarts. Not setting this or setting a value of 0 disables this "+
			"feature.",
		0,
	)
)

// Server is the cockroach server node.
type Server struct {
	nodeIDContainer base.NodeIDContainer

	cfg                Config
	st                 *cluster.Settings
	mux                *http.ServeMux
	clock              *hlc.Clock
	rpcContext         *rpc.Context
	grpc               *grpc.Server
	gossip             *gossip.Gossip
	nodeLiveness       *storage.NodeLiveness
	storePool          *storage.StorePool
	tcsFactory         *kv.TxnCoordSenderFactory
	distSender         *kv.DistSender
	db                 *client.DB
	pgServer           *pgwire.Server
	distSQLServer      *distsqlrun.ServerImpl
	node               *Node
	registry           *metric.Registry
	recorder           *status.MetricsRecorder
	runtime            status.RuntimeStatSampler
	admin              *adminServer
	status             *statusServer
	authentication     *authenticationServer
	initServer         *initServer
	tsDB               *ts.DB
	tsServer           ts.Server
	raftTransport      *storage.RaftTransport
	stopper            *stop.Stopper
	sqlExecutor        *sql.Executor
	execCfg            *sql.ExecutorConfig
	leaseMgr           *sql.LeaseManager
	sessionRegistry    *sql.SessionRegistry
	jobRegistry        *jobs.Registry
	engines            Engines
	internalMemMetrics sql.MemoryMetrics
	adminMemMetrics    sql.MemoryMetrics
	serveMode
}

// NewServer creates a Server from a server.Config.
func NewServer(cfg Config, stopper *stop.Stopper) (*Server, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", cfg.AdvertiseAddr, err)
	}

	st := cfg.Settings

	if cfg.AmbientCtx.Tracer == nil {
		panic(errors.New("no tracer set in AmbientCtx"))
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Duration(cfg.MaxOffset))
	s := &Server{
		st:       st,
		mux:      http.NewServeMux(),
		clock:    clock,
		stopper:  stopper,
		cfg:      cfg,
		registry: metric.NewRegistry(),
	}
	s.serveMode.set(modeInitializing)

	// If the tracer has a Close function, call it after the server stops.
	if tr, ok := cfg.AmbientCtx.Tracer.(stop.Closer); ok {
		stopper.AddCloser(tr)
	}

	// Attempt to load TLS configs right away, failures are permanent.
	if certMgr, err := cfg.InitializeNodeTLSConfigs(stopper); err != nil {
		return nil, err
	} else if certMgr != nil {
		// The certificate manager is non-nil in secure mode.
		s.registry.AddMetricStruct(certMgr.Metrics())
	}

	// Add a dynamic log tag value for the node ID.
	//
	// We need to pass an ambient context to the various server components, but we
	// won't know the node ID until we Start(). At that point it's too late to
	// change the ambient contexts in the components (various background processes
	// will have already started using them).
	//
	// NodeIDContainer allows us to add the log tag to the context now and update
	// the value asynchronously. It's not significantly more expensive than a
	// regular tag since it's just doing an (atomic) load when a log/trace message
	// is constructed. The node ID is set by the Store if this host was
	// bootstrapped; otherwise a new one is allocated in Node.
	s.cfg.AmbientCtx.AddLogTag("n", &s.nodeIDContainer)

	ctx := s.AnnotateCtx(context.Background())

	s.rpcContext = rpc.NewContext(s.cfg.AmbientCtx, s.cfg.Config, s.clock, s.stopper,
		&cfg.Settings.Version)
	s.rpcContext.HeartbeatCB = func() {
		if err := s.rpcContext.RemoteClocks.VerifyClockOffset(ctx); err != nil {
			log.Fatal(ctx, err)
		}
	}

	s.grpc = rpc.NewServerWithInterceptor(s.rpcContext, s.Intercept())

	s.gossip = gossip.New(
		s.cfg.AmbientCtx,
		&s.rpcContext.ClusterID,
		&s.nodeIDContainer,
		s.rpcContext,
		s.grpc,
		s.stopper,
		s.registry,
	)

	// A custom RetryOptions is created which uses stopper.ShouldQuiesce() as
	// the Closer. This prevents infinite retry loops from occurring during
	// graceful server shutdown
	//
	// Such a loop occurs when the DistSender attempts a connection to the
	// local server during shutdown, and receives an internal server error (HTTP
	// Code 5xx). This is the correct error for a server to return when it is
	// shutting down, and is normally retryable in a cluster environment.
	// However, on a single-node setup (such as a test), retries will never
	// succeed because the only server has been shut down; thus, the
	// DistSender needs to know that it should not retry in this situation.
	retryOpts := s.cfg.RetryOptions
	if retryOpts == (retry.Options{}) {
		retryOpts = base.DefaultRetryOptions()
	}
	retryOpts.Closer = s.stopper.ShouldQuiesce()
	distSenderCfg := kv.DistSenderConfig{
		AmbientCtx:      s.cfg.AmbientCtx,
		Settings:        st,
		Clock:           s.clock,
		RPCContext:      s.rpcContext,
		RPCRetryOptions: &retryOpts,
	}
	if distSenderTestingKnobs := s.cfg.TestingKnobs.DistSender; distSenderTestingKnobs != nil {
		distSenderCfg.TestingKnobs = *distSenderTestingKnobs.(*kv.DistSenderTestingKnobs)
	}
	s.distSender = kv.NewDistSender(distSenderCfg, s.gossip)
	s.registry.AddMetricStruct(s.distSender.Metrics())

	txnMetrics := kv.MakeTxnMetrics(s.cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(txnMetrics)
	s.tcsFactory = kv.NewTxnCoordSenderFactory(
		s.cfg.AmbientCtx,
		st,
		s.distSender,
		s.clock,
		s.cfg.Linearizable,
		s.stopper,
		txnMetrics,
	)
	dbCtx := client.DefaultDBContext()
	dbCtx.NodeID = &s.nodeIDContainer
	s.db = client.NewDBWithContext(s.tcsFactory, s.clock, dbCtx)

	nlActive, nlRenewal := s.cfg.NodeLivenessDurations()

	s.nodeLiveness = storage.NewNodeLiveness(
		s.cfg.AmbientCtx,
		s.clock,
		s.db,
		s.engines,
		s.gossip,
		nlActive,
		nlRenewal,
		s.cfg.HistogramWindowInterval(),
	)
	s.registry.AddMetricStruct(s.nodeLiveness.Metrics())

	s.storePool = storage.NewStorePool(
		s.cfg.AmbientCtx,
		s.st,
		s.gossip,
		s.clock,
		storage.MakeStorePoolNodeLivenessFunc(s.nodeLiveness),
		/* deterministic */ false,
	)

	s.raftTransport = storage.NewRaftTransport(
		s.cfg.AmbientCtx, st, storage.GossipAddressResolver(s.gossip), s.grpc, s.rpcContext,
	)

	// Set up internal memory metrics for use by internal SQL executors.
	s.internalMemMetrics = sql.MakeMemMetrics("internal", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.internalMemMetrics)

	// Set up Lease Manager
	var lmKnobs sql.LeaseManagerTestingKnobs
	if leaseManagerTestingKnobs := cfg.TestingKnobs.SQLLeaseManager; leaseManagerTestingKnobs != nil {
		lmKnobs = *leaseManagerTestingKnobs.(*sql.LeaseManagerTestingKnobs)
	}
	s.leaseMgr = sql.NewLeaseManager(
		s.cfg.AmbientCtx,
		nil, /* execCfg - will be set later because of circular dependencies */
		lmKnobs,
		s.stopper,
		&s.internalMemMetrics,
		s.cfg.LeaseManagerConfig,
	)

	// We do not set memory monitors or a noteworthy limit because the children of
	// this monitor will be setting their own noteworthy limits.
	rootSQLMemoryMonitor := mon.MakeMonitor(
		"root",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	rootSQLMemoryMonitor.Start(context.Background(), nil, mon.MakeStandaloneBudget(s.cfg.SQLMemoryPoolSize))

	// Set up the DistSQL temp engine.

	tempEngine, err := engine.NewTempEngine(s.cfg.TempStorageConfig)
	if err != nil {
		return nil, errors.Wrap(err, "could not create temp storage")
	}
	s.stopper.AddCloser(tempEngine)
	// Remove temporary directory linked to tempEngine after closing
	// tempEngine.
	s.stopper.AddCloser(stop.CloserFn(func() {
		firstStore := cfg.Stores.Specs[0]
		var err error
		if firstStore.InMemory {
			// First store is in-memory so we remove the temp
			// directory directly since there is no record file.
			err = os.RemoveAll(s.cfg.TempStorageConfig.Path)
		} else {
			// If record file exists, we invoke CleanupTempDirs to
			// also remove the record after the temp directory is
			// removed.
			recordPath := filepath.Join(firstStore.Path, TempDirsRecordFilename)
			err = engine.CleanupTempDirs(recordPath)
		}
		if err != nil {
			log.Errorf(context.TODO(), "could not remove temporary store directory: %v", err.Error())
		}
	}))

	// Set up admin memory metrics for use by admin SQL executors.
	s.adminMemMetrics = sql.MakeMemMetrics("admin", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.adminMemMetrics)

	s.tsDB = ts.NewDB(s.db, s.cfg.Settings)
	s.registry.AddMetricStruct(s.tsDB.Metrics())
	nodeCountFn := func() int64 {
		return s.nodeLiveness.Metrics().LiveNodes.Value()
	}
	s.tsServer = ts.MakeServer(s.cfg.AmbientCtx, s.tsDB, nodeCountFn, s.cfg.TimeSeriesServerConfig, s.stopper)

	// The InternalExecutor will be further initialized later, as we create more
	// of the server's components. There's a circular dependency - many things
	// need an InternalExecutor, but the InternalExecutor needs an ExecutorConfig,
	// which in turn needs many things. That's why everybody that needs an
	// InternalExecutor uses this one instance.
	internalExecutor := &sql.InternalExecutor{}

	// Similarly for execCfg.
	var execCfg sql.ExecutorConfig

	// TODO(bdarnell): make StoreConfig configurable.
	storeCfg := storage.StoreConfig{
		Settings:                st,
		AmbientCtx:              s.cfg.AmbientCtx,
		RaftConfig:              s.cfg.RaftConfig,
		Clock:                   s.clock,
		DB:                      s.db,
		Gossip:                  s.gossip,
		NodeLiveness:            s.nodeLiveness,
		Transport:               s.raftTransport,
		RPCContext:              s.rpcContext,
		ScanInterval:            s.cfg.ScanInterval,
		ScanMaxIdleTime:         s.cfg.ScanMaxIdleTime,
		TimestampCachePageSize:  s.cfg.TimestampCachePageSize,
		HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
		StorePool:               s.storePool,
		SQLExecutor:             internalExecutor,
		LogRangeEvents:          s.cfg.EventLogEnabled,
		TimeSeriesDataStore:     s.tsDB,

		EnableEpochRangeLeases: true,
	}
	if storeTestingKnobs := s.cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*storage.StoreTestingKnobs)
	}

	s.recorder = status.NewMetricsRecorder(s.clock, s.nodeLiveness, s.rpcContext, s.gossip, st)
	s.registry.AddMetricStruct(s.rpcContext.RemoteClocks.Metrics())

	s.runtime = status.MakeRuntimeStatSampler(s.clock)
	s.registry.AddMetricStruct(s.runtime)

	s.node = NewNode(
		storeCfg, s.recorder, s.registry, s.stopper,
		txnMetrics, nil /* execCfg */, &s.rpcContext.ClusterID)
	roachpb.RegisterInternalServer(s.grpc, s.node)
	storage.RegisterConsistencyServer(s.grpc, s.node.storesServer)

	s.sessionRegistry = sql.MakeSessionRegistry()
	s.jobRegistry = jobs.MakeRegistry(
		s.cfg.AmbientCtx,
		s.clock,
		s.db,
		internalExecutor,
		&s.nodeIDContainer,
		st,
		func(opName, user string) (interface{}, func()) {
			// This is a hack to get around a Go package dependency cycle. See comment
			// in sql/jobs/registry.go on planHookMaker.
			return sql.NewInternalPlanner(opName, nil, user, &sql.MemoryMetrics{}, &execCfg)
		},
	)

	distSQLMetrics := distsqlrun.MakeDistSQLMetrics(cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(distSQLMetrics)

	// Set up the DistSQL server.
	distSQLCfg := distsqlrun.ServerConfig{
		AmbientContext: s.cfg.AmbientCtx,
		Settings:       st,
		DB:             s.db,
		Executor:       internalExecutor,
		FlowDB:         client.NewDB(s.tcsFactory, s.clock),
		RPCContext:     s.rpcContext,
		Stopper:        s.stopper,
		NodeID:         &s.nodeIDContainer,

		TempStorage: tempEngine,
		DiskMonitor: s.cfg.TempStorageConfig.Mon,

		ParentMemoryMonitor: &rootSQLMemoryMonitor,

		Metrics: &distSQLMetrics,

		JobRegistry: s.jobRegistry,
		Gossip:      s.gossip,
	}
	if distSQLTestingKnobs := s.cfg.TestingKnobs.DistSQL; distSQLTestingKnobs != nil {
		distSQLCfg.TestingKnobs = *distSQLTestingKnobs.(*distsqlrun.TestingKnobs)
	}

	s.distSQLServer = distsqlrun.NewServer(ctx, distSQLCfg)
	distsqlrun.RegisterDistSQLServer(s.grpc, s.distSQLServer)

	s.admin = newAdminServer(s, internalExecutor)
	s.status = newStatusServer(
		s.cfg.AmbientCtx,
		st,
		s.cfg.Config,
		s.admin,
		s.db,
		s.gossip,
		s.recorder,
		s.nodeLiveness,
		s.rpcContext,
		s.node.stores,
		s.stopper,
		s.sessionRegistry,
	)
	s.authentication = newAuthenticationServer(s, internalExecutor)
	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, &s.tsServer} {
		gw.RegisterService(s.grpc)
	}

	s.initServer = newInitServer(s)
	s.initServer.semaphore.acquire()

	serverpb.RegisterInitServer(s.grpc, s.initServer)

	nodeInfo := sql.NodeInfo{
		AdminURL:  cfg.AdminURL,
		PGURL:     cfg.PGURL,
		ClusterID: s.ClusterID,
		NodeID:    &s.nodeIDContainer,
	}

	virtualSchemas, err := sql.NewVirtualSchemaHolder(ctx, st)
	if err != nil {
		log.Fatal(ctx, err)
	}

	// Set up Executor

	var sqlExecutorTestingKnobs *sql.ExecutorTestingKnobs
	if k := s.cfg.TestingKnobs.SQLExecutor; k != nil {
		sqlExecutorTestingKnobs = k.(*sql.ExecutorTestingKnobs)
	} else {
		sqlExecutorTestingKnobs = new(sql.ExecutorTestingKnobs)
	}

	execCfg = sql.ExecutorConfig{
		Settings:                s.st,
		NodeInfo:                nodeInfo,
		AmbientCtx:              s.cfg.AmbientCtx,
		DB:                      s.db,
		Gossip:                  s.gossip,
		DistSender:              s.distSender,
		RPCContext:              s.rpcContext,
		LeaseManager:            s.leaseMgr,
		Clock:                   s.clock,
		DistSQLSrv:              s.distSQLServer,
		StatusServer:            s.status,
		SessionRegistry:         s.sessionRegistry,
		JobRegistry:             s.jobRegistry,
		VirtualSchemas:          virtualSchemas,
		HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
		RangeDescriptorCache:    s.distSender.RangeDescriptorCache(),
		LeaseHolderCache:        s.distSender.LeaseHolderCache(),
		TestingKnobs:            sqlExecutorTestingKnobs,

		DistSQLPlanner: sql.NewDistSQLPlanner(
			ctx,
			distsqlrun.Version,
			s.st,
			// The node descriptor will be set later, once it is initialized.
			roachpb.NodeDescriptor{},
			s.rpcContext,
			s.distSQLServer,
			s.distSender,
			s.gossip,
			s.stopper,
			s.nodeLiveness,
			sqlExecutorTestingKnobs.DistSQLPlannerKnobs,
		),

		TableStatsCache: stats.NewTableStatisticsCache(
			s.cfg.SQLTableStatCacheSize,
			s.gossip,
			s.db,
			internalExecutor,
		),

		ExecLogger: log.NewSecondaryLogger(
			nil /* dirName */, "sql-exec", true /* enableGc */, false, /*forceSyncWrites*/
		),

		AuditLogger: log.NewSecondaryLogger(
			s.cfg.SQLAuditLogDirName, "sql-audit", true /*enableGc*/, true, /*forceSyncWrites*/
		),

		ConnResultsBufferBytes: s.cfg.ConnResultsBufferBytes,
	}

	if sqlSchemaChangerTestingKnobs := s.cfg.TestingKnobs.SQLSchemaChanger; sqlSchemaChangerTestingKnobs != nil {
		execCfg.SchemaChangerTestingKnobs = sqlSchemaChangerTestingKnobs.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = new(sql.SchemaChangerTestingKnobs)
	}
	if sqlEvalContext := s.cfg.TestingKnobs.SQLEvalContext; sqlEvalContext != nil {
		execCfg.EvalContextTestingKnobs = *sqlEvalContext.(*tree.EvalContextTestingKnobs)
	}
	s.sqlExecutor = sql.NewExecutor(execCfg, s.stopper)
	if s.cfg.UseLegacyConnHandling {
		s.registry.AddMetricStruct(s.sqlExecutor)
	}
	s.PeriodicallyClearStmtStats(ctx)

	s.pgServer = pgwire.MakeServer(
		s.cfg.AmbientCtx,
		s.cfg.Config,
		s.ClusterSettings(),
		s.sqlExecutor,
		&s.internalMemMetrics,
		&rootSQLMemoryMonitor,
		s.cfg.HistogramWindowInterval(),
		&execCfg,
	)
	s.registry.AddMetricStruct(s.pgServer.Metrics())
	if !s.cfg.UseLegacyConnHandling {
		s.registry.AddMetricStruct(s.pgServer.StatementCounters())
		s.registry.AddMetricStruct(s.pgServer.EngineMetrics())
	}

	internalExecutor.ExecCfg = &execCfg
	s.execCfg = &execCfg

	s.leaseMgr.SetExecCfg(&execCfg)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)

	s.node.InitLogger(&execCfg)

	return s, nil
}

// ClusterSettings returns the cluster settings.
func (s *Server) ClusterSettings() *cluster.Settings {
	return s.st
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Server) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (s *Server) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, opentracing.Span) {
	return s.cfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// ClusterID returns the ID of the cluster this server is a part of.
func (s *Server) ClusterID() uuid.UUID {
	return s.rpcContext.ClusterID.Get()
}

// NodeID returns the ID of this node within its cluster.
func (s *Server) NodeID() roachpb.NodeID {
	return s.node.Descriptor.NodeID
}

// InitialBoot returns whether this is the first time the node has booted.
// Only intended to help print debugging info during server startup.
func (s *Server) InitialBoot() bool {
	return s.node.initialBoot
}

// grpcGatewayServer represents a grpc service with HTTP endpoints through GRPC
// gateway.
type grpcGatewayServer interface {
	RegisterService(g *grpc.Server)
	RegisterGateway(
		ctx context.Context,
		mux *gwruntime.ServeMux,
		conn *grpc.ClientConn,
	) error
}

// ListenError is returned from Start when we fail to start listening on either
// the main Cockroach port or the HTTP port, so that the CLI can instruct the
// user on what might have gone wrong.
type ListenError struct {
	error
	Addr string
}

func inspectEngines(
	ctx context.Context,
	engines []engine.Engine,
	minVersion,
	serverVersion roachpb.Version,
	clusterIDContainer *base.ClusterIDContainer,
) (
	bootstrappedEngines []engine.Engine,
	emptyEngines []engine.Engine,
	_ cluster.ClusterVersion,
	_ error,
) {
	for _, engine := range engines {
		storeIdent, err := storage.ReadStoreIdent(ctx, engine)
		if _, notBootstrapped := err.(*storage.NotBootstrappedError); notBootstrapped {
			emptyEngines = append(emptyEngines, engine)
			continue
		} else if err != nil {
			return nil, nil, cluster.ClusterVersion{}, err
		}
		clusterID := clusterIDContainer.Get()
		if storeIdent.ClusterID != uuid.Nil {
			if clusterID == uuid.Nil {
				clusterIDContainer.Set(ctx, storeIdent.ClusterID)
			} else if storeIdent.ClusterID != clusterID {
				return nil, nil, cluster.ClusterVersion{},
					errors.Errorf("conflicting store cluster IDs: %s, %s", storeIdent.ClusterID, clusterID)
			}
		}
		bootstrappedEngines = append(bootstrappedEngines, engine)
	}

	cv, err := storage.SynthesizeClusterVersionFromEngines(ctx, bootstrappedEngines, minVersion, serverVersion)
	if err != nil {
		return nil, nil, cluster.ClusterVersion{}, err
	}
	return bootstrappedEngines, emptyEngines, cv, nil
}

// listenerInfo is a helper used to write files containing various listener
// information to the store directories. In contrast to the "listening url
// file", these are written once the listeners are available, before the server
// is necessarily ready to serve.
type listenerInfo struct {
	listen    string // the (RPC) listen address
	advertise string // equals `listen` unless --advertise-host is used
	http      string // the HTTP endpoint
}

// Iter returns a mapping of file names to desired contents.
func (li listenerInfo) Iter() map[string]string {
	return map[string]string{
		"cockroach.advertise-addr": li.advertise,
		"cockroach.http-addr":      li.http,
		"cockroach.listen-addr":    li.listen,
	}
}

type singleListener struct {
	conn net.Conn
}

func (s *singleListener) Accept() (net.Conn, error) {
	if c := s.conn; c != nil {
		s.conn = nil
		return c, nil
	}
	return nil, io.EOF
}

func (s *singleListener) Close() error {
	return nil
}

func (s *singleListener) Addr() net.Addr {
	return s.conn.LocalAddr()
}

// startMonitoringForwardClockJumps starts a background task to monitor forward
// clock jumps based on a cluster setting
func (s *Server) startMonitoringForwardClockJumps(ctx context.Context) {
	forwardJumpCheckEnabled := make(chan bool, 1)
	s.stopper.AddCloser(stop.CloserFn(func() { close(forwardJumpCheckEnabled) }))

	forwardClockJumpCheckEnabled.SetOnChange(&s.st.SV, func() {
		forwardJumpCheckEnabled <- forwardClockJumpCheckEnabled.Get(&s.st.SV)
	})

	if err := s.clock.StartMonitoringForwardClockJumps(
		forwardJumpCheckEnabled,
		time.NewTicker,
		nil, /* tick callback */
	); err != nil {
		log.Fatal(ctx, err)
	}

	log.Info(ctx, "monitoring forward clock jumps based on server.clock.forward_jump_check_enabled")
}

// ensureClockMonotonicity sleeps till the wall time reaches
// prevHLCUpperBound. prevHLCUpperBound > 0 implies we need to guarantee HLC
// monotonicity across server restarts. prevHLCUpperBound is the last
// successfully persisted timestamp greater then any wall time used by the
// server.
//
// If prevHLCUpperBound is 0, the function sleeps up to max offset
func ensureClockMonotonicity(
	ctx context.Context,
	clock *hlc.Clock,
	startTime time.Time,
	prevHLCUpperBound int64,
	sleepUntilFn func(until int64, currTime func() int64),
) {
	var sleepUntil int64
	if prevHLCUpperBound != 0 {
		// Sleep until previous HLC upper bound to ensure wall time monotonicity
		sleepUntil = prevHLCUpperBound + 1
	} else {
		// Previous HLC Upper bound is not known
		// We might have to sleep a bit to protect against this node producing non-
		// monotonic timestamps. Before restarting, its clock might have been driven
		// by other nodes' fast clocks, but when we restarted, we lost all this
		// information. For example, a client might have written a value at a
		// timestamp that's in the future of the restarted node's clock, and if we
		// don't do something, the same client's read would not return the written
		// value. So, we wait up to MaxOffset; we couldn't have served timestamps more
		// than MaxOffset in the future (assuming that MaxOffset was not changed, see
		// #9733).
		//
		// As an optimization for tests, we don't sleep if all the stores are brand
		// new. In this case, the node will not serve anything anyway until it
		// synchronizes with other nodes.

		// Don't have to sleep for monotonicity when using clockless reads
		// (nor can we, for we would sleep forever).
		if maxOffset := clock.MaxOffset(); maxOffset != timeutil.ClocklessMaxOffset {
			sleepUntil = startTime.UnixNano() + int64(maxOffset) + 1
		}
	}

	currentWallTimeFn := func() int64 { /* function to report current time */
		return clock.Now().WallTime
	}
	currentWallTime := currentWallTimeFn()
	delta := time.Duration(sleepUntil - currentWallTime)
	if delta > 0 {
		log.Infof(
			ctx,
			"Sleeping till wall time %v to catches up to %v to ensure monotonicity. Delta: %v",
			currentWallTime,
			sleepUntil,
			delta,
		)
		sleepUntilFn(sleepUntil, currentWallTimeFn)
	}
}

// periodicallyPersistHLCUpperBound periodically persists an upper bound of
// the HLC's wall time. The interval for persisting is read from
// persistHLCUpperBoundIntervalCh. An interval of 0 disables persisting.
//
// persistHLCUpperBoundFn is used to persist the hlc upper bound, and should
// return an error if the persist fails.
//
// tickerFn is used to create the ticker used for persisting
//
// tickCallback is called whenever a tick is processed
func periodicallyPersistHLCUpperBound(
	clock *hlc.Clock,
	persistHLCUpperBoundIntervalCh chan time.Duration,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
	stopCh <-chan struct{},
	tickCallback func(),
) {
	// Create a ticker which can be used in selects.
	// This ticker is turned on / off based on persistHLCUpperBoundIntervalCh
	ticker := tickerFn(time.Hour)
	ticker.Stop()

	// persistInterval is the interval used for persisting the
	// an upper bound of the HLC
	var persistInterval time.Duration
	var ok bool

	persistHLCUpperBound := func() {
		if err := clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(persistInterval*3), /* delta to compute upper bound */
		); err != nil {
			log.Fatalf(
				context.Background(),
				"error persisting HLC upper bound: %v",
				err,
			)
		}
	}

	for {
		select {
		case persistInterval, ok = <-persistHLCUpperBoundIntervalCh:
			ticker.Stop()
			if !ok {
				return
			}

			if persistInterval > 0 {
				ticker = tickerFn(persistInterval)
				persistHLCUpperBound()
				log.Info(context.Background(), "persisting HLC upper bound is enabled")
			} else {
				if err := clock.ResetHLCUpperBound(persistHLCUpperBoundFn); err != nil {
					log.Fatalf(
						context.Background(),
						"error resetting hlc upper bound: %v",
						err,
					)
				}
				log.Info(context.Background(), "persisting HLC upper bound is disabled")
			}

		case <-ticker.C:
			if persistInterval > 0 {
				persistHLCUpperBound()
			}

		case <-stopCh:
			ticker.Stop()
			return
		}

		if tickCallback != nil {
			tickCallback()
		}
	}
}

// startPersistingHLCUpperBound starts a goroutine to persist an upper bound
// to the HLC.
//
// persistHLCUpperBoundFn is used to persist upper bound of the HLC, and should
// return an error if the persist fails
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever persistHLCUpperBoundCh or a ticker tick is
// processed
func (s *Server) startPersistingHLCUpperBound(
	hlcUpperBoundExists bool,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
) {
	persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)
	persistHLCUpperBoundInterval.SetOnChange(&s.st.SV, func() {
		persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	})

	if hlcUpperBoundExists {
		// The feature to persist upper bounds to wall times is enabled.
		// Persist a new upper bound to continue guaranteeing monotonicity
		// Going forward the goroutine launched below will take over persisting
		// the upper bound
		if err := s.clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(5*time.Second),
		); err != nil {
			log.Fatal(context.TODO(), err)
		}
	}

	s.stopper.RunWorker(
		context.TODO(),
		func(context.Context) {
			periodicallyPersistHLCUpperBound(
				s.clock,
				persistHLCUpperBoundIntervalCh,
				persistHLCUpperBoundFn,
				tickerFn,
				s.stopper.ShouldStop(),
				nil, /* tick callback */
			)
		},
	)
}

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context. This is complex since
// it sets up the listeners and the associated port muxing, but especially since
// it has to solve the "bootstrapping problem": nodes need to connect to Gossip
// fairly early, but what drives Gossip connectivity are the first range
// replicas in the kv store. This in turn suggests opening the Gossip server
// early. However, naively doing so also serves most other services prematurely,
// which exposes a large surface of potentially underinitialized services. This
// is avoided with some additional complexity that can be summarized as follows:
//
// - before blocking trying to connect to the Gossip network, we already open
//   the admin UI (so that its diagnostics are available)
// - we also allow our Gossip and our connection health Ping service
// - everything else returns Unavailable errors (which are retryable)
// - once the node has started, unlock all RPCs.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *Server) Start(ctx context.Context) error {
	if !s.st.Initialized {
		return errors.New("must pass initialized ClusterSettings")
	}
	ctx = s.AnnotateCtx(ctx)

	startTime := timeutil.Now()
	s.startMonitoringForwardClockJumps(ctx)

	tlsConfig, err := s.cfg.GetServerTLSConfig()
	if err != nil {
		return err
	}

	httpServer := netutil.MakeServer(s.stopper, tlsConfig, s)

	// The following code is a specialization of util/net.go's ListenAndServe
	// which adds pgwire support. A single port is used to serve all protocols
	// (pg, http, h2) via the following construction:
	//
	// non-TLS case:
	// net.Listen -> cmux.New
	//               |
	//               -  -> pgwire.Match -> pgwire.Server.ServeConn
	//               -  -> cmux.Any -> grpc.(*Server).Serve
	//
	// TLS case:
	// net.Listen -> cmux.New
	//               |
	//               -  -> pgwire.Match -> pgwire.Server.ServeConn
	//               -  -> cmux.Any -> grpc.(*Server).Serve
	//
	// Note that the difference between the TLS and non-TLS cases exists due to
	// Go's lack of an h2c (HTTP2 Clear Text) implementation. See inline comments
	// in util.ListenAndServe for an explanation of how h2c is implemented there
	// and here.

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return ListenError{
			error: err,
			Addr:  s.cfg.Addr,
		}
	}
	log.Eventf(ctx, "listening on port %s", s.cfg.Addr)
	unresolvedListenAddr, err := officialAddr(ctx, s.cfg.Addr, ln.Addr(), os.Hostname)
	if err != nil {
		return err
	}
	s.cfg.Addr = unresolvedListenAddr.String()
	unresolvedAdvertAddr, err := officialAddr(ctx, s.cfg.AdvertiseAddr, ln.Addr(), os.Hostname)
	if err != nil {
		return err
	}
	s.cfg.AdvertiseAddr = unresolvedAdvertAddr.String()

	s.rpcContext.SetLocalInternalServer(s.node)

	// The cmux matches don't shut down properly unless serve is called on the
	// cmux at some point. Use serveOnMux to ensure it's called during shutdown
	// if we wouldn't otherwise reach the point where we start serving on it.
	var serveOnMux sync.Once
	m := cmux.New(ln)

	pgL := m.Match(func(r io.Reader) bool {
		return pgwire.Match(r)
	})

	anyL := m.Match(cmux.Any())

	httpLn, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return ListenError{
			error: err,
			Addr:  s.cfg.HTTPAddr,
		}
	}
	unresolvedHTTPAddr, err := officialAddr(ctx, s.cfg.HTTPAddr, httpLn.Addr(), os.Hostname)
	if err != nil {
		return err
	}
	s.cfg.HTTPAddr = unresolvedHTTPAddr.String()

	workersCtx := s.AnnotateCtx(context.Background())

	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Fatal(workersCtx, err)
		}
	})

	if tlsConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		s.stopper.RunWorker(workersCtx, func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		})

		s.stopper.RunWorker(workersCtx, func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle("/health", s)

			plainRedirectServer := netutil.MakeServer(s.stopper, tlsConfig, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		})

		httpLn = tls.NewListener(tlsL, tlsConfig)
	}

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(httpServer.Serve(httpLn))
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		<-s.stopper.ShouldQuiesce()
		// TODO(bdarnell): Do we need to also close the other listeners?
		netutil.FatalIfUnexpected(anyL.Close())
		<-s.stopper.ShouldStop()
		s.grpc.Stop()
		serveOnMux.Do(func() {
			// A cmux can't gracefully shut down without Serve being called on it.
			netutil.FatalIfUnexpected(m.Serve())
		})
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(anyL))
	})

	// Running the SQL migrations safely requires that we aren't serving SQL
	// requests at the same time -- to ensure that, block the serving of SQL
	// traffic until the migrations are done, as indicated by this channel.
	serveSQL := make(chan bool)

	tcpKeepAlive := envutil.EnvOrDefaultDuration("COCKROACH_SQL_TCP_KEEP_ALIVE", time.Minute)
	var loggedKeepAliveStatus int32

	// Attempt to set TCP keep-alive on connection. Don't fail on errors.
	setTCPKeepAlive := func(ctx context.Context, conn net.Conn) {
		if tcpKeepAlive == 0 {
			return
		}

		muxConn, ok := conn.(*cmux.MuxConn)
		if !ok {
			return
		}
		tcpConn, ok := muxConn.Conn.(*net.TCPConn)
		if !ok {
			return
		}

		// Only log success/failure once.
		doLog := atomic.CompareAndSwapInt32(&loggedKeepAliveStatus, 0, 1)
		if err := tcpConn.SetKeepAlive(true); err != nil {
			if doLog {
				log.Warningf(ctx, "failed to enable TCP keep-alive for pgwire: %v", err)
			}
			return

		}
		if err := tcpConn.SetKeepAlivePeriod(tcpKeepAlive); err != nil {
			if doLog {
				log.Warningf(ctx, "failed to set TCP keep-alive duration for pgwire: %v", err)
			}
			return
		}

		if doLog {
			log.VEventf(ctx, 2, "setting TCP keep-alive to %s for pgwire", tcpKeepAlive)
		}
	}

	// Enable the debug endpoints first to provide an earlier window into what's
	// going on with the node in advance of exporting node functionality.
	//
	// TODO(marc): when cookie-based authentication exists, apply it to all web
	// endpoints.
	s.mux.Handle(debug.Endpoint, debug.NewServer(s.st))

	// Also throw the landing page in there. It won't work well, but it's better than a 404.
	// The remaining endpoints will be opened late, when we're sure that the subsystems they
	// talk to are functional.
	s.mux.Handle("/", http.FileServer(&assetfs.AssetFS{
		Asset:     ui.Asset,
		AssetDir:  ui.AssetDir,
		AssetInfo: ui.AssetInfo,
	}))

	// Initialize grpc-gateway mux and context in order to get the /health
	// endpoint working even before the node has fully initialized.
	jsonpb := &protoutil.JSONPb{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
	}
	protopb := new(protoutil.ProtoPb)
	gwMux := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, jsonpb),
		gwruntime.WithMarshalerOption(httputil.JSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.AltJSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.ProtoContentType, protopb),
		gwruntime.WithMarshalerOption(httputil.AltProtoContentType, protopb),
		gwruntime.WithOutgoingHeaderMatcher(authenticationHeaderMatcher),
	)
	gwCtx, gwCancel := context.WithCancel(s.AnnotateCtx(context.Background()))
	s.stopper.AddCloser(stop.CloserFn(gwCancel))

	var authHandler http.Handler = gwMux
	if s.cfg.RequireWebSession() {
		authHandler = newAuthenticationMux(s.authentication, authHandler)
	}

	// Setup HTTP<->gRPC handlers.
	c1, c2 := net.Pipe()

	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		for _, c := range []net.Conn{c1, c2} {
			if err := c.Close(); err != nil {
				log.Fatal(workersCtx, err)
			}
		}
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(&singleListener{
			conn: c1,
		}))
	})

	// Eschew `(*rpc.Context).GRPCDial` to avoid unnecessary moving parts on the
	// uniquely in-process connection.
	dialOpts, err := s.rpcContext.GRPCDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.DialContext(ctx, s.cfg.AdvertiseAddr, append(
		dialOpts,
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return c2, nil
		}),
	)...)
	if err != nil {
		return err
	}
	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		if err := conn.Close(); err != nil {
			log.Fatal(workersCtx, err)
		}
	})

	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, &s.tsServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return err
		}
	}
	s.mux.Handle("/health", gwMux)

	s.engines, err = s.cfg.CreateEngines(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create engines")
	}
	s.stopper.AddCloser(&s.engines)

	// Write listener info files early in the startup sequence. `listenerInfo` has a comment.
	listenerFiles := listenerInfo{
		advertise: unresolvedAdvertAddr.String(),
		http:      unresolvedHTTPAddr.String(),
		listen:    unresolvedListenAddr.String(),
	}.Iter()

	for _, storeSpec := range s.cfg.Stores.Specs {
		if storeSpec.InMemory {
			continue
		}
		for base, val := range listenerFiles {
			file := filepath.Join(storeSpec.Path, base)
			if err := ioutil.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
	}

	bootstrappedEngines, _, _, err := inspectEngines(
		ctx, s.engines, s.cfg.Settings.Version.MinSupportedVersion,
		s.cfg.Settings.Version.ServerVersion, &s.rpcContext.ClusterID)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Signal readiness. This unblocks the process when running with
	// --background or under systemd. At this point we have bound our
	// listening port but the server is not yet running, so any
	// connection attempts will be queued up in the kernel. We turn on
	// servers below, first HTTP and later pgwire. If we're in
	// initializing mode, we don't start the pgwire server until after
	// initialization completes, so connections to that port will
	// continue to block until we're initialized.
	if err := sdnotify.Ready(); err != nil {
		log.Errorf(ctx, "failed to signal readiness using systemd protocol: %s", err)
	}

	// Filter the gossip bootstrap resolvers based on the listen and
	// advertise addresses.
	filtered := s.cfg.FilterGossipBootstrapResolvers(ctx, unresolvedListenAddr, unresolvedAdvertAddr)
	s.gossip.Start(unresolvedAdvertAddr, filtered)
	log.Event(ctx, "started gossip")

	defer time.AfterFunc(30*time.Second, func() {
		msg := `The server appears to be unable to contact the other nodes in the cluster. Please try

- starting the other nodes, if you haven't already
- double-checking that the '--join' and '--host' flags are set up correctly
- running the 'cockroach init' command if you are trying to initialize a new cluster

If problems persist, please see ` + base.DocsURL("cluster-setup-troubleshooting.html") + "."

		log.Shout(context.Background(), log.Severity_WARNING,
			msg)
	}).Stop()

	var hlcUpperBoundExists bool
	if len(bootstrappedEngines) > 0 {
		hlcUpperBound, err := storage.ReadMaxHLCUpperBound(ctx, bootstrappedEngines)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if hlcUpperBound > 0 {
			hlcUpperBoundExists = true
		}

		ensureClockMonotonicity(
			ctx,
			s.clock,
			startTime,
			hlcUpperBound,
			timeutil.SleepUntil,
		)

	} else if len(s.cfg.GossipBootstrapResolvers) == 0 {
		// If the _unfiltered_ list of hosts from the --join flag is
		// empty, then this node can bootstrap a new cluster. We disallow
		// this if this node is being started with itself specified as a
		// --join host, because that's too likely to be operator error.
		bootstrapVersion := s.cfg.Settings.Version.BootstrapVersion()
		if s.cfg.TestingKnobs.Store != nil {
			if storeKnobs, ok := s.cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs); ok && storeKnobs.BootstrapVersion != nil {
				bootstrapVersion = *storeKnobs.BootstrapVersion
			}
		}
		if err := s.node.bootstrap(ctx, s.engines, bootstrapVersion); err != nil {
			return err
		}
		log.Infof(ctx, "**** add additional nodes by specifying --join=%s", s.cfg.AdvertiseAddr)
	} else {
		log.Info(ctx, "no stores bootstrapped and --join flag specified, awaiting init command.")

		// Note that when we created the init server, we acquired its semaphore
		// (to stop anyone from rushing in).
		s.initServer.semaphore.release()

		s.stopper.RunWorker(workersCtx, func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})

		if err := s.initServer.awaitBootstrap(); err != nil {
			return err
		}

		// Reacquire the semaphore, allowing the code below to be oblivious to
		// the fact that this branch was taken.
		s.initServer.semaphore.acquire()
	}

	// Release the semaphore of the init server. Anyone still managing to talk
	// to it may do so, but will be greeted with an error telling them that the
	// cluster is already initialized.
	s.initServer.semaphore.release()

	// This opens the main listener.
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		serveOnMux.Do(func() {
			netutil.FatalIfUnexpected(m.Serve())
		})
	})

	// We ran this before, but might've bootstrapped in the meantime. This time
	// we'll get the actual list of bootstrapped and empty engines.
	bootstrappedEngines, emptyEngines, cv, err := inspectEngines(
		ctx, s.engines, s.cfg.Settings.Version.MinSupportedVersion,
		s.cfg.Settings.Version.ServerVersion, &s.rpcContext.ClusterID)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been bootstrapped or
	// we're joining an existing cluster for the first time.
	if err := s.node.start(
		ctx,
		unresolvedAdvertAddr,
		bootstrappedEngines, emptyEngines,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		cv,
	); err != nil {
		return err
	}
	log.Event(ctx, "started node")
	s.startPersistingHLCUpperBound(
		hlcUpperBoundExists,
		func(t int64) error { /* function to persist upper bound of HLC to all stores */
			return s.node.SetHLCUpperBound(context.Background(), t)
		},
		time.NewTicker,
	)

	s.execCfg.DistSQLPlanner.SetNodeDesc(s.node.Descriptor)

	// Cluster ID should have been determined by this point.
	if s.rpcContext.ClusterID.Get() == uuid.Nil {
		log.Fatal(ctx, "Cluster ID failed to be determined during node startup.")
	}

	s.refreshSettings()

	raven.SetTagsContext(map[string]string{
		"cluster":   s.ClusterID().String(),
		"node":      s.NodeID().String(),
		"server_id": fmt.Sprintf("%s-%s", s.ClusterID().Short(), s.NodeID()),
	})

	// We can now add the node registry.
	s.recorder.AddNode(s.registry, s.node.Descriptor, s.node.startedAt, s.cfg.AdvertiseAddr, s.cfg.HTTPAddr)

	// Begin recording runtime statistics.
	s.startSampleEnvironment(DefaultMetricsSampleInterval)

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	// Begin recording status summaries.
	s.node.startWriteSummaries(DefaultMetricsSampleInterval)

	// Create and start the schema change manager only after a NodeID
	// has been assigned.
	var testingKnobs *sql.SchemaChangerTestingKnobs
	if s.cfg.TestingKnobs.SQLSchemaChanger != nil {
		testingKnobs = s.cfg.TestingKnobs.SQLSchemaChanger.(*sql.SchemaChangerTestingKnobs)
	} else {
		testingKnobs = new(sql.SchemaChangerTestingKnobs)
	}

	sql.NewSchemaChangeManager(
		s.cfg.AmbientCtx,
		s.execCfg,
		testingKnobs,
		*s.db,
		s.node.Descriptor,
		s.execCfg.DistSQLPlanner,
	).Start(s.stopper)

	s.sqlExecutor.Start(ctx, s.execCfg.DistSQLPlanner)
	s.distSQLServer.Start()
	s.pgServer.Start(ctx, s.stopper)

	s.serveMode.set(modeOperational)

	s.mux.Handle(adminPrefix, authHandler)
	s.mux.Handle(ts.URLPrefix, authHandler)
	s.mux.Handle(statusPrefix, authHandler)
	s.mux.Handle(authPrefix, gwMux)
	s.mux.Handle(statusVars, http.HandlerFunc(s.status.handleVars))
	log.Event(ctx, "added http endpoints")

	log.Infof(ctx, "starting %s server at %s", s.cfg.HTTPRequestScheme(), unresolvedHTTPAddr)
	log.Infof(ctx, "starting grpc/postgres server at %s", unresolvedListenAddr)
	log.Infof(ctx, "advertising CockroachDB node at %s", unresolvedAdvertAddr)

	log.Event(ctx, "accepting connections")

	// Begin the node liveness heartbeat. Add a callback which
	// 1. records the local store "last up" timestamp for every store whenever the
	//    liveness record is updated.
	// 2. sets Draining if Decommissioning is set in the liveness record
	decommissionSem := make(chan struct{}, 1)
	s.nodeLiveness.StartHeartbeat(ctx, s.stopper, func(ctx context.Context) {
		now := s.clock.Now()
		if err := s.node.stores.VisitStores(func(s *storage.Store) error {
			return s.WriteLastUpTimestamp(ctx, now)
		}); err != nil {
			log.Warning(ctx, errors.Wrap(err, "writing last up timestamp"))
		}

		if liveness, err := s.nodeLiveness.Self(); err != nil && err != storage.ErrNoLivenessRecord {
			log.Warning(ctx, errors.Wrap(err, "retrieving own liveness record"))
		} else if liveness != nil && liveness.Decommissioning && !liveness.Draining {
			select {
			case decommissionSem <- struct{}{}:
				s.stopper.RunWorker(ctx, func(context.Context) {
					defer func() {
						<-decommissionSem
					}()

					// Don't use ctx because there is an associated timeout
					// meant to be used when heartbeating.
					if _, err := s.Drain(context.Background(), GracefulDrainModes); err != nil {
						log.Warningf(ctx, "failed to set Draining when Decommissioning: %v", err)
					}
				})
			default:
				// Already have an active goroutine trying to drain; don't add a
				// second one.
			}
		}
	})

	{
		var regLiveness jobs.NodeLiveness = s.nodeLiveness
		if testingLiveness := s.cfg.TestingKnobs.RegistryLiveness; testingLiveness != nil {
			regLiveness = testingLiveness.(*jobs.FakeNodeLiveness)
		}
		if err := s.jobRegistry.Start(
			ctx, s.stopper, regLiveness, jobs.DefaultCancelInterval, jobs.DefaultAdoptInterval,
		); err != nil {
			return err
		}
	}

	// Before serving SQL requests, we have to make sure the database is
	// in an acceptable form for this version of the software.
	// We have to do this after actually starting up the server to be able to
	// seamlessly use the kv client against other nodes in the cluster.
	var mmKnobs sqlmigrations.MigrationManagerTestingKnobs
	if migrationManagerTestingKnobs := s.cfg.TestingKnobs.SQLMigrationManager; migrationManagerTestingKnobs != nil {
		mmKnobs = *migrationManagerTestingKnobs.(*sqlmigrations.MigrationManagerTestingKnobs)
	}
	migMgr := sqlmigrations.NewManager(
		s.stopper,
		s.db,
		s.sqlExecutor,
		s.clock,
		mmKnobs,
		&s.internalMemMetrics,
		s.NodeID().String(),
	)
	if err := migMgr.EnsureMigrations(ctx); err != nil {
		select {
		case <-s.stopper.ShouldQuiesce():
			// Avoid turning an early shutdown into a fatal error. See #19579.
			return errors.New("server is shutting down")
		default:
			log.Fatal(ctx, err)
		}
	}
	log.Infof(ctx, "done ensuring all necessary migrations have run")
	close(serveSQL)

	log.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
	s.stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
		select {
		case <-serveSQL:
		case <-s.stopper.ShouldQuiesce():
			return
		}
		netutil.FatalIfUnexpected(httpServer.ServeWith(pgCtx, s.stopper, pgL, func(conn net.Conn) {
			connCtx := log.WithLogTagStr(pgCtx, "client", conn.RemoteAddr().String())
			setTCPKeepAlive(connCtx, conn)

			var serveFn func(ctx context.Context, conn net.Conn) error
			if !s.cfg.UseLegacyConnHandling {
				serveFn = s.pgServer.ServeConn2
			} else {
				serveFn = s.pgServer.ServeConn
			}
			if err := serveFn(connCtx, conn); err != nil && !netutil.IsClosedConnection(err) {
				// Report the error on this connection's context, so that we
				// know which remote client caused the error when looking at
				// the logs.
				log.Error(connCtx, err)
			}
		}))
	})
	if len(s.cfg.SocketFile) != 0 {
		log.Infof(ctx, "starting postgres server at unix:%s", s.cfg.SocketFile)

		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", s.cfg.SocketFile)
		if err != nil {
			return err
		}

		s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
			<-s.stopper.ShouldQuiesce()
			if err := unixLn.Close(); err != nil {
				log.Fatal(workersCtx, err)
			}
		})

		s.stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
			select {
			case <-serveSQL:
			case <-s.stopper.ShouldQuiesce():
				return
			}
			netutil.FatalIfUnexpected(httpServer.ServeWith(pgCtx, s.stopper, unixLn, func(conn net.Conn) {
				connCtx := log.WithLogTagStr(pgCtx, "client", conn.RemoteAddr().String())
				if err := s.pgServer.ServeConn(connCtx, conn); err != nil &&
					!netutil.IsClosedConnection(err) {
					// Report the error on this connection's context, so that we
					// know which remote client caused the error when looking at
					// the logs.
					log.Error(connCtx, err)
				}
			}))
		})
	}

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent()

	if s.cfg.PIDFile != "" {
		if err := ioutil.WriteFile(s.cfg.PIDFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
			log.Error(ctx, err)
		}
	}

	if s.cfg.ListeningURLFile != "" {
		pgURL, err := s.cfg.PGURL(url.User(security.RootUser))
		if err == nil {
			err = ioutil.WriteFile(s.cfg.ListeningURLFile, []byte(fmt.Sprintf("%s\n", pgURL)), 0644)
		}

		if err != nil {
			log.Error(ctx, err)
		}
	}

	log.Event(ctx, "server ready")

	return nil
}

func (s *Server) doDrain(
	ctx context.Context, modes []serverpb.DrainMode, setTo bool,
) ([]serverpb.DrainMode, error) {
	for _, mode := range modes {
		switch mode {
		case serverpb.DrainMode_CLIENT:
			if setTo {
				s.serveMode.set(modeDraining)
				// Wait for drainUnreadyWait. This will fail load balancer checks and
				// delay draining so that client traffic can move off this node.
				time.Sleep(drainWait.Get(&s.st.SV))
			}
			if err := func() error {
				if !setTo {
					// Execute this last.
					defer func() { s.serveMode.set(modeOperational) }()
				}
				// Since enabling the lease manager's draining mode will prevent
				// the acquisition of new leases, the switch must be made after
				// the pgServer has given sessions a chance to finish ongoing
				// work.
				defer s.leaseMgr.SetDraining(setTo)

				if !setTo {
					s.distSQLServer.Undrain(ctx)
					s.pgServer.Undrain()
					return nil
				}

				drainMaxWait := queryWait.Get(&s.st.SV)
				if err := s.pgServer.Drain(drainMaxWait); err != nil {
					return err
				}
				s.distSQLServer.Drain(ctx, drainMaxWait)
				return nil
			}(); err != nil {
				return nil, err
			}
		case serverpb.DrainMode_LEASES:
			s.nodeLiveness.SetDraining(ctx, setTo)
			if err := s.node.SetDraining(setTo); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("unknown drain mode: %s", mode)
		}
	}
	var nowOn []serverpb.DrainMode
	if s.pgServer.IsDraining() {
		nowOn = append(nowOn, serverpb.DrainMode_CLIENT)
	}
	if s.node.IsDraining() {
		nowOn = append(nowOn, serverpb.DrainMode_LEASES)
	}
	return nowOn, nil
}

// Drain idempotently activates the given DrainModes on the Server in the order
// in which they are supplied.
// For example, Drain is typically called with [CLIENT,LEADERSHIP] before
// terminating the process for graceful shutdown.
// On success, returns all active drain modes after carrying out the request.
// On failure, the system may be in a partially drained state and should be
// recovered by calling Undrain() with the same (or a larger) slice of modes.
func (s *Server) Drain(ctx context.Context, on []serverpb.DrainMode) ([]serverpb.DrainMode, error) {
	return s.doDrain(ctx, on, true)
}

// Undrain idempotently deactivates the given DrainModes on the Server in the
// order in which they are supplied.
// On success, returns any remaining active drain modes.
func (s *Server) Undrain(ctx context.Context, off []serverpb.DrainMode) []serverpb.DrainMode {
	nowActive, err := s.doDrain(ctx, off, false)
	if err != nil {
		panic(fmt.Sprintf("error returned to Undrain: %s", err))
	}
	return nowActive
}

// Decommission idempotently sets the decommissioning flag for specified nodes.
func (s *Server) Decommission(ctx context.Context, setTo bool, nodeIDs []roachpb.NodeID) error {
	eventLogger := sql.MakeEventLogger(s.execCfg)
	eventType := sql.EventLogNodeDecommissioned
	if !setTo {
		eventType = sql.EventLogNodeRecommissioned
	}
	for _, nodeID := range nodeIDs {
		changeCommitted, err := s.nodeLiveness.SetDecommissioning(ctx, nodeID, setTo)
		if err != nil {
			return errors.Wrapf(err, "during liveness update %d -> %t", nodeID, setTo)
		}
		if changeCommitted {
			// If we die right now or if this transaction fails to commit, the
			// commissioning event will not be recorded to the event log. While we
			// could insert the event record in the same transaction as the liveness
			// update, this would force a 2PC and potentially leave write intents in
			// the node liveness range. Better to make the event logging best effort
			// than to slow down future node liveness transactions.
			if err := s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				return eventLogger.InsertEventRecord(
					ctx, txn, eventType, int32(nodeID), int32(s.NodeID()), struct{}{},
				)
			}); err != nil {
				log.Errorf(ctx, "unable to record %s event for node %d: %s", eventType, nodeID, err)
			}
		}
	}
	return nil
}

// startSampleEnvironment begins a worker that periodically instructs the
// runtime stat sampler to sample the environment.
func (s *Server) startSampleEnvironment(frequency time.Duration) {
	// Immediately record summaries once on server startup.
	ctx := s.AnnotateCtx(context.Background())
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.runtime.SampleEnvironment(ctx)
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop(context.TODO())
}

// ServeHTTP is necessary to implement the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This is our base handler, so catch all panics and make sure they stick.
	defer log.FatalOnPanic()

	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	ae := r.Header.Get(httputil.AcceptEncodingHeader)
	switch {
	case strings.Contains(ae, httputil.GzipEncoding):
		w.Header().Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
		gzw := newGzipResponseWriter(w)
		defer func() {
			// Certain requests must not have a body, yet closing the gzip writer will
			// attempt to write the gzip header. Avoid logging a warning in this case.
			// This is notably triggered by:
			//
			// curl -H 'Accept-Encoding: gzip' \
			// 	    -H 'If-Modified-Since: Thu, 29 Mar 2018 22:36:32 GMT' \
			//      -v http://localhost:8080/favicon.ico > /dev/null
			//
			// which results in a 304 Not Modified.
			if err := gzw.Close(); err != http.ErrBodyNotAllowed {
				ctx := s.AnnotateCtx(r.Context())
				log.Warningf(ctx, "error closing gzip response writer: %v", err)
			}
		}()
		w = gzw
	}
	s.mux.ServeHTTP(w, r)
}

// TempDir returns the filepath of the temporary directory used for temp storage.
// It is empty for an in-memory temp storage.
func (s *Server) TempDir() string {
	return s.cfg.TempStorageConfig.Path
}

type gzipResponseWriter struct {
	gz gzip.Writer
	http.ResponseWriter
}

func newGzipResponseWriter(rw http.ResponseWriter) *gzipResponseWriter {
	var w *gzipResponseWriter
	if wI := gzipResponseWriterPool.Get(); wI == nil {
		w = new(gzipResponseWriter)
	} else {
		w = wI.(*gzipResponseWriter)
	}
	w.Reset(rw)
	return w
}

func (w *gzipResponseWriter) Reset(rw http.ResponseWriter) {
	w.gz.Reset(rw)
	w.ResponseWriter = rw
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.gz.Write(b)
}

// Flush implements http.Flusher as required by grpc-gateway for clients
// which access streaming endpoints (as exercised by the acceptance tests
// at time of writing).
func (w *gzipResponseWriter) Flush() {
	// If Flush returns an error, we'll see it on the next call to Write or
	// Close as well, so we can ignore it here.
	if err := w.gz.Flush(); err == nil {
		// Flush the wrapped ResponseWriter as well, if possible.
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// Close implements the io.Closer interface. It is not safe to use the
// writer after calling Close.
func (w *gzipResponseWriter) Close() error {
	err := w.gz.Close()
	w.Reset(nil) // release ResponseWriter reference.
	gzipResponseWriterPool.Put(w)
	return err
}

func officialAddr(
	ctx context.Context, cfgAddr string, lnAddr net.Addr, osHostname func() (string, error),
) (*util.UnresolvedAddr, error) {
	cfgHost, cfgPort, err := net.SplitHostPort(cfgAddr)
	if err != nil {
		return nil, err
	}

	lnHost, lnPort, err := net.SplitHostPort(lnAddr.String())
	if err != nil {
		return nil, err
	}

	host := cfgHost
	if len(host) == 0 {
		// A host was not provided. Ask the system.
		name, err := osHostname()
		if err != nil {
			return nil, errors.Wrap(err, "unable to get hostname")
		}
		host = name
	}
	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to lookup hostname %q", host)
	}
	if len(addrs) == 0 {
		return nil, errors.Errorf("hostname %q did not resolve to any addresses; listener address: %s", host, lnHost)
	}

	// cfgPort may need to be used if --advertise-port was set on the command line.
	port := lnPort
	if i, err := strconv.Atoi(cfgPort); err == nil && i > 0 {
		port = cfgPort
	}

	return util.NewUnresolvedAddr(lnAddr.Network(), net.JoinHostPort(host, port)), nil
}
