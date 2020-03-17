// Copyright 2014 The Cockroach Authors.
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
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/container"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/reports"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagebase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/goroutinedumper"
	"github.com/cockroachdb/cockroach/pkg/server/heapprofiler"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	_ "github.com/cockroachdb/cockroach/pkg/sql/gcjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/logtags"
	raven "github.com/getsentry/raven-go"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/marusama/semaphore"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	// Allocation pool for gzipResponseWriters.
	gzipResponseWriterPool sync.Pool

	// GracefulDrainModes is the standard succession of drain modes entered
	// for a graceful shutdown.
	GracefulDrainModes = []serverpb.DrainMode{serverpb.DrainMode_CLIENT, serverpb.DrainMode_LEASES}

	queryWait = settings.RegisterPublicDurationSetting(
		"server.shutdown.query_wait",
		"the server will wait for at least this amount of time for active queries to finish",
		10*time.Second,
	)

	drainWait = settings.RegisterPublicDurationSetting(
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with the rest "+
			"of the shutdown process",
		0*time.Second,
	)

	forwardClockJumpCheckEnabled = settings.RegisterPublicBoolSetting(
		"server.clock.forward_jump_check_enabled",
		"if enabled, forward clock jumps > max_offset/2 will cause a panic",
		false,
	)

	persistHLCUpperBoundInterval = settings.RegisterPublicDurationSetting(
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

// TODO(peter): Until go1.11, ServeMux.ServeHTTP was not safe to call
// concurrently with ServeMux.Handle. So we provide our own wrapper with proper
// locking. Slightly less efficient because it locks unnecessarily, but
// safe. See TestServeMuxConcurrency. Should remove once we've upgraded to
// go1.11.
type safeServeMux struct {
	mu  syncutil.RWMutex
	mux http.ServeMux
}

func (mux *safeServeMux) Handle(pattern string, handler http.Handler) {
	mux.mu.Lock()
	mux.mux.Handle(pattern, handler)
	mux.mu.Unlock()
}

func (mux *safeServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux.mu.RLock()
	mux.mux.ServeHTTP(w, r)
	mux.mu.RUnlock()
}

// Server is the cockroach server node.
type Server struct {
	nodeIDContainer base.NodeIDContainer

	cfg        Config
	st         *cluster.Settings
	mux        safeServeMux
	clock      *hlc.Clock
	startTime  time.Time
	rpcContext *rpc.Context
	// The gRPC server on which the different RPC handlers will be registered.
	grpc             *grpcServer
	gossip           *gossip.Gossip
	nodeDialer       *nodedialer.Dialer
	nodeLiveness     *kvserver.NodeLiveness
	storePool        *kvserver.StorePool
	tcsFactory       *kvcoord.TxnCoordSenderFactory
	distSender       *kvcoord.DistSender
	db               *kv.DB
	pgServer         *pgwire.Server
	distSQLServer    *distsql.ServerImpl
	node             *Node
	registry         *metric.Registry
	recorder         *status.MetricsRecorder
	runtime          *status.RuntimeStatSampler
	admin            *adminServer
	status           *statusServer
	authentication   *authenticationServer
	initServer       *initServer
	tsDB             *ts.DB
	tsServer         ts.Server
	raftTransport    *kvserver.RaftTransport
	stopper          *stop.Stopper
	execCfg          *sql.ExecutorConfig
	internalExecutor *sql.InternalExecutor
	leaseMgr         *sql.LeaseManager
	blobService      *blobs.Service
	debug            *debug.Server
	// sessionRegistry can be queried for info on running SQL sessions. It is
	// shared between the sql.Server and the statusServer.
	sessionRegistry        *sql.SessionRegistry
	jobRegistry            *jobs.Registry
	statsRefresher         *stats.Refresher
	replicationReporter    *reports.Reporter
	temporaryObjectCleaner *sql.TemporaryObjectCleaner
	engines                Engines
	internalMemMetrics     sql.MemoryMetrics
	adminMemMetrics        sql.MemoryMetrics
	// sqlMemMetrics are used to track memory usage of sql sessions.
	sqlMemMetrics         sql.MemoryMetrics
	protectedtsProvider   protectedts.Provider
	protectedtsReconciler *ptreconcile.Reconciler
}

// NewServer creates a Server from a server.Config.
func NewServer(cfg Config, stopper *stop.Stopper) (*Server, error) {
	if err := cfg.ValidateAddrs(context.Background()); err != nil {
		return nil, err
	}

	st := cfg.Settings

	if cfg.AmbientCtx.Tracer == nil {
		panic(errors.New("no tracer set in AmbientCtx"))
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Duration(cfg.MaxOffset))
	s := &Server{
		st:       st,
		clock:    clock,
		stopper:  stopper,
		cfg:      cfg,
		registry: metric.NewRegistry(),
	}

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

	// Check the compatibility between the configured addresses and that
	// provided in certificates. This also logs the certificate
	// addresses in all cases to aid troubleshooting.
	// This must be called after the certificate manager was initialized
	// and after ValidateAddrs().
	s.cfg.CheckCertificateAddrs(ctx)

	if knobs := s.cfg.TestingKnobs.Server; knobs != nil {
		serverKnobs := knobs.(*TestingKnobs)
		s.rpcContext = rpc.NewContextWithTestingKnobs(
			s.cfg.AmbientCtx, s.cfg.Config, s.clock, s.stopper, cfg.Settings,
			serverKnobs.ContextTestingKnobs,
		)
	} else {
		s.rpcContext = rpc.NewContext(s.cfg.AmbientCtx, s.cfg.Config, s.clock, s.stopper,
			cfg.Settings)
	}
	s.rpcContext.HeartbeatCB = func() {
		if err := s.rpcContext.RemoteClocks.VerifyClockOffset(ctx); err != nil {
			log.Fatal(ctx, err)
		}
	}
	s.registry.AddMetricStruct(s.rpcContext.Metrics())

	s.grpc = newGRPCServer(s.rpcContext)

	s.gossip = gossip.New(
		s.cfg.AmbientCtx,
		&s.rpcContext.ClusterID,
		&s.nodeIDContainer,
		s.rpcContext,
		s.grpc.Server,
		s.stopper,
		s.registry,
		s.cfg.Locality,
		&s.cfg.DefaultZoneConfig,
	)
	s.nodeDialer = nodedialer.New(s.rpcContext, gossip.AddressResolver(s.gossip))

	// Create blob service for inter-node file sharing.
	if blobService, err := blobs.NewBlobService(s.ClusterSettings().ExternalIODir); err != nil {
		log.Fatal(ctx, err)
	} else {
		s.blobService = blobService
	}
	blobspb.RegisterBlobServer(s.grpc.Server, s.blobService)

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
	var clientTestingKnobs kvcoord.ClientTestingKnobs
	if kvKnobs := s.cfg.TestingKnobs.KVClient; kvKnobs != nil {
		clientTestingKnobs = *kvKnobs.(*kvcoord.ClientTestingKnobs)
	}
	retryOpts := s.cfg.RetryOptions
	if retryOpts == (retry.Options{}) {
		retryOpts = base.DefaultRetryOptions()
	}
	retryOpts.Closer = s.stopper.ShouldQuiesce()
	distSenderCfg := kvcoord.DistSenderConfig{
		AmbientCtx:      s.cfg.AmbientCtx,
		Settings:        st,
		Clock:           s.clock,
		RPCContext:      s.rpcContext,
		RPCRetryOptions: &retryOpts,
		TestingKnobs:    clientTestingKnobs,
		NodeDialer:      s.nodeDialer,
	}
	s.distSender = kvcoord.NewDistSender(distSenderCfg, s.gossip)
	s.registry.AddMetricStruct(s.distSender.Metrics())

	txnMetrics := kvcoord.MakeTxnMetrics(s.cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(txnMetrics)
	txnCoordSenderFactoryCfg := kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx:   s.cfg.AmbientCtx,
		Settings:     st,
		Clock:        s.clock,
		Stopper:      s.stopper,
		Linearizable: s.cfg.Linearizable,
		Metrics:      txnMetrics,
		TestingKnobs: clientTestingKnobs,
	}
	s.tcsFactory = kvcoord.NewTxnCoordSenderFactory(txnCoordSenderFactoryCfg, s.distSender)

	dbCtx := kv.DefaultDBContext()
	dbCtx.NodeID = &s.nodeIDContainer
	dbCtx.Stopper = s.stopper
	s.db = kv.NewDBWithContext(s.cfg.AmbientCtx, s.tcsFactory, s.clock, dbCtx)

	nlActive, nlRenewal := s.cfg.NodeLivenessDurations()

	s.nodeLiveness = kvserver.NewNodeLiveness(
		s.cfg.AmbientCtx,
		s.clock,
		s.db,
		s.engines,
		s.gossip,
		nlActive,
		nlRenewal,
		s.st,
		s.cfg.HistogramWindowInterval(),
	)
	s.registry.AddMetricStruct(s.nodeLiveness.Metrics())

	s.storePool = kvserver.NewStorePool(
		s.cfg.AmbientCtx,
		s.st,
		s.gossip,
		s.clock,
		s.nodeLiveness.GetNodeCount,
		kvserver.MakeStorePoolNodeLivenessFunc(s.nodeLiveness),
		/* deterministic */ false,
	)

	s.raftTransport = kvserver.NewRaftTransport(
		s.cfg.AmbientCtx, st, s.nodeDialer, s.grpc.Server, s.stopper,
	)

	// Set up internal memory metrics for use by internal SQL executors.
	s.internalMemMetrics = sql.MakeMemMetrics("internal", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.internalMemMetrics)

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

	// bulkMemoryMonitor is the parent to all child SQL monitors tracking bulk
	// operations (IMPORT, index backfill). It is itself a child of the
	// ParentMemoryMonitor.
	bulkMemoryMonitor := mon.MakeMonitorInheritWithLimit("bulk-mon", 0 /* limit */, &rootSQLMemoryMonitor)
	bulkMetrics := bulk.MakeBulkMetrics(cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(bulkMetrics)
	bulkMemoryMonitor.SetMetrics(bulkMetrics.CurBytesCount, bulkMetrics.MaxBytesHist)
	bulkMemoryMonitor.Start(context.Background(), &rootSQLMemoryMonitor, mon.BoundAccount{})

	// Set up the DistSQL temp engine.

	useStoreSpec := cfg.Stores.Specs[s.cfg.TempStorageConfig.SpecIdx]
	tempEngine, tempFS, err := storage.NewTempEngine(ctx, s.cfg.StorageEngine, s.cfg.TempStorageConfig, useStoreSpec)
	if err != nil {
		return nil, errors.Wrap(err, "could not create temp storage")
	}
	s.stopper.AddCloser(tempEngine)
	// Remove temporary directory linked to tempEngine after closing
	// tempEngine.
	s.stopper.AddCloser(stop.CloserFn(func() {
		firstStore := cfg.Stores.Specs[s.cfg.TempStorageConfig.SpecIdx]
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
			err = storage.CleanupTempDirs(recordPath)
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

	// This function defines how ExternalStorage objects are created.
	externalStorage := func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
		return cloud.MakeExternalStorage(
			ctx, dest, s.cfg.ExternalIOConfig, st,
			blobs.NewBlobClientFactory(
				s.nodeIDContainer.Get(),
				s.nodeDialer,
				st.ExternalIODir,
			),
		)
	}
	externalStorageFromURI := func(ctx context.Context, uri string) (cloud.ExternalStorage, error) {
		return cloud.ExternalStorageFromURI(
			ctx, uri, s.cfg.ExternalIOConfig, st,
			blobs.NewBlobClientFactory(
				s.nodeIDContainer.Get(),
				s.nodeDialer,
				st.ExternalIODir,
			),
		)
	}

	if s.protectedtsProvider, err = ptprovider.New(ptprovider.Config{
		DB:               s.db,
		InternalExecutor: internalExecutor,
		Settings:         st,
	}); err != nil {
		return nil, err
	}

	// Similarly for execCfg.
	var execCfg sql.ExecutorConfig

	// TODO(bdarnell): make StoreConfig configurable.
	storeCfg := kvserver.StoreConfig{
		DefaultZoneConfig:       &s.cfg.DefaultZoneConfig,
		Settings:                st,
		AmbientCtx:              s.cfg.AmbientCtx,
		RaftConfig:              s.cfg.RaftConfig,
		Clock:                   s.clock,
		DB:                      s.db,
		Gossip:                  s.gossip,
		NodeLiveness:            s.nodeLiveness,
		Transport:               s.raftTransport,
		NodeDialer:              s.nodeDialer,
		RPCContext:              s.rpcContext,
		ScanInterval:            s.cfg.ScanInterval,
		ScanMinIdleTime:         s.cfg.ScanMinIdleTime,
		ScanMaxIdleTime:         s.cfg.ScanMaxIdleTime,
		TimestampCachePageSize:  s.cfg.TimestampCachePageSize,
		HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
		StorePool:               s.storePool,
		SQLExecutor:             internalExecutor,
		LogRangeEvents:          s.cfg.EventLogEnabled,
		RangeDescriptorCache:    s.distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:     s.tsDB,

		// Initialize the closed timestamp subsystem. Note that it won't
		// be ready until it is .Start()ed, but the grpc server can be
		// registered early.
		ClosedTimestamp: container.NewContainer(container.Config{
			Settings: st,
			Stopper:  s.stopper,
			Clock:    s.nodeLiveness.AsLiveClock(),
			// NB: s.node is not defined at this point, but it will be
			// before this is ever called.
			Refresh: func(rangeIDs ...roachpb.RangeID) {
				for _, rangeID := range rangeIDs {
					repl, _, err := s.node.stores.GetReplicaForRangeID(rangeID)
					if err != nil || repl == nil {
						continue
					}
					repl.EmitMLAI()
				}
			},
			Dialer: s.nodeDialer.CTDialer(),
		}),

		EnableEpochRangeLeases:  true,
		ExternalStorage:         externalStorage,
		ExternalStorageFromURI:  externalStorageFromURI,
		ProtectedTimestampCache: s.protectedtsProvider,
	}
	if storeTestingKnobs := s.cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*kvserver.StoreTestingKnobs)
	}

	s.recorder = status.NewMetricsRecorder(s.clock, s.nodeLiveness, s.rpcContext, s.gossip, st)
	s.registry.AddMetricStruct(s.rpcContext.RemoteClocks.Metrics())

	s.runtime = status.NewRuntimeStatSampler(ctx, s.clock)
	s.registry.AddMetricStruct(s.runtime)

	s.node = NewNode(
		storeCfg, s.recorder, s.registry, s.stopper,
		txnMetrics, nil /* execCfg */, &s.rpcContext.ClusterID)
	roachpb.RegisterInternalServer(s.grpc.Server, s.node)
	kvserver.RegisterPerReplicaServer(s.grpc.Server, s.node.perReplicaServer)
	s.node.storeCfg.ClosedTimestamp.RegisterClosedTimestampServer(s.grpc.Server)
	s.replicationReporter = reports.NewReporter(
		s.db, s.node.stores, s.storePool,
		s.ClusterSettings(), s.nodeLiveness, internalExecutor)

	s.sessionRegistry = sql.NewSessionRegistry()
	var jobAdoptionStopFile string
	for _, spec := range s.cfg.Stores.Specs {
		if !spec.InMemory && spec.Path != "" {
			jobAdoptionStopFile = filepath.Join(spec.Path, jobs.PreventAdoptionFile)
			break
		}
	}
	s.jobRegistry = jobs.MakeRegistry(
		s.cfg.AmbientCtx,
		s.stopper,
		s.clock,
		s.db,
		internalExecutor,
		&s.nodeIDContainer,
		st,
		s.cfg.HistogramWindowInterval(),
		func(opName, user string) (interface{}, func()) {
			// This is a hack to get around a Go package dependency cycle. See comment
			// in sql/jobs/registry.go on planHookMaker.
			return sql.NewInternalPlanner(opName, nil, user, &sql.MemoryMetrics{}, &execCfg)
		},
		jobAdoptionStopFile,
	)
	s.registry.AddMetricStruct(s.jobRegistry.MetricsStruct())
	s.protectedtsReconciler = ptreconcile.NewReconciler(ptreconcile.Config{
		Settings: s.st,
		Stores:   s.node.stores,
		DB:       s.db,
		Storage:  s.protectedtsProvider,
		Cache:    s.protectedtsProvider,
		StatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.MetaType: jobsprotectedts.MakeStatusFunc(s.jobRegistry),
		},
	})
	s.registry.AddMetricStruct(s.protectedtsReconciler.Metrics())

	distSQLMetrics := execinfra.MakeDistSQLMetrics(cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(distSQLMetrics)

	// Set up Lease Manager
	var lmKnobs sql.LeaseManagerTestingKnobs
	if leaseManagerTestingKnobs := cfg.TestingKnobs.SQLLeaseManager; leaseManagerTestingKnobs != nil {
		lmKnobs = *leaseManagerTestingKnobs.(*sql.LeaseManagerTestingKnobs)
	}
	s.leaseMgr = sql.NewLeaseManager(
		s.cfg.AmbientCtx,
		&s.nodeIDContainer,
		s.db,
		s.clock,
		nil, /* internalExecutor - will be set later because of circular dependencies */
		st,
		lmKnobs,
		s.stopper,
		s.cfg.LeaseManagerConfig,
	)

	// Set up the DistSQL server.
	distSQLCfg := execinfra.ServerConfig{
		AmbientContext: s.cfg.AmbientCtx,
		Settings:       st,
		RuntimeStats:   s.runtime,
		DB:             s.db,
		Executor:       internalExecutor,
		FlowDB:         kv.NewDB(s.cfg.AmbientCtx, s.tcsFactory, s.clock),
		RPCContext:     s.rpcContext,
		Stopper:        s.stopper,
		NodeID:         &s.nodeIDContainer,
		ClusterID:      &s.rpcContext.ClusterID,
		ClusterName:    s.cfg.ClusterName,

		TempStorage:     tempEngine,
		TempStoragePath: s.cfg.TempStorageConfig.Path,
		TempFS:          tempFS,
		// COCKROACH_VEC_MAX_OPEN_FDS specifies the maximum number of open file
		// descriptors that the vectorized execution engine may have open at any
		// one time. This limit is implemented as a weighted semaphore acquired
		// before opening files.
		VecFDSemaphore: semaphore.New(envutil.EnvOrDefaultInt("COCKROACH_VEC_MAX_OPEN_FDS", colexec.VecMaxOpenFDsLimit)),
		DiskMonitor:    s.cfg.TempStorageConfig.Mon,

		ParentMemoryMonitor: &rootSQLMemoryMonitor,
		BulkAdder: func(
			ctx context.Context, db *kv.DB, ts hlc.Timestamp, opts storagebase.BulkAdderOptions,
		) (storagebase.BulkAdder, error) {
			// Attach a child memory monitor to enable control over the BulkAdder's
			// memory usage.
			bulkMon := execinfra.NewMonitor(ctx, &bulkMemoryMonitor, fmt.Sprintf("bulk-adder-monitor"))
			return bulk.MakeBulkAdder(ctx, db, s.distSender.RangeDescriptorCache(), s.st, ts, opts, bulkMon)
		},

		Metrics: &distSQLMetrics,

		JobRegistry:  s.jobRegistry,
		Gossip:       s.gossip,
		NodeDialer:   s.nodeDialer,
		LeaseManager: s.leaseMgr,

		ExternalStorage:        externalStorage,
		ExternalStorageFromURI: externalStorageFromURI,
	}
	s.cfg.TempStorageConfig.Mon.SetMetrics(distSQLMetrics.CurDiskBytesCount, distSQLMetrics.MaxDiskBytesHist)
	if distSQLTestingKnobs := s.cfg.TestingKnobs.DistSQL; distSQLTestingKnobs != nil {
		distSQLCfg.TestingKnobs = *distSQLTestingKnobs.(*execinfra.TestingKnobs)
	}

	s.distSQLServer = distsql.NewServer(ctx, distSQLCfg)
	execinfrapb.RegisterDistSQLServer(s.grpc.Server, s.distSQLServer)

	s.admin = newAdminServer(s)
	s.status = newStatusServer(
		s.cfg.AmbientCtx,
		st,
		s.cfg.Config,
		s.admin,
		s.db,
		s.gossip,
		s.recorder,
		s.nodeLiveness,
		s.storePool,
		s.rpcContext,
		s.node.stores,
		s.stopper,
		s.sessionRegistry,
		internalExecutor,
	)
	s.authentication = newAuthenticationServer(s)
	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, &s.tsServer} {
		gw.RegisterService(s.grpc.Server)
	}

	// TODO(andrei): We're creating an initServer even through the inspection of
	// our engines in Server.Start() might reveal that we're already bootstrapped
	// and so we don't need to accept a Bootstrap RPC. The creation of this server
	// early means that a Bootstrap RPC might erroneously succeed. We should
	// figure out early if our engines are bootstrapped and, if they are, create a
	// dummy implementation of the InitServer that rejects all RPCs.
	s.initServer = newInitServer(s.gossip.Connected, s.stopper.ShouldStop())
	serverpb.RegisterInitServer(s.grpc.Server, s.initServer)

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

	var sqlExecutorTestingKnobs sql.ExecutorTestingKnobs
	if k := s.cfg.TestingKnobs.SQLExecutor; k != nil {
		sqlExecutorTestingKnobs = *k.(*sql.ExecutorTestingKnobs)
	} else {
		sqlExecutorTestingKnobs = sql.ExecutorTestingKnobs{}
	}

	loggerCtx, _ := s.stopper.WithCancelOnStop(ctx)

	execCfg = sql.ExecutorConfig{
		Settings:                s.st,
		NodeInfo:                nodeInfo,
		DefaultZoneConfig:       &s.cfg.DefaultZoneConfig,
		Locality:                s.cfg.Locality,
		AmbientCtx:              s.cfg.AmbientCtx,
		DB:                      s.db,
		Gossip:                  s.gossip,
		MetricsRecorder:         s.recorder,
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
		RoleMemberCache:         &sql.MembershipCache{},
		TestingKnobs:            sqlExecutorTestingKnobs,

		DistSQLPlanner: sql.NewDistSQLPlanner(
			ctx,
			execinfra.Version,
			s.st,
			// The node descriptor will be set later, once it is initialized.
			roachpb.NodeDescriptor{},
			s.rpcContext,
			s.distSQLServer,
			s.distSender,
			s.gossip,
			s.stopper,
			s.nodeLiveness,
			s.nodeDialer,
		),

		TableStatsCache: stats.NewTableStatisticsCache(
			s.cfg.SQLTableStatCacheSize,
			s.gossip,
			s.db,
			internalExecutor,
		),

		// Note: don't forget to add the secondary loggers as closers
		// on the Stopper, below.

		ExecLogger: log.NewSecondaryLogger(
			loggerCtx, nil /* dirName */, "sql-exec",
			true /* enableGc */, false /*forceSyncWrites*/, true, /* enableMsgCount */
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
			true /* enableGc */, true /*forceSyncWrites*/, true, /* enableMsgCount */
		),

		// AuditLogger syncs to disk for the same reason as AuthLogger.
		AuditLogger: log.NewSecondaryLogger(
			loggerCtx, s.cfg.SQLAuditLogDirName, "sql-audit",
			true /*enableGc*/, true /*forceSyncWrites*/, true, /* enableMsgCount */
		),

		SlowQueryLogger: log.NewSecondaryLogger(
			loggerCtx, nil, "sql-slow",
			true /*enableGc*/, false /*forceSyncWrites*/, true, /* enableMsgCount */
		),

		QueryCache:                 querycache.New(s.cfg.SQLQueryCacheSize),
		ProtectedTimestampProvider: s.protectedtsProvider,
	}

	s.stopper.AddCloser(execCfg.ExecLogger)
	s.stopper.AddCloser(execCfg.AuditLogger)
	s.stopper.AddCloser(execCfg.SlowQueryLogger)
	s.stopper.AddCloser(execCfg.AuthLogger)

	if sqlSchemaChangerTestingKnobs := s.cfg.TestingKnobs.SQLSchemaChanger; sqlSchemaChangerTestingKnobs != nil {
		execCfg.SchemaChangerTestingKnobs = sqlSchemaChangerTestingKnobs.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = new(sql.SchemaChangerTestingKnobs)
	}
	if gcJobTestingKnobs := s.cfg.TestingKnobs.GCJob; gcJobTestingKnobs != nil {
		execCfg.GCJobTestingKnobs = gcJobTestingKnobs.(*sql.GCJobTestingKnobs)
	} else {
		execCfg.GCJobTestingKnobs = new(sql.GCJobTestingKnobs)
	}
	if distSQLRunTestingKnobs := s.cfg.TestingKnobs.DistSQL; distSQLRunTestingKnobs != nil {
		execCfg.DistSQLRunTestingKnobs = distSQLRunTestingKnobs.(*execinfra.TestingKnobs)
	} else {
		execCfg.DistSQLRunTestingKnobs = new(execinfra.TestingKnobs)
	}
	if sqlEvalContext := s.cfg.TestingKnobs.SQLEvalContext; sqlEvalContext != nil {
		execCfg.EvalContextTestingKnobs = *sqlEvalContext.(*tree.EvalContextTestingKnobs)
	}
	if pgwireKnobs := s.cfg.TestingKnobs.PGWireTestingKnobs; pgwireKnobs != nil {
		execCfg.PGWireTestingKnobs = pgwireKnobs.(*sql.PGWireTestingKnobs)
	}

	s.statsRefresher = stats.MakeRefresher(
		s.st,
		internalExecutor,
		execCfg.TableStatsCache,
		stats.DefaultAsOfTime,
	)
	execCfg.StatsRefresher = s.statsRefresher

	// Set up internal memory metrics for use by internal SQL executors.
	s.sqlMemMetrics = sql.MakeMemMetrics("sql", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.sqlMemMetrics)
	s.pgServer = pgwire.MakeServer(
		s.cfg.AmbientCtx,
		s.cfg.Config,
		s.ClusterSettings(),
		s.sqlMemMetrics,
		&rootSQLMemoryMonitor,
		s.cfg.HistogramWindowInterval(),
		&execCfg,
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
			s.pgServer.SQLServer,
			s.sqlMemMetrics,
			s.st,
		)
		ie.SetSessionData(sessionData)
		return &ie
	}
	s.distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory = ieFactory
	s.jobRegistry.SetSessionBoundInternalExecutorFactory(ieFactory)

	s.distSQLServer.ServerConfig.ProtectedTimestampProvider = execCfg.ProtectedTimestampProvider

	for _, m := range s.pgServer.Metrics() {
		s.registry.AddMetricStruct(m)
	}
	*internalExecutor = sql.MakeInternalExecutor(
		ctx, s.pgServer.SQLServer, s.internalMemMetrics, s.ClusterSettings(),
	)
	s.internalExecutor = internalExecutor
	execCfg.InternalExecutor = internalExecutor
	s.status.stmtDiagnosticsRequester = execCfg.NewStmtDiagnosticsRequestRegistry()
	s.execCfg = &execCfg

	s.leaseMgr.SetInternalExecutor(execCfg.InternalExecutor)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)
	s.leaseMgr.PeriodicallyRefreshSomeLeases()

	s.node.InitLogger(&execCfg)
	s.cfg.DefaultZoneConfig = cfg.DefaultZoneConfig

	s.debug = debug.NewServer(s.ClusterSettings(), s.pgServer.HBADebugFn())

	s.temporaryObjectCleaner = sql.NewTemporaryObjectCleaner(
		s.st,
		s.db,
		s.distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory,
		s.status,
		s.node.stores.IsMeta1Leaseholder,
		sqlExecutorTestingKnobs,
	)

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

// inspectEngines goes through engines and checks which ones are bootstrapped
// and which ones are empty.
// It also calls SynthesizeClusterVersionFromEngines to get the cluster version,
// or to set it if no engines have a version in them already.
func inspectEngines(
	ctx context.Context,
	engines []storage.Engine,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
	clusterIDContainer *base.ClusterIDContainer,
) (
	bootstrappedEngines []storage.Engine,
	emptyEngines []storage.Engine,
	_ clusterversion.ClusterVersion,
	_ error,
) {
	for _, eng := range engines {
		storeIdent, err := kvserver.ReadStoreIdent(ctx, eng)
		if _, notBootstrapped := err.(*kvserver.NotBootstrappedError); notBootstrapped {
			emptyEngines = append(emptyEngines, eng)
			continue
		} else if err != nil {
			return nil, nil, clusterversion.ClusterVersion{}, err
		}
		clusterID := clusterIDContainer.Get()
		if storeIdent.ClusterID != uuid.Nil {
			if clusterID == uuid.Nil {
				clusterIDContainer.Set(ctx, storeIdent.ClusterID)
			} else if storeIdent.ClusterID != clusterID {
				return nil, nil, clusterversion.ClusterVersion{},
					errors.Errorf("conflicting store cluster IDs: %s, %s", storeIdent.ClusterID, clusterID)
			}
		}
		bootstrappedEngines = append(bootstrappedEngines, eng)
	}

	cv, err := kvserver.SynthesizeClusterVersionFromEngines(ctx, bootstrappedEngines, binaryVersion, binaryMinSupportedVersion)
	if err != nil {
		return nil, nil, clusterversion.ClusterVersion{}, err
	}
	return bootstrappedEngines, emptyEngines, cv, nil
}

// listenerInfo is a helper used to write files containing various listener
// information to the store directories. In contrast to the "listening url
// file", these are written once the listeners are available, before the server
// is necessarily ready to serve.
type listenerInfo struct {
	listenRPC    string // the (RPC) listen address, rewritten after name resolution and port allocation
	advertiseRPC string // contains the original addr part of --listen/--advertise, with actual port number after port allocation if original was 0
	listenHTTP   string // the HTTP endpoint
	listenSQL    string // the SQL endpoint, rewritten after name resolution and port allocation
	advertiseSQL string // contains the original addr part of --sql-addr, with actual port number after port allocation if original was 0
}

// Iter returns a mapping of file names to desired contents.
func (li listenerInfo) Iter() map[string]string {
	return map[string]string{
		"cockroach.advertise-addr":     li.advertiseRPC,
		"cockroach.http-addr":          li.listenHTTP,
		"cockroach.listen-addr":        li.listenRPC,
		"cockroach.sql-addr":           li.listenSQL,
		"cockroach.advertise-sql-addr": li.advertiseSQL,
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
		sleepUntil = startTime.UnixNano() + int64(clock.MaxOffset()) + 1
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
	ctx = s.AnnotateCtx(ctx)

	// Start the time sanity checker.
	s.startTime = timeutil.Now()
	s.startMonitoringForwardClockJumps(ctx)

	// Connect the node as loopback handler for RPC requests to the
	// local node.
	s.rpcContext.SetLocalInternalServer(s.node)

	// Load the TLS configuration for the HTTP server.
	uiTLSConfig, err := s.cfg.GetUIServerTLSConfig()
	if err != nil {
		return err
	}

	// connManager tracks incoming connections accepted via listeners
	// and automatically closes them when the stopper indicates a
	// shutdown.
	// This handles both:
	// - HTTP connections for the admin UI with an optional TLS handshake over HTTP.
	// - SQL client connections with a TLS handshake over TCP.
	// (gRPC connections are handled separately via s.grpc and perform
	// their TLS handshake on their own)
	connManager := netutil.MakeServer(s.stopper, uiTLSConfig, s)

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	// Start the admin UI server. This opens the HTTP listen socket,
	// optionally sets up TLS, and dispatches the server worker for the
	// web UI.
	if err := s.startServeUI(ctx, workersCtx, connManager, uiTLSConfig); err != nil {
		return err
	}

	// Start the RPC server. This opens the RPC/SQL listen socket,
	// and dispatches the server worker for the RPC.
	// The SQL listener is returned, to start the SQL server later
	// below when the server has initialized.
	pgL, startRPCServer, err := s.startListenRPCAndSQL(ctx, workersCtx)
	if err != nil {
		return err
	}

	if s.cfg.TestingKnobs.Server != nil {
		knobs := s.cfg.TestingKnobs.Server.(*TestingKnobs)
		if knobs.SignalAfterGettingRPCAddress != nil {
			close(knobs.SignalAfterGettingRPCAddress)
		}
		if knobs.PauseAfterGettingRPCAddress != nil {
			<-knobs.PauseAfterGettingRPCAddress
		}
	}

	// Enable the debug endpoints first to provide an earlier window into what's
	// going on with the node in advance of exporting node functionality.
	//
	// TODO(marc): when cookie-based authentication exists, apply it to all web
	// endpoints.
	s.mux.Handle(debug.Endpoint, s.debug)

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
		gwruntime.WithMetadata(forwardAuthenticationMetadata),
	)
	gwCtx, gwCancel := context.WithCancel(s.AnnotateCtx(context.Background()))
	s.stopper.AddCloser(stop.CloserFn(gwCancel))

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
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
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
	// Handle /health early. This is necessary for orchestration.  Note
	// that /health is not authenticated, on purpose. This is both
	// because it needs to be available before the cluster is up and can
	// serve authentication requests, and also because it must work for
	// monitoring tools which operate without authentication.
	s.mux.Handle("/health", gwMux)

	s.engines, err = s.cfg.CreateEngines(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create engines")
	}
	s.stopper.AddCloser(&s.engines)

	s.node.startAssertEngineHealth(ctx, s.engines)

	// Write listener info files early in the startup sequence. `listenerInfo` has a comment.
	listenerFiles := listenerInfo{
		listenRPC:    s.cfg.Addr,
		advertiseRPC: s.cfg.AdvertiseAddr,
		listenSQL:    s.cfg.SQLAddr,
		advertiseSQL: s.cfg.SQLAdvertiseAddr,
		listenHTTP:   s.cfg.HTTPAdvertiseAddr,
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
		ctx, s.engines,
		s.cfg.Settings.Version.BinaryVersion(),
		s.cfg.Settings.Version.BinaryMinSupportedVersion(),
		&s.rpcContext.ClusterID)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Filter the gossip bootstrap resolvers based on the listen and
	// advertise addresses.
	listenAddrU := util.NewUnresolvedAddr("tcp", s.cfg.Addr)
	advAddrU := util.NewUnresolvedAddr("tcp", s.cfg.AdvertiseAddr)
	advSQLAddrU := util.NewUnresolvedAddr("tcp", s.cfg.SQLAdvertiseAddr)
	filtered := s.cfg.FilterGossipBootstrapResolvers(ctx, listenAddrU, advAddrU)
	s.gossip.Start(advAddrU, filtered)
	log.Event(ctx, "started gossip")

	if s.cfg.DelayedBootstrapFn != nil {
		defer time.AfterFunc(30*time.Second, s.cfg.DelayedBootstrapFn).Stop()
	}

	var hlcUpperBoundExists bool
	// doBootstrap is set if we're the ones who bootstrapped the cluster.
	var doBootstrap bool
	if len(bootstrappedEngines) > 0 {
		// The cluster was already initialized.
		doBootstrap = false
		if s.cfg.ReadyFn != nil {
			s.cfg.ReadyFn(false /*waitForInit*/)
		}

		hlcUpperBound, err := kvserver.ReadMaxHLCUpperBound(ctx, bootstrappedEngines)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if hlcUpperBound > 0 {
			hlcUpperBoundExists = true
		}

		ensureClockMonotonicity(
			ctx,
			s.clock,
			s.startTime,
			hlcUpperBound,
			timeutil.SleepUntil,
		)

		// Ensure that any subsequent use of `cockroach init` will receive
		// an error "the cluster was already initialized."
		if _, err := s.initServer.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
			log.Fatal(ctx, err)
		}
	} else {
		// We have no existing stores. We start an initServer and then wait for
		// one of the following:
		//
		// - gossip connects (i.e. we're joining an existing cluster, perhaps
		//   freshly bootstrapped but this node doesn't have to know)
		// - we auto-bootstrap (if no join flags were given)
		// - a client bootstraps a cluster via node.
		//
		// TODO(knz): This may need tweaking when #24118 is addressed.

		startRPCServer(workersCtx)

		ready := make(chan struct{})
		if s.cfg.ReadyFn != nil {
			// s.cfg.ReadyFn must be called in any case because the `start`
			// command requires it to signal readiness to a process manager.
			//
			// However we want to be somewhat precisely informative to the user
			// about whether the node is waiting on init / join, or whether
			// the join was successful straight away. So we spawn this goroutine
			// and either:
			// - its timer will fire after 2 seconds and we call ReadyFn(true)
			// - bootstrap completes earlier and the ready chan gets closed,
			//   then we call ReadyFn(false).
			go func() {
				waitForInit := false
				tm := time.After(2 * time.Second)
				select {
				case <-tm:
					waitForInit = true
				case <-ready:
				}
				s.cfg.ReadyFn(waitForInit)
			}()
		}

		log.Info(ctx, "no stores bootstrapped and --join flag specified, awaiting init command or join with an already initialized node.")

		if len(s.cfg.GossipBootstrapResolvers) == 0 {
			// If the _unfiltered_ list of hosts from the --join flag is
			// empty, then this node can bootstrap a new cluster. We disallow
			// this if this node is being started with itself specified as a
			// --join host, because that's too likely to be operator error.
			if _, err := s.initServer.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
				return errors.Wrap(err, "while bootstrapping")
			}
			log.Infof(ctx, "**** add additional nodes by specifying --join=%s", s.cfg.AdvertiseAddr)
		}

		initRes, err := s.initServer.awaitBootstrap()
		close(ready)
		if err != nil {
			return err
		}

		doBootstrap = initRes == needBootstrap
		if doBootstrap {
			if err := s.bootstrapCluster(ctx, s.bootstrapVersion()); err != nil {
				return err
			}
		}
	}

	// This opens the main listener.
	startRPCServer(workersCtx)

	// We ran this before, but might've bootstrapped in the meantime. This time
	// we'll get the actual list of bootstrapped and empty engines.
	bootstrappedEngines, emptyEngines, cv, err := inspectEngines(
		ctx, s.engines,
		s.cfg.Settings.Version.BinaryVersion(),
		s.cfg.Settings.Version.BinaryMinSupportedVersion(),
		&s.rpcContext.ClusterID)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	timeThreshold := s.clock.Now().WallTime

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been bootstrapped or
	// we're joining an existing cluster for the first time.
	if err := s.node.start(
		ctx,
		advAddrU, advSQLAddrU,
		bootstrappedEngines, emptyEngines,
		s.cfg.ClusterName,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		cv,
		s.cfg.LocalityAddresses,
		s.execCfg.DistSQLPlanner.SetNodeDesc,
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
	s.replicationReporter.Start(ctx, s.stopper)
	s.temporaryObjectCleaner.Start(ctx, s.stopper)

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
	s.recorder.AddNode(s.registry, s.node.Descriptor, s.node.startedAt, s.cfg.AdvertiseAddr, s.cfg.HTTPAdvertiseAddr, s.cfg.SQLAdvertiseAddr)

	// Begin recording runtime statistics.
	s.startSampleEnvironment(ctx, DefaultMetricsSampleInterval)

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	var graphiteOnce sync.Once
	graphiteEndpoint.SetOnChange(&s.st.SV, func() {
		if graphiteEndpoint.Get(&s.st.SV) != "" {
			graphiteOnce.Do(func() {
				s.node.startGraphiteStatsExporter(s.st)
			})
		}
	})

	s.distSQLServer.Start()
	s.pgServer.Start(ctx, s.stopper)

	s.grpc.setMode(modeOperational)

	log.Infof(ctx, "starting %s server at %s (use: %s)",
		s.cfg.HTTPRequestScheme(), s.cfg.HTTPAddr, s.cfg.HTTPAdvertiseAddr)
	rpcConnType := "grpc/postgres"
	if s.cfg.SplitListenSQL {
		rpcConnType = "grpc"
		log.Infof(ctx, "starting postgres server at %s (use: %s)", s.cfg.SQLAddr, s.cfg.SQLAdvertiseAddr)
	}
	log.Infof(ctx, "starting %s server at %s", rpcConnType, s.cfg.Addr)
	log.Infof(ctx, "advertising CockroachDB node at %s", s.cfg.AdvertiseAddr)

	log.Event(ctx, "accepting connections")

	// Begin the node liveness heartbeat. Add a callback which records the local
	// store "last up" timestamp for every store whenever the liveness record is
	// updated.
	s.nodeLiveness.StartHeartbeat(ctx, s.stopper, func(ctx context.Context) {
		now := s.clock.Now()
		if err := s.node.stores.VisitStores(func(s *kvserver.Store) error {
			return s.WriteLastUpTimestamp(ctx, now)
		}); err != nil {
			log.Warning(ctx, errors.Wrap(err, "writing last up timestamp"))
		}
	})

	// Begin recording status summaries.
	s.node.startWriteNodeStatus(DefaultMetricsSampleInterval)

	// Start the background thread for periodically refreshing table statistics.
	if err := s.statsRefresher.Start(ctx, s.stopper, stats.DefaultRefreshInterval); err != nil {
		return err
	}

	// Start the protected timestamp subsystem.
	if err := s.protectedtsProvider.Start(ctx, s.stopper); err != nil {
		return err
	}
	if err := s.protectedtsReconciler.Start(ctx, s.stopper); err != nil {
		return err
	}

	// Before serving SQL requests, we have to make sure the database is
	// in an acceptable form for this version of the software.
	// We have to do this after actually starting up the server to be able to
	// seamlessly use the kv client against other nodes in the cluster.
	var mmKnobs sqlmigrations.MigrationManagerTestingKnobs
	if migrationManagerTestingKnobs := s.cfg.TestingKnobs.SQLMigrationManager; migrationManagerTestingKnobs != nil {
		mmKnobs = *migrationManagerTestingKnobs.(*sqlmigrations.MigrationManagerTestingKnobs)
	}
	migrationsExecutor := sql.MakeInternalExecutor(
		ctx, s.pgServer.SQLServer, s.internalMemMetrics, s.ClusterSettings())
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
		s.stopper,
		s.db,
		&migrationsExecutor,
		s.clock,
		mmKnobs,
		s.NodeID().String(),
		s.ClusterSettings(),
	)

	// Start garbage collecting system events.
	s.startSystemLogsGC(ctx)

	// Serve UI assets.
	//
	// The authentication mux used here is created in "allow anonymous" mode so that the UI
	// assets are served up whether or not there is a session. If there is a session, the mux
	// adds it to the context, and it is templated into index.html so that the UI can show
	// the username of the currently-logged-in user.
	authenticatedUIHandler := newAuthenticationMuxAllowAnonymous(
		s.authentication,
		ui.Handler(ui.Config{
			ExperimentalUseLogin: s.cfg.EnableWebSessionAuthentication,
			LoginEnabled:         s.cfg.RequireWebSession(),
			NodeID:               &s.nodeIDContainer,
			GetUser: func(ctx context.Context) *string {
				if u, ok := ctx.Value(webSessionUserKey{}).(string); ok {
					return &u
				}
				return nil
			},
		}),
	)
	s.mux.Handle("/", authenticatedUIHandler)

	// Register gRPC-gateway endpoints used by the admin UI.
	var authHandler http.Handler = gwMux
	if s.cfg.RequireWebSession() {
		authHandler = newAuthenticationMux(s.authentication, authHandler)
	}

	s.mux.Handle(adminPrefix, authHandler)
	// Exempt the health check endpoint from authentication.
	// This mirrors the handling of /health above.
	s.mux.Handle("/_admin/v1/health", gwMux)
	s.mux.Handle(ts.URLPrefix, authHandler)
	s.mux.Handle(statusPrefix, authHandler)
	// The /login endpoint is, by definition, available pre-authentication.
	s.mux.Handle(loginPath, gwMux)
	s.mux.Handle(logoutPath, authHandler)
	// The /_status/vars endpoint is not authenticated either. Useful for monitoring.
	s.mux.Handle(statusVars, http.HandlerFunc(s.status.handleVars))
	log.Event(ctx, "added http endpoints")

	// Start the jobs subsystem.
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

	// Run startup migrations (note: these depend on jobs subsystem running).
	var bootstrapVersion roachpb.Version
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.GetProto(ctx, keys.BootstrapVersionKey, &bootstrapVersion)
	}); err != nil {
		return err
	}
	if err := migMgr.EnsureMigrations(ctx, bootstrapVersion); err != nil {
		select {
		case <-s.stopper.ShouldQuiesce():
			// Avoid turning an early shutdown into a fatal error. See #19579.
			return errors.New("server is shutting down")
		default:
			log.Fatalf(ctx, "%+v", err)
		}
	}
	log.Infof(ctx, "done ensuring all necessary migrations have run")

	// Attempt to upgrade cluster version.
	s.startAttemptUpgrade(ctx)

	// Start serving SQL clients.
	if err := s.startServeSQL(ctx, workersCtx, connManager, pgL); err != nil {
		return err
	}

	// Record node start in telemetry. Get the right counter for this storage
	// engine type as well as type of start (initial boot vs restart).
	nodeStartCounter := "storage.engine."
	switch s.cfg.StorageEngine {
	case enginepb.EngineTypePebble:
		nodeStartCounter += "pebble."
	case enginepb.EngineTypeDefault:
		nodeStartCounter += "default."
	case enginepb.EngineTypeRocksDB:
		nodeStartCounter += "rocksdb."
	case enginepb.EngineTypeTeePebbleRocksDB:
		nodeStartCounter += "pebble+rocksdb."
	}
	if s.InitialBoot() {
		nodeStartCounter += "initial-boot"
	} else {
		nodeStartCounter += "restart"
	}
	telemetry.Count(nodeStartCounter)

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent()

	// Delete all orphaned table leases created by a prior instance of this
	// node. This also uses SQL.
	s.leaseMgr.DeleteOrphanedLeases(timeThreshold)

	log.Event(ctx, "server ready")

	return nil
}

// startListenRPCAndSQL starts the RPC and SQL listeners.
// It returns the SQL listener, which can be used
// to start the SQL server when initialization has completed.
// It also returns a function that starts the RPC server,
// when the cluster is known to have bootstrapped or
// when waiting for init().
func (s *Server) startListenRPCAndSQL(
	ctx, workersCtx context.Context,
) (sqlListener net.Listener, startRPCServer func(ctx context.Context), err error) {
	rpcChanName := "rpc/sql"
	if s.cfg.SplitListenSQL {
		rpcChanName = "rpc"
	}
	var ln net.Listener
	if k := s.cfg.TestingKnobs.Server; k != nil {
		knobs := k.(*TestingKnobs)
		ln = knobs.RPCListener
	}
	if ln == nil {
		var err error
		ln, err = listen(ctx, &s.cfg.Addr, &s.cfg.AdvertiseAddr, rpcChanName)
		if err != nil {
			return nil, nil, err
		}
		log.Eventf(ctx, "listening on port %s", s.cfg.Addr)
	}

	var pgL net.Listener
	if s.cfg.SplitListenSQL {
		pgL, err = listen(ctx, &s.cfg.SQLAddr, &s.cfg.SQLAdvertiseAddr, "sql")
		if err != nil {
			return nil, nil, err
		}
		// The SQL listener shutdown worker, which closes everything under
		// the SQL port when the stopper indicates we are shutting down.
		s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
			<-s.stopper.ShouldQuiesce()
			if err := pgL.Close(); err != nil {
				log.Fatal(workersCtx, err)
			}
		})
		log.Eventf(ctx, "listening on sql port %s", s.cfg.SQLAddr)
	}

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

	// serveOnMux is used to ensure that the mux gets listened on eventually,
	// either via the returned startRPCServer() or upon stopping.
	var serveOnMux sync.Once

	m := cmux.New(ln)

	if !s.cfg.SplitListenSQL {
		// If the pg port is split, it will be opened above. Otherwise,
		// we make it hang off the RPC listener via cmux here.
		pgL = m.Match(func(r io.Reader) bool {
			return pgwire.Match(r)
		})
		// Also if the pg port is not split, the actual listen
		// and advertise address for SQL become equal to that of RPC,
		// regardless of what was configured.
		s.cfg.SQLAddr = s.cfg.Addr
		s.cfg.SQLAdvertiseAddr = s.cfg.AdvertiseAddr
	}

	anyL := m.Match(cmux.Any())
	if serverTestKnobs, ok := s.cfg.TestingKnobs.Server.(*TestingKnobs); ok {
		if serverTestKnobs.ContextTestingKnobs.ArtificialLatencyMap != nil {
			anyL = rpc.NewDelayingListener(anyL)
		}
	}

	// The remainder shutdown worker.
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		<-s.stopper.ShouldQuiesce()
		// TODO(bdarnell): Do we need to also close the other listeners?
		netutil.FatalIfUnexpected(anyL.Close())
		<-s.stopper.ShouldStop()
		s.grpc.Stop()
		serveOnMux.Do(func() {
			// The cmux matches don't shut down properly unless serve is called on the
			// cmux at some point. Use serveOnMux to ensure it's called during shutdown
			// if we wouldn't otherwise reach the point where we start serving on it.
			netutil.FatalIfUnexpected(m.Serve())
		})
	})

	// Serve the gRPC endpoint.
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(anyL))
	})

	// startRPCServer starts the RPC server. We do not do this
	// immediately because we want the cluster to be ready (or ready to
	// initialize) before we accept RPC requests. The caller
	// (Server.Start) will call this at the right moment.
	startRPCServer = func(ctx context.Context) {
		s.stopper.RunWorker(ctx, func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})
	}

	return pgL, startRPCServer, nil
}

func (s *Server) startServeUI(
	ctx, workersCtx context.Context, connManager netutil.Server, uiTLSConfig *tls.Config,
) error {
	httpLn, err := listen(ctx, &s.cfg.HTTPAddr, &s.cfg.HTTPAdvertiseAddr, "http")
	if err != nil {
		return err
	}
	log.Eventf(ctx, "listening on http port %s", s.cfg.HTTPAddr)

	// The HTTP listener shutdown worker, which closes everything under
	// the HTTP port when the stopper indicates we are shutting down.
	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Fatal(workersCtx, err)
		}
	})

	if uiTLSConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		// Dispatch incoming requests to either clearL or tlsL.
		s.stopper.RunWorker(workersCtx, func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		})

		// Serve the plain HTTP (non-TLS) connection over clearL.
		// This produces a HTTP redirect to the `https` URL for the path /,
		// handles the request normally (via s.ServeHTTP) for the path /health,
		// and produces 404 for anything else.
		s.stopper.RunWorker(workersCtx, func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle("/health", s)

			plainRedirectServer := netutil.MakeServer(s.stopper, uiTLSConfig, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		})

		httpLn = tls.NewListener(tlsL, uiTLSConfig)
	}

	// Serve the HTTP endpoint. This will be the original httpLn
	// listening on --http-listen-addr without TLS if uiTLSConfig was
	// nil, or overridden above if uiTLSConfig was not nil to come from
	// the TLS negotiation over the HTTP port.
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})

	return nil
}

func (s *Server) startServeSQL(
	ctx, workersCtx context.Context, connManager netutil.Server, pgL net.Listener,
) error {
	log.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
	tcpKeepAlive := tcpKeepAliveManager{
		tcpKeepAlive: envutil.EnvOrDefaultDuration("COCKROACH_SQL_TCP_KEEP_ALIVE", time.Minute),
	}

	s.stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
		netutil.FatalIfUnexpected(connManager.ServeWith(pgCtx, s.stopper, pgL, func(conn net.Conn) {
			connCtx := logtags.AddTag(pgCtx, "client", conn.RemoteAddr().String())
			tcpKeepAlive.configure(connCtx, conn)

			if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketTCP); err != nil {
				log.Errorf(connCtx, "serving SQL client conn: %v", err)
			}
		}))
	})

	// If a unix socket was requested, start serving there too.
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
			netutil.FatalIfUnexpected(connManager.ServeWith(pgCtx, s.stopper, unixLn, func(conn net.Conn) {
				connCtx := logtags.AddTag(pgCtx, "client", conn.RemoteAddr().String())
				if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketUnix); err != nil {
					log.Error(connCtx, err)
				}
			}))
		})
	}
	return nil
}

func (s *Server) bootstrapVersion() roachpb.Version {
	v := s.cfg.Settings.Version.BinaryVersion()
	if knobs := s.cfg.TestingKnobs.Server; knobs != nil {
		if ov := knobs.(*TestingKnobs).BootstrapVersionOverride; ov != (roachpb.Version{}) {
			v = ov
		}
	}
	return v
}

func (s *Server) bootstrapCluster(ctx context.Context, bootstrapVersion roachpb.Version) error {
	if err := s.node.bootstrapCluster(
		ctx, s.engines, clusterversion.ClusterVersion{Version: bootstrapVersion},
		&s.cfg.DefaultZoneConfig, &s.cfg.DefaultSystemZoneConfig,
	); err != nil {
		return err
	}
	// Force all the system ranges through the replication queue so they
	// upreplicate as quickly as possible when a new node joins. Without this
	// code, the upreplication would be up to the whim of the scanner, which
	// might be too slow for new clusters.
	done := false
	return s.node.stores.VisitStores(func(store *kvserver.Store) error {
		if !done {
			done = true
			return store.ForceReplicationScanAndProcess()
		}
		return nil
	})
}

func (s *Server) doDrain(
	ctx context.Context, modes []serverpb.DrainMode, setTo bool,
) ([]serverpb.DrainMode, error) {
	for _, mode := range modes {
		switch mode {
		case serverpb.DrainMode_CLIENT:
			if setTo {
				s.grpc.setMode(modeDraining)
				// Wait for drainUnreadyWait. This will fail load balancer checks and
				// delay draining so that client traffic can move off this node.
				time.Sleep(drainWait.Get(&s.st.SV))
			}
			if err := func() error {
				if !setTo {
					// Execute this last.
					defer func() { s.grpc.setMode(modeOperational) }()
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
	return s.doDrain(ctx, on, true /* setTo */)
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
			if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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

// startSampleEnvironment begins the heap profiler worker.
func (s *Server) startSampleEnvironment(ctx context.Context, frequency time.Duration) {
	// Immediately record summaries once on server startup.
	ctx = s.AnnotateCtx(ctx)
	goroutineDumper, err := goroutinedumper.NewGoroutineDumper(s.cfg.GoroutineDumpDirName)
	if err != nil {
		log.Infof(ctx, "Could not start goroutine dumper worker due to: %s", err)
	}

	// We're not going to take heap profiles if running with in-memory stores.
	// This helps some tests that can't write any files.
	allStoresInMem := true
	for _, storeSpec := range s.cfg.Stores.Specs {
		if !storeSpec.InMemory {
			allStoresInMem = false
			break
		}
	}

	var heapProfiler *heapprofiler.HeapProfiler
	if s.cfg.HeapProfileDirName != "" && !allStoresInMem {
		if err := os.MkdirAll(s.cfg.HeapProfileDirName, 0755); err != nil {
			log.Fatalf(ctx, "Could not create heap profiles dir: %s", err)
		}
		heapProfiler, err = heapprofiler.NewHeapProfiler(
			s.cfg.HeapProfileDirName, s.ClusterSettings())
		if err != nil {
			log.Fatalf(ctx, "Could not start heap profiler worker due to: %s", err)
		}
	}

	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		var goMemStats atomic.Value // *status.GoMemStats
		goMemStats.Store(&status.GoMemStats{})
		var collectingMemStats int32 // atomic, 1 when stats call is ongoing

		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(frequency)

		for {
			select {
			case <-s.stopper.ShouldStop():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(frequency)

				// We read the heap stats on another goroutine and give up after 1s.
				// This is necessary because as of Go 1.12, runtime.ReadMemStats()
				// "stops the world" and that requires first waiting for any current GC
				// run to finish. With a large heap and under extreme conditions, a
				// single GC run may take longer than the default sampling period of
				// 10s. Under normal operations and with more recent versions of Go,
				// this hasn't been observed to be a problem.
				statsCollected := make(chan struct{})
				if atomic.CompareAndSwapInt32(&collectingMemStats, 0, 1) {
					if err := s.stopper.RunAsyncTask(ctx, "get-mem-stats", func(ctx context.Context) {
						var ms status.GoMemStats
						runtime.ReadMemStats(&ms.MemStats)
						ms.Collected = timeutil.Now()
						log.VEventf(ctx, 2, "memstats: %+v", ms)

						goMemStats.Store(&ms)
						atomic.StoreInt32(&collectingMemStats, 0)
						close(statsCollected)
					}); err != nil {
						close(statsCollected)
					}
				}

				select {
				case <-statsCollected:
					// Good; we managed to read the Go memory stats quickly enough.
				case <-time.After(time.Second):
				}

				curStats := goMemStats.Load().(*status.GoMemStats)
				s.runtime.SampleEnvironment(ctx, *curStats)
				if goroutineDumper != nil {
					goroutineDumper.MaybeDump(ctx, s.ClusterSettings(), s.runtime.Goroutines.Value())
				}
				if heapProfiler != nil {
					heapProfiler.MaybeTakeProfile(ctx, curStats.MemStats)
				}

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
			if err := gzw.Close(); err != nil && err != http.ErrBodyNotAllowed {
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

// PGServer exports the pgwire server. Used by tests.
func (s *Server) PGServer() *pgwire.Server {
	return s.pgServer
}

// TODO(benesch): Use https://github.com/NYTimes/gziphandler instead.
// gzipResponseWriter reinvents the wheel and is not as robust.
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
	// The underlying http.ResponseWriter can't sniff gzipped data properly, so we
	// do our own sniffing on the uncompressed data.
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
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

func init() {
	tracing.RegisterTagRemapping("n", "node")
}

// configure attempts to set TCP keep-alive on
// connection. Does not fail on errors.
func (k *tcpKeepAliveManager) configure(ctx context.Context, conn net.Conn) {
	if k.tcpKeepAlive == 0 {
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
	doLog := atomic.CompareAndSwapInt32(&k.loggedKeepAliveStatus, 0, 1)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		if doLog {
			log.Warningf(ctx, "failed to enable TCP keep-alive for pgwire: %v", err)
		}
		return

	}
	if err := tcpConn.SetKeepAlivePeriod(k.tcpKeepAlive); err != nil {
		if doLog {
			log.Warningf(ctx, "failed to set TCP keep-alive duration for pgwire: %v", err)
		}
		return
	}

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive to %s for pgwire", k.tcpKeepAlive)
	}
}

type tcpKeepAliveManager struct {
	// The keepalive duration.
	tcpKeepAlive time.Duration
	// loggedKeepAliveStatus ensures that errors about setting the TCP
	// keepalive status are only reported once.
	loggedKeepAliveStatus int32
}

func listen(
	ctx context.Context, addr, advertiseAddr *string, connName string,
) (net.Listener, error) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return nil, ListenError{
			error: err,
			Addr:  *addr,
		}
	}
	if err := base.UpdateAddrs(ctx, addr, advertiseAddr, ln.Addr()); err != nil {
		return nil, errors.Wrapf(err, "internal error: cannot parse %s listen address", connName)
	}
	return ln, nil
}

// RunLocalSQL calls fn on a SQL internal executor on this server.
// This is meant for use for SQL initialization during bootstrapping.
//
// The internal SQL interface should be used instead of a regular SQL
// network connection for SQL initializations when setting up a new
// server, because it is possible for the server to listen on a
// network interface that is not reachable from loopback. It is also
// possible for the TLS certificates to be invalid when used locally
// (e.g. if the hostname in the cert is an advertised address that's
// only reachable externally).
func (s *Server) RunLocalSQL(
	ctx context.Context, fn func(ctx context.Context, sqlExec *sql.InternalExecutor) error,
) error {
	return fn(ctx, s.internalExecutor)
}
