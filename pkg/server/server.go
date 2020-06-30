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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/container"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/reports"
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
	_ "github.com/cockroachdb/cockroach/pkg/sql/gcjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/sentry-go"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	// Allocation pool for gzipResponseWriters.
	gzipResponseWriterPool sync.Pool

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
	// The following fields are populated in NewServer.

	nodeIDContainer *base.NodeIDContainer
	cfg             Config
	st              *cluster.Settings
	mux             safeServeMux
	clock           *hlc.Clock
	rpcContext      *rpc.Context
	// The gRPC server on which the different RPC handlers will be registered.
	grpc         *grpcServer
	gossip       *gossip.Gossip
	nodeDialer   *nodedialer.Dialer
	nodeLiveness *kvserver.NodeLiveness
	storePool    *kvserver.StorePool
	tcsFactory   *kvcoord.TxnCoordSenderFactory
	distSender   *kvcoord.DistSender
	db           *kv.DB
	node         *Node
	registry     *metric.Registry
	recorder     *status.MetricsRecorder
	runtime      *status.RuntimeStatSampler

	admin          *adminServer
	status         *statusServer
	authentication *authenticationServer
	tsDB           *ts.DB
	tsServer       *ts.Server
	raftTransport  *kvserver.RaftTransport
	stopper        *stop.Stopper

	debug *debug.Server

	replicationReporter   *reports.Reporter
	protectedtsProvider   protectedts.Provider
	protectedtsReconciler *ptreconcile.Reconciler

	sqlServer *sqlServer

	// Created in NewServer but initialized (made usable) in `(*Server).Start`.
	externalStorageBuilder *externalStorageBuilder

	// The following fields are populated at start time, i.e. in `(*Server).Start`.

	startTime time.Time
	engines   Engines
}

// externalStorageBuilder is a wrapper around the ExternalStorage factory
// methods. It allows us to separate the creation and initialization of the
// builder between NewServer() and Start() respectively.
// TODO(adityamaru): Consider moving this to pkg/storage/cloudimpl at a future
// stage of the ongoing refactor.
type externalStorageBuilder struct {
	conf              base.ExternalIODirConfig
	settings          *cluster.Settings
	blobClientFactory blobs.BlobClientFactory
	initCalled        bool
	ie                *sql.InternalExecutor
	db                *kv.DB
}

func (e *externalStorageBuilder) init(
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	ie *sql.InternalExecutor,
	db *kv.DB,
) {
	e.conf = conf
	e.settings = settings
	e.blobClientFactory = blobClientFactory
	e.initCalled = true
	e.ie = ie
	e.db = db
}

func (e *externalStorageBuilder) makeExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloudimpl.MakeExternalStorage(ctx, dest, e.conf, e.settings, e.blobClientFactory, e.ie,
		e.db)
}

func (e *externalStorageBuilder) makeExternalStorageFromURI(
	ctx context.Context, uri string, user string,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloudimpl.ExternalStorageFromURI(ctx, uri, e.conf, e.settings, e.blobClientFactory, user, e.ie, e.db)
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

	var clock *hlc.Clock
	if cfg.ClockDevicePath != "" {
		clockSrc, err := hlc.MakeClockSource(context.Background(), cfg.ClockDevicePath)
		if err != nil {
			return nil, errors.Wrap(err, "instantiating clock source")
		}
		clock = hlc.NewClock(clockSrc.UnixNano, time.Duration(cfg.MaxOffset))
	} else {
		clock = hlc.NewClock(hlc.UnixNano, time.Duration(cfg.MaxOffset))
	}
	registry := metric.NewRegistry()
	// If the tracer has a Close function, call it after the server stops.
	if tr, ok := cfg.AmbientCtx.Tracer.(stop.Closer); ok {
		stopper.AddCloser(tr)
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
	nodeIDContainer := &base.NodeIDContainer{}
	cfg.AmbientCtx.AddLogTag("n", nodeIDContainer)
	const sqlInstanceID = base.SQLInstanceID(0)
	idContainer := base.NewSQLIDContainer(sqlInstanceID, nodeIDContainer, true /* exposed */)

	ctx := cfg.AmbientCtx.AnnotateCtx(context.Background())

	var rpcContext *rpc.Context
	if knobs := cfg.TestingKnobs.Server; knobs != nil {
		serverKnobs := knobs.(*TestingKnobs)
		rpcContext = rpc.NewContext(rpc.ContextOptions{
			AmbientCtx: cfg.AmbientCtx,
			Config:     cfg.Config,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cfg.Settings,
			Knobs:      serverKnobs.ContextTestingKnobs,
		})
	} else {
		rpcContext = rpc.NewContext(rpc.ContextOptions{
			AmbientCtx: cfg.AmbientCtx,
			Config:     cfg.Config,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cfg.Settings,
		})
	}
	rpcContext.HeartbeatCB = func() {
		if err := rpcContext.RemoteClocks.VerifyClockOffset(ctx); err != nil {
			log.Fatalf(ctx, "%v", err)
		}
	}
	registry.AddMetricStruct(rpcContext.Metrics())

	// Attempt to load TLS configs right away, failures are permanent.
	if !cfg.Insecure {
		// TODO(peter): Call methods on CertificateManager directly. Need to call
		// base.wrapError or similar on the resulting error.
		if _, err := rpcContext.GetServerTLSConfig(); err != nil {
			return nil, err
		}
		if _, err := rpcContext.GetUIServerTLSConfig(); err != nil {
			return nil, err
		}
		if _, err := rpcContext.GetClientTLSConfig(); err != nil {
			return nil, err
		}
		cm, err := rpcContext.GetCertificateManager()
		if err != nil {
			return nil, err
		}
		cm.RegisterSignalHandler(stopper)
		registry.AddMetricStruct(cm.Metrics())
	}

	// Check the compatibility between the configured addresses and that
	// provided in certificates. This also logs the certificate
	// addresses in all cases to aid troubleshooting.
	// This must be called after the certificate manager was initialized
	// and after ValidateAddrs().
	rpcContext.CheckCertificateAddrs(ctx)

	grpcServer := newGRPCServer(rpcContext)

	g := gossip.New(
		cfg.AmbientCtx,
		&rpcContext.ClusterID,
		nodeIDContainer,
		rpcContext,
		grpcServer.Server,
		stopper,
		registry,
		cfg.Locality,
		&cfg.DefaultZoneConfig,
	)
	nodeDialer := nodedialer.New(rpcContext, gossip.AddressResolver(g))

	runtimeSampler := status.NewRuntimeStatSampler(ctx, clock)
	registry.AddMetricStruct(runtimeSampler)

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
	if kvKnobs := cfg.TestingKnobs.KVClient; kvKnobs != nil {
		clientTestingKnobs = *kvKnobs.(*kvcoord.ClientTestingKnobs)
	}
	retryOpts := cfg.RetryOptions
	if retryOpts == (retry.Options{}) {
		retryOpts = base.DefaultRetryOptions()
	}
	retryOpts.Closer = stopper.ShouldQuiesce()
	distSenderCfg := kvcoord.DistSenderConfig{
		AmbientCtx:         cfg.AmbientCtx,
		Settings:           st,
		Clock:              clock,
		NodeDescs:          g,
		RPCContext:         rpcContext,
		RPCRetryOptions:    &retryOpts,
		NodeDialer:         nodeDialer,
		FirstRangeProvider: g,
		TestingKnobs:       clientTestingKnobs,
	}
	distSender := kvcoord.NewDistSender(distSenderCfg)
	registry.AddMetricStruct(distSender.Metrics())

	txnMetrics := kvcoord.MakeTxnMetrics(cfg.HistogramWindowInterval())
	registry.AddMetricStruct(txnMetrics)
	txnCoordSenderFactoryCfg := kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx:   cfg.AmbientCtx,
		Settings:     st,
		Clock:        clock,
		Stopper:      stopper,
		Linearizable: cfg.Linearizable,
		Metrics:      txnMetrics,
		TestingKnobs: clientTestingKnobs,
	}
	tcsFactory := kvcoord.NewTxnCoordSenderFactory(txnCoordSenderFactoryCfg, distSender)

	dbCtx := kv.DefaultDBContext()
	dbCtx.NodeID = idContainer
	dbCtx.Stopper = stopper
	db := kv.NewDBWithContext(cfg.AmbientCtx, tcsFactory, clock, dbCtx)

	nlActive, nlRenewal := cfg.NodeLivenessDurations()

	nodeLiveness := kvserver.NewNodeLiveness(
		cfg.AmbientCtx,
		clock,
		db,
		g,
		nlActive,
		nlRenewal,
		st,
		cfg.HistogramWindowInterval(),
	)
	registry.AddMetricStruct(nodeLiveness.Metrics())

	storePool := kvserver.NewStorePool(
		cfg.AmbientCtx,
		st,
		g,
		clock,
		nodeLiveness.GetNodeCount,
		kvserver.MakeStorePoolNodeLivenessFunc(nodeLiveness),
		/* deterministic */ false,
	)

	raftTransport := kvserver.NewRaftTransport(
		cfg.AmbientCtx, st, nodeDialer, grpcServer.Server, stopper,
	)

	tsDB := ts.NewDB(db, cfg.Settings)
	registry.AddMetricStruct(tsDB.Metrics())
	nodeCountFn := func() int64 {
		return nodeLiveness.Metrics().LiveNodes.Value()
	}
	sTS := ts.MakeServer(cfg.AmbientCtx, tsDB, nodeCountFn, cfg.TimeSeriesServerConfig, stopper)

	// The InternalExecutor will be further initialized later, as we create more
	// of the server's components. There's a circular dependency - many things
	// need an InternalExecutor, but the InternalExecutor needs an ExecutorConfig,
	// which in turn needs many things. That's why everybody that needs an
	// InternalExecutor uses this one instance.
	internalExecutor := &sql.InternalExecutor{}
	jobRegistry := &jobs.Registry{} // ditto

	// Create an ExternalStorageBuilder. This is only usable after Start() where
	// we initialize all the configuration params.
	externalStorageBuilder := &externalStorageBuilder{}
	externalStorage := func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.
		ExternalStorage, error) {
		return externalStorageBuilder.makeExternalStorage(ctx, dest)
	}
	externalStorageFromURI := func(ctx context.Context, uri string,
		user string) (cloud.ExternalStorage, error) {
		return externalStorageBuilder.makeExternalStorageFromURI(ctx, uri, user)
	}

	protectedtsProvider, err := ptprovider.New(ptprovider.Config{
		DB:               db,
		InternalExecutor: internalExecutor,
		Settings:         st,
	})
	if err != nil {
		return nil, err
	}

	// Break a circular dependency: we need a Node to make a StoreConfig (for
	// ClosedTimestamp), but the Node needs a StoreConfig to be made.
	var lateBoundNode *Node

	storeCfg := kvserver.StoreConfig{
		DefaultZoneConfig:       &cfg.DefaultZoneConfig,
		Settings:                st,
		AmbientCtx:              cfg.AmbientCtx,
		RaftConfig:              cfg.RaftConfig,
		Clock:                   clock,
		DB:                      db,
		Gossip:                  g,
		NodeLiveness:            nodeLiveness,
		Transport:               raftTransport,
		NodeDialer:              nodeDialer,
		RPCContext:              rpcContext,
		ScanInterval:            cfg.ScanInterval,
		ScanMinIdleTime:         cfg.ScanMinIdleTime,
		ScanMaxIdleTime:         cfg.ScanMaxIdleTime,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		StorePool:               storePool,
		SQLExecutor:             internalExecutor,
		LogRangeEvents:          cfg.EventLogEnabled,
		RangeDescriptorCache:    distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:     tsDB,

		// Initialize the closed timestamp subsystem. Note that it won't
		// be ready until it is .Start()ed, but the grpc server can be
		// registered early.
		ClosedTimestamp: container.NewContainer(container.Config{
			Settings: st,
			Stopper:  stopper,
			Clock:    nodeLiveness.AsLiveClock(),
			// NB: s.node is not defined at this point, but it will be
			// before this is ever called.
			Refresh: func(rangeIDs ...roachpb.RangeID) {
				for _, rangeID := range rangeIDs {
					repl, _, err := lateBoundNode.stores.GetReplicaForRangeID(rangeID)
					if err != nil || repl == nil {
						continue
					}
					repl.EmitMLAI()
				}
			},
			Dialer: nodeDialer.CTDialer(),
		}),

		EnableEpochRangeLeases:  true,
		ExternalStorage:         externalStorage,
		ExternalStorageFromURI:  externalStorageFromURI,
		ProtectedTimestampCache: protectedtsProvider,
	}
	if storeTestingKnobs := cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*kvserver.StoreTestingKnobs)
	}

	recorder := status.NewMetricsRecorder(clock, nodeLiveness, rpcContext, g, st)
	registry.AddMetricStruct(rpcContext.RemoteClocks.Metrics())

	node := NewNode(
		storeCfg, recorder, registry, stopper,
		txnMetrics, nil /* execCfg */, &rpcContext.ClusterID)
	lateBoundNode = node
	roachpb.RegisterInternalServer(grpcServer.Server, node)
	kvserver.RegisterPerReplicaServer(grpcServer.Server, node.perReplicaServer)
	node.storeCfg.ClosedTimestamp.RegisterClosedTimestampServer(grpcServer.Server)
	replicationReporter := reports.NewReporter(
		db, node.stores, storePool, st, nodeLiveness, internalExecutor)

	protectedtsReconciler := ptreconcile.NewReconciler(ptreconcile.Config{
		Settings: st,
		Stores:   node.stores,
		DB:       db,
		Storage:  protectedtsProvider,
		Cache:    protectedtsProvider,
		StatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.MetaType: jobsprotectedts.MakeStatusFunc(jobRegistry),
		},
	})
	registry.AddMetricStruct(protectedtsReconciler.Metrics())

	lateBoundServer := &Server{}
	// TODO(tbg): give adminServer only what it needs (and avoid circular deps).
	sAdmin := newAdminServer(lateBoundServer)
	sessionRegistry := sql.NewSessionRegistry()

	sStatus := newStatusServer(
		cfg.AmbientCtx,
		st,
		cfg.Config,
		sAdmin,
		db,
		g,
		recorder,
		nodeLiveness,
		storePool,
		rpcContext,
		node.stores,
		stopper,
		sessionRegistry,
		internalExecutor,
	)
	// TODO(tbg): don't pass all of Server into this to avoid this hack.
	sAuth := newAuthenticationServer(lateBoundServer)
	for i, gw := range []grpcGatewayServer{sAdmin, sStatus, sAuth, &sTS} {
		if reflect.ValueOf(gw).IsNil() {
			return nil, errors.Errorf("%d: nil", i)
		}
		gw.RegisterService(grpcServer.Server)
	}

	var jobAdoptionStopFile string
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory && spec.Path != "" {
			jobAdoptionStopFile = filepath.Join(spec.Path, jobs.PreventAdoptionFile)
			break
		}
	}

	sqlServer, err := newSQLServer(ctx, sqlServerArgs{
		sqlServerOptionalArgs: sqlServerOptionalArgs{
			rpcContext:             rpcContext,
			distSender:             distSender,
			statusServer:           serverpb.MakeOptionalStatusServer(sStatus),
			nodeLiveness:           sqlbase.MakeOptionalNodeLiveness(nodeLiveness),
			gossip:                 gossip.MakeExposedGossip(g),
			nodeDialer:             nodeDialer,
			grpcServer:             grpcServer.Server,
			recorder:               recorder,
			nodeIDContainer:        idContainer,
			externalStorage:        externalStorage,
			externalStorageFromURI: externalStorageFromURI,
			isMeta1Leaseholder:     node.stores.IsMeta1Leaseholder,
		},
		SQLConfig:                &cfg.SQLConfig,
		BaseConfig:               &cfg.BaseConfig,
		stopper:                  stopper,
		clock:                    clock,
		runtime:                  runtimeSampler,
		db:                       db,
		registry:                 registry,
		sessionRegistry:          sessionRegistry,
		circularInternalExecutor: internalExecutor,
		circularJobRegistry:      jobRegistry,
		jobAdoptionStopFile:      jobAdoptionStopFile,
		protectedtsProvider:      protectedtsProvider,
	})
	if err != nil {
		return nil, err
	}
	sStatus.setStmtDiagnosticsRequester(sqlServer.execCfg.StmtDiagnosticsRecorder)
	debugServer := debug.NewServer(st, sqlServer.pgServer.HBADebugFn())
	node.InitLogger(sqlServer.execCfg)

	*lateBoundServer = Server{
		nodeIDContainer:        nodeIDContainer,
		cfg:                    cfg,
		st:                     st,
		clock:                  clock,
		rpcContext:             rpcContext,
		grpc:                   grpcServer,
		gossip:                 g,
		nodeDialer:             nodeDialer,
		nodeLiveness:           nodeLiveness,
		storePool:              storePool,
		tcsFactory:             tcsFactory,
		distSender:             distSender,
		db:                     db,
		node:                   node,
		registry:               registry,
		recorder:               recorder,
		runtime:                runtimeSampler,
		admin:                  sAdmin,
		status:                 sStatus,
		authentication:         sAuth,
		tsDB:                   tsDB,
		tsServer:               &sTS,
		raftTransport:          raftTransport,
		stopper:                stopper,
		debug:                  debugServer,
		replicationReporter:    replicationReporter,
		protectedtsProvider:    protectedtsProvider,
		protectedtsReconciler:  protectedtsReconciler,
		sqlServer:              sqlServer,
		externalStorageBuilder: externalStorageBuilder,
	}
	return lateBoundServer, err
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
	cause error
	Addr  string
}

// Error implements error.
func (l *ListenError) Error() string { return l.cause.Error() }

// Unwrap is because ListenError is a wrapper.
func (l *ListenError) Unwrap() error { return l.cause }

// inspectEngines goes through engines and populates in initDiskState. It also
// calls SynthesizeClusterVersionFromEngines, which selects and backfills the
// cluster version to all initialized engines.
//
// The initDiskState returned by this method will reflect a zero NodeID if none
// has been assigned yet (i.e. if none of the engines is initialized).
func inspectEngines(
	ctx context.Context,
	engines []storage.Engine,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
) (*initDiskState, error) {
	state := &initDiskState{}

	for _, eng := range engines {
		storeIdent, err := kvserver.ReadStoreIdent(ctx, eng)
		if errors.HasType(err, (*kvserver.NotBootstrappedError)(nil)) {
			state.newEngines = append(state.newEngines, eng)
			continue
		} else if err != nil {
			return nil, err
		}

		if state.clusterID != uuid.Nil && state.clusterID != storeIdent.ClusterID {
			return nil, errors.Errorf("conflicting store ClusterIDs: %s, %s", storeIdent.ClusterID, state.clusterID)
		}
		state.clusterID = storeIdent.ClusterID

		if storeIdent.StoreID == 0 || storeIdent.NodeID == 0 || storeIdent.ClusterID == uuid.Nil {
			return nil, errors.Errorf("partially initialized store: %+v", storeIdent)
		}

		if state.nodeID != 0 && state.nodeID != storeIdent.NodeID {
			return nil, errors.Errorf("conflicting store NodeIDs: %s, %s", storeIdent.NodeID, state.nodeID)
		}
		state.nodeID = storeIdent.NodeID

		state.initializedEngines = append(state.initializedEngines, eng)
	}

	cv, err := kvserver.SynthesizeClusterVersionFromEngines(ctx, state.initializedEngines, binaryVersion, binaryMinSupportedVersion)
	if err != nil {
		return nil, err
	}
	state.clusterVersion = cv
	return state, nil
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

// startMonitoringForwardClockJumps starts a background task to monitor forward
// clock jumps based on a cluster setting
func (s *Server) startMonitoringForwardClockJumps(ctx context.Context) error {
	forwardJumpCheckEnabled := make(chan bool, 1)
	s.stopper.AddCloser(stop.CloserFn(func() { close(forwardJumpCheckEnabled) }))

	forwardClockJumpCheckEnabled.SetOnChange(&s.st.SV, func() {
		forwardJumpCheckEnabled <- forwardClockJumpCheckEnabled.Get(&s.st.SV)
	})

	if err := s.clock.StartMonitoringForwardClockJumps(
		ctx,
		forwardJumpCheckEnabled,
		time.NewTicker,
		nil, /* tick callback */
	); err != nil {
		return errors.Wrap(err, "monitoring forward clock jumps")
	}

	log.Info(ctx, "monitoring forward clock jumps based on server.clock.forward_jump_check_enabled")
	return nil
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
	ctx context.Context,
	hlcUpperBoundExists bool,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
) error {
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
			return errors.Wrap(err, "refreshing HLC upper bound")
		}
	}

	s.stopper.RunWorker(
		ctx,
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
	return nil
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
	if err := s.startMonitoringForwardClockJumps(ctx); err != nil {
		return err
	}

	// Connect the node as loopback handler for RPC requests to the
	// local node.
	s.rpcContext.SetLocalInternalServer(s.node)

	// Load the TLS configuration for the HTTP server.
	uiTLSConfig, err := s.rpcContext.GetUIServerTLSConfig()
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

	s.engines, err = s.cfg.CreateEngines(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create engines")
	}
	s.stopper.AddCloser(&s.engines)

	// Initialize the external storage builders configuration params now that the
	// engines have been created. The object can be used to create ExternalStorage
	// objects hereafter.
	fileTableInternalExecutor := sql.MakeInternalExecutor(ctx, s.PGServer().SQLServer, sql.MemoryMetrics{}, s.st)
	s.externalStorageBuilder.init(s.cfg.ExternalIODirConfig, s.st,
		blobs.NewBlobClientFactory(s.nodeIDContainer.Get(),
			s.nodeDialer, s.st.ExternalIODir), &fileTableInternalExecutor, s.db)

	bootstrapVersion := s.cfg.Settings.Version.BinaryVersion()
	if knobs := s.cfg.TestingKnobs.Server; knobs != nil {
		if ov := knobs.(*TestingKnobs).BootstrapVersionOverride; ov != (roachpb.Version{}) {
			bootstrapVersion = ov
		}
	}

	// Set up the init server. We have to do this relatively early because we
	// can't call RegisterInitServer() after `grpc.Serve`, which is called in
	// startRPCServer (and for the loopback grpc-gw connection).
	initServer, err := setupInitServer(
		ctx,
		s.cfg.Settings.Version.BinaryVersion(),
		s.cfg.Settings.Version.BinaryMinSupportedVersion(),
		bootstrapVersion,
		&s.cfg.DefaultZoneConfig,
		&s.cfg.DefaultSystemZoneConfig,
		s.engines,
	)
	if err != nil {
		return err
	}

	{
		// Set up the callback that persists gossiped version bumps to the
		// engines. The invariant we uphold here is that the bump needs to be
		// persisted on all engines before it becomes "visible" to the version
		// setting. To this end,
		//
		// a) make sure Gossip is not started yet, and
		// b) set up the BeforeChange callback on the version setting to persist
		//    incoming updates to all engines.
		// c) write back the disk-loaded cluster version to all engines,
		// d) initialize the version setting (with the disk-loaded version).
		//
		// Note that "all engines" means "all engines", not "all initialized
		// engines". We cannot initialize engines this early in the boot
		// sequence.
		s.gossip.AssertNotStarted(ctx)

		// Serialize the callback through a mutex to make sure we're not
		// clobbering the disk state if callback gets fired off concurrently.
		var mu syncutil.Mutex
		cb := func(ctx context.Context, newCV clusterversion.ClusterVersion) {
			mu.Lock()
			defer mu.Unlock()
			v := s.cfg.Settings.Version
			prevCV, err := kvserver.SynthesizeClusterVersionFromEngines(
				ctx, s.engines, v.BinaryVersion(), v.BinaryMinSupportedVersion(),
			)
			if err != nil {
				log.Fatalf(ctx, "%v", err)
			}
			if !prevCV.Version.Less(newCV.Version) {
				// If nothing needs to be updated, don't do anything. The
				// callbacks fire async (or at least we want to assume the worst
				// case in which they do) and so an old update might happen
				// after a new one.
				return
			}
			if err := kvserver.WriteClusterVersionToEngines(ctx, s.engines, newCV); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
			log.Infof(ctx, "active cluster version is now %s (up from %s)", newCV, prevCV)
		}
		clusterversion.SetBeforeChange(ctx, &s.cfg.Settings.SV, cb)

		diskClusterVersion := initServer.DiskClusterVersion()
		// The version setting loaded from disk is the maximum cluster version
		// seen on any engine. If new stores are being added to the server right
		// now, or if the process crashed earlier half-way through the callback,
		// that version won't be on all engines. For that reason, we backfill
		// once.
		if err := kvserver.WriteClusterVersionToEngines(
			ctx, s.engines, diskClusterVersion,
		); err != nil {
			return err
		}

		// NB: if we bootstrap a new server (in initServer.ServeAndWait below)
		// we will call Initialize a second time, to eagerly move it to the
		// bootstrap version (from the min supported version). Initialize()
		// tolerates that. Note that in that case we know that the callback
		// has not fired yet, since Gossip won't connect (to itself) until
		// the server starts and so the callback will never fire prior to
		// that second Initialize() call. Note also that at this point in
		// the code we don't know if we'll bootstrap or join an existing
		// cluster, so we have to conservatively go with the version from
		// disk, which in the case of no initialized engines is the binary
		// min supported version.
		if err := clusterversion.Initialize(ctx, diskClusterVersion.Version, &s.cfg.Settings.SV); err != nil {
			return err
		}

		// At this point, we've established the invariant: all engines hold the
		// version currently visible to the setting. And we have the callback in
		// place that will persist an incoming updated version on all engines
		// before making it visible to the setting.
	}

	serverpb.RegisterInitServer(s.grpc.Server, initServer)

	s.node.startAssertEngineHealth(ctx, s.engines)

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

	// loopback handles the HTTP <-> RPC loopback connection.
	loopback := newLoopbackListener(workersCtx, s.stopper)

	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		_ = loopback.Close()
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(loopback))
	})

	// Eschew `(*rpc.Context).GRPCDial` to avoid unnecessary moving parts on the
	// uniquely in-process connection.
	dialOpts, err := s.rpcContext.GRPCDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.DialContext(ctx, s.cfg.AdvertiseAddr, append(
		dialOpts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return loopback.Connect(ctx)
		}),
	)...)
	if err != nil {
		return err
	}
	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		if err := conn.Close(); err != nil {
			log.Fatalf(workersCtx, "%v", err)
		}
	})

	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, s.tsServer} {
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

		for name, val := range listenerFiles {
			file := filepath.Join(storeSpec.Path, name)
			if err := ioutil.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
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

	// Set up calling s.cfg.ReadyFn at the right time. Essentially, this call
	// determines when `./cockroach [...] --background` returns. For any initialized
	// nodes (i.e. already part of a cluster) this is when this method returns
	// (assuming there's no error). For nodes that need to join a cluster, we
	// return once the initServer is ready to accept requests.
	var onSuccessfulReturnFn, onInitServerReady func()
	selfBootstrap := initServer.NeedsInit() && len(s.cfg.GossipBootstrapResolvers) == 0
	if selfBootstrap {
		// If a new node is started without join flags, self-bootstrap.
		//
		// Note: this is behavior slated for removal, see:
		// https://github.com/cockroachdb/cockroach/pull/44112
		_, err := initServer.Bootstrap(ctx, &serverpb.BootstrapRequest{})
		switch {
		case err == nil:
			log.Infof(ctx, "**** add additional nodes by specifying --join=%s", s.cfg.AdvertiseAddr)
		case errors.Is(err, ErrClusterInitialized):
		default:
			// Process is shutting down.
		}
	}
	{
		readyFn := func(bool) {}
		if s.cfg.ReadyFn != nil {
			readyFn = s.cfg.ReadyFn
		}
		if !initServer.NeedsInit() || selfBootstrap {
			onSuccessfulReturnFn = func() { readyFn(false /* waitForInit */) }
			onInitServerReady = func() {}
		} else {
			onSuccessfulReturnFn = func() {}
			onInitServerReady = func() { readyFn(true /* waitForInit */) }
		}
	}

	// This opens the main listener. When the listener is open, we can call
	// initServerReadyFn since any request initated to the initServer at that
	// point will reach it once ServeAndWait starts handling the queue of incoming
	// connections.
	startRPCServer(workersCtx)
	onInitServerReady()
	state, err := initServer.ServeAndWait(ctx, s.stopper, &s.cfg.Settings.SV, s.gossip)
	if err != nil {
		return errors.Wrap(err, "during init")
	}

	s.rpcContext.ClusterID.Set(ctx, state.clusterID)
	// If there's no NodeID here, then we didn't just bootstrap. The Node will
	// read its ID from the stores or request a new one via KV.
	if state.nodeID != 0 {
		s.rpcContext.NodeID.Set(ctx, state.nodeID)
	}

	// TODO(tbg): split this method here. Everything above this comment is
	// the early stage of startup -- setting up listeners and determining the
	// initState -- and everything after it is actually starting the server,
	// using the listeners and init state.

	// Defense in depth: set up an eager sanity check that we're not
	// accidentally being pointed at a different cluster. We have checks for
	// this in the RPC layer, but since the RPC layer gets set up before the
	// clusterID is known, early connections won't validate the clusterID (at
	// least not until the next Ping).
	//
	// The check is simple: listen for clusterID changes from Gossip. If we see
	// one, make sure it's the clusterID we already know (and are guaranteed to
	// know) at this point. If it's not the same, explode.
	//
	// TODO(tbg): remove this when we have changed ServeAndWait() to join an
	// existing cluster via a one-off RPC, at which point we can create gossip
	// (and thus the RPC layer) only after the clusterID is already known. We
	// can then rely on the RPC layer's protection against cross-cluster
	// communication.
	{
		// We populated this above, so it should still be set. This is just to
		// demonstrate that we're not doing anything functional here (and to
		// prevent bugs during further refactors).
		if s.rpcContext.ClusterID.Get() == uuid.Nil {
			return errors.New("gossip should already be connected")
		}
		unregister := s.gossip.RegisterCallback(gossip.KeyClusterID, func(string, roachpb.Value) {
			clusterID, err := s.gossip.GetClusterID()
			if err != nil {
				log.Fatalf(ctx, "unable to read ClusterID: %v", err)
			}
			s.rpcContext.ClusterID.Set(ctx, clusterID) // fatals on mismatch
		})
		defer unregister()
	}

	// Spawn a goroutine that will print a nice message when Gossip connects.
	// Note that we already know the clusterID, but we don't know that Gossip
	// has connected. The pertinent case is that of restarting an entire
	// cluster. Someone has to gossip the ClusterID before Gossip is connected,
	// but this gossip only happens once the first range has a leaseholder, i.e.
	// when a quorum of nodes has gone fully operational.
	_ = s.stopper.RunAsyncTask(ctx, "connect-gossip", func(ctx context.Context) {
		log.Infof(ctx, "connecting to gossip network to verify cluster ID %q", state.clusterID)
		select {
		case <-s.gossip.Connected:
			log.Infof(ctx, "node connected via gossip")
		case <-ctx.Done():
		case <-s.stopper.ShouldQuiesce():
		}
	})

	// NB: if this store is freshly bootstrapped (or no upper bound was
	// persisted), hlcUpperBound will be zero.
	hlcUpperBound, err := kvserver.ReadMaxHLCUpperBound(ctx, s.engines)
	if err != nil {
		return errors.Wrap(err, "reading max HLC upper bound")
	}

	if hlcUpperBound > 0 {
		ensureClockMonotonicity(
			ctx,
			s.clock,
			s.startTime,
			hlcUpperBound,
			timeutil.SleepUntil,
		)
	}

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	//
	// TODO(tbg): clarify the contract here and move closer to usage if possible.
	orphanedLeasesTimeThresholdNanos := s.clock.Now().WallTime

	onSuccessfulReturnFn()

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been bootstrapped or
	// we're joining an existing cluster for the first time.
	if err := s.node.start(
		ctx,
		advAddrU, advSQLAddrU,
		*state,
		s.cfg.ClusterName,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		s.cfg.LocalityAddresses,
		s.sqlServer.execCfg.DistSQLPlanner.SetNodeInfo,
	); err != nil {
		return err
	}

	log.Event(ctx, "started node")
	if err := s.startPersistingHLCUpperBound(
		ctx,
		hlcUpperBound > 0,
		func(t int64) error { /* function to persist upper bound of HLC to all stores */
			return s.node.SetHLCUpperBound(context.Background(), t)
		},
		time.NewTicker,
	); err != nil {
		return err
	}
	s.replicationReporter.Start(ctx, s.stopper)

	s.refreshSettings()

	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cluster":     s.ClusterID().String(),
			"node":        s.NodeID().String(),
			"server_id":   fmt.Sprintf("%s-%s", s.ClusterID().Short(), s.NodeID()),
			"engine_type": s.cfg.StorageEngine.String(),
		})
	})

	// We can now add the node registry.
	s.recorder.AddNode(s.registry, s.node.Descriptor, s.node.startedAt, s.cfg.AdvertiseAddr, s.cfg.HTTPAdvertiseAddr, s.cfg.SQLAdvertiseAddr)

	// Begin recording runtime statistics.
	if err := s.startSampleEnvironment(ctx, base.DefaultMetricsSampleInterval); err != nil {
		return err
	}

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, base.DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	var graphiteOnce sync.Once
	graphiteEndpoint.SetOnChange(&s.st.SV, func() {
		if graphiteEndpoint.Get(&s.st.SV) != "" {
			graphiteOnce.Do(func() {
				s.node.startGraphiteStatsExporter(s.st)
			})
		}
	})

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

	if state.bootstrapped {
		// If a new cluster is just starting up, force all the system ranges
		// through the replication queue so they upreplicate as quickly as
		// possible when a new node joins. Without this code, the upreplication
		// would be up to the whim of the scanner, which might be too slow for
		// new clusters.
		// TODO(tbg): instead of this dubious band-aid we should make the
		// replication queue reactive enough to avoid relying on the scanner
		// alone.
		var done bool
		return s.node.stores.VisitStores(func(store *kvserver.Store) error {
			if !done {
				done = true
				return store.ForceReplicationScanAndProcess()
			}
			return nil
		})
	}

	// Begin the node liveness heartbeat. Add a callback which records the local
	// store "last up" timestamp for every store whenever the liveness record is
	// updated.
	s.nodeLiveness.StartHeartbeat(ctx, s.stopper, s.engines, func(ctx context.Context) {
		now := s.clock.Now()
		if err := s.node.stores.VisitStores(func(s *kvserver.Store) error {
			return s.WriteLastUpTimestamp(ctx, now)
		}); err != nil {
			log.Warningf(ctx, "writing last up timestamp: %v", err)
		}
	})

	// Begin recording status summaries.
	s.node.startWriteNodeStatus(base.DefaultMetricsSampleInterval)

	// Start the protected timestamp subsystem.
	if err := s.protectedtsProvider.Start(ctx, s.stopper); err != nil {
		return err
	}
	if err := s.protectedtsReconciler.Start(ctx, s.stopper); err != nil {
		return err
	}

	// Start garbage collecting system events.
	//
	// NB: As written, this falls awkwardly between SQL and KV. KV is used only
	// to make sure this runs only on one node. SQL is used to actually GC. We
	// count it as a KV operation since it grooms cluster-wide data, not
	// something associated to SQL tenants.
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
			NodeID:               s.nodeIDContainer,
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

	// Register debugging endpoints.
	var debugHandler http.Handler = s.debug
	if s.cfg.RequireWebSession() {
		// TODO(bdarnell): Refactor our authentication stack.
		// authenticationMux guarantees that we have a non-empty user
		// session, but our machinery for verifying the roles of a user
		// lives on adminServer and is tied to GRPC metadata.
		debugHandler = newAuthenticationMux(s.authentication, http.HandlerFunc(
			func(w http.ResponseWriter, req *http.Request) {
				md := forwardAuthenticationMetadata(req.Context(), req)
				authCtx := metadata.NewIncomingContext(req.Context(), md)
				_, err := s.admin.requireAdminUser(authCtx)
				if errors.Is(err, errInsufficientPrivilege) {
					http.Error(w, "admin privilege required", http.StatusUnauthorized)
					return
				} else if err != nil {
					log.Infof(authCtx, "web session error: %s", err)
					http.Error(w, "error checking authentication", http.StatusInternalServerError)
					return
				}
				s.debug.ServeHTTP(w, req)
			}))
	}
	s.mux.Handle(debug.Endpoint, debugHandler)

	log.Event(ctx, "added http endpoints")

	// Attempt to upgrade cluster version.
	s.startAttemptUpgrade(ctx)

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

	if err := s.sqlServer.start(
		workersCtx,
		s.stopper,
		s.cfg.TestingKnobs,
		connManager,
		pgL,
		s.cfg.SocketFile,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return err
	}

	if err := s.debug.RegisterEngines(s.cfg.Stores.Specs, s.engines); err != nil {
		return errors.Wrapf(err, "failed to register engines with debug server")
	}

	// Start scheduled jobs daemon.
	jobs.StartJobSchedulerDaemon(
		ctx, s.stopper, &s.st.SV, jobs.ProdJobSchedulerEnv, s.db, s.sqlServer.internalExecutor)

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
				log.Fatalf(workersCtx, "%v", err)
			}
		})
		log.Eventf(ctx, "listening on sql port %s", s.cfg.SQLAddr)
	}

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

	// startRPCServer starts the RPC server. We do not do this
	// immediately because we want the cluster to be ready (or ready to
	// initialize) before we accept RPC requests. The caller
	// (Server.Start) will call this at the right moment.
	startRPCServer = func(ctx context.Context) {
		// Serve the gRPC endpoint.
		s.stopper.RunWorker(workersCtx, func(context.Context) {
			netutil.FatalIfUnexpected(s.grpc.Serve(anyL))
		})

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
			log.Fatalf(workersCtx, "%v", err)
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
	// listening on --http-addr without TLS if uiTLSConfig was
	// nil, or overridden above if uiTLSConfig was not nil to come from
	// the TLS negotiation over the HTTP port.
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})

	return nil
}

// TODO(tbg): move into server_sql.go.
func (s *sqlServer) startServeSQL(
	ctx context.Context,
	stopper *stop.Stopper,
	connManager netutil.Server,
	pgL net.Listener,
	socketFile string,
) error {
	log.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
	tcpKeepAlive := tcpKeepAliveManager{
		tcpKeepAlive: envutil.EnvOrDefaultDuration("COCKROACH_SQL_TCP_KEEP_ALIVE", time.Minute),
	}

	stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
		netutil.FatalIfUnexpected(connManager.ServeWith(pgCtx, stopper, pgL, func(conn net.Conn) {
			connCtx := logtags.AddTag(pgCtx, "client", conn.RemoteAddr().String())
			tcpKeepAlive.configure(connCtx, conn)

			if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketTCP); err != nil {
				log.Errorf(connCtx, "serving SQL client conn: %v", err)
			}
		}))
	})

	// If a unix socket was requested, start serving there too.
	if len(socketFile) != 0 {
		log.Infof(ctx, "starting postgres server at unix:%s", socketFile)

		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", socketFile)
		if err != nil {
			return err
		}

		stopper.RunWorker(ctx, func(workersCtx context.Context) {
			<-stopper.ShouldQuiesce()
			if err := unixLn.Close(); err != nil {
				log.Fatalf(workersCtx, "%v", err)
			}
		})

		stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
			netutil.FatalIfUnexpected(connManager.ServeWith(pgCtx, stopper, unixLn, func(conn net.Conn) {
				connCtx := logtags.AddTag(pgCtx, "client", conn.RemoteAddr().String())
				if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketUnix); err != nil {
					log.Errorf(connCtx, "%v", err)
				}
			}))
		})
	}
	return nil
}

// Decommission idempotently sets the decommissioning flag for specified nodes.
func (s *Server) Decommission(ctx context.Context, setTo bool, nodeIDs []roachpb.NodeID) error {
	eventLogger := sql.MakeEventLogger(s.sqlServer.execCfg)
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
func (s *Server) startSampleEnvironment(ctx context.Context, frequency time.Duration) error {
	// Immediately record summaries once on server startup.
	ctx = s.AnnotateCtx(ctx)

	// We're not going to take heap profiles or goroutine dumps if
	// running only with in-memory stores.  This helps some tests that
	// can't write any files.
	allStoresInMem := true
	for _, storeSpec := range s.cfg.Stores.Specs {
		if !storeSpec.InMemory {
			allStoresInMem = false
			break
		}
	}

	var goroutineDumper *goroutinedumper.GoroutineDumper
	var heapProfiler *heapprofiler.HeapProfiler

	if !allStoresInMem {
		var err error
		if s.cfg.GoroutineDumpDirName != "" {
			if err := os.MkdirAll(s.cfg.GoroutineDumpDirName, 0755); err != nil {
				return errors.Wrap(err, "creating goroutine dump dir")
			}
			goroutineDumper, err = goroutinedumper.NewGoroutineDumper(s.cfg.GoroutineDumpDirName)
			if err != nil {
				return errors.Wrap(err, "starting goroutine dumper worker")
			}
		}

		if s.cfg.HeapProfileDirName != "" {
			if err := os.MkdirAll(s.cfg.HeapProfileDirName, 0755); err != nil {
				return errors.Wrap(err, "creating heap profiles dir")
			}
			heapProfiler, err = heapprofiler.NewHeapProfiler(s.cfg.HeapProfileDirName, s.ClusterSettings())
			if err != nil {
				return errors.Wrap(err, "starting heap profiler worker")
			}
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
	return nil
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop(context.Background())
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
			if err := gzw.Close(); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
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
	return s.sqlServer.pgServer
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
		return nil, &ListenError{
			cause: err,
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
	return fn(ctx, s.sqlServer.internalExecutor)
}
