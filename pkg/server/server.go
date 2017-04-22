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
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/elazarl/go-bindata-assetfs"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migrations"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// Allocation pool for gzip writers.
	gzipWriterPool sync.Pool

	// GracefulDrainModes is the standard succession of drain modes entered
	// for a graceful shutdown.
	GracefulDrainModes = []serverpb.DrainMode{serverpb.DrainMode_CLIENT, serverpb.DrainMode_LEASES}
)

// Server is the cockroach server node.
type Server struct {
	nodeIDContainer base.NodeIDContainer

	cfg                Config
	mux                *http.ServeMux
	clock              *hlc.Clock
	rpcContext         *rpc.Context
	grpc               *grpc.Server
	gossip             *gossip.Gossip
	nodeLiveness       *storage.NodeLiveness
	storePool          *storage.StorePool
	txnCoordSender     *kv.TxnCoordSender
	distSender         *kv.DistSender
	db                 *client.DB
	kvDB               *kv.DBServer
	pgServer           *pgwire.Server
	distSQLServer      *distsqlrun.ServerImpl
	node               *Node
	registry           *metric.Registry
	recorder           *status.MetricsRecorder
	runtime            status.RuntimeStatSampler
	admin              *adminServer
	status             *statusServer
	tsDB               *ts.DB
	tsServer           ts.Server
	raftTransport      *storage.RaftTransport
	stopper            *stop.Stopper
	sqlExecutor        *sql.Executor
	leaseMgr           *sql.LeaseManager
	engines            Engines
	internalMemMetrics sql.MemoryMetrics
	adminMemMetrics    sql.MemoryMetrics
}

// NewServer creates a Server from a server.Context.
func NewServer(cfg Config, stopper *stop.Stopper) (*Server, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", cfg.AdvertiseAddr, err)
	}

	if cfg.AmbientCtx.Tracer == nil {
		cfg.AmbientCtx.Tracer = tracing.NewTracer()
	}

	// Attempt to load TLS configs right away, failures are permanent.
	if err := cfg.InitializeNodeTLSConfigs(stopper); err != nil {
		return nil, err
	}

	s := &Server{
		mux:     http.NewServeMux(),
		clock:   hlc.NewClock(hlc.UnixNano, cfg.MaxOffset),
		stopper: stopper,
		cfg:     cfg,
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
	if s.cfg.Insecure {
		log.Shout(ctx, log.Severity_WARNING,
			"running in insecure mode, this is strongly discouraged. See --insecure.")
	}

	s.rpcContext = rpc.NewContext(s.cfg.AmbientCtx, s.cfg.Config, s.clock, s.stopper)
	s.rpcContext.HeartbeatCB = func() {
		if err := s.rpcContext.RemoteClocks.VerifyClockOffset(ctx); err != nil {
			log.Fatal(ctx, err)
		}
	}
	s.grpc = rpc.NewServer(s.rpcContext)

	s.registry = metric.NewRegistry()
	s.gossip = gossip.New(
		s.cfg.AmbientCtx,
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
		Clock:           s.clock,
		RPCContext:      s.rpcContext,
		RPCRetryOptions: &retryOpts,
		SendNextTimeout: s.cfg.SendNextTimeout,
	}
	s.distSender = kv.NewDistSender(distSenderCfg, s.gossip)
	s.registry.AddMetricStruct(s.distSender.Metrics())

	txnMetrics := kv.MakeTxnMetrics(s.cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(txnMetrics)
	s.txnCoordSender = kv.NewTxnCoordSender(
		s.cfg.AmbientCtx,
		s.distSender,
		s.clock,
		s.cfg.Linearizable,
		s.stopper,
		txnMetrics,
	)
	s.db = client.NewDB(s.txnCoordSender, s.clock)

	active, renewal := storage.NodeLivenessDurations(
		storage.RaftElectionTimeout(s.cfg.RaftTickInterval, s.cfg.RaftElectionTimeoutTicks))
	s.nodeLiveness = storage.NewNodeLiveness(
		s.cfg.AmbientCtx, s.clock, s.db, s.gossip, active, renewal,
	)
	s.registry.AddMetricStruct(s.nodeLiveness.Metrics())

	s.storePool = storage.NewStorePool(
		s.cfg.AmbientCtx,
		s.gossip,
		s.clock,
		storage.MakeStorePoolNodeLivenessFunc(s.nodeLiveness),
		s.cfg.TimeUntilStoreDead,
		/* deterministic */ false,
	)

	s.raftTransport = storage.NewRaftTransport(
		s.cfg.AmbientCtx, storage.GossipAddressResolver(s.gossip), s.grpc, s.rpcContext,
	)

	s.kvDB = kv.NewDBServer(s.cfg.Config, s.txnCoordSender, s.stopper)
	roachpb.RegisterExternalServer(s.grpc, s.kvDB)

	// Set up internal memory metrics for use by internal SQL executors.
	s.internalMemMetrics = sql.MakeMemMetrics("internal", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.internalMemMetrics)

	// Set up Lease Manager
	var lmKnobs sql.LeaseManagerTestingKnobs
	if cfg.TestingKnobs.SQLLeaseManager != nil {
		lmKnobs = *s.cfg.TestingKnobs.SQLLeaseManager.(*sql.LeaseManagerTestingKnobs)
	}
	s.leaseMgr = sql.NewLeaseManager(&s.nodeIDContainer, *s.db, s.clock, lmKnobs,
		s.stopper, &s.internalMemMetrics)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)

	s.refreshSettings()

	// We do not set memory monitors or a noteworthy limit because the children of
	// this monitor will be setting their own noteworthy limits.
	rootSQLMemoryMonitor := mon.MakeMonitor(
		"root",
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
	)
	rootSQLMemoryMonitor.Start(context.Background(), nil, mon.MakeStandaloneBudget(s.cfg.SQLMemoryPoolSize))

	distSQLMetrics := sql.MakeMemMetrics("distsql", cfg.HistogramWindowInterval())
	s.registry.AddMetric(distSQLMetrics.CurBytesCount)
	s.registry.AddMetric(distSQLMetrics.MaxBytesHist)

	// Set up the DistSQL server
	distSQLCfg := distsqlrun.ServerConfig{
		AmbientContext: s.cfg.AmbientCtx,
		DB:             s.db,
		RPCContext:     s.rpcContext,
		Stopper:        s.stopper,
		NodeID:         &s.nodeIDContainer,

		ParentMemoryMonitor: &rootSQLMemoryMonitor,
		Counter:             distSQLMetrics.CurBytesCount,
		Hist:                distSQLMetrics.MaxBytesHist,
	}
	if s.cfg.TestingKnobs.DistSQL != nil {
		distSQLCfg.TestingKnobs = *s.cfg.TestingKnobs.DistSQL.(*distsqlrun.TestingKnobs)
	}
	s.distSQLServer = distsqlrun.NewServer(ctx, distSQLCfg)
	distsqlrun.RegisterDistSQLServer(s.grpc, s.distSQLServer)

	// Set up admin memory metrics for use by admin SQL executors.
	s.adminMemMetrics = sql.MakeMemMetrics("admin", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.adminMemMetrics)

	// Set up Executor
	execCfg := sql.ExecutorConfig{
		AmbientCtx:              s.cfg.AmbientCtx,
		NodeID:                  &s.nodeIDContainer,
		DB:                      s.db,
		Gossip:                  s.gossip,
		DistSender:              s.distSender,
		RPCContext:              s.rpcContext,
		LeaseManager:            s.leaseMgr,
		Clock:                   s.clock,
		DistSQLSrv:              s.distSQLServer,
		HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
		RangeDescriptorCache:    s.distSender.RangeDescriptorCache(),
		LeaseHolderCache:        s.distSender.LeaseHolderCache(),
	}
	if s.cfg.TestingKnobs.SQLExecutor != nil {
		execCfg.TestingKnobs = s.cfg.TestingKnobs.SQLExecutor.(*sql.ExecutorTestingKnobs)
	} else {
		execCfg.TestingKnobs = &sql.ExecutorTestingKnobs{}
	}
	if s.cfg.TestingKnobs.SQLSchemaChanger != nil {
		execCfg.SchemaChangerTestingKnobs =
			s.cfg.TestingKnobs.SQLSchemaChanger.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = &sql.SchemaChangerTestingKnobs{}
	}
	s.sqlExecutor = sql.NewExecutor(execCfg, s.stopper)
	s.registry.AddMetricStruct(s.sqlExecutor)

	s.pgServer = pgwire.MakeServer(
		s.cfg.AmbientCtx,
		s.cfg.Config,
		s.sqlExecutor,
		&s.internalMemMetrics,
		&rootSQLMemoryMonitor,
		s.cfg.HistogramWindowInterval(),
	)
	s.registry.AddMetricStruct(s.pgServer.Metrics())

	s.tsDB = ts.NewDB(s.db)
	s.tsServer = ts.MakeServer(s.cfg.AmbientCtx, s.tsDB, s.cfg.TimeSeriesServerConfig, s.stopper)

	// TODO(bdarnell): make StoreConfig configurable.
	storeCfg := storage.StoreConfig{
		AmbientCtx:                     s.cfg.AmbientCtx,
		Clock:                          s.clock,
		DB:                             s.db,
		Gossip:                         s.gossip,
		NodeLiveness:                   s.nodeLiveness,
		Transport:                      s.raftTransport,
		RPCContext:                     s.rpcContext,
		RangeRetryOptions:              s.cfg.RetryOptions,
		RaftTickInterval:               s.cfg.RaftTickInterval,
		ScanInterval:                   s.cfg.ScanInterval,
		ScanMaxIdleTime:                s.cfg.ScanMaxIdleTime,
		ConsistencyCheckInterval:       s.cfg.ConsistencyCheckInterval,
		ConsistencyCheckPanicOnFailure: s.cfg.ConsistencyCheckPanicOnFailure,
		MetricsSampleInterval:          s.cfg.MetricsSampleInterval,
		HistogramWindowInterval:        s.cfg.HistogramWindowInterval(),
		StorePool:                      s.storePool,
		SQLExecutor: sql.InternalExecutor{
			LeaseManager: s.leaseMgr,
		},
		LogRangeEvents:            s.cfg.EventLogEnabled,
		RangeLeaseActiveDuration:  active,
		RangeLeaseRenewalDuration: renewal,
		TimeSeriesDataStore:       s.tsDB,

		EnableEpochRangeLeases: true,
	}
	if s.cfg.TestingKnobs.Store != nil {
		storeCfg.TestingKnobs = *s.cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs)
	}

	s.recorder = status.NewMetricsRecorder(s.clock)
	s.registry.AddMetricStruct(s.rpcContext.RemoteClocks.Metrics())

	s.runtime = status.MakeRuntimeStatSampler(s.clock)
	s.registry.AddMetricStruct(s.runtime)

	s.node = NewNode(storeCfg, s.recorder, s.registry, s.stopper, txnMetrics, sql.MakeEventLogger(s.leaseMgr))
	roachpb.RegisterInternalServer(s.grpc, s.node)
	storage.RegisterConsistencyServer(s.grpc, s.node.storesServer)

	s.admin = newAdminServer(s)
	s.status = newStatusServer(
		s.cfg.AmbientCtx,
		s.db,
		s.gossip,
		s.recorder,
		s.nodeLiveness,
		s.rpcContext,
		s.node.stores,
		s.stopper,
	)
	for _, gw := range []grpcGatewayServer{s.admin, s.status, &s.tsServer} {
		gw.RegisterService(s.grpc)
	}

	return s, nil
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
	return s.node.ClusterID
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

// ClusterStores returns a list of known stores in the cluster.
func (s *Server) ClusterStores() []roachpb.ReplicationTarget {
	return s.storePool.GetStores()
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

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *Server) Start(ctx context.Context) error {
	ctx = s.AnnotateCtx(ctx)

	startTime := timeutil.Now()

	tlsConfig, err := s.cfg.GetServerTLSConfig()
	if err != nil {
		return err
	}

	httpServer := netutil.MakeServer(s.stopper, tlsConfig, s)
	plainRedirectServer := netutil.MakeServer(s.stopper, tlsConfig, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusPermanentRedirect)
	}))

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
		return err
	}
	log.Eventf(ctx, "listening on port %s", s.cfg.Addr)
	unresolvedListenAddr, err := officialAddr(s.cfg.Addr, ln.Addr())
	if err != nil {
		return err
	}
	s.cfg.Addr = unresolvedListenAddr.String()
	unresolvedAdvertAddr, err := officialAddr(s.cfg.AdvertiseAddr, ln.Addr())
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
	pgL := m.Match(pgwire.Match)
	anyL := m.Match(cmux.Any())

	httpLn, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return err
	}
	unresolvedHTTPAddr, err := officialAddr(s.cfg.HTTPAddr, httpLn.Addr())
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
			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		})

		httpLn = tls.NewListener(tlsL, tlsConfig)
	}

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(httpServer.Serve(httpLn))
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		<-s.stopper.ShouldQuiesce()
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

			if err := s.pgServer.ServeConn(connCtx, conn); err != nil && !netutil.IsClosedConnection(err) {
				// Report the error on this connection's context, so that we
				// know which remote client caused the error when looking at
				// the logs.
				log.Error(connCtx, err)
			}
		}))
	})

	// Enable the debug endpoints first to provide an earlier window
	// into what's going on with the node in advance of exporting node
	// functionality.
	// TODO(marc): when cookie-based authentication exists,
	// apply it for all web endpoints.
	s.mux.HandleFunc(debugEndpoint, http.HandlerFunc(handleDebug))

	// Filter the gossip bootstrap resolvers based on the listen and
	// advertise addresses.
	filtered := s.cfg.FilterGossipBootstrapResolvers(ctx, unresolvedListenAddr, unresolvedAdvertAddr)
	s.gossip.Start(unresolvedAdvertAddr, filtered)
	log.Event(ctx, "started gossip")

	s.engines, err = s.cfg.CreateEngines()
	if err != nil {
		return errors.Wrap(err, "failed to create engines")
	}
	s.stopper.AddCloser(&s.engines)

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
	{
		anyStoreBootstrapped := false
		for _, e := range s.engines {
			if _, err := storage.ReadStoreIdent(ctx, e); err != nil {
				// NotBootstrappedError is expected.
				if _, ok := err.(*storage.NotBootstrappedError); !ok {
					return err
				}
			} else {
				anyStoreBootstrapped = true
				break
			}
		}
		if anyStoreBootstrapped {
			sleepDuration := s.clock.MaxOffset() - timeutil.Since(startTime)
			if sleepDuration > 0 {
				log.Infof(ctx, "sleeping for %s to guarantee HLC monotonicity", sleepDuration)
				time.Sleep(sleepDuration)
			}
		}
	}

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas.
	err = s.node.start(
		ctx,
		unresolvedAdvertAddr,
		s.engines,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		// If the _unfiltered_ list of hosts from the --join flag is
		// empty, then this node can bootstrap a new cluster. We disallow
		// this if this node is being started with itself specified as a
		// --join host, because that's too likely to be operator error.
		len(s.cfg.GossipBootstrapResolvers) == 0, /* canBootstrap */
	)
	if err != nil {
		return err
	}
	log.Event(ctx, "started node")

	// We can now add the node registry.
	s.recorder.AddNode(s.registry, s.node.Descriptor, s.node.startedAt, s.cfg.AdvertiseAddr, s.cfg.HTTPAddr)

	// Begin recording runtime statistics.
	s.startSampleEnvironment(s.cfg.MetricsSampleInterval)

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, s.cfg.MetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	// Begin recording status summaries.
	s.node.startWriteSummaries(s.cfg.MetricsSampleInterval)

	// Create and start the schema change manager only after a NodeID
	// has been assigned.
	testingKnobs := &sql.SchemaChangerTestingKnobs{}
	if s.cfg.TestingKnobs.SQLSchemaChanger != nil {
		testingKnobs = s.cfg.TestingKnobs.SQLSchemaChanger.(*sql.SchemaChangerTestingKnobs)
	}
	sql.NewSchemaChangeManager(
		testingKnobs,
		*s.db,
		s.node.Descriptor,
		s.rpcContext,
		s.distSQLServer,
		s.distSender,
		s.gossip,
		s.leaseMgr,
	).Start(s.stopper)

	s.sqlExecutor.Start(ctx, &s.adminMemMetrics, s.node.Descriptor)
	s.distSQLServer.Start()

	log.Infof(ctx, "starting %s server at %s", s.cfg.HTTPRequestScheme(), unresolvedHTTPAddr)
	log.Infof(ctx, "starting grpc/postgres server at %s", unresolvedListenAddr)
	log.Infof(ctx, "advertising CockroachDB node at %s", unresolvedAdvertAddr)
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		serveOnMux.Do(func() {
			netutil.FatalIfUnexpected(m.Serve())
		})
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

		pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
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

	log.Event(ctx, "accepting connections")

	// Begin the node liveness heartbeat. Add a callback which records the local
	// store "last up" timestamp for every store whenever the liveness record
	// is updated.
	s.nodeLiveness.StartHeartbeat(ctx, s.stopper, func(ctx context.Context) error {
		now := s.clock.Now()
		return s.node.stores.VisitStores(func(s *storage.Store) error {
			return s.WriteLastUpTimestamp(ctx, now)
		})
	})

	// Initialize grpc-gateway mux and context.
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
	)
	gwCtx, gwCancel := context.WithCancel(s.AnnotateCtx(context.Background()))
	s.stopper.AddCloser(stop.CloserFn(gwCancel))

	// Setup HTTP<->gRPC handlers.
	conn, err := s.rpcContext.GRPCDial(s.cfg.Addr)
	if err != nil {
		return errors.Errorf("error constructing grpc-gateway: %s; are your certificates valid?", err)
	}

	for _, gw := range []grpcGatewayServer{s.admin, s.status, &s.tsServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return err
		}
	}

	s.mux.Handle("/", http.FileServer(&assetfs.AssetFS{
		Asset:     ui.Asset,
		AssetDir:  ui.AssetDir,
		AssetInfo: ui.AssetInfo,
	}))

	// TODO(marc): when cookie-based authentication exists,
	// apply it for all web endpoints.
	s.mux.Handle(adminPrefix, gwMux)
	s.mux.Handle(ts.URLPrefix, gwMux)
	s.mux.Handle(statusPrefix, gwMux)
	s.mux.Handle("/health", gwMux)
	s.mux.Handle(statusVars, http.HandlerFunc(s.status.handleVars))
	s.mux.Handle(rangeDebugEndpoint, http.HandlerFunc(s.status.handleDebugRange))
	s.mux.Handle(problemRangesDebugEndpoint, http.HandlerFunc(s.status.handleProblemRanges))
	log.Event(ctx, "added http endpoints")

	// Before serving SQL requests, we have to make sure the database is
	// in an acceptable form for this version of the software.
	// We have to do this after actually starting up the server to be able to
	// seamlessly use the kv client against other nodes in the cluster.
	migMgr := migrations.NewManager(
		s.stopper, s.db, s.sqlExecutor, s.clock, &s.internalMemMetrics, s.NodeID().String())
	if err := migMgr.EnsureMigrations(ctx); err != nil {
		log.Fatal(ctx, err)
	}
	log.Infof(ctx, "done ensuring all necessary migrations have run")
	close(serveSQL)
	log.Info(ctx, "serving sql connections")

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent()

	if s.cfg.PIDFile != "" {
		if err := ioutil.WriteFile(s.cfg.PIDFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
			log.Error(ctx, err)
		}
	}
	if err := sdnotify.Ready(); err != nil {
		log.Errorf(ctx, "failed to signal readiness using systemd protocol: %s", err)
	}
	log.Event(ctx, "server ready")

	return nil
}

func (s *Server) doDrain(modes []serverpb.DrainMode, setTo bool) ([]serverpb.DrainMode, error) {
	for _, mode := range modes {
		switch mode {
		case serverpb.DrainMode_CLIENT:
			if err := func() error {
				// Since enabling the lease manager's draining mode will prevent
				// the acquisition of new leases, the switch must be made after
				// the pgServer has given sessions a chance to finish ongoing
				// work.
				defer s.leaseMgr.SetDraining(setTo)
				return s.pgServer.SetDraining(setTo)
			}(); err != nil {
				return nil, err
			}
		case serverpb.DrainMode_LEASES:
			s.nodeLiveness.SetDraining(context.TODO(), setTo)
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
func (s *Server) Drain(on []serverpb.DrainMode) ([]serverpb.DrainMode, error) {
	return s.doDrain(on, true)
}

// Undrain idempotently deactivates the given DrainModes on the Server in the
// order in which they are supplied.
// On success, returns any remaining active drain modes.
func (s *Server) Undrain(off []serverpb.DrainMode) []serverpb.DrainMode {
	nowActive, err := s.doDrain(off, false)
	if err != nil {
		panic(fmt.Sprintf("error returned to Undrain: %s", err))
	}
	return nowActive
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
		defer gzw.Close()
		w = gzw
	}
	s.mux.ServeHTTP(w, r)
}

type gzipResponseWriter struct {
	io.WriteCloser
	http.ResponseWriter
}

// Flush implements http.Flusher as required by grpc-gateway for clients
// which access streaming endpoints (as exercised by the acceptance tests
// at time of writing).
func (*gzipResponseWriter) Flush() {}

func newGzipResponseWriter(w http.ResponseWriter) *gzipResponseWriter {
	var gz *gzip.Writer
	if gzI := gzipWriterPool.Get(); gzI == nil {
		gz = gzip.NewWriter(w)
	} else {
		gz = gzI.(*gzip.Writer)
		gz.Reset(w)
	}
	return &gzipResponseWriter{WriteCloser: gz, ResponseWriter: w}
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.WriteCloser.Write(b)
}

func (w *gzipResponseWriter) Close() {
	if w.WriteCloser != nil {
		w.WriteCloser.Close()
		gzipWriterPool.Put(w.WriteCloser)
		w.WriteCloser = nil
	}
}

func officialAddr(unresolvedAddr string, resolvedAddr net.Addr) (*util.UnresolvedAddr, error) {
	unresolvedHost, unresolvedPort, err := net.SplitHostPort(unresolvedAddr)
	if err != nil {
		return nil, err
	}

	resolvedHost, resolvedPort, err := net.SplitHostPort(resolvedAddr.String())
	if err != nil {
		return nil, err
	}

	var host string
	if unresolvedHost != "" {
		// A host was provided, use it.
		host = unresolvedHost
	} else {
		// A host was not provided. Ask the system, and fall back to the listener.
		if hostname, err := os.Hostname(); err == nil {
			host = hostname
		} else {
			host = resolvedHost
		}
	}

	var port string
	if unresolvedPort != "0" {
		// A port was provided, use it.
		port = unresolvedPort
	} else {
		// A port was not provided, but the system assigned one.
		port = resolvedPort
	}

	return util.NewUnresolvedAddr(resolvedAddr.Network(), net.JoinHostPort(host, port)), nil
}
