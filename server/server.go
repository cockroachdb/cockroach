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
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/elazarl/go-bindata-assetfs"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/distsql"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/ui"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/netutil"
	"github.com/cockroachdb/cockroach/util/sdnotify"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
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
	Tracer        opentracing.Tracer
	ctx           Context
	mux           *http.ServeMux
	clock         *hlc.Clock
	rpcContext    *rpc.Context
	grpc          *grpc.Server
	gossip        *gossip.Gossip
	storePool     *storage.StorePool
	distSender    *kv.DistSender
	db            *client.DB
	kvDB          *kv.DBServer
	pgServer      *pgwire.Server
	distSQLServer *distsql.ServerImpl
	node          *Node
	recorder      *status.MetricsRecorder
	runtime       status.RuntimeStatSampler
	admin         adminServer
	status        *statusServer
	tsDB          *ts.DB
	tsServer      ts.Server
	raftTransport *storage.RaftTransport
	stopper       *stop.Stopper
	sqlExecutor   *sql.Executor
	leaseMgr      *sql.LeaseManager
}

// NewServer creates a Server from a server.Context.
func NewServer(ctx Context, stopper *stop.Stopper) (*Server, error) {
	if _, err := net.ResolveTCPAddr("tcp", ctx.Addr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", ctx.Addr, err)
	}

	if ctx.Insecure {
		log.Warning(context.TODO(), "running in insecure mode, this is strongly discouraged. See --insecure.")
	}
	// Try loading the TLS configs before anything else.
	if _, err := ctx.GetServerTLSConfig(); err != nil {
		return nil, err
	}
	if _, err := ctx.GetClientTLSConfig(); err != nil {
		return nil, err
	}

	s := &Server{
		Tracer:  tracing.NewTracer(),
		ctx:     ctx,
		mux:     http.NewServeMux(),
		clock:   hlc.NewClock(hlc.UnixNano),
		stopper: stopper,
	}
	s.clock.SetMaxOffset(ctx.MaxOffset)

	s.rpcContext = rpc.NewContext(ctx.Context, s.clock, s.stopper)
	s.rpcContext.HeartbeatCB = func() {
		if err := s.rpcContext.RemoteClocks.VerifyClockOffset(); err != nil {
			log.Fatal(context.TODO(), err)
		}
	}
	s.grpc = rpc.NewServer(s.rpcContext)

	gossipRegistry := metric.NewRegistry()
	s.gossip = gossip.New(s.rpcContext, s.grpc, s.ctx.GossipBootstrapResolvers, s.stopper, gossipRegistry)
	s.storePool = storage.NewStorePool(
		s.gossip,
		s.clock,
		s.rpcContext,
		ctx.ReservationsEnabled,
		ctx.TimeUntilStoreDead,
		s.stopper,
	)

	// A custom RetryOptions is created which uses stopper.ShouldQuiesce() as
	// the Closer. This prevents infinite retry loops from occurring during
	// graceful server shutdown
	//
	// Such a loop loop occurs with the DistSender attempts a connection to the
	// local server during shutdown, and receives an internal server error (HTTP
	// Code 5xx). This is the correct error for a server to return when it is
	// shutting down, and is normally retryable in a cluster environment.
	// However, on a single-node setup (such as a test), retries will never
	// succeed because the only server has been shut down; thus, thus the
	// DistSender needs to know that it should not retry in this situation.
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = s.stopper.ShouldQuiesce()
	s.distSender = kv.NewDistSender(&kv.DistSenderContext{
		Clock:           s.clock,
		RPCContext:      s.rpcContext,
		RPCRetryOptions: &retryOpts,
	}, s.gossip)
	txnRegistry := metric.NewRegistry()
	txnMetrics := kv.NewTxnMetrics(txnRegistry)
	sender := kv.NewTxnCoordSender(s.distSender, s.clock, ctx.Linearizable, s.Tracer,
		s.stopper, txnMetrics)
	s.db = client.NewDB(sender)

	s.raftTransport = storage.NewRaftTransport(storage.GossipAddressResolver(s.gossip), s.grpc, s.rpcContext)

	s.kvDB = kv.NewDBServer(s.ctx.Context, sender, s.stopper)
	roachpb.RegisterExternalServer(s.grpc, s.kvDB)

	// Set up Lease Manager
	var lmKnobs sql.LeaseManagerTestingKnobs
	if ctx.TestingKnobs.SQLLeaseManager != nil {
		lmKnobs = *ctx.TestingKnobs.SQLLeaseManager.(*sql.LeaseManagerTestingKnobs)
	}
	s.leaseMgr = sql.NewLeaseManager(0, *s.db, s.clock, lmKnobs, s.stopper)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)

	// Set up the DistSQL server
	distSQLCtx := distsql.ServerContext{
		Context:    context.Background(),
		DB:         s.db,
		RPCContext: s.rpcContext,
	}
	s.distSQLServer = distsql.NewServer(distSQLCtx)
	distsql.RegisterDistSQLServer(s.grpc, s.distSQLServer)

	// Set up Executor
	eCtx := sql.ExecutorContext{
		Context:      context.Background(),
		DB:           s.db,
		Gossip:       s.gossip,
		LeaseManager: s.leaseMgr,
		Clock:        s.clock,
		DistSQLSrv:   s.distSQLServer,
	}
	if ctx.TestingKnobs.SQLExecutor != nil {
		eCtx.TestingKnobs = ctx.TestingKnobs.SQLExecutor.(*sql.ExecutorTestingKnobs)
	} else {
		eCtx.TestingKnobs = &sql.ExecutorTestingKnobs{}
	}

	sqlRegistry := metric.NewRegistry()
	s.sqlExecutor = sql.NewExecutor(eCtx, s.stopper, sqlRegistry)

	s.pgServer = pgwire.MakeServer(s.ctx.Context, s.sqlExecutor, sqlRegistry)

	// TODO(bdarnell): make StoreConfig configurable.
	nCtx := storage.StoreContext{
		Clock:                          s.clock,
		DB:                             s.db,
		Gossip:                         s.gossip,
		Transport:                      s.raftTransport,
		RaftTickInterval:               s.ctx.RaftTickInterval,
		ScanInterval:                   s.ctx.ScanInterval,
		ScanMaxIdleTime:                s.ctx.ScanMaxIdleTime,
		ConsistencyCheckInterval:       s.ctx.ConsistencyCheckInterval,
		ConsistencyCheckPanicOnFailure: s.ctx.ConsistencyCheckPanicOnFailure,
		Tracer:    s.Tracer,
		StorePool: s.storePool,
		SQLExecutor: sql.InternalExecutor{
			LeaseManager: s.leaseMgr,
		},
		LogRangeEvents: true,
		AllocatorOptions: storage.AllocatorOptions{
			AllowRebalance: true,
		},
	}
	if ctx.TestingKnobs.Store != nil {
		nCtx.TestingKnobs = *ctx.TestingKnobs.Store.(*storage.StoreTestingKnobs)
	}

	s.recorder = status.NewMetricsRecorder(s.clock)
	s.recorder.AddNodeRegistry("sql.%s", sqlRegistry)
	s.recorder.AddNodeRegistry("txn.%s", txnRegistry)
	s.recorder.AddNodeRegistry("clock-offset.%s", s.rpcContext.RemoteClocks.Registry())
	s.recorder.AddNodeRegistry("gossip.%s", gossipRegistry)

	s.runtime = status.MakeRuntimeStatSampler(s.clock)
	s.recorder.AddNodeRegistry("sys.%s", s.runtime.Registry())

	s.node = NewNode(nCtx, s.recorder, s.stopper, txnMetrics, sql.MakeEventLogger(s.leaseMgr))
	roachpb.RegisterInternalServer(s.grpc, s.node)
	roachpb.RegisterInternalStoresServer(s.grpc, s.node.InternalStoresServer)

	s.tsDB = ts.NewDB(s.db)
	s.tsServer = ts.MakeServer(s.tsDB)

	s.admin = makeAdminServer(s)
	s.status = newStatusServer(s.db, s.gossip, s.recorder, s.ctx.Context, s.rpcContext, s.node.stores)
	for _, gw := range []grpcGatewayServer{&s.admin, s.status, &s.tsServer} {
		gw.RegisterService(s.grpc)
	}

	return s, nil
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

// Start starts the server on the specified port, starts gossip and
// initializes the node using the engines from the server's context.
func (s *Server) Start() error {
	tlsConfig, err := s.ctx.GetServerTLSConfig()
	if err != nil {
		return err
	}

	httpServer := netutil.MakeServer(s.stopper, tlsConfig, s)
	plainRedirectServer := netutil.MakeServer(s.stopper, tlsConfig, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO(tamird): s/308/http.StatusPermanentRedirect/ when it exists.
		http.Redirect(w, r, "https://"+r.Host+r.RequestURI, 308)
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

	ln, err := net.Listen("tcp", s.ctx.Addr)
	if err != nil {
		return err
	}
	unresolvedAddr, err := officialAddr(s.ctx.Addr, ln.Addr())
	if err != nil {
		return err
	}
	s.ctx.Addr = unresolvedAddr.String()
	s.rpcContext.SetLocalInternalServer(s.node)

	m := cmux.New(ln)
	pgL := m.Match(pgwire.Match)
	anyL := m.Match(cmux.Any())

	httpLn, err := net.Listen("tcp", s.ctx.HTTPAddr)
	if err != nil {
		return err
	}
	unresolvedHTTPAddr, err := officialAddr(s.ctx.HTTPAddr, httpLn.Addr())
	if err != nil {
		return err
	}
	s.ctx.HTTPAddr = unresolvedHTTPAddr.String()

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Fatal(context.TODO(), err)
		}
	})

	if tlsConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		s.stopper.RunWorker(func() {
			netutil.FatalIfUnexpected(httpMux.Serve())
		})

		s.stopper.RunWorker(func() {
			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		})

		httpLn = tls.NewListener(tlsL, tlsConfig)
	}

	s.stopper.RunWorker(func() {
		netutil.FatalIfUnexpected(httpServer.Serve(httpLn))
	})

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(anyL.Close())
		<-s.stopper.ShouldStop()
		s.grpc.Stop()
	})

	s.stopper.RunWorker(func() {
		netutil.FatalIfUnexpected(s.grpc.Serve(anyL))
	})

	s.stopper.RunWorker(func() {
		netutil.FatalIfUnexpected(httpServer.ServeWith(s.stopper, pgL, func(conn net.Conn) {
			if err := s.pgServer.ServeConn(conn); err != nil && !netutil.IsClosedConnection(err) {
				log.Error(context.TODO(), err)
			}
		}))
	})

	if len(s.ctx.SocketFile) != 0 {
		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", s.ctx.SocketFile)
		if err != nil {
			return err
		}

		s.stopper.RunWorker(func() {
			<-s.stopper.ShouldQuiesce()
			if err := unixLn.Close(); err != nil {
				log.Fatal(context.TODO(), err)
			}
		})

		s.stopper.RunWorker(func() {
			netutil.FatalIfUnexpected(httpServer.ServeWith(s.stopper, unixLn, func(conn net.Conn) {
				if err := s.pgServer.ServeConn(conn); err != nil &&
					!netutil.IsClosedConnection(err) {
					log.Error(context.TODO(), err)
				}
			}))
		})
	}

	s.gossip.Start(unresolvedAddr)

	ctx := context.Background()
	if err := s.node.start(ctx, unresolvedAddr, s.ctx.Engines, s.ctx.NodeAttributes); err != nil {
		return err
	}

	// Begin recording runtime statistics.
	s.startSampleEnvironment(s.ctx.MetricsSampleInterval)

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(s.recorder, s.ctx.MetricsSampleInterval, ts.Resolution10s, s.stopper)

	// Begin recording status summaries.
	s.node.startWriteSummaries(s.ctx.MetricsSampleInterval)

	s.sqlExecutor.SetNodeID(s.node.Descriptor.NodeID)
	s.distSQLServer.SetNodeID(s.node.Descriptor.NodeID)

	// Create and start the schema change manager only after a NodeID
	// has been assigned.
	testingKnobs := new(sql.SchemaChangeManagerTestingKnobs)
	if s.ctx.TestingKnobs.SQLSchemaChangeManager != nil {
		testingKnobs = s.ctx.TestingKnobs.SQLSchemaChangeManager.(*sql.SchemaChangeManagerTestingKnobs)
	}
	sql.NewSchemaChangeManager(testingKnobs, *s.db, s.gossip, s.leaseMgr).Start(s.stopper)

	log.Infof(context.TODO(), "starting %s server at %s", s.ctx.HTTPRequestScheme(), unresolvedHTTPAddr)
	log.Infof(context.TODO(), "starting grpc/postgres server at %s", unresolvedAddr)
	if len(s.ctx.SocketFile) != 0 {
		log.Infof(context.TODO(), "starting postgres server at unix:%s", s.ctx.SocketFile)
	}

	s.stopper.RunWorker(func() {
		netutil.FatalIfUnexpected(m.Serve())
	})

	// Initialize grpc-gateway mux and context.
	jsonpb := &util.JSONPb{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
	}
	protopb := new(util.ProtoPb)
	gwMux := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, jsonpb),
		gwruntime.WithMarshalerOption(util.JSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(util.AltJSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(util.ProtoContentType, protopb),
		gwruntime.WithMarshalerOption(util.AltProtoContentType, protopb),
	)
	gwCtx, gwCancel := context.WithCancel(ctx)
	s.stopper.AddCloser(stop.CloserFn(gwCancel))

	// Setup HTTP<->gRPC handlers.
	conn, err := s.rpcContext.GRPCDial(s.ctx.Addr)
	if err != nil {
		return errors.Errorf("error constructing grpc-gateway: %s; are your certificates valid?", err)
	}

	for _, gw := range []grpcGatewayServer{&s.admin, s.status, &s.tsServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return err
		}
	}

	var uiFileSystem http.FileSystem
	uiDebug := envutil.EnvOrDefaultBool("debug_ui", false)
	if uiDebug {
		uiFileSystem = http.Dir("ui")
	} else {
		uiFileSystem = &assetfs.AssetFS{
			Asset:     ui.Asset,
			AssetDir:  ui.AssetDir,
			AssetInfo: ui.AssetInfo,
		}
	}
	uiFileServer := http.FileServer(uiFileSystem)

	s.mux.HandleFunc("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			if uiDebug {
				r.URL.Path = "debug.html"
			} else {
				r.URL.Path = "release.html"
			}
		}
		uiFileServer.ServeHTTP(w, r)
	}))

	// TODO(marc): when cookie-based authentication exists,
	// apply it for all web endpoints.
	s.mux.HandleFunc(debugEndpoint, http.HandlerFunc(handleDebug))
	s.mux.Handle(adminEndpoint, gwMux)
	s.mux.Handle(ts.URLPrefix, gwMux)
	s.mux.Handle(statusPrefix, s.status)
	s.mux.Handle(healthEndpoint, s.status)

	if err := sdnotify.Ready(); err != nil {
		log.Errorf(context.TODO(), "failed to signal readiness using systemd protocol: %s", err)
	}

	return nil
}

func (s *Server) doDrain(modes []serverpb.DrainMode, setTo bool) ([]serverpb.DrainMode, error) {
	for _, mode := range modes {
		var err error
		switch {
		case mode == serverpb.DrainMode_CLIENT:
			err = s.pgServer.SetDraining(setTo)
		case mode == serverpb.DrainMode_LEASES:
			err = s.node.SetDraining(setTo)
		default:
			err = errors.Errorf("unknown drain mode: %v (%d)", mode, mode)
		}
		if err != nil {
			return nil, err
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
	s.stopper.RunWorker(func() {
		ticker := time.NewTicker(frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.runtime.SampleEnvironment()
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop()
}

// ServeHTTP is necessary to implement the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This is our base handler, so catch all panics and make sure they stick.
	defer log.FatalOnPanic()

	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	ae := r.Header.Get(util.AcceptEncodingHeader)
	switch {
	case strings.Contains(ae, util.GzipEncoding):
		w.Header().Set(util.ContentEncodingHeader, util.GzipEncoding)
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
