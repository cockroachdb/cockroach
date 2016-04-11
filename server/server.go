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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
)

var (
	// Allocation pool for gzip writers.
	gzipWriterPool sync.Pool
	// Allocation pool for snappy writers.
	snappyWriterPool sync.Pool
)

// Server is the cockroach server node.
type Server struct {
	Tracer              opentracing.Tracer
	ctx                 *Context
	mux                 *http.ServeMux
	clock               *hlc.Clock
	rpcContext          *rpc.Context
	grpc                *grpc.Server
	gossip              *gossip.Gossip
	storePool           *storage.StorePool
	db                  *client.DB
	kvDB                *kv.DBServer
	pgServer            pgwire.Server
	node                *Node
	recorder            *status.MetricsRecorder
	runtime             status.RuntimeStatSampler
	admin               *adminServer
	status              *statusServer
	tsDB                *ts.DB
	tsServer            *ts.Server
	raftTransport       *storage.RaftTransport
	stopper             *stop.Stopper
	sqlExecutor         *sql.Executor
	leaseMgr            *sql.LeaseManager
	schemaChangeManager *sql.SchemaChangeManager
	parsedUpdatesURL    *url.URL
	parsedReportingURL  *url.URL
}

// NewServer creates a Server from a server.Context.
func NewServer(ctx *Context, stopper *stop.Stopper) (*Server, error) {
	if ctx == nil {
		return nil, util.Errorf("ctx must not be null")
	}

	if _, err := net.ResolveTCPAddr("tcp", ctx.Addr); err != nil {
		return nil, util.Errorf("unable to resolve RPC address %q: %v", ctx.Addr, err)
	}

	if ctx.Insecure {
		log.Warning("running in insecure mode, this is strongly discouraged. See --insecure.")
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

	s.rpcContext = rpc.NewContext(&ctx.Context, s.clock, stopper)
	s.rpcContext.HeartbeatCB = func() {
		if err := s.rpcContext.RemoteClocks.VerifyClockOffset(); err != nil {
			log.Fatal(err)
		}
	}

	s.gossip = gossip.New(s.rpcContext, s.ctx.GossipBootstrapResolvers, stopper)
	s.storePool = storage.NewStorePool(s.gossip, s.clock, ctx.TimeUntilStoreDead, stopper)

	// A custom RetryOptions is created which uses stopper.ShouldDrain() as
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
	retryOpts := kv.GetDefaultDistSenderRetryOptions()
	retryOpts.Closer = stopper.ShouldDrain()
	ds := kv.NewDistSender(&kv.DistSenderContext{
		Clock:           s.clock,
		RPCContext:      s.rpcContext,
		RPCRetryOptions: &retryOpts,
	}, s.gossip)
	txnRegistry := metric.NewRegistry()
	txnMetrics := kv.NewTxnMetrics(txnRegistry)
	sender := kv.NewTxnCoordSender(ds, s.clock, ctx.Linearizable, s.Tracer, s.stopper, txnMetrics)
	s.db = client.NewDB(sender)

	s.grpc = rpc.NewServer(s.rpcContext)
	s.raftTransport = storage.NewRaftTransport(storage.GossipAddressResolver(s.gossip), s.grpc, s.rpcContext)

	s.kvDB = kv.NewDBServer(&s.ctx.Context, sender, stopper)
	roachpb.RegisterExternalServer(s.grpc, s.kvDB)

	s.leaseMgr = sql.NewLeaseManager(0, *s.db, s.clock)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)
	eCtx := sql.ExecutorContext{
		DB:           s.db,
		Gossip:       s.gossip,
		LeaseManager: s.leaseMgr,
		Clock:        s.clock,
		TestingKnobs: &ctx.TestingKnobs.ExecutorTestingKnobs,
	}

	sqlRegistry := metric.NewRegistry()
	s.sqlExecutor = sql.NewExecutor(eCtx, s.stopper, sqlRegistry)

	s.pgServer = pgwire.MakeServer(&s.ctx.Context, s.sqlExecutor, sqlRegistry)

	// TODO(bdarnell): make StoreConfig configurable.
	nCtx := storage.StoreContext{
		Clock:                          s.clock,
		DB:                             s.db,
		Gossip:                         s.gossip,
		Transport:                      s.raftTransport,
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
		TestingKnobs: ctx.TestingKnobs.StoreTestingKnobs,
	}

	s.recorder = status.NewMetricsRecorder(s.clock)
	s.recorder.AddNodeRegistry("sql.%s", sqlRegistry)
	s.recorder.AddNodeRegistry("txn.%s", txnRegistry)
	s.recorder.AddNodeRegistry("clock-offset.%s", s.rpcContext.RemoteClocks.Registry())

	s.runtime = status.MakeRuntimeStatSampler(s.clock)
	s.recorder.AddNodeRegistry("sys.%s", s.runtime.Registry())

	s.node = NewNode(nCtx, s.recorder, s.stopper, txnMetrics)
	roachpb.RegisterInternalServer(s.grpc, s.node)

	s.admin = newAdminServer(s.db, s.stopper, s.sqlExecutor, ds, s.node)
	s.tsDB = ts.NewDB(s.db)
	s.tsServer = ts.NewServer(s.tsDB)
	s.status = newStatusServer(s.db, s.gossip, s.recorder, s.ctx)

	return s, nil
}

// Start starts the server on the specified port, starts gossip and
// initializes the node using the engines from the server's context.
func (s *Server) Start() error {
	s.initHTTP()

	tlsConfig, err := s.ctx.GetServerTLSConfig()
	if err != nil {
		return err
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

	ln, err := net.Listen("tcp", s.ctx.Addr)
	if err != nil {
		return err
	}
	unresolvedAddr, err := officialAddr(s.ctx.Addr, ln.Addr())
	if err != nil {
		return err
	}
	s.ctx.Addr = unresolvedAddr.String()
	s.rpcContext.SetLocalInternalServer(s.node, s.ctx.Addr)

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldDrain()
		if err := ln.Close(); err != nil {
			log.Fatal(err)
		}
	})

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
		<-s.stopper.ShouldDrain()
		if err := httpLn.Close(); err != nil {
			log.Fatal(err)
		}
	})

	if tlsConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1Fast())
		tlsL := httpMux.Match(cmux.Any())

		s.stopper.RunWorker(func() {
			util.FatalIfUnexpected(httpMux.Serve())
		})

		util.ServeHandler(s.stopper, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// TODO(tamird): s/308/http.StatusPermanentRedirect/ when it exists.
			http.Redirect(w, r, "https://"+r.Host+r.RequestURI, 308)
		}), clearL, tlsConfig)

		httpLn = tls.NewListener(tlsL, tlsConfig)
	}

	serveConn := util.ServeHandler(s.stopper, s, httpLn, tlsConfig)

	s.stopper.RunWorker(func() {
		util.FatalIfUnexpected(s.grpc.Serve(anyL))
	})

	s.stopper.RunWorker(func() {
		util.FatalIfUnexpected(serveConn(pgL, func(conn net.Conn) {
			if err := s.pgServer.ServeConn(conn); err != nil && !util.IsClosedConnection(err) {
				log.Error(err)
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
			<-s.stopper.ShouldDrain()
			if err := unixLn.Close(); err != nil {
				log.Fatal(err)
			}
		})

		s.stopper.RunWorker(func() {
			util.FatalIfUnexpected(serveConn(unixLn, func(conn net.Conn) {
				if err := s.pgServer.ServeConn(conn); err != nil && !util.IsClosedConnection(err) {
					log.Error(err)
				}
			}))
		})
	}

	s.gossip.Start(s.grpc, unresolvedAddr)

	if err := s.node.start(unresolvedAddr, s.ctx.Engines, s.ctx.NodeAttributes); err != nil {
		return err
	}

	// Begin recording runtime statistics.
	s.startSampleEnvironment(s.ctx.MetricsSampleInterval)

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(s.recorder, s.ctx.MetricsSampleInterval, ts.Resolution10s, s.stopper)

	// Begin recording status summaries.
	s.node.startWriteSummaries(s.ctx.MetricsSampleInterval)

	s.sqlExecutor.SetNodeID(s.node.Descriptor.NodeID)
	// Create and start the schema change manager only after a NodeID
	// has been assigned.
	s.schemaChangeManager = sql.NewSchemaChangeManager(*s.db, s.gossip, s.leaseMgr)
	s.schemaChangeManager.Start(s.stopper)

	s.periodicallyCheckForUpdates()

	log.Infof("starting %s server at %s", s.ctx.HTTPRequestScheme(), unresolvedHTTPAddr)
	log.Infof("starting grpc/postgres server at %s", unresolvedAddr)
	if len(s.ctx.SocketFile) != 0 {
		log.Infof("starting postgres server at unix:%s", s.ctx.SocketFile)
	}

	s.stopper.RunWorker(func() {
		util.FatalIfUnexpected(m.Serve())
	})

	// Register admin service. Must happen after serving starts.
	s.stopper.AddCloser(s.admin)
	RegisterAdminServer(s.grpc, s.admin)
	return s.admin.RegisterGRPCGateway(s.ctx)
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

var uiFileSystem http.FileSystem

// initHTTP registers http prefixes.
func (s *Server) initHTTP() {
	s.mux.Handle("/", http.FileServer(uiFileSystem))

	// The admin server handles both /debug/ and /_admin/
	// TODO(marc): when cookie-based authentication exists,
	// apply it for all web endpoints.
	s.mux.Handle(adminEndpoint, s.admin)
	s.mux.Handle(debugEndpoint, s.admin)
	s.mux.Handle(statusPrefix, s.status)
	s.mux.Handle(healthEndpoint, s.status)
	s.mux.Handle(ts.URLPrefix, s.tsServer)
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop()
}

// ServeHTTP is necessary to implement the http.Handler interface. It
// will snappy a response if the appropriate request headers are set.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This is our base handler, so catch all panics and make sure they stick.
	defer log.FatalOnPanic()

	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	ae := r.Header.Get(util.AcceptEncodingHeader)
	switch {
	case strings.Contains(ae, util.SnappyEncoding):
		w.Header().Set(util.ContentEncodingHeader, util.SnappyEncoding)
		s := newSnappyResponseWriter(w)
		defer s.Close()
		w = s
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

type snappyResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func newSnappyResponseWriter(w http.ResponseWriter) *snappyResponseWriter {
	var s *snappy.Writer
	if sI := snappyWriterPool.Get(); sI == nil {
		// TODO(pmattis): It would be better to use the C++ snappy code
		// like rpc/codec is doing. Would have to copy the snappy.Writer
		// implementation from snappy-go.
		s = snappy.NewWriter(w)
	} else {
		s = sI.(*snappy.Writer)
		s.Reset(w)
	}
	return &snappyResponseWriter{Writer: s, ResponseWriter: w}
}

func (w *snappyResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w *snappyResponseWriter) Close() {
	if w.Writer != nil {
		snappyWriterPool.Put(w.Writer)
		w.Writer = nil
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
