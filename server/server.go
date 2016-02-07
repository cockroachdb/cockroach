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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/opentracing/opentracing-go"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	crpc "github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/ui"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/grpcutil"
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

const txnRegistryName = "txn"

// Server is the cockroach server node.
type Server struct {
	Tracer opentracing.Tracer
	ctx    *Context

	mux           *http.ServeMux
	httpReady     chan struct{}
	clock         *hlc.Clock
	rpcContext    *crpc.Context
	rpc           *crpc.Server
	grpc          *grpc.Server
	gossip        *gossip.Gossip
	storePool     *storage.StorePool
	db            *client.DB
	kvDB          *kv.DBServer
	sqlServer     sql.Server
	pgServer      pgwire.Server
	node          *Node
	recorder      *status.NodeStatusRecorder
	admin         *adminServer
	status        *statusServer
	tsDB          *ts.DB
	tsServer      *ts.Server
	raftTransport storage.RaftTransport

	// registry is the root metrics Registry in a Cockroach node. Adding metrics directly to this
	// registry should be rare. Instead, add more specialized registries to this one. Refer to the
	// package docs for util/metric for more information on metrics.
	registry *metric.Registry

	stopper             *stop.Stopper
	sqlExecutor         *sql.Executor
	leaseMgr            *sql.LeaseManager
	schemaChangeManager *sql.SchemaChangeManager
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
		log.Warning("running in insecure mode, this is strongly discouraged. See --insecure and --certs.")
	}
	// Try loading the TLS configs before anything else.
	if _, err := ctx.GetServerTLSConfig(); err != nil {
		return nil, err
	}
	if _, err := ctx.GetClientTLSConfig(); err != nil {
		return nil, err
	}

	s := &Server{
		Tracer:    tracing.NewTracer(),
		ctx:       ctx,
		mux:       http.NewServeMux(),
		httpReady: make(chan struct{}),
		clock:     hlc.NewClock(hlc.UnixNano),
		registry:  metric.NewRegistry(),
		stopper:   stopper,
	}
	s.clock.SetMaxOffset(ctx.MaxOffset)

	s.rpcContext = crpc.NewContext(&ctx.Context, s.clock, stopper)
	stopper.RunWorker(func() {
		s.rpcContext.RemoteClocks.MonitorRemoteOffsets(stopper)
	})

	s.rpc = crpc.NewServer(s.rpcContext)

	s.gossip = gossip.New(s.rpcContext, s.ctx.GossipBootstrapResolvers, stopper)
	s.storePool = storage.NewStorePool(s.gossip, s.clock, ctx.TimeUntilStoreDead, stopper)

	feed := util.NewFeed(stopper)

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

	s.grpc = grpc.NewServer()
	s.raftTransport = newRPCTransport(s.gossip, s.grpc, s.rpcContext)
	s.stopper.AddCloser(s.raftTransport)

	s.kvDB = kv.NewDBServer(&s.ctx.Context, sender, stopper)
	if err := s.kvDB.RegisterRPC(s.rpc); err != nil {
		return nil, err
	}

	s.leaseMgr = sql.NewLeaseManager(0, *s.db, s.clock)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)
	s.sqlExecutor = sql.NewExecutor(*s.db, s.gossip, s.leaseMgr, s.stopper)
	s.sqlServer = sql.MakeServer(&s.ctx.Context, s.sqlExecutor)
	if err := s.sqlServer.RegisterRPC(s.rpc); err != nil {
		return nil, err
	}

	s.pgServer = pgwire.MakeServer(&s.ctx.Context, s.sqlServer.Executor)

	// TODO(bdarnell): make StoreConfig configurable.
	nCtx := storage.StoreContext{
		Clock:           s.clock,
		DB:              s.db,
		Gossip:          s.gossip,
		Transport:       s.raftTransport,
		ScanInterval:    s.ctx.ScanInterval,
		ScanMaxIdleTime: s.ctx.ScanMaxIdleTime,
		EventFeed:       feed,
		Tracer:          s.Tracer,
		StorePool:       s.storePool,
		SQLExecutor: sql.InternalExecutor{
			LeaseManager: s.leaseMgr,
		},
		LogRangeEvents: true,
		AllocatorOptions: storage.AllocatorOptions{
			AllowRebalance: true,
			Mode:           s.ctx.BalanceMode,
		},
	}
	subRegistries := []status.NodeSubregistry{
		{"pgwire", s.pgServer.Registry()},
		{"sql", s.sqlExecutor.Registry()},
		{txnRegistryName, txnRegistry},
	}
	s.node = NewNode(nCtx, s.registry, s.stopper, subRegistries, txnMetrics)
	s.admin = newAdminServer(s.db, s.stopper, s.sqlExecutor)
	s.tsDB = ts.NewDB(s.db)
	s.tsServer = ts.NewServer(s.tsDB)

	return s, nil
}

// Start starts the server on the specified port, starts gossip and
// initializes the node using the engines from the server's context.
func (s *Server) Start() error {
	tlsConfig, err := s.ctx.GetServerTLSConfig()
	if err != nil {
		return err
	}

	// The following code is a specialization of util/net.go's ListenAndServe
	// which adds pgwire support. A single port is used to serve all protocols
	// (pg, http, h2) via the following construction:
	//
	// non-TLS case:
	// net.Listen -> cmux.New -> pgwire.Match -> pgwire.Server.ServeConn
	//               |
	//               -  -> cmux.HTTP2 -> http2.(*Server).ServeConn
	//               -  -> cmux.Any -> http.(*Server).Serve
	//
	// TLS case:
	// net.Listen -> cmux.New -> pgwire.Match -> pgwire.Server.ServeConn
	//               |
	//               -  -> cmux.Any -> tls.NewListener -> http.(*Server).Serve
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

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldDrain()
		if err := ln.Close(); err != nil {
			log.Fatal(err)
		}
	})

	m := cmux.New(ln)
	pgL := m.Match(pgwire.Match)

	var serveConn func(net.Listener, func(net.Conn)) error
	if tlsConfig != nil {
		anyL := m.Match(cmux.Any())
		serveConn = util.ServeHandler(s.stopper, s, tls.NewListener(anyL, tlsConfig), tlsConfig)
	} else {
		h2L := m.Match(cmux.HTTP2())
		anyL := m.Match(cmux.Any())

		var h2 http2.Server
		serveConnOpts := &http2.ServeConnOpts{
			Handler: s,
		}
		serveH2 := func(conn net.Conn) {
			h2.ServeConn(conn, serveConnOpts)
		}

		serveConn = util.ServeHandler(s.stopper, s, anyL, tlsConfig)

		s.stopper.RunWorker(func() {
			util.FatalIfUnexpected(serveConn(h2L, serveH2))
		})
	}

	s.stopper.RunWorker(func() {
		util.FatalIfUnexpected(serveConn(pgL, func(conn net.Conn) {
			if err := s.pgServer.ServeConn(conn); err != nil && !util.IsClosedConnection(err) {
				log.Error(err)
			}
		}))
	})

	s.stopper.RunWorker(func() {
		util.FatalIfUnexpected(m.Serve())
	})

	s.rpcContext.SetLocalServer(s.rpc, s.ctx.Addr)

	s.gossip.Start(s.grpc, unresolvedAddr)

	if err := s.node.start(s.rpc, unresolvedAddr, s.ctx.Engines, s.ctx.NodeAttributes); err != nil {
		return err
	}

	// Begin recording runtime statistics.
	runtime := status.NewRuntimeStatRecorder(s.node.Descriptor.NodeID, s.clock)
	s.tsDB.PollSource(runtime, s.ctx.MetricsFrequency, ts.Resolution10s, s.stopper)

	// Begin recording time series data collected by the status monitor.
	s.recorder = status.NewNodeStatusRecorder(s.node.status, s.clock)
	s.tsDB.PollSource(s.recorder, s.ctx.MetricsFrequency, ts.Resolution10s, s.stopper)

	// Begin recording status summaries.
	s.startWriteSummaries()

	s.sqlServer.SetNodeID(s.node.Descriptor.NodeID)
	// Create and start the schema change manager only after a NodeID
	// has been assigned.
	s.schemaChangeManager = sql.NewSchemaChangeManager(*s.db, s.gossip, s.leaseMgr)
	s.schemaChangeManager.Start(s.stopper)

	s.status = newStatusServer(s.db, s.gossip, s.registry, s.ctx)

	log.Infof("starting %s/postgres server at %s", s.ctx.HTTPRequestScheme(), unresolvedAddr)
	s.initHTTP()

	return nil
}

// initHTTP registers http prefixes.
func (s *Server) initHTTP() {
	s.mux.Handle(rpc.DefaultRPCPath, s.rpc)

	s.mux.Handle("/", grpcutil.GRPCHandlerFunc(s.grpc, http.FileServer(
		&assetfs.AssetFS{
			Asset:     ui.Asset,
			AssetDir:  ui.AssetDir,
			AssetInfo: ui.AssetInfo,
		},
	)))

	// The admin server handles both /debug/ and /_admin/
	// TODO(marc): when cookie-based authentication exists,
	// apply it for all web endpoints.
	s.mux.Handle(adminEndpoint, s.admin)
	s.mux.Handle(debugEndpoint, s.admin)
	s.mux.Handle(statusPrefix, s.status)
	s.mux.Handle(healthEndpoint, s.status)
	s.mux.Handle(ts.URLPrefix, s.tsServer)

	// The SQL endpoints handles its own authentication, verifying user
	// credentials against the requested user.
	s.mux.Handle(driver.Endpoint, s.sqlServer)

	close(s.httpReady)
}

// startWriteSummaries begins periodically persisting status summaries for the
// node and its stores.
func (s *Server) startWriteSummaries() {
	s.stopper.RunWorker(func() {
		ticker := time.NewTicker(s.ctx.MetricsFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.stopper.RunTask(func() {
					if err := s.writeSummaries(); err != nil {
						log.Error(err)
					}
				})
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// writeSummaries retrieves status summaries from the supplied
// NodeStatusRecorder and persists them to the cockroach data store.
func (s *Server) writeSummaries() error {
	nodeStatus, storeStatuses := s.recorder.GetStatusSummaries()
	if nodeStatus != nil {
		key := keys.NodeStatusKey(int32(nodeStatus.Desc.NodeID))
		if pErr := s.db.Put(key, nodeStatus); pErr != nil {
			return pErr.GoError()
		}
		if log.V(1) {
			statusJSON, err := json.Marshal(nodeStatus)
			if err != nil {
				log.Errorf("error marshaling nodeStatus to json: %s", err)
			}
			log.Infof("node %d status: %s", nodeStatus.Desc.NodeID, statusJSON)
		}
	}

	for _, ss := range storeStatuses {
		key := keys.StoreStatusKey(int32(ss.Desc.StoreID))
		if pErr := s.db.Put(key, &ss); pErr != nil {
			return pErr.GoError()
		}
		if log.V(1) {
			statusJSON, err := json.Marshal(&ss)
			if err != nil {
				log.Errorf("error marshaling storeStatus to json: %s", err)
			}
			log.Infof("store %d status: %s", ss.Desc.StoreID, statusJSON)
		}
	}
	return nil
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop()
}

// ServeHTTP is necessary to implement the http.Handler interface. It
// will snappy a response if the appropriate request headers are set.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check if we're draining; if so return 503, service unavailable.
	if !s.stopper.RunTask(func() {
		// This is our base handler, so catch all panics and make sure they stick.
		defer log.FatalOnPanic()

		// If server is not available, return 503 http response code.
		select {
		case <-s.httpReady:
			// Proceed.
		default:
			http.Error(w, "node not yet available", http.StatusServiceUnavailable)
			return
		}
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
	}) {
		http.Error(w, "service is draining", http.StatusServiceUnavailable)
	}
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
