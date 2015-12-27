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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
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
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracer"
	assetfs "github.com/elazarl/go-bindata-assetfs"
)

var (
	// Allocation pool for gzip writers.
	gzipWriterPool sync.Pool
	// Allocation pool for snappy writers.
	snappyWriterPool sync.Pool
)

// Server is the cockroach server node.
type Server struct {
	ctx *Context

	listener net.Listener // Only used in tests.

	mux           *http.ServeMux
	clock         *hlc.Clock
	rpcContext    *crpc.Context
	rpc           *crpc.Server
	gossip        *gossip.Gossip
	storePool     *storage.StorePool
	db            *client.DB
	kvDB          *kv.DBServer
	sqlServer     sql.Server
	pgServer      *pgwire.Server
	node          *Node
	recorder      *status.NodeStatusRecorder
	admin         *adminServer
	status        *statusServer
	tsDB          *ts.DB
	tsServer      *ts.Server
	raftTransport storage.RaftTransport
	stopper       *stop.Stopper
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
		ctx:     ctx,
		mux:     http.NewServeMux(),
		clock:   hlc.NewClock(hlc.UnixNano),
		stopper: stopper,
	}
	s.clock.SetMaxOffset(ctx.MaxOffset)

	s.rpcContext = crpc.NewContext(&ctx.Context, s.clock, stopper)
	stopper.RunWorker(func() {
		s.rpcContext.RemoteClocks.MonitorRemoteOffsets(stopper)
	})

	s.rpc = crpc.NewServer(s.rpcContext)

	s.gossip = gossip.New(s.rpcContext, s.ctx.GossipBootstrapResolvers)
	s.storePool = storage.NewStorePool(s.gossip, s.clock, ctx.TimeUntilStoreDead, stopper)

	feed := util.NewFeed(stopper)
	tracer := tracer.NewTracer(feed, ctx.Addr)

	ds := kv.NewDistSender(&kv.DistSenderContext{Clock: s.clock, RPCContext: s.rpcContext}, s.gossip)
	sender := kv.NewTxnCoordSender(ds, s.clock, ctx.Linearizable, tracer, s.stopper)
	s.db = client.NewDB(sender)

	var err error
	s.raftTransport, err = newRPCTransport(s.gossip, s.rpc, s.rpcContext)
	if err != nil {
		return nil, err
	}
	s.stopper.AddCloser(s.raftTransport)

	s.kvDB = kv.NewDBServer(&s.ctx.Context, sender)
	if err := s.kvDB.RegisterRPC(s.rpc); err != nil {
		return nil, err
	}

	leaseMgr := sql.NewLeaseManager(0, *s.db, s.clock)
	leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)
	s.sqlServer = sql.MakeServer(&s.ctx.Context, *s.db, s.gossip, leaseMgr, s.stopper)
	if err := s.sqlServer.RegisterRPC(s.rpc); err != nil {
		return nil, err
	}

	s.pgServer = pgwire.NewServer(&pgwire.Context{
		Context:  &s.ctx.Context,
		Executor: s.sqlServer.Executor,
		Stopper:  stopper,
	})

	// TODO(bdarnell): make StoreConfig configurable.
	nCtx := storage.StoreContext{
		Clock:           s.clock,
		DB:              s.db,
		Gossip:          s.gossip,
		Transport:       s.raftTransport,
		ScanInterval:    s.ctx.ScanInterval,
		ScanMaxIdleTime: s.ctx.ScanMaxIdleTime,
		EventFeed:       feed,
		Tracer:          tracer,
		StorePool:       s.storePool,
		AllocatorOptions: storage.AllocatorOptions{
			AllowRebalance: true,
			Mode:           s.ctx.BalanceMode,
		},
	}
	s.node = NewNode(nCtx)
	s.admin = newAdminServer(s.db, s.stopper)
	s.status = newStatusServer(s.db, s.gossip, ctx)
	s.tsDB = ts.NewDB(s.db)
	s.tsServer = ts.NewServer(s.tsDB)

	return s, nil
}

// Start runs the RPC and HTTP servers, starts the gossip instance (if
// selfBootstrap is true, uses the rpc server's address as the gossip
// bootstrap), and starts the node using the supplied engines slice.
func (s *Server) Start(selfBootstrap bool) error {
	tlsConfig, err := s.ctx.GetServerTLSConfig()
	if err != nil {
		return err
	}

	unresolvedAddr := util.MakeUnresolvedAddr("tcp", s.ctx.Addr)
	ln, err := util.ListenAndServe(s.stopper, s, unresolvedAddr, tlsConfig)
	if err != nil {
		return err
	}

	s.listener = ln

	addr := ln.Addr()
	addrStr := addr.String()

	// Handle self-bootstrapping case for a single node.
	if selfBootstrap {
		selfResolver, err := resolver.NewResolver(&s.ctx.Context, addrStr)
		if err != nil {
			return err
		}
		s.gossip.SetResolvers([]resolver.Resolver{selfResolver})
	}
	s.gossip.Start(s.rpc, addr, s.stopper)

	if err := s.node.start(s.rpc, addr, s.ctx.Engines, s.ctx.NodeAttributes, s.stopper); err != nil {
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

	log.Infof("starting %s server at %s", s.ctx.HTTPRequestScheme(), addr)
	s.initHTTP()

	// TODO(tamird): pick a port here
	host, _, err := net.SplitHostPort(addrStr)
	if err != nil {
		return err
	}

	return s.pgServer.Start(util.MakeUnresolvedAddr("tcp", net.JoinHostPort(host, "0")))
}

// initHTTP registers http prefixes.
func (s *Server) initHTTP() {
	s.mux.Handle(rpc.DefaultRPCPath, s.rpc)

	s.mux.Handle("/", http.FileServer(
		&assetfs.AssetFS{Asset: ui.Asset, AssetDir: ui.AssetDir}))

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
		if err := s.db.Put(key, nodeStatus); err != nil {
			return err
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
		if err := s.db.Put(key, &ss); err != nil {
			return err
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
