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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/resource"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	assetfs "github.com/elazarl/go-bindata-assetfs"
)

// Allocation pool for gzip writers.
var gzipWriterPool sync.Pool

// Server is the cockroach server node.
type Server struct {
	ctx *Context

	host           string
	mux            *http.ServeMux
	clock          *hlc.Clock
	rpc            *rpc.Server
	gossip         *gossip.Gossip
	kv             *client.KV
	kvDB           *kv.DBServer
	kvREST         *kv.RESTServer
	node           *Node
	admin          *adminServer
	status         *statusServer
	structuredDB   structured.DB
	structuredREST *structured.RESTServer
	httpListener   *net.Listener // holds http endpoint information
	raftTransport  multiraft.Transport
}

// NewServer creates a Server from a server.Context.
func NewServer(ctx *Context) (*Server, error) {
	if ctx == nil {
		return nil, util.Error("ctx must not be null")
	}
	// Determine hostname in case it hasn't been specified in -rpc or -http.
	host, err := os.Hostname()
	if err != nil {
		host = "127.0.0.1"
	}

	rpcAddr := ctx.RPC
	// If the specified rpc address includes no host component, use the hostname.
	if strings.HasPrefix(rpcAddr, ":") {
		rpcAddr = host + rpcAddr
	}
	_, err = net.ResolveTCPAddr("tcp", rpcAddr)
	if err != nil {
		return nil, util.Errorf("unable to resolve RPC address %q: %v", rpcAddr, err)
	}

	var tlsConfig *rpc.TLSConfig
	if ctx.Certs == "" {
		tlsConfig = rpc.LoadInsecureTLSConfig()
	} else {
		if tlsConfig, err = rpc.LoadTLSConfigFromDir(ctx.Certs); err != nil {
			return nil, util.Errorf("unable to load TLS config: %v", err)
		}
	}

	s := &Server{
		ctx:   ctx,
		host:  host,
		mux:   http.NewServeMux(),
		clock: hlc.NewClock(hlc.UnixNano),
	}
	s.clock.SetMaxOffset(ctx.MaxOffset)

	rpcContext := rpc.NewContext(s.clock, tlsConfig)
	go rpcContext.RemoteClocks.MonitorRemoteOffsets()

	s.rpc = rpc.NewServer(util.MakeRawAddr("tcp", rpcAddr), rpcContext)
	s.gossip = gossip.New(rpcContext, s.ctx.GossipInterval, s.ctx.GossipBootstrap)

	// Create a client.KVSender instance for use with this node's
	// client to the key value database as well as
	ds := kv.NewDistSender(s.clock, s.gossip)
	sender := kv.NewTxnCoordSender(ds, s.clock, ctx.Linearizable)
	s.kv = client.NewKV(nil, sender)
	s.kv.User = storage.UserRoot

	s.raftTransport, err = newRPCTransport(s.gossip, s.rpc, rpcContext)
	if err != nil {
		return nil, err
	}

	s.kvDB = kv.NewDBServer(sender)
	s.kvREST = kv.NewRESTServer(s.kv)
	// TODO(bdarnell): make StoreConfig configurable.
	s.node = NewNode(s.kv, s.gossip, storage.StoreConfig{}, s.raftTransport)
	s.admin = newAdminServer(s.kv)
	s.status = newStatusServer(s.kv, s.gossip)
	s.structuredDB = structured.NewDB(s.kv)
	s.structuredREST = structured.NewRESTServer(s.structuredDB)

	return s, nil
}

// Start runs the RPC and HTTP servers, starts the gossip instance (if
// selfBootstrap is true, uses the rpc server's address as the gossip
// bootstrap), and starts the node using the supplied engines slice.
func (s *Server) Start(selfBootstrap bool) error {
	// Bind RPC socket and launch goroutine.
	if err := s.rpc.Start(); err != nil {
		return err
	}
	log.Infof("Started RPC server at %s", s.rpc.Addr())

	// Handle self-bootstrapping case for a single node.
	if selfBootstrap {
		s.gossip.SetBootstrap([]net.Addr{s.rpc.Addr()})
	}
	s.gossip.Start(s.rpc)
	log.Infoln("Started gossip instance")

	if err := s.node.start(s.rpc, s.clock, s.ctx.Engines, s.ctx.NodeAttributes); err != nil {
		return err
	}

	// TODO(spencer): add tls to the HTTP server.
	s.initHTTP()
	httpAddr := s.ctx.HTTP
	ln, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return util.Errorf("could not listen on %s: %s", httpAddr, err)
	}
	// Obtaining the http end point listener is difficult using
	// http.ListenAndServe(), so we are storing it with the server.
	s.httpListener = &ln
	log.Infof("Starting HTTP server at %s", ln.Addr())
	go http.Serve(ln, s)
	return nil
}

func (s *Server) initHTTP() {
	s.mux.Handle("/", http.FileServer(
		&assetfs.AssetFS{Asset: resource.Asset, AssetDir: resource.AssetDir, Prefix: "./ui/"}))

	// Admin handlers.
	s.admin.registerHandlers(s.mux)

	// Status endpoints:
	s.status.registerHandlers(s.mux)

	s.mux.Handle(kv.RESTPrefix, s.kvREST)
	s.mux.Handle(kv.DBPrefix, s.kvDB)
	s.mux.Handle(structured.StructuredKeyPrefix, s.structuredREST)
}

// Stop stops the server.
func (s *Server) Stop() {
	s.node.stop()
	s.gossip.Stop()
	s.rpc.Close()
	s.kv.Close()
	s.raftTransport.Close()
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

// ServeHTTP is necessary to implement the http.Handler interface. It
// will gzip a response if the appropriate request headers are set.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		s.mux.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gzw := newGzipResponseWriter(w)
	defer gzw.Close()
	s.mux.ServeHTTP(gzw, r)
}
