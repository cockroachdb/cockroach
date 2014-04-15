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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

// Package server implements a basic HTTP server for interacting with a node.
package server

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/structured"
)

var (
	httpAddr = flag.String("http_addr", "localhost:8080", "TCP network address to bind to for HTTP traffic")
	rpcAddr  = flag.String("rpc_addr", "localhost:8081", "TCP network address to bind to for RPC traffic")
)

type server struct {
	mux            *http.ServeMux
	rpc            *rpc.Server
	gossip         *gossip.Gossip
	node           *storage.Node
	kvDB           kv.DB
	kvREST         *kv.RESTServer
	structuredDB   *structured.DB
	structuredREST *structured.RESTServer
}

// ListenAndServe starts an HTTP server at --http_addr and an RPC server
// at --rpc_addr. This method won't return unless the server is shutdown
// or a non-temporary error occurs on the HTTP server connection.
func ListenAndServe() error {
	s, err := newServer()
	if err != nil {
		return err
	}
	err = s.start()
	s.stop()
	return err
}

func newServer() (*server, error) {
	addr, err := net.ResolveTCPAddr("tcp", *rpcAddr)
	if err != nil {
		return nil, err
	}
	s := &server{
		mux: http.NewServeMux(),
		rpc: rpc.NewServer(addr),
	}

	s.gossip = gossip.New(s.rpc)
	s.node = storage.NewNode(s.rpc, s.gossip)
	s.kvDB = kv.NewDB(s.gossip)
	s.kvREST = kv.NewRESTServer(s.kvDB)
	s.structuredDB = structured.NewDB(s.kvDB)
	s.structuredREST = structured.NewRESTServer(s.structuredDB)

	return s, nil
}

func (s *server) start() error {
	log.Println("Starting RPC server at", *rpcAddr)
	go s.rpc.ListenAndServe() // blocks, so launch in a goroutine

	log.Println("Starting gossip instance")
	s.gossip.Start()

	s.initHTTP()
	return http.ListenAndServe(*httpAddr, s)
}

func (s *server) initHTTP() {
	log.Println("Starting HTTP server at", *httpAddr)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc(kv.KVKeyPrefix, s.kvREST.HandleAction)
	s.mux.HandleFunc(structured.StructuredKeyPrefix, s.structuredREST.HandleAction)
}

func (s *server) stop() {
	s.gossip.Stop()
	s.rpc.Close()
}

type gzipResponseWriter struct {
	io.WriteCloser
	http.ResponseWriter
}

func newGzipResponseWriter(w http.ResponseWriter) *gzipResponseWriter {
	gz := gzip.NewWriter(w)
	return &gzipResponseWriter{WriteCloser: gz, ResponseWriter: w}
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.WriteCloser.Write(b)
}

// ServeHTTP is necessary to implement the http.Handler interface. It
// will gzip a response if the appropriate request headers are set.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		s.mux.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gzw := newGzipResponseWriter(w)
	defer gzw.Close()
	s.mux.ServeHTTP(gzw, r)
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
}
