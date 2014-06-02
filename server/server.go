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
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

var (
	httpAddr = flag.String("http_addr", "localhost:8080", "TCP network address to bind to for HTTP traffic")
	rpcAddr  = flag.String("rpc_addr", "localhost:8081", "TCP network address to bind to for RPC traffic")

	// dataDirs is specified to enable durable storage via
	// RocksDB-backed key-value stores. Memory-backed key value stores
	// may be optionally specified via a comma-separated list of integer
	// sizes.
	dataDirs = flag.String("data_dirs", "", "specify a comma-separated list of disk "+
		"type and path or integer size in bytes. For solid state disks, ssd=<path>; "+
		"for spinning disks, hdd=<path>; for in-memory, mem=<size in bytes>. E.g. "+
		"-data_dirs=hdd=/mnt/hda1,ssd=/mnt/ssd01,ssd=/mnt/ssd02,mem=1073741824")

	// Regular expression for capturing data directory specifications.
	dataDirRE = regexp.MustCompile(`^(mem)=([\d]*)|(ssd|hdd)=(.*)$`)
)

// A CmdStart command starts nodes by joining the gossip network.
var CmdStart = &commander.Command{
	UsageLine: "start",
	Short:     "start node by joining the gossip network",
	Long: fmt.Sprintf(`
Start Cockroach node by joining the gossip network and exporting key
ranges stored on physical device(s). The gossip network is joined by
contacting one or more well-known hosts specified by the
-gossip_bootstrap command line flag. Every node should be run with
the same list of bootstrap hosts to guarantee a connected network. An
alternate approach is to use a single host for -gossip_bootstrap and
round-robin DNS.

Each node exports data from one or more physical devices. These
devices are specified via the -data_dirs command line flag. This is a
comma-separated list of paths to storage directories. Although the
paths should be specified to correspond uniquely to physical devices,
this requirement isn't strictly enforced.

A node exports an HTTP API with the following endpoints:

  Health check:           http://%s/healthz
  Key-value REST:         http://%s%s
  Structured Schema REST: http://%s%s
`, *httpAddr, *httpAddr, kv.KVKeyPrefix, *httpAddr, structured.StructuredKeyPrefix),
	Run:  runStart,
	Flag: *flag.CommandLine,
}

type server struct {
	mux            *http.ServeMux
	rpc            *rpc.Server
	gossip         *gossip.Gossip
	kvDB           kv.DB
	kvREST         *kv.RESTServer
	node           *Node
	admin          *adminServer
	structuredDB   *structured.DB
	structuredREST *structured.RESTServer
}

// ListenAndServe starts an HTTP server at -http_addr and an RPC server
// at -rpc_addr. This method won't return unless the server is shutdown
// or a non-temporary error occurs on the HTTP server connection.
func runStart(cmd *commander.Command, args []string) {
	glog.Info("starting cockroach cluster")
	s, err := newServer()
	if err != nil {
		glog.Fatal(err)
	}
	// init engines from -data_dirs
	engines, err := initEngines()
	if err != nil {
		glog.Fatal(err)
	}

	err = s.start(engines)

	s.stop()
	if err != nil {
		glog.Fatal(err)
	}
}

// initEngines interprets the -data_dirs command line flag to
// initialize a slice of storage.Engine objects.
func initEngines() ([]storage.Engine, error) {
	engines := make([]storage.Engine, 0, 1)
	for _, dir := range strings.Split(*dataDirs, ",") {
		engine, err := initEngine(dir)
		if err != nil {
			glog.Warningf("%v; skipping...will not serve data", err)
			continue
		}
		engines = append(engines, engine)
	}

	return engines, nil
}

// initEngine parses the engine specification according to the
// dataDirRE regexp and instantiates an engine of correct type.
func initEngine(spec string) (storage.Engine, error) {
	// Error if regexp doesn't match.
	matches := dataDirRE.FindStringSubmatch(spec)
	if matches == nil || len(matches) != 5 {
		return nil, util.Errorf("invalid engine specification %q", spec)
	}

	var engine storage.Engine
	var err error
	if matches[1] == "mem" {
		size, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			return nil, util.Errorf("unable to init in-memory storage %q", spec)
		}
		engine = storage.NewInMem(size)
	} else {
		// type, file = matches[3], matches[4]
		var typ storage.DiskType
		switch matches[3] {
		case "hdd":
			typ = storage.HDD
		case "ssd":
			typ = storage.SSD
		default:
			return nil, util.Errorf("unhandled disk type %q", matches[3])
		}
		engine, err = storage.NewRocksDB(typ, matches[4])
		if err != nil {
			return nil, util.Errorf("unable to init rocksdb with data dir %q", matches[4])
		}
	}
	return engine, nil
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
	s.kvDB = kv.NewDB(s.gossip)
	s.kvREST = kv.NewRESTServer(s.kvDB)
	s.node = NewNode(s.rpc, s.kvDB, s.gossip)
	s.admin = newAdminServer(s.kvDB)
	s.structuredDB = structured.NewDB(s.kvDB)
	s.structuredREST = structured.NewRESTServer(s.structuredDB)

	return s, nil
}

func (s *server) start(engines []storage.Engine) error {
	go s.rpc.ListenAndServe() // blocks, so launch in a goroutine
	glog.Infoln("Started RPC server at", *rpcAddr)

	s.gossip.Start()
	glog.Infoln("Started gossip instance")

	// Init the engines specified via command line flags if not supplied.
	if engines == nil {
		var err error
		engines, err = initEngines()
		if err != nil {
			return err
		}
	}
	if err := s.node.start(engines); err != nil {
		return err
	}
	glog.Infoln("Initialized %d storage engines", len(engines))

	s.initHTTP()
	return http.ListenAndServe(*httpAddr, s)
}

func (s *server) initHTTP() {
	glog.Infof("Starting HTTP server at %s", *httpAddr)
	s.mux.HandleFunc(adminKeyPrefix+"healthz", s.admin.handleHealthz)
	s.mux.HandleFunc(zoneKeyPrefix, s.admin.handleZoneAction)
	s.mux.HandleFunc(kv.KVKeyPrefix, s.kvREST.HandleAction)
	s.mux.HandleFunc(structured.StructuredKeyPrefix, s.structuredREST.HandleAction)
}

func (s *server) stop() {
	// TODO(spencer): the http server should exit; this functionality is
	// slated for go 1.3.
	s.node.stop()
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
