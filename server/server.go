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
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/hlc"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

var (
	rpcAddr  = flag.String("rpc", ":0", "host:port to bind for RPC traffic; 0 to pick unused port")
	httpAddr = flag.String("http", ":8080", "host:port to bind for HTTP traffic; 0 to pick unused port")

	// stores is specified to enable durable storage via RocksDB-backed
	// key-value stores. Memory-backed key value stores may be
	// optionally specified via mem=<integer byte size>.
	stores = flag.String("stores", "", "specify a comma-separated list of stores, "+
		"specified by a colon-separated list of device attributes followed by '=' and "+
		"either a filepath for a persistent store or an integer size in bytes for an "+
		"in-memory store. Device attributes typically include whether the store is "+
		"flash (ssd), spinny disk (hdd), fusion-io (fio), in-memory (mem); device "+
		"attributes might also include speeds and other specs (7200rpm, 200kiops, etc.). "+
		"For example, -store=hdd:7200rpm=/mnt/hda1,ssd=/mnt/ssd01,ssd=/mnt/ssd02,mem=1073741824")

	// attrs specifies node topography or machine capabilities, used to
	// match capabilities or location preferences specified in zone configs.
	attrs = flag.String("attrs", "", "specify a comma-separated list of node "+
		"attributes. Attributes are arbitrary strings specifying topography or "+
		"machine capabilities. Topography might include datacenter designation (e.g. "+
		"\"us-west-1a\", \"us-west-1b\", \"us-east-1c\"). Machine capabilities "+
		"might include specialized hardware or number of cores (e.g. \"gpu\", "+
		"\"x16c\"). For example: -attrs=us-west-1b,gpu")

	maxDrift = flag.Duration("max_drift", 250*time.Millisecond, "specify "+
		"the maximum clock drift for the cluster. Clock drift is measured on all "+
		"node-to-node links and if any node notices it has clock drift in excess "+
		"of -max_drift, it will commit suicide.")

	// Regular expression for capturing data directory specifications.
	storesRE = regexp.MustCompile(`([^=]+)=([^,]+)(,|$)`)
)

// A CmdStart command starts nodes by joining the gossip network.
var CmdStart = &commander.Command{
	UsageLine: "start -gossip=host1:port1[,host2:port2...] " +
		"-stores=(ssd=<data-dir>|hdd=<data-dir>|mem=<capacity-in-bytes>)[,...]",
	Short: "start node by joining the gossip network",
	Long: fmt.Sprintf(`

Start Cockroach node by joining the gossip network and exporting key
ranges stored on physical device(s). The gossip network is joined by
contacting one or more well-known hosts specified by the -gossip
command line flag. Every node should be run with the same list of
bootstrap hosts to guarantee a connected network. An alternate
approach is to use a single host for -gossip and round-robin DNS.

Each node exports data from one or more physical devices. These
devices are specified via the -stores command line flag. This is a
comma-separated list of paths to storage directories or for in-memory
stores, the number of bytes. Although the paths should be specified to
correspond uniquely to physical devices, this requirement isn't
strictly enforced.

A node exports an HTTP API with the following endpoints:

  Health check:           /healthz
  Key-value REST:         %s
  Structured Schema REST: %s
`, kv.APIPrefix, structured.StructuredKeyPrefix),
	Run:  runStart,
	Flag: *flag.CommandLine,
}

type server struct {
	host           string
	mux            *http.ServeMux
	rpc            *rpc.Server
	gossip         *gossip.Gossip
	kvDB           kv.DB
	kvREST         *kv.RESTServer
	node           *Node
	admin          *adminServer
	status         *statusServer
	structuredDB   *structured.DB
	structuredREST *structured.RESTServer
	httpListener   *net.Listener // holds http endpoint information
}

// runStart starts the cockroach node using -stores as the list of
// storage devices ("stores") on this machine and -gossip as the list
// of "well-known" hosts used to join this node to the cockroach
// cluster via the gossip network.
func runStart(cmd *commander.Command, args []string) {
	glog.Info("Starting cockroach cluster")
	s, err := newServer()
	if err != nil {
		glog.Errorf("Failed to start Cockroach server: %v", err)
		return
	}

	// Create a new hybrid-logical clock using the internal clock.
	clock := hlc.NewHLClock(hlc.UnixNano)
	clock.SetMaxDrift(*maxDrift)

	// Init engines from -stores.
	engines, err := initEngines(*stores)
	if err != nil {
		glog.Errorf("Failed to initialize engines from -stores=%q: %v", *stores, err)
		return
	}
	if len(engines) == 0 {
		glog.Errorf("No valid engines specified after initializing from -stores=%q", *stores)
		return
	}

	err = s.start(clock, engines, false)
	defer s.stop()
	if err != nil {
		glog.Errorf("Cockroach server exited with error: %v", err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	// Block until one of the signals above is received.
	<-c
}

// parseAttributes parses a colon-separated list of strings,
// filtering empty strings (i.e. ",," will yield no attributes.
// Returns the list of strings as Attributes.
func parseAttributes(attrsStr string) storage.Attributes {
	var filtered []string
	for _, attr := range strings.Split(attrsStr, ":") {
		if len(attr) != 0 {
			filtered = append(filtered, attr)
		}
	}
	sort.Strings(filtered)
	return storage.Attributes(filtered)
}

// initEngines interprets the stores parameter to initialize a slice of
// storage.Engine objects.
func initEngines(stores string) ([]storage.Engine, error) {
	// Error if regexp doesn't match.
	storeSpecs := storesRE.FindAllStringSubmatch(stores, -1)
	if storeSpecs == nil || len(storeSpecs) == 0 {
		return nil, util.Errorf("invalid or empty engines specification %q", stores)
	}

	engines := []storage.Engine{}
	for _, store := range storeSpecs {
		if len(store) != 4 {
			return nil, util.Errorf("unable to parse attributes and path from store %q", store[0])
		}
		// There are two matches for each store specification: the colon-separated
		// list of attributes and the path.
		engine, err := initEngine(store[1], store[2])
		if err != nil {
			return nil, util.Errorf("unable to init engine for store %q: %v", store[0], err)
		}
		engines = append(engines, engine)
	}

	return engines, nil
}

// initEngine parses the store attributes as a colon-separated list
// and instantiates an engine based on the dir parameter. If dir parses
// to an integer, it's taken to mean an in-memory engine; otherwise,
// dir is treated as a path and a RocksDB engine is created.
func initEngine(attrsStr, path string) (storage.Engine, error) {
	attrs := parseAttributes(attrsStr)
	var engine storage.Engine
	if size, err := strconv.ParseUint(path, 10, 64); err == nil {
		if size == 0 {
			return nil, util.Errorf("unable to initialize an in-memory store with capacity 0")
		}
		engine = storage.NewInMem(attrs, int64(size))
	} else {
		engine, err = storage.NewRocksDB(attrs, path)
		if err != nil {
			return nil, util.Errorf("unable to init rocksdb with data dir %q: %v", path, err)
		}
	}

	return engine, nil
}

func newServer() (*server, error) {
	// Determine hostname in case it hasn't been specified in -rpc or -http.
	host, err := os.Hostname()
	if err != nil {
		host = "127.0.0.1"
	}

	// Resolve
	if strings.HasPrefix(*rpcAddr, ":") {
		*rpcAddr = host + *rpcAddr
	}
	addr, err := net.ResolveTCPAddr("tcp", *rpcAddr)
	if err != nil {
		return nil, util.Errorf("unable to resolve RPC address %q: %v", *rpcAddr, err)
	}

	s := &server{
		host: host,
		mux:  http.NewServeMux(),
		rpc:  rpc.NewServer(addr),
	}

	s.gossip = gossip.New()
	s.kvDB = kv.NewDB(s.gossip)
	s.kvREST = kv.NewRESTServer(s.kvDB)
	s.node = NewNode(s.kvDB, s.gossip)
	s.admin = newAdminServer(s.kvDB)
	s.status = newStatusServer(s.kvDB)
	s.structuredDB = structured.NewDB(s.kvDB)
	s.structuredREST = structured.NewRESTServer(s.structuredDB)

	return s, nil
}

// start runs the RPC and HTTP servers, starts the gossip instance (if
// selfBootstrap is true, uses the rpc server's address as the gossip
// bootstrap), and starts the node using the supplied engines slice.
func (s *server) start(clock *hlc.HLClock, engines []storage.Engine, selfBootstrap bool) error {
	s.rpc.Start() // bind RPC socket and launch goroutine.
	glog.Infof("Started RPC server at %s", s.rpc.Addr())

	// Handle self-bootstrapping case for a single node.
	if selfBootstrap {
		s.gossip.SetBootstrap([]net.Addr{s.rpc.Addr()})
	}
	s.gossip.Start(s.rpc)
	glog.Infoln("Started gossip instance")

	// Init the engines specified via command line flags if not supplied.
	if engines == nil {
		var err error
		engines, err = initEngines(*stores)
		if err != nil {
			return err
		}
	}

	// Init the node attributes from the -attrs command line flag.
	nodeAttrs := parseAttributes(*attrs)

	if err := s.node.start(s.rpc, clock, engines, nodeAttrs); err != nil {
		return err
	}
	glog.Infof("Initialized %d storage engine(s)", len(engines))

	s.initHTTP()
	if strings.HasPrefix(*httpAddr, ":") {
		*httpAddr = s.host + *httpAddr
	}
	ln, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		return util.Errorf("could not listen on %s: %s", *httpAddr, err)
	}
	// Obtaining the http end point listener is difficult using
	// http.ListenAndServe(), so we are storing it with the server
	s.httpListener = &ln
	glog.Infof("Starting HTTP server at %s", ln.Addr())
	go http.Serve(ln, s)
	return nil
}

func (s *server) initHTTP() {
	// TODO(shawn) pretty "/" landing page
	s.mux.HandleFunc(adminKeyPrefix+"healthz", s.admin.handleHealthz)

	// Status endpoints:
	s.mux.HandleFunc(statusKeyPrefix, s.status.handleStatus)
	s.mux.HandleFunc(statusNodesKeyPrefix, s.status.handleNodeStatus)
	s.mux.HandleFunc(statusGossipKeyPrefix, s.status.handleGossipStatus)
	s.mux.HandleFunc(statusStoresKeyPrefix, s.status.handleStoresStatus)
	s.mux.HandleFunc(statusTransactionsKeyPrefix, s.status.handleTransactionStatus)
	s.mux.HandleFunc(statusLocalKeyPrefix, s.status.handleLocalStatus)

	s.mux.HandleFunc(zoneKeyPrefix, s.admin.handleZoneAction)
	s.mux.HandleFunc(kv.APIPrefix, s.kvREST.HandleAction)
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
