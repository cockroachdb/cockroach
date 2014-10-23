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
	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	rpcAddr  = flag.String("rpc", ":0", "host:port to bind for RPC traffic; 0 to pick unused port")
	httpAddr = flag.String("http", ":8080", "host:port to bind for HTTP traffic; 0 to pick unused port")

	certDir = flag.String("certs", "", "directory containing RSA key and x509 certs")

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

	maxOffset = flag.Duration("max_offset", 250*time.Millisecond, "specify "+
		"the maximum clock offset for the cluster. Clock offset is measured on all "+
		"node-to-node links and if any node notices it has clock offset in excess "+
		"of -max_drift, it will commit suicide. Setting this value too high may "+
		"decrease transaction performance in the presence of contention.")

	bootstrapOnly = flag.Bool("bootstrap_only", false, "specify --bootstrap_only "+
		"to avoid starting the server after bootstrapping with the init command.")

	// Regular expression for capturing data directory specifications.
	storesRE = regexp.MustCompile(`([^=]+)=([^,]+)(,|$)`)
)

var cmdStartLongDescription = `

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
  Key-value REST:         ` + kv.RESTPrefix + `
  Structured Schema REST: ` + structured.StructuredKeyPrefix

// A CmdInit command initializes a new Cockroach cluster.
var CmdInit = &commander.Command{
	UsageLine: "init -gossip=host1:port1[,host2:port2...] " +
		"-certs=<cert-dir>" +
		"-stores=(ssd=<data-dir>,hdd|7200rpm=<data-dir>,mem=<capacity-in-bytes>)[,...]",
	Short: "init new Cockroach cluster and start server",
	Long: `
Initialize a new Cockroach cluster on this node using the first
directory specified in the -stores command line flag as the only
replica of the first range.

For example:

  cockroach init -gossip=host1:port1,host2:port2 -stores=ssd=/mnt/ssd1,ssd=/mnt/ssd2

If any specified store is already part of a pre-existing cluster, the
bootstrap will fail.

After bootstrap initialization: ` + cmdStartLongDescription,
	Run:  runInit,
	Flag: *flag.CommandLine,
}

func runInit(cmd *commander.Command, args []string) {
	// Initialize the engine based on the first argument and
	// then verify it's not in-memory.
	engines, err := initEngines(*stores)
	if err != nil {
		log.Errorf("Failed to initialize engines from -stores=%s: %v", *stores, err)
		return
	}
	if len(engines) == 0 {
		log.Errorf("No valid engines specified after initializing from -stores=%s", *stores)
		return
	}
	e := engines[0]
	if _, ok := e.(*engine.InMem); ok {
		log.Errorf("Cannot initialize a cluster using an in-memory store")
		return
	}
	// Generate a new UUID for cluster ID and bootstrap the cluster.
	clusterID := uuid.New()
	localDB, err := BootstrapCluster(clusterID, e)
	if err != nil {
		log.Errorf("Failed to bootstrap cluster: %v", err)
		return
	}
	// Close localDB and bootstrap engine.
	localDB.Close()
	e.Stop()

	fmt.Printf("Cockroach cluster %s has been initialized\n", clusterID)
	if *bootstrapOnly {
		fmt.Printf("To start the cluster, run \"cockroach start\"\n")
		return
	}
	runStart(cmd, args)
}

// A CmdStart command starts nodes by joining the gossip network.
var CmdStart = &commander.Command{
	UsageLine: "start -gossip=host1:port1[,host2:port2...] " +
		"-certs=<cert-dir>" +
		"-stores=(ssd=<data-dir>,hdd|7200rpm=<data-dir>|mem=<capacity-in-bytes>)[,...]",
	Short: "start node by joining the gossip network",
	Long:  cmdStartLongDescription,
	Run:   runStart,
	Flag:  *flag.CommandLine,
}

type server struct {
	host           string
	mux            *http.ServeMux
	clock          *hlc.Clock
	rpc            *rpc.Server
	gossip         *gossip.Gossip
	kvDB           *kv.DB
	kvREST         *kv.Server
	node           *Node
	admin          *adminServer
	status         *statusServer
	structuredDB   structured.DB
	structuredREST *structured.RESTServer
	httpListener   *net.Listener // holds http endpoint information
}

// runStart starts the cockroach node using -stores as the list of
// storage devices ("stores") on this machine and -gossip as the list
// of "well-known" hosts used to join this node to the cockroach
// cluster via the gossip network.
func runStart(cmd *commander.Command, args []string) {
	log.Info("Starting cockroach cluster")
	s, err := newServer()
	if err != nil {
		log.Errorf("Failed to start Cockroach server: %v", err)
		return
	}

	// Init engines from -stores.
	engines, err := initEngines(*stores)
	if err != nil {
		log.Errorf("Failed to initialize engines from -stores=%s: %v", *stores, err)
		return
	}
	if len(engines) == 0 {
		log.Errorf("No valid engines specified after initializing from -stores=%s", *stores)
		return
	}

	err = s.start(engines, false)
	defer s.stop()
	if err != nil {
		log.Errorf("Cockroach server exited with error: %v", err)
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
func parseAttributes(attrsStr string) proto.Attributes {
	var filtered []string
	for _, attr := range strings.Split(attrsStr, ":") {
		if len(attr) != 0 {
			filtered = append(filtered, attr)
		}
	}
	sort.Strings(filtered)
	return proto.Attributes{Attrs: filtered}
}

// initEngines interprets the stores parameter to initialize a slice of
// engine.Engine objects.
func initEngines(stores string) ([]engine.Engine, error) {
	// Error if regexp doesn't match.
	storeSpecs := storesRE.FindAllStringSubmatch(stores, -1)
	if storeSpecs == nil || len(storeSpecs) == 0 {
		return nil, util.Errorf("invalid or empty engines specification %q", stores)
	}

	engines := []engine.Engine{}
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
func initEngine(attrsStr, path string) (engine.Engine, error) {
	attrs := parseAttributes(attrsStr)
	if size, err := strconv.ParseUint(path, 10, 64); err == nil {
		if size == 0 {
			return nil, util.Errorf("unable to initialize an in-memory store with capacity 0")
		}
		return engine.NewInMem(attrs, int64(size)), nil
		// TODO(spencer): should be using rocksdb for in-memory stores and
		// relegate the InMem engine to usage only from unittests.
	}
	return engine.NewRocksDB(attrs, path), nil
}

func newServer() (*server, error) {
	// Determine hostname in case it hasn't been specified in -rpc or -http.
	host, err := os.Hostname()
	if err != nil {
		host = "127.0.0.1"
	}

	// If the specified rpc address includes no host component, use the hostname.
	if strings.HasPrefix(*rpcAddr, ":") {
		*rpcAddr = host + *rpcAddr
	}
	_, err = net.ResolveTCPAddr("tcp", *rpcAddr)
	if err != nil {
		return nil, util.Errorf("unable to resolve RPC address %q: %v", *rpcAddr, err)
	}

	var tlsConfig *rpc.TLSConfig
	if *certDir == "" {
		tlsConfig = rpc.LoadInsecureTLSConfig()
	} else {
		var err error
		if tlsConfig, err = rpc.LoadTLSConfig(*certDir); err != nil {
			return nil, util.Errorf("unable to load TLS config: %v", err)
		}
	}

	s := &server{
		host:  host,
		mux:   http.NewServeMux(),
		clock: hlc.NewClock(hlc.UnixNano),
	}
	s.clock.SetMaxOffset(*maxOffset)

	rpcContext := rpc.NewContext(s.clock, tlsConfig)
	go rpcContext.RemoteClocks.MonitorRemoteOffsets()

	s.rpc = rpc.NewServer(util.MakeRawAddr("tcp", *rpcAddr), rpcContext)
	s.gossip = gossip.New(rpcContext)
	s.kvDB = kv.NewDB(kv.NewDistKV(s.gossip), s.clock)
	s.kvREST = kv.NewRESTServer(s.kvDB)
	s.node = NewNode(s.kvDB, s.gossip)
	s.admin = newAdminServer(s.kvDB)
	s.status = newStatusServer(s.kvDB, s.gossip)
	s.structuredDB = structured.NewDB(s.kvDB)
	s.structuredREST = structured.NewRESTServer(s.structuredDB)

	return s, nil
}

// start runs the RPC and HTTP servers, starts the gossip instance (if
// selfBootstrap is true, uses the rpc server's address as the gossip
// bootstrap), and starts the node using the supplied engines slice.
func (s *server) start(engines []engine.Engine, selfBootstrap bool) error {
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

	if err := s.node.start(s.rpc, s.clock, engines, nodeAttrs); err != nil {
		return err
	}
	log.Infof("Initialized %d storage engine(s)", len(engines))

	s.initHTTP()
	if strings.HasPrefix(*httpAddr, ":") {
		*httpAddr = s.host + *httpAddr
	}
	ln, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		return util.Errorf("could not listen on %s: %s", *httpAddr, err)
	}
	// Obtaining the http end point listener is difficult using
	// http.ListenAndServe(), so we are storing it with the server.
	s.httpListener = &ln
	log.Infof("Starting HTTP server at %s", ln.Addr())
	go http.Serve(ln, s)
	return nil
}

func (s *server) initHTTP() {
	// TODO(shawn) pretty "/" landing page

	// Admin handlers.
	s.admin.RegisterHandlers(s.mux)

	// Status endpoints:
	s.status.RegisterHandlers(s.mux)

	s.mux.Handle(kv.RESTPrefix, s.kvREST)
	s.mux.Handle(structured.StructuredKeyPrefix, s.structuredREST)
}

func (s *server) stop() {
	s.node.stop()
	s.gossip.Stop()
	s.rpc.Close()
	s.kvDB.Close()
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

// GetContentType pulls out the content type from a request header
// it ignores every value after the first semicolon
func GetContentType(request *http.Request) string {
	contentType := request.Header.Get("Content-Type")
	semicolonIndex := strings.Index(contentType, ";")
	if semicolonIndex > -1 {
		contentType = contentType[0:semicolonIndex]
	}
	return contentType
}

// TODO(bram): Add function to marshalBody
//   func marshalBody(req *http.Request, msg gogoproto.Message) ([]byte, error)
