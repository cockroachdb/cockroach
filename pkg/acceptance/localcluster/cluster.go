// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package localcluster

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"go/build"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/gogo/protobuf/proto"
	// Import postgres driver.
	_ "github.com/lib/pq"
)

func repoRoot() string {
	root, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		panic(fmt.Sprintf("must run from within the cockroach repository: %s", err))
	}
	return root.Dir
}

// SourceBinary returns the path of the cockroach binary that was built with the
// local source.
func SourceBinary() string {
	return filepath.Join(repoRoot(), "cockroach")
}

const listeningURLFile = "cockroachdb-url"

// IsUnavailableError returns true iff the error corresponds to a GRPC
// connection unavailable error.
func IsUnavailableError(err error) bool {
	return strings.Contains(err.Error(), "grpc: the connection is unavailable")
}

// A ClusterConfig holds the configuration for a Cluster.
type ClusterConfig struct {
	Ephemeral   bool               // when true, wipe DataDir on Close()
	Binary      string             // path to cockroach, defaults go <cockroach_repo>/cockroach
	AllNodeArgs []string           // args to pass to ./cockroach on all nodes
	NumNodes    int                // number of nodes in the cluster
	DataDir     string             // node i will use storage DataDir/<i>
	LogDir      string             // when empty, node i defaults to DataDir/<i>/logs
	PerNodeCfg  map[int]NodeConfig // optional map of nodeIndex -> configuration
	DB          string             // database to configure DB connection for
	NumWorkers  int                // SetMaxOpenConns to use for DB connection
	NoWait      bool               // if set, return from Start before cluster ready
}

// NodeConfig is a configuration for a node in a Cluster. Options with the zero
// value are typically populated from the corresponding Cluster's ClusterConfig.
type NodeConfig struct {
	Binary            string   // when specified, overrides the node's binary
	DataDir           string   // when specified, overrides the node's data dir
	LogDir            string   // when specified, overrides the node's log dir
	Addr              string   // listening host, defaults to 127.0.0.1
	ExtraArgs         []string // extra arguments for ./cockroach start
	ExtraEnv          []string // environment variables in format key=value
	RPCPort, HTTPPort int      // zero for auto-assign
	DB                string   // see ClusterConfig
	NumWorkers        int      // see ClusterConfig
}

// MakePerNodeFixedPortsCfg makes a PerNodeCfg map of the given number of nodes
// with odd ports starting at 26257 for the RPC endpoint, and even points for
// the ui.
func MakePerNodeFixedPortsCfg(numNodes int) map[int]NodeConfig {
	perNodeCfg := make(map[int]NodeConfig)

	for i := 0; i < numNodes; i++ {
		perNodeCfg[i] = NodeConfig{
			RPCPort:  26257 + 2*i,
			HTTPPort: 26258 + 2*i,
		}
	}

	return perNodeCfg
}

// Cluster holds the state for a local cluster, providing methods for common
// operations, access to the underlying nodes and per-node KV and SQL clients.
type Cluster struct {
	Cfg     ClusterConfig
	seq     *seqGen
	Nodes   []*Node
	stopper *stop.Stopper
	started time.Time
}

type seqGen int32

func (s *seqGen) Next() int32 {
	return atomic.AddInt32((*int32)(s), 1)
}

// New creates a Cluster with the given configuration.
func New(cfg ClusterConfig) *Cluster {
	if cfg.Binary == "" {
		cfg.Binary = SourceBinary()
	}
	return &Cluster{
		Cfg:     cfg,
		seq:     new(seqGen),
		stopper: stop.NewStopper(),
	}
}

// Start starts a cluster. The numWorkers parameter controls the SQL connection
// settings to avoid unnecessary connection creation. The allNodeArgs parameter
// can be used to pass extra arguments to every node. The perNodeArgs parameter
// can be used to pass extra arguments to an individual node. If not nil, its
// size must equal the number of nodes.
func (c *Cluster) Start(ctx context.Context) {
	c.started = timeutil.Now()

	chs := make([]<-chan error, c.Cfg.NumNodes)
	for i := 0; i < c.Cfg.NumNodes; i++ {
		cfg := c.Cfg.PerNodeCfg[i] // zero value is ok
		if cfg.Binary == "" {
			cfg.Binary = c.Cfg.Binary
		}
		if cfg.DataDir == "" {
			cfg.DataDir = filepath.Join(c.Cfg.DataDir, fmt.Sprintf("%d", i+1))
		}
		if cfg.LogDir == "" && c.Cfg.LogDir != "" {
			cfg.LogDir = filepath.Join(c.Cfg.LogDir, fmt.Sprintf("%d", i+1))
		}
		if cfg.Addr == "" {
			cfg.Addr = "127.0.0.1"
		}
		if cfg.DB == "" {
			cfg.DB = c.Cfg.DB
		}
		if cfg.NumWorkers == 0 {
			cfg.NumWorkers = c.Cfg.NumWorkers
		}
		cfg.ExtraArgs = append(append([]string(nil), c.Cfg.AllNodeArgs...), cfg.ExtraArgs...)
		var node *Node
		node, chs[i] = c.makeNode(ctx, i, cfg)
		c.Nodes = append(c.Nodes, node)
		if i == 0 && cfg.RPCPort == 0 && c.Cfg.NumNodes > 1 {
			// The first node must know its RPCPort or we can't possibly tell
			// the other nodes the correct one to go to.
			//
			// Note: we can't set up a cluster first and clone it for each test,
			// because all ports change so the cluster won't come together.
			// Luckily, it takes only ~2 seconds from zero to a replicated 4
			// node cluster.
			if err := <-chs[0]; err != nil {
				log.Fatalf(ctx, "while starting first node: %s", err)
			}
			ch := make(chan error)
			close(ch)
			chs[0] = ch
		}
	}

	if !c.Cfg.NoWait {
		for i := range chs {
			if err := <-chs[i]; err != nil {
				log.Fatalf(ctx, "node %d: %s", i+1, err)
			}
		}
	}

	log.Infof(context.Background(), "started %.3fs", timeutil.Since(c.started).Seconds())

	if c.Cfg.NumNodes > 1 || !c.Cfg.NoWait {
		c.waitForFullReplication()
	} else {
		// NB: This is useful for TestRapidRestarts.
		log.Infof(ctx, "not waiting for initial replication")
	}
}

// Close stops the cluster, killing all of the nodes.
func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		n.Kill()
	}
	c.stopper.Stop(context.Background())
	if c.Cfg.Ephemeral {
		_ = os.RemoveAll(c.Cfg.DataDir)
	}
}

func (c *Cluster) joins() []string {
	type addrAndSeq struct {
		addr string
		seq  int32
	}

	var joins []addrAndSeq
	for _, node := range c.Nodes {
		advertAddr := node.AdvertiseAddr()
		if advertAddr != "" {
			joins = append(joins, addrAndSeq{
				addr: advertAddr,
				seq:  atomic.LoadInt32(&node.startSeq),
			})
		}
	}
	sort.Slice(joins, func(i, j int) bool {
		return joins[i].seq < joins[j].seq
	})

	if len(joins) == 0 {
		return nil
	}

	// Return the node with the smallest startSeq, i.e. the node that was
	// started first. This is the node that might have no --join flag set, and
	// we must point the other nodes at it, and *only* at it (or the other nodes
	// may connect sufficiently and never bother to talk to this node).
	return []string{joins[0].addr}
}

// IPAddr returns the IP address of the specified node.
func (c *Cluster) IPAddr(nodeIdx int) string {
	return c.Nodes[nodeIdx].IPAddr()
}

// RPCPort returns the RPC port of the specified node. Returns zero if unknown.
func (c *Cluster) RPCPort(nodeIdx int) string {
	return c.Nodes[nodeIdx].RPCPort()
}

func (c *Cluster) makeNode(ctx context.Context, nodeIdx int, cfg NodeConfig) (*Node, <-chan error) {
	baseCtx := &base.Config{
		User:     security.NodeUserName(),
		Insecure: true,
	}
	rpcCtx := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Config:     baseCtx,
		Clock:      hlc.NewClock(hlc.UnixNano, 0),
		Stopper:    c.stopper,
		Settings:   cluster.MakeTestingClusterSettings(),
	})

	n := &Node{
		Cfg:    cfg,
		rpcCtx: rpcCtx,
		seq:    c.seq,
	}

	args := []string{
		cfg.Binary,
		"start",
		"--insecure",
		// Although --host/--port are deprecated, we cannot yet replace
		// this here by --listen-addr/--listen-port, because
		// TestVersionUpgrade will also try old binaries.
		fmt.Sprintf("--host=%s", n.IPAddr()),
		fmt.Sprintf("--port=%d", cfg.RPCPort),
		fmt.Sprintf("--http-port=%d", cfg.HTTPPort),
		fmt.Sprintf("--store=%s", cfg.DataDir),
		fmt.Sprintf("--listening-url-file=%s", n.listeningURLFile()),
		"--cache=256MiB",
	}

	if n.Cfg.LogDir != "" {
		args = append(args, fmt.Sprintf("--log-dir=%s", n.Cfg.LogDir))
	}

	n.Cfg.ExtraArgs = append(args, cfg.ExtraArgs...)

	if err := os.MkdirAll(n.logDir(), 0755); err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}

	joins := c.joins()
	if nodeIdx > 0 && len(joins) == 0 {
		ch := make(chan error, 1)
		ch <- errors.Errorf("node %d started without join flags", nodeIdx+1)
		return nil, ch
	}
	ch := n.StartAsync(ctx, joins...)
	return n, ch
}

// waitForFullReplication waits for the cluster to be fully replicated.
func (c *Cluster) waitForFullReplication() {
	for i := 1; true; i++ {
		done, detail := c.isReplicated()
		if (done && i >= 50) || (i%50) == 0 {
			fmt.Print(detail)
			log.Infof(context.Background(), "waiting for replication")
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Infof(context.Background(), "replicated %.3fs", timeutil.Since(c.started).Seconds())
}

func (c *Cluster) isReplicated() (bool, string) {
	db := c.Nodes[0].DB()
	rows, err := db.Query(`SELECT range_id, start_key, end_key, array_length(replicas, 1) FROM crdb_internal.ranges`)
	if err != nil {
		// Versions <= 1.1 do not contain the crdb_internal table, which is what's used
		// to determine whether a cluster has up-replicated. This is relevant for the
		// version upgrade acceptance test. Just skip the replication check for this case.
		if testutils.IsError(err, "(table|relation) \"crdb_internal.ranges\" does not exist") {
			return true, ""
		}
		log.Fatalf(context.Background(), "%v", err)
	}
	defer rows.Close()

	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	done := true
	for rows.Next() {
		var rangeID int64
		var startKey, endKey roachpb.Key
		var numReplicas int
		if err := rows.Scan(&rangeID, &startKey, &endKey, &numReplicas); err != nil {
			log.Fatalf(context.Background(), "unable to scan range replicas: %s", err)
		}
		fmt.Fprintf(tw, "\t%s\t%s\t[%d]\t%d\n", startKey, endKey, rangeID, numReplicas)
		// This check is coarse since it doesn't know the real configuration.
		// Assume all is well when there are 3+ replicas, or if there are as
		// many replicas as there are nodes.
		if numReplicas < 3 && numReplicas != len(c.Nodes) {
			done = false
		}
	}
	_ = tw.Flush()
	return done, buf.String()
}

// UpdateZoneConfig updates the default zone config for the cluster.
func (c *Cluster) UpdateZoneConfig(rangeMinBytes, rangeMaxBytes int64) {
	zone := zonepb.DefaultZoneConfig()
	zone.RangeMinBytes = proto.Int64(rangeMinBytes)
	zone.RangeMaxBytes = proto.Int64(rangeMaxBytes)

	buf, err := protoutil.Marshal(&zone)
	if err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}
	_, err = c.Nodes[0].DB().Exec(`UPSERT INTO system.zones (id, config) VALUES (0, $1)`, buf)
	if err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}
}

// Split splits the range containing the split key at the specified split key.
func (c *Cluster) Split(nodeIdx int, splitKey roachpb.Key) error {
	return errors.Errorf("Split is unimplemented and should be re-implemented using SQL")
}

// TransferLease transfers the lease for the range containing key to a random
// alive node in the range.
func (c *Cluster) TransferLease(nodeIdx int, r *rand.Rand, key roachpb.Key) (bool, error) {
	return false, errors.Errorf("TransferLease is unimplemented and should be re-implemented using SQL")
}

// RandNode returns the index of a random alive node.
func (c *Cluster) RandNode(f func(int) int) int {
	for {
		i := f(len(c.Nodes))
		if c.Nodes[i].Alive() {
			return i
		}
	}
}

// Node holds the state for a single node in a local cluster and provides
// methods for starting, pausing, resuming and stopping the node.
type Node struct {
	Cfg    NodeConfig
	rpcCtx *rpc.Context
	seq    *seqGen

	startSeq int32        // updated atomically on start, nonzero while running
	waitErr  atomic.Value // last `error`` returned from cmd.Wait()

	syncutil.Mutex
	notRunning     chan struct{}
	cmd            *exec.Cmd
	rpcPort, pgURL string // legacy: remove once 1.0.x is no longer tested
	db             *gosql.DB
	statusClient   serverpb.StatusClient
}

// RPCPort returns the RPC + Postgres port.
func (n *Node) RPCPort() string {
	if s := func() string {
		// Legacy case. To be removed.
		n.Lock()
		defer n.Unlock()
		if n.rpcPort != "" && n.rpcPort != "0" {
			return n.rpcPort
		}
		return ""
	}(); s != "" {
		return s
	}

	advAddr := readFileOrEmpty(n.advertiseAddrFile())
	if advAddr == "" {
		return ""
	}
	_, p, _ := net.SplitHostPort(advAddr)
	return p
}

// RPCAddr returns the RPC + Postgres address, or an empty string if it is not known
// (for instance since the node is down).
func (n *Node) RPCAddr() string {
	port := n.RPCPort()
	if port == "" || port == "0" {
		return ""
	}
	return net.JoinHostPort(n.IPAddr(), port)
}

// HTTPAddr returns the HTTP address (once known).
func (n *Node) HTTPAddr() string {
	return readFileOrEmpty(n.httpAddrFile())
}

// PGUrl returns the postgres connection string (may be empty until known).
func (n *Node) PGUrl() string {
	n.Lock()
	defer n.Unlock()
	return n.pgURL
}

// Alive returns true if the node is alive (i.e. not stopped). Note that a
// paused node is considered alive.
func (n *Node) Alive() bool {
	n.Lock()
	defer n.Unlock()
	return n.cmd != nil
}

// StatusClient returns a StatusClient set up to talk to this node.
func (n *Node) StatusClient() serverpb.StatusClient {
	n.Lock()
	existingClient := n.statusClient
	n.Unlock()

	if existingClient != nil {
		return existingClient
	}

	conn, _, err := n.rpcCtx.GRPCDialRaw(n.RPCAddr())
	if err != nil {
		log.Fatalf(context.Background(), "failed to initialize status client: %s", err)
	}
	return serverpb.NewStatusClient(conn)
}

func (n *Node) logDir() string {
	if n.Cfg.LogDir == "" {
		return filepath.Join(n.Cfg.DataDir, "logs")
	}
	return n.Cfg.LogDir
}

func (n *Node) listeningURLFile() string {
	return filepath.Join(n.Cfg.DataDir, listeningURLFile)
}

// Start starts a node.
func (n *Node) Start(ctx context.Context, joins ...string) {
	if err := <-n.StartAsync(ctx, joins...); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

func (n *Node) setNotRunningLocked(waitErr *exec.ExitError) {
	_ = os.Remove(n.listeningURLFile())
	_ = os.Remove(n.advertiseAddrFile())
	_ = os.Remove(n.httpAddrFile())
	if n.notRunning != nil {
		close(n.notRunning)
	}
	n.notRunning = make(chan struct{})
	n.db = nil
	n.statusClient = nil
	n.cmd = nil
	n.rpcPort = ""
	n.waitErr.Store(waitErr)
	atomic.StoreInt32(&n.startSeq, 0)
}

func (n *Node) startAsyncInnerLocked(ctx context.Context, joins ...string) error {
	n.setNotRunningLocked(nil)

	args := append([]string(nil), n.Cfg.ExtraArgs[1:]...)
	for _, join := range joins {
		args = append(args, "--join", join)
	}
	n.cmd = exec.Command(n.Cfg.ExtraArgs[0], args...)
	n.cmd.Env = os.Environ()
	n.cmd.Env = append(n.cmd.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms") // speed up rebalancing
	n.cmd.Env = append(n.cmd.Env, n.Cfg.ExtraEnv...)

	atomic.StoreInt32(&n.startSeq, n.seq.Next())

	_ = os.MkdirAll(n.logDir(), 0755)

	stdoutPath := filepath.Join(n.logDir(), "stdout")
	stdout, err := os.OpenFile(stdoutPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrapf(err, "unable to open file %s", stdoutPath)
	}
	// This causes the "node startup header" to be printed to stdout, which is
	// helpful and not too noisy.
	n.cmd.Stdout = io.MultiWriter(stdout, os.Stdout)

	stderrPath := filepath.Join(n.logDir(), "stderr")
	stderr, err := os.OpenFile(stderrPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrapf(err, "unable to open file %s", stderrPath)
	}
	n.cmd.Stderr = stderr

	if n.Cfg.RPCPort > 0 {
		n.rpcPort = fmt.Sprintf("%d", n.Cfg.RPCPort)
	}

	if err := n.cmd.Start(); err != nil {
		if err := stdout.Close(); err != nil {
			log.Warningf(ctx, "%v", err)
		}
		if err := stderr.Close(); err != nil {
			log.Warningf(ctx, "%v", err)
		}
		return errors.Wrapf(err, "running %s %v", n.cmd.Path, n.cmd.Args)
	}

	log.Infof(ctx, "process %d starting: %s", n.cmd.Process.Pid, n.cmd.Args)

	go func(cmd *exec.Cmd) {
		waitErr := cmd.Wait()
		if waitErr != nil {
			log.Warningf(ctx, "%v", waitErr)
		}
		if err := stdout.Close(); err != nil {
			log.Warningf(ctx, "%v", err)
		}
		if err := stderr.Close(); err != nil {
			log.Warningf(ctx, "%v", err)
		}

		log.Infof(ctx, "process %d: %s", cmd.Process.Pid, cmd.ProcessState)

		var execErr *exec.ExitError
		_ = errors.As(waitErr, &execErr)
		n.Lock()
		n.setNotRunningLocked(execErr)
		n.Unlock()
	}(n.cmd)

	return nil
}

// StartAsync starts a node asynchronously. It returns a buffered channel that
// receives either an error, or, once the node has started up and is fully
// functional, `nil`.
//
// StartAsync is a no-op if the node is already running.
func (n *Node) StartAsync(ctx context.Context, joins ...string) <-chan error {
	ch := make(chan error, 1)

	if err := func() error {
		n.Lock()
		defer n.Unlock()
		if n.cmd != nil {
			return errors.New("server is already running")
		}
		return n.startAsyncInnerLocked(ctx, joins...)
	}(); err != nil {
		ch <- err
		return ch
	}

	go func() {
		// If the node does not become live within a minute, something is wrong and
		// it's better not to hang indefinitely.
		ch <- n.waitUntilLive(time.Minute)
	}()

	return ch
}

func portFromURL(rawURL string) (string, *url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, err
	}

	_, port, err := net.SplitHostPort(u.Host)
	return port, u, err
}

func makeDB(url string, numWorkers int, dbName string) *gosql.DB {
	conn, err := gosql.Open("postgres", url)
	if err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}
	if numWorkers == 0 {
		numWorkers = 1
	}
	conn.SetMaxOpenConns(numWorkers)
	conn.SetMaxIdleConns(numWorkers)
	return conn
}

func (n *Node) advertiseAddrFile() string {
	return filepath.Join(n.Cfg.DataDir, "cockroach.advertise-addr")
}

func (n *Node) httpAddrFile() string {
	return filepath.Join(n.Cfg.DataDir, "cockroach.http-addr")
}

func readFileOrEmpty(f string) string {
	c, err := ioutil.ReadFile(f)
	if err != nil {
		if !oserror.IsNotExist(err) {
			panic(err)
		}
		return ""
	}
	return string(c)
}

// AdvertiseAddr returns the Node's AdvertiseAddr or empty if none is available.
func (n *Node) AdvertiseAddr() (s string) {
	addr := readFileOrEmpty(n.advertiseAddrFile())
	if addr != "" {
		return addr
	}
	// The below is part of the workaround for nodes at v1.0 which don't
	// write the file above, explained in more detail in StartAsync().
	if port := n.RPCPort(); port != "" {
		return net.JoinHostPort(n.IPAddr(), n.RPCPort())
	}
	return addr
}

func (n *Node) waitUntilLive(dur time.Duration) error {
	ctx := context.Background()
	closer := make(chan struct{})
	defer time.AfterFunc(dur, func() { close(closer) }).Stop()
	opts := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Multiplier:     2,
		Closer:         closer,
	}
	for r := retry.Start(opts); r.Next(); {
		var pid int
		n.Lock()
		if n.cmd != nil {
			pid = n.cmd.Process.Pid
		}
		n.Unlock()
		if pid == 0 {
			log.Info(ctx, "process already quit")
			return nil
		}

		urlBytes, err := ioutil.ReadFile(n.listeningURLFile())
		if err != nil {
			log.Infof(ctx, "%v", err)
			continue
		}

		var pgURL *url.URL
		_, pgURL, err = portFromURL(string(urlBytes))
		if err != nil {
			log.Infof(ctx, "%v", err)
			continue
		}

		if n.Cfg.RPCPort == 0 {
			n.Lock()
			n.rpcPort = pgURL.Port()
			n.Unlock()
		}

		pgURL.Path = n.Cfg.DB
		n.Lock()
		n.pgURL = pgURL.String()
		n.Unlock()

		var uiURL *url.URL

		defer func() {
			log.Infof(ctx, "process %d started (db: %s ui: %s)", pid, pgURL, uiURL)
		}()

		// We're basically running, but (at least) the decommissioning test sometimes starts
		// up servers that can already be draining when they get here. For that reason, leave
		// the admin port undefined if we don't manage to get it.
		//
		// This can be improved by making the below code run opportunistically whenever the
		// http port is required but isn't initialized yet.
		n.Lock()
		n.db = makeDB(n.pgURL, n.Cfg.NumWorkers, n.Cfg.DB)
		n.Unlock()

		{
			var uiStr string
			if err := n.db.QueryRow(
				`SELECT value FROM crdb_internal.node_runtime_info WHERE component='UI' AND field = 'URL'`,
			).Scan(&uiStr); err != nil {
				log.Infof(ctx, "%v", err)
				return nil
			}

			_, uiURL, err = portFromURL(uiStr)
			if err != nil {
				log.Infof(ctx, "%v", err)
				// TODO(tschottdorf): see above.
			}
		}
		return nil
	}
	return errors.Errorf("node %+v was unable to join cluster within %s", n.Cfg, dur)
}

// Kill stops a node abruptly by sending it SIGKILL.
func (n *Node) Kill() {
	n.Signal(os.Kill)
	// Wait for the process to have been cleaned up (or a call to Start() could
	// turn into an unintended no-op).
	for ok := false; !ok; {
		n.Lock()
		ok = n.cmd == nil
		n.Unlock()
	}
}

// IPAddr returns the node's listening address (for ui, inter-node, cli, and
// Postgres alike).
func (n *Node) IPAddr() string {
	return n.Cfg.Addr
}

// DB returns a Postgres connection set up to talk to the node.
func (n *Node) DB() *gosql.DB {
	n.Lock()
	defer n.Unlock()
	return n.db
}

// Signal sends the given signal to the process. It is a no-op if the process is
// not running.
func (n *Node) Signal(s os.Signal) {
	n.Lock()
	defer n.Unlock()
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	if err := n.cmd.Process.Signal(s); err != nil {
		log.Warningf(context.Background(), "%v", err)
	}
}

// Wait waits for the process to terminate and returns its process' Wait(). This
// is nil if the process terminated with a zero exit code.
func (n *Node) Wait() *exec.ExitError {
	n.Lock()
	ch := n.notRunning
	n.Unlock()
	if ch == nil {
		log.Warning(context.Background(), "(*Node).Wait called when node was not running")
		return nil
	}
	<-ch
	ee, _ := n.waitErr.Load().(*exec.ExitError)
	return ee
}

// Silence unused warning.
var _ = (*Node)(nil).Wait
