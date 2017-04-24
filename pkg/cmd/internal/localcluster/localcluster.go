// Copyright 2016 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package localcluster

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	// Import postgres driver.
	_ "github.com/lib/pq"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const basePort = 26257
const dataDir = "cockroach-data"

// CockroachBin is the path to the cockroach binary.
var CockroachBin = func() string {
	bin := "./cockroach"
	if _, err := os.Stat(bin); os.IsNotExist(err) {
		bin = "cockroach"
	} else if err != nil {
		panic(err)
	}
	return bin
}()

// IsUnavailableError returns true iff the error corresponds to a GRPC
// connection unavailable error.
func IsUnavailableError(err error) bool {
	return strings.Contains(err.Error(), "grpc: the connection is unavailable")
}

// Cluster holds the state for a local cluster, providing methods for common
// operations, access to the underlying nodes and per-node KV and SQL clients.
type Cluster struct {
	rpcCtx        *rpc.Context
	Nodes         []*Node
	Clients       []*client.DB
	Status        []serverpb.StatusClient
	DB            []*gosql.DB
	separateAddrs bool
	stopper       *stop.Stopper
	started       time.Time
}

// New creates a cluster of size nodes.
// separateAddrs controls whether all the nodes use the same localhost IP
// address (127.0.0.1) or separate addresses (e.g. 127.1.1.1, 127.1.1.2, etc).
func New(size int, separateAddrs bool) *Cluster {
	return &Cluster{
		Nodes:         make([]*Node, size),
		Clients:       make([]*client.DB, size),
		Status:        make([]serverpb.StatusClient, size),
		DB:            make([]*gosql.DB, size),
		separateAddrs: separateAddrs,
		stopper:       stop.NewStopper(),
	}
}

// Start starts a cluster. The numWorkers parameter controls the SQL connection
// settings to avoid unnecessary connection creation. The allNodeArgs parameter
// can be used to pass extra arguments to every node. The perNodeArgs parameter
// can be used to pass extra arguments to an individual node. If not nil, its
// size must equal the number of nodes.
func (c *Cluster) Start(
	db string,
	numWorkers int,
	binary string,
	allNodeArgs []string,
	perNodeArgs, perNodeEnv map[int][]string,
) {
	c.started = timeutil.Now()

	baseCtx := &base.Config{
		User:     security.NodeUser,
		Insecure: true,
	}
	c.rpcCtx = rpc.NewContext(log.AmbientContext{}, baseCtx,
		hlc.NewClock(hlc.UnixNano, 0), c.stopper)

	if perNodeArgs != nil && len(perNodeArgs) != len(c.Nodes) {
		panic(fmt.Sprintf("there are %d nodes, but perNodeArgs' length is %d",
			len(c.Nodes), len(perNodeArgs)))
	}

	for i := range c.Nodes {
		c.Nodes[i] = c.makeNode(i, binary, append(append([]string(nil), allNodeArgs...), perNodeArgs[i]...), perNodeEnv[i])
		c.Clients[i] = c.makeClient(i)
		c.Status[i] = c.makeStatus(i)
		c.DB[i] = c.makeDB(i, numWorkers, db)
	}

	log.Infof(context.Background(), "started %.3fs", timeutil.Since(c.started).Seconds())
	c.waitForFullReplication()
}

// Close stops the cluster, killing all of the nodes.
func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		n.Kill()
	}
	c.stopper.Stop(context.Background())
}

// IPAddr returns the IP address of the specified node.
func (c *Cluster) IPAddr(nodeIdx int) string {
	if c.separateAddrs {
		return fmt.Sprintf("127.1.1.%d", nodeIdx+1)
	}
	return "127.0.0.1"
}

// RPCAddr returns the RPC address of the specified node.
func (c *Cluster) RPCAddr(nodeIdx int) string {
	return fmt.Sprintf("%s:%d", c.IPAddr(nodeIdx), RPCPort(nodeIdx))
}

// RPCPort returns the RPC port of the specified node.
func RPCPort(nodeIdx int) int {
	return basePort + nodeIdx*2
}

// HTTPPort returns the HTTP port of the specified node.
func HTTPPort(nodeIdx int) int {
	return RPCPort(nodeIdx) + 1
}

func (c *Cluster) makeNode(nodeIdx int, binary string, extraArgs, extraEnv []string) *Node {
	name := fmt.Sprintf("%d", nodeIdx+1)
	dir := filepath.Join(dataDir, name)
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatal(context.Background(), err)
	}

	args := []string{
		binary,
		"start",
		"--insecure",
		fmt.Sprintf("--host=%s", c.IPAddr(nodeIdx)),
		fmt.Sprintf("--port=%d", RPCPort(nodeIdx)),
		fmt.Sprintf("--http-port=%d", HTTPPort(nodeIdx)),
		fmt.Sprintf("--store=%s", dir),
		fmt.Sprintf("--cache=256MiB"),
		fmt.Sprintf("--logtostderr"),
	}
	if nodeIdx > 0 {
		args = append(args, fmt.Sprintf("--join=%s", c.RPCAddr(0)))
	}
	args = append(args, extraArgs...)

	node := &Node{
		logDir: logDir,
		args:   args,
		env:    extraEnv,
	}
	node.Start()
	return node
}

func (c *Cluster) makeClient(nodeIdx int) *client.DB {
	conn, err := c.rpcCtx.GRPCDial(c.RPCAddr(nodeIdx))
	if err != nil {
		log.Fatalf(context.Background(), "failed to initialize KV client: %s", err)
	}
	return client.NewDB(client.NewSender(conn), c.rpcCtx.LocalClock)
}

func (c *Cluster) makeStatus(nodeIdx int) serverpb.StatusClient {
	conn, err := c.rpcCtx.GRPCDial(c.RPCAddr(nodeIdx))
	if err != nil {
		log.Fatalf(context.Background(), "failed to initialize status client: %s", err)
	}
	return serverpb.NewStatusClient(conn)
}

func (c *Cluster) makeDB(nodeIdx, numWorkers int, dbName string) *gosql.DB {
	url := fmt.Sprintf("postgresql://root@%s/%s?sslmode=disable",
		c.RPCAddr(nodeIdx), dbName)
	conn, err := gosql.Open("postgres", url)
	if err != nil {
		log.Fatal(context.Background(), err)
	}
	if numWorkers == 0 {
		numWorkers = 1
	}
	conn.SetMaxOpenConns(numWorkers)
	conn.SetMaxIdleConns(numWorkers)
	return conn
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
	db := c.Clients[0]
	rows, err := db.Scan(context.Background(), keys.Meta2Prefix, keys.Meta2Prefix.PrefixEnd(), 100000)
	if err != nil {
		if IsUnavailableError(err) {
			return false, ""
		}
		log.Fatalf(context.Background(), "scan failed: %s\n", err)
	}

	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	done := true
	for _, row := range rows {
		desc := &roachpb.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			log.Fatalf(context.Background(), "%s: unable to unmarshal range descriptor\n", row.Key)
			continue
		}
		var storeIDs []roachpb.StoreID
		for _, replica := range desc.Replicas {
			storeIDs = append(storeIDs, replica.StoreID)
		}
		fmt.Fprintf(tw, "\t%s\t%s\t[%d]\t%d\n",
			desc.StartKey, desc.EndKey, desc.RangeID, storeIDs)
		if len(desc.Replicas) != 3 {
			done = false
		}
	}
	_ = tw.Flush()
	return done, buf.String()
}

// UpdateZoneConfig updates the default zone config for the cluster.
func (c *Cluster) UpdateZoneConfig(rangeMinBytes, rangeMaxBytes int64) {
	zone := config.DefaultZoneConfig()
	zone.RangeMinBytes = rangeMinBytes
	zone.RangeMaxBytes = rangeMaxBytes

	buf, err := protoutil.Marshal(&zone)
	if err != nil {
		log.Fatal(context.Background(), err)
	}
	_, err = c.DB[0].Exec(`UPSERT INTO system.zones (id, config) VALUES (0, $1)`, buf)
	if err != nil {
		log.Fatal(context.Background(), err)
	}
}

// Split splits the range containing the split key at the specified split key.
func (c *Cluster) Split(nodeIdx int, splitKey roachpb.Key) error {
	return c.Clients[nodeIdx].AdminSplit(context.Background(), splitKey)
}

// TransferLease transfers the lease for the range containing key to a random
// alive node in the range.
func (c *Cluster) TransferLease(nodeIdx int, r *rand.Rand, key roachpb.Key) (bool, error) {
	desc, err := c.lookupRange(nodeIdx, key)
	if err != nil {
		return false, err
	}
	if len(desc.Replicas) <= 1 {
		return false, nil
	}

	var target roachpb.StoreID
	for {
		target = desc.Replicas[r.Intn(len(desc.Replicas))].StoreID
		if c.Nodes[target-1].Alive() {
			break
		}
	}
	if err := c.Clients[nodeIdx].AdminTransferLease(context.Background(), key, target); err != nil {
		return false, errors.Errorf("%s: transfer lease: %s", key, err)
	}
	return true, nil
}

func (c *Cluster) lookupRange(nodeIdx int, key roachpb.Key) (*roachpb.RangeDescriptor, error) {
	req := &roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(keys.MustAddr(key)),
		},
		MaxRanges: 1,
	}
	sender := c.Clients[nodeIdx].GetSender()
	resp, pErr := client.SendWrapped(context.Background(), sender, req)
	if pErr != nil {
		return nil, errors.Errorf("%s: lookup range: %s", key, pErr)
	}
	return &resp.(*roachpb.RangeLookupResponse).Ranges[0], nil
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
	syncutil.Mutex
	logDir string
	args   []string
	env    []string
	cmd    *exec.Cmd
}

// Alive returns true if the node is alive (i.e. not stopped). Note that a
// paused node is considered alive.
func (n *Node) Alive() bool {
	n.Lock()
	defer n.Unlock()
	return n.cmd != nil
}

// Start starts a node.
func (n *Node) Start() {
	n.Lock()
	defer n.Unlock()

	if n.cmd != nil {
		return
	}

	n.cmd = exec.Command(n.args[0], n.args[1:]...)
	n.cmd.Env = os.Environ()
	n.cmd.Env = append(n.cmd.Env, n.env...)

	ctx := context.Background()

	stdoutPath := filepath.Join(n.logDir, "stdout")
	stdout, err := os.OpenFile(stdoutPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf(ctx, "unable to open file %s: %s", stdoutPath, err)
	}
	n.cmd.Stdout = stdout

	stderrPath := filepath.Join(n.logDir, "stderr")
	stderr, err := os.OpenFile(stderrPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf(ctx, "unable to open file %s: %s", stderrPath, err)
	}
	n.cmd.Stderr = stderr

	if err := n.cmd.Start(); err != nil {
		log.Error(ctx, err)
		if err := stdout.Close(); err != nil {
			log.Warning(ctx, err)
		}
		if err := stderr.Close(); err != nil {
			log.Warning(ctx, err)
		}
		return
	}

	pid := n.cmd.Process.Pid
	log.Infof(ctx, "process %d started: %s", pid, n.cmd.Args)

	go func(cmd *exec.Cmd) {
		if err := cmd.Wait(); err != nil {
			log.Errorf(ctx, "waiting for command: %s", err)
		}
		if err := stdout.Close(); err != nil {
			log.Warning(ctx, err)
		}
		if err := stderr.Close(); err != nil {
			log.Warning(ctx, err)
		}

		log.Infof(ctx, "Process %d: %s", pid, cmd.ProcessState)

		n.Lock()
		n.cmd = nil
		n.Unlock()
	}(n.cmd)
}

// Kill stops a node abruptly by sending it SIGKILL.
func (n *Node) Kill() {
	n.Lock()
	defer n.Unlock()
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Kill()
}
