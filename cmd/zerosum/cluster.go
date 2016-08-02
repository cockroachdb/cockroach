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

package main

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"

	"github.com/pkg/errors"
	// Import postgres driver.
	_ "github.com/cockroachdb/pq"
	"golang.org/x/net/context"
)

const basePort = 26257
const dataDir = "cockroach-data"

var cockroachBin = func() string {
	bin := "./cockroach"
	if _, err := os.Stat(bin); err == nil {
		return bin
	}
	return "cockroach"
}()

type cluster struct {
	rpcCtx  *rpc.Context
	nodes   []*node
	clients []*client.DB
	db      []*gosql.DB
	stopper *stop.Stopper
	started time.Time
}

func newCluster(size int) *cluster {
	return &cluster{
		nodes:   make([]*node, size),
		clients: make([]*client.DB, size),
		db:      make([]*gosql.DB, size),
		stopper: stop.NewStopper(),
	}
}

func (c *cluster) start(db string, args []string) {
	c.started = timeutil.Now()

	baseCtx := &base.Context{
		User:     security.NodeUser,
		Insecure: true,
	}
	c.rpcCtx = rpc.NewContext(baseCtx, nil, c.stopper)

	for i := range c.nodes {
		c.nodes[i] = c.makeNode(i, args)
		c.clients[i] = c.makeClient(i)
		c.db[i] = c.makeDB(i, db)
	}

	log.Infof(context.Background(), "started %.3fs", timeutil.Since(c.started).Seconds())
}

func (c *cluster) close() {
	for _, n := range c.nodes {
		n.kill()
	}
	c.stopper.Stop()
}

func (c *cluster) rpcPort(nodeIdx int) int {
	return basePort + nodeIdx*2
}

func (c *cluster) rpcAddr(nodeIdx int) string {
	return fmt.Sprintf("localhost:%d", c.rpcPort(nodeIdx))
}

func (c *cluster) httpPort(nodeIdx int) int {
	return c.rpcPort(nodeIdx) + 1
}

func (c *cluster) makeNode(nodeIdx int, extraArgs []string) *node {
	name := fmt.Sprintf("%d", nodeIdx+1)
	dir := filepath.Join(dataDir, name)
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatal(context.Background(), err)
	}

	args := []string{
		cockroachBin,
		"start",
		"--insecure",
		fmt.Sprintf("--port=%d", c.rpcPort(nodeIdx)),
		fmt.Sprintf("--http-port=%d", c.httpPort(nodeIdx)),
		fmt.Sprintf("--store=%s", dir),
		fmt.Sprintf("--cache=256MiB"),
		fmt.Sprintf("--logtostderr"),
	}
	if nodeIdx > 0 {
		args = append(args, fmt.Sprintf("--join=localhost:%d", c.rpcPort(0)))
	}
	args = append(args, extraArgs...)

	node := &node{
		LogDir: logDir,
		Args:   args,
	}
	node.start()
	return node
}

func (c *cluster) makeClient(nodeIdx int) *client.DB {
	sender, err := client.NewSender(c.rpcCtx, c.rpcAddr(nodeIdx))
	if err != nil {
		log.Fatalf(context.Background(), "failed to initialize KV client: %s", err)
	}
	return client.NewDB(sender)
}

func (c *cluster) makeDB(nodeIdx int, dbName string) *gosql.DB {
	url := fmt.Sprintf("postgresql://root@localhost:%d/%s?sslmode=disable",
		c.rpcPort(nodeIdx), dbName)
	conn, err := gosql.Open("postgres", url)
	if err != nil {
		log.Fatal(context.Background(), err)
	}
	return conn
}

func (c *cluster) waitForFullReplication() {
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

func (c *cluster) isReplicated() (bool, string) {
	db := c.clients[0]
	rows, err := db.Scan(keys.Meta2Prefix, keys.Meta2Prefix.PrefixEnd(), 100000)
	if err != nil {
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

func (c *cluster) split(nodeIdx int, splitKey roachpb.Key) error {
	return c.clients[nodeIdx].AdminSplit(splitKey)
}

func (c *cluster) transferLease(nodeIdx int, r *rand.Rand, key roachpb.Key) (bool, error) {
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
		if c.nodes[target-1].alive() {
			break
		}
	}
	if err := c.clients[nodeIdx].AdminTransferLease(key, target); err != nil {
		return false, errors.Errorf("%s: transfer lease: %s", key, err)
	}
	return true, nil
}

func (c *cluster) lookupRange(nodeIdx int, key roachpb.Key) (*roachpb.RangeDescriptor, error) {
	req := &roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(keys.MustAddr(key)),
		},
		MaxRanges:       1,
		ConsiderIntents: false,
	}
	sender := c.clients[nodeIdx].GetSender()
	resp, pErr := client.SendWrapped(sender, nil, req)
	if pErr != nil {
		return nil, errors.Errorf("%s: lookup range: %s", key, pErr)
	}
	return &resp.(*roachpb.RangeLookupResponse).Ranges[0], nil
}

func (c *cluster) freeze(nodeIdx int, freeze bool) {
	addr := c.rpcAddr(nodeIdx)
	conn, err := c.rpcCtx.GRPCDial(addr)
	if err != nil {
		log.Fatalf(context.Background(), "unable to dial: %s: %v", addr, err)
	}

	adminClient := serverpb.NewAdminClient(conn)
	stream, err := adminClient.ClusterFreeze(
		context.Background(), &serverpb.ClusterFreezeRequest{Freeze: freeze})
	if err != nil {
		log.Fatal(context.Background(), err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(context.Background(), err)
		}
		fmt.Println(resp.Message)
	}
	fmt.Println("ok")
}

func (c *cluster) randNode(f func(int) int) int {
	for {
		i := f(len(c.nodes))
		if c.nodes[i].alive() {
			return i
		}
	}
}

type node struct {
	syncutil.Mutex
	LogDir string
	Args   []string
	Cmd    *exec.Cmd
}

func (n *node) alive() bool {
	n.Lock()
	defer n.Unlock()
	return n.Cmd != nil
}

func (n *node) start() {
	n.Lock()
	defer n.Unlock()

	if n.Cmd != nil {
		return
	}

	n.Cmd = exec.Command(n.Args[0], n.Args[1:]...)

	stdoutPath := filepath.Join(n.LogDir, "stdout")
	stdout, err := os.OpenFile(stdoutPath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf(context.Background(), "unable to open file %s: %s", stdoutPath, err)
	}
	n.Cmd.Stdout = stdout

	stderrPath := filepath.Join(n.LogDir, "stderr")
	stderr, err := os.OpenFile(stderrPath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf(context.Background(), "unable to open file %s: %s", stderrPath, err)
	}
	n.Cmd.Stderr = stderr

	err = n.Cmd.Start()
	if n.Cmd.Process != nil {
		log.Infof(context.Background(), "process %d started: %s",
			n.Cmd.Process.Pid, strings.Join(n.Args, " "))
	}
	if err != nil {
		log.Infof(context.Background(), "%v", err)
		_ = stdout.Close()
		_ = stderr.Close()
		return
	}

	go func(cmd *exec.Cmd) {
		if err := cmd.Wait(); err != nil {
			log.Errorf(context.Background(), "waiting for command: %v", err)
		}
		_ = stdout.Close()
		_ = stderr.Close()

		ps := cmd.ProcessState
		sy := ps.Sys().(syscall.WaitStatus)

		log.Infof(context.Background(), "Process %d exited with status %d",
			ps.Pid(), sy.ExitStatus())
		log.Infof(context.Background(), ps.String())

		n.Lock()
		n.Cmd = nil
		n.Unlock()
	}(n.Cmd)
}

func (n *node) pause() {
	n.Lock()
	defer n.Unlock()
	if n.Cmd == nil || n.Cmd.Process == nil {
		return
	}
	_ = n.Cmd.Process.Signal(syscall.SIGSTOP)
}

// TODO(peter): node.pause is currently unused.
var _ = (*node).pause

func (n *node) resume() {
	n.Lock()
	defer n.Unlock()
	if n.Cmd == nil || n.Cmd.Process == nil {
		return
	}
	_ = n.Cmd.Process.Signal(syscall.SIGCONT)
}

// TODO(peter): node.resume is currently unused.
var _ = (*node).resume

func (n *node) kill() {
	n.Lock()
	defer n.Unlock()
	if n.Cmd == nil || n.Cmd.Process == nil {
		return
	}
	_ = n.Cmd.Process.Kill()
}
