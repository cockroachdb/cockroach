// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var (
	local     bool
	artifacts string
	cockroach string
	workload  string
	clusterID string
	username  = os.Getenv("ROACHPROD_USER")
)

func ifLocal(trueVal, falseVal string) string {
	if local {
		return trueVal
	}
	return falseVal
}

func findBinary(binary, defValue string) (string, error) {
	if binary == "" {
		binary = defValue
	}

	// Check to see if binary exists and is a regular file and executable.
	if fi, err := os.Stat(binary); err == nil && fi.Mode().IsRegular() && (fi.Mode()&0111) != 0 {
		return filepath.Abs(binary)
	}

	// Find the binary to run and translate it to an absolute path. First, look
	// for the binary in PATH.
	path, err := exec.LookPath(binary)
	if err != nil {
		if strings.HasPrefix(binary, "/") {
			return "", err
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return "", err
		}
		path = filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/", binary)
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return "", err
		}
	}
	return filepath.Abs(path)
}

func initBinaries() {
	var err error
	cockroach, err = findBinary(cockroach, "cockroach")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	workload, err = findBinary(workload, "workload")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var clusters = map[string]struct{}{}
var clustersMu syncutil.Mutex
var clustersOnce sync.Once

func registerCluster(clusterName string) {
	clustersOnce.Do(func() {
		go func() {
			// Shut down test clusters when interrupted (for example CTRL+C).
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt)
			<-sig

			// Fire off a goroutine to destroy all of the clusters.
			done := make(chan struct{})
			go func() {
				defer close(done)

				var wg sync.WaitGroup
				clustersMu.Lock()
				wg.Add(len(clusters))
				for name := range clusters {
					go func(name string) {
						cmd := exec.Command("roachprod", "destroy", name)
						if err := cmd.Run(); err != nil {
							fmt.Fprintln(os.Stderr, err)
						}
					}(name)
				}
				clustersMu.Unlock()

				wg.Wait()
			}()

			// Wait up to 5 min for clusters to be destroyed. This can take a while and
			// we don't want to rush it.
			select {
			case <-done:
			case <-time.After(5 * time.Minute):
			}
		}()
	})

	clustersMu.Lock()
	clusters[clusterName] = struct{}{}
	clustersMu.Unlock()
}

func execCmd(ctx context.Context, l *logger, args ...string) error {
	l.printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = l.stdout
	cmd.Stderr = l.stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, `%s`, strings.Join(args, ` `))
	}
	return nil
}

func makeClusterName(t testI) string {
	if local {
		return "local"
	}

	// TODO(peter): Add an option to use an existing cluster.

	if username == "" {
		usr, err := user.Current()
		if err != nil {
			panic(fmt.Sprintf("user.Current: %s", err))
		}
		username = usr.Username
	}
	id := clusterID
	if id == "" {
		id = fmt.Sprintf("%d", timeutil.Now().Unix())
	}
	name := fmt.Sprintf("%s-%s-%s", username, id, t.Name())
	name = strings.ToLower(name)
	name = regexp.MustCompile(`[^-a-z0-9]+`).ReplaceAllString(name, "-")
	name = regexp.MustCompile(`-+`).ReplaceAllString(name, "-")
	return name
}

type testI interface {
	Name() string
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool
}

type option interface {
	option()
}

type nodeSelector interface {
	option
	merge(nodeListOption) nodeListOption
}

type nodeListOption []int

func (n nodeListOption) option() {}

func (n nodeListOption) merge(o nodeListOption) nodeListOption {
	t := make(nodeListOption, 0, len(n)+len(o))
	t = append(t, n...)
	t = append(t, o...)
	sort.Ints([]int(t))
	r := t[:1]
	for i := 1; i < len(t); i++ {
		if r[len(r)-1] != t[i] {
			r = append(r, t[i])
		}
	}
	return r
}

func (n nodeListOption) String() string {
	if len(n) == 0 {
		return ""
	}

	var buf bytes.Buffer
	buf.WriteByte(':')

	appendRange := func(start, end int) {
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		if start == end {
			fmt.Fprintf(&buf, "%d", start)
		} else {
			fmt.Fprintf(&buf, "%d-%d", start, end)
		}
	}

	start, end := -1, -1
	for _, i := range n {
		if start != -1 && end == i-1 {
			end = i
			continue
		}
		if start != -1 {
			appendRange(start, end)
		}
		start, end = i, i
	}
	if start != -1 {
		appendRange(start, end)
	}
	return buf.String()
}

// cluster provides an interface for interacting with a set of machines,
// starting and stopping a cockroach cluster on a subset of those machines, and
// running load generators and other operations on the machines.
//
// A cluster is intended to be used only by a single test. Sharing of a cluster
// between a test and a subtest is current disallowed (see cluster.assertT). A
// cluster is safe for concurrent use by multiple goroutines.
type cluster struct {
	name   string
	nodes  int
	status func(...interface{})
	t      testI
	l      *logger
}

// TODO(peter): Should set the lifetime of clusters to 2x the expected test
// duration. The default lifetime of 12h is too long for some tests and will be
// too short for others.
func newCluster(ctx context.Context, t testI, nodes int, args ...interface{}) *cluster {
	l, err := rootLogger(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster{
		name:   makeClusterName(t),
		nodes:  nodes,
		status: func(...interface{}) {},
		t:      t,
		l:      l,
	}
	registerCluster(c.name)

	if impl, ok := t.(*test); ok {
		c.status = impl.Status
	}

	sargs := []string{"roachprod", "create", c.name, "-n", fmt.Sprint(nodes)}
	for _, arg := range args {
		sargs = append(sargs, fmt.Sprint(arg))
	}

	c.status("creating cluster")
	if err := execCmd(ctx, l, sargs...); err != nil {
		t.Fatal(err)
		return nil
	}
	return c
}

// All returns a node list containing all of the nodes in the cluster.
func (c *cluster) All() nodeListOption {
	return c.Range(1, c.nodes)
}

// All returns a node list containing the nodes [begin,end].
func (c *cluster) Range(begin, end int) nodeListOption {
	if begin < 1 || end > c.nodes {
		c.t.Fatalf("invalid node range: %d-%d (1-%d)", begin, end, c.nodes)
	}
	r := make(nodeListOption, 0, 1+end-begin)
	for i := begin; i <= end; i++ {
		r = append(r, i)
	}
	return r
}

// All returns a node list containing only the node i.
func (c *cluster) Node(i int) nodeListOption {
	return c.Range(i, i)
}

func (c *cluster) Destroy(ctx context.Context) {
	if !c.isLocal() {
		// TODO(peter): Figure out how to retrieve local logs. One option would be
		// to stop the cluster and mv ~/local to wherever we want it.
		c.status("retrieving logs")
		_ = execCmd(ctx, c.l, "roachprod", "get", c.name, "logs",
			filepath.Join(artifacts, c.t.Name(), "logs"))
	}
	c.status("destroying cluster")
	if err := execCmd(ctx, c.l, "roachprod", "destroy", c.name); err != nil {
		c.l.errorf("%s", err)
	}

	clustersMu.Lock()
	delete(clusters, c.name)
	clustersMu.Unlock()

	c.l.close()
}

// Put a local file to all of the machines in a cluster.
func (c *cluster) Put(ctx context.Context, src, dest string) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	c.status("uploading binary")
	err := execCmd(ctx, c.l, "roachprod", "put", c.name, src, dest)
	if err != nil {
		c.t.Fatal(err)
	}
}

// Start cockroach nodes on a subset of the cluster. The nodes parameter can
// either be a specific node, empty (to indicate all nodes), or a pair of nodes
// indicating a range.
func (c *cluster) Start(ctx context.Context, opts ...option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	c.status("starting cluster")
	err := execCmd(ctx, c.l, "roachprod", "start", c.makeNodes(opts))
	if err != nil {
		c.t.Fatal(err)
	}
}

// Stop cockroach nodes running on a subset of the cluster. See cluster.Start()
// for a description of the nodes parameter.
func (c *cluster) Stop(ctx context.Context, opts ...option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	c.status("stopping cluster")
	err := execCmd(ctx, c.l, "roachprod", "stop", c.makeNodes(opts))
	if err != nil {
		c.t.Fatal(err)
	}
}

// Wipe a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *cluster) Wipe(ctx context.Context, opts ...option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	c.status("wiping cluster")
	err := execCmd(ctx, c.l, "roachprod", "wipe", c.makeNodes(opts))
	if err != nil {
		c.t.Fatal(err)
	}
}

// TODO(pmattis): cluster.Stop and cluster.Wipe are neither used or
// tested. Silence unused warning.
var _ = (*cluster).Stop
var _ = (*cluster).Wipe

// Run a command on the specified node.
func (c *cluster) Run(ctx context.Context, node int, args ...string) {
	c.RunL(ctx, c.l, node, args...)
}

// RunL runs a command on the specified node.
func (c *cluster) RunL(ctx context.Context, l *logger, node int, args ...string) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	err := execCmd(ctx, l,
		append([]string{"roachprod", "ssh", fmt.Sprintf("%s:%d", c.name, node), "--"}, args...)...)
	if err != nil {
		c.t.Fatal(err)
	}
}

func (c *cluster) makeNodes(opts []option) string {
	var r nodeListOption
	for _, o := range opts {
		if s, ok := o.(nodeSelector); ok {
			r = s.merge(r)
		}
	}
	return c.name + r.String()
}

func (c *cluster) isLocal() bool {
	return c.name == "local"
}

type monitor struct {
	t      testI
	l      *logger
	nodes  string
	ctx    context.Context
	cancel func()
	g      *errgroup.Group
}

func newMonitor(ctx context.Context, c *cluster, opts ...option) *monitor {
	m := &monitor{
		t:     c.t,
		l:     c.l,
		nodes: c.makeNodes(opts),
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.g, m.ctx = errgroup.WithContext(m.ctx)
	return m
}

func (m *monitor) Go(fn func(context.Context) error) {
	m.g.Go(func() error {
		return fn(m.ctx)
	})
}

func (m *monitor) Wait() {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}

	err := m.wait("roachprod", "monitor", m.nodes)
	if err != nil {
		m.t.Fatal(err)
	}
}

func (m *monitor) wait(args ...string) error {
	// It is surprisingly difficult to get the cancelation semantics exactly
	// right. We need to watch for the "workers" group (m.g) to finish, or for
	// the monitor command to emit an unexpected node failure, or for the monitor
	// command itself to exit. We want to capture whichever error happens first
	// and then cancel the other goroutines. This ordering prevents the usage of
	// an errgroup.Group for the goroutines below. Consider:
	//
	//   g, _ := errgroup.WithContext(m.ctx)
	//   g.Go(func(context.Context) error {
	//     defer m.cancel()
	//     return m.g.Wait()
	//   })
	//
	// Now consider what happens when an error is returned. Before the error
	// reaches the errgroup, we invoke the cancelation closure which can cause
	// the other goroutines to wake up and perhaps race and set the errgroup
	// error first.
	//
	// The solution is to implement our own errgroup mechanism here which allows
	// us to set the error before performing the cancelation.

	var errOnce sync.Once
	var err error
	setErr := func(e error) {
		if e != nil {
			errOnce.Do(func() {
				err = e
			})
		}
	}

	// 1. The first goroutine waits for the worker errgroup to exit.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			m.cancel()
			wg.Done()
		}()
		setErr(m.g.Wait())
	}()

	// 2. The second goroutine forks/execs the monitoring command.
	pipeR, pipeW := io.Pipe()
	wg.Add(1)
	go func() {
		defer func() {
			_ = pipeW.Close()
			wg.Done()
			// NB: we explicitly do not want to call m.cancel() here as we want the
			// goroutine that is reading the monitoring events to be able to decide
			// on the error if the monitoring command exits peacefully.
		}()

		monL, err := m.l.childLogger(`MONITOR`)
		if err != nil {
			setErr(err)
			return
		}
		defer monL.close()

		cmd := exec.CommandContext(m.ctx, args[0], args[1:]...)
		cmd.Stdout = io.MultiWriter(pipeW, monL.stdout)
		cmd.Stderr = monL.stderr
		if err := cmd.Run(); err != nil {
			if err != context.Canceled && !strings.Contains(err.Error(), "killed") {
				// The expected reason for an error is that the monitor was killed due
				// to the context being canceled. Any other error is an actual error.
				setErr(err)
				return
			}
		}
		// Returning will cause the pipe to be closed which will cause the reader
		// goroutine to exit and close the monitoring channel.
	}()

	// 3. The third goroutine reads from the monitoring pipe, watching for any
	// unexpected death events.
	wg.Add(1)
	go func() {
		defer func() {
			_ = pipeR.Close()
			m.cancel()
			wg.Done()
		}()

		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			msg := scanner.Text()
			var id int
			var s string
			if n, _ := fmt.Sscanf(msg, "%d: %s", &id, &s); n == 2 {
				if strings.Contains(s, "dead") {
					setErr(fmt.Errorf("unexpected node event: %s", msg))
					return
				}
			}
		}
	}()

	wg.Wait()
	return err
}
