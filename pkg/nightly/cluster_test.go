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

package nightly

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var clusterID = flag.String("clusterid", "", "An identifier to use in the test cluster's name")

func runCmd(ctx context.Context, l *logger, args ...string) error {
	l.printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = l.stdout
	cmd.Stderr = l.stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, `%s`, strings.Join(args, ` `))
	}
	return nil
}

func makeClusterName(t *testing.T) string {
	if *local {
		return "local"
	}

	t.Parallel()
	username := os.Getenv("ROACHPROD_USER")
	if username == "" {
		usr, err := user.Current()
		if err != nil {
			panic(fmt.Sprintf("user.Current: %s", err))
		}
		username = usr.Username
	}
	id := *clusterID
	if id == "" {
		id = fmt.Sprintf("%d", timeutil.Now().Unix())
	}
	name := fmt.Sprintf("%s-%s-%s", username, id, t.Name())
	name = strings.ToLower(name)
	name = regexp.MustCompile(`[^-a-z0-9]+`).ReplaceAllString(name, "-")
	name = regexp.MustCompile(`-+`).ReplaceAllString(name, "-")
	registerCluster(name)
	return name
}

// cluster provides an interface for interacting with a set of machines,
// starting and stopping a cockroach cluster on a subset of those machines, and
// running load generators and other operations on the machines.
//
// A cluster is intended to be used only by a single test. Sharing of a cluster
// between a test and a subtest is current disallowed (see cluster.assertT). A
// cluster is safe for concurrent use by multiple goroutines.
type cluster struct {
	name     string
	testName string
	l        *logger
}

// TODO(peter): Should set the lifetime of clusters to 2x the expected test
// duration. The default lifetime of 12h is too long for some tests and will be
// too short for others.
func newCluster(ctx context.Context, t *testing.T, args ...string) *cluster {
	var l *logger
	if *local {
		l = stdLogger(t.Name())
	} else {
		var err error
		if l, err = rootLogger(t.Name()); err != nil {
			t.Fatal(err)
		}
	}

	c := &cluster{
		name:     makeClusterName(t),
		testName: t.Name(),
		l:        l,
	}

	args = append([]string{"roachprod", "create", c.name}, args...)
	if err := runCmd(ctx, l, args...); err != nil {
		t.Fatal(err)
		return nil
	}
	return c
}

func (c *cluster) assertT(t *testing.T) {
	if c.testName != t.Name() {
		panic(fmt.Sprintf("cluster created by %s, but used by %s", c.testName, t.Name()))
	}
}

func (c *cluster) Destroy(ctx context.Context, t *testing.T) {
	c.assertT(t)
	if !c.isLocal() {
		// TODO(peter): Figure out how to retrieve local logs. One option would be
		// to stop the cluster and mv ~/local to wherever we want it.
		_ = runCmd(ctx, c.l, "roachprod", "get", c.name, "logs",
			filepath.Join(*artifacts, fileutil.EscapeFilename(c.testName)))
	}
	if err := runCmd(ctx, c.l, "roachprod", "destroy", c.name); err != nil {
		c.l.errorf("%s", err)
	}
	unregisterCluster(c.name)
	c.l.close()
}

// Put a local file to all of the machines in a cluster.
func (c *cluster) Put(ctx context.Context, t *testing.T, src, dest string) {
	c.assertT(t)
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	t.Helper()
	if c.isLocal() {
		switch dest {
		case "<cockroach>", "<workload>":
			// We don't have to put binaries for local clusters.
			return
		}
	}
	err := runCmd(ctx, c.l, "roachprod", "put", c.name, src, c.expand(dest))
	if err != nil {
		t.Fatal(err)
	}
}

// Start cockroach nodes on a subset of the cluster. The nodes parameter can
// either be a specific node, empty (to indicate all nodes), or a pair of nodes
// indicating a range.
func (c *cluster) Start(ctx context.Context, t *testing.T, nodes ...int) {
	c.assertT(t)
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	t.Helper()
	binary := "./cockroach"
	if c.isLocal() {
		binary = *cockroach
	}
	err := runCmd(ctx, c.l, "roachprod", "start", "-b", binary, c.selector(t, nodes...))
	if err != nil {
		t.Fatal(err)
	}
}

// Stop cockroach nodes running on a subset of the cluster. See cluster.Start()
// for a description of the nodes parameter.
func (c *cluster) Stop(ctx context.Context, t *testing.T, nodes ...int) {
	c.assertT(t)
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	t.Helper()
	err := runCmd(ctx, c.l, "roachprod", "stop", c.selector(t, nodes...))
	if err != nil {
		t.Fatal(err)
	}
}

// wipe a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *cluster) Wipe(ctx context.Context, t *testing.T, nodes ...int) {
	c.assertT(t)
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	t.Helper()
	err := runCmd(ctx, c.l, "roachprod", "wipe", c.selector(t, nodes...))
	if err != nil {
		t.Fatal(err)
	}
}

// TODO(pmattis): cluster.Stop and cluster.Wipe are neither used or
// tested. Silence unused warning.
var _ = (*cluster).Stop
var _ = (*cluster).Wipe

// Run a command on the specified node.
func (c *cluster) Run(ctx context.Context, t *testing.T, node int, args ...string) {
	t.Helper()
	c.RunL(ctx, t, c.l, node, args...)
}

// RunL runs a command on the specified node.
func (c *cluster) RunL(ctx context.Context, t *testing.T, l *logger, node int, args ...string) {
	c.assertT(t)
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	t.Helper()
	for i := range args {
		args[i] = c.expand(args[i])
	}
	err := runCmd(ctx, l,
		append([]string{"roachprod", "ssh", fmt.Sprintf("%s:%d", c.name, node), "--"}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
}

// Monitor monitors the specified nodes in a cockroach cluster, watching for
// unexpected failures. A failure occurs if either the errgroup returns an
// error or if a cockroach node dies. See cluster.Start() for a description of
// the nodes parameter.
func (c *cluster) Monitor(ctx context.Context, t *testing.T, g *errgroup.Group, nodes ...int) {
	c.assertT(t)
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	t.Helper()

	err := c.monitor(ctx, t, g, nodes, "roachprod", "monitor", c.name)
	if err != nil {
		t.Fatal(err)
	}
}

func (c *cluster) monitor(
	ctx context.Context, t *testing.T, g *errgroup.Group, nodes []int, args ...string,
) error {
	t.Helper()

	_ = c.selector(t, nodes...) // check for a valid nodes specification

	done := make(chan error)
	go func() {
		done <- g.Wait()
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fatal := t.Fatal
	pipeR, pipeW := io.Pipe()
	go func() {
		defer func() { _ = pipeW.Close() }()

		monL, err := c.l.childLogger(`MONITOR`)
		if err != nil {
			fatal(err)
		}
		defer monL.close()

		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		if testing.Verbose() {
			cmd.Stdout = io.MultiWriter(pipeW, monL.stdout)
		} else {
			cmd.Stdout = pipeW
		}
		cmd.Stderr = monL.stderr
		if err := cmd.Run(); err != nil {
			if err != context.Canceled && !strings.Contains(err.Error(), "killed") {
				// The expected reason for an error is that the monitor was killed due
				// to the context being canceled. Any other error is an actual error.
				fatal(err)
			}
			// Returning will cause the pipe to be closed which will cause the reader
			// goroutine to exit and close the monitoring channel.
			return
		}
	}()

	monitorCh := make(chan string)
	go func() {
		defer func() { _ = pipeR.Close() }()
		defer close(monitorCh)

		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			monitorCh <- scanner.Text()
		}
	}()

	watchedNode := func(id int) bool {
		switch len(nodes) {
		case 0:
			return true
		case 1:
			return id == nodes[0]
		case 2:
			return nodes[0] <= id && id <= nodes[1]
		default:
			return false
		}
	}

	for {
		select {
		case err := <-done:
			return err
		case msg, ok := <-monitorCh:
			if !ok {
				return fmt.Errorf("monitor died unexpectedly")
			}

			var id int
			var s string
			if n, _ := fmt.Sscanf(msg, "%d: %s", &id, &s); n == 2 {
				if watchedNode(id) && strings.Contains(s, "dead") {
					return fmt.Errorf("unexpected node event: %s", msg)
				}
			}
		}
	}
}

func (c *cluster) selector(t *testing.T, nodes ...int) string {
	t.Helper()
	switch len(nodes) {
	case 0:
		return c.name
	case 1:
		return fmt.Sprintf("%s:%d", c.name, nodes[0])
	case 2:
		return fmt.Sprintf("%s:%d-%d", c.name, nodes[0], nodes[1])
	default:
		t.Fatalf("bad nodes specification: %d", nodes)
		return ""
	}
}

func (c *cluster) expand(s string) string {
	if c.isLocal() {
		s = strings.Replace(s, "<cockroach>", *cockroach, -1)
		s = strings.Replace(s, "<workload>", *workload, -1)
	} else {
		s = strings.Replace(s, "<cockroach>", "./cockroach", -1)
		s = strings.Replace(s, "<workload>", "./workload", -1)
	}
	return s
}

func (c *cluster) isLocal() bool {
	return c.name == "local"
}

func TestInvalidClusterUsage(t *testing.T) {
	// Verify that it is invalid to share a cluster between a parent test and
	// child tests. This is necessary because we arrange to run nightly tests in
	// parallel and such sharing of a cluster would lead to all sorts of
	// weirdness in the test.
	c := &cluster{testName: t.Name()}

	t.Run("child", func(t *testing.T) {
		defer func() {
			err := recover()
			if !strings.Contains(fmt.Sprint(err), "but used by") {
				t.Fatalf("expected a invalid usage panic, but found %v", err)
			}
		}()
		c.Put(context.Background(), t, "foo", "bar")
	})
}

func TestClusterMonitor(t *testing.T) {
	t.Run(`success`, func(t *testing.T) {
		c := &cluster{testName: t.Name(), l: stdLogger(t.Name())}
		g := &errgroup.Group{}
		g.Go(func() error { return nil })
		if err := c.monitor(context.Background(), t, g, nil, `sleep`, `100`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run(`dead`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &cluster{testName: t.Name(), l: stdLogger(t.Name())}
		g := &errgroup.Group{}
		g.Go(func() error {
			<-ctx.Done()
			return ctx.Err()
		})

		err := c.monitor(ctx, t, g, nil, `echo`, "1: 100\n1: dead")
		expectedErr := `dead`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`nodes`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &cluster{testName: t.Name(), l: stdLogger(t.Name())}
		g := &errgroup.Group{}
		g.Go(func() error {
			<-ctx.Done()
			return ctx.Err()
		})

		// Node 1 is dead, but we were only monitoring node 2.
		err := c.monitor(ctx, t, g, []int{2}, `echo`, "1: dead")
		expectedErr := `monitor died unexpectedly`
		if !testutils.IsError(err, expectedErr) {
			t.Fatal(err)
		}
	})

	t.Run(`worker-fail`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &cluster{testName: t.Name(), l: stdLogger(t.Name())}
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return errors.New(`worker-fail`)
		})
		g.Go(func() error {
			<-ctx.Done()
			return ctx.Err()
		})

		err := c.monitor(ctx, t, g, nil, `sleep`, `100`)
		expectedErr := `worker-fail`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})
}
