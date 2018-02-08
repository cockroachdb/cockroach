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
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
)

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
}

// Put a local file to all of the machines in a cluster.
func (c *cluster) Put(ctx context.Context, t *testing.T, src, dest string) {
	c.assertT(t)
	t.Helper()
	if c.isLocal() {
		switch dest {
		case "<cockroach>", "<workload>":
			// We don't have to put binaries for local clusters.
			return
		}
	}
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
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
	t.Helper()
	binary := "./cockroach"
	if c.isLocal() {
		binary = *cockroach
	}
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	err := runCmd(ctx, c.l, "roachprod", "start", "-b", binary, c.selector(t, nodes...))
	if err != nil {
		t.Fatal(err)
	}
}

// Stop cockroach nodes running on a subset of the cluster. See start() for a
// description of the nodes paramter.
func (c *cluster) Stop(ctx context.Context, t *testing.T, nodes ...int) {
	c.assertT(t)
	t.Helper()
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	err := runCmd(ctx, c.l, "roachprod", "stop", c.selector(t, nodes...))
	if err != nil {
		t.Fatal(err)
	}
}

// wipe a subset of the nodes in a cluster. See start() for a description of
// the nodes paramter.
func (c *cluster) Wipe(ctx context.Context, t *testing.T, nodes ...int) {
	c.assertT(t)
	t.Helper()
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	err := runCmd(ctx, c.l, "roachprod", "wipe", c.selector(t, nodes...))
	if err != nil {
		t.Fatal(err)
	}
}

// Run a command on the specified node.
func (c *cluster) Run(ctx context.Context, t *testing.T, node int, cmd string) {
	c.assertT(t)
	t.Helper()
	if t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	err := runCmd(ctx, c.l,
		"roachprod", "ssh", fmt.Sprintf("%s:%d", c.name, node), "--", c.expand(cmd))
	if err != nil {
		t.Fatal(err)
	}
}

func (c *cluster) selector(t *testing.T, nodes ...int) string {
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
