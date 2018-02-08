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

type cluster struct {
	name string
	t    *testing.T
	l    *logger
}

func newCluster(ctx context.Context, t *testing.T, args ...string) *cluster {
	l, err := rootLogger(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster{
		name: makeClusterName(t),
		t:    t,
		l:    l,
	}

	args = append([]string{"roachprod", "create", c.name}, args...)
	if err := runCmd(ctx, l, args...); err != nil {
		t.Fatal(err)
		return nil
	}
	return c
}

func (c *cluster) destroy(ctx context.Context) {
	if !c.isLocal() {
		// TODO(peter): Figure out how to retrieve local logs.
		_ = runCmd(ctx, c.l, "roachprod", "get", c.name, "logs",
			filepath.Join(*artifacts, fileutil.EscapeFilename(c.t.Name())))
	}
	if err := runCmd(ctx, c.l, "roachprod", "destroy", c.name); err != nil {
		c.l.errorf("%s", err)
	}
	unregisterCluster(c.name)
}

func (c *cluster) put(ctx context.Context, src, dest string) {
	c.t.Helper()
	if c.isLocal() {
		switch dest {
		case "<cockroach>", "<workload>":
			// We don't have to put binaries for local clusters.
			return
		}
	}
	c.run(ctx, "roachprod", "put", c.name, src, c.expand(dest))
}

func (c *cluster) ssh(ctx context.Context, node int, cmd string) {
	c.t.Helper()
	c.run(ctx, "roachprod", "ssh", fmt.Sprintf("%s:%d", c.name, node), "--", c.expand(cmd))
}

func (c *cluster) start(ctx context.Context, nodes ...int) {
	c.t.Helper()
	binary := "./cockroach"
	if c.isLocal() {
		binary = *cockroach
	}
	c.run(ctx, "roachprod", "start", "-b", binary, c.selector(nodes...))
}

func (c *cluster) selector(nodes ...int) string {
	switch len(nodes) {
	case 0:
		return c.name
	case 1:
		return fmt.Sprintf("%s:%d", c.name, nodes[0])
	case 2:
		return fmt.Sprintf("%s:%d-%d", c.name, nodes[0], nodes[1])
	default:
		c.t.Fatalf("bad nodes specification: %d", nodes)
		return ""
	}
}

func (c *cluster) run(ctx context.Context, args ...string) {
	c.t.Helper()
	if err := runCmd(ctx, c.l, args...); err != nil {
		c.t.Fatal(err)
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
