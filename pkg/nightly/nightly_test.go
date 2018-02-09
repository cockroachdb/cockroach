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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"golang.org/x/sync/errgroup"
)

func TestLocal(t *testing.T) {
	if !*local {
		t.Skipf("-local not specified")
	}
	maybeSkip(t)

	ctx := context.Background()
	c := newCluster(ctx, t, "-n", "1")
	defer c.Destroy(ctx, t)

	c.Put(ctx, t, *cockroach, "<cockroach>")
	c.Put(ctx, t, *workload, "<workload>")
	c.Start(ctx, t, 1)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		c.Run(ctx, t, 1, "<workload> run kv --init --read-percent=95 --splits=100 --duration=10s")
		return nil
	})
	c.Monitor(ctx, t, g)
}

func TestSingleDC(t *testing.T) {
	maybeSkip(t)

	concurrency := " --concurrency=384"
	duration := " --duration=10m"
	splits := " --splits=100000"
	if *local {
		concurrency = ""
		duration = " --duration=10s"
		splits = " --splits=2000"
	}

	run := func(name, cmd string) {
		t.Run(name, func(t *testing.T) {
			// TODO(dan): It's not clear to me yet how all the various configurations
			// of clusters (# of machines in each locality) are going to
			// generalize. For now, just hardcode what we had before.
			ctx := context.Background()
			c := newCluster(ctx, t, "-n", "4")
			defer c.Destroy(ctx, t)

			c.Put(ctx, t, *cockroach, "<cockroach>")
			c.Put(ctx, t, *workload, "<workload>")
			c.Start(ctx, t, 1, 3)

			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				c.Run(ctx, t, 4, cmd)
				return nil
			})
			c.Monitor(ctx, t, g, 1, 3)
		})
	}

	run("kv0", "<workload> run kv --init --read-percent=0 --splits=1000"+concurrency+duration)
	run("kv95", "<workload> run kv --init --read-percent=95 --splits=1000"+concurrency+duration)
	run("splits", "<workload> run kv --init --read-percent=0 --max-ops=1"+concurrency+splits)
	run("tpcc_w1", "<workload> run tpcc --init --warehouses=1 --wait=false"+concurrency+duration)
	run("tpmc_w1", "<workload> run tpcc --init --warehouses=1 --concurrency=10"+duration)
}

func TestRoachmart(t *testing.T) {
	maybeSkip(t)

	testutils.RunTrueAndFalse(t, "partition", func(t *testing.T, partition bool) {
		ctx := context.Background()
		c := newCluster(ctx, t, "--geo", "--nodes", "9")
		defer c.Destroy(ctx, t)

		c.Put(ctx, t, *cockroach, "<cockroach>")
		c.Put(ctx, t, *workload, "<workload>")
		c.Start(ctx, t, 1, 9)

		// TODO(benesch): avoid hardcoding this list.
		nodes := []struct {
			i    int
			zone string
		}{
			{1, "us-east1-b"},
			{4, "us-west1-b"},
			{7, "europe-west2-b"},
		}

		roachmartRun := func(ctx context.Context, t *testing.T, i int, args ...string) {
			t.Helper()
			args = append(args,
				"--local-zone="+nodes[i].zone,
				"--local-percent=90",
				"--users=10",
				"--orders=100",
				fmt.Sprintf("--partition=%v", partition))

			l, err := c.l.childLogger(fmt.Sprint(nodes[i].i))
			if err != nil {
				t.Fatal(err)
			}
			c.RunL(ctx, t, l, nodes[i].i, args...)
		}
		roachmartRun(ctx, t, 0, "<workload>", "init", "roachmart")

		duration := "10m"
		if *local {
			duration = "10s"
		}

		g, ctx := errgroup.WithContext(ctx)
		for i := range nodes {
			i := i
			g.Go(func() error {
				roachmartRun(ctx, t, i, "<workload>", "run", "roachmart", "--duration", duration)
				return nil
			})
		}

		c.Monitor(ctx, t, g)
	})
}
