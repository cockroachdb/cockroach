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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
)

func init() {
	runVersion := func(ctx context.Context, t *test, c *cluster, version string) {
		nodes := c.nodes - 1
		goos := ifLocal(runtime.GOOS, "linux")

		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: version,
			GOOS:    goos,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Put(ctx, workload, "./workload", c.Node(nodes+1))

		c.Put(ctx, b, "./cockroach", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes))

		stageDuration := 10 * time.Minute
		buffer := 10 * time.Minute
		if local {
			c.l.printf("local mode: speeding up test\n")
			stageDuration = 10 * time.Second
			buffer = time.Minute
		}

		time.Sleep(10 * time.Second)

		loadDuration := " --duration=" + (time.Duration(3*nodes+2)*stageDuration + buffer).String()

		workloads := []string{
			"./workload run tpcc --tolerate-errors --init --warehouses=1" + loadDuration + " {pgurl:1-%d}",
			// TODO(tschottdorf): adding `--splits=10000` results in a barrage of messages of the type:
			//
			// W180301 04:25:28.785116 21 workload/workload.go:323  ALTER TABLE kv
			// SCATTER FROM (-7199834944475770217) TO (-7199834944475770217): pq:
			// kv/dist_sender.go:918: truncation resulted in empty batch on
			// /Table/52/1/-71998349444
			//
			// Perhaps the scatter invocation is wrong (it has FROM==TO), but this
			// looks concerning.
			"./workload run kv --tolerate-errors --init" + loadDuration + " {pgurl:1-%d}",
		}

		// TODO(tschottdorf): `c.newMonitor` is far from suitable for tests that actually restart nodes
		// and trying to fix it up does not seem worth it. What would work well here is a monitor that
		// connects directly to the nodes' PG endpoints and polls their uptime, informing a callback as
		// needed.
		var m *errgroup.Group
		m, ctx = errgroup.WithContext(ctx)
		for i, cmd := range workloads {
			cmd := cmd // loop-local copy
			i := i     // ditto
			m.Go(func() error {
				cmd = fmt.Sprintf(cmd, nodes)
				// TODO(tschottdorf): we need to be able to cleanly terminate processes we
				// started. In this test, we'd like to send a signal to workload when the
				// upgrade goroutine has decided the time has come. Perhaps context
				// cancellation can be used for that purpose, but it doesn't seem quite
				// right. For now, hold over water with durations, but those don't measure
				// init time correctly (which can be quite substantial).
				// TODO(tschottdorf): It's a bit silly that we use a dedicated load gen
				// machine here, but `c.Stop` calls `roachprod stop` and that kills
				// *everything* on that machine, not just the cockroach process.
				quietL, err := newLogger(cmd, strconv.Itoa(i), "workload"+strconv.Itoa(i), ioutil.Discard, os.Stderr)
				if err != nil {
					return err
				}
				return c.RunL(ctx, quietL, c.Node(nodes+1), cmd)
			})
		}

		m.Go(func() error {
			// NB: the number of calls to `sleep` needs to be reflected in `loadDuration`.
			sleepAndCheck := func() error {
				t.Status("sleeping")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(stageDuration):
				}
				// Make sure everyone is still running.
				for i := 1; i <= nodes; i++ {
					t.Status("checking ", i)
					db := c.Conn(ctx, 1)
					defer db.Close()
					rows, err := db.Query(`SHOW DATABASES`)
					if err != nil {
						return err
					}
					if err := rows.Close(); err != nil {
						return err
					}
				}
				return nil
			}

			db := c.Conn(ctx, 1)
			defer db.Close()

			// First let the load generators run in the cluster at `version`.
			if err := sleepAndCheck(); err != nil {
				return err
			}

			stop := func(node int) error {
				port := fmt.Sprintf("{pgport:%d}", node)
				if err := c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --port "+port); err != nil {
					return err
				}
				// NB: we still call Stop to make sure the process is dead when we try
				// to restart it (or we'll catch an error from the RocksDB dir being
				// locked). This won't happen unless run with --local due to timing.
				// However, it serves as a reminder that `./cockroach quit` doesn't yet
				// work well enough -- ideally all listeners and engines are closed by
				// the time it returns to the client.
				//
				// TODO(tschottdorf): should return an error. I doubt that we want to
				// call these *testing.T-style methods on goroutines.
				c.Stop(ctx, c.Node(node))
				return nil
			}

			// Now perform a rolling restart into the new binary.
			for i := 1; i <= nodes; i++ {
				t.Status("upgrading ", i)
				if err := stop(i); err != nil {
					return err
				}
				c.Put(ctx, cockroach, "./cockroach", c.Node(i))
				c.Start(ctx, c.Node(i))
				if err := sleepAndCheck(); err != nil {
					return err
				}
			}

			// Changed our mind, let's roll that back.
			for i := 1; i <= nodes; i++ {
				t.Status("downgrading", i)
				if err := stop(i); err != nil {
					return err
				}
				c.Put(ctx, b, "./cockroach", c.Node(i))
				c.Start(ctx, c.Node(i))
				if err := sleepAndCheck(); err != nil {
					return err
				}
			}

			// OK, let's go forward again.
			for i := 1; i <= nodes; i++ {
				t.Status("upgrading", i, "(again)")
				if err := stop(i); err != nil {
					return err
				}
				c.Put(ctx, cockroach, "./cockroach", c.Node(i))
				c.Start(ctx, c.Node(i))
				if err := sleepAndCheck(); err != nil {
					return err
				}
			}

			// Finally, bump the cluster version (completing the upgrade).
			t.Status("bumping cluster version")
			if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING version = crdb_internal.node_executable_version()`); err != nil {
				return err
			}

			return sleepAndCheck()
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	const version = "v1.1.5"
	for _, n := range []int{3, 5} {
		tests.Add(testSpec{
			Name:  fmt.Sprintf("version/mixedWith=%s/nodes=%d", version, n),
			Nodes: nodes(n + 1),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runVersion(ctx, t, c, version)
			},
		})
	}
}
