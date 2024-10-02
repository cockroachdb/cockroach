// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func registerDiskFull(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "disk-full",
		Owner:            registry.OwnerStorage,
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode()),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() {
				t.Skip("you probably don't want to fill your local disk")
			}

			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

			// Node 1 will soon be killed, when the ballast file fills up its disk. To
			// ensure that the ranges containing system tables are available on other
			// nodes, we wait here for 3x replication of each range. Without this,
			// it's possible that we end up deadlocked on a system query that requires
			// a range on node 1, but node 1 will not restart until the query
			// completes.
			db := c.Conn(ctx, t.L(), 1)
			err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
			require.NoError(t, err)
			_ = db.Close()

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf(
					"./cockroach workload run kv --tolerate-errors --init --read-percent=0"+
						" --concurrency=10 --duration=4m {pgurl:2-%d}",
					len(c.CRDBNodes()))
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
				return nil
			})

			// Each node should have an automatically created
			// EMERGENCY_BALLAST file in the auxiliary directory.
			c.Run(ctx, option.WithNodes(c.CRDBNodes()), "stat {store-dir}/auxiliary/EMERGENCY_BALLAST")

			m.Go(func(ctx context.Context) error {
				const n = 1

				t.L().Printf("filling disk on %d\n", n)
				// Create a manual ballast that fills up the entire disk
				// (size=100%). The "|| true" is used to ignore the
				// error returned by `debug ballast`.
				m.ExpectDeath()
				c.Run(ctx, option.WithNodes(c.Node(n)), "./cockroach debug ballast {store-dir}/largefile --size=100% || true")

				// Node 1 should forcibly exit due to a full disk.
				for isLive := true; isLive; {
					db := c.Conn(ctx, t.L(), 2)
					err := db.QueryRow(`SELECT is_live FROM crdb_internal.gossip_nodes WHERE node_id = 1;`).Scan(&isLive)
					if err != nil {
						t.Fatal(err)
					}
					if isLive {
						t.L().Printf("waiting for n%d to die due to full disk\n", n)
						time.Sleep(time.Second)
					}
				}

				t.L().Printf("node n%d died as expected\n", n)

				// Restart cockroach in a loop for 30s.
				for start := timeutil.Now(); timeutil.Since(start) < 30*time.Second; {
					if t.Failed() {
						return nil
					}
					t.L().Printf("starting n%d when disk is full\n", n)

					// We expect cockroach to die during startup with
					// exit code 10 (Disk Full). Just in case the
					// monitor detects the death, expect it.
					m.ExpectDeath()

					err := c.StartE(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(n))
					t.L().Printf("starting n%d: error %v", n, err)
					if err == nil {
						t.Fatal("node successfully started unexpectedly")
					} else if strings.Contains(cluster.GetStderr(err), "a panic has occurred") {
						t.Fatal(err)
					}

					// The error returned from StartE, which is
					// propagated from roachprod, obscures the Cockroach
					// exit code. There should still be a record of it
					// in the systemd logs.
					result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(n)), fmt.Sprintf(
						`systemctl status %s | grep 'Main PID' | grep -oE '\((.+)\)'`,
						roachtestutil.SystemInterfaceSystemdUnitName(),
					))
					if err != nil {
						t.Fatal(err)
					}
					exitLogs := strings.TrimSpace(result.Stdout)
					const want = `(code=exited, status=10)`
					if exitLogs != want {
						t.Fatalf("cockroach systemd status: got %q, want %q", exitLogs, want)
					}
				}

				// Clear the emergency ballast. Clearing the ballast
				// should allow the node to start, perform compactions,
				// etc. Allow a death here as the monitor may detect the
				// node is still dead until the node has had its ballast
				// file removed and has been successfully restarted.
				t.L().Printf("removing the emergency ballast on n%d\n", n)
				m.ExpectDeath()
				c.Run(ctx, option.WithNodes(c.Node(n)), "rm -f {store-dir}/auxiliary/EMERGENCY_BALLAST")
				if err := c.StartE(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(n)); err != nil {
					t.Fatal(err)
				}
				m.ResetDeaths()

				// Wait a little while and delete the large file we
				// added to induce the out-of-disk condition.
				time.Sleep(30 * time.Second)
				t.L().Printf("removing n%d's large file to free up available disk space.\n", n)
				c.Run(ctx, option.WithNodes(c.Node(n)), "rm -f {store-dir}/largefile")

				// When CockroachDB detects that it has sufficient
				// capacity available, it should recreate the emergency
				// ballast file automatically.
				t.L().Printf("waiting for node n%d's emergency ballast to be restored.\n", n)
				for {
					err := c.RunE(ctx, option.WithNodes(c.Node(1)), "stat {store-dir}/auxiliary/EMERGENCY_BALLAST")
					if err == nil {
						return nil
					}
					t.L().Printf("node n%d's emergency ballast doesn't exist yet\n", n)
					time.Sleep(time.Second)
				}
			})
			m.Wait()
		},
	})
}
