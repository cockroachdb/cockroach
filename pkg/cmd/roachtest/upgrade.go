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
	"runtime"
	"time"

	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
)

func registerUpgrade(r *registry) {
	runUpgrade := func(ctx context.Context, t *test, c *cluster, oldVersion string) {
		nodes := c.nodes
		goos := ifLocal(runtime.GOOS, "linux")

		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: oldVersion,
			GOOS:    goos,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Put(ctx, b, "./cockroach", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes))

		stageDuration := 30 * time.Second

		sleep := func(ts time.Duration) error {
			t.Status("sleeping")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(ts):
				return nil
			}
		}

		db := c.Conn(ctx, 1)
		defer db.Close()

		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		stop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --port "+port); err != nil {
				return err
			}
			c.Stop(ctx, c.Node(node))
			return nil
		}

		decommission := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --decommission --insecure --port "+port); err != nil {
				return err
			}
			c.Stop(ctx, c.Node(node))
			return nil
		}

		clusterVersion := func() (string, error) {
			var version string
			if err := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`).Scan(&version); err != nil {
				return "", err
			}
			return version, nil
		}

		oldVersion, err = clusterVersion()
		if err != nil {
			t.Fatal(err)
		}

		checkUpgraded := func() (bool, error) {
			if upgradedVersion, err := clusterVersion(); err != nil {
				return false, err
			} else if upgradedVersion == oldVersion {
				return false, nil
			}
			return true, nil
		}

		// Now perform a rolling restart into the new binary, except the last node.
		t.Status("check cluster version not upgraded")
		for i := 1; i < nodes; i++ {
			t.Status("upgrading ", i)
			if err := stop(i); err != nil {
				t.Fatal(err)
			}
			c.Put(ctx, cockroach, "./cockroach", c.Node(i))
			c.Start(ctx, c.Node(i))
			if err := sleep(stageDuration); err != nil {
				t.Fatal(err)
			}

			// Check cluster version is not upgraded until all nodes are running the new version.
			if upgraded, err := checkUpgraded(); err != nil {
				t.Fatal(err)
			} else if upgraded {
				t.Fatal("cluster setting version shouldn't be upgraded before all nodes are running the new version")
			}
		}

		// Now stop a previously started node and upgrade the last node.
		// Check cluster version is not upgraded.
		if err := stop(nodes - 1); err != nil {
			t.Fatal(err)
		}
		if err := stop(nodes); err != nil {
			t.Fatal(err)
		}
		c.Put(ctx, cockroach, "./cockroach", c.Node(nodes))
		c.Start(ctx, c.Node(nodes))
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if upgraded {
			t.Fatal("cluster setting version shouldn't be upgraded before all non-decommissioned nodes are alive")
		}

		// Now decommission and stop the second last node.
		// The decommissioned nodes should not prevent auto upgrade.
		if err := decommission(nodes - 2); err != nil {
			t.Fatal(err)
		}
		if err := sleep(6 * time.Minute); err != nil {
			t.Fatal(err)
		}

		// Set cluster setting cluster.preserve_downgrade_option to be current
		// cluster version to prevent upgrade.
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("SET CLUSTER SETTING cluster.preserve_downgrade_option = '%s';", oldVersion),
		); err != nil {
			t.Fatal(err)
		}
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		// Restart the previously stopped node.
		c.Put(ctx, cockroach, "./cockroach", c.Node(nodes-1))
		c.Start(ctx, c.Node(nodes-1))
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		t.Status("check cluster version has not been upgraded")
		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if upgraded {
			t.Fatal("cluster setting version shouldn't be upgraded because cluster.preserve_downgrade_option is set properly")
		}

		// Reset cluster.preserve_downgrade_option to enable upgrade.
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("RESET CLUSTER SETTING cluster.preserve_downgrade_option;"),
		); err != nil {
			t.Fatal(err)
		}
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		// Check if the cluster version has been upgraded.
		t.Status("check cluster version has been upgraded")
		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if !upgraded {
			t.Fatalf("cluster setting version is not upgraded, still %s", oldVersion)
		}

		// Finally, check if the cluster.preserve_downgrade_option has been reset.
		t.Status("check cluster setting cluster.preserve_downgrade_option has been set to an empty string")
		var downgradeVersion string
		if err := db.QueryRowContext(ctx,
			`SHOW CLUSTER SETTING cluster.preserve_downgrade_option`,
		).Scan(&downgradeVersion); err != nil {
			t.Fatal(err)
		}
		if downgradeVersion != "" {
			t.Fatalf("cluster setting cluster.preserve_downgrade_option is %s, should be an empty string", downgradeVersion)
		}
	}

	const oldVersion = "v2.0.0"
	for _, n := range []int{5, 7} {
		r.Add(testSpec{
			Name:  fmt.Sprintf("upgrade/oldVersion=%s/nodes=%d", oldVersion, n),
			Nodes: nodes(n),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runUpgrade(ctx, t, c, oldVersion)
			},
		})
	}
}
