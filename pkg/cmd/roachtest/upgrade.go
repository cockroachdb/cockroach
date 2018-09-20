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
	"math/rand"
	"runtime"
	"time"

	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
		// Force disable encryption.
		// TODO(mberhault): allow it once oldVersion >= 2.1.
		c.Start(ctx, c.Range(1, nodes), startArgsDontEncrypt)

		const stageDuration = 30 * time.Second
		const timeUntilStoreDead = 90 * time.Second
		const buff = 10 * time.Second

		sleep := func(ts time.Duration) error {
			t.WorkerStatus("sleeping")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(ts):
				return nil
			}
		}

		db := c.Conn(ctx, 1)
		defer db.Close()

		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING server.time_until_store_dead = $1", timeUntilStoreDead.String(),
		); err != nil {
			t.Fatal(err)
		}

		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		stop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			// Note that the following command line needs to run against both v2.0
			// and the current branch. Do not change it in a manner that is
			// incompatible with 2.0.
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --port="+port); err != nil {
				return err
			}
			c.Stop(ctx, c.Node(node))
			return nil
		}

		decommissionAndStop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			// Note that the following command line needs to run against both v2.0
			// and the current branch. Do not change it in a manner that is
			// incompatible with 2.0.
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --decommission --insecure --port="+port); err != nil {
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
			upgradedVersion, err := clusterVersion()
			if err != nil {
				return false, err
			}
			return upgradedVersion != oldVersion, nil
		}

		checkDowngradeOption := func(version string) error {
			if _, err := db.ExecContext(ctx,
				"SET CLUSTER SETTING cluster.preserve_downgrade_option = $1;", version,
			); err == nil {
				return fmt.Errorf("cluster.preserve_downgrade_option shouldn't be set to any other values besides current cluster version")
			} else if !testutils.IsError(err, "cannot set cluster.preserve_downgrade_option") {
				return err
			}
			return nil
		}

		// Now perform a rolling restart into the new binary, except the last node.
		for i := 1; i < nodes; i++ {
			t.WorkerStatus("upgrading ", i)
			if err := stop(i); err != nil {
				t.Fatal(err)
			}
			c.Put(ctx, cockroach, "./cockroach", c.Node(i))
			c.Start(ctx, c.Node(i), startArgsDontEncrypt)
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
		c.Start(ctx, c.Node(nodes), startArgsDontEncrypt)
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
		if err := decommissionAndStop(nodes - 2); err != nil {
			t.Fatal(err)
		}
		if err := sleep(timeUntilStoreDead + buff); err != nil {
			t.Fatal(err)
		}

		// Check cannot set cluster setting cluster.preserve_downgrade_option to any
		// value besides the old cluster version.
		if err := checkDowngradeOption("1.1"); err != nil {
			t.Fatal(err)
		}
		if err := checkDowngradeOption("2.2"); err != nil {
			t.Fatal(err)
		}

		// Set cluster setting cluster.preserve_downgrade_option to be current
		// cluster version to prevent upgrade.
		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING cluster.preserve_downgrade_option = $1;", oldVersion,
		); err != nil {
			t.Fatal(err)
		}
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		// Restart the previously stopped node.
		c.Start(ctx, c.Node(nodes-1), startArgsDontEncrypt)
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		t.WorkerStatus("check cluster version has not been upgraded")
		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if upgraded {
			t.Fatal("cluster setting version shouldn't be upgraded because cluster.preserve_downgrade_option is set properly")
		}

		// Check cannot set cluster setting version until cluster.preserve_downgrade_option
		// is cleared.
		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING version = crdb_internal.node_executable_version();",
		); err == nil {
			t.Fatal("should not be able to set cluster setting version before resetting cluster.preserve_downgrade_option")
		} else if !testutils.IsError(err, "cluster.preserve_downgrade_option is set to") {
			t.Fatal(err)
		}

		// Reset cluster.preserve_downgrade_option to enable upgrade.
		if _, err := db.ExecContext(ctx,
			"RESET CLUSTER SETTING cluster.preserve_downgrade_option;",
		); err != nil {
			t.Fatal(err)
		}
		if err := sleep(stageDuration); err != nil {
			t.Fatal(err)
		}

		// Check if the cluster version has been upgraded.
		t.WorkerStatus("check cluster version has been upgraded")
		if upgraded, err := checkUpgraded(); err != nil {
			t.Fatal(err)
		} else if !upgraded {
			t.Fatalf("cluster setting version is not upgraded, still %s", oldVersion)
		}

		// Finally, check if the cluster.preserve_downgrade_option has been reset.
		t.WorkerStatus("check cluster setting cluster.preserve_downgrade_option has been set to an empty string")
		var downgradeVersion string
		if err := db.QueryRowContext(ctx,
			"SHOW CLUSTER SETTING cluster.preserve_downgrade_option",
		).Scan(&downgradeVersion); err != nil {
			t.Fatal(err)
		}
		if downgradeVersion != "" {
			t.Fatalf("cluster setting cluster.preserve_downgrade_option is %s, should be an empty string", downgradeVersion)
		}
	}

	const oldVersion = "v2.0.5"
	for _, n := range []int{5} {
		r.Add(testSpec{
			Name:       fmt.Sprintf("upgrade/oldVersion=%s/nodes=%d", oldVersion, n),
			MinVersion: "v2.1.0",
			Nodes:      nodes(n),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runUpgrade(ctx, t, c, oldVersion)
			},
		})
	}
}

func runVersionUpgrade(ctx context.Context, t *test, c *cluster) {
	nodes := c.Range(1, 3)
	goos := ifLocal(runtime.GOOS, "linux")
	const headVersion = "HEAD"

	// versionStep is an isolated version migration on a running cluster.
	type versionStep struct {
		clusterVersion string
		run            func()
	}

	uploadVersion := func(newVersion string) option {
		var binary string
		if newVersion == headVersion {
			binary = cockroach
		} else {
			var err error
			binary, err = binfetcher.Download(ctx, binfetcher.Options{
				Binary:  "cockroach",
				Version: newVersion,
				GOOS:    goos,
				GOARCH:  "amd64",
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		target := "./cockroach-" + newVersion
		c.Put(ctx, binary, target, nodes)
		return startArgs("--binary=" + target)
	}

	checkNode := func(nodeIdx int, newVersion string) {
		err := retry.ForDuration(30*time.Second, func() error {
			db := c.Conn(ctx, nodeIdx)
			defer db.Close()

			// 'Version' for 1.1, 'Tag' in 1.0.x.
			var version string
			if err := db.QueryRow(
				`SELECT value FROM crdb_internal.node_build_info where field IN ('Version' , 'Tag')`,
			).Scan(&version); err != nil {
				return err
			}
			if version != newVersion && newVersion != headVersion {
				t.Fatalf("created node at v%s, but it is %s", newVersion, version)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// binaryVersionUpgrade performs a rolling upgrade of the specified nodes in
	// the cluster.
	binaryVersionUpgrade := func(newVersion string, nodes nodeListOption) versionStep {
		return versionStep{
			run: func() {
				c.l.Printf("%s: binary\n", newVersion)
				args := uploadVersion(newVersion)

				// Restart nodes in a random order; otherwise node 1 would be running all
				// the migrations and it probably also has all the leases.
				rand.Shuffle(len(nodes), func(i, j int) {
					nodes[i], nodes[j] = nodes[j], nodes[i]
				})
				for _, node := range nodes {
					c.l.Printf("%s: upgrading node %d\n", newVersion, node)
					c.Stop(ctx, c.Node(node))
					c.Start(ctx, c.Node(node), args)

					checkNode(node, newVersion)

					// TODO(nvanbenschoten): add upgrade qualification step. What should we
					// test? We could run logictests. We could add custom logic here. Maybe
					// this should all be pushed to nightly migration tests instead.
					time.Sleep(1 * time.Second)
				}
			},
		}
	}

	// clusterVersionUpgrade performs a cluster version upgrade to its version.
	// It waits until all nodes have seen the upgraded cluster version.
	var currentVersion string
	clusterVersionUpgrade := func(newVersion string, manual bool) versionStep {
		return versionStep{
			clusterVersion: newVersion,
			run: func() {
				c.l.Printf("%s: cluster\n", newVersion)

				// hasShowSettingBug is true when we're working around
				// https://github.com/cockroachdb/cockroach/issues/22796.
				//
				// The problem there is that `SHOW CLUSTER SETTING version` does not
				// take into account the gossiped value of that setting but reads it
				// straight from the KV store. This means that even though a node may
				// report a certain version, it may not actually have processed it yet,
				// which leads to illegal upgrades in this test. When this flag is set
				// to true, we query `crdb_internal.cluster_settings` instead, which
				// *does* take everything from Gossip.
				v, err := roachpb.ParseVersion(newVersion)
				if err != nil {
					t.Fatal(err)
				}
				hasShowSettingBug := v.Less(roachpb.Version{Major: 1, Minor: 1, Unstable: 1})

				if manual {
					func() {
						node := nodes.randNode()[0]
						db := c.Conn(ctx, node)
						defer db.Close()

						c.l.Printf("%s: upgrading cluster version (node %d)\n", newVersion, node)
						if _, err := db.Exec(fmt.Sprintf(`SET CLUSTER SETTING version = '%s'`, newVersion)); err != nil {
							t.Fatal(err)
						}
					}()
				}

				if hasShowSettingBug {
					c.l.Printf("%s: using workaround for upgrade\n", newVersion)
				}

				for i := 1; i < c.nodes; i++ {
					err := retry.ForDuration(30*time.Second, func() error {
						db := c.Conn(ctx, i)
						defer db.Close()

						if !hasShowSettingBug {
							if err := db.QueryRow("SHOW CLUSTER SETTING version").Scan(&currentVersion); err != nil {
								t.Fatalf("%d: %s", i, err)
							}
						} else {
							// This uses the receiving node's Gossip and as such allows us to verify that all of the
							// nodes have gotten wind of the version bump.
							if err := db.QueryRow(
								`SELECT current_value FROM crdb_internal.cluster_settings WHERE name = 'version'`,
							).Scan(&currentVersion); err != nil {
								t.Fatalf("%d: %s", i, err)
							}
						}
						if currentVersion != newVersion {
							return fmt.Errorf("%d: expected version %s, got %s", i, newVersion, currentVersion)
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}

				c.l.Printf("%s: cluster is upgraded\n", newVersion)

				// TODO(nvanbenschoten): add upgrade qualification step.
				time.Sleep(1 * time.Second)
			},
		}
	}

	const baseVersion = "v1.0.6"
	steps := []versionStep{
		// v1.1.0 is the first binary version that knows about cluster versions,
		// but thinks it can only support up to 1.0-3.
		binaryVersionUpgrade("v1.1.0", nodes),
		clusterVersionUpgrade("1.0", true /* manual */),
		clusterVersionUpgrade("1.0-3", true /* manual */),

		binaryVersionUpgrade("v1.1.1", nodes),
		clusterVersionUpgrade("1.1", true /* manual */),

		binaryVersionUpgrade("v2.0.0", nodes),
		clusterVersionUpgrade("1.1-6", true /* manual */),
		clusterVersionUpgrade("2.0", true /* manual */),

		binaryVersionUpgrade("HEAD", nodes),
	}

	type feature struct {
		name              string
		minAllowedVersion string
		query             string
	}

	features := []feature{
		{
			name:              "JSONB",
			minAllowedVersion: "2.0-0",
			query: `
	CREATE DATABASE IF NOT EXISTS test;
	CREATE TABLE test.t (j JSONB);
	DROP TABLE test.t;
	`,
		}, {
			name:              "Sequences",
			minAllowedVersion: "2.0-0",
			query: `
	 CREATE DATABASE IF NOT EXISTS test;
	 CREATE SEQUENCE test.test_sequence;
	 DROP SEQUENCE test.test_sequence;
	`,
		}, {
			name:              "Computed Columns",
			minAllowedVersion: "2.0-0",
			query: `
	CREATE DATABASE IF NOT EXISTS test;
	CREATE TABLE test.t (x INT AS (3) STORED);
	DROP TABLE test.t;
	`,
		},
	}

	testFeature := func(f feature, cv string) {
		db := c.Conn(ctx, 1)
		defer db.Close()

		minAllowedVersion, err := roachpb.ParseVersion(f.minAllowedVersion)
		if err != nil {
			t.Fatal(err)
		}
		actualVersion, err := roachpb.ParseVersion(cv)
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(f.query)
		if actualVersion.Less(minAllowedVersion) {
			if err == nil {
				t.Fatalf("expected %s to fail on cluster version %s", f.name, cv)
			}
			c.l.Printf("%s: %s fails expected\n", cv, f.name)
		} else {
			if err != nil {
				t.Fatalf("expected %s to succeed on cluster version %s, got %s", f.name, cv, err)
			}
			c.l.Printf("%s: %s works as expected\n", cv, f.name)
		}
	}

	args := uploadVersion(baseVersion)
	// Hack to skip initializing settings which doesn't work on very old versions
	// of cockroach.
	c.Run(ctx, c.Node(1), "mkdir -p {store-dir} && touch {store-dir}/settings-initialized")
	c.Start(ctx, nodes, args)

	func() {
		// Create a bunch of tables, over the batch size on which some migrations
		// operate. It generally seems like a good idea to have a bunch of tables in
		// the cluster, and we had a bug about migrations on large numbers of tables:
		// #22370.
		db := c.Conn(ctx, 1)
		defer db.Close()
		if _, err := db.Exec(fmt.Sprintf("create database lotsatables")); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			_, err := db.Exec(fmt.Sprintf("create table lotsatables.t%d (x int primary key)", i))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	for _, node := range nodes {
		checkNode(node, baseVersion)
	}

	for _, step := range steps {
		step.run()
		if step.clusterVersion != "" {
			for _, feature := range features {
				testFeature(feature, step.clusterVersion)
			}
		}
	}

	func() {
		db := c.Conn(ctx, 1)
		defer db.Close()

		var nodeVersion string
		if err := db.QueryRow(
			`SELECT crdb_internal.node_executable_version()`,
		).Scan(&nodeVersion); err != nil {
			t.Fatal(err)
		}
		if nodeVersion != currentVersion {
			clusterVersionUpgrade(nodeVersion, false /* manual */).run()
			for _, feature := range features {
				testFeature(feature, nodeVersion)
			}
		}
	}()
}
