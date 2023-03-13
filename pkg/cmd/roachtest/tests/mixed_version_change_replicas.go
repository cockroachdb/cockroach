// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerChangeReplicasMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "change-replicas/mixed-version",
		Owner:   registry.OwnerReplication,
		Cluster: r.MakeClusterSpec(4),
		Run:     runChangeReplicasMixedVersion,
		Timeout: 20 * time.Minute,
	})
}

// runChangeReplicasMixedVersion is a regression test for
// https://github.com/cockroachdb/cockroach/issues/94834. It runs replica config
// changes (moves replicas around) in mixed-version clusters, both explicitly
// with ALTER RANGE RELOCATE and implicitly via zone configs and the replicate
// queue. It does so in several sequential scenarios:
//
// 1. Mixed pre/main nodes.
// 2. All main nodes, unfinalized.
// 3. All pre nodes, downgraded from main.
// 4. All main nodes, finalized.
func runChangeReplicasMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodeCount := c.Spec().NodeCount
	require.Equal(t, 4, nodeCount)

	rng, _ := randutil.NewTestRand()
	randomNodeID := func() int {
		return rng.Intn(nodeCount) + 1
	}

	preVersion, err := version.PredecessorVersion(*t.BuildVersion())
	require.NoError(t, err)

	// scanTableStep runs a count(*) scan across a table, asserting the row count.
	scanTableStep := func(table string, expectRows int) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			gateway := randomNodeID()
			t.L().Printf("scanning table %s via gateway n%d", table, gateway)
			conn := u.c.Conn(ctx, t.L(), gateway)
			defer conn.Close()
			var count int
			row := conn.QueryRowContext(ctx, `SELECT count(*) FROM `+table)
			require.NoError(t, row.Scan(&count))
			require.Equal(t, expectRows, count)
		}
	}

	// scatterTableStep scatters the replicas and leases for a table.
	scatterTableStep := func(table string) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			gateway := randomNodeID()
			t.L().Printf("scattering table %s via n%d", table, gateway)
			conn := u.c.Conn(ctx, t.L(), gateway)
			defer conn.Close()
			_, err = conn.ExecContext(ctx, `ALTER TABLE test SCATTER`)
			require.NoError(t, err)
		}
	}

	// changeReplicasRelocateFromNodeStep moves all table replicas from the given
	// node onto any other node that doesn't already have a replica, using ALTER
	// TABLE RELOCATE via a random gateway node.
	changeReplicasRelocateFromNodeStep := func(table string, nodeID int) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {

			// Disable the replicate queue, but re-enable it when we're done.
			setReplicateQueueEnabled := func(enabled bool) {
				for n := 1; n <= nodeCount; n++ {
					conn := u.c.Conn(ctx, t.L(), n)
					defer conn.Close()
					_, err := conn.ExecContext(ctx,
						`SELECT crdb_internal.kv_set_queue_active('replicate', $1)`, enabled)
					require.NoError(t, err)
				}
			}
			setReplicateQueueEnabled(false)
			defer setReplicateQueueEnabled(true)

			// Relocate replicas to other nodes which don't have one, in random order.
			targets := []int{}
			for n := 1; n <= nodeCount; n++ {
				if n != nodeID {
					targets = append(targets, n)
				}
			}
			rand.Shuffle(len(targets), func(i, j int) {
				targets[i], targets[j] = targets[j], targets[i]
			})

			for _, target := range targets {
				gateway := randomNodeID()
				conn := u.c.Conn(ctx, t.L(), gateway)
				defer conn.Close()

				retryOpts := retry.Options{
					InitialBackoff: 100 * time.Millisecond,
					MaxBackoff:     5 * time.Second,
					Multiplier:     2,
					MaxRetries:     12,
				}
				var rangeErrors map[int]string
				for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
					setReplicateQueueEnabled(false)
					if errCount := len(rangeErrors); errCount > 0 {
						t.L().Printf("%d ranges failed, retrying", errCount)
					}
					t.L().Printf("moving replicas from n%d to n%d via gateway n%d using ALTER TABLE RELOCATE",
						nodeID, target, gateway)

					var rangeID int
					var pretty, result string
					rows, err := conn.QueryContext(ctx, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
						`SELECT range_id FROM [SHOW RANGES FROM TABLE test] `+
						`WHERE $1::int = ANY(replicas) AND $2::int != ALL(replicas)`, nodeID, target)
					require.NoError(t, err)

					rangeErrors = map[int]string{}
					for rows.Next() {
						require.NoError(t, rows.Scan(&rangeID, &pretty, &result))
						if result != "ok" {
							rangeErrors[rangeID] = result
						}
					}
					require.NoError(t, rows.Err())
					if len(rangeErrors) == 0 {
						break
					}
					// The failure may be caused by conflicts with ongoing configuration
					// changes by the replicate queue, so we re-enable it and let it run
					// for a bit before the next retry.
					setReplicateQueueEnabled(true)
				}

				if len(rangeErrors) > 0 {
					for rangeID, result := range rangeErrors {
						t.L().Printf("failed to move r%d from n%d to n%d via n%d: %s",
							rangeID, nodeID, target, gateway, result)
					}
					t.Fatalf("failed to move %d replicas from n%d to n%d using gateway n%d",
						len(rangeErrors), nodeID, target, gateway)
				}
			}
		}
	}

	// changeReplicasZoneConfigFromNodeStep moves all table replicas from the
	// given node onto any other node that doesn't already have a replica, using a
	// zone config exclusion and manual enqueueing.
	changeReplicasZoneConfigFromNodeStep := func(table string, nodeID int) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			conns := map[int]*gosql.DB{}
			for n := 1; n <= nodeCount; n++ {
				conns[n] = u.c.Conn(ctx, t.L(), n)
				defer conns[n].Close()
			}
			gateway := randomNodeID()
			conn := conns[gateway]

			t.L().Printf("moving replicas off n%d using zone config via gateway n%d", nodeID, gateway)

			// Set zone constraint to exclude the node.
			_, err := conn.ExecContext(ctx, fmt.Sprintf(
				`ALTER TABLE %s CONFIGURE ZONE USING constraints = '[-node%d]'`, table, nodeID))
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				// Manually enqueue ranges across all nodes, since queues are slow.
				for n, c := range conns {
					if u.binaryVersion(ctx, t, n).LessEq(roachpb.MustParseVersion("22.2")) {
						_, err = c.ExecContext(ctx, fmt.Sprintf(
							`SELECT crdb_internal.kv_enqueue_replica(range_id, 'replicate', true) `+
								`FROM [SHOW RANGES FROM TABLE %s] WHERE lease_holder = %d`, table, n))
					} else {
						_, err = c.ExecContext(ctx, fmt.Sprintf(
							`SELECT crdb_internal.kv_enqueue_replica(range_id, 'replicate', true) `+
								`FROM [SHOW RANGES FROM TABLE %s WITH DETAILS] WHERE lease_holder = %d`, table, n))
					}
					if err != nil {
						t.L().Printf("kv_enqueue_replica failed: %s", err)
					}
				}

				// Check if ranges have moved yet.
				var rangeCount int
				row := conn.QueryRowContext(ctx, `SELECT count(*) FROM `+
					`[SHOW RANGES FROM TABLE test] WHERE $1::int = ANY(replicas)`, nodeID)
				require.NoError(t, row.Scan(&rangeCount))
				t.L().Printf("table %s has %d replicas on n%d", table, rangeCount, nodeID)
				return rangeCount == 0
			}, 2*time.Minute, time.Second)

			// Reset zone constraint.
			_, err = conn.ExecContext(ctx, fmt.Sprintf(
				`ALTER TABLE %s CONFIGURE ZONE USING constraints = '[]'`, table))
			require.NoError(t, err)
		}
	}

	u := newVersionUpgradeTest(c,
		// Start the cluster with preVersion and wait for it to bootstrap, then
		// disable auto-upgrades to MainVersion. The checkpoint fixture is not
		// necessary in this test, but we pull it in to get better test coverage of
		// historical cluster state.
		uploadAndStartFromCheckpointFixture(c.All(), preVersion),
		waitForUpgradeStep(c.All()),
		preventAutoUpgradeStep(1),

		// Create a test table and wait for upreplication.
		func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			conn := u.c.Conn(ctx, t.L(), 1)
			defer conn.Close()
			_, err = conn.ExecContext(ctx, `CREATE TABLE test (id INT PRIMARY KEY)`)
			require.NoError(t, err)
			require.NoError(t, WaitFor3XReplication(ctx, t, conn))
		},

		// Upgrade n1,n2 to MainVersion, leave n3,n4 at preVersion.
		binaryUpgradeStep(c.Nodes(1, 2), clusterupgrade.MainVersion),

		// Scatter the table's single range, to randomize replica/lease
		// placement -- in particular, who's responsible for splits.
		scatterTableStep("test"),

		// Create 100 splits of the test table.
		func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			const ranges = 100
			gateway := randomNodeID()
			t.L().Printf("splitting table test into %d ranges via gateway n%d", ranges, gateway)

			conn := u.c.Conn(ctx, t.L(), gateway)
			defer conn.Close()
			_, err = conn.ExecContext(ctx,
				`ALTER TABLE test SPLIT AT SELECT i FROM generate_series(1, $1) AS g(i)`, ranges-1)
			require.NoError(t, err)

			var rangeCount int
			row := conn.QueryRowContext(ctx, `SELECT count(*) FROM [SHOW RANGES FROM TABLE test]`)
			require.NoError(t, row.Scan(&rangeCount))
			require.Equal(t, ranges, rangeCount)
		},

		// Scatter the table after the splits, and run a table scan.
		scatterTableStep("test"),
		scanTableStep("test", 0),

		// Move all replicas off of each node using ALTER RANGE RELOCATE.
		changeReplicasRelocateFromNodeStep("test", 1),
		changeReplicasRelocateFromNodeStep("test", 2),
		changeReplicasRelocateFromNodeStep("test", 3),
		changeReplicasRelocateFromNodeStep("test", 4),

		// Scatter the table again, and run a table scan.
		scatterTableStep("test"),
		scanTableStep("test", 0),

		// Move all replicas off of each node using a zone config.
		changeReplicasZoneConfigFromNodeStep("test", 1),
		changeReplicasZoneConfigFromNodeStep("test", 2),
		changeReplicasZoneConfigFromNodeStep("test", 3),
		changeReplicasZoneConfigFromNodeStep("test", 4),

		// Scatter the table again, and run a table scan.
		scatterTableStep("test"),
		scanTableStep("test", 0),

		// Upgrade n3,n4 (the remaining nodes) to MainVersion, verify that we can
		// run a table scan, move ranges around, scatter, and scan again.
		binaryUpgradeStep(c.Nodes(3, 4), clusterupgrade.MainVersion),
		scanTableStep("test", 0),
		changeReplicasRelocateFromNodeStep("test", 1),
		changeReplicasZoneConfigFromNodeStep("test", 2),
		changeReplicasRelocateFromNodeStep("test", 3),
		changeReplicasZoneConfigFromNodeStep("test", 3),
		scatterTableStep("test"),
		scanTableStep("test", 0),

		// Downgrade all to preVersion, verify that we can run a table scan, shuffle
		// the ranges, scatter them, and scan again.
		binaryUpgradeStep(c.All(), preVersion),
		scanTableStep("test", 0),
		changeReplicasRelocateFromNodeStep("test", 1),
		changeReplicasZoneConfigFromNodeStep("test", 2),
		changeReplicasRelocateFromNodeStep("test", 3),
		changeReplicasZoneConfigFromNodeStep("test", 3),
		scatterTableStep("test"),
		scanTableStep("test", 0),

		// Upgrade all to MainVersion and finalize the upgrade. Verify that we can
		// run a table scan, shuffle the ranges, scatter them, and scan again.
		allowAutoUpgradeStep(1),
		binaryUpgradeStep(c.All(), clusterupgrade.MainVersion),
		waitForUpgradeStep(c.All()),
		scanTableStep("test", 0),
		changeReplicasRelocateFromNodeStep("test", 1),
		changeReplicasZoneConfigFromNodeStep("test", 2),
		changeReplicasRelocateFromNodeStep("test", 3),
		changeReplicasZoneConfigFromNodeStep("test", 3),
		scatterTableStep("test"),
		scanTableStep("test", 0),
	)

	u.run(ctx, t)
}
