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
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func registerChangeReplicasMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "change-replicas/mixed-version",
		Owner:            registry.OwnerReplication,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run:              runChangeReplicasMixedVersion,
		Timeout:          60 * time.Minute,
	})
}

// runChangeReplicasMixedVersion is a regression test for
// https://github.com/cockroachdb/cockroach/issues/94834. It runs replica config
// changes (moves replicas around) in mixed-version clusters, both explicitly
// with ALTER RANGE RELOCATE and implicitly via zone configs and the replicate
// queue.
func runChangeReplicasMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {

	// createTable creates a test table, and splits and scatters it.
	createTable := func(
		ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
	) error {

		l.Printf("creating table")
		if err := h.Exec(r, `CREATE TABLE test (id INT PRIMARY KEY)`); err != nil {
			return err
		}
		_, db := h.RandomDB(r, c.All())
		if err := WaitFor3XReplication(ctx, t, db); err != nil {
			return err
		}

		l.Printf("splitting table")
		const ranges = 100
		err := h.Exec(r, `ALTER TABLE test SPLIT AT SELECT i FROM generate_series(1, $1) AS g(i)`,
			ranges-1)
		if err != nil {
			return err
		}

		l.Printf("scattering table")
		if err := h.Exec(r, `ALTER TABLE test SCATTER`); err != nil {
			return err
		}
		return nil
	}

	// evacuateNodeUsingZoneConfig moves replicas off of a node using a zone
	// config.
	evacuateNodeUsingZoneConfig := func(
		ctx context.Context,
		l *logger.Logger,
		r *rand.Rand,
		h *mixedversion.Helper,
		node int,
	) error {
		l.Printf("moving replicas off of n%d using zone config", node)

		err := h.Exec(r, fmt.Sprintf(
			`ALTER TABLE test CONFIGURE ZONE USING constraints = '[-node%d]'`, node))
		if err != nil {
			return err
		}

		var rangeCount int
		for i := 0; i < 30; i++ {
			err := h.QueryRow(r, `SELECT count(*) FROM `+
				`[SHOW RANGES FROM TABLE test] WHERE $1::int = ANY(replicas)`, node).Scan(&rangeCount)
			if err != nil {
				return err
			}
			l.Printf("%d replicas on n%d", rangeCount, node)
			if rangeCount == 0 {
				break
			}
			time.Sleep(3 * time.Second)
		}
		if rangeCount > 0 {
			return errors.Errorf("n%d still has %d replicas", node, rangeCount)
		}

		if err = h.Exec(r, `ALTER TABLE test CONFIGURE ZONE USING constraints = '[]'`); err != nil {
			return err
		}

		return nil
	}

	// evacuateNodeUsingRelocate moves replicas off of a node using ALTER TABLE
	// RELOCATE.
	evacuateNodeUsingRelocate := func(
		ctx context.Context,
		l *logger.Logger,
		r *rand.Rand,
		h *mixedversion.Helper,
		node int,
	) error {
		setReplicateQueueEnabled := func(enabled bool) error {
			for _, node := range c.All() {
				_, err := h.Connect(node).ExecContext(ctx,
					`SELECT crdb_internal.kv_set_queue_active('replicate', $1)`, enabled)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// Relocate replicas to other nodes which don't already have one.
		for _, target := range c.All() {
			if target == node {
				continue
			}

			var rangeErrors map[int]string
			for i := 0; i < 30; i++ {
				l.Printf("moving replicas from n%d to n%d using ALTER TABLE RELOCATE", node, target)

				// Disable the replicate queue, to avoid interference.
				if err := setReplicateQueueEnabled(false); err != nil {
					return err
				}

				// Relocate ranges.
				rows, err := h.Query(r, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
					`SELECT range_id FROM [SHOW RANGES FROM TABLE test] `+
					`WHERE $1::int = ANY(replicas) AND $2::int != ALL(replicas)`, node, target)
				if err != nil {
					return err
				}

				var relocated int
				rangeErrors = map[int]string{}
				for rows.Next() {
					var rangeID int
					var pretty, result string
					if err := rows.Scan(&rangeID, &pretty, &result); err != nil {
						return err
					} else if result != "ok" {
						rangeErrors[rangeID] = result
					} else {
						relocated++
					}
				}
				if err := rows.Err(); err != nil {
					return err
				}

				l.Printf("%d replicas relocated, %d errors", relocated, len(rangeErrors))

				// Re-enable the replicate queue. This is needed not only to reset the
				// state for the test, but also to fix any conflicts with ongoing
				// configuration changes made by the replicate queue before the next
				// retry.
				if err := setReplicateQueueEnabled(true); err != nil {
					return err
				}

				// If all ranges were relocated, move onto the next target.
				if len(rangeErrors) == 0 {
					break
				}

				time.Sleep(3 * time.Second)
			}

			// If ranges still failed after exhausting retries, give up.
			if len(rangeErrors) > 0 {
				for rangeID, result := range rangeErrors {
					t.L().Printf("failed to move r%d from n%d to n%d: %s",
						rangeID, node, target, result)
				}
				return errors.Errorf("failed to move %d replicas from n%d to n%d",
					len(rangeErrors), node, target)
			}
		}

		return nil
	}

	// moveReplicas moves replicas around between nodes randomly.
	moveReplicas := func(
		ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
	) error {

		// First, scatter the range.
		l.Printf("scattering table")
		if err := h.Exec(r, `ALTER TABLE test SCATTER`); err != nil {
			return err
		}

		// Evacuate each node, in random order, using a random mechanism.
		nodes := append(option.NodeListOption{}, c.All()...)
		r.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})

		for _, node := range nodes {
			if r.Float64() < 0.5 {
				if err := evacuateNodeUsingZoneConfig(ctx, l, r, h, node); err != nil {
					return err
				}
			} else {
				if err := evacuateNodeUsingRelocate(ctx, l, r, h, node); err != nil {
					return err
				}
			}
		}

		// Finally, scan the table to ensure all ranges are functional. We don't
		// expect any rows, since the table is empty, but this still requires a scan
		// across each range.
		l.Printf("scanning table")
		var count int
		row := h.QueryRow(r, `SELECT count(*) FROM test`)
		if err := row.Scan(&count); err != nil {
			return err
		} else if count != 0 {
			return errors.Errorf("unexpected row count %d", count)
		}

		return nil
	}

	// Set up and run test.
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(), mixedversion.ClusterSettingOption(
		install.EnvOption{"COCKROACH_SCAN_MAX_IDLE_TIME=10ms"})) // speed up queues

	mvt.OnStartup("create test table", createTable)
	mvt.InMixedVersion("move replicas", moveReplicas)
	mvt.AfterUpgradeFinalized("move replicas", moveReplicas)
	mvt.Run()
}
