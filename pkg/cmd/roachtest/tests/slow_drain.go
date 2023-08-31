// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func registerSlowDrain(r registry.Registry) {
	numNodes := 6
	duration := time.Minute

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("slow-drain/duration=%s", duration),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Leases:  registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSlowDrain(ctx, t, c, duration)
		},
	})
}

// runSlowDrain drains 5 nodes in a test cluster of 6 (with a replication factor of 5),
// which will guarantee a lease transfer stall. This test is meant to ensure that more
// verbose logging occurs during lease transfer stalls.
func runSlowDrain(ctx context.Context, t test.Test, c cluster.Cluster, duration time.Duration) {
	const (
		numNodes          = 6
		pinnedNodeID      = 1
		replicationFactor = 5
	)

	var verboseStoreLogRe = "failed to transfer lease.*when draining.*no suitable transfer target found"

	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
	require.NoError(t, err)

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	run := func(db *gosql.DB, stmt string) {
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err)

		t.L().Printf("run: %s\n", stmt)
	}

	{
		db := c.Conn(ctx, t.L(), pinnedNodeID)
		defer db.Close()

		// Set the replication factor.
		run(db, fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))
		run(db, fmt.Sprintf(`ALTER DATABASE system CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))

		// Wait for initial up-replication.
		err := WaitForReplication(ctx, t, db, replicationFactor, atLeastReplicationFactor)
		require.NoError(t, err)

		// Ensure that leases are sent away from pinned node to avoid situation
		// where none of the leases should actually move during drain.
		const q = `select range_id, lease_holder, replicas from crdb_internal.ranges;`
		rows, err := db.QueryContext(ctx, q)
		require.NoError(t, err, "failed to query ranges")

		for rows.Next() {
			var rangeID int
			var leaseHolder int32
			var replicas []int32
			err = rows.Scan(&rangeID, &leaseHolder, pq.Array(&replicas))
			require.NoError(t, err, "failed to scan replicas of range")
			if leaseHolder != 1 || len(replicas) == 1 {
				continue
			}
			rand.Shuffle(len(replicas), func(i, j int) {
				replicas[i], replicas[j] = replicas[j], replicas[i]
			})
			var newLeaseHolder int32 = 0
			for _, r := range replicas {
				if r != 1 {
					newLeaseHolder = r
					break
				}
			}
			_, err = db.ExecContext(ctx, "ALTER RANGE $1 RELOCATE LEASE TO $2", rangeID, newLeaseHolder)
			if err != nil {
				t.L().Printf("failed relocating lease for r%d: %s", rangeID, err)
			}
		}
	}

	// Drain the last 5 nodes from the cluster, resulting in immovable leases on
	// at least one of the nodes.
	m := c.NewMonitor(ctx)
	for nodeID := 2; nodeID <= numNodes; nodeID++ {
		id := nodeID
		m.Go(func(ctx context.Context) error {
			drain := func(id int) error {
				t.Status(fmt.Sprintf("draining node %d", id))
				return c.RunE(ctx,
					c.Node(id),
					fmt.Sprintf("./cockroach node drain %d --insecure --drain-wait=%s", id, duration.String()),
				)
			}
			return drain(id)
		})
	}

	// Let the drain commands run for a small amount of time to avoid immediate
	// spinning.
	time.Sleep(10 * time.Second)

	// Check for more verbose logging concerning lease transfer stalls.
	// The extra logging should exist on the logs of at least one of the nodes.
	t.Status("checking for stalling drain logging...")
	testutils.SucceedsWithin(t, func() error {
		for nodeID := 2; nodeID <= numNodes; nodeID++ {
			if err := c.RunE(ctx, c.Node(nodeID),
				fmt.Sprintf("grep -q '%s' logs/cockroach.log", verboseStoreLogRe),
			); err == nil {
				return nil
			}
		}
		return errors.New("lease transfer error message not found in logs")
	}, time.Minute)
	t.Status("log messages found")

	// Expect the drain timeout to expire.
	t.Status("waiting for the drain timeout to elapse...")
	err = m.WaitE()
	require.Error(t, err)
}
