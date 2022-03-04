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
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerSlowDrain(r registry.Registry) {
	numNodes := 6
	duration := time.Minute

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("slow-drain/duration=%s", duration),
		Owner:   registry.OwnerServer,
		Cluster: r.MakeClusterSpec(numNodes),
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

	var verboseStoreLogRe = regexp.MustCompile("failed to transfer lease")

	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
	require.NoError(t, err)

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	run := func(stmt string) {
		db := c.Conn(ctx, t.L(), pinnedNodeID)
		defer db.Close()

		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err)

		t.L().Printf("run: %s\n", stmt)
	}

	waitForReplication := func(db *gosql.DB) {
		t.Status("waiting for initial up-replication")
		for {
			fullReplicated := false

			err = db.QueryRow(
				// Check if all ranges are fully replicated.
				"SELECT min(array_length(replicas, 1)) >= $1 FROM crdb_internal.ranges",
				replicationFactor,
			).Scan(&fullReplicated)
			require.NoError(t, err)

			if fullReplicated {
				break
			}

			time.Sleep(time.Second)
		}
	}

	{
		db := c.Conn(ctx, t.L(), pinnedNodeID)
		defer db.Close()

		// Set the replication factor.
		run(fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))
		run(fmt.Sprintf(`ALTER DATABASE system CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))

		// Wait for initial up-replication.
		waitForReplication(db)
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

	// Let the drain commands run for a small amount of time.
	time.Sleep(30 * time.Second)

	// Check for more verbose logging concerning lease transfer stalls.
	// The extra logging should exist on the logs of at least one of the nodes.
	t.Status("checking for stalling drain logging...")
	found := false
	for nodeID := 2; nodeID <= numNodes; nodeID++ {
		if err := c.RunE(ctx, c.Node(nodeID),
			fmt.Sprintf("grep -q '%s' logs/cockroach.log", verboseStoreLogRe),
		); err == nil {
			found = true
		}
	}
	require.True(t, found)
	t.Status("log messages found")

	// Expect the drain timeout to expire.
	t.Status("waiting for the drain timeout to elapse...")
	err = m.WaitE()
	require.Error(t, err)
}
