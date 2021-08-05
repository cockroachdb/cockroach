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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// runSlowDrain drains 5 nodes in a test cluster of 6 (with a replication factor of 5),
// which will guarantee a lease transfer stall. This test is meant to ensure that more
// verbose logging occurs during lease transfer stalls.
func runSlowDrain(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		numNodes          = 6
		pinnedNodeID      = 1
		replicationFactor = 5
	)

	var verboseStoreLogRe = regexp.MustCompile("failed to transfer lease")

	if err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All()); err != nil {
		t.Fatal(err)
	}
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
	c.Run(ctx, c.Node(pinnedNodeID), `./cockroach workload init kv --drop --splits 1000`)

	run := func(stmt string) {
		db := c.Conn(ctx, t.L(), pinnedNodeID)
		defer db.Close()

		t.Status(stmt)
		_, err := db.ExecContext(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("run: %s\n", stmt)
	}

	waitForReplication := func(db *gosql.DB) {
		t.Status("waiting for initial up-replication")
		for {
			fullReplicated := false
			if err := db.QueryRow(
				// Check if all ranges are fully replicated.
				"SELECT min(array_length(replicas, 1)) >= $1 FROM crdb_internal.ranges",
				replicationFactor,
			).Scan(&fullReplicated); err != nil {
				t.Fatal(err)
			}
			if fullReplicated {
				break
			}
			time.Sleep(time.Second)
		}
	}

	run(fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))
	run(fmt.Sprintf(`ALTER DATABASE system CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))

	db := c.Conn(ctx, t.L(), pinnedNodeID)
	defer db.Close()

	waitForReplication(db)

	var m *errgroup.Group
	dur := 30 * time.Minute
	m, ctx = errgroup.WithContext(ctx)
	m.Go(
		func() error {
			return c.RunE(ctx, c.Node(pinnedNodeID),
				fmt.Sprintf("./cockroach workload run kv --max-rate 500 --tolerate-errors --duration=%s {pgurl:1-%d}",
					dur.String(), numNodes-4,
				),
			)
		},
	)

	// Let the workload run for a small amount of time.
	time.Sleep(1 * time.Minute)

	// Drain 5 nodes from the cluster, resulting in immovable leases.
	m, ctx = errgroup.WithContext(ctx)
	for nodeID := numNodes - 4; nodeID <= numNodes; nodeID++ {
		id := nodeID
		m.Go(func() error {
			drain := func(id int) error {
				t.Status(fmt.Sprintf("draining node %d", id))
				return c.RunE(ctx, c.Node(id), "./cockroach node drain --insecure")
			}
			return drain(id)
		})
	}

	// Let the drain commands run for a small amount of time.
	time.Sleep(30 * time.Second)

	// Check for more verbose logging while the lease transfers stall.
	// Leases should stall on node 2.
	if err := c.RunE(ctx, c.Node(2),
		fmt.Sprintf("grep -q '%s' logs/cockroach.log", verboseStoreLogRe),
	); err != nil {
		t.Fatal(errors.Wrap(err, "expected additional logging"))
	}
}
