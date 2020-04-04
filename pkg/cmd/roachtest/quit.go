// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// runQuitTransfersLeases performs rolling restarts on a
// 3-node cluster and ascertains that each node shutting down
// transfers all its leases reliably to other nodes prior to
// terminating.
func runQuitTransfersLeases(
	ctx context.Context,
	t *test,
	c *cluster,
	methodName string,
	method func(ctx context.Context, t *test, c *cluster, nodeID int),
) {
	args := startArgs("--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t, args)

	// rwt runs a command with a 1-minute timeout.
	rwt := func(fn func(ctx context.Context)) {
		if err := contextutil.RunWithTimeout(ctx, "do", time.Minute, func(ctx context.Context) error {
			fn(ctx)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Now start the remainder of the nodes, then wait for
	// up-replication.
	func() {
		db := c.Conn(ctx, 1)
		defer db.Close()

		// We'll want rebalancing to be a bit fast than normal, so
		// that the up-replication does not take ages.
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING	kv.snapshot_rebalance.max_rate = '128MiB'`); err != nil {
			t.Fatal(err)
		}

		err := retry.ForDuration(30*time.Second, func() error {
			t.l.Printf("waiting for up-replication\n")
			row := db.QueryRowContext(ctx, `SELECT min(array_length(replicas, 1)) FROM crdb_internal.ranges_no_leases`)
			minReplicas := 0
			if err := row.Scan(&minReplicas); err != nil {
				t.Fatal(err)
			}
			if minReplicas < 3 {
				time.Sleep(time.Second)
				return errors.Newf("some ranges not up-replicated yet")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("cluster did not up-replicate: %v", err)
		}
	}()

	// Create a bunch of ranges.
	const numRanges = 500
	func() {
		db := c.Conn(ctx, 1)
		defer db.Close()
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE t(x, y, PRIMARY KEY(x)) AS SELECT @1, 1 FROM generate_series(1,%[1]d)`,
			numRanges)); err != nil {
			t.Fatal(err)
		}
		// We split them from right-to-left so we're peeling at most 1
		// row each time on the right.
		for i := numRanges; i > 1; i -= 100 {
			t.l.Printf("creating 100 ranges (%d-%d)...\n", i, i-99)
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`
ALTER TABLE t SPLIT AT TABLE generate_series(%[1]d,%[1]d-99,-1)`, i)); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// checkNoLeases verifies that no range has a lease on the node
	// that's just been shut down.
	checkNoLease := func(ctx context.Context, nodeID int) {
		// We need to use SQL against a node that's not the one we're
		// shutting down.
		otherNodeID := 1 + nodeID%c.spec.NodeCount
		t.l.Printf("checking lease status via node %d\n", otherNodeID)
		db := c.Conn(ctx, otherNodeID)
		defer db.Close()
		rows, err := db.QueryContext(ctx, `
SELECT range_id, start_pretty, end_pretty
  FROM crdb_internal.ranges
 WHERE lease_holder = $1`, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		matrix, err := sqlutils.RowsToStrMatrix(rows)
		if err != nil {
			t.Fatal(err)
		}
		if len(matrix) > 0 {
			t.Fatalf("expected no ranges with lease on stopped node, found:\n%s\n",
				sqlutils.MatrixToStr(matrix))
		}

		// For good measure, also write to the table. This ensures it
		// remains available.
		if _, err := db.ExecContext(ctx, `UPDATE t SET y = y + 1`); err != nil {
			t.Fatal(err)
		}
	}

	// For every method other than "signal", simulate requiring
	// more than one Drain round to transfer all leases.
	// This way, we exercise the iterating code in quit/ node drain.
	//
	// The reason why we don't do this for "signal" is that the server
	// shutdown via a signal performs just one round of drain (i.e. it
	// does not iterate).
	if methodName != "signal" {
		func() {
			db := c.Conn(ctx, 1)
			defer db.Close()
			if _, err := db.ExecContext(ctx, `
SET CLUSTER SETTING server.shutdown.lease_transfer_wait = '50ms'`); err != nil {
				if strings.Contains(err.Error(), "unknown cluster setting") {
					// old version; ok
				} else {
					t.Fatal(err)
				}
			}
		}()
	}

	// runTest iterates through the cluster two times and restarts each
	// node in turn. After each node shutdown it verifies that there are
	// no orphan ranges.
	//
	// The shutdown method can be customized; the different
	// methods are exercised below.
	t.l.Printf("now running restart loop\n")
	for i := 0; i < 3; i++ {
		t.l.Printf("iteration %d\n", i)
		for nodeID := 1; nodeID <= c.spec.NodeCount; nodeID++ {
			t.l.Printf("stopping node %d\n", nodeID)
			rwt(func(ctx context.Context) { method(ctx, t, c, nodeID) })
			rwt(func(ctx context.Context) { checkNoLease(ctx, nodeID) })
			t.l.Printf("restarting node %d\n", nodeID)
			rwt(func(ctx context.Context) { c.Start(ctx, t, args, c.Node(nodeID)) })
		}
	}
}

func registerQuitTransfersLeases(r *testRegistry) {
	registerTest := func(name, minver string, method func(context.Context, *test, *cluster, int)) {
		r.Add(testSpec{
			Name:       fmt.Sprintf("transfer-leases/%s", name),
			Owner:      OwnerKV,
			Cluster:    makeClusterSpec(3),
			MinVersion: minver,
			Run: func(ctx context.Context, t *test, c *cluster) {
				runQuitTransfersLeases(ctx, t, c, name, method)
			},
		})
	}

	// Uses 'roachprod stop --sig 15 --wait', ie send SIGTERM and wait
	// until the process exits.
	registerTest("signal", "v19.2.0", func(ctx context.Context, t *test, c *cluster, nodeID int) {
		c.Stop(ctx, c.Node(nodeID),
			roachprodArgOption{"--sig", "15", "--wait"}, // graceful shutdown
		)
	})

	// Uses 'cockroach quit' which should drain and then request a
	// shutdown. It then uses 'roachprod stop --sig 0 --wait' to wait
	// for the process to self-exit.
	registerTest("quit", "v19.2.0", func(ctx context.Context, t *test, c *cluster, nodeID int) {
		buf, err := c.RunWithBuffer(ctx, t.l, c.Node(nodeID),
			"./cockroach", "quit", "--insecure", fmt.Sprintf("--port={pgport:%d}", nodeID),
		)
		t.l.Printf("cockroach quit:\n%s\n", buf)
		if err != nil {
			t.Fatal(err)
		}
		c.Stop(ctx, c.Node(nodeID),
			roachprodArgOption{"--sig", "0", "--wait"}, // no shutdown, just wait for exit
		)
	})

	// Uses 'cockroach drain', followed by a non-graceful process
	// kill. If the drain is successful, the leases are transferred
	// successfully even if if the process terminates non-gracefully.
	registerTest("drain", "v20.1.0", func(ctx context.Context, t *test, c *cluster, nodeID int) {
		buf, err := c.RunWithBuffer(ctx, t.l, c.Node(nodeID),
			"./cockroach", "node", "drain", "--insecure",
			fmt.Sprintf("--port={pgport:%d}", nodeID),
		)
		t.l.Printf("cockroach node drain:\n%s\n", buf)
		if err != nil {
			t.Fatal(err)
		}
		c.Stop(ctx, c.Node(nodeID))
	})
}
