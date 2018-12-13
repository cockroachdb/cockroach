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
// permissions and limitations under the License.

package main

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerReplicaGC(r *registry) {

	r.Add(testSpec{
		Name:  "replicagc-changed-peers/withRestart",
		Nodes: nodes(6),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runReplicaGCChangedPeers(ctx, t, c, true /* withRestart */)
		},
	})
	r.Add(testSpec{
		Name:  "replicagc-changed-peers/noRestart",
		Nodes: nodes(6),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runReplicaGCChangedPeers(ctx, t, c, false /* withRestart */)
		},
	})
}

func runReplicaGCChangedPeers(ctx context.Context, t *test, c *cluster, withRestart bool) {
	if c.nodes != 6 {
		t.Fatal("test needs to be run with 6 nodes")
	}

	args := startArgs("--sequential", "--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, cockroach, "./cockroach")
	c.Put(ctx, workload, "./workload", c.Node(1))
	c.Start(ctx, t, args, c.Range(1, 3))

	t.Status("waiting for full replication")
	func() {
		db := c.Conn(ctx, 3)
		defer func() {
			_ = db.Close()
		}()
		for {
			var fullReplicated bool
			if err := db.QueryRow(
				// Check if all ranges are fully replicated.
				"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
			).Scan(&fullReplicated); err != nil {
				t.Fatal(err)
			}
			if fullReplicated {
				break
			}
			time.Sleep(time.Second)
		}
	}()

	c.Run(ctx, c.Node(1), "./workload run kv {pgurl:1} --init --max-ops=1 --splits 100")

	// Kill the third node so it won't know that all of its replicas are moved
	// elsewhere. (We don't use the first because that's what roachprod will
	// join new nodes to).
	c.Stop(ctx, c.Node(3))

	// Start three new nodes that will take over all data.
	c.Start(ctx, t, args, c.Range(4, 6))

	if _, err := execCLI(ctx, t, c, 2, "node", "decommission", "1", "2", "3"); err != nil {
		t.Fatal(err)
	}

	// Stop the remaining two old nodes.
	c.Stop(ctx, c.Range(1, 2))

	db4 := c.Conn(ctx, 4)
	defer func() {
		_ = db4.Close()
	}()

	for _, change := range []string{
		"RANGE default", "RANGE meta", "RANGE system", "RANGE liveness", "DATABASE system", "TABLE system.jobs",
	} {
		stmt := `ALTER ` + change + ` CONFIGURE ZONE = 'constraints: {"-deadnode"}'`
		c.l.Printf(stmt + "\n")
		if _, err := db4.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	// Recommission n3 so that when it starts again, it doesn't even know that
	// it was decommissioned (being decommissioning basically lets the replica
	// GC queue run wild). We also recommission the other nodes, for if we didn't,
	// n3 would learn that they are decommissioned and would try to perform
	// replication changes on its ranges, which acquires the lease, which hits
	// the eager GC path since the Raft groups get initialized.
	if _, err := execCLI(ctx, t, c, 4, "node", "recommission", "1", "2", "3"); err != nil {
		t.Fatal(err)
	}

	if withRestart {
		// Restart the remainder of the cluster. This makes sure there are lots of
		// dormant ranges but also and importantly removes all trace of n1 and n2
		// from the Gossip network. If n3 upon restarting learns that n1 and n2
		// used to exist, the replicate queue wakes up a number of ranges due to
		// rebalancing and repair attempts. Lacking this information it does not
		// do that within the store dead interval (5m, i.e. too long for this
		// test).
		c.Stop(ctx, c.Range(4, 6))
		c.Start(ctx, t, args, c.Range(4, 6))
	}

	// Restart n3. We have to manually tell it where to find a new node or it
	// won't be able to connect. Give it the attribute that we've used as a
	// negative constraint for "everything" so that no new replicas are added
	// to this node.
	addr4 := c.InternalAddr(ctx, c.Node(4))[0]
	c.Start(ctx, t, c.Node(3), startArgs(
		"--args=--join="+addr4,
		"--args=--attrs=deadnode",
		"--args=--vmodule=raft=5,replicate_queue=5,allocator=5",
		"--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms",
	))

	db3 := c.Conn(ctx, 3)
	defer func() {
		_ = db3.Close()
	}()

	// Loop for two metric sample intervals (10s) to make sure n3 doesn't see any
	// underreplicated ranges.
	var sawNonzero bool
	var n int
	for tBegin := timeutil.Now(); timeutil.Since(tBegin) < 5*time.Minute; time.Sleep(time.Second) {
		if err := db3.QueryRowContext(
			ctx,
			`SELECT value FROM crdb_internal.node_metrics WHERE name = 'replicas'`,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		c.l.Printf("%d replicas on n3\n", n)
		if sawNonzero && n == 0 {
			break
		}
		sawNonzero = true
	}
	if n != 0 {
		t.Fatalf("replica count didn't drop to zero: %d", n)
	}
}
