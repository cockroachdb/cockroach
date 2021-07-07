// Copyright 2018 The Cockroach Authors.
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
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerReplicaGC(r registry.Registry) {
	for _, restart := range []bool{true, false} {
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("replicagc-changed-peers/restart=%t", restart),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(6),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runReplicaGCChangedPeers(ctx, t, c, restart)
			},
		})
	}
}

var deadNodeAttr = "deadnode"

// runReplicaGCChangedPeers checks that when a node has all of its replicas
// taken away in absentia restarts, without it being able to talk to any of its
// old peers, it will still replicaGC its (now stale) replicas quickly.
//
// It does so by setting up a six node cluster, but initially with just three
// live nodes. After adding a bit of data into the system and waiting for full
// replication, it downs a node and adds the remaining three nodes. It then
// attempts to decommission the original three nodes in order to move the
// replicas off of them, and after having done so, it recommissions the downed
// node. It expects the downed node to discover the new replica placement and gc
// its replicas.
func runReplicaGCChangedPeers(
	ctx context.Context, t test.Test, c cluster.Cluster, withRestart bool,
) {
	if c.Spec().NodeCount != 6 {
		t.Fatal("test needs to be run with 6 nodes")
	}

	args := option.StartArgs("--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(1))
	c.Start(ctx, args, c.Range(1, 3))

	h := &replicagcTestHelper{c: c, t: t}

	t.Status("waiting for full replication")
	h.waitForFullReplication(ctx)

	// Fill in a bunch of data.
	c.Run(ctx, c.Node(1), "./workload init kv {pgurl:1} --splits 100")

	// Kill the third node so it won't know that all of its replicas are moved
	// elsewhere (we don't use the first because that's what roachprod will
	// join new nodes to).
	c.Stop(ctx, c.Node(3))

	// Start three new nodes that will take over all data.
	c.Start(ctx, args, c.Range(4, 6))

	// Recommission n1-3, with n3 in absentia, moving the replicas to n4-6.
	if err := h.decommission(ctx, c.Range(1, 3), 2, "--wait=none"); err != nil {
		t.Fatal(err)
	}

	t.Status("waiting for zero replicas on n1")
	h.waitForZeroReplicas(ctx, 1)

	t.Status("waiting for zero replicas on n2")
	h.waitForZeroReplicas(ctx, 2)

	// Stop the remaining two old nodes, no replicas remaining there.
	c.Stop(ctx, c.Range(1, 2))

	// Set up zone configs to isolate out nodes with the `deadNodeAttr`
	// attribute. We'll later start n3 using this attribute to test GC replica
	// count.
	h.isolateDeadNodes(ctx, 4) // Run this on n4 (it's live, that's all that matters).

	// Recommission n3 so that when it starts again, it doesn't even know that
	// it was marked for decommissioning (which basically let the replica
	// GC queue run wild). We also recommission the other nodes, for if we didn't,
	// n3 would learn that they were marked for decommissioning, and would try
	// to perform replication changes on its ranges, which acquires the lease,
	// which hits the eager GC path since the Raft groups get initialized.
	if err := h.recommission(ctx, c.Range(1, 3), 4); err != nil {
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
		c.Start(ctx, args, c.Range(4, 6))
	}

	// Restart n3. We have to manually tell it where to find a new node or it
	// won't be able to connect. Give it the deadNodeAttr attribute that we've
	// used as a negative constraint for "everything", which should prevent new
	// replicas from being added to it.
	internalAddrs, err := c.InternalAddr(ctx, c.Node(4))
	if err != nil {
		t.Fatal(err)
	}
	c.Start(ctx, c.Node(3), option.StartArgs(
		"--args=--join="+internalAddrs[0],
		"--args=--attrs="+deadNodeAttr,
		"--args=--vmodule=raft=5,replicate_queue=5,allocator=5",
		"--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms",
	))

	// Loop for two metric sample intervals (10s) to make sure n3 doesn't see any
	// underreplicated ranges.
	h.waitForZeroReplicas(ctx, 3)

	// Restart the remaining nodes to satisfy the dead node detector.
	c.Start(ctx, c.Range(1, 2))
}

type replicagcTestHelper struct {
	t test.Test
	c cluster.Cluster
}

func (h *replicagcTestHelper) waitForFullReplication(ctx context.Context) {
	db := h.c.Conn(ctx, 1)
	defer func() {
		_ = db.Close()
	}()

	for {
		var fullReplicated bool
		if err := db.QueryRow(
			// Check if all ranges are fully replicated.
			"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
		).Scan(&fullReplicated); err != nil {
			h.t.Fatal(err)
		}
		if fullReplicated {
			break
		}
		time.Sleep(time.Second)
	}
}

func (h *replicagcTestHelper) waitForZeroReplicas(ctx context.Context, targetNode int) {
	db := h.c.Conn(ctx, targetNode)
	defer func() {
		_ = db.Close()
	}()

	var n = 0
	for tBegin := timeutil.Now(); timeutil.Since(tBegin) < 5*time.Minute; time.Sleep(5 * time.Second) {
		n = h.numReplicas(ctx, db, targetNode)
		if n == 0 {
			break
		}
	}
	if n != 0 {
		h.t.Fatalf("replica count on n%d didn't drop to zero: %d", targetNode, n)
	}
}

// numReplicas returns the number of replicas found on targetNode, provided a db
// connected to the targetNode.
func (h *replicagcTestHelper) numReplicas(ctx context.Context, db *gosql.DB, targetNode int) int {
	var n int
	if err := db.QueryRowContext(
		ctx,
		`SELECT value FROM crdb_internal.node_metrics WHERE name = 'replicas'`,
	).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	h.t.L().Printf("found %d replicas found on n%d\n", n, targetNode)
	return n
}

// decommission decommissions the given targetNodes, running the process
// through the specified runNode.
func (h *replicagcTestHelper) decommission(
	ctx context.Context, targetNodes option.NodeListOption, runNode int, verbs ...string,
) error {
	args := []string{"node", "decommission"}
	args = append(args, verbs...)

	for _, target := range targetNodes {
		args = append(args, strconv.Itoa(target))
	}
	_, err := execCLI(ctx, h.t, h.c, runNode, args...)
	return err
}

// recommission recommissions the given targetNodes, running the process
// through the specified runNode.
func (h *replicagcTestHelper) recommission(
	ctx context.Context, targetNodes option.NodeListOption, runNode int, verbs ...string,
) error {
	args := []string{"node", "recommission"}
	args = append(args, verbs...)
	for _, target := range targetNodes {
		args = append(args, strconv.Itoa(target))
	}
	_, err := execCLI(ctx, h.t, h.c, runNode, args...)
	return err
}

// isolateDeadNodes sets up the zone configs so as to avoid replica placement to
// nodes started with deadNodeAttr. This can then be used as a negative
// constraint for everything.
func (h *replicagcTestHelper) isolateDeadNodes(ctx context.Context, runNode int) {
	db := h.c.Conn(ctx, runNode)
	defer func() {
		_ = db.Close()
	}()

	for _, change := range []string{
		"RANGE default", "RANGE meta", "RANGE system", "RANGE liveness", "DATABASE system", "TABLE system.jobs",
	} {
		stmt := `ALTER ` + change + ` CONFIGURE ZONE = 'constraints: {"-` + deadNodeAttr + `"}'`
		h.t.L().Printf(stmt + "\n")
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			h.t.Fatal(err)
		}
	}
}
