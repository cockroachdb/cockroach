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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

func registerJoinInitMixed(r *testRegistry) {
	numNodes := 4
	r.Add(testSpec{
		Name:       "join-init/mixed",
		Owner:      OwnerKV,
		MinVersion: "v20.2.0",
		Cluster:    makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runJoinInitMixed(ctx, t, c, r.buildVersion)
		},
	})
}

// runJoinInitMixed tests the mechanism used to allocate node IDs and
// disseminate cluster IDs in mixed version clusters.
//
// TODO(irfansharif): This test is only really useful for the 20.1/20.2
// timeframe where we introduced the Join RPC; we should remove this test at a
// future point.
func runJoinInitMixed(ctx context.Context, t *test, c *cluster, buildVersion version.Version) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""

	// This test starts off with a two node cluster (node{1,2}) running at
	// predecessor version. It then rolls over node2 to the current version. It
	// then adds node3 to the cluster, which is randomized to be either the
	// current version or the predecessor. It is also randomly configured to
	// point to one of node1 or node2 (they're running different binary
	// versions) in its join flags.
	node1, node2, node3, node4 := 1, 2, 3, 4

	nodeX := 1 + rand.Intn(2)   // Either node1 or node2.
	versionX := func() string { // Either current or predecessor version.
		if rand.Intn(2) == 0 {
			return mainVersion
		}
		return predecessorVersion
	}()
	t.l.Printf("nodeX = %d; versionX = \"%s\"", nodeX, versionX)

	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		// We upload both binaries to each node, to be able to vary the binary
		// used when issuing `cockroach node` subcommands.
		uploadVersion(allNodes, predecessorVersion),
		uploadVersion(allNodes, mainVersion),

		// Start everything at predecessor version.
		startVersion(c.Range(1, 2), predecessorVersion),
		waitForUpgradeStep(c.Range(1, 2)),
		preventAutoUpgradeStep(node1),

		checkNodeAndStoreIDs(node1, 1),
		checkNodeAndStoreIDs(node2, 2),

		// If we upgrade too soon, we some times run into "last used with
		// cockroach version vX-1, is too old for running version vX+1" errors.
		// Give it a generous window to persist the right version marker on
		// disk.
		//
		// TODO(irfansharif): Figure out a better way to address this. This is
		// applicable to a lot of tests. I'd naively expect `waitForUpgrade` to
		// also wait for the on-disk version marker to get bumped. We might need
		// to change crdb code to make that happen.
		sleepStep(time.Minute),

		// Roll node2 into the new version and check to see that it retains its
		// node/cluster ID.
		binaryUpgradeStep(c.Node(node2), mainVersion),
		checkClusterIDsMatch(node1, node2),
		checkNodeAndStoreIDs(node2, 2),

		// Add node3 (running either predecessor version binary or current) to
		// the cluster, pointing at nodeX (running either predecessor version
		// binary or current).
		addNodeStep(c.Node(node3), nodeX, versionX),
		checkClusterIDsMatch(nodeX, node3),
		checkNodeAndStoreIDs(node3, 3),

		// Roll all nodes forward, and finalize upgrade.
		binaryUpgradeStep(c.Range(1, 3), mainVersion),
		allowAutoUpgradeStep(node1),
		waitForUpgradeStep(c.Range(1, 3)),

		checkNodeAndStoreIDs(node1, 1),
		checkNodeAndStoreIDs(node2, 2),
		checkNodeAndStoreIDs(node3, 3),

		// TODO(irfansharif): We'd like to add a step like the one below, and
		// will only be able to do so once 20.2 is cut. 20.1 code does not make
		// use of the Join RPC to join the cluster, so this "gating mechanism"
		// does not apply.
		//
		// Add node4 (running at predecessor version) to the cluster, pointing
		// at nodeX (running new version, now with new cluster version active).
		// We expect this to fail.
		//
		// unsuccessfullyAddNodeStep(c.Node(node4), nodeX, predecessorVersion),

		// Add node4 (running at new version) to the cluster, pointing at nodeX.
		// (running new version, now with new cluster version active).
		addNodeStep(c.Node(node4), nodeX, mainVersion),
		checkClusterIDsMatch(node1, node4),
		checkNodeAndStoreIDs(node4, 4),
	)

	u.run(ctx, t)
}

func addNodeStep(nodes nodeListOption, joinNode int, newVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		args := u.uploadVersion(ctx, t, nodes, newVersion)

		for _, node := range nodes {
			t.l.Printf("adding node %d to the cluster\n", node)
			joinAddr := c.InternalAddr(ctx, c.Node(joinNode))[0]
			c.Start(ctx, t, c.Node(node), args,
				startArgs(fmt.Sprintf("-a=--join=%s", joinAddr)),
			)
		}
	}
}

func unsuccessfullyAddNodeStep(nodes nodeListOption, joinNode int, newVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		args := u.uploadVersion(ctx, t, nodes, newVersion)

		for _, node := range nodes {
			t.l.Printf("adding node %d to the cluster\n", node)
			joinAddr := c.InternalAddr(ctx, c.Node(joinNode))[0]
			err := c.StartE(ctx, c.Node(node), args,
				// TODO(irfansharif): `roachprod` should be taught to skip
				// adding default flags if manually specified via --args/-a.
				// Today it includes both versions, which seems silly.
				startArgs(fmt.Sprintf("-a=--join=%s", joinAddr)),
			)
			if !errors.Is(err, server.ErrIncompatibleBinaryVersion) {
				t.Fatalf("expected err: %s, got %v", server.ErrIncompatibleBinaryVersion, err)
			}
		}
	}
}

var _ = unsuccessfullyAddNodeStep

func checkClusterIDsMatch(nodeA, nodeB int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		var clusterIDA, clusterIDB uuid.UUID
		{
			db := u.conn(ctx, t, nodeA)
			if err := db.QueryRow(`select crdb_internal.cluster_id();`).Scan(&clusterIDA); err != nil {
				t.Fatal(err)
			}
		}
		{
			db := u.conn(ctx, t, nodeB)
			if err := db.QueryRow(`select crdb_internal.cluster_id();`).Scan(&clusterIDB); err != nil {
				t.Fatal(err)
			}
		}

		if clusterIDA != clusterIDB {
			t.Fatalf("expected to cluster ids %s and %s to match", clusterIDA.String(), clusterIDB.String())
		}
	}
}

func checkNodeAndStoreIDs(from int, exp int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, from)
		var nodeID, storeID int
		if err := db.QueryRow(`SELECT node_id FROM crdb_internal.node_runtime_info LIMIT 1;`).Scan(&nodeID); err != nil {
			t.Fatal(err)
		}

		if exp != nodeID {
			t.Fatalf("expected to find node id %d, found %d", exp, nodeID)
		}

		if err := db.QueryRow(`SELECT store_id FROM crdb_internal.kv_store_status WHERE node_id = $1 LIMIT 1;`, nodeID).Scan(&storeID); err != nil {
			t.Fatal(err)
		}

		if exp != storeID {
			t.Fatalf("expected to find store id %d, found %d", exp, storeID)
		}
	}
}

func sleepStep(duration time.Duration) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		t.l.Printf("sleeping for %s...", duration.String())
		time.Sleep(duration)
	}
}
