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
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func registerDecommission(r registry.Registry) {
	{
		numNodes := 4
		duration := time.Hour

		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("decommission/nodes=%d/duration=%s", numNodes, duration),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				if c.IsLocal() {
					duration = 5 * time.Minute
					t.L().Printf("running with duration=%s in local mode\n", duration)
				}
				runDecommission(ctx, t, c, numNodes, duration)
			},
		})
	}
	{
		numNodes := 9
		duration := 30 * time.Minute
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("drain-and-decommission/nodes=%d", numNodes),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDrainAndDecommission(ctx, t, c, numNodes, duration)
			},
		})
	}
	{
		numNodes := 4
		r.Add(registry.TestSpec{
			Name:    "decommission/drains",
			Owner:   registry.OwnerServer,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDecommissionDrains(ctx, t, c)
			},
		})
	}
	{
		numNodes := 6
		r.Add(registry.TestSpec{
			Name:    "decommission/randomized",
			Owner:   registry.OwnerKV,
			Timeout: 10 * time.Minute,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDecommissionRandomized(ctx, t, c)
			},
		})
	}
	{
		numNodes := 4
		r.Add(registry.TestSpec{
			Name:    "decommission/mixed-versions",
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDecommissionMixedVersions(ctx, t, c, *t.BuildVersion())
			},
		})
	}
	{
		numNodes := 6
		r.Add(registry.TestSpec{
			Name:    "decommission/slow",
			Owner:   registry.OwnerServer,
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDecommissionSlow(ctx, t, c)
			},
		})
	}
}

// runDrainAndDecommission marks 3 nodes in the test cluster as "draining" and
// then attempts to decommission a fourth node from the cluster. This test is
// meant to ensure that, in the allocator, we consider "draining" nodes as
// "live" for the purposes of determining whether a range can achieve quorum.
// Note that, if "draining" nodes were not considered live for this purpose,
// decommissioning would stall forever since the allocator would incorrectly
// think that at least a handful of ranges (that need to be moved off the
// decommissioning node) are unavailable.
func runDrainAndDecommission(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes int, duration time.Duration,
) {
	const defaultReplicationFactor = 5
	if defaultReplicationFactor > nodes {
		t.Fatal("improper configuration: replication factor greater than number of nodes in the test")
	}
	pinnedNode := 1
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	for i := 1; i <= nodes; i++ {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(i))
	}
	c.Run(ctx, c.Node(pinnedNode), `./cockroach workload init kv --drop --splits 1000`)

	run := func(stmt string) {
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		t.Status(stmt)
		_, err := db.ExecContext(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("run: %s\n", stmt)
	}

	{
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		run(fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE USING num_replicas=%d`, defaultReplicationFactor))
		run(fmt.Sprintf(`ALTER DATABASE system CONFIGURE ZONE USING num_replicas=%d`, defaultReplicationFactor))

		// Speed up the decommissioning.
		run(`SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='2GiB'`)
		run(`SET CLUSTER SETTING kv.snapshot_recovery.max_rate='2GiB'`)

		// Wait for initial up-replication.
		err := WaitFor3XReplication(ctx, t, db)
		require.NoError(t, err)
	}

	var m *errgroup.Group
	m, ctx = errgroup.WithContext(ctx)
	m.Go(
		func() error {
			return c.RunE(ctx, c.Node(pinnedNode),
				fmt.Sprintf("./cockroach workload run kv --max-rate 500 --tolerate-errors --duration=%s {pgurl:1-%d}",
					duration.String(), nodes-4,
				),
			)
		},
	)

	// Let the workload run for a small amount of time.
	time.Sleep(1 * time.Minute)

	// Drain the last 3 nodes from the cluster.
	for nodeID := nodes - 2; nodeID <= nodes; nodeID++ {
		id := nodeID
		m.Go(func() error {
			drain := func(id int) error {
				t.Status(fmt.Sprintf("draining node %d", id))
				return c.RunE(ctx, c.Node(id), "./cockroach node drain --insecure")
			}
			return drain(id)
		})
	}
	// Sleep for long enough that all the other nodes in the cluster learn about
	// the draining status of the two nodes we just drained.
	time.Sleep(30 * time.Second)

	m.Go(func() error {
		// Decommission the fourth-to-last node from the cluster.
		id := nodes - 3
		decom := func(id int) error {
			t.Status(fmt.Sprintf("decommissioning node %d", id))
			return c.RunE(ctx, c.Node(id), "./cockroach node decommission --self --insecure")
		}
		return decom(id)
	})
	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
}

// runDecommission decommissions and wipes nodes in a cluster repeatedly,
// alternating between the node being shut down gracefully before and after the
// decommissioning operation, while some light load is running against the
// cluster (to manually verify that the qps don't dip too much).
//
// TODO(tschottdorf): verify that the logs don't contain the messages
// that would spam the log before #23605. I wonder if we should really
// start grepping the logs. An alternative is to introduce a metric
// that would have signaled this and check that instead.
func runDecommission(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes int, duration time.Duration,
) {
	const defaultReplicationFactor = 3
	if defaultReplicationFactor > nodes {
		t.Fatal("improper configuration: replication factor greater than number of nodes in the test")
	}
	// The number of nodes we're going to cycle through. Since we're sometimes
	// killing the nodes and then removing them, this means having to be careful
	// with loss of quorum. So only ever touch a fixed minority of nodes and
	// swap them out for as long as the test runs. The math boils down to `1`,
	// but conceivably we'll want to run a test with replication factor five
	// at some point.
	numDecom := (defaultReplicationFactor - 1) / 2

	// node1 is kept pinned (i.e. not decommissioned/restarted), and is the node
	// through which we run the workload and other queries.
	pinnedNode := 1
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(pinnedNode))

	for i := 1; i <= nodes; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=node%d", i))
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}
	c.Run(ctx, c.Node(pinnedNode), `./workload init kv --drop`)

	waitReplicatedAwayFrom := func(downNodeID int) error {
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer func() {
			_ = db.Close()
		}()

		for {
			var count int
			if err := db.QueryRow(
				// Check if the down node has any replicas.
				"SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
				downNodeID,
			).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				fullReplicated := false
				if err := db.QueryRow(
					// Check if all ranges are fully replicated.
					"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
				).Scan(&fullReplicated); err != nil {
					return err
				}
				if fullReplicated {
					break
				}
			}
			time.Sleep(time.Second)
		}
		return nil
	}

	waitUpReplicated := func(targetNode, targetNodeID int) error {
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer func() {
			_ = db.Close()
		}()

		var count int
		for {
			// Check to see that there are no ranges where the target node is
			// not part of the replica set.
			stmtReplicaCount := fmt.Sprintf(
				`SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, %d) IS NULL and database_name = 'kv';`, targetNodeID)
			if err := db.QueryRow(stmtReplicaCount).Scan(&count); err != nil {
				return err
			}
			t.Status(fmt.Sprintf("node%d missing %d replica(s)", targetNode, count))
			if count == 0 {
				break
			}
			time.Sleep(time.Second)
		}
		return nil
	}

	if err := waitReplicatedAwayFrom(0 /* no down node */); err != nil {
		t.Fatal(err)
	}

	workloads := []string{
		// TODO(tschottdorf): in remote mode, the ui shows that we consistently write
		// at 330 qps (despite asking for 500 below). Locally we get 500qps (and a lot
		// more without rate limiting). Check what's up with that.
		fmt.Sprintf("./workload run kv --max-rate 500 --tolerate-errors --duration=%s {pgurl:1-%d}", duration.String(), nodes),
	}

	run := func(stmt string) {
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		t.Status(stmt)
		_, err := db.ExecContext(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("run: %s\n", stmt)
	}

	var m *errgroup.Group // see comment in version.go
	m, ctx = errgroup.WithContext(ctx)
	for _, cmd := range workloads {
		cmd := cmd // copy is important for goroutine
		m.Go(func() error {
			return c.RunE(ctx, c.Node(pinnedNode), cmd)
		})
	}

	m.Go(func() error {
		getNodeID := func(node int) (int, error) {
			dbNode := c.Conn(ctx, t.L(), node)
			defer dbNode.Close()

			var nodeID int
			if err := dbNode.QueryRow(`SELECT node_id FROM crdb_internal.node_runtime_info LIMIT 1`).Scan(&nodeID); err != nil {
				return 0, err
			}
			return nodeID, nil
		}

		stop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			defer time.Sleep(time.Second) // work around quit returning too early
			return c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --host=:"+port)
		}

		decom := func(id int) error {
			port := fmt.Sprintf("{pgport:%d}", pinnedNode) // always use the pinned node
			t.Status(fmt.Sprintf("decommissioning node %d", id))
			return c.RunE(ctx, c.Node(pinnedNode), fmt.Sprintf("./cockroach node decommission --insecure --wait=all --host=:%s %d", port, id))
		}

		tBegin, whileDown := timeutil.Now(), true
		node := nodes
		for timeutil.Since(tBegin) <= duration {
			// Alternate between the node being shut down gracefully before and
			// after the decommissioning operation.
			whileDown = !whileDown
			// Cycle through the last numDecom nodes.
			node = nodes - (node % numDecom)
			if node == pinnedNode {
				t.Fatalf("programming error: not expecting to decommission/wipe node%d", pinnedNode)
			}

			t.Status(fmt.Sprintf("decommissioning %d (down=%t)", node, whileDown))
			nodeID, err := getNodeID(node)
			if err != nil {
				return err
			}

			run(fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE = 'constraints: {"+node%d"}'`, node))
			if err := waitUpReplicated(node, nodeID); err != nil {
				return err
			}

			if whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}

			run(fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE = 'constraints: {"-node%d"}'`, node))

			if err := decom(nodeID); err != nil {
				return err
			}

			if err := waitReplicatedAwayFrom(nodeID); err != nil {
				return err
			}

			if !whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}

			// Wipe the node and re-add to cluster with a new node ID.
			if err := c.RunE(ctx, c.Node(node), "rm -rf {store-dir}"); err != nil {
				return err
			}

			db := c.Conn(ctx, t.L(), pinnedNode)
			defer db.Close()

			internalAddrs, err := c.InternalAddr(ctx, t.L(), c.Node(pinnedNode))
			if err != nil {
				return err
			}
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--join %s --attrs=node%d", internalAddrs[0], node))
			if err := c.StartE(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(node)); err != nil {
				return err
			}
		}
		// TODO(tschottdorf): run some ui sanity checks about decommissioned nodes
		// having disappeared. Verify that the workloads don't dip their qps or
		// show spikes in latencies.
		return nil
	})
	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
}

// runDecommissionRandomized tests a bunch of node
// decommissioning/recommissioning procedures, all the while checking for
// replica movement and appropriate membership status detection behavior. We go
// through partial decommissioning of random nodes, ensuring we're able to undo
// those operations. We then fully decommission nodes, verifying it's an
// irreversible operation.
func runDecommissionRandomized(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings)

	h := newDecommTestHelper(t, c)

	firstNodeID := h.nodeIDs[0]
	retryOpts := retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}

	warningFilter := []string{
		"^warning: node [0-9]+ is already decommissioning or decommissioned$",
	}

	// Partially decommission then recommission n1, from another
	// random node. Run a couple of status checks while doing so.
	// We hard-code n1 to guard against the hypothetical case in which
	// targetNode has no replicas (yet) to begin with, which would make
	// it permanently decommissioned even with `--wait=none`.
	// We can choose runNode freely (including chosing targetNode) since
	// the decommission process won't succeed.
	{
		targetNode, runNode := h.nodeIDs[0], h.getRandNode()
		t.L().Printf("partially decommissioning n%d from n%d\n", targetNode, runNode)
		o, err := h.decommission(ctx, c.Node(targetNode), runNode,
			"--wait=none", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(targetNode), "true", `\d+`, "true", "decommissioning", "false"},
		}
		if err := cli.MatchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
		// Check that `node status` reflects an ongoing decommissioning status
		// for the second node.
		{
			runNode = h.getRandNode()
			t.L().Printf("checking that `node status` (from n%d) shows n%d as decommissioning\n",
				runNode, targetNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols, err := cli.GetCsvNumCols(o)
			require.NoError(t, err)
			exp := h.expectCell(targetNode-1, /* node IDs are 1-indexed */
				statusHeaderMembershipColumnIdx, `decommissioning`, c.Spec().NodeCount, numCols)
			if err := cli.MatchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// Recommission the target node, cancel the in-flight decommissioning
		// process.
		{
			runNode = h.getRandNode()
			t.L().Printf("recommissioning n%d (from n%d)\n", targetNode, runNode)
			if _, err := h.recommission(ctx, c.Node(targetNode), runNode); err != nil {
				t.Fatalf("recommission failed: %v", err)
			}
		}

		// Check that `node status` now reflects a 'active' status for the
		// target node.
		{
			runNode = h.getRandNode()
			t.L().Printf("checking that `node status` (from n%d) shows n%d as active\n",
				targetNode, runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols, err := cli.GetCsvNumCols(o)
			require.NoError(t, err)
			exp := h.expectCell(targetNode-1, /* node IDs are 1-indexed */
				statusHeaderMembershipColumnIdx, `active`, c.Spec().NodeCount, numCols)
			if err := cli.MatchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Check to see that operators aren't able to decommission into
	// availability. We'll undo the attempted decommissioning event by
	// recommissioning the targeted nodes. Note that the decommission
	// attempt will not result in any individual nodes being marked
	// as decommissioned, as this would only happen if *all* targeted
	// nodes report no replicas, which is impossible since we're targeting
	// the whole cluster here.
	{
		// Attempt to decommission all the nodes.
		{
			// NB: the retry loop here is mostly silly, but since some of the fields below
			// are async, we may for example find an 'active' node instead of 'decommissioning'.
			if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
				runNode := h.getRandNode()
				t.L().Printf("attempting to decommission all nodes from n%d\n", runNode)
				o, err := h.decommission(ctx, c.All(), runNode,
					"--wait=none", "--format=csv")
				if err != nil {
					t.Fatalf("decommission failed: %v", err)
				}

				exp := [][]string{decommissionHeader}
				for i := 1; i <= c.Spec().NodeCount; i++ {
					rowRegex := []string{strconv.Itoa(i), "true", `\d+`, "true", "decommissioning", "false"}
					exp = append(exp, rowRegex)
				}
				if err := cli.MatchCSV(o, exp); err != nil {
					t.Fatalf("decommission failed: %v", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		// Check that `node status` reflects an ongoing decommissioning status for
		// all nodes.
		{
			runNode := h.getRandNode()
			t.L().Printf("checking that `node status` (from n%d) shows all nodes as decommissioning\n",
				runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols, err := cli.GetCsvNumCols(o)
			require.NoError(t, err)
			var colRegex []string
			for i := 1; i <= c.Spec().NodeCount; i++ {
				colRegex = append(colRegex, `decommissioning`)
			}
			exp := h.expectColumn(statusHeaderMembershipColumnIdx, colRegex, c.Spec().NodeCount, numCols)
			if err := cli.MatchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// Check that we can still do stuff, creating a database should be good
		// enough.
		{
			runNode := h.getRandNode()
			t.L().Printf("checking that we're able to create a database (from n%d)\n", runNode)
			db := c.Conn(ctx, t.L(), runNode)
			defer db.Close()

			if _, err := db.Exec(`create database still_working;`); err != nil {
				t.Fatal(err)
			}
		}

		// Cancel in-flight decommissioning process of all nodes.
		{
			runNode := h.getRandNode()
			t.L().Printf("recommissioning all nodes (from n%d)\n", runNode)
			if _, err := h.recommission(ctx, c.All(), runNode); err != nil {
				t.Fatalf("recommission failed: %v", err)
			}
		}

		// Check that `node status` now reflects an 'active' status for all
		// nodes.
		{
			runNode := h.getRandNode()
			t.L().Printf("checking that `node status` (from n%d) shows all nodes as active\n",
				runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols, err := cli.GetCsvNumCols(o)
			require.NoError(t, err)
			var colRegex []string
			for i := 1; i <= c.Spec().NodeCount; i++ {
				colRegex = append(colRegex, `active`)
			}
			exp := h.expectColumn(statusHeaderMembershipColumnIdx, colRegex, c.Spec().NodeCount, numCols)
			if err := cli.MatchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Fully decommission two random nodes, from a random node, randomly choosing
	// between using --wait={all,none}. We pin these two nodes to not re-use
	// them for the block after, as they will have been fully decommissioned and
	// by definition, non-operational.
	decommissionedNodeA := h.getRandNode()
	h.blockFromRandNode(decommissionedNodeA)
	decommissionedNodeB := h.getRandNode() // note A != B
	h.blockFromRandNode(decommissionedNodeB)
	{
		targetNodeA, targetNodeB := decommissionedNodeA, decommissionedNodeB
		if targetNodeB < targetNodeA {
			targetNodeB, targetNodeA = targetNodeA, targetNodeB
		}

		runNode := h.getRandNode()
		waitStrategy := "all" // Blocking decommission.
		if i := rand.Intn(2); i == 0 {
			waitStrategy = "none" // Polling decommission.
		}

		t.L().Printf("fully decommissioning [n%d,n%d] from n%d, using --wait=%s\n",
			targetNodeA, targetNodeB, runNode, waitStrategy)

		// When using --wait=none, we poll the decommission status.
		maxAttempts := 50
		if waitStrategy == "all" {
			// --wait=all is a one shot attempt at decommissioning, that polls
			// internally.
			maxAttempts = 1
		}

		// Decommission two nodes.
		if err := retry.WithMaxAttempts(ctx, retryOpts, maxAttempts, func() error {
			o, err := h.decommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode,
				fmt.Sprintf("--wait=%s", waitStrategy), "--format=csv")
			if err != nil {
				t.Fatalf("decommission failed: %v", err)
			}

			exp := [][]string{
				decommissionHeader,
				{strconv.Itoa(targetNodeA), "true|false", "0", "true", "decommissioned", "false"},
				{strconv.Itoa(targetNodeB), "true|false", "0", "true", "decommissioned", "false"},
				decommissionFooter,
			}
			return cli.MatchCSV(cli.RemoveMatchingLines(o, warningFilter), exp)
		}); err != nil {
			t.Fatal(err)
		}

		// Check that the two decommissioned nodes vanish from `node ls` (since they
		// will not remain live as the cluster refuses connections from them).
		{
			if err := retry.WithMaxAttempts(ctx, retry.Options{}, 50, func() error {
				runNode = h.getRandNode()
				t.L().Printf("checking that `node ls` (from n%d) shows n-2 nodes\n", runNode)
				o, err := execCLI(ctx, t, c, runNode, "node", "ls", "--format=csv")
				if err != nil {
					t.Fatalf("node-ls failed: %v", err)
				}
				exp := [][]string{{"id"}}
				for i := 1; i <= c.Spec().NodeCount; i++ {
					if _, ok := h.randNodeBlocklist[i]; ok {
						// Decommissioned, so should go away in due time.
						continue
					}
					exp = append(exp, []string{strconv.Itoa(i)})
				}
				return cli.MatchCSV(o, exp)
			}); err != nil {
				t.Fatal(err)
			}
		}

		// Ditto for `node status`.
		{
			if err := retry.WithMaxAttempts(ctx, retry.Options{}, 50, func() error {
				runNode = h.getRandNode()
				t.L().Printf("checking that `node status` (from n%d) shows only live nodes\n", runNode)
				o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv")
				if err != nil {
					t.Fatalf("node-status failed: %v", err)
				}

				numCols, err := cli.GetCsvNumCols(o)
				require.NoError(t, err)

				colRegex := []string{}
				for i := 1; i <= c.Spec().NodeCount; i++ {
					if _, ok := h.randNodeBlocklist[i]; ok {
						continue // decommissioned
					}
					colRegex = append(colRegex, strconv.Itoa(i))
				}
				exp := h.expectIDsInStatusOut(colRegex, numCols)
				return cli.MatchCSV(o, exp)
			}); err != nil {
				t.Fatal(err)
			}
		}

		// Attempt to recommission the fully decommissioned nodes (expecting it
		// to fail).
		{
			runNode = h.getRandNode()
			t.L().Printf("expected to fail: recommissioning [n%d,n%d] (from n%d)\n",
				targetNodeA, targetNodeB, runNode)
			if _, err := h.recommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode); err == nil {
				t.Fatal("expected recommission to fail")
			}
		}

		// Decommissioning the same nodes again should be a no-op. We do it from
		// a random node.
		{
			runNode = h.getRandNode()
			t.L().Printf("checking that decommissioning [n%d,n%d] (from n%d) is a no-op\n",
				targetNodeA, targetNodeB, runNode)
			o, err := h.decommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode,
				"--wait=all", "--format=csv")
			if err != nil {
				t.Fatalf("decommission failed: %v", err)
			}

			exp := [][]string{
				decommissionHeader,
				// NB: the "false" is liveness. We waited above for these nodes to
				// vanish from `node ls`, so definitely not live at this point.
				{strconv.Itoa(targetNodeA), "false", "0", "true", "decommissioned", "false"},
				{strconv.Itoa(targetNodeB), "false", "0", "true", "decommissioned", "false"},
				decommissionFooter,
			}
			if err := cli.MatchCSV(cli.RemoveMatchingLines(o, warningFilter), exp); err != nil {
				t.Fatal(err)
			}
		}

		// We restart the nodes and attempt to recommission (should still fail).
		{
			runNode = h.getRandNode()
			t.L().Printf("expected to fail: restarting [n%d,n%d] and attempting to recommission through n%d\n",
				targetNodeA, targetNodeB, runNode)
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(targetNodeA, targetNodeB))
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Nodes(targetNodeA, targetNodeB))

			if _, err := h.recommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode); err == nil {
				t.Fatalf("expected recommission to fail")
			}
			// Now stop+wipe them for good to keep the logs simple and the dead node detector happy.
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Nodes(targetNodeA, targetNodeB))
			c.Wipe(ctx, c.Nodes(targetNodeA, targetNodeB))
		}
	}

	// Decommission a downed node (random selected), randomly choosing between
	// bringing the node back to life or leaving it permanently dead.
	//
	// TODO(irfansharif): We could pull merge this "deadness" check into the
	// previous block, when fully decommissioning multiple nodes, to reduce the
	// total number of nodes needed in the cluster.
	{
		restartDownedNode := false
		if i := rand.Intn(2); i == 0 {
			restartDownedNode = true
		}

		if !restartDownedNode {
			// We want to test decommissioning a truly dead node. Make sure we
			// don't waste too much time waiting for the node to be recognized
			// as dead. Note that we don't want to set this number too low or
			// everything will seem dead to the allocator at all times, so
			// nothing will ever happen.
			func() {
				db := c.Conn(ctx, t.L(), h.getRandNode())
				defer db.Close()
				const stmt = "SET CLUSTER SETTING server.time_until_store_dead = '1m15s'"
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					t.Fatal(err)
				}
			}()
		}

		// We also have to exclude the first node seeing as how we're going to
		// wiping it below. Roachprod attempts to initialize a cluster when
		// starting a "fresh" first node (without an existing bootstrap marker
		// on disk, which we happen to also be wiping away).
		targetNode := h.getRandNodeOtherThan(firstNodeID)
		h.blockFromRandNode(targetNode)
		t.L().Printf("intentionally killing n%d to later decommission it when down\n", targetNode)
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(targetNode))

		// Pick a runNode that is still in commission and will
		// remain so (or it won't be able to contact cluster).
		runNode := h.getRandNode()
		t.L().Printf("decommissioning n%d (from n%d) in absentia\n", targetNode, runNode)
		if _, err := h.decommission(ctx, c.Node(targetNode), runNode,
			"--wait=all", "--format=csv"); err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		if restartDownedNode {
			t.L().Printf("restarting n%d for verification\n", targetNode)

			// Bring targetNode it back up to verify that its replicas still get
			// removed.
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(targetNode))
		}

		// Run decommission a second time to wait until the replicas have
		// all been GC'ed. Note that we specify "all" because even though
		// the target node is now running, it may not be live by the time
		// the command runs.
		o, err := h.decommission(ctx, c.Node(targetNode), runNode,
			"--wait=all", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(targetNode), "true|false", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}
		if err := cli.MatchCSV(cli.RemoveMatchingLines(o, warningFilter), exp); err != nil {
			t.Fatal(err)
		}

		if !restartDownedNode {
			// Check that (at least after a bit) the node disappears from `node
			// ls` because it is decommissioned and not live.
			if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
				runNode := h.getRandNode()
				o, err := execCLI(ctx, t, c, runNode, "node", "ls", "--format=csv")
				if err != nil {
					t.Fatalf("node-ls failed: %v", err)
				}

				var exp [][]string
				// We expect an entry for every node we haven't decommissioned yet.
				for i := 1; i <= c.Spec().NodeCount-len(h.randNodeBlocklist); i++ {
					exp = append(exp, []string{fmt.Sprintf("[^%d]", targetNode)})
				}

				return cli.MatchCSV(o, exp)
			}); err != nil {
				t.Fatal(err)
			}

			// Ditto for `node status`
			if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
				runNode := h.getRandNode()
				o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv")
				if err != nil {
					t.Fatalf("node-status failed: %v", err)
				}

				numCols, err := cli.GetCsvNumCols(o)
				require.NoError(t, err)
				var expC []string
				for i := 1; i <= c.Spec().NodeCount-len(h.randNodeBlocklist); i++ {
					expC = append(expC, fmt.Sprintf("[^%d].*", targetNode))
				}
				exp := h.expectIDsInStatusOut(expC, numCols)
				return cli.MatchCSV(o, exp)
			}); err != nil {
				t.Fatal(err)
			}
		}

		{
			t.L().Printf("wiping n%d and adding it back to the cluster as a new node\n", targetNode)

			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(targetNode))
			c.Wipe(ctx, c.Node(targetNode))

			joinNode := h.getRandNode()
			internalAddrs, err := c.InternalAddr(ctx, t.L(), c.Node(joinNode))
			if err != nil {
				t.Fatal(err)
			}
			joinAddr := internalAddrs[0]
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--join %s", joinAddr))
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(targetNode))
		}

		if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
			o, err := execCLI(ctx, t, c, h.getRandNode(), "node", "status", "--format=csv")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}
			numCols, err := cli.GetCsvNumCols(o)
			require.NoError(t, err)
			var expC []string
			// The decommissioned nodes should all disappear. (We
			// abuse that nodeIDs are single-digit in this test).
			re := `[^`
			for id := range h.randNodeBlocklist {
				re += fmt.Sprint(id)
			}
			re += `].*`
			// We expect to all the decommissioned nodes to
			// disappear, but we let one rejoin as a fresh node.
			for i := 1; i <= c.Spec().NodeCount-len(h.randNodeBlocklist)+1; i++ {
				expC = append(expC, re)
			}
			exp := h.expectIDsInStatusOut(expC, numCols)
			return cli.MatchCSV(o, exp)
		}); err != nil {
			t.Fatal(err)
		}
	}

	// We'll verify the set of events, in order, we expect to get posted to
	// system.eventlog.
	if err := retry.ForDuration(time.Minute, func() error {
		// Verify the event log has recorded exactly one decommissioned or
		// recommissioned event for each membership operation.
		db := c.Conn(ctx, t.L(), h.getRandNode())
		defer db.Close()

		rows, err := db.Query(`
			SELECT "eventType" FROM system.eventlog WHERE "eventType" IN ($1, $2, $3) ORDER BY timestamp
			`, "node_decommissioned", "node_decommissioning", "node_recommissioned",
		)
		if err != nil {
			t.L().Printf("retrying: %v\n", err)
			return err
		}
		defer rows.Close()

		matrix, err := sqlutils.RowsToStrMatrix(rows)
		if err != nil {
			return err
		}

		expMatrix := [][]string{
			// Partial decommission attempt of a single node.
			{"node_decommissioning"},
			{"node_recommissioned"},

			// Cluster wide decommissioning attempt.
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},

			// Cluster wide recommissioning, to undo previous decommissioning attempt.
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},

			// Full decommission of two nodes.
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioned"},
			{"node_decommissioned"},

			// Full decommission of a single node.
			{"node_decommissioning"},
			{"node_decommissioned"},
		}

		if !reflect.DeepEqual(matrix, expMatrix) {
			t.Fatalf("unexpected diff(matrix, expMatrix):\n%s\n%s\nvs.\n%s", pretty.Diff(matrix, expMatrix), matrix, expMatrix)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// runDecommissionDrains tests that a decommission implies a drain.
// This means that SQL connections of a decommissioning node will be closed near
// the end of decommissioning. The test cluster contains 4 nodes and the fourth
// node is decommissioned. While the decommissioning node has open SQL
// connections, queries should never fail.
func runDecommissionDrains(ctx context.Context, t test.Test, c cluster.Cluster) {
	var (
		numNodes     = 4
		pinnedNodeID = 1
		decommNodeID = numNodes
		decommNode   = c.Node(decommNodeID)
	)

	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
	require.NoError(t, err)

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	h := newDecommTestHelper(t, c)

	run := func(db *gosql.DB, query string) error {
		_, err = db.ExecContext(ctx, query)
		t.L().Printf("run: %s\n", query)
		return err
	}

	{
		db := c.Conn(ctx, t.L(), pinnedNodeID)
		defer db.Close()

		// Increase the speed of decommissioning.
		err = run(db, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='2GiB'`)
		require.NoError(t, err)
		err = run(db, `SET CLUSTER SETTING kv.snapshot_recovery.max_rate='2GiB'`)
		require.NoError(t, err)

		// Wait for initial up-replication.
		err := WaitFor3XReplication(ctx, t, db)
		require.NoError(t, err)
	}

	// Connect to node 4 (the target node of the decommission).
	decommNodeDB := c.Conn(ctx, t.L(), decommNodeID)
	defer decommNodeDB.Close()

	// Decommission node 4 and poll its status during the decommission.
	var (
		maxAttempts = 50
		retryOpts   = retry.Options{
			InitialBackoff: time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2,
		}
		// The expected output of decommission while the node is about to be drained/is draining.
		expReplicasTransferred = [][]string{
			decommissionHeader,
			{strconv.Itoa(decommNodeID), "true|false", "0", "true", "decommissioning", "false"},
			decommissionFooter,
		}
		// The expected output of decommission once the node is finally marked as "decommissioned."
		expDecommissioned = [][]string{
			decommissionHeader,
			{strconv.Itoa(decommNodeID), "true|false", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}
	)
	t.Status(fmt.Sprintf("decommissioning node %d", decommNodeID))
	e := retry.WithMaxAttempts(ctx, retryOpts, maxAttempts, func() error {
		o, err := h.decommission(ctx, decommNode, pinnedNodeID, "--wait=none", "--format=csv")
		require.NoError(t, errors.Wrapf(err, "decommission failed"))

		// Check if all the replicas have been transferred.
		if err = cli.MatchCSV(o, expReplicasTransferred); err != nil {
			return err
		}

		// Check to see if the node has been drained.
		// If not, queries should not fail.
		if err = run(decommNodeDB, `SHOW DATABASES`); err != nil {
			if strings.Contains(err.Error(), "not accepting clients") { // drained
				return nil
			}
			t.Fatal(err)
		}

		return errors.New("not drained")
	})
	require.NoError(t, e)

	// Check that the node is fully decommissioned.
	o, err := h.decommission(ctx, decommNode, pinnedNodeID, "--format=csv")
	require.NoError(t, err)
	require.NoError(t, cli.MatchCSV(o, expDecommissioned))
}

// runDecommissionSlow decommissions 5 nodes in a test cluster of 6
// (with a replication factor of 5), which will guarantee a replica transfer
// stall. This test is meant to ensure that decommissioning replicas are
// reported when replica transfers stall.
func runDecommissionSlow(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		numNodes          = 6
		pinnedNodeID      = 1
		replicationFactor = 5
	)

	var verboseStoreLogRe = regexp.MustCompile("possible decommission stall detected")

	err := c.PutE(ctx, t.L(), t.Cockroach(), "./cockroach", c.All())
	require.NoError(t, err)

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

	run := func(db *gosql.DB, query string) {
		_, err = db.ExecContext(ctx, query)
		require.NoError(t, err)
		t.L().Printf("run: %s\n", query)
	}

	{
		db := c.Conn(ctx, t.L(), pinnedNodeID)
		defer db.Close()

		// Set the replication factor to 5.
		run(db, fmt.Sprintf(`ALTER RANGE default CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))
		run(db, fmt.Sprintf(`ALTER DATABASE system CONFIGURE ZONE USING num_replicas=%d`, replicationFactor))

		// Increase the speed of decommissioning.
		run(db, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='2GiB'`)
		run(db, `SET CLUSTER SETTING kv.snapshot_recovery.max_rate='2GiB'`)

		// Wait for initial up-replication.
		err := WaitForReplication(ctx, t, db, replicationFactor)
		require.NoError(t, err)
	}

	// Decommission 5 nodes from the cluster, resulting in immovable replicas.
	// Be prepared to cancel the context for the processes running decommissions
	// since the decommissions will stall.
	decomCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	m := c.NewMonitor(decomCtx)
	for nodeID := 2; nodeID <= numNodes; nodeID++ {
		id := nodeID
		m.Go(func(ctx context.Context) error {
			decom := func(id int) error {
				t.Status(fmt.Sprintf("decommissioning node %d", id))
				return c.RunE(ctx,
					c.Node(id),
					fmt.Sprintf("./cockroach node decommission %d --insecure", id),
				)
			}
			return decom(id)
		})
	}

	// Check for reported decommissioning replicas.
	t.Status("checking for decommissioning replicas report...")
	testutils.SucceedsWithin(t, func() error {
		for nodeID := 1; nodeID <= numNodes; nodeID++ {
			if err = c.RunE(ctx,
				c.Node(nodeID),
				fmt.Sprintf("grep -q '%s' logs/cockroach.log", verboseStoreLogRe),
			); err == nil {
				return nil
			}
		}
		return errors.New("still waiting for decommissioning replicas report")
	},
		3*time.Minute,
	)
}

// Header from the output of `cockroach node decommission`.
var decommissionHeader = []string{
	"id", "is_live", "replicas", "is_decommissioning", "membership", "is_draining",
}

// Footer from the output of `cockroach node decommission`, after successful
// decommission.
var decommissionFooter = []string{
	"No more data reported on target nodes. " +
		"Please verify cluster health before removing the nodes.",
}

// Header from the output of `cockroach node status`.
var statusHeader = []string{
	"id", "address", "sql_address", "build", "started_at", "updated_at", "locality", "is_available", "is_live",
}

// Header from the output of `cockroach node status --decommission`.
var statusHeaderWithDecommission = []string{
	"id", "address", "sql_address", "build", "started_at", "updated_at", "locality", "is_available", "is_live",
	"gossiped_replicas", "is_decommissioning", "membership", "is_draining",
}

// Index of `membership` column in statusHeaderWithDecommission
const statusHeaderMembershipColumnIdx = 11

type decommTestHelper struct {
	t       test.Test
	c       cluster.Cluster
	nodeIDs []int
	// randNodeBlocklist are the nodes that won't be returned from randNode().
	// populated via blockFromRandNode().
	randNodeBlocklist map[int]struct{}
}

func newDecommTestHelper(t test.Test, c cluster.Cluster) *decommTestHelper {
	var nodeIDs []int
	for i := 1; i <= c.Spec().NodeCount; i++ {
		nodeIDs = append(nodeIDs, i)
	}
	return &decommTestHelper{
		t:                 t,
		c:                 c,
		nodeIDs:           nodeIDs,
		randNodeBlocklist: map[int]struct{}{},
	}
}

// decommission decommissions the given targetNodes, running the process
// through the specified runNode.
func (h *decommTestHelper) decommission(
	ctx context.Context, targetNodes option.NodeListOption, runNode int, verbs ...string,
) (string, error) {
	args := []string{"node", "decommission"}
	args = append(args, verbs...)

	if len(targetNodes) == 1 && targetNodes[0] == runNode {
		args = append(args, "--self")
	} else {
		for _, target := range targetNodes {
			args = append(args, strconv.Itoa(target))
		}
	}
	return execCLI(ctx, h.t, h.c, runNode, args...)
}

// recommission recommissions the given targetNodes, running the process
// through the specified runNode.
func (h *decommTestHelper) recommission(
	ctx context.Context, targetNodes option.NodeListOption, runNode int, verbs ...string,
) (string, error) {
	args := []string{"node", "recommission"}
	args = append(args, verbs...)

	if len(targetNodes) == 1 && targetNodes[0] == runNode {
		args = append(args, "--self")
	} else {
		for _, target := range targetNodes {
			args = append(args, strconv.Itoa(target))
		}
	}
	return execCLI(ctx, h.t, h.c, runNode, args...)
}

// expectColumn constructs a matching regex for a given column (identified
// by its column index).
func (h *decommTestHelper) expectColumn(
	column int, columnRegex []string, numRows, numCols int,
) [][]string {
	var res [][]string
	for r := 0; r < numRows; r++ {
		build := []string{}
		for c := 0; c < numCols; c++ {
			if c == column {
				build = append(build, columnRegex[r])
			} else {
				build = append(build, `.*`)
			}
		}
		res = append(res, build)
	}
	return res
}

// expectCell constructs a matching regex for a given cell (identified by
// its row and column indexes).
func (h *decommTestHelper) expectCell(
	row, column int, regex string, numRows, numCols int,
) [][]string {
	var res [][]string
	for r := 0; r < numRows; r++ {
		build := []string{}
		for c := 0; c < numCols; c++ {
			if r == row && c == column {
				build = append(build, regex)
			} else {
				build = append(build, `.*`)
			}
		}
		res = append(res, build)
	}
	return res
}

// expectIDsInStatusOut constructs a matching regex for output of `cockroach
// node status`. It matches against the `id` column in the output generated
// with and without the `--decommission` flag.
func (h *decommTestHelper) expectIDsInStatusOut(ids []string, numCols int) [][]string {
	var res [][]string
	switch numCols {
	case len(statusHeader):
		res = append(res, statusHeader)
	case len(statusHeaderWithDecommission):
		res = append(res, statusHeaderWithDecommission)
	default:
		h.t.Fatalf(
			"Expected status output numCols to be one of %d or %d, found %d",
			len(statusHeader),
			len(statusHeaderWithDecommission),
			numCols,
		)
	}
	for _, id := range ids {
		build := []string{id}
		for i := 0; i < numCols-1; i++ {
			build = append(build, `.*`)
		}
		res = append(res, build)
	}
	return res
}

// blockFromRandNode ensures the node isn't returned from getRandNode{,OtherThan)
// in the future.
func (h *decommTestHelper) blockFromRandNode(id int) {
	h.randNodeBlocklist[id] = struct{}{}
}

// getRandNode returns a random node that hasn't
// been passed to blockFromRandNode() earlier.
func (h *decommTestHelper) getRandNode() int {
	return h.getRandNodeOtherThan()
}

// getRandNodeOtherThan is like getRandNode, but additionally
// avoids returning the specified ids.
func (h *decommTestHelper) getRandNodeOtherThan(ids ...int) int {
	m := map[int]struct{}{}
	for _, id := range ids {
		m[id] = struct{}{}
	}
	for id := range h.randNodeBlocklist {
		m[id] = struct{}{}
	}
	if len(m) == len(h.nodeIDs) {
		h.t.Fatal("all nodes are blocked")
	}
	for {
		id := h.nodeIDs[rand.Intn(len(h.nodeIDs))]
		if _, ok := m[id]; !ok {
			return id
		}
	}
}

func execCLI(
	ctx context.Context, t test.Test, c cluster.Cluster, runNode int, extraArgs ...string,
) (string, error) {
	args := []string{"./cockroach"}
	args = append(args, extraArgs...)
	args = append(args, "--insecure")
	args = append(args, fmt.Sprintf("--port={pgport:%d}", runNode))
	result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(runNode), args...)
	t.L().Printf("%s\n", result.Stdout)
	return result.Stdout, err
}
