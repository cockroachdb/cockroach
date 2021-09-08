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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

type quitTest struct {
	t    test.Test
	c    cluster.Cluster
	args option.Option
}

// runQuitTransfersLeases performs rolling restarts on a
// 3-node cluster and ascertains that each node shutting down
// transfers all its leases reliably to other nodes prior to
// terminating.
func runQuitTransfersLeases(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	methodName string,
	method func(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int),
) {
	q := quitTest{t: t, c: c}
	q.init(ctx)
	q.runTest(ctx, method)
}

func (q *quitTest) init(ctx context.Context) {
	q.args = option.StartArgs(
		"--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms",               // iterate fast for rebalancing
		"-a", "--vmodule=store=1,replica=1,replica_proposal=1", // verbosity to troubleshoot drains
	)
	q.c.Put(ctx, q.t.Cockroach(), "./cockroach")
	q.c.Start(ctx, q.args)
}

func (q *quitTest) Fatal(args ...interface{}) {
	q.t.Fatal(args...)
}

func (q *quitTest) Fatalf(format string, args ...interface{}) {
	q.t.Fatalf(format, args...)
}

func (q *quitTest) runTest(
	ctx context.Context, method func(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int),
) {
	q.waitForUpReplication(ctx)
	q.createRanges(ctx)
	q.setupIncrementalDrain(ctx)

	// runTest iterates through the cluster two times and restarts each
	// node in turn. After each node shutdown it verifies that there are
	// no leases held by the down node. (See the comments inside
	// checkNoLeases() for details.)
	//
	// The shutdown method is passed in via the 'method' parameter, used
	// below.
	q.t.L().Printf("now running restart loop\n")
	for i := 0; i < 3; i++ {
		q.t.L().Printf("iteration %d\n", i)
		for nodeID := 1; nodeID <= q.c.Spec().NodeCount; nodeID++ {
			q.t.L().Printf("stopping node %d\n", nodeID)
			q.runWithTimeout(ctx, func(ctx context.Context) { method(ctx, q.t, q.c, nodeID) })
			q.runWithTimeout(ctx, func(ctx context.Context) { q.checkNoLeases(ctx, nodeID) })
			q.t.L().Printf("restarting node %d\n", nodeID)
			q.runWithTimeout(ctx, func(ctx context.Context) { q.restartNode(ctx, nodeID) })
		}
	}
}

// restartNode restarts one node and waits until it's up and ready to
// accept clients.
func (q *quitTest) restartNode(ctx context.Context, nodeID int) {
	q.c.Start(ctx, q.args, q.c.Node(nodeID))

	q.t.L().Printf("waiting for readiness of node %d\n", nodeID)
	// Now perform a SQL query. This achieves two goals:
	// - it waits until the server is ready.
	// - the particular query forces a cluster-wide RPC; which
	//   forces any circuit breaker to trip and re-establish
	//   the RPC connection if needed.
	db := q.c.Conn(ctx, nodeID)
	defer db.Close()
	if _, err := db.ExecContext(ctx, `TABLE crdb_internal.cluster_sessions`); err != nil {
		q.Fatal(err)
	}
}

func (q *quitTest) waitForUpReplication(ctx context.Context) {
	db := q.c.Conn(ctx, 1)
	defer db.Close()

	// We'll want rebalancing to be a bit faster than normal, so
	// that the up-replication does not take ages.
	if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING	kv.snapshot_rebalance.max_rate = '128MiB'`); err != nil {
		q.Fatal(err)
	}

	err := retry.ForDuration(30*time.Second, func() error {
		q.t.L().Printf("waiting for up-replication\n")
		row := db.QueryRowContext(ctx, `SELECT min(array_length(replicas, 1)) FROM crdb_internal.ranges_no_leases`)
		minReplicas := 0
		if err := row.Scan(&minReplicas); err != nil {
			q.Fatal(err)
		}
		if minReplicas < 3 {
			time.Sleep(time.Second)
			return errors.Newf("some ranges not up-replicated yet")
		}
		return nil
	})
	if err != nil {
		q.Fatalf("cluster did not up-replicate: %v", err)
	}
}

// runWithTimeout runs a command with a 1-minute timeout.
func (q *quitTest) runWithTimeout(ctx context.Context, fn func(ctx context.Context)) {
	if err := contextutil.RunWithTimeout(ctx, "do", time.Minute, func(ctx context.Context) error {
		fn(ctx)
		return nil
	}); err != nil {
		q.Fatal(err)
	}
}

// setupIncrementalDrain simulate requiring more than one Drain round
// to transfer all leases. This way, we exercise the iterating code in
// quit/node drain.
func (q *quitTest) setupIncrementalDrain(ctx context.Context) {
	db := q.c.Conn(ctx, 1)
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
SET CLUSTER SETTING server.shutdown.lease_transfer_wait = '10ms'`); err != nil {
		if strings.Contains(err.Error(), "unknown cluster setting") {
			// old version; ok
		} else {
			q.Fatal(err)
		}
	}
}

// createRanges creates a bunch of ranges on the test cluster.
func (q *quitTest) createRanges(ctx context.Context) {
	const numRanges = 500

	db := q.c.Conn(ctx, 1)
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE t(x, y, PRIMARY KEY(x)) AS SELECT @1, 1 FROM generate_series(1,%[1]d)`,
		numRanges)); err != nil {
		q.Fatal(err)
	}
	// We split them from right-to-left so we're peeling at most 1
	// row each time on the right.
	//
	// Also we do it a hundred at a time, so as to be able to see the
	// progress when watching the roachtest progress interactively.
	for i := numRanges; i > 1; i -= 100 {
		q.t.L().Printf("creating %d ranges (%d-%d)...\n", numRanges, i, i-99)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
ALTER TABLE t SPLIT AT TABLE generate_series(%[1]d,%[1]d-99,-1)`, i)); err != nil {
			q.Fatal(err)
		}
	}
}

// checkNoLeases verifies that no range has a lease on the node
// that's just been shut down.
func (q *quitTest) checkNoLeases(ctx context.Context, nodeID int) {
	// We need to use SQL against a node that's not the one we're
	// shutting down.
	otherNodeID := 1 + nodeID%q.c.Spec().NodeCount

	// Now we're going to check two things:
	//
	// 1) *immediately*, that every range in the cluster has a lease
	//    some other place than nodeID.
	//
	//    Note that for with this condition, it is possible that _some_
	//    replica of any given range think that the leaseholder is
	//    nodeID, even though _another_ replica has become leaseholder
	//    already. That's because followers can lag behind and
	//    drain does not wait for followers to catch up.
	//    https://github.com/cockroachdb/cockroach/issues/47100
	//
	// 2) *eventually* that every other node than nodeID has no range
	//    replica whose lease refers to nodeID, i.e. the followers
	//    have all caught up.
	//    Note: when issue #47100 is fixed, this 2nd condition
	//    must be true immediately -- drain is then able to wait
	//    for all followers to learn who the new leaseholder is.

	if err := testutils.SucceedsSoonError(func() error {
		// To achieve that, we ask first each range in turn for its range
		// report.
		//
		// For condition (1) we accumulate all the known ranges in
		// knownRanges, and assign them the node ID of their leaseholder
		// whenever it is not nodeID. Then at the end we check that every
		// entry in the map has a non-zero value.
		knownRanges := map[string]int{}
		//
		// For condition (2) we accumulate the unwanted leases in
		// invLeaseMap, then check at the end that the map is empty.
		invLeaseMap := map[int][]string{}
		for i := 1; i <= q.c.Spec().NodeCount; i++ {
			if i == nodeID {
				// Can't request this node. Ignore.
				continue
			}

			q.t.L().Printf("retrieving ranges for node %d\n", i)
			// Get the report via HTTP.
			// Flag -s is to remove progress on stderr, so that the buffer
			// contains the JSON of the response and nothing else.
			adminAddrs, err := q.c.InternalAdminUIAddr(ctx, q.c.Node(otherNodeID))
			if err != nil {
				q.Fatal(err)
			}
			buf, err := q.c.RunWithBuffer(ctx, q.t.L(), q.c.Node(otherNodeID),
				"curl", "-s", fmt.Sprintf("http://%s/_status/ranges/%d",
					adminAddrs[0], i))
			if err != nil {
				q.Fatal(err)
			}
			// We need just a subset of the response. Make an ad-hoc
			// struct with just the bits of interest.
			type jsonOutput struct {
				Ranges []struct {
					State struct {
						State struct {
							Desc struct {
								RangeID string `json:"rangeId"`
							} `json:"desc"`
							Lease struct {
								Replica struct {
									NodeID int `json:"nodeId"`
								} `json:"replica"`
							} `json:"lease"`
						} `json:"state"`
					} `json:"state"`
				} `json:"ranges"`
			}
			var details jsonOutput
			if err := json.Unmarshal(buf, &details); err != nil {
				q.Fatal(err)
			}
			// Some sanity check.
			if len(details.Ranges) == 0 {
				q.Fatal("expected some ranges from RPC, got none")
			}
			// Is there any range whose lease refers to nodeID?
			var invalidLeases []string
			for _, r := range details.Ranges {
				// Some more sanity check.
				if r.State.State.Lease.Replica.NodeID == 0 {
					q.Fatalf("expected a valid lease state, got %# v", pretty.Formatter(r))
				}
				curLeaseHolder := knownRanges[r.State.State.Desc.RangeID]
				if r.State.State.Lease.Replica.NodeID == nodeID {
					// As per condition (2) above we want to know which ranges
					// have an unexpected left over lease on nodeID.
					invalidLeases = append(invalidLeases, r.State.State.Desc.RangeID)
				} else {
					// As per condition (1) above we track in knownRanges if there
					// is at least one known other than nodeID that thinks that
					// the lease has been transferred.
					curLeaseHolder = r.State.State.Lease.Replica.NodeID
				}
				knownRanges[r.State.State.Desc.RangeID] = curLeaseHolder
			}
			if len(invalidLeases) > 0 {
				invLeaseMap[i] = invalidLeases
			}
		}
		// (1): is there a range with no replica outside of nodeID?
		var leftOver []string
		for r, n := range knownRanges {
			if n == 0 {
				leftOver = append(leftOver, r)
			}
		}
		if len(leftOver) > 0 {
			q.Fatalf("(1) ranges with no lease outside of node %d: %# v", nodeID, pretty.Formatter(leftOver))
		}
		// (2): is there a range with left over replicas on nodeID?
		//
		// TODO(knz): Eventually we want this condition to be always
		// true, i.e. fail the test immediately if found to be false
		// instead of waiting. (#47100)
		if len(invLeaseMap) > 0 {
			err := errors.Newf(
				"(2) ranges with remaining leases on node %d, per node: %# v",
				nodeID, pretty.Formatter(invLeaseMap))
			q.t.L().Printf("condition failed: %v\n", err)
			q.t.L().Printf("retrying until SucceedsSoon has enough...\n")
			return err
		}
		return nil
	}); err != nil {
		q.Fatal(err)
	}

	db := q.c.Conn(ctx, otherNodeID)
	defer db.Close()
	// For good measure, also write to the table. This ensures it
	// remains available.
	if _, err := db.ExecContext(ctx, `UPDATE t SET y = y + 1`); err != nil {
		q.Fatal(err)
	}
}

func registerQuitTransfersLeases(r registry.Registry) {
	registerTest := func(name, minver string, method func(context.Context, test.Test, cluster.Cluster, int)) {
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("transfer-leases/%s", name),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(3),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runQuitTransfersLeases(ctx, t, c, name, method)
			},
		})
	}

	// Uses 'roachprod stop --sig 15 --wait', ie send SIGTERM and wait
	// until the process exits.
	registerTest("signal", "v19.2.0", func(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int) {
		c.Stop(ctx, c.Node(nodeID),
			option.RoachprodArgOption{"--sig", "15", "--wait"}, // graceful shutdown
		)
	})

	// Uses 'cockroach quit' which should drain and then request a
	// shutdown. It then waits for the process to self-exit.
	registerTest("quit", "v19.2.0", func(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int) {
		_ = runQuit(ctx, t, c, nodeID)
	})

	// Uses 'cockroach drain', followed by a non-graceful process
	// kill. If the drain is successful, the leases are transferred
	// successfully even if if the process terminates non-gracefully.
	registerTest("drain", "v20.1.0", func(ctx context.Context, t test.Test, c cluster.Cluster, nodeID int) {
		buf, err := c.RunWithBuffer(ctx, t.L(), c.Node(nodeID),
			"./cockroach", "node", "drain", "--insecure", "--logtostderr=INFO",
			fmt.Sprintf("--port={pgport:%d}", nodeID),
		)
		t.L().Printf("cockroach node drain:\n%s\n", buf)
		if err != nil {
			t.Fatal(err)
		}
		// Send first SIGHUP to the process to force it to flush its logs
		// before terminating. Otherwise the SIGKILL below will truncate
		// the log.
		c.Stop(ctx, c.Node(nodeID),
			option.RoachprodArgOption{"--sig", "1"},
		)
		// We use SIGKILL to terminate nodes here. Of course, an operator
		// should not do this and instead terminate with SIGTERM even
		// after a complete graceful drain. However, what this test is
		// asserting is that a graceful drain is *sufficient* to make
		// everything look smooth from the perspective of other nodes,
		// even if the node goes "kaput" after the drain.
		//
		// (This also ensures that the test exercises separate code; if we
		// used SIGTERM here we'd be combining the graceful drain by 'node
		// drain' with the graceful drain by the signal handler. If either
		// becomes broken, the test wouldn't help identify which one needs
		// attention.)
		c.Stop(ctx, c.Node(nodeID),
			option.RoachprodArgOption{"--sig", "9", "--wait"})
	})
}

func runQuit(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int, extraArgs ...string,
) []byte {
	args := append([]string{
		"./cockroach", "quit", "--insecure", "--logtostderr=INFO",
		fmt.Sprintf("--port={pgport:%d}", nodeID)},
		extraArgs...)
	buf, err := c.RunWithBuffer(ctx, t.L(), c.Node(nodeID), args...)
	t.L().Printf("cockroach quit:\n%s\n", buf)
	if err != nil {
		t.Fatal(err)
	}
	c.Stop(ctx, c.Node(nodeID),
		option.RoachprodArgOption{"--sig", "0", "--wait"}, // no shutdown, just wait for exit
	)
	return buf
}

func registerQuitAllNodes(r registry.Registry) {
	// This test verifies that 'cockroach quit' can terminate all nodes
	// in the cluster: normally as long as there's quorum, then with a
	// short --drain-wait for the remaining nodes under quorum.
	r.Add(registry.TestSpec{
		Name:    "quit-all-nodes",
		Owner:   registry.OwnerServer,
		Cluster: r.MakeClusterSpec(5),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			q := quitTest{t: t, c: c}

			// Start the cluster.
			q.init(ctx)
			// Wait for up-replication so that the cluster expects 1 ranges
			// everywhere for system ranges.
			q.waitForUpReplication(ctx)

			// Shut one nodes down gracefully with a very long wait (longer
			// than the test timeout). This is guaranteed to work - we still
			// have quorum at that point.
			q.runWithTimeout(ctx, func(ctx context.Context) { _ = runQuit(ctx, q.t, q.c, 5, "--drain-wait=1h") })

			// Now shut down the remaining 4 nodes less gracefully, with a
			// short wait.

			// For the next two nodes, we may or may not observe that
			// the graceful shutdown succeed. It may succeed if every
			// range has enough quorum on the last 2 nodes (shut down later below).
			// It may fail if some ranges have a quorum composed of n3, n4, n5.
			// See: https://github.com/cockroachdb/cockroach/issues/48339
			q.runWithTimeout(ctx, func(ctx context.Context) { _ = runQuit(ctx, q.t, q.c, 4, "--drain-wait=4s") })
			q.runWithTimeout(ctx, func(ctx context.Context) { _ = runQuit(ctx, q.t, q.c, 3, "--drain-wait=4s") })

			// For the lat two nodes, we are always under quorum. In this
			// case we can expect `quit` to always report a hard shutdown
			// was required.
			q.runWithTimeout(ctx, func(ctx context.Context) { expectHardShutdown(ctx, q.t, runQuit(ctx, q.t, q.c, 2, "--drain-wait=4s")) })
			q.runWithTimeout(ctx, func(ctx context.Context) { expectHardShutdown(ctx, q.t, runQuit(ctx, q.t, q.c, 1, "--drain-wait=4s")) })

			// At the end, restart all nodes. We do this to check that
			// the cluster can indeed restart, and also to please
			// the dead node detection check at the end of each test.
			q.c.Start(ctx, q.args)
		},
	})
}

// expectHardShutdown expects a "drain did not complete successfully" message.
func expectHardShutdown(ctx context.Context, t test.Test, cmdOut []byte) {
	if !strings.Contains(string(cmdOut), "drain did not complete successfully") {
		t.Fatalf("expected 'drain did not complete successfully' in quit output, got:\n%s", cmdOut)
	}
}
