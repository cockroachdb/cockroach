// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ballastPath returns the path to the disk-fill ballast file for the
// given store index (1-based).
func ballastPath(storeIdx int) string {
	return fmt.Sprintf("{store-dir:%d}/disk-fill-ballast", storeIdx)
}

// removeFillFiles removes ballast files from all stores on the target
// node. rm -f is idempotent, so this is safe to call multiple times or
// when files do not exist.
func removeFillFiles(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	node option.NodeListOption,
	storeCount int,
) {
	for i := 1; i <= storeCount; i++ {
		if err := c.RunE(
			ctx, option.WithNodes(node),
			fmt.Sprintf("rm -f %s", ballastPath(i)),
		); err != nil {
			o.Errorf("failed to remove ballast on store %d: %v", i, err)
		}
	}
}

type cleanupDiskFill struct {
	node       option.NodeListOption
	storeCount int
}

func (cl *cleanupDiskFill) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status(fmt.Sprintf(
		"removing fill files on node %s (%d stores)",
		cl.node.NodeIDsString(), cl.storeCount,
	))
	removeFillFiles(ctx, o, c, cl.node, cl.storeCount)

	// Retry the SQL connection a few times before concluding the node
	// is down. Disk pressure may cause temporary unresponsiveness that
	// resolves on its own once the fill files are removed.
	if helpers.CheckNodeHealth(ctx, o, c, cl.node[0], 5 /* maxAttempts */, 5*time.Second) {
		o.Status(fmt.Sprintf("node %s healthy after cleanup", cl.node.NodeIDsString()))
		return
	}

	// Node is unresponsive — restart it via cockroach.sh (matches
	// disk_stall cleanup pattern).
	o.Status(fmt.Sprintf(
		"node %d unresponsive after cleanup, restarting via cockroach.sh",
		cl.node[0],
	))
	if err := c.RunE(ctx, option.WithNodes(cl.node), "./cockroach.sh"); err != nil {
		o.Errorf("failed to restart node %d: %v", cl.node[0], err)
		return
	}

	// Verify health after restart.
	if !helpers.CheckNodeHealth(ctx, o, c, cl.node[0], 5 /* maxAttempts */, 10*time.Second) {
		o.Errorf("node %d still unresponsive after restart", cl.node[0])
		return
	}
	o.Status(fmt.Sprintf("node %d healthy after restart", cl.node[0]))
}

// queryStoreAvailKB returns the available disk space in KB for a
// specific store (by 1-based index) on the target node, or -1 if the
// query or parse fails.
func queryStoreAvailKB(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	node option.NodeListOption,
	storeIdx int,
) int64 {
	result, err := c.RunWithDetailsSingleNode(
		ctx, o.L(), option.WithNodes(node),
		fmt.Sprintf(
			"df --output=avail {store-dir:%d} | tail -1 | tr -d ' '",
			storeIdx,
		),
	)
	if err != nil {
		o.Errorf("failed to query disk space on store %d: %v", storeIdx, err)
		return -1
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(result.Stdout), 10, 64)
	if err != nil {
		o.Errorf("failed to parse available space on store %d: %v", storeIdx, err)
		return -1
	}
	return kb
}

// queryUnavailableRanges returns the count of unavailable ranges
// across the cluster, queried from the given node. Returns -1 on
// failure.
func queryUnavailableRanges(
	ctx context.Context, o operation.Operation, c cluster.Cluster, nodeID int,
) int {
	db, err := c.ConnE(ctx, o.L(), nodeID)
	if err != nil {
		o.Errorf("failed to connect to node %d for range check: %v", nodeID, err)
		return -1
	}
	defer db.Close()

	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COALESCE(sum(unavailable_ranges), 0) FROM system.replication_stats",
	).Scan(&count)
	if err != nil {
		o.Errorf("failed to query unavailable ranges from node %d: %v", nodeID, err)
		return -1
	}
	return count
}

func runDiskFill(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	defer func() {
		if r := recover(); r != nil {
			// Log the panic but do NOT nil out cleanup — if it was
			// assigned before the panic, the framework should still
			// run it to remove ballast files and restart the node.
			o.Errorf("panic during disk fill: %v", r)
		}
	}()

	nodes := c.All()
	if len(nodes) < 2 {
		o.Status("skipping disk-fill: need at least 2 nodes")
		return nil
	}

	// Connect to any available node and query gossip for live nodes so
	// we only target a node that is actually running.
	var liveNodes option.NodeListOption
	for _, n := range nodes {
		db, err := c.ConnE(ctx, o.L(), n)
		if err != nil {
			continue
		}
		rows, err := db.QueryContext(ctx,
			"SELECT node_id FROM crdb_internal.gossip_nodes WHERE is_live = true")
		if err != nil {
			db.Close()
			continue
		}
		for rows.Next() {
			var nid int
			if err := rows.Scan(&nid); err != nil {
				break
			}
			liveNodes = append(liveNodes, nid)
		}
		rows.Close()
		db.Close()
		break
	}
	if len(liveNodes) < 2 {
		o.Status(fmt.Sprintf(
			"skipping disk-fill: need at least 2 live nodes, found %d",
			len(liveNodes),
		))
		return nil
	}

	rng, _ := randutil.NewPseudoRand()
	targetNode := liveNodes.SeededRandNode(rng)

	// Determine store count on the target node.
	db, err := c.ConnE(ctx, o.L(), targetNode[0])
	if err != nil {
		o.Fatalf("failed to connect to target node %d: %v", targetNode[0], err)
	}
	storeCount := helpers.GetStoreCount(ctx, o, db, targetNode[0])
	db.Close()

	const (
		// reserveBytes is the free space to leave per store after
		// filling. 1 GiB per store creates real pressure; with 4
		// stores that's 4 GiB total headroom on the node.
		reserveBytes = 1 << 30 // 1 GiB
		// minFreeRequired is the minimum free space per store
		// required to proceed with filling. Skip stores that are
		// already tight.
		minFreeRequired = 2 << 30 // 2 GiB
	)

	// Assign cleanup before any destructive action so the ballast
	// files are always removed — even if a later step panics.
	cleanup = &cleanupDiskFill{node: targetNode, storeCount: storeCount}

	// Fill each store on the target node.
	filledAny := false
	for i := 1; i <= storeCount; i++ {
		availKB := queryStoreAvailKB(ctx, o, c, targetNode, i)
		if availKB < 0 {
			o.Errorf("unable to determine disk space on store %d, skipping", i)
			continue
		}
		availBytes := availKB * 1024

		if availBytes < minFreeRequired {
			o.Status(fmt.Sprintf(
				"store %d: only %d MiB free, need at least %d MiB — skipping",
				i, availBytes>>20, minFreeRequired>>20,
			))
			continue
		}

		fillBytes := availBytes - reserveBytes
		o.Status(fmt.Sprintf(
			"store %d: filling %d MiB (leaving %d MiB reserve)",
			i, fillBytes>>20, reserveBytes>>20,
		))

		// fallocate is O(1) on ext4/xfs (standard on GCE and AWS).
		if err := c.RunE(
			ctx, option.WithNodes(targetNode),
			fmt.Sprintf("fallocate -l %d %s", fillBytes, ballastPath(i)),
		); err != nil {
			o.Errorf("fallocate failed on store %d: %v", i, err)
			continue
		}
		filledAny = true
	}

	if !filledAny {
		o.Status("no stores filled — cleaning up and returning")
		removeFillFiles(ctx, o, c, targetNode, storeCount)
		return nil
	}

	// Pick a peer node for cluster-level health and range checks.
	var peerNodeID int
	for _, n := range liveNodes {
		if n != targetNode[0] {
			peerNodeID = n
			break
		}
	}

	// Monitor the node under disk pressure. Each iteration checks:
	// (a) all stores' free space, (b) target node health, (c) peer
	// node health, and (d) range availability.
	//
	// If the target node goes down, that is an expected outcome of
	// the pressure test — log it and proceed to cleanup. If a peer
	// node goes down, that indicates a broader cluster problem and
	// is fatal.
	const (
		monitorDuration = 15 * time.Minute
		checkInterval   = 2 * time.Minute
		// dangerThresholdKB is the minimum free space (512 MiB)
		// below which we abort the pressure period early. This
		// aligns with Pebble's emergency ballast size.
		dangerThresholdKB = 512 * 1024 // 512 MiB in KB
	)
	deadline := timeutil.Now().Add(monitorDuration)
	monitorCtx, monitorCancel := context.WithDeadline(ctx, deadline)
	defer monitorCancel()
	tick := 0
	for timeutil.Now().Before(deadline) {
		if monitorCtx.Err() != nil {
			o.Status("monitor period ended, proceeding to cleanup")
			break
		}

		tick++

		// Check target node health. If it's down, that's an
		// expected outcome of disk pressure — log and exit the
		// monitoring loop.
		if !helpers.CheckNodeHealth(monitorCtx, o, c, targetNode[0], 3 /* maxAttempts */, 5*time.Second) {
			o.Status(fmt.Sprintf(
				"check %d: target node %d went down under disk pressure (expected behavior)",
				tick, targetNode[0],
			))
			break
		}

		// Check peer node health — if a non-target node is
		// unresponsive, something is wrong beyond our test.
		if !helpers.CheckNodeHealth(monitorCtx, o, c, peerNodeID, 3 /* maxAttempts */, 5*time.Second) {
			o.Fatalf("check %d: peer node %d unresponsive", tick, peerNodeID)
		}

		// Log per-store disk usage and check danger threshold
		// across all stores.
		belowDanger := false
		for i := 1; i <= storeCount; i++ {
			curAvailKB := queryStoreAvailKB(monitorCtx, o, c, targetNode, i)
			if curAvailKB < 0 {
				o.Errorf("check %d: unable to read store %d disk space", tick, i)
				continue
			}
			remaining := time.Until(deadline).Truncate(time.Second)
			o.Status(fmt.Sprintf(
				"check %d: store %d: %d MiB free, %s remaining",
				tick, i, curAvailKB/1024, remaining,
			))
			if curAvailKB < dangerThresholdKB {
				o.Status(fmt.Sprintf(
					"check %d: store %d dropped to %d MiB (below %d MiB danger threshold)",
					tick, i, curAvailKB/1024, dangerThresholdKB/1024,
				))
				belowDanger = true
			}
		}
		if belowDanger {
			o.Status("ending pressure period early — store below danger threshold")
			break
		}

		// Check range availability from the peer node.
		unavail := queryUnavailableRanges(monitorCtx, o, c, peerNodeID)
		if unavail > 0 {
			o.Status(fmt.Sprintf(
				"check %d: %d unavailable ranges detected during pressure",
				tick, unavail,
			))
		} else if unavail == 0 {
			o.Status(fmt.Sprintf("check %d: 0 unavailable ranges", tick))
		}

		// Sleep until next check, waking on context cancellation.
		sleepDur := checkInterval
		if untilDeadline := time.Until(deadline); untilDeadline < sleepDur {
			sleepDur = untilDeadline
		}
		if sleepDur > 0 {
			select {
			case <-time.After(sleepDur):
			case <-monitorCtx.Done():
			}
		}
	}

	elapsed := monitorDuration - time.Until(deadline)
	o.Status(fmt.Sprintf(
		"disk pressure period complete on node %d after %s",
		targetNode[0], elapsed.Truncate(time.Second),
	))

	return cleanup
}

func registerDiskFill(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "disk-fill/ood",
		Owner:              registry.OwnerStorage,
		Timeout:            45 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies: []registry.OperationDependency{},
		Run: runDiskFill,
	})
}
