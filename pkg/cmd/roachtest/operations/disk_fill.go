// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// reserveBytes is the free space to leave per store after
	// filling. 1 GiB per store creates real pressure; with 4
	// stores that's 4 GiB total headroom on the node.
	reserveBytes int64 = 1 << 30 // 1 GiB
	// minFreeRequired is the minimum free space per store
	// required to proceed with filling. Skip stores that are
	// already tight.
	minFreeRequired int64 = 2 << 30 // 2 GiB

	monitorDuration = 15 * time.Minute
	checkInterval   = 2 * time.Minute
	// dangerThresholdBytes is the minimum free space (512 MiB)
	// below which we abort the pressure period early. This
	// aligns with Pebble's emergency ballast size.
	dangerThresholdBytes int64 = 512 << 20 // 512 MiB
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

// fillStores creates ballast files on each store of the target node,
// leaving reserveBytes free per store. Returns true if at least one
// store was filled.
func fillStores(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	targetNode option.NodeListOption,
	storeCount int,
) bool {
	filledAny := false
	for i := 1; i <= storeCount; i++ {
		availBytes, err := roachtestutil.GetDiskAvailInBytes(
			ctx, c, o.L(), targetNode[0], i,
		)
		if err != nil {
			o.Errorf("unable to determine disk space on store %d: %v", i, err)
			continue
		}

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
	return filledAny
}

// runMonitorCheck performs a single iteration of the disk pressure
// monitoring loop. It checks target and peer node health, per-store
// free space, and range availability. Returns true if monitoring
// should stop (target node down, danger threshold hit, etc.).
func runMonitorCheck(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	tick int,
	targetNode option.NodeListOption,
	peerNodeID int,
	storeCount int,
	deadline time.Time,
) bool {
	// Check target node health. If it's down, that's an expected
	// outcome of disk pressure — log and stop monitoring.
	if !helpers.CheckNodeHealth(ctx, o, c, targetNode[0], 3 /* maxAttempts */, 5*time.Second) {
		o.Status(fmt.Sprintf(
			"check %d: target node %d went down under disk pressure (expected behavior)",
			tick, targetNode[0],
		))
		return true
	}

	// Check peer node health — if a non-target node is
	// unresponsive, something is wrong beyond our test.
	if !helpers.CheckNodeHealth(ctx, o, c, peerNodeID, 3 /* maxAttempts */, 5*time.Second) {
		o.Fatalf("check %d: peer node %d unresponsive", tick, peerNodeID)
	}

	// Log per-store disk usage and check danger threshold.
	belowDanger := false
	for i := 1; i <= storeCount; i++ {
		curAvailBytes, err := roachtestutil.GetDiskAvailInBytes(
			ctx, c, o.L(), targetNode[0], i,
		)
		if err != nil {
			o.Errorf("check %d: unable to read store %d disk space: %v", tick, i, err)
			continue
		}
		remaining := time.Until(deadline).Truncate(time.Second)
		o.Status(fmt.Sprintf(
			"check %d: store %d: %d MiB free, %s remaining",
			tick, i, curAvailBytes>>20, remaining,
		))
		if curAvailBytes < dangerThresholdBytes {
			o.Status(fmt.Sprintf(
				"check %d: store %d dropped to %d MiB (below %d MiB danger threshold)",
				tick, i, curAvailBytes>>20, dangerThresholdBytes>>20,
			))
			belowDanger = true
		}
	}
	if belowDanger {
		o.Status("ending pressure period early — store below danger threshold")
		return true
	}

	// Check range availability from the peer node. Uses the same
	// query as checkZeroUnavailableRanges in dependency.go, but
	// connects to a specific peer (the target may be under pressure)
	// and logs the count rather than returning a bool.
	db, err := c.ConnE(ctx, o.L(), peerNodeID)
	if err != nil {
		o.Errorf("check %d: failed to connect to node %d for range check: %v",
			tick, peerNodeID, err)
		return false
	}
	defer db.Close()
	var unavail int
	if err := db.QueryRowContext(ctx,
		"SELECT COALESCE(sum(unavailable_ranges), 0) FROM system.replication_stats",
	).Scan(&unavail); err != nil {
		o.Errorf("check %d: failed to query unavailable ranges: %v", tick, err)
	} else if unavail > 0 {
		o.Status(fmt.Sprintf(
			"check %d: %d unavailable ranges detected during pressure",
			tick, unavail,
		))
	} else {
		o.Status(fmt.Sprintf("check %d: 0 unavailable ranges", tick))
	}

	return false
}

// monitorDiskPressure watches the cluster while a node is under disk
// pressure. It checks node health, store free space, and range
// availability at regular intervals for monitorDuration.
func monitorDiskPressure(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	targetNode option.NodeListOption,
	storeCount int,
	peerNodeID int,
) time.Duration {
	start := timeutil.Now()
	deadline := start.Add(monitorDuration)
	monitorCtx, monitorCancel := context.WithDeadline(ctx, deadline)
	defer monitorCancel()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	elapsed := func() time.Duration { return timeutil.Since(start) }

	// Run the first check immediately so we catch fast failures
	// right after filling (the ticker only fires after the first
	// interval).
	tick := 1
	if runMonitorCheck(monitorCtx, o, c, tick, targetNode,
		peerNodeID, storeCount, deadline) {
		return elapsed()
	}

	for {
		select {
		case <-ticker.C:
			if timeutil.Now().After(deadline) {
				return elapsed()
			}
			tick++
			if runMonitorCheck(monitorCtx, o, c, tick, targetNode,
				peerNodeID, storeCount, deadline) {
				return elapsed()
			}
		case <-monitorCtx.Done():
			o.Status("monitor period ended, proceeding to cleanup")
			return elapsed()
		}
	}
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

	liveNodes := helpers.GetLiveNodes(ctx, o, c)
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

	// Assign cleanup before any destructive action so the ballast
	// files are always removed — even if a later step panics.
	cleanup = &cleanupDiskFill{node: targetNode, storeCount: storeCount}

	if !fillStores(ctx, o, c, targetNode, storeCount) {
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

	elapsed := monitorDiskPressure(ctx, o, c, targetNode, storeCount, peerNodeID)

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
		Dependencies:       []registry.OperationDependency{},
		Run:                runDiskFill,
	})
}
