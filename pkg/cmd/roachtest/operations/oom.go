// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
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
	oomMonitorDuration = 10 * time.Minute
	oomCheckInterval   = 30 * time.Second
	// oomReserveBytes is the amount of free memory to leave on the
	// node after filling /dev/shm. Enough headroom to keep
	// cockroach alive under pressure so we can capture heap
	// profiles before the OOM killer fires.
	oomReserveBytes int64 = 3 << 30 // 3 GiB
	// oomMinFreeRequired is the minimum free memory required to
	// proceed with the fill. Skip nodes that are already tight.
	oomMinFreeRequired int64 = 2 << 30 // 2 GiB
	oomBallastPath           = "/dev/shm/oom-ballast"
)

type cleanupOOM struct {
	node option.NodeListOption
}

func (cl *cleanupOOM) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status(fmt.Sprintf("cleaning up OOM operation on node %s", cl.node.NodeIDsString()))

	// runCmd wraps each remote command in a timeout so cleanup does
	// not hang indefinitely when the node is unresponsive after OOM.
	runCmd := func(cmd string, timeout time.Duration) error {
		cmdCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return c.RunE(cmdCtx, option.WithNodes(cl.node), cmd)
	}

	removeBallast := func() {
		_ = runCmd(fmt.Sprintf("rm -f %s", oomBallastPath), 30*time.Second)
		_ = runCmd("sudo mount -o remount,size=50% /dev/shm || true", 30*time.Second)
	}

	// Try to free the ballast. If the node is unresponsive, these
	// will time out in 30s instead of hanging forever.
	removeBallast()

	healthCtx, healthCancel := context.WithTimeout(ctx, 60*time.Second)
	defer healthCancel()
	if helpers.CheckNodeHealth(healthCtx, o, c, cl.node[0], 3 /* maxAttempts */, 5*time.Second) {
		o.Status(fmt.Sprintf("node %s healthy after OOM cleanup", cl.node.NodeIDsString()))
		return
	}

	o.Status(fmt.Sprintf(
		"node %d unresponsive after OOM, restarting via cockroach.sh",
		cl.node[0],
	))
	if err := runCmd("./cockroach.sh", 2*time.Minute); err != nil {
		o.Errorf("failed to restart node %d: %v", cl.node[0], err)
		return
	}

	// Retry ballast removal after restart — it may still be present.
	removeBallast()

	if !helpers.CheckNodeHealth(ctx, o, c, cl.node[0], 5 /* maxAttempts */, 10*time.Second) {
		o.Errorf("node %d still unresponsive after restart", cl.node[0])
		return
	}
	o.Status(fmt.Sprintf("node %d healthy after restart", cl.node[0]))
}

// captureHeapProfile fetches a Go heap profile from the target node's
// pprof HTTP endpoint. Failures are non-fatal since the node may
// become unresponsive under memory pressure.
func captureHeapProfile(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	node option.NodeListOption,
	client *roachtestutil.RoachtestHTTPClient,
	profileNum int,
) {
	adminAddrs, err := c.ExternalAdminUIAddr(ctx, o.L(), node)
	if err != nil {
		o.Errorf("heap profile %d: failed to get admin UI addr: %v", profileNum, err)
		return
	}

	url := fmt.Sprintf("https://%s/debug/pprof/heap", adminAddrs[0])
	resp, err := client.Get(ctx, url)
	if err != nil {
		o.Errorf("heap profile %d: HTTP request failed: %v", profileNum, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		o.Errorf("heap profile %d: HTTP %d", profileNum, resp.StatusCode)
		return
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		o.Errorf("heap profile %d: failed to read response: %v", profileNum, err)
		return
	}

	o.Status(fmt.Sprintf("heap profile %d: captured %d bytes", profileNum, buf.Len()))
}

// getAvailableMemBytes reads MemAvailable from /proc/meminfo on the
// target node. Returns the available memory in bytes.
func getAvailableMemBytes(
	ctx context.Context, o operation.Operation, c cluster.Cluster, node option.NodeListOption,
) (int64, error) {
	result, err := c.RunWithDetailsSingleNode(
		ctx, o.L(), option.WithNodes(node),
		"awk '/MemAvailable/ {print $2}' /proc/meminfo",
	)
	if err != nil {
		return 0, err
	}

	var kB int64
	if _, err := fmt.Sscanf(result.Stdout, "%d", &kB); err != nil {
		return 0, fmt.Errorf("parsing MemAvailable: %w", err)
	}
	return kB * 1024, nil
}

// getShmAvailBytes returns the available space in /dev/shm in bytes.
func getShmAvailBytes(
	ctx context.Context, o operation.Operation, c cluster.Cluster, node option.NodeListOption,
) (int64, error) {
	result, err := c.RunWithDetailsSingleNode(
		ctx, o.L(), option.WithNodes(node),
		"df -B1 /dev/shm | awk 'NR==2 {print $4}'",
	)
	if err != nil {
		return 0, err
	}

	var b int64
	if _, err := fmt.Sscanf(result.Stdout, "%d", &b); err != nil {
		return 0, fmt.Errorf("parsing /dev/shm available: %w", err)
	}
	return b, nil
}

// resizeSHM remounts /dev/shm to use up to 90% of total RAM. The
// default tmpfs size is 50% of total RAM, which limits how much
// memory pressure we can create.
func resizeSHM(
	ctx context.Context, o operation.Operation, c cluster.Cluster, node option.NodeListOption,
) {
	o.Status("remounting /dev/shm to 90% of total RAM")
	if err := c.RunE(
		ctx, option.WithNodes(node),
		"sudo mount -o remount,size=90% /dev/shm",
	); err != nil {
		o.Errorf("failed to resize /dev/shm: %v", err)
	}
}

// fillMemory writes a ballast file to /dev/shm (a RAM-backed tmpfs)
// to consume memory on the target node, leaving oomReserveBytes free.
// Returns true if the fill succeeded.
func fillMemory(
	ctx context.Context, o operation.Operation, c cluster.Cluster, node option.NodeListOption,
) bool {
	resizeSHM(ctx, o, c, node)

	availBytes, err := getAvailableMemBytes(ctx, o, c, node)
	if err != nil {
		o.Errorf("unable to determine available memory: %v", err)
		return false
	}

	if availBytes < oomMinFreeRequired {
		o.Status(fmt.Sprintf(
			"only %d MiB free, need at least %d MiB — skipping",
			availBytes>>20, oomMinFreeRequired>>20,
		))
		return false
	}

	shmAvail, err := getShmAvailBytes(ctx, o, c, node)
	if err != nil {
		o.Errorf("unable to determine /dev/shm space: %v", err)
		return false
	}

	fillBytes := availBytes - oomReserveBytes
	if fillBytes > shmAvail {
		fillBytes = shmAvail
	}

	o.Status(fmt.Sprintf(
		"filling /dev/shm with %d MiB (mem available: %d MiB, shm available: %d MiB, reserve: %d MiB)",
		fillBytes>>20, availBytes>>20, shmAvail>>20, oomReserveBytes>>20,
	))

	// Use /dev/urandom instead of /dev/zero — the kernel
	// deduplicates identical zero pages via KSM, so zero-filled
	// tmpfs pages get reclaimed almost immediately.
	cmd := fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=1M count=%d",
		oomBallastPath, fillBytes>>20,
	)
	if err := c.RunE(ctx, option.WithNodes(node), cmd); err != nil {
		o.Errorf("dd to /dev/shm failed: %v", err)
		return false
	}
	return true
}

// monitorOOMPressure watches the cluster while a node is under memory
// pressure. It checks node health, captures heap profiles, and logs
// memory status at regular intervals. Returns the elapsed monitoring
// duration.
func monitorOOMPressure(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	targetNode option.NodeListOption,
	peerNodeID int,
	client *roachtestutil.RoachtestHTTPClient,
) time.Duration {
	start := timeutil.Now()
	deadline := start.Add(oomMonitorDuration)
	monitorCtx, monitorCancel := context.WithDeadline(ctx, deadline)
	defer monitorCancel()

	ticker := time.NewTicker(oomCheckInterval)
	defer ticker.Stop()

	elapsed := func() time.Duration { return timeutil.Since(start) }

	profileNum := 1
	runCheck := func(tick int) bool {
		if !helpers.CheckNodeHealth(
			monitorCtx, o, c, targetNode[0], 2 /* maxAttempts */, 3*time.Second,
		) {
			o.Status(fmt.Sprintf(
				"check %d: target node %d went down under memory pressure (expected)",
				tick, targetNode[0],
			))
			return true
		}

		if !helpers.CheckNodeHealth(monitorCtx, o, c, peerNodeID, 3 /* maxAttempts */, 5*time.Second) {
			o.Fatalf("check %d: peer node %d unresponsive", tick, peerNodeID)
		}

		captureHeapProfile(monitorCtx, o, c, targetNode, client, profileNum)
		profileNum++

		// Log memory status on the target node. Failure is non-fatal.
		result, err := c.RunWithDetailsSingleNode(
			monitorCtx, o.L(), option.WithNodes(targetNode), "free -m | head -2",
		)
		if err == nil {
			o.Status(fmt.Sprintf("check %d: memory:\n%s", tick, result.Stdout))
		}

		// Check range availability from the peer node.
		db, err := c.ConnE(monitorCtx, o.L(), peerNodeID)
		if err != nil {
			o.Errorf("check %d: failed to connect to peer %d: %v", tick, peerNodeID, err)
			return false
		}
		defer db.Close()

		var unavail int
		if err := db.QueryRowContext(monitorCtx,
			"SELECT COALESCE(sum(unavailable_ranges), 0) FROM system.replication_stats",
		).Scan(&unavail); err != nil {
			o.Errorf("check %d: failed to query unavailable ranges: %v", tick, err)
		} else if unavail > 0 {
			o.Status(fmt.Sprintf(
				"check %d: %d unavailable ranges during OOM pressure",
				tick, unavail,
			))
		} else {
			o.Status(fmt.Sprintf("check %d: 0 unavailable ranges", tick))
		}

		return false
	}

	// Run the first check immediately.
	tick := 1
	if runCheck(tick) {
		return elapsed()
	}

	for {
		select {
		case <-ticker.C:
			if timeutil.Now().After(deadline) {
				o.Status("OOM monitor period ended")
				return elapsed()
			}
			tick++
			if runCheck(tick) {
				return elapsed()
			}
		case <-monitorCtx.Done():
			o.Status("OOM monitor period ended")
			return elapsed()
		}
	}
}

func runOOM(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	defer func() {
		if r := recover(); r != nil {
			o.Errorf("panic during OOM operation: %v", r)
		}
	}()

	nodes := c.All()
	if len(nodes) < 2 {
		o.Status("skipping oom: need at least 2 nodes")
		return nil
	}

	liveNodes := helpers.GetLiveNodes(ctx, o, c)
	if len(liveNodes) < 2 {
		o.Status(fmt.Sprintf(
			"skipping oom: need at least 2 live nodes, found %d",
			len(liveNodes),
		))
		return nil
	}

	rng, _ := randutil.NewPseudoRand()
	targetNode := liveNodes.SeededRandNode(rng)

	// Assign cleanup before any destructive action.
	cleanup = &cleanupOOM{node: targetNode}

	client := roachtestutil.DefaultHTTPClient(c, o.L(), roachtestutil.HTTPTimeout(30*time.Second))

	// Capture baseline heap profile before inducing pressure.
	captureHeapProfile(ctx, o, c, targetNode, client, 0 /* profileNum */)

	if !fillMemory(ctx, o, c, targetNode) {
		o.Status("memory fill failed — cleaning up and returning")
		_ = c.RunE(ctx, option.WithNodes(targetNode), fmt.Sprintf("rm -f %s", oomBallastPath))
		return nil
	}

	// Pick a peer node for health and range checks.
	var peerNodeID int
	for _, n := range liveNodes {
		if n != targetNode[0] {
			peerNodeID = n
			break
		}
	}

	elapsed := monitorOOMPressure(ctx, o, c, targetNode, peerNodeID, client)

	o.Status(fmt.Sprintf(
		"OOM pressure period complete on node %d after %s",
		targetNode[0], elapsed.Truncate(time.Second),
	))

	return cleanup
}

func registerOOM(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "oom",
		Owner:              registry.OwnerServer,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies: []registry.OperationDependency{
			registry.OperationRequiresZeroUnderreplicatedRanges,
		},
		Run: runOOM,
	})
}
