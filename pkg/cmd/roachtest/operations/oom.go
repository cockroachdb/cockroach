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
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	oomPressureWorkers = 16
	oomQueryRetryDelay = 2 * time.Second
	oomWorkmemSetting  = "2GiB"
)

// oomPressureQueries returns SQL statements sized to hold ~1.53 GiB of
// intermediate data in memory per query (25,000 rows x 64 KiB each).
// With workmem set to 2 GiB, operators hold all data in memory rather
// than spilling to disk. With 16 concurrent workers, the aggregate
// demand (~24.5 GiB) exceeds the root SQL memory pool (~25% of RAM),
// keeping it saturated.
func oomPressureQueries() []string {
	return []string{
		// In-memory sort: the sort operator buffers all 25K rows
		// (~1.53 GiB) before producing output.
		`SELECT sum(length(data)) FROM (
			SELECT i, repeat('x', 65536) AS data
			FROM generate_series(1, 25000) AS g(i)
			ORDER BY data, i
		) sub`,

		// Hash join on unique 64 KiB strings: the build side loads
		// 25K rows into the hash table (~1.53 GiB). Each lpad()
		// produces a unique string so the join is 1:1.
		`SELECT count(*) FROM (
			SELECT i, lpad(i::text, 65536, 'x') AS data
			FROM generate_series(1, 25000) AS g(i)
		) a JOIN (
			SELECT i, lpad(i::text, 65536, 'x') AS data
			FROM generate_series(1, 25000) AS g(i)
		) b ON a.data = b.data`,

		// Hash aggregation with 25K unique 64 KiB group keys: the
		// hash table holds all distinct keys (~1.53 GiB).
		`SELECT count(*) FROM (
			SELECT lpad(i::text, 65536, 'x') AS data, count(*) AS c
			FROM generate_series(1, 25000) AS g(i)
			GROUP BY data
		) sub`,
	}
}

// runPressureWorker opens a SQL connection to the target node and
// repeatedly runs memory-intensive queries until ctx is cancelled.
func runPressureWorker(
	ctx context.Context, o operation.Operation, c cluster.Cluster, targetNodeID int, workerID int,
) {
	db, err := c.ConnE(ctx, o.L(), targetNodeID)
	if err != nil {
		o.Status(fmt.Sprintf(
			"pressure worker %d: connection failed: %v", workerID, err,
		))
		return
	}
	defer db.Close()

	queries := oomPressureQueries()
	for iteration := 0; ; iteration++ {
		if ctx.Err() != nil {
			return
		}
		query := queries[iteration%len(queries)]
		if _, err := db.ExecContext(ctx, query); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Memory budget errors are the intended outcome — only
			// log unexpected failures.
			if !strings.Contains(err.Error(), "memory budget exceeded") {
				o.Status(fmt.Sprintf(
					"pressure worker %d: query failed: %v", workerID, err,
				))
			}
			select {
			case <-time.After(oomQueryRetryDelay):
			case <-ctx.Done():
				return
			}
		}
	}
}

// startPressure launches oomPressureWorkers goroutines that run
// memory-intensive SQL queries on the target node. Returns a cancel
// function to stop all workers and a channel that closes when all
// workers have exited.
func startPressure(
	ctx context.Context, o operation.Operation, c cluster.Cluster, targetNodeID int,
) (context.CancelFunc, <-chan struct{}) {
	pressureCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	doneCh := make(chan struct{})

	wg.Add(oomPressureWorkers)
	for i := 0; i < oomPressureWorkers; i++ {
		workerID := i
		go func() {
			defer wg.Done()
			runPressureWorker(pressureCtx, o, c, targetNodeID, workerID)
		}()
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	return cancel, doneCh
}

type cleanupOOM struct {
	node           option.NodeListOption
	peerNodeID     int
	cancelPressure context.CancelFunc
	pressureDone   <-chan struct{}
}

func (cl *cleanupOOM) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status(fmt.Sprintf(
		"cleaning up OOM operation on node %s", cl.node.NodeIDsString(),
	))

	cl.cancelPressure()

	select {
	case <-cl.pressureDone:
		o.Status("all pressure workers stopped")
	case <-time.After(30 * time.Second):
		o.Status("timed out waiting for pressure workers to stop")
	}

	// Reset the cluster setting via a peer node since the target may
	// be dead. Non-fatal if this fails — the default is safe.
	if db, err := c.ConnE(ctx, o.L(), cl.peerNodeID); err == nil {
		_, resetErr := db.ExecContext(ctx,
			"RESET CLUSTER SETTING sql.distsql.temp_storage.workmem",
		)
		db.Close()
		if resetErr != nil {
			o.Errorf("failed to reset workmem cluster setting: %v", resetErr)
		} else {
			o.Status("reset sql.distsql.temp_storage.workmem to default")
		}
	} else {
		o.Errorf("failed to connect to peer %d for setting reset: %v",
			cl.peerNodeID, err,
		)
	}

	healthCtx, healthCancel := context.WithTimeout(ctx, 60*time.Second)
	defer healthCancel()
	if helpers.CheckNodeHealth(
		healthCtx, o, c, cl.node[0], 3 /* maxAttempts */, 5*time.Second,
	) {
		o.Status(fmt.Sprintf(
			"node %s healthy after OOM cleanup", cl.node.NodeIDsString(),
		))
		return
	}

	o.Status(fmt.Sprintf(
		"node %d unresponsive after OOM, restarting via cockroach.sh",
		cl.node[0],
	))
	restartCtx, restartCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer restartCancel()
	if err := c.RunE(
		restartCtx, option.WithNodes(cl.node), "./cockroach.sh",
	); err != nil {
		o.Errorf("failed to restart node %d: %v", cl.node[0], err)
		return
	}

	if !helpers.CheckNodeHealth(
		ctx, o, c, cl.node[0], 5 /* maxAttempts */, 10*time.Second,
	) {
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
		o.Status(fmt.Sprintf(
			"heap profile %d: failed to get admin UI addr: %v", profileNum, err,
		))
		return
	}

	url := fmt.Sprintf("https://%s/debug/pprof/heap", adminAddrs[0])
	resp, err := client.Get(ctx, url)
	if err != nil {
		o.Status(fmt.Sprintf(
			"heap profile %d: HTTP request failed: %v", profileNum, err,
		))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		o.Status(fmt.Sprintf(
			"heap profile %d: HTTP %d", profileNum, resp.StatusCode,
		))
		return
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		o.Status(fmt.Sprintf(
			"heap profile %d: failed to read response: %v", profileNum, err,
		))
		return
	}

	profilePath := filepath.Join(
		filepath.Dir(o.L().File.Name()),
		fmt.Sprintf("heap_%d.pb.gz", profileNum),
	)
	if err := os.WriteFile(profilePath, buf.Bytes(), 0644); err != nil {
		o.Status(fmt.Sprintf(
			"heap profile %d: failed to write to %s: %v", profileNum, profilePath, err,
		))
		return
	}

	o.Status(fmt.Sprintf(
		"heap profile %d: saved %d bytes to %s", profileNum, buf.Len(), profilePath,
	))
}

// monitorOOMPressure watches the cluster while a node is under memory
// pressure from SQL workload. It checks node health, captures heap
// profiles, and logs memory status at regular intervals.
func monitorOOMPressure(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	targetNode option.NodeListOption,
	peerNodeID int,
	client *roachtestutil.RoachtestHTTPClient,
	cancelPressure context.CancelFunc,
) time.Duration {
	start := timeutil.Now()
	deadline := start.Add(oomMonitorDuration)
	monitorCtx, monitorCancel := context.WithDeadline(ctx, deadline)
	defer monitorCancel()

	ticker := time.NewTicker(oomCheckInterval)
	defer ticker.Stop()

	elapsed := func() time.Duration { return timeutil.Since(start) }

	peerDB, err := c.ConnE(monitorCtx, o.L(), peerNodeID)
	if err != nil {
		o.Errorf("failed to connect to peer %d for monitoring: %v", peerNodeID, err)
		return elapsed()
	}
	defer peerDB.Close()

	profileNum := 1
	runCheck := func(tick int) bool {
		if !helpers.CheckNodeHealth(
			monitorCtx, o, c, targetNode[0], 2 /* maxAttempts */, 3*time.Second,
		) {
			o.Status(fmt.Sprintf(
				"check %d: target node %d went down under memory pressure (expected)",
				tick, targetNode[0],
			))
			cancelPressure()
			return true
		}

		if !helpers.CheckNodeHealth(
			monitorCtx, o, c, peerNodeID, 3 /* maxAttempts */, 5*time.Second,
		) {
			o.Errorf(
				"check %d: peer node %d unresponsive — aborting monitor",
				tick, peerNodeID,
			)
			cancelPressure()
			return true
		}

		captureHeapProfile(monitorCtx, o, c, targetNode, client, profileNum)
		profileNum++

		result, err := c.RunWithDetailsSingleNode(
			monitorCtx, o.L(), option.WithNodes(targetNode),
			"free -m | head -2",
		)
		if err == nil {
			o.Status(fmt.Sprintf("check %d: memory:\n%s", tick, result.Stdout))
		}

		if err := peerDB.PingContext(monitorCtx); err != nil {
			// Reconnect if the persistent connection has gone stale.
			peerDB.Close()
			newDB, connErr := c.ConnE(monitorCtx, o.L(), peerNodeID)
			if connErr != nil {
				o.Errorf(
					"check %d: failed to reconnect to peer %d: %v",
					tick, peerNodeID, connErr,
				)
				return false
			}
			peerDB = newDB
		}

		var unavail int
		if err := peerDB.QueryRowContext(monitorCtx,
			"SELECT COALESCE(sum(unavailable_ranges), 0) FROM system.replication_stats",
		).Scan(&unavail); err != nil {
			o.Errorf(
				"check %d: failed to query unavailable ranges: %v", tick, err,
			)
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

	var peerNodeID int
	for _, n := range liveNodes {
		if n != targetNode[0] {
			peerNodeID = n
			break
		}
	}

	// Raise the cluster-wide workmem so operators hold more data in
	// memory before spilling to disk. This is reset in cleanup. The
	// defer below is a safety net: if we panic between setting the
	// cluster setting and assigning the cleanup struct, the setting
	// would otherwise remain elevated permanently.
	workmemRaised := false
	defer func() {
		if workmemRaised && cleanup == nil {
			if resetDB, err := c.ConnE(ctx, o.L(), peerNodeID); err == nil {
				_, _ = resetDB.ExecContext(ctx,
					"RESET CLUSTER SETTING sql.distsql.temp_storage.workmem",
				)
				resetDB.Close()
			}
		}
	}()

	db, err := c.ConnE(ctx, o.L(), peerNodeID)
	if err != nil {
		o.Fatalf("failed to connect to peer %d for workmem setup: %v",
			peerNodeID, err,
		)
	}
	o.Status(fmt.Sprintf(
		"raising sql.distsql.temp_storage.workmem to %s", oomWorkmemSetting,
	))
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"SET CLUSTER SETTING sql.distsql.temp_storage.workmem = '%s'",
		oomWorkmemSetting,
	)); err != nil {
		db.Close()
		o.Fatalf("failed to raise workmem cluster setting: %v", err)
	}
	db.Close()
	workmemRaised = true

	client := roachtestutil.DefaultHTTPClient(
		c, o.L(), roachtestutil.HTTPTimeout(30*time.Second),
	)

	captureHeapProfile(ctx, o, c, targetNode, client, 0 /* profileNum */)

	o.Status(fmt.Sprintf(
		"starting %d pressure workers on node %d",
		oomPressureWorkers, targetNode[0],
	))
	cancelPressure, pressureDone := startPressure(
		ctx, o, c, targetNode[0],
	)

	cleanup = &cleanupOOM{
		node:           targetNode,
		peerNodeID:     peerNodeID,
		cancelPressure: cancelPressure,
		pressureDone:   pressureDone,
	}

	elapsed := monitorOOMPressure(
		ctx, o, c, targetNode, peerNodeID, client, cancelPressure,
	)

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
