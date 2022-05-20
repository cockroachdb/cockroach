// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	defaultTimeout = 1 * time.Hour
)

type decommissionBenchSpec struct {
	nodes            int
	cpus             int
	warehouses       int
	load             bool
	admissionControl bool
	whileDown        bool
	snapshotRate     int
}

// registerDecommissionBench defines all decommission benchmark configurations
// and adds them to the roachtest registry.
func registerDecommissionBench(r registry.Registry) {
	for _, benchSpec := range []decommissionBenchSpec{
		{
			nodes:            4,
			cpus:             4,
			warehouses:       100,
			load:             true,
			admissionControl: true,
		},
		{
			nodes:            4,
			cpus:             16,
			warehouses:       1000,
			load:             true,
			admissionControl: true,
		},
		{
			nodes:            4,
			cpus:             16,
			warehouses:       1000,
			load:             true,
			admissionControl: true,
			whileDown:        true,
		},
		{
			nodes:            4,
			cpus:             16,
			warehouses:       1000,
			load:             true,
			admissionControl: false,
		},
		{
			nodes:            8,
			cpus:             16,
			warehouses:       3000,
			load:             true,
			admissionControl: true,
		},
	} {
		registerDecommissionBenchSpec(r, benchSpec)
	}
}

// registerDecommissionBenchSpec adds a test using the specified configuration to the registry.
func registerDecommissionBenchSpec(r registry.Registry, benchSpec decommissionBenchSpec) {
	timeout := defaultTimeout
	extraNameParts := []string{""}

	if benchSpec.snapshotRate != 0 {
		extraNameParts = append(extraNameParts, fmt.Sprintf("snapshotRate=%dmb", benchSpec.snapshotRate))
	}

	if benchSpec.whileDown {
		extraNameParts = append(extraNameParts, "while-down")
	}

	if !benchSpec.load {
		extraNameParts = append(extraNameParts, "no-load")
	}

	if !benchSpec.admissionControl {
		extraNameParts = append(extraNameParts, "no-admission")
	}

	extraName := strings.Join(extraNameParts, "/")

	r.Add(registry.TestSpec{
		Name: fmt.Sprintf("decommissionBench/nodes=%d/cpu=%d/warehouses=%d%s",
			benchSpec.nodes, benchSpec.cpus, benchSpec.warehouses, extraName),
		Owner:             registry.OwnerKV,
		Cluster:           r.MakeClusterSpec(benchSpec.nodes+1, spec.CPU(benchSpec.cpus)),
		Timeout:           timeout,
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDecommissionBench(ctx, t, c, benchSpec, timeout)
		},
	})
}

// decommBenchTicker is a simple struct for encapsulating function
// closures to be called before and after an operation to be benchmarked.
type decommBenchTicker struct {
	pre  func()
	post func()
}

// runDecommissionBench initializes a cluster with TPCC and attempts to
// benchmark the decommissioning of a single node picked at random. The cluster
// may or may not be running under load.
func runDecommissionBench(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	benchSpec decommissionBenchSpec,
	testTimeout time.Duration,
) {
	// node1 is kept pinned (i.e. not decommissioned/restarted), and is the node
	// through which we run decommissions. The last node is used for the workload.
	pinnedNode := 1
	workloadNode := benchSpec.nodes + 1
	crdbNodes := c.Range(pinnedNode, benchSpec.nodes)
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))
	for i := 1; i <= benchSpec.nodes; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--attrs=node%d", i))
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	rampDuration := 3 * time.Minute
	rampStarted := make(chan struct{}, 1)
	importCmd := fmt.Sprintf(`./cockroach workload fixtures import tpcc --warehouses=%d`,
		benchSpec.warehouses)
	workloadCmd := fmt.Sprintf("./workload run tpcc --warehouses=%d --duration=%s "+
		"--histograms=%s/stats.json --ramp=%s --tolerate-errors {pgurl:1-%d}", benchSpec.warehouses,
		testTimeout, t.PerfArtifactsDir(), rampDuration, benchSpec.nodes)
	t.Status(fmt.Sprintf("initializing cluster with %d warehouses", benchSpec.warehouses))
	c.Run(ctx, c.Node(pinnedNode), importCmd)

	SetAdmissionControl(ctx, t, c, benchSpec.admissionControl)
	{
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		if benchSpec.snapshotRate != 0 {
			for _, stmt := range []string{
				fmt.Sprintf(`SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='%dMiB'`,
					benchSpec.snapshotRate),
				fmt.Sprintf(`SET CLUSTER SETTING kv.snapshot_recovery.max_rate='%dMiB'`,
					benchSpec.snapshotRate),
			} {
				t.Status(stmt)
				_, err := db.ExecContext(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
				t.L().Printf("run: %s\n", stmt)
			}
		}

		// Wait for initial up-replication.
		err := WaitFor3XReplication(ctx, t, db)
		require.NoError(t, err)
	}

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := c.NewMonitor(workloadCtx, crdbNodes)

	if benchSpec.load {
		m.Go(
			func(ctx context.Context) error {
				rampStarted <- struct{}{}

				// Run workload effectively indefinitely, to be later killed by context
				// cancellation once decommission has completed.
				err := c.RunE(ctx, c.Node(workloadNode), workloadCmd)
				if errors.Is(ctx.Err(), context.Canceled) {
					// Workload intentionally cancelled via context, so don't return error.
					return nil
				}
				if err != nil {
					t.L().Printf("workload error: %s", err)
				}
				return err
			},
		)
	}

	// Utilize the "bulk job" form of recording performance which, rather than
	// record the workload operation rate with the histograms recorded in a
	// per-second "tick", we will simply tick at the start of the decommission
	// and again at the completion. Roachperf will use the elapsed time between
	// these ticks to plot the duration of the decommission.
	tick, perfBuf := initBulkJobPerfArtifacts("decommission", defaultTimeout)
	recorder := &decommBenchTicker{pre: tick, post: tick}

	m.Go(func(ctx context.Context) error {
		defer workloadCancel()

		h := newDecommTestHelper(t, c)
		h.blockFromRandNode(workloadNode)

		// If we are running a workload, wait until it has started and completed its
		// ramp time before initiating a decommission.
		if benchSpec.load {
			<-rampStarted
			t.Status("Waiting for workload to ramp up...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rampDuration):
				// Workload ramp-up complete.
			}
		}

		m.ExpectDeath()
		defer m.ResetDeaths()
		return runSingleDecommission(ctx, h, pinnedNode, benchSpec.whileDown, false, /* reuse */
			recorder, nil /* upreplicateTicker */)
	})

	if err := m.WaitE(); err != nil {
		t.Fatal(err)
	}

	// Store the perf artifacts on the pinned node so that the test
	// runner copies it into an appropriate directory path.
	dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, c.Node(pinnedNode), "mkdir -p "+filepath.Dir(dest)); err != nil {
		t.L().Errorf("failed to create perf dir: %+v", err)
	}
	if err := c.PutString(ctx, perfBuf.String(), dest, 0755, c.Node(pinnedNode)); err != nil {
		t.L().Errorf("failed to upload perf artifacts to node: %s", err.Error())
	}

	// Get the workload perf artifacts and move them to the pinned node, so that
	// they can be used to display the workload operation rates during decommission.
	if benchSpec.load {
		workloadStatsSrc := filepath.Join(t.PerfArtifactsDir(), "stats.json")
		localWorkloadStatsPath := filepath.Join(t.ArtifactsDir(), "workload_stats.json")
		workloadStatsDest := filepath.Join(t.PerfArtifactsDir(), "workload_stats.json")
		if err := c.Get(ctx, t.L(), workloadStatsSrc, localWorkloadStatsPath, c.Node(workloadNode)); err != nil {
			t.L().Errorf("failed to download workload perf artifacts from workload node: %s", err.Error())
		}

		if err := c.PutE(ctx, t.L(), localWorkloadStatsPath, workloadStatsDest,
			c.Node(pinnedNode)); err != nil {
			t.L().Errorf("failed to upload workload perf artifacts to node: %s", err.Error())
		}

		if err := c.RunE(ctx, c.Node(pinnedNode),
			fmt.Sprintf("cat %s >> %s", workloadStatsDest, dest)); err != nil {
			t.L().Errorf("failed to concatenate workload perf artifacts with "+
				"decommission perf artifacts: %s", err.Error())
		}

		if err := c.RunE(ctx, c.Node(pinnedNode),
			fmt.Sprintf("rm %s", workloadStatsDest)); err != nil {
			t.L().Errorf("failed to cleanup workload perf artifacts: %s", err.Error())
		}
	}
}

// runSingleDecommission picks a random node and attempts to decommission that
// node from the pinned (i.e. first) node, validating and recording the duration.
// If the reuse flag is passed in, the node will be wiped and re-added to the
// cluster, with the upreplication duration tracked by upreplicateTicker.
func runSingleDecommission(
	ctx context.Context,
	h *decommTestHelper,
	pinnedNode int,
	stopFirst, reuse bool,
	decommTicker, upreplicateTicker *decommBenchTicker,
) error {
	target := h.getRandNodeOtherThan(pinnedNode)
	h.t.Status(fmt.Sprintf("targeting node%d (n%d) for decommission", target, target))

	// TODO(sarkesian): Consider adding a future test for decommissions that get
	// stuck with replicas in purgatory, by pinning them to a node.

	// Gather metadata for logging purposes and wait for balance.
	var bytesUsed, rangeCount, totalRanges int64
	{
		dbNode := h.c.Conn(ctx, h.t.L(), target)
		defer dbNode.Close()

		if err := dbNode.QueryRow(
			`SELECT count(*) FROM crdb_internal.ranges_no_leases`,
		).Scan(&totalRanges); err != nil {
			return err
		}

		h.t.Status("waiting for cluster balance")
		if err := waitForRebalance(ctx, h.t.L(), dbNode, float64(totalRanges)/3.0); err != nil {
			return err
		}

		if err := dbNode.QueryRow(
			`SELECT sum(range_count), sum(used) FROM crdb_internal.kv_store_status where node_id = $1`,
			target).Scan(&rangeCount, &bytesUsed); err != nil {
			return err
		}
	}

	if stopFirst {
		h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
		if err := h.stop(ctx, target); err != nil {
			return err
		}
	}

	h.t.Status(fmt.Sprintf("decommissioning node%d (n%d)", target, target))
	tBegin := timeutil.Now()
	decommTicker.pre()
	if _, err := h.decommission(ctx, h.c.Node(target), pinnedNode, "--wait=all"); err != nil {
		return err
	}
	if err := h.waitReplicatedAwayFrom(ctx, target, pinnedNode); err != nil {
		return err
	}
	if err := h.checkDecommissioned(ctx, target, pinnedNode); err != nil {
		return err
	}
	decommTicker.post()
	elapsed := timeutil.Since(tBegin)

	h.t.Status(fmt.Sprintf("decommissioned node%d (n%d) in %s (ranges: %d, size: %s)",
		target, target, elapsed, rangeCount, humanizeutil.IBytes(bytesUsed)))

	if reuse {
		if !stopFirst {
			h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
			if err := h.stop(ctx, target); err != nil {
				return err
			}
		}

		// TODO(sarkesian): Wipe the node and re-add to cluster.
	}

	return nil
}
