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
	defaultTimeout        = 1 * time.Hour
	defaultSnapshotRateMb = 32
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
	for _, tc := range []decommissionBenchSpec{
		{
			nodes:            4,
			cpus:             4,
			warehouses:       100,
			load:             true,
			admissionControl: true,
		},
		{
			nodes:            4,
			cpus:             4,
			warehouses:       100,
			load:             true,
			admissionControl: true,
			whileDown:        true,
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
			admissionControl: false,
		},
		{
			nodes:            4,
			cpus:             16,
			warehouses:       1000,
			load:             false,
			admissionControl: false,
		},
	} {
		registerDecommissionBenchSpec(r, tc)
	}
}

// registerDecommissionBenchSpec adds a test using the specified configuration to the registry.
func registerDecommissionBenchSpec(r registry.Registry, tc decommissionBenchSpec) {
	timeout := defaultTimeout
	extraNameParts := []string{""}

	if tc.snapshotRate == 0 {
		tc.snapshotRate = defaultSnapshotRateMb
	} else {
		extraNameParts = append(extraNameParts, fmt.Sprintf("snapshotRate=%dmb", tc.snapshotRate))
	}

	if tc.whileDown {
		extraNameParts = append(extraNameParts, "while-down")
	}

	if !tc.load {
		extraNameParts = append(extraNameParts, "no-load")
	}

	if !tc.admissionControl {
		extraNameParts = append(extraNameParts, "no-admission")
	}

	extraName := strings.Join(extraNameParts, "/")

	r.Add(registry.TestSpec{
		Name: fmt.Sprintf("decommissionBench/nodes=%d/cpu=%d/warehouses=%d%s",
			tc.nodes, tc.cpus, tc.warehouses, extraName),
		Owner:             registry.OwnerKV,
		Cluster:           r.MakeClusterSpec(tc.nodes+1, spec.CPU(tc.cpus)),
		Timeout:           timeout,
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDecommissionBench(ctx, t, c, tc, timeout)
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
	tc decommissionBenchSpec,
	testTimeout time.Duration,
) {
	// node1 is kept pinned (i.e. not decommissioned/restarted), and is the node
	// through which we run decommissions. The last node is used for the workload.
	pinnedNode := 1
	workloadNode := tc.nodes + 1
	crdbNodes := c.Range(pinnedNode, tc.nodes)
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))
	for i := 1; i <= tc.nodes; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=node%d", i))
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	database := "tpcc"
	importCmd := fmt.Sprintf(`./cockroach workload fixtures import tpcc --warehouses=%d`, tc.warehouses)
	workloadCmd := fmt.Sprintf("./workload run tpcc --warehouses=%d --duration=%s --histograms=%s/stats.json --tolerate-errors {pgurl:1-%d}", tc.warehouses, testTimeout, t.PerfArtifactsDir(), tc.nodes)
	t.Status(fmt.Sprintf("initializing cluster with %d warehouses", tc.warehouses))
	c.Run(ctx, c.Node(pinnedNode), importCmd)

	SetAdmissionControl(ctx, t, c, tc.admissionControl)
	{
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		for _, stmt := range []string{
			fmt.Sprintf(`SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='%dMiB'`, tc.snapshotRate),
			fmt.Sprintf(`SET CLUSTER SETTING kv.snapshot_recovery.max_rate='%dMiB'`, tc.snapshotRate),
		} {
			t.Status(stmt)
			_, err := db.ExecContext(ctx, stmt)
			if err != nil {
				t.Fatal(err)
			}
			t.L().Printf("run: %s\n", stmt)
		}

		// Wait for initial up-replication.
		err := WaitFor3XReplication(ctx, t, db)
		require.NoError(t, err)
	}

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := c.NewMonitor(workloadCtx, crdbNodes)

	if tc.load {
		m.Go(
			func(ctx context.Context) error {
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

	tick, perfBuf := initBulkJobPerfArtifacts(t.Name(), defaultTimeout)
	recorder := &decommBenchTicker{pre: tick, post: tick}

	m.Go(func(ctx context.Context) error {
		defer workloadCancel()

		h := newDecommTestHelper(t, c)
		h.blockFromRandNode(workloadNode)

		m.ExpectDeath()
		defer m.ResetDeaths()
		return runSingleDecommission(ctx, h, pinnedNode, database, tc.whileDown, false /* reuse */, recorder, nil /* upreplicateTicker */)
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
}

// runSingleDecommission picks a random node and attempts to decommission that
// node from the pinned (i.e. first) node, validating and recording the duration.
// If the reuse flag is passed in, the node will be wiped and re-added to the
// cluster, with the upreplication duration tracked by upreplicateTicker.
func runSingleDecommission(
	ctx context.Context,
	h *decommTestHelper,
	pinnedNode int,
	database string,
	stopFirst, reuse bool,
	decommTicker, upreplicateTicker *decommBenchTicker,
) error {
	target := h.getRandNodeOtherThan(pinnedNode)
	h.t.Status(fmt.Sprintf("targeting node%d (n%d) for decommission", target, target))

	// Pin all default ranges (all warehouses) to the node, for uniformity.
	h.t.Status(fmt.Sprintf("ensuring all default ranges present on node%d", target))
	if err := h.pinRangesOnNode(ctx, target, pinnedNode); err != nil {
		return err
	}
	if err := h.waitUpReplicated(ctx, target, target, database); err != nil {
		return err
	}

	// Gather metadata for logging purposes.
	var bytesUsed, rangeCount int64
	{
		dbNode := h.c.Conn(ctx, h.t.L(), target)
		defer dbNode.Close()

		if err := dbNode.QueryRow(`SELECT sum(range_count), sum(used) FROM crdb_internal.kv_store_status where node_id = $1`, target).Scan(&rangeCount, &bytesUsed); err != nil {
			return err
		}
	}

	if stopFirst {
		h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
		if err := h.stop(ctx, target); err != nil {
			return err
		}
	}

	// Remove pinned ranges, so we can decommission cleanly.
	// TODO(sarkesian): Consider adding a future test for decommissions that get
	// stuck with replicas in purgatory.
	if err := h.unpinRangesOnNode(ctx, target, pinnedNode); err != nil {
		return err
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
