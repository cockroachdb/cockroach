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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

// registerDecommissionBenchSpec creates a test using the specified configuration.
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

type ticker struct {
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
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))
	for i := 1; i <= tc.nodes; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=node%d", i))
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	t.Status(fmt.Sprintf("initializing cluster with %d warehouses", tc.warehouses))
	cmd := fmt.Sprintf(`./cockroach workload fixtures import tpcc --warehouses=%d`, tc.warehouses)
	c.Run(ctx, c.Node(pinnedNode), cmd)

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

	var m *errgroup.Group
	m, ctx = errgroup.WithContext(ctx)
	if tc.load {
		m.Go(
			func() error {
				// Run workload effectively indefinitely, to be later killed once
				// decommission has completed.
				return c.RunE(ctx, c.Node(workloadNode),
					fmt.Sprintf("./workload run tpcc --warehouses=%d --duration=%s --tolerate-errors {pgurl:1-%d}", testTimeout, tc.warehouses, tc.nodes),
				)
			},
		)
	}

	tick, perfBuf := initBulkJobPerfArtifacts(t.Name(), defaultTimeout)
	var startTime, endTime time.Time
	recorder := &ticker{pre: func() {
		startTime = timeutil.Now()
		tick()
	}, post: func() {
		endTime = timeutil.Now()
		tick()
	}}

	m.Go(func() error {
		h := newDecommTestHelper(t, c)
		h.blockFromRandNode(workloadNode)

		if err := runSingleDecommission(ctx, h, pinnedNode, tc.whileDown, false /* reuse */, recorder, nil); err != nil {
			return err
		}

		return c.RunE(ctx, c.Node(workloadNode), "killall workload")
	})

	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}

	elapsedTime := endTime.Sub(startTime)
	t.Status(fmt.Sprintf("decommissioning node with %d warehouses completed in %s", tc.warehouses, elapsedTime))

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
	stopFirst, reuse bool,
	decommTicker, upreplicateTicker *ticker,
) error {
	target := h.getRandNodeOtherThan(pinnedNode)

	// Ensure all ranges (all warehouses) are present on the node, for uniformity.
	h.t.Status(fmt.Sprintf("ensuring all default ranges present on %d", target))
	if err := h.defaultRangesOnNode(ctx, target, pinnedNode); err != nil {
		return err
	}
	if err := h.waitUpReplicated(ctx, target, target); err != nil {
		return err
	}

	if stopFirst {
		h.t.Status(fmt.Sprintf("gracefully stopping node %d", target))
		if err := h.stop(ctx, target); err != nil {
			return err
		}
	}

	h.t.Status(fmt.Sprintf("decommissioning node %d", target))
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

	if reuse {
		if !stopFirst {
			h.t.Status(fmt.Sprintf("gracefully stopping node %d", target))
			if err := h.stop(ctx, target); err != nil {
				return err
			}
		}

		// TODO(sarkesian): wipe the node and re-add to cluster
	}

	return nil
}
