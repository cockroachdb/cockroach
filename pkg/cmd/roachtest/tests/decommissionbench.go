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
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
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
	snapshotRate     int
	duration         time.Duration

	// When true, the test will attempt to stop the node prior to decommission.
	whileDown bool

	// An override for the default timeout, if needed.
	timeout time.Duration

	skip string
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
			cpus:             4,
			warehouses:       100,
			load:             true,
			admissionControl: true,
			duration:         30 * time.Minute,
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
			duration:         1 * time.Hour,
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
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 3 * time.Hour,
		},
	} {
		registerDecommissionBenchSpec(r, benchSpec)
	}
}

// registerDecommissionBenchSpec adds a test using the specified configuration
// to the registry.
func registerDecommissionBenchSpec(r registry.Registry, benchSpec decommissionBenchSpec) {
	timeout := defaultTimeout
	if benchSpec.timeout != time.Duration(0) {
		timeout = benchSpec.timeout
	}
	extraNameParts := []string{""}

	if benchSpec.snapshotRate != 0 {
		extraNameParts = append(extraNameParts,
			fmt.Sprintf("snapshotRate=%dmb", benchSpec.snapshotRate))
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

	if benchSpec.duration > 0 {
		timeout = benchSpec.duration * 3
		extraNameParts = append(extraNameParts, fmt.Sprintf("duration=%s", benchSpec.duration))
	}

	extraName := strings.Join(extraNameParts, "/")

	// TODO(sarkesian): add a configuration that tests decommission of a node
	// while upreplication to a recently added node is still ongoing. This
	// will require the test to allocate additional Roachprod nodes up front,
	// and start them just prior to initiating the decommission.

	r.Add(registry.TestSpec{
		Name: fmt.Sprintf("decommissionBench/nodes=%d/cpu=%d/warehouses=%d%s",
			benchSpec.nodes, benchSpec.cpus, benchSpec.warehouses, extraName),
		Owner:             registry.OwnerKV,
		Cluster:           r.MakeClusterSpec(benchSpec.nodes+1, spec.CPU(benchSpec.cpus)),
		Timeout:           timeout,
		NonReleaseBlocker: true,
		Skip:              benchSpec.skip,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if benchSpec.duration > 0 {
				runDecommissionBenchLong(ctx, t, c, benchSpec, timeout)
			} else {
				runDecommissionBench(ctx, t, c, benchSpec, timeout)
			}
		},
	})
}

// decommBenchTicker is a simple struct for encapsulating function
// closures to be called before and after an operation to be benchmarked.
type decommBenchTicker struct {
	pre  func()
	post func()
}

// setupDecommissionBench performs the initial cluster setup needed prior to
// running a workload and benchmarking decommissioning.
func setupDecommissionBench(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	benchSpec decommissionBenchSpec,
	workloadNode, pinnedNode int,
	importCmd string,
) {
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))
	for i := 1; i <= benchSpec.nodes; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--attrs=node%d", i))
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

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
}

// uploadPerfArtifacts puts the contents of perfBuf onto the pinned node so
// that the results will be picked up by Roachperf. If there is a workload
// running, it will also get the perf artifacts from the workload node and
// concatenate them with the decommission perf artifacts so that the effects
// of the decommission on foreground traffic can also be visualized.
func uploadPerfArtifacts(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	benchSpec decommissionBenchSpec,
	pinnedNode, workloadNode int,
	perfBuf *bytes.Buffer,
) {
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
		if err := c.Get(
			ctx, t.L(), workloadStatsSrc, localWorkloadStatsPath, c.Node(workloadNode),
		); err != nil {
			t.L().Errorf(
				"failed to download workload perf artifacts from workload node: %s", err.Error(),
			)
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

		if err := c.RunE(
			ctx, c.Node(pinnedNode), fmt.Sprintf("rm %s", workloadStatsDest),
		); err != nil {
			t.L().Errorf("failed to cleanup workload perf artifacts: %s", err.Error())
		}
	}
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

	maxRate := tpccMaxRate(benchSpec.warehouses)
	rampDuration := 3 * time.Minute
	rampStarted := make(chan struct{}, 1)
	importCmd := fmt.Sprintf(
		`./cockroach workload fixtures import tpcc --warehouses=%d`, benchSpec.warehouses,
	)
	workloadCmd := fmt.Sprintf("./workload run tpcc --warehouses=%d --max-rate=%d --duration=%s "+
		"--histograms=%s/stats.json --ramp=%s --tolerate-errors {pgurl:1-%d}", maxRate, benchSpec.warehouses,
		testTimeout, t.PerfArtifactsDir(), rampDuration, benchSpec.nodes)
	setupDecommissionBench(ctx, t, c, benchSpec, workloadNode, pinnedNode, importCmd)

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
	tick, perfBuf := initBulkJobPerfArtifacts("decommission", testTimeout)
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
		return runSingleDecommission(ctx, h, pinnedNode, benchSpec.whileDown,
			false /* reuse */, recorder, nil /* upreplicateTicker */)
	})

	if err := m.WaitE(); err != nil {
		t.Fatal(err)
	}

	uploadPerfArtifacts(ctx, t, c, benchSpec, pinnedNode, workloadNode, perfBuf)
}

// runDecommissionBenchLong initializes a cluster with TPCC and attempts to
// benchmark the decommissioning of nodes picked at random before subsequently
// wiping them and re-adding them to the cluster to continually execute the
// decommissioning process over the runtime of the test. The cluster may or may
// not be running under load.
func runDecommissionBenchLong(
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

	maxRate := tpccMaxRate(benchSpec.warehouses)
	rampDuration := 3 * time.Minute
	rampStarted := make(chan struct{}, 1)
	importCmd := fmt.Sprintf(
		`./cockroach workload fixtures import tpcc --warehouses=%d`, benchSpec.warehouses,
	)
	workloadCmd := fmt.Sprintf("./workload run tpcc --warehouses=%d --max-rate=%d --duration=%s "+
		"--histograms=%s/stats.json --ramp=%s --tolerate-errors {pgurl:1-%d}", maxRate, benchSpec.warehouses,
		testTimeout, t.PerfArtifactsDir(), rampDuration, benchSpec.nodes)

	setupDecommissionBench(ctx, t, c, benchSpec, workloadNode, pinnedNode, importCmd)

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := c.NewMonitor(workloadCtx, crdbNodes)

	if benchSpec.load {
		m.Go(
			func(ctx context.Context) error {
				rampStarted <- struct{}{}

				// Run workload indefinitely, to be later killed by context
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

	// Create a histogram registry for recording multiple decommission metrics,
	// following the "bulk job" form of measuring performance.
	// See runDecommissionBench for more explanation.
	reg := histogram.NewRegistry(
		defaultTimeout,
		histogram.MockWorkloadName,
	)

	perfBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(perfBuf)

	// Initialize operation-specific metric ticks.
	opNames := []string{"decommission", "decommission:upreplicate"}
	opTickByName := make(map[string]func())

	makeOpTick := func(reg *histogram.Registry, jsonEnc *json.Encoder, opName string) func() {
		return func() {
			reg.Tick(func(tick histogram.Tick) {
				if tick.Name == opName {
					_ = jsonEnc.Encode(tick.Snapshot())
				}
			})
		}
	}

	for _, opName := range opNames {
		reg.GetHandle().Get(opName)
		opTickByName[opName] = makeOpTick(reg, jsonEnc, opName)
	}

	decommRecorder := &decommBenchTicker{
		pre:  opTickByName["decommission"],
		post: opTickByName["decommission"],
	}
	upreplicateRecorder := &decommBenchTicker{
		pre:  opTickByName["decommission:upreplicate"],
		post: opTickByName["decommission:upreplicate"],
	}

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

		for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= benchSpec.duration; {
			m.ExpectDeath()
			err := runSingleDecommission(ctx, h, pinnedNode, benchSpec.whileDown,
				true /* reuse */, decommRecorder, upreplicateRecorder)
			m.ResetDeaths()
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err := m.WaitE(); err != nil {
		t.Fatal(err)
	}

	uploadPerfArtifacts(ctx, t, c, benchSpec, pinnedNode, workloadNode, perfBuf)
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
	targetLogicalNodeID, err := h.getLogicalNodeID(ctx, target)
	if err != nil {
		return err
	}
	h.t.Status(fmt.Sprintf("targeting node%d (n%d) for decommission", target, targetLogicalNodeID))

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
		if err := waitForRebalance(
			ctx, h.t.L(), dbNode, float64(totalRanges)/3.0, 60, /* stableSeconds */
		); err != nil {
			return err
		}

		if err := dbNode.QueryRow(
			`SELECT sum(range_count), sum(used) FROM crdb_internal.kv_store_status where node_id = $1`,
			targetLogicalNodeID,
		).Scan(&rangeCount, &bytesUsed); err != nil {
			return err
		}
	}

	if stopFirst {
		h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
		h.stop(ctx, target)
	}

	h.t.Status(fmt.Sprintf("decommissioning node%d (n%d)", target, targetLogicalNodeID))
	targetLogicalNodeIDList := option.NodeListOption{targetLogicalNodeID}
	tBegin := timeutil.Now()
	decommTicker.pre()
	if _, err := h.decommission(
		ctx, targetLogicalNodeIDList, pinnedNode, "--wait=all",
	); err != nil {
		return err
	}
	if err := h.waitReplicatedAwayFrom(ctx, targetLogicalNodeID, pinnedNode); err != nil {
		return err
	}
	if err := h.checkDecommissioned(ctx, targetLogicalNodeID, pinnedNode); err != nil {
		return err
	}
	decommTicker.post()
	elapsed := timeutil.Since(tBegin)

	h.t.Status(fmt.Sprintf("decommissioned node%d (n%d) in %s (ranges: %d, size: %s)",
		target, targetLogicalNodeID, elapsed, rangeCount, humanizeutil.IBytes(bytesUsed)))

	if reuse {
		if !stopFirst {
			h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
			h.stop(ctx, target)
		}

		// Wipe the node and re-add to cluster with a new node ID.
		if err := h.c.RunE(ctx, h.c.Node(target), "rm -rf {store-dir}"); err != nil {
			return err
		}

		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(
			startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=node%d", target),
		)
		if err := h.c.StartE(
			ctx, h.t.L(), startOpts, install.MakeClusterSettings(), h.c.Node(target),
		); err != nil {
			return err
		}

		newLogicalNodeID, err := h.getLogicalNodeID(ctx, target)
		if err != nil {
			return err
		}

		upreplicateTicker.pre()
		h.t.Status("waiting for replica counts to balance across nodes")
		{
			dbNode := h.c.Conn(ctx, h.t.L(), pinnedNode)
			defer dbNode.Close()

			for {
				var membership string
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if err := dbNode.QueryRow(
						"SELECT membership FROM crdb_internal.gossip_liveness WHERE node_id = $1",
						newLogicalNodeID,
					).Scan(&membership); err != nil {
						return err
					}
				}

				if membership == "active" {
					break
				}
			}

			var totalRanges int64
			if err := dbNode.QueryRow(
				`SELECT count(*) FROM crdb_internal.ranges_no_leases`,
			).Scan(&totalRanges); err != nil {
				return err
			}

			if err := waitForRebalance(
				ctx, h.t.L(), dbNode, float64(totalRanges)/3.0, 60, /* stableSeconds */
			); err != nil {
				return err
			}
		}
		upreplicateTicker.post()
	}

	return nil
}
