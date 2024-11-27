// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	defaultTimeout            = 2 * time.Hour
	envDecommissionNoSkipFlag = "ROACHTEST_DECOMMISSION_NOSKIP"
	envDecommissionGrafana    = "ROACHTEST_DECOMMISSION_GRAFANA"
	envDecommissionGrafanaURL = "ROACHTEST_DECOMMISSION_GRAFANA_URL"

	// Metrics for recording and sending to roachperf.
	decommissionMetric = "decommission"
	upreplicateMetric  = "decommission:upreplicate"
	estimatedMetric    = "decommission:estimated"
	bytesUsedMetric    = "targetBytesUsed"

	// Used to calculate estimated decommission time. Should remain in sync with
	// setting `kv.snapshot_rebalance.max_rate` in store_snapshot.go.
	defaultSnapshotRateMb = 32

	// Skip message for tests not meant to be run nightly.
	manualBenchmarkingOnly = "This config can be used to perform some manual " +
		"benchmarking and is not meant to be run on a nightly basis"
)

type decommissionBenchSpec struct {
	nodes        int
	warehouses   int
	noLoad       bool
	multistore   bool
	snapshotRate int
	duration     time.Duration

	// Whether the test cluster nodes are in multiple regions.
	multiregion bool

	// When true, the test will attempt to stop the node prior to decommission.
	whileDown bool

	// When true, will drain the SQL connections and leases from a node first.
	drainFirst bool

	// When true, the test will add a node to the cluster prior to decommission,
	// so that the upreplication will overlap with the decommission.
	whileUpreplicating bool

	// When true, attempts to simulate decommissioning a node with high read
	// amplification by slowing writes on the target in a write-heavy workload.
	slowWrites bool

	// An override for the default timeout, if needed.
	timeout time.Duration

	// An override for the decommission node to make it choose a predictable node
	// instead of a random node.
	decommissionNode int

	skip string
}

// registerDecommissionBench defines all decommission benchmark configurations
// and adds them to the roachtest registry.
func registerDecommissionBench(r registry.Registry) {
	for _, benchSpec := range []decommissionBenchSpec{
		// Basic benchmark configurations, to be run nightly.
		{
			nodes:      4,
			warehouses: 1000,
		},
		{
			nodes:      4,
			warehouses: 1000,
			duration:   1 * time.Hour,
		},
		{
			nodes:      4,
			warehouses: 1000,
			whileDown:  true,
		},
		{
			nodes:      8,
			warehouses: 3000,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 4 * time.Hour,
		},
		// Manually run benchmark configurations.
		{
			// Add a new node during decommission (no drain).
			nodes:              8,
			warehouses:         3000,
			whileUpreplicating: true,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 4 * time.Hour,
			skip:    manualBenchmarkingOnly,
		},
		{
			// Drain before decommission, without adding a new node.
			nodes:      8,
			warehouses: 3000,
			drainFirst: true,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 4 * time.Hour,
			skip:    manualBenchmarkingOnly,
		},
		{
			// Drain before decommission, and add a new node.
			nodes:              8,
			warehouses:         3000,
			whileUpreplicating: true,
			drainFirst:         true,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 4 * time.Hour,
			skip:    manualBenchmarkingOnly,
		},
		{
			nodes:      4,
			warehouses: 1000,
			drainFirst: true,
			skip:       manualBenchmarkingOnly,
		},
		{
			nodes:      4,
			warehouses: 1000,
			slowWrites: true,
			skip:       manualBenchmarkingOnly,
		},
		{
			nodes:      8,
			warehouses: 3000,
			slowWrites: true,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 4 * time.Hour,
			skip:    manualBenchmarkingOnly,
		},
		{
			nodes:      12,
			warehouses: 3000,
			multistore: true,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 3 * time.Hour,
			skip:    manualBenchmarkingOnly,
		},
		{
			// Test to compare 12 4-store nodes vs 48 single-store nodes
			nodes:      48,
			warehouses: 3000,
			// This test can take nearly an hour to import and achieve balance, so
			// we extend the timeout to let it complete.
			timeout: 3 * time.Hour,
			skip:    manualBenchmarkingOnly,
		},
		{
			// Multiregion decommission, and add a new node in the same region.
			nodes:              6,
			warehouses:         1000,
			whileUpreplicating: true,
			drainFirst:         true,
			multiregion:        true,
			decommissionNode:   2,
		},
		{
			// Multiregion decommission, and add a new node in a different region.
			nodes:              6,
			warehouses:         1000,
			whileUpreplicating: true,
			drainFirst:         true,
			multiregion:        true,
			decommissionNode:   3,
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
	addlNodeCount := 0
	specOptions := []spec.Option{spec.CPU(16)}

	if benchSpec.snapshotRate != 0 {
		extraNameParts = append(extraNameParts,
			fmt.Sprintf("snapshotRate=%dmb", benchSpec.snapshotRate))
	}

	if benchSpec.whileDown {
		extraNameParts = append(extraNameParts, "while-down")
	}

	if benchSpec.drainFirst {
		extraNameParts = append(extraNameParts, "drain-first")
	}

	if benchSpec.multistore {
		extraNameParts = append(extraNameParts, "multi-store")
		specOptions = append(specOptions, spec.SSD(4))
	}

	if benchSpec.whileUpreplicating {
		// Only add additional nodes if we aren't performing repeated decommissions.
		if benchSpec.duration == 0 {
			addlNodeCount = 1
		}
		extraNameParts = append(extraNameParts, "while-upreplicating")
	}

	if benchSpec.noLoad {
		extraNameParts = append(extraNameParts, "no-load")
	}

	if benchSpec.slowWrites {
		extraNameParts = append(extraNameParts, "hi-read-amp")
	}

	if benchSpec.decommissionNode != 0 {
		extraNameParts = append(extraNameParts,
			fmt.Sprintf("target=%d", benchSpec.decommissionNode))
	}

	if benchSpec.duration > 0 {
		timeout = benchSpec.duration * 3
		extraNameParts = append(extraNameParts, fmt.Sprintf("duration=%s", benchSpec.duration))
	}

	if benchSpec.multiregion {
		geoZones := []string{regionUsEast, regionUsWest, regionUsCentral}
		specOptions = append(specOptions, spec.GCEZones(strings.Join(geoZones, ",")))
		specOptions = append(specOptions, spec.Geo())
		extraNameParts = append(extraNameParts, "multi-region")
	}

	// Save some money and CPU quota by using a smaller workload CPU. Only
	// do this for cluster of size 3 or smaller to avoid regressions.
	specOptions = append(specOptions, spec.WorkloadNode())
	if benchSpec.nodes > 3 {
		spec.WorkloadNodeCPU(16)
	}

	// If run with ROACHTEST_DECOMMISSION_NOSKIP=1, roachtest will enable all specs.
	noSkipFlag := os.Getenv(envDecommissionNoSkipFlag)
	if noSkipFlag != "" {
		benchSpec.skip = ""
	}

	extraName := strings.Join(extraNameParts, "/")

	r.Add(registry.TestSpec{
		Name: fmt.Sprintf("decommissionBench/nodes=%d/warehouses=%d%s",
			benchSpec.nodes, benchSpec.warehouses, extraName),
		Owner:     registry.OwnerKV,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			benchSpec.nodes+addlNodeCount+1,
			specOptions...,
		),
		CompatibleClouds:    registry.OnlyGCE,
		Suites:              registry.Suites(registry.Nightly),
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Timeout:             timeout,
		NonReleaseBlocker:   true,
		Skip:                benchSpec.skip,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if benchSpec.duration > 0 {
				runDecommissionBenchLong(ctx, t, c, benchSpec, timeout)
			} else {
				runDecommissionBench(ctx, t, c, benchSpec, timeout)
			}
		},
	})
}

// fireAfter executes fn after the duration elapses. If the passed context, or
// tasker context, expires first, it will not be executed.
func fireAfter(ctx context.Context, t task.Tasker, duration time.Duration, fn func()) {
	t.Go(func(taskCtx context.Context, _ *logger.Logger) error {
		var fireTimer timeutil.Timer
		defer fireTimer.Stop()
		fireTimer.Reset(duration)
		select {
		case <-ctx.Done():
		case <-taskCtx.Done():
		case <-fireTimer.C:
			fireTimer.Read = true
			fn()
		}
		return nil
	})
}

// createDecommissionBenchPerfArtifacts initializes a histogram registry for
// measuring decommission performance metrics. Metrics initialized via opNames
// will be registered with the registry and are intended to be used as
// long-running metrics measured by the elapsed time between each "tick",
// rather than utilizing the values recorded in the histogram, and can be
// recorded in the perfBuf by utilizing the returned tickByName(name) function.
func createDecommissionBenchPerfArtifacts(
	t test.Test, c cluster.Cluster, opNames ...string,
) (
	reg *histogram.Registry,
	tickByName func(name string),
	perfBuf *bytes.Buffer,
	exporter exporter.Exporter,
) {

	exporter = roachtestutil.CreateWorkloadHistogramExporter(t, c)

	// Create a histogram registry for recording multiple decommission metrics,
	// following the "bulk job" form of measuring performance.
	// See runDecommissionBench for more explanation.
	reg = histogram.NewRegistryWithExporter(
		defaultTimeout,
		histogram.MockWorkloadName,
		exporter,
	)

	perfBuf = bytes.NewBuffer([]byte{})
	writer := io.Writer(perfBuf)
	exporter.Init(&writer)

	registeredOpNames := make(map[string]struct{})
	for _, opName := range opNames {
		registeredOpNames[opName] = struct{}{}
		reg.GetHandle().Get(opName)
	}

	tickByName = func(name string) {
		reg.Tick(func(tick histogram.Tick) {
			if _, ok := registeredOpNames[name]; ok && tick.Name == name {
				_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
			}
		})
	}

	return reg, tickByName, perfBuf, exporter
}

// setupDecommissionBench performs the initial cluster setup needed prior to
// running a workload and benchmarking decommissioning.
func setupDecommissionBench(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	benchSpec decommissionBenchSpec,
	pinnedNode int,
	importCmd string,
) {
	for i := 1; i <= benchSpec.nodes; i++ {
		// Don't start a scheduled backup as this roachtest reports to roachperf.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--attrs=node%d", i),
			"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}
	{
		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		// Note that we are waiting for 3 replicas only. We can't assume 5 replicas
		// here because 5 only applies to system ranges so we will never reach this
		// number globally. We also don't know if all upreplication succeeded, but
		// at least we should have 2 voter replicas running by the time this method
		// succeeds and that should be enough to make progress. Without this check
		// import can saturate snapshots and leave underreplicated system ranges
		// struggling.
		// See GH issue #101532 for longer term solution.
		if err := roachtestutil.WaitForReplication(ctx, t.L(), db, 3, roachtestutil.AtLeastReplicationFactor); err != nil {
			t.Fatal(err)
		}

		t.Status(fmt.Sprintf("initializing cluster with %d warehouses", benchSpec.warehouses))
		// Add the connection string here as the port is not decided until c.Start() is called.

		importCmd = fmt.Sprintf("%s {pgurl:1}", importCmd)
		c.Run(ctx, option.WithNodes(c.Node(pinnedNode)), importCmd)

		if benchSpec.snapshotRate != 0 {
			for _, stmt := range []string{
				fmt.Sprintf(`SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='%dMiB'`,
					benchSpec.snapshotRate),
			} {
				t.Status(stmt)
				_, err := db.ExecContext(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		// Disable load-based splitting/rebalancing when attempting to simulate high
		// read amplification, so replicas on a store with slow writes will not move.
		if benchSpec.slowWrites {
			for _, stmt := range []string{
				`SET CLUSTER SETTING kv.range_split.by_load_enabled=false`,
				`SET CLUSTER SETTING kv.allocator.load_based_rebalancing=0`,
			} {
				t.Status(stmt)
				_, err := db.ExecContext(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		// Wait for initial up-replication.
		err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
		require.NoError(t, err)
	}
}

// trackBytesUsed repeatedly checks the number of bytes used by stores on the
// target node and records them into the bytesUsedMetric histogram.
func trackBytesUsed(
	ctx context.Context,
	db *gosql.DB,
	targetNodeAtomic *uint32,
	hists *histogram.Histograms,
	tickByName func(name string),
) error {
	const statsInterval = 3 * time.Second
	var statsTimer timeutil.Timer
	defer statsTimer.Stop()
	for {
		statsTimer.Reset(statsInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-statsTimer.C:
			statsTimer.Read = true
			var bytesUsed int64

			// If we have a target node, read the bytes used and record them.
			if logicalNodeID := atomic.LoadUint32(targetNodeAtomic); logicalNodeID > 0 {
				if err := db.QueryRow(
					`SELECT ifnull(sum(used), 0) FROM crdb_internal.kv_store_status WHERE node_id=$1`,
					logicalNodeID,
				).Scan(&bytesUsed); err != nil {
					return err
				}

				hists.Get(bytesUsedMetric).RecordValue(bytesUsed)
				tickByName(bytesUsedMetric)
			}
		}
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

	err := roachtestutil.UploadPerfStats(ctx, t, c, perfBuf, c.Node(pinnedNode), "")
	if err != nil {
		t.L().Errorf("error creating perf stats file: %s", err)
		return
	}
	destFileName := roachtestutil.GetBenchmarkMetricsFileName(t)
	dest := filepath.Join(t.PerfArtifactsDir(), destFileName)

	// Get the workload perf artifacts and move them to the pinned node, so that
	// they can be used to display the workload operation rates during decommission.
	if !benchSpec.noLoad {
		workloadStatsSrc := filepath.Join(t.PerfArtifactsDir(), destFileName)
		localWorkloadStatsPath := filepath.Join(t.ArtifactsDir(), "workload_"+destFileName)
		workloadStatsDest := filepath.Join(t.PerfArtifactsDir(), "workload_"+destFileName)
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

		if err := c.RunE(ctx, option.WithNodes(c.Node(pinnedNode)),
			fmt.Sprintf("cat %s >> %s", workloadStatsDest, dest)); err != nil {
			t.L().Errorf("failed to concatenate workload perf artifacts with "+
				"decommission perf artifacts: %s", err.Error())
		}

		if err := c.RunE(
			ctx, option.WithNodes(c.Node(pinnedNode)), fmt.Sprintf("rm %s", workloadStatsDest),
		); err != nil {
			t.L().Errorf("failed to cleanup workload perf artifacts: %s", err.Error())
		}
	}
}

// setupGrafana initializes Prometheus & Grafana only if environment variables
// ROACHTEST_DECOMMISSION_GRAFANA or ROACHTEST_DECOMMISSION_GRAFANA_URL are set.
// A URL set in ROACHTEST_DECOMMISSION_GRAFANA_URL will be used to initialize
// the Grafana dashboard.
func setupGrafana(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	crdbNodes option.NodeListOption,
	prometheusNode int,
) (cleanupFunc func()) {
	grafana := os.Getenv(envDecommissionGrafana)
	grafanaURL := os.Getenv(envDecommissionGrafanaURL)
	if grafana == "" && grafanaURL == "" {
		return func() {
			// noop
		}
	}

	cfg := &prometheus.Config{}
	cfg.Grafana.Enabled = true
	cfg = cfg.
		WithCluster(crdbNodes.InstallNodes()).
		WithNodeExporter(crdbNodes.InstallNodes()).
		WithPrometheusNode(c.Node(prometheusNode).InstallNodes()[0])

	if grafanaURL != "" {
		cfg = cfg.WithGrafanaDashboard(grafanaURL)
	}

	err := c.StartGrafana(ctx, t.L(), cfg)
	require.NoError(t, err)

	cleanupFunc = func() {
		if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
		}
	}
	return cleanupFunc
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
	workloadNode := c.Spec().NodeCount
	crdbNodes := c.Range(pinnedNode, benchSpec.nodes)
	t.L().Printf("nodes %d - %d are crdb nodes", crdbNodes[0], crdbNodes[len(crdbNodes)-1])

	// If we have additional nodes to start (so we are upreplicating during
	// decommission), they will be part of addlNodes.
	var addlNodes option.NodeListOption
	if c.Spec().NodeCount > benchSpec.nodes+1 {
		addlNodes = c.Range(benchSpec.nodes+1, c.Spec().NodeCount-1)
		t.L().Printf("nodes %d - %d are addl nodes", addlNodes[0], addlNodes[len(addlNodes)-1])
	}

	t.L().Printf("node %d is the workload node", workloadNode)

	maxRate := tpccMaxRate(benchSpec.warehouses)
	rampDuration := 3 * time.Minute
	rampStarted := make(chan struct{})
	importCmd := fmt.Sprintf(
		`./cockroach workload fixtures import tpcc --warehouses=%d`,
		benchSpec.warehouses,
	)
	workloadCmd := fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --max-rate=%d --duration=%s "+
		"%s --ramp=%s --tolerate-errors {pgurl:1-%d}", maxRate, benchSpec.warehouses,
		testTimeout, roachtestutil.GetWorkloadHistogramArgs(t, c, nil), rampDuration, benchSpec.nodes)

	// In the case that we want to simulate high read amplification, we use kv0
	// to run a write-heavy workload known to be difficult for compactions to keep
	// pace with.
	if benchSpec.slowWrites {
		workloadCmd = fmt.Sprintf("./cockroach workload run kv --init --concurrency=%d --splits=1000 "+
			"--read-percent=50 --min-block-bytes=8192 --max-block-bytes=8192 --duration=%s "+
			"%s --ramp=%s --tolerate-errors {pgurl:1-%d}", benchSpec.nodes*64,
			testTimeout, roachtestutil.GetWorkloadHistogramArgs(t, c, nil), rampDuration, benchSpec.nodes)
	}

	setupDecommissionBench(ctx, t, c, benchSpec, pinnedNode, importCmd)

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := c.NewMonitor(workloadCtx, crdbNodes)

	if !benchSpec.noLoad {
		m.Go(
			func(ctx context.Context) error {
				close(rampStarted)

				// Run workload effectively indefinitely, to be later killed by context
				// cancellation once decommission has completed.
				err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), workloadCmd)
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

	// Setup Prometheus/Grafana using workload node.
	allCrdbNodes := crdbNodes.Merge(addlNodes)
	cleanupFunc := setupGrafana(ctx, t, c, allCrdbNodes, workloadNode)
	defer cleanupFunc()

	// Create a histogram registry for recording multiple decommission metrics.
	// Note that "decommission.*" metrics are special in that they are
	// long-running metrics measured by the elapsed time between each tick,
	// as opposed to the histograms of workload operation latencies or other
	// recorded values that are typically output in a "tick" each second.
	reg, tickByName, perfBuf, exporter := createDecommissionBenchPerfArtifacts(
		t, c,
		decommissionMetric,
		estimatedMetric,
		bytesUsedMetric,
	)

	defer func() {
		if err := exporter.Close(func() error {
			uploadPerfArtifacts(ctx, t, c, benchSpec, pinnedNode, workloadNode, perfBuf)
			return nil
		}); err != nil {
			t.Errorf("error closing perf exporter: %s", err)
		}
	}()

	// The logical node id of the current decommissioning node.
	var targetNodeAtomic uint32

	m.Go(func(ctx context.Context) error {
		defer workloadCancel()

		h := newDecommTestHelper(t, c)
		h.blockFromRandNode(workloadNode)

		// If we are running a workload, wait until it has started and completed its
		// ramp time before initiating a decommission.
		if !benchSpec.noLoad {
			<-rampStarted
			t.Status("Waiting for workload to ramp up...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rampDuration + 1*time.Minute):
				// Workload ramp-up complete, plus 1 minute of recording workload stats.
			}
		}

		if len(addlNodes) > 0 {
			h.t.Status(fmt.Sprintf("starting %d additional node(s)", len(addlNodes)))
			h.c.Start(ctx, h.t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), addlNodes)
			for _, addlNode := range addlNodes {
				h.blockFromRandNode(addlNode)
			}

			// Include an additional minute of buffer time before starting decommission.
			time.Sleep(1 * time.Minute)
		}

		m.ExpectDeath()
		defer m.ResetDeaths()
		err := runSingleDecommission(ctx, c, h, pinnedNode, benchSpec.decommissionNode, &targetNodeAtomic, benchSpec.snapshotRate,
			benchSpec.whileDown, benchSpec.drainFirst, false /* reuse */, benchSpec.whileUpreplicating,
			true /* estimateDuration */, benchSpec.slowWrites, tickByName,
		)

		// Include an additional minute of buffer time post-decommission to gather
		// workload stats.
		time.Sleep(1 * time.Minute)

		return err
	})

	m.Go(func(ctx context.Context) error {
		hists := reg.GetHandle()

		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		return trackBytesUsed(ctx, db, &targetNodeAtomic, hists, tickByName)
	})

	if err := m.WaitE(); err != nil {
		t.Fatal(err)
	}
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
	t.L().Printf("nodes %d - %d are crdb nodes", crdbNodes[0], crdbNodes[len(crdbNodes)-1])
	t.L().Printf("node %d is the workload node", workloadNode)

	maxRate := tpccMaxRate(benchSpec.warehouses)
	rampDuration := 3 * time.Minute
	rampStarted := make(chan struct{})
	importCmd := fmt.Sprintf(
		`./cockroach workload fixtures import tpcc --warehouses=%d`,
		benchSpec.warehouses,
	)
	workloadCmd := fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --max-rate=%d --duration=%s "+
		"%s --ramp=%s --tolerate-errors {pgurl:1-%d}", maxRate, benchSpec.warehouses,
		testTimeout, roachtestutil.GetWorkloadHistogramArgs(t, c, nil), rampDuration, benchSpec.nodes)

	setupDecommissionBench(ctx, t, c, benchSpec, pinnedNode, importCmd)

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := c.NewMonitor(workloadCtx, crdbNodes)

	if !benchSpec.noLoad {
		m.Go(
			func(ctx context.Context) error {
				close(rampStarted)

				// Run workload indefinitely, to be later killed by context
				// cancellation once decommission has completed.
				err := c.RunE(ctx, option.WithNodes(c.Node(workloadNode)), workloadCmd)
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

	// Setup Prometheus/Grafana using workload node.
	cleanupFunc := setupGrafana(ctx, t, c, crdbNodes, workloadNode)
	defer cleanupFunc()

	// Create a histogram registry for recording multiple decommission metrics.
	// Note that "decommission.*" metrics are special in that they are
	// long-running metrics measured by the elapsed time between each tick,
	// as opposed to the histograms of workload operation latencies or other
	// recorded values that are typically output in a "tick" each second.
	reg, tickByName, perfBuf, exporter := createDecommissionBenchPerfArtifacts(t, c,
		decommissionMetric, upreplicateMetric, bytesUsedMetric,
	)

	defer func() {
		if err := exporter.Close(func() error {
			uploadPerfArtifacts(ctx, t, c, benchSpec, pinnedNode, workloadNode, perfBuf)
			return nil
		}); err != nil {
			t.Errorf("error closing perf exporter: %s", err)
		}
	}()

	// The logical node id of the current decommissioning node.
	var targetNodeAtomic uint32

	m.Go(func(ctx context.Context) error {
		defer workloadCancel()

		h := newDecommTestHelper(t, c)
		h.blockFromRandNode(workloadNode)

		// If we are running a workload, wait until it has started and completed its
		// ramp time before initiating a decommission.
		if !benchSpec.noLoad {
			<-rampStarted
			t.Status("Waiting for workload to ramp up...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rampDuration + 1*time.Minute):
				// Workload ramp-up complete, plus 1 minute of recording workload stats.
			}
		}

		for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= benchSpec.duration; {
			m.ExpectDeath()
			err := runSingleDecommission(ctx, c, h, pinnedNode, benchSpec.decommissionNode, &targetNodeAtomic, benchSpec.snapshotRate,
				benchSpec.whileDown, benchSpec.drainFirst, true /* reuse */, benchSpec.whileUpreplicating,
				true /* estimateDuration */, benchSpec.slowWrites, tickByName,
			)
			m.ResetDeaths()
			if err != nil {
				return err
			}
		}

		// Include an additional minute of buffer time post-decommission to gather
		// workload stats.
		time.Sleep(1 * time.Minute)

		return nil
	})

	m.Go(func(ctx context.Context) error {
		hists := reg.GetHandle()

		db := c.Conn(ctx, t.L(), pinnedNode)
		defer db.Close()

		return trackBytesUsed(ctx, db, &targetNodeAtomic, hists, tickByName)
	})

	if err := m.WaitE(); err != nil {
		t.Fatal(err)
	}

}

// runSingleDecommission picks a random node and attempts to decommission that
// node from the pinned (i.e. first) node, validating and recording the duration.
// If the reuse flag is passed in, the node will be wiped and re-added to the
// cluster, with the upreplication duration tracked by upreplicateTicker.
func runSingleDecommission(
	ctx context.Context,
	c cluster.Cluster,
	h *decommTestHelper,
	pinnedNode int,
	target int,
	targetLogicalNodeAtomic *uint32,
	snapshotRateMb int,
	stopFirst, drainFirst, reuse, noBalanceWait, estimateDuration, slowWrites bool,
	tickByName func(name string),
) error {
	if target == 0 {
		target = h.getRandNodeOtherThan(pinnedNode)
	}
	targetLogicalNodeID, err := h.getLogicalNodeID(ctx, target)
	if err != nil {
		return err
	}
	h.t.Status(fmt.Sprintf("targeting node%d (n%d) for decommission", target, targetLogicalNodeID))

	// TODO(sarkesian): Consider adding a future test for decommissions that get
	// stuck with replicas in purgatory, by pinning them to a node.

	// We stop nodes gracefully when needed.
	stopOpts := option.NewStopOpts(option.Graceful(shutdownGracePeriod))

	// Gather metadata for logging purposes and wait for balance.
	var bytesUsed, rangeCount, totalRanges int64
	var candidateStores, avgBytesPerReplica int64
	{
		dbNode := h.c.Conn(ctx, h.t.L(), target)
		defer dbNode.Close()

		if err := dbNode.QueryRow(
			`SELECT count(*) FROM crdb_internal.ranges_no_leases`,
		).Scan(&totalRanges); err != nil {
			return err
		}

		if !noBalanceWait {
			h.t.Status("waiting for cluster balance")
			if err := waitForRebalance(
				ctx, h.t.L(), dbNode, float64(totalRanges)/3.0, 60, /* stableSeconds */
			); err != nil {
				return err
			}
		}

		if err := dbNode.QueryRow(
			"SELECT sum(range_count), sum(used) "+
				"FROM crdb_internal.kv_store_status WHERE node_id = $1 GROUP BY node_id LIMIT 1",
			targetLogicalNodeID,
		).Scan(&rangeCount, &bytesUsed); err != nil {
			return err
		}

		if err := dbNode.QueryRow(
			"SELECT avg(range_size)::int "+
				"FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
			targetLogicalNodeID,
		).Scan(&avgBytesPerReplica); err != nil {
			return err
		}

		if err := dbNode.QueryRow(
			"SELECT count(store_id) FROM crdb_internal.kv_store_status where node_id != $1",
			targetLogicalNodeID,
		).Scan(&candidateStores); err != nil {
			return err
		}
	}

	if drainFirst {
		h.t.Status(fmt.Sprintf("draining node%d", target))
		cmd := fmt.Sprintf("./cockroach node drain --certs-dir=%s --port={pgport%s} --self", install.CockroachNodeCertsDir, c.Node(target))
		if err := h.c.RunE(ctx, option.WithNodes(h.c.Node(target)), cmd); err != nil {
			return err
		}
	}

	if stopFirst {
		h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
		if err := h.c.StopE(ctx, h.t.L(), stopOpts, c.Node(target)); err != nil {
			return err
		}
		// Wait after stopping the node to distinguish the impact of the node being
		// down vs decommissioning.
		time.Sleep(5 * time.Minute)
	}

	if slowWrites {
		h.t.Status(fmt.Sprintf("limiting write bandwith to 100MiBps and awaiting "+
			"increased read amplification on node%d", target))
		if err := h.c.RunE(
			ctx, option.WithNodes(h.c.Node(target)),
			fmt.Sprintf("sudo bash -c 'echo \"259:0  %d\" > "+
				"/sys/fs/cgroup/blkio/system.slice/%s.service/blkio.throttle.write_bps_device'",
				100*(1<<20), roachtestutil.SystemInterfaceSystemdUnitName())); err != nil {
			return err
		}
		// Wait for some time after limiting write bandwidth in order to affect read amplification.
		time.Sleep(5 * time.Minute)

		// Print LSM health to logs. Intentionally swallows error to avoid flakes.
		_ = logLSMHealth(ctx, h.t.L(), h.c, target)
	}

	if estimateDuration {
		estimateDecommissionDuration(
			ctx, h.t, h.t.L(), tickByName, snapshotRateMb, bytesUsed, candidateStores,
			rangeCount, avgBytesPerReplica,
		)
	}

	h.t.Status(fmt.Sprintf("decommissioning node%d (n%d)", target, targetLogicalNodeID))
	atomic.StoreUint32(targetLogicalNodeAtomic, uint32(targetLogicalNodeID))
	tBegin := timeutil.Now()
	tickByName(decommissionMetric)
	targetLogicalNodeIDList := option.NodeListOption{targetLogicalNodeID}
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
	atomic.StoreUint32(targetLogicalNodeAtomic, uint32(0))
	tickByName(decommissionMetric)
	elapsed := timeutil.Since(tBegin)

	h.t.Status(fmt.Sprintf("decommissioned node%d (n%d) in %s (ranges: %d, size: %s)",
		target, targetLogicalNodeID, elapsed, rangeCount, humanizeutil.IBytes(bytesUsed)))

	if reuse {
		if !stopFirst {
			h.t.Status(fmt.Sprintf("gracefully stopping node%d", target))
			if err := h.c.StopE(ctx, h.t.L(), stopOpts, c.Node(target)); err != nil {
				return err
			}
		}

		// Wipe the node and re-add to cluster with a new node ID.
		if err := h.c.RunE(ctx, option.WithNodes(h.c.Node(target)), "rm -rf {store-dir}"); err != nil {
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

		// If we don't need to wait for the cluster to rebalance, wait a short
		// buffer period before the next decommission and bail out early.
		if noBalanceWait {
			time.Sleep(1 * time.Minute)
			return nil
		}

		newLogicalNodeID, err := h.getLogicalNodeID(ctx, target)
		if err != nil {
			return err
		}
		atomic.StoreUint32(targetLogicalNodeAtomic, uint32(newLogicalNodeID))

		tickByName(upreplicateMetric)
		{
			dbNode := h.c.Conn(ctx, h.t.L(), pinnedNode)
			defer dbNode.Close()

			h.t.Status("waiting for new node to become active")
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

			// Before checking for balance, ensure we have started upreplication to
			// the new node.
			h.t.Status("waiting for new node to have >1 replicas")
			for {
				var count int
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if err := dbNode.QueryRow(
						"SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
						newLogicalNodeID,
					).Scan(&count); err != nil {
						return err
					}
				}

				if count > 1 {
					break
				}
			}

			h.t.Status("waiting for replica counts to balance across nodes")
			if err := waitForRebalance(
				ctx, h.t.L(), dbNode, float64(totalRanges)/3.0, 60, /* stableSeconds */
			); err != nil {
				return err
			}
		}
		atomic.StoreUint32(targetLogicalNodeAtomic, 0)
		tickByName(upreplicateMetric)
	}

	return nil
}

// logLSMHealth is a convenience method that logs the output of /debug/lsm.
func logLSMHealth(ctx context.Context, l *logger.Logger, c cluster.Cluster, target int) error {
	l.Printf("LSM Health of node%d", target)
	adminAddrs, err := c.InternalAdminUIAddr(ctx, l, c.Node(target))
	if err != nil {
		return err
	}
	result, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(c.Node(target)),
		"curl", "-s", fmt.Sprintf("http://%s/debug/lsm",
			adminAddrs[0]))
	if err == nil {
		l.Printf(result.Stdout)
	} else {
		l.Printf(result.Stderr)
	}
	return err
}

// estimateDecommissionDuration attempts to come up with a rough estimate for
// the theoretical minimum decommission duration as well as the estimated
// actual duration of the decommission and writes them to the log and the
// recorded perf artifacts as ticks.
func estimateDecommissionDuration(
	ctx context.Context,
	t task.Tasker,
	log *logger.Logger,
	tickByName func(name string),
	snapshotRateMb int,
	bytesUsed int64,
	candidateStores int64,
	rangeCount int64,
	avgBytesPerReplica int64,
) {
	tickByName(estimatedMetric)

	if snapshotRateMb == 0 {
		snapshotRateMb = defaultSnapshotRateMb
	}
	snapshotRateBytes := float64(snapshotRateMb * 1024 * 1024)

	// The theoretical minimum decommission time is simply:
	// bytesToMove = bytesUsed / candidateStores
	// minTime = bytesToMove / bytesPerSecond
	minSeconds := (float64(bytesUsed) / float64(candidateStores)) / snapshotRateBytes
	minDuration := time.Duration(minSeconds * float64(time.Second))

	// The estimated decommission time incorporates the concept of replicas,
	// which are the unit on which we send snapshots. Thus,
	// snapshotsToSend = ceil(replicaCount / candidateStores)
	// avgTimePerSnapshot = avgBytesPerReplica / bytesPerSecond
	// estTime = snapshotsToSend * avgTimePerSnapshot
	estSeconds := math.Ceil(float64(rangeCount)/float64(candidateStores)) *
		float64(avgBytesPerReplica) / snapshotRateBytes
	estDuration := time.Duration(estSeconds * float64(time.Second))

	log.Printf(
		"Given %d replicas with %s avg replica size, decommission time is estimated at:\n"+
			"\ttheoretical min: %s"+
			"\t      estimated: %s",
		rangeCount, humanizeutil.IBytes(avgBytesPerReplica), minDuration, estDuration,
	)

	fireAfter(ctx, t, estDuration, func() {
		tickByName(estimatedMetric)
	})
}
