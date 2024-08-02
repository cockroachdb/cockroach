// Copyright 2024 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The tests in this file measure the latency impact of various perturbations to
// the system. Many support escalations are driven by unexpected latency
// increases. The goal of the tests is twofold. First for perturbations that we
// have implemented a mitigation for, we want to measure how well the mitigation
// works. For perturbations that we have not yet implemented a mitigation for,
// this test can be used internally to understand the causes of the mitigation.
//
// This testing uses 3 different approaches than other tests.
//
// 1) The client observed latency from the workload nodes is sent back to the
// roachtest node to allow it to measure latency and throughput during the test.
//
// 2) The max throughput during measurement is adjusted during the test based on
// the test configuration. This allows running the test metamorphically and
// checking the impact.
//
// 3) During the test we capture SQL traces from high latency operations to
// allow easier debugging after the fact.

// variations specifies the different variations to use for the test.
//
// T0: Test starts and is initialized.
// T1: Test runs at 100% until filled
// T2: Workload runs at 30% usage until the end - 2x validation duration + perturbation duration.
// T3: Start the perturbation.
// T4: End the perturbation.
// T5: End the test.
// The latency from T2-T3 is used as a baseline for measurement, and the times
// from T3-T4 and T4-T5 are used as two different measurements to compare to the
// baseline. In some variations there is a small window immediately after the
// perturbation is started where we don't measure the latency since we expect an
// impact (such as after a network partition or unexpected node crash)
//
// TODO(baptist): Add a timeline describing the test in more detail.
type variations struct {
	// These fields are set up during construction.
	seed                 int64
	fillDuration         time.Duration
	maxBlockBytes        int
	perturbationDuration time.Duration
	validationDuration   time.Duration
	ratioOfMax           float64
	splits               int
	numNodes             int
	numWorkloadNodes     int
	partitionSite        bool
	vcpu                 int
	disks                int
	leaseType            registry.LeaseType
	cleanRestart         bool
	perturbation         perturbation
	workload             workloadType
	acceptableChange     float64

	// These fields are set up at the start of the test run
	cluster       cluster.Cluster
	histogramPort int
	roachtestAddr string
}

const NUM_REGIONS = 3

var durationOptions = []time.Duration{10 * time.Second, 10 * time.Minute, 30 * time.Minute}
var splitOptions = []int{1, 100, 10000}
var maxBlockBytes = []int{1, 1024, 4096}
var numNodes = []int{5, 12, 36}
var numVCPUs = []int{4, 8, 16, 32}
var numDisks = []int{1, 2}

var leases = []registry.LeaseType{
	registry.EpochLeases,
	registry.ExpirationLeases,
}

func (v variations) String() string {
	return fmt.Sprintf("seed: %d, fillDuration: %s, maxBlockBytes: %d, perturbationDuration: %s, "+
		"validationDuration: %s, ratioOfMax: %f, splits: %d, numNodes: %d, numWorkloadNodes: %d, "+
		"partitionSite: %t, vcpu: %d, disks: %d, leaseType: %s", v.seed, v.fillDuration, v.maxBlockBytes,
		v.perturbationDuration, v.validationDuration, v.ratioOfMax, v.splits, v.numNodes, v.numWorkloadNodes,
		v.partitionSite, v.vcpu, v.disks, v.leaseType)
}

// Normally a single worker can handle 20-40 nodes. If we find this is
// insufficient we can bump it up.
const numNodesPerWorker = 20

// setup sets up the parameters that can be manually set up without having the
// cluster spec. finishSetup below sets up the remainder after the cluster is
// defined.
func setupMetamorphic(p perturbation) variations {
	// TODO(baptist): Allow specifying the seed to reproduce a metamorphic run.
	rng, seed := randutil.NewPseudoRand()

	v := variations{}
	v.seed = seed
	v.workload = kvWorkload{}
	v.fillDuration = 10 * time.Minute
	v.validationDuration = 5 * time.Minute
	v.ratioOfMax = 0.5
	v.splits = splitOptions[rng.Intn(len(splitOptions))]
	v.maxBlockBytes = maxBlockBytes[rng.Intn(len(maxBlockBytes))]
	v.perturbationDuration = durationOptions[rng.Intn(len(durationOptions))]
	v.leaseType = leases[rng.Intn(len(leases))]
	v.numNodes = numNodes[rng.Intn(len(numNodes))]
	v.numWorkloadNodes = v.numNodes/numNodesPerWorker + 1
	v.vcpu = numVCPUs[rng.Intn(len(numVCPUs))]
	v.disks = numDisks[rng.Intn(len(numDisks))]
	v.partitionSite = rng.Intn(2) == 0
	v.cleanRestart = rng.Intn(2) == 0
	v.perturbation = p
	return v
}

// setupFull sets up the full test with a fixed set of parameters.
func setupFull(p perturbation) variations {
	v := variations{}
	v.workload = kvWorkload{}
	v.leaseType = registry.ExpirationLeases
	v.maxBlockBytes = 4096
	v.splits = 10000
	v.numNodes = 12
	v.numWorkloadNodes = v.numNodes/numNodesPerWorker + 1
	v.partitionSite = true
	v.vcpu = 16
	v.disks = 2
	v.fillDuration = 10 * time.Minute
	v.validationDuration = 5 * time.Minute
	v.perturbationDuration = 10 * time.Minute
	v.ratioOfMax = 0.5
	v.cleanRestart = true
	v.perturbation = p
	return v
}

// setupDev sets up the test for local development.
func setupDev(p perturbation) variations {
	v := variations{}
	v.workload = kvWorkload{}
	v.leaseType = registry.ExpirationLeases
	v.maxBlockBytes = 1024
	v.splits = 1
	v.numNodes = 4
	v.numWorkloadNodes = 1
	v.vcpu = 4
	v.disks = 1
	v.partitionSite = true
	v.fillDuration = 20 * time.Second
	v.validationDuration = 10 * time.Second
	v.perturbationDuration = 30 * time.Second
	v.ratioOfMax = 0.5
	v.cleanRestart = true
	v.perturbation = p
	return v
}

// finishSetup is called after the cluster is defined to determine the running nodes.
func (v *variations) finishSetup(c cluster.Cluster, port int) {
	v.cluster = c
	if c.Spec().NodeCount != v.numNodes+v.numWorkloadNodes {
		panic(fmt.Sprintf("node count mismatch, %d != %d + %d", c.Spec().NodeCount, v.numNodes, v.numWorkloadNodes))
	}
	if c.IsLocal() {
		v.roachtestAddr = "localhost"
	} else {
		listenerAddr, err := getListenAddr(context.Background())
		if err != nil {
			panic(err)
		}
		v.roachtestAddr = listenerAddr
	}
	v.histogramPort = port
}

func registerLatencyTests(r registry.Registry) {
	// NB: If these tests fail because they are flaky, increase the numbers
	// until they pass. Additionally add the seed (from the log) that caused
	// them to fail as a comment in the test.
	addTest(r, "perturbation/metamorphic/restart", setupMetamorphic(restart{}), 100)
	addTest(r, "perturbation/metamorphic/partition", setupMetamorphic(partition{}), 100)
	addTest(r, "perturbation/metamorphic/add-node", setupMetamorphic(addNode{}), 2)
	addTest(r, "perturbation/metamorphic/decommission", setupMetamorphic(decommission{}), 3)

	addTest(r, "perturbation/full/restart", setupFull(restart{}), 20)
	addTest(r, "perturbation/full/partition", setupFull(partition{}), 100)
	addTest(r, "perturbation/full/add-node", setupFull(addNode{}), 1.5)
	addTest(r, "perturbation/full/decommission", setupFull(decommission{}), 1.5)

	addTest(r, "perturbation/dev/restart", setupDev(restart{}), 100)
	addTest(r, "perturbation/dev/partition", setupDev(partition{}), 100)
	addTest(r, "perturbation/dev/add-node", setupDev(addNode{}), 100)
	addTest(r, "perturbation/dev/decommission", setupDev(decommission{}), 100)
}

func (v variations) makeClusterSpec() spec.ClusterSpec {
	// NB: We use low memory to force non-cache disk reads earlier.
	return spec.MakeClusterSpec(v.numNodes+v.numWorkloadNodes, spec.CPU(v.vcpu), spec.SSD(v.disks), spec.Mem(spec.Low))
}

func addTest(r registry.Registry, testName string, v variations, acceptableChange float64) {
	v.acceptableChange = acceptableChange
	// TODO(baptist): Reenable this for weekly testing once consistently passing.
	r.Add(registry.TestSpec{
		Name:             testName,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.ManualOnly,
		Owner:            registry.OwnerAdmissionControl,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Run:              v.runTest,
	})
}

type perturbation interface {
	// startTargetNode is called for custom logic starting the target node(s).
	// Some of the perturbations need special logic for starting the target
	// node.
	startTargetNode(ctx context.Context, l *logger.Logger, v variations)

	// startPerturbation begins the system change and blocks until it is
	// finished. It returns the duration looking backwards to collect
	// performance stats.
	startPerturbation(ctx context.Context, l *logger.Logger, v variations) time.Duration

	// endPerturbation ends the system change. Not all perturbations do anything on stop.
	// It returns the duration looking backwards to collect performance stats.
	endPerturbation(ctx context.Context, l *logger.Logger, v variations) time.Duration
}

// restart will gracefully stop and then restart a node after a custom duration.
type restart struct{}

var _ perturbation = restart{}

func (r restart) startTargetNode(ctx context.Context, l *logger.Logger, v variations) {
	v.startNoBackup(ctx, l, v.targetNodes())
}

// startPerturbation stops the target node with a graceful shutdown.
func (r restart) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	gracefulOpts := option.DefaultStopOpts()
	// SIGTERM for clean shutdown
	if v.cleanRestart {
		gracefulOpts.RoachprodOpts.Sig = 15
	} else {
		gracefulOpts.RoachprodOpts.Sig = 9
	}
	gracefulOpts.RoachprodOpts.Wait = true
	v.cluster.Stop(ctx, l, gracefulOpts, v.targetNodes())
	waitDuration(ctx, v.perturbationDuration)
	if v.cleanRestart {
		return timeutil.Since(startTime)
	}
	// If it is not a clean restart, we ignore the first 10 seconds to allow for lease movement.
	return timeutil.Since(startTime) + 10*time.Second
}

// endPerturbation restarts the node.
func (r restart) endPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	v.startNoBackup(ctx, l, v.targetNodes())
	waitDuration(ctx, v.validationDuration)
	return timeutil.Since(startTime)
}

// partition nodes in the first region.
func (v variations) withPartitionedNodes(c cluster.Cluster) install.RunOptions {
	numPartitionNodes := 1
	if v.partitionSite {
		numPartitionNodes = v.numNodes / NUM_REGIONS
	}
	return option.WithNodes(c.Range(1, numPartitionNodes))
}

// partition will partition the target node from either one other node or all
// other nodes in a different AZ.
type partition struct{}

var _ perturbation = partition{}

func (p partition) startTargetNode(ctx context.Context, l *logger.Logger, v variations) {
	v.startNoBackup(ctx, l, v.targetNodes())
}

func (p partition) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	targetIPs, err := v.cluster.InternalIP(ctx, l, v.targetNodes())
	if err != nil {
		panic(err)
	}

	if !v.cluster.IsLocal() {
		v.cluster.Run(
			ctx,
			v.withPartitionedNodes(v.cluster),
			fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s -j DROP`, targetIPs[0]))
	}
	waitDuration(ctx, v.perturbationDuration)
	// During the first 10 seconds after the partition, we expect latency to drop,
	// start measuring after 20 seconds.
	return v.perturbationDuration - 20*time.Second
}

func (p partition) endPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	if !v.cluster.IsLocal() {
		v.cluster.Run(ctx, v.withPartitionedNodes(v.cluster), `sudo iptables -F`)
	}
	waitDuration(ctx, v.validationDuration)
	return timeutil.Since(startTime)
}

// addNode will add a node during the start phase and wait for it to complete.
// It doesn't do anything during the stop phase.
type addNode struct{}

var _ perturbation = addNode{}

func (addNode) startTargetNode(ctx context.Context, l *logger.Logger, v variations) {
}

func (a addNode) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	v.startNoBackup(ctx, l, v.targetNodes())
	// Wait out the time until the store is no longer suspect. The 11s is based
	// on the 10s server.time_after_store_suspect setting which we set below
	// plus 1 sec for the store to propagate its gossip information.
	waitDuration(ctx, 11*time.Second)
	waitForRebalanceToStop(ctx, l, v.cluster)
	return timeutil.Since(startTime)
}

// endPerturbation already waited for completion as part of start, so it doesn't
// need to wait again here.
func (addNode) endPerturbation(ctx context.Context, l *logger.Logger, v variations) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}

// restart will gracefully stop and then restart a node after a custom duration.
type decommission struct{}

var _ perturbation = restart{}

func (d decommission) startTargetNode(ctx context.Context, l *logger.Logger, v variations) {
	v.startNoBackup(ctx, l, v.targetNodes())
}

func (d decommission) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	// TODO(baptist): If we want to support multiple decommissions in parallel,
	// run drain and decommission in separate goroutine.
	l.Printf("draining target nodes")
	for _, node := range v.targetNodes() {
		drainCmd := fmt.Sprintf(
			"./cockroach node drain --self --certs-dir=%s --port={pgport:%d}",
			install.CockroachNodeCertsDir,
			node,
		)
		v.cluster.Run(ctx, option.WithNodes(v.cluster.Node(node)), drainCmd)
	}

	// Wait for all the other nodes to see the drain over gossip.
	time.Sleep(10 * time.Second)

	l.Printf("decommissioning nodes")
	for _, node := range v.targetNodes() {
		decommissionCmd := fmt.Sprintf(
			"./cockroach node decommission --self --certs-dir=%s --port={pgport:%d}",
			install.CockroachNodeCertsDir,
			node,
		)
		v.cluster.Run(ctx, option.WithNodes(v.cluster.Node(node)), decommissionCmd)
	}

	l.Printf("stopping decommissioned nodes")
	v.cluster.Stop(ctx, l, option.DefaultStopOpts(), v.targetNodes())
	return timeutil.Since(startTime)
}

// endPerturbation already waited for completion as part of start, so it doesn't
// need to wait again here.
func (d decommission) endPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}

func prettyPrint(title string, stats map[string]trackedStat) string {
	var outputStr strings.Builder
	outputStr.WriteString(title + "\n")
	keys := sortedStringKeys(stats)
	for _, name := range keys {
		outputStr.WriteString(fmt.Sprintf("%-15s: %s\n", name, stats[name]))
	}
	return outputStr.String()
}

// waitForRebalanceToStop polls the system.rangelog every second to see if there
// have been any transfers in the last 5 seconds. It returns once the system
// stops transferring replicas.
func waitForRebalanceToStop(ctx context.Context, l *logger.Logger, c cluster.Cluster) {
	db := c.Conn(ctx, l, 1)
	defer db.Close()
	q := `SELECT extract_duration(seconds FROM now()-timestamp) FROM system.rangelog WHERE "eventType" = 'add_voter' ORDER BY timestamp DESC LIMIT 1`

	opts := retry.Options{
		InitialBackoff: 1 * time.Second,
		Multiplier:     1,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if row := db.QueryRow(q); row != nil {
			var secondsSinceLastEvent int
			if err := row.Scan(&secondsSinceLastEvent); err != nil && !errors.Is(err, gosql.ErrNoRows) {
				panic(err)
			}
			if secondsSinceLastEvent > 5 {
				return
			}
		}
	}
	panic("retry should not have exited")
}

// runTest is the main entry point for all the tests. Its ste
func (v variations) runTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	r := histogram.CreateUdpReceiver()
	v.finishSetup(c, r.Port())
	t.L().Printf("test variations are: %+v", v)
	t.Status("T0: starting nodes")

	// Track the three operations that we are sending in this test.
	m := c.NewMonitor(ctx, v.stableNodes())
	lm := newLatencyMonitor(v.workload.operations())

	// Start the stable nodes and let the perturbation start the target node(s).
	v.startNoBackup(ctx, t.L(), v.stableNodes())
	v.perturbation.startTargetNode(ctx, t.L(), v)

	func() {
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		// This isn't strictly necessary, but it would be nice if this test passed at 10s (or lower).
		if _, err := db.Exec(
			`SET CLUSTER SETTING server.time_after_store_suspect = '10s'`); err != nil {
			t.Fatal(err)
		}
		// Avoid stores up-replicating away from the target node, reducing the
		// backlog of work.
		if _, err := db.Exec(
			fmt.Sprintf(
				`SET CLUSTER SETTING server.time_until_store_dead = '%s'`, v.perturbationDuration+time.Minute)); err != nil {
			t.Fatal(err)
		}
	}()

	// Wait for rebalancing to finish before starting to fill. This minimizes the time to finish.
	waitForRebalanceToStop(ctx, t.L(), v.cluster)
	require.NoError(t, v.workload.initWorkload(ctx, v))

	// Start collecting latency measurements after the workload has been initialized.
	m.Go(r.Listen)
	cancelReporter := m.GoWithCancel(func(ctx context.Context) error {
		return lm.start(ctx, t.L(), r, func(t trackedStat) time.Duration {
			// This is an ad-hoc score. The general idea is to weigh the p50, p99 and p99.9
			// latency based on their relative values. Set the score to infinity if the
			// throughput dropped to 0.
			if t.count > 0 {
				return time.Duration(math.Sqrt((float64(v.numWorkloadNodes+v.vcpu) * float64((t.p50 + t.p99/3 + t.p999/10)) / float64(t.count) * float64(time.Second))))
			} else {

				return time.Duration(math.Inf(1))
			}
		})
	})

	// Start filling the system without a rate.
	t.Status("T1: filling at full rate")
	require.NoError(t, v.workload.runWorkload(ctx, v, v.fillDuration, 0))

	// Capture the max rate near the last 1/4 of the fill process.
	sampleWindow := v.fillDuration / 4
	ratioOfWrites := 0.5
	fillStats := lm.worstStats(sampleWindow)
	stableRate := int(float64(fillStats[`write`].count)*v.ratioOfMax/ratioOfWrites) / v.numWorkloadNodes

	// Start the consistent workload and begin collecting profiles.
	t.L().Printf("stable rate for this cluster is %d per node", stableRate)
	t.Status("T2: running workload at stable rate")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		if err := v.workload.runWorkload(ctx, v, 0, stableRate); !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	go func() {
		waitDuration(ctx, v.validationDuration/2)
		initialStats := lm.worstStats(v.validationDuration / 2)

		// Collect profiles for the twice the 99.9th percentile of the write
		// latency or 10ms whichever is higher.
		const minProfileThreshold = 10 * time.Millisecond

		profileThreshold := initialStats[`write`].p999 * 2
		// Set the threshold high enough that we don't spuriously collect lots of useless profiles.
		if profileThreshold < minProfileThreshold {
			profileThreshold = minProfileThreshold
		}

		t.L().Printf("profiling statements that take longer than %s", profileThreshold)
		if err := profileTopStatements(ctx, c, t.L(), profileThreshold); err != nil {
			t.Fatal(err)
		}
	}()

	// Let enough data be written to all nodes in the cluster, then induce the perturbation.
	waitDuration(ctx, v.validationDuration)
	// Collect the baseline after the workload has stabilized.
	baselineStats := lm.worstStats(v.validationDuration / 2)
	// Now start the perturbation.
	t.Status("T3: inducing perturbation")
	perturbationDuration := v.perturbation.startPerturbation(ctx, t.L(), v)
	perturbationStats := lm.worstStats(perturbationDuration)

	t.Status("T4: recovery from the perturbation")
	afterDuration := v.perturbation.endPerturbation(ctx, t.L(), v)
	afterStats := lm.worstStats(afterDuration)

	t.L().Printf("%s\n", prettyPrint("Fill stats", fillStats))
	t.L().Printf("%s\n", prettyPrint("Baseline stats", baselineStats))
	t.L().Printf("%s\n", prettyPrint("Perturbation stats", perturbationStats))
	t.L().Printf("%s\n", prettyPrint("Recovery stats", afterStats))

	t.Status("T5: validating results")
	// Stop all the running threads and wait for them.
	r.Close()
	cancelReporter()
	cancelWorkload()
	m.Wait()
	require.NoError(t, downloadProfiles(ctx, c, t.L(), t.ArtifactsDir()))

	t.L().Printf("validating stats during the perturbation")
	duringOK := v.isAcceptableChange(t.L(), baselineStats, perturbationStats)
	t.L().Printf("validating stats after the perturbation")
	afterOK := v.isAcceptableChange(t.L(), baselineStats, afterStats)
	require.True(t, duringOK && afterOK)
}

// Track all histograms for up to one hour in the past.
const numOpsToTrack = 3600

// trackedStat is a collection of the relevant values from the histogram. The
// score is a unitless composite measure representing the throughput and latency
// of a histogram. Lower scores are better.
type trackedStat struct {
	clockTime time.Time
	count     int64
	mean      time.Duration
	stddev    time.Duration
	p50       time.Duration
	p99       time.Duration
	p999      time.Duration
	score     time.Duration
}

type scoreCalculator func(trackedStat) time.Duration

func (t trackedStat) String() string {
	return fmt.Sprintf("%s: score: %s, count: %d, mean: %s, stddev: %s, p50: %s, p99: %s, p99.9: %s",
		t.clockTime, t.score, t.count, t.mean.String(), t.stddev.String(), t.p50.String(), t.p99.String(), t.p999.String())
}

// latencyMonitor tracks the latency of operations ver a period of time. It has
// a fixed number of operations to track and rolls the previous operations into
// the buffer.
type latencyMonitor struct {
	statNames []string
	// track the relevant stats for the last N operations in a circular buffer.
	mu struct {
		syncutil.Mutex
		index   int
		tracker map[string]*[numOpsToTrack]trackedStat
	}
}

func newLatencyMonitor(statNames []string) *latencyMonitor {
	lm := latencyMonitor{}
	lm.statNames = statNames
	lm.mu.tracker = make(map[string]*[numOpsToTrack]trackedStat)
	for _, name := range lm.statNames {
		lm.mu.tracker[name] = &[numOpsToTrack]trackedStat{}
	}
	return &lm
}

// worstStats captures the worst case ver the relevant stats for the last N
func (lm *latencyMonitor) worstStats(window time.Duration) map[string]trackedStat {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	stats := make(map[string]trackedStat)
	// startIndex is always between 0 and numOpsToTrack - 1.
	startIndex := (lm.mu.index + numOpsToTrack - int(window.Seconds())) % numOpsToTrack
	// endIndex is alwayss greater than startIndex.
	endIndex := (lm.mu.index + numOpsToTrack) % numOpsToTrack
	if endIndex < startIndex {
		endIndex += numOpsToTrack
	}

	for name, stat := range lm.mu.tracker {
		s := stat[startIndex]
		for i := startIndex + 1; i < endIndex; i++ {
			if stat[i%numOpsToTrack].score > s.score {
				s = stat[i%numOpsToTrack]
			}
		}
		stats[name] = s
	}
	return stats
}

// minAcceptableLatencyThreshold is the threshold below which we consider the
// latency acceptable regardless of any relative change from the baseline.
const minAcceptableLatencyThreshold = 10 * time.Millisecond

// isAcceptableChange determines if a change from the baseline is acceptable.
// It compares all the metrics rather than failing fast. Normally multiple
// metrics will fail at once if a test is going to fail and it is helpful to see
// all the differences.
func (v variations) isAcceptableChange(
	logger *logger.Logger, baseline, other map[string]trackedStat,
) bool {
	// This can happen if we aren't measuring one of the phases.
	if len(other) == 0 {
		return true
	}
	allPassed := true
	keys := sortedStringKeys(baseline)
	for _, name := range keys {
		baseStat := baseline[name]
		otherStat := other[name]
		increase := float64(otherStat.score) / float64(baseStat.score)
		if float64(otherStat.p99) > float64(minAcceptableLatencyThreshold) && increase > v.acceptableChange {
			logger.Printf("FAILURE: %-15s: Increase %.4f > %.4f BASE: %v SCORE: %v\n", name, increase, v.acceptableChange, baseStat.score, otherStat.score)
			allPassed = false
		} else {
			logger.Printf("PASSED : %-15s: Increase %.4f <= %.4f BASE: %v SCORE: %v\n", name, increase, v.acceptableChange, baseStat.score, otherStat.score)
		}
	}
	return allPassed
}

// start the latency monitor which will run until the context is cancelled.
func (lm *latencyMonitor) start(
	ctx context.Context, l *logger.Logger, r *histogram.UdpReceiver, c scoreCalculator,
) error {
outer:
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			histograms := r.Tick()
			sMap := make(map[string]trackedStat)

			// Pull out the relevant data from the stats.
			for _, name := range lm.statNames {
				if histograms[name] == nil {
					l.Printf("no histogram for %s, %v", name, histograms)
					continue outer
				}
				h := histograms[name]
				stat := trackedStat{
					clockTime: timeutil.Now(),
					count:     h.TotalCount(),
					mean:      time.Duration(h.Mean()),
					stddev:    time.Duration(h.StdDev()),
					p50:       time.Duration(h.ValueAtQuantile(50)),

					p99:  time.Duration(h.ValueAtQuantile(99)),
					p999: time.Duration(h.ValueAtQuantile(99.9)),
				}
				stat.score = c(stat)
				sMap[name] = stat
			}

			// Roll the latest stats into the tracker under lock.
			func() {
				lm.mu.Lock()
				defer lm.mu.Unlock()
				for _, name := range lm.statNames {
					lm.mu.tracker[name][lm.mu.index] = sMap[name]
				}
				lm.mu.index = (lm.mu.index + 1) % numOpsToTrack
			}()

			l.Printf("%s", shortString(sMap))
		}
	}
}

func shortString(sMap map[string]trackedStat) string {
	var outputStr strings.Builder
	var count int64

	outputStr.WriteString("[op: p99(score)] ")
	keys := sortedStringKeys(sMap)
	for _, name := range keys {
		hist := sMap[name]
		outputStr.WriteString(fmt.Sprintf("%s: %s(%s), ", name, humanizeutil.Duration(hist.p99), humanizeutil.Duration(hist.score)))
		count += hist.count
	}
	outputStr.WriteString(fmt.Sprintf("op/s: %d", count))
	return outputStr.String()
}

// startNoBackup starts the nodes without enabling backup.
func (v variations) startNoBackup(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) {
	nodesPerRegion := v.numNodes / NUM_REGIONS
	for _, node := range nodes {
		// Don't start a backup schedule because this test is timing sensitive.
		opts := option.NewStartOpts(option.NoBackupSchedule)
		opts.RoachprodOpts.StoreCount = v.disks
		opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--locality=region=fake-%d", (node-1)/nodesPerRegion))
		v.cluster.Start(ctx, l, opts, install.MakeClusterSettings(), v.cluster.Node(node))
	}
}

func (v variations) workloadNodes() option.NodeListOption {
	return v.cluster.Range(v.numNodes+1, v.numNodes+v.numWorkloadNodes)
}

func (v variations) stableNodes() option.NodeListOption {
	return v.cluster.Range(1, v.numNodes-1)
}

// Note this is always only a single node today. If we have perturbations that
// require multiple targets we could add multi-target support.
func (v variations) targetNodes() option.NodeListOption {
	return v.cluster.Node(v.numNodes)
}

type workloadType interface {
	operations() []string
	initWorkload(ctx context.Context, v variations) error
	runWorkload(ctx context.Context, v variations, duration time.Duration, maxRate int) error
}

type kvWorkload struct{}

func (w kvWorkload) operations() []string {
	return []string{"write", "read", "follower-read"}
}

func (w kvWorkload) initWorkload(ctx context.Context, v variations) error {
	initCmd := fmt.Sprintf("./cockroach workload init kv --splits %d {pgurl:1}", v.splits)
	return v.cluster.RunE(ctx, option.WithNodes(v.cluster.Node(1)), initCmd)
}

// Don't run a workload against the node we're going to shut down.
func (w kvWorkload) runWorkload(
	ctx context.Context, v variations, duration time.Duration, maxRate int,
) error {
	runCmd := fmt.Sprintf(
		"./cockroach workload run kv --duration=%s --max-rate=%d --tolerate-errors --max-block-bytes=%d --read-percent=50 --follower-read-percent=50 --concurrency=500 --operation-receiver=%s:%d {pgurl%s}",
		duration, maxRate, v.maxBlockBytes, v.roachtestAddr, v.histogramPort, v.stableNodes())
	return v.cluster.RunE(ctx, option.WithNodes(v.workloadNodes()), runCmd)
}

// waitDuration waits until either the duration has passed or the context is cancelled.
// TODO(baptist): Is there a better place for this utility method?
func waitDuration(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(duration):
	}
}

func sortedStringKeys(m map[string]trackedStat) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
