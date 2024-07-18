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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
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
	seed                 int64
	cluster              cluster.Cluster
	fillDuration         time.Duration
	maxBlockBytes        int
	perturbationDuration time.Duration
	validationDuration   time.Duration
	ratioOfMax           float64
	splits               int
	stableNodes          int
	targetNode           int
	partitionSite        bool
	workloadNode         int
	crdbNodes            int
	histogramPort        int
	numNodes             int
	vcpu                 int
	disks                int
	roachtestAddr        string
	leaseType            registry.LeaseType
	perturbation         perturbation
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
	return fmt.Sprintf(
		`seed: %d, fill: %s, validation: %s, perturbation: %s, splits: %d,
stable: %d, target: %d, workload: %d, crdb: %d,  lease: %s, udp: %s:%d, 
nodes: %d, vcpu: %d, disks: %d`,
		v.seed,
		v.fillDuration,
		v.validationDuration,
		v.perturbationDuration,
		v.splits,
		v.stableNodes,
		v.targetNode,
		v.workloadNode,
		v.crdbNodes,
		v.leaseType,
		v.roachtestAddr,
		v.histogramPort,
		v.numNodes,
		v.vcpu,
		v.disks,
	)
}

// setup sets up the parameters that can be manually set up without having the
// cluster spec. finishSetup below sets up the remainder after the cluster is
// defined.
func setupMetamorphic(p perturbation) variations {
	// TODO(baptist): Allow specifying the seed to reproduce a metamorphic run.
	rng, seed := randutil.NewPseudoRand()

	v := variations{}
	v.seed = seed
	v.fillDuration = 10 * time.Minute
	v.validationDuration = 5 * time.Minute
	v.ratioOfMax = 0.5

	v.splits = splitOptions[rng.Intn(len(splitOptions))]
	v.maxBlockBytes = maxBlockBytes[rng.Intn(len(maxBlockBytes))]
	v.perturbationDuration = durationOptions[rng.Intn(len(durationOptions))]
	v.leaseType = leases[rng.Intn(len(leases))]
	v.numNodes = numNodes[rng.Intn(len(numNodes))]
	v.vcpu = numVCPUs[rng.Intn(len(numVCPUs))]
	v.disks = numDisks[rng.Intn(len(numDisks))]
	v.partitionSite = rng.Intn(2) == 0
	v.perturbation = p
	return v
}

// setupFull sets up the full test with a fixed set of parameters.
func setupFull(p perturbation) variations {
	v := variations{}
	v.leaseType = registry.ExpirationLeases
	v.maxBlockBytes = 4096
	v.splits = 10000
	v.numNodes = 12
	v.partitionSite = true
	v.vcpu = 16
	v.disks = 2
	v.fillDuration = 10 * time.Minute
	v.validationDuration = 5 * time.Minute
	v.perturbationDuration = 10 * time.Minute
	v.ratioOfMax = 0.5
	v.perturbation = p
	return v
}

// setupDev sets up the test for local development.
func setupDev(p perturbation) variations {
	v := variations{}
	v.leaseType = registry.ExpirationLeases
	v.maxBlockBytes = 1024
	v.splits = 100
	v.numNodes = 4
	v.vcpu = 4
	v.disks = 1
	v.partitionSite = true
	v.fillDuration = 20 * time.Second
	v.validationDuration = 10 * time.Second
	v.perturbationDuration = 30 * time.Second
	v.ratioOfMax = 0.5
	v.perturbation = p
	return v
}

// finishSetup is called after the cluster is defined to determine the running nodes.
func finishSetup(c cluster.Cluster, v *variations, port int) {
	v.cluster = c
	v.crdbNodes = c.Spec().NodeCount - 1
	v.stableNodes = v.crdbNodes - 1
	v.targetNode = v.crdbNodes
	v.workloadNode = c.Spec().NodeCount
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
	addTest(r, "perturbation/metamorphic/restart", setupMetamorphic(restart{}))
	addTest(r, "perturbation/metamorphic/partition", setupMetamorphic(partition{}))
	addTest(r, "perturbation/metamorphic/add-node", setupMetamorphic(addNode{}))
	addTest(r, "perturbation/metamorphic/decommission", setupMetamorphic(decommission{}))

	addTest(r, "perturbation/full/restart", setupFull(restart{}))
	addTest(r, "perturbation/full/partition", setupFull(partition{}))
	addTest(r, "perturbation/full/add-node", setupFull(addNode{}))
	addTest(r, "perturbation/full/decommission", setupFull(decommission{}))

	addTest(r, "perturbation/dev/restart", setupDev(restart{}))
	addTest(r, "perturbation/dev/partition", setupDev(partition{}))
	addTest(r, "perturbation/dev/add-node", setupDev(addNode{}))
	addTest(r, "perturbation/dev/decommission", setupDev(decommission{}))
}

func (v variations) makeClusterSpec() spec.ClusterSpec {
	// TODO(baptist): Make numWorkers increase based on v.numNodes.
	numWorkers := 1
	return spec.MakeClusterSpec(v.numNodes+numWorkers, spec.CPU(v.vcpu), spec.SSD(v.disks))
}

func addTest(r registry.Registry, testName string, v variations) {
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
	v.startNoBackup(ctx, l, v.cluster.Node(v.targetNode))
}

// startPerturbation stops the target node with a graceful shutdown.
func (r restart) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	gracefulOpts := option.DefaultStopOpts()
	// SIGTERM for clean shutdown
	gracefulOpts.RoachprodOpts.Sig = 15
	gracefulOpts.RoachprodOpts.Wait = true
	v.cluster.Stop(ctx, l, gracefulOpts, v.cluster.Node(v.targetNode))
	waitDuration(ctx, v.perturbationDuration)
	return timeutil.Since(startTime)
}

// endPerturbation restarts the node.
func (r restart) endPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	v.startNoBackup(ctx, l, v.cluster.Node(v.targetNode))
	waitDuration(ctx, v.validationDuration)
	return timeutil.Since(startTime)
}

// partition nodes in the first region.
func (v variations) withPartitionedNodes(c cluster.Cluster) install.RunOptions {
	numPartitionNodes := 1
	if v.partitionSite {
		numPartitionNodes = v.crdbNodes / NUM_REGIONS
	}
	return option.WithNodes(c.Range(1, numPartitionNodes))
}

// partition will partition the target node from either one other node or all
// other nodes in a different AZ.
type partition struct{}

var _ perturbation = partition{}

func (p partition) startTargetNode(ctx context.Context, l *logger.Logger, v variations) {
	v.startNoBackup(ctx, l, v.cluster.Node(v.targetNode))
}

func (p partition) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	targetIPs, err := v.cluster.InternalIP(ctx, l, v.cluster.Node(v.targetNode))
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
	v.startNoBackup(ctx, l, v.cluster.Node(v.targetNode))
	// Wait out the time until the store is no longer suspect. The 11s is based
	// on the 10s server.time_after_store_suspect setting which we set below
	// plus 1 sec for the store to propagate its gossip information.
	waitDuration(ctx, 11*time.Second)

	if err := waitForRebalanceToStop(ctx, l, v.cluster); err != nil {
		panic(err)
	}
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
	v.startNoBackup(ctx, l, v.cluster.Node(v.targetNode))
}

func (d decommission) startPerturbation(
	ctx context.Context, l *logger.Logger, v variations,
) time.Duration {
	startTime := timeutil.Now()
	l.Printf("draining target node")
	drainCmd := fmt.Sprintf(
		"./cockroach node drain --self --certs-dir=%s --port={pgport:%d}",
		install.CockroachNodeCertsDir,
		v.targetNode,
	)
	v.cluster.Run(ctx, option.WithNodes(v.cluster.Node(v.targetNode)), drainCmd)

	// Wait for all the other nodes to see the drain.
	time.Sleep(10 * time.Second)

	l.Printf("decommissioning node")
	decommissionCmd := fmt.Sprintf(
		"./cockroach node decommission --self --certs-dir=%s --port={pgport:%d}",
		install.CockroachNodeCertsDir,
		v.targetNode,
	)
	v.cluster.Run(ctx, option.WithNodes(v.cluster.Node(v.targetNode)), decommissionCmd)

	l.Printf("stopping decommissioned node")
	v.cluster.Stop(ctx, l, option.DefaultStopOpts(), v.cluster.Node(v.targetNode))
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

// waitForRebalanceToStop polls the system.rangelog every second to see if there
// have been any transfers in the last 5 seconds. It returns once the system
// stops transferring replicas.
func waitForRebalanceToStop(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
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
				return err
			}
			if secondsSinceLastEvent > 5 {
				return nil
			}
		}
	}
	return errors.New("retry should not have exited")
}

// runTest is the main entry point for all the tests. Its ste
func (v variations) runTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	r := histogram.CreateUdpReceiver()
	finishSetup(c, &v, r.Port())
	t.L().Printf("test variations are: %+v", v)
	t.Status("T0: starting nodes")

	// Track the three operations that we are sending in this test.
	m := c.NewMonitor(ctx, c.Range(1, v.stableNodes))
	lm := newLatencyMonitor([]string{`read`, `write`, `follower-read`})

	// Start the stable nodes and let the perturbation start the target node(s).
	v.startNoBackup(ctx, t.L(), c.Range(1, v.stableNodes))
	v.perturbation.startTargetNode(ctx, t.L(), v)

	func() {
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		// This isn't strictly necessary, but it would be nice if this test passed at 10s (or lower).
		if _, err := db.Exec(
			`SET CLUSTER SETTING server.time_after_store_suspect = '10s'`); err != nil {
			t.Fatal(err)
		}
	}()
	v.initWorkload(ctx, t.L())

	// Start collecting latency measurements after the workload has been initialized.
	m.Go(r.Listen)
	cancelReporter := m.GoWithCancel(func(ctx context.Context) error {
		return lm.start(ctx, t.L(), r)
	})

	// Start filling the system without a rate.
	t.Status("T1: filling at full rate")
	require.NoError(t, v.runWorkload(ctx, v.fillDuration, 0))

	// Capture the max rate near the last 1/4 of the fill process.
	sampleWindow := v.fillDuration / 4
	ratioOfWrites := 0.5
	fillStats := lm.mergeStats(sampleWindow)
	stableRate := int(float64(fillStats[`write`].TotalCount()) / sampleWindow.Seconds() * v.ratioOfMax / ratioOfWrites)

	// Start the consistent workload and begin collecting profiles.
	t.L().Printf("stable rate for this cluster is %d", stableRate)
	t.Status("T2: running workload at stable rate")
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		if err := v.runWorkload(ctx, 0, stableRate); !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	go func() {
		waitDuration(ctx, v.validationDuration/2)
		initialStats := lm.mergeStats(v.validationDuration / 2)

		// Collect profiles for the twice the 99.9th percentile of the write
		// latency or 10ms whichever is higher.
		const collectProfileQuantile = 99.9
		const minProfileThreshold = 10 * time.Millisecond

		profileThreshold := time.Duration(initialStats[`write`].ValueAtQuantile(collectProfileQuantile)) * 2
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
	baselineStats := lm.worstStats(t.L(), v.validationDuration/2)
	// Now start the perturbation.
	t.Status("T3: inducing perturbation")
	perturbationDuration := v.perturbation.startPerturbation(ctx, t.L(), v)
	perturbationStats := lm.worstStats(t.L(), perturbationDuration)

	t.Status("T4: recovery from the perturbation")
	afterDuration := v.perturbation.endPerturbation(ctx, t.L(), v)
	afterStats := lm.worstStats(t.L(), afterDuration)

	t.L().Printf("Fill stats %s", shortString(fillStats))
	t.L().Printf("Baseline stats %s", shortString(baselineStats))
	t.L().Printf("Stats during perturbation: %s", shortString(perturbationStats))
	t.L().Printf("Stats after perturbation: %s", shortString(afterStats))

	t.Status("T5: validating results")
	// Stop all the running threads and wait for them.
	r.Close()
	cancelReporter()
	cancelWorkload()
	m.Wait()
	if err := downloadProfiles(ctx, c, t.L(), t.ArtifactsDir()); err != nil {
		t.Fatal(err)
	}

	t.L().Printf("validating stats during the perturbation")
	duringOK := isAcceptableChange(t.L(), baselineStats, perturbationStats)
	t.L().Printf("validating stats after the perturbation")
	afterOK := isAcceptableChange(t.L(), baselineStats, afterStats)

	if !duringOK || !afterOK {
		t.Fatal("unacceptable increase in latency from the perturbation")
	}
}

// Track all histograms for up to one hour in the past.
const numOpsToTrack = 3600

// latencyMonitor tracks the latency of operations ver a period of time. It has
// a fixed number of operations to track and rolls the previous operations into
// the buffer.
type latencyMonitor struct {
	statNames []string
	// track the relevant stats for the last N operations in a circular buffer.
	mu struct {
		syncutil.Mutex
		index   int
		tracker map[string]*[numOpsToTrack]*hdrhistogram.Histogram
	}
}

func newLatencyMonitor(statNames []string) *latencyMonitor {
	lm := latencyMonitor{}
	lm.statNames = statNames
	lm.mu.tracker = make(map[string]*[numOpsToTrack]*hdrhistogram.Histogram)
	for _, name := range lm.statNames {
		lm.mu.tracker[name] = &[numOpsToTrack]*hdrhistogram.Histogram{}
	}
	return &lm
}

// mergeStats captures the average ver the relevant stats for the last N
// operations.
func (lm *latencyMonitor) mergeStats(window time.Duration) map[string]*hdrhistogram.Histogram {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// startIndex is always between 0 and numOpsToTrack - 1.
	startIndex := (lm.mu.index + numOpsToTrack - int(window.Seconds())) % numOpsToTrack
	// endIndex is always greater than startIndex.
	endIndex := (lm.mu.index + numOpsToTrack) % numOpsToTrack
	if endIndex < startIndex {
		endIndex += numOpsToTrack
	}

	stats := make(map[string]*hdrhistogram.Histogram)
	for name, stat := range lm.mu.tracker {
		s := stat[startIndex]
		for i := startIndex + 1; i < endIndex; i++ {
			s.Merge(stat[i%numOpsToTrack])
		}
		stats[name] = s
	}
	return stats
}

// worstStats captures the worst case ver the relevant stats for the last N
func (lm *latencyMonitor) worstStats(
	logger *logger.Logger, window time.Duration,
) map[string]*hdrhistogram.Histogram {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	stats := make(map[string]*hdrhistogram.Histogram)
	// startIndex is always between 0 and numOpsToTrack - 1.
	startIndex := (lm.mu.index + numOpsToTrack - int(window.Seconds())) % numOpsToTrack
	// endIndex is always greater than startIndex.
	endIndex := (lm.mu.index + numOpsToTrack) % numOpsToTrack
	if endIndex < startIndex {
		endIndex += numOpsToTrack
	}

	for name, stat := range lm.mu.tracker {
		s := stat[startIndex]
		for i := startIndex + 1; i < endIndex; i++ {
			if isWorse(logger, stat[i%numOpsToTrack], s) {
				s = stat[i%numOpsToTrack]
			}
		}
		stats[name] = s
	}
	return stats
}

const acceptableCountDecrease = 0.1
const acceptableP50Increase = 1.0
const acceptableP99Increase = 1.0
const acceptableP999Increase = 3.0 // this can have higher variations and not part of what we are trying to solve short term.
// minAcceptableLatencyThreshold is the threshold below which we consider the
// latency acceptable regardless of any relative change from the baseline.
const minAcceptableLatencyThreshold = 10 * time.Millisecond

// isAcceptableChange determines if a change from the baseline is acceptable.
// An acceptable change is defined as the following:
//  1. Num ops/second is within 10% of the baseline.
//  2. The P50th percentile latency is within 4x of the baseline.
//  3. The P99th percentile latency is within 2x of the baseline.
//  4. The P99.9th percentile latency is within 2x of the baseline.
//  5. OR the p50/99/99.9th percentile latencies are below 10ms, in which case
//     the latency relative to the baseline is ignored.
//
// It compares all the metrics rather than failing fast. Normally multiple
// metrics will fail at once if a test is going to fail and it is helpful to see
// all the differences.
func isAcceptableChange(
	logger *logger.Logger, baseline, other map[string]*hdrhistogram.Histogram,
) bool {
	// This can happen if we aren't measuring one of the phases.
	if len(other) == 0 {
		return true
	}
	allPassed := true
	for name, baseStat := range baseline {
		otherStat := other[name]
		if float64(otherStat.TotalCount()) < float64(baseStat.TotalCount())*(1-acceptableCountDecrease) {
			logger.Printf("%s: qps decreased from %d to %d", name, baseStat.TotalCount(), otherStat.TotalCount())
			allPassed = false
		}
		if !isAcceptableLatencyChange(baseStat, otherStat, 50 /* p50 */, acceptableP50Increase) {
			logger.Printf("%s: P50 increased from %s to %s", name, time.Duration(baseStat.ValueAtQuantile(50)), time.Duration(otherStat.ValueAtQuantile(50)))
			allPassed = false
		}
		if !isAcceptableLatencyChange(baseStat, otherStat, 99 /* p99 */, acceptableP99Increase) {
			logger.Printf("%s: P99 increased from %s to %s", name, time.Duration(baseStat.ValueAtQuantile(99)), time.Duration(otherStat.ValueAtQuantile(99)))
			allPassed = false
		}
		if !isAcceptableLatencyChange(baseStat, otherStat, 99.9 /* p99.9 */, acceptableP999Increase) {
			logger.Printf("%s: P99.9 increased from %s to %s", name, time.Duration(baseStat.ValueAtQuantile(99.9)), time.Duration(otherStat.ValueAtQuantile(99.9)))
			allPassed = false
		}
	}
	return allPassed
}

// isAcceptableLatencyChange determines if a change in latency, between change
// and baseline, is acceptable.
func isAcceptableLatencyChange(
	base, change *hdrhistogram.Histogram, p, relativeThreshold float64,
) bool {
	changeLatency := time.Duration(change.ValueAtQuantile(p))
	latencyThreshold := time.Duration(float64(base.ValueAtQuantile(p)) * (1 + relativeThreshold))
	return changeLatency <= latencyThreshold || changeLatency < minAcceptableLatencyThreshold
}

// isWorse returns true if a is significantly worse than b. Note that this is
// not symmetric, and will only return true if a is enough worse.
// TODO(baptist): Consider alternative ways to do this comparison.
func isWorse(logger *logger.Logger, a, b *hdrhistogram.Histogram) bool {
	// Allow some delta on counts to handle variations on the total and the mean latency.
	delta := 0.95
	// Stat a is worse if its total count is significantly lower.
	if float64(a.TotalCount()) < float64(b.TotalCount())*delta {
		return true
	}
	// Stat a is worse if its mean latency is significantly higher.
	if float64(a.ValueAtQuantile(50))*delta > float64(b.ValueAtQuantile(50)) {
		return true
	}
	// If the total and the value are close, then the 99.9th percentile is the
	// deciding factor. Don't multiply either by delta for this stat.
	if float64(a.ValueAtQuantile(99.9)) > float64(b.ValueAtQuantile(99.9)) {
		return true
	}
	// Stat a is not significantly worse.
	return false
}

// start the latency monitor which will run until the context is cancelled.
func (lm *latencyMonitor) start(
	ctx context.Context, l *logger.Logger, r *histogram.UdpReceiver,
) error {
outer:
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			histograms := r.Tick()
			sMap := make(map[string]*hdrhistogram.Histogram)

			// Pull out the relevant data from the stats.
			for _, name := range lm.statNames {
				if histograms[name] == nil {
					l.Printf("no histogram for %s, %v", name, histograms)
					continue outer
				}
				sMap[name] = histograms[name]
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

func shortString(sMap map[string]*hdrhistogram.Histogram) string {
	var outputStr strings.Builder
	var count int64
	for name, hist := range sMap {
		outputStr.WriteString(fmt.Sprintf("%s: %s, ", name, time.Duration(hist.ValueAtQuantile(99))))
		count += hist.TotalCount()
	}
	outputStr.WriteString(fmt.Sprintf("op/s: %d", count))
	return outputStr.String()
}

// startNoBackup starts the nodes without enabling backup.
func (v variations) startNoBackup(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) {
	nodesPerRegion := v.crdbNodes / NUM_REGIONS
	for _, node := range nodes {
		// Don't start a backup schedule because this test is timing sensitive.
		opts := option.NewStartOpts(option.NoBackupSchedule)
		opts.RoachprodOpts.StoreCount = v.disks
		opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--locality=region=fake-%d", (node-1)/nodesPerRegion))
		v.cluster.Start(ctx, l, opts, install.MakeClusterSettings(), v.cluster.Node(node))
	}
}

// TODO(baptist): Parameterize the workload to allow running TPCC.
func (v variations) initWorkload(ctx context.Context, l *logger.Logger) {
	initCmd := fmt.Sprintf("./cockroach workload init kv --splits %d {pgurl:1}", v.splits)
	v.cluster.Run(ctx, option.WithNodes(v.cluster.Node(v.workloadNode)), initCmd)

	db := v.cluster.Conn(ctx, l, 1)
	defer db.Close()

	// Wait for rebalancing to finish before starting to fill. This minimizes the time to finish.
	if err := waitForRebalanceToStop(ctx, l, v.cluster); err != nil {
		panic(err)
	}
}

// Don't run a workload against the node we're going to shut down.
func (v variations) runWorkload(ctx context.Context, duration time.Duration, maxRate int) error {
	runCmd := fmt.Sprintf(
		"./cockroach workload run kv --duration=%s --max-rate=%d --tolerate-errors --max-block-bytes=%d --read-percent=50 --follower-read-percent=50 --concurrency=500 --operation-receiver=%s:%d {pgurl:1-%d}",
		duration, maxRate, v.maxBlockBytes, v.roachtestAddr, v.histogramPort, v.stableNodes)
	return v.cluster.RunE(ctx, option.WithNodes(v.cluster.Node(v.workloadNode)), runCmd)
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
