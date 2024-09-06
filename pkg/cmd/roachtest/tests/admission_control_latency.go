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
	"reflect"
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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/cli"
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
	// cluster is set up at the start of the test run
	cluster.Cluster

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
	cloud                registry.CloudSet
}

const NUM_REGIONS = 3

var durationOptions = []time.Duration{10 * time.Second, 10 * time.Minute, 30 * time.Minute}
var splitOptions = []int{1, 100, 10000}
var maxBlockBytes = []int{1, 1024, 4096}
var numNodes = []int{5, 12, 30}
var numVCPUs = []int{4, 8, 16, 32}
var numDisks = []int{1, 2}
var cloudSets = []registry.CloudSet{registry.OnlyAWS, registry.OnlyGCE, registry.OnlyAzure}

var leases = []registry.LeaseType{
	registry.EpochLeases,
	registry.ExpirationLeases,
}

func (v variations) String() string {
	return fmt.Sprintf("seed: %d, fillDuration: %s, maxBlockBytes: %d, perturbationDuration: %s, "+
		"validationDuration: %s, ratioOfMax: %f, splits: %d, numNodes: %d, numWorkloadNodes: %d, "+
		"partitionSite: %t, vcpu: %d, disks: %d, leaseType: %s, cloud: %v",
		v.seed, v.fillDuration, v.maxBlockBytes,
		v.perturbationDuration, v.validationDuration, v.ratioOfMax, v.splits, v.numNodes, v.numWorkloadNodes,
		v.partitionSite, v.vcpu, v.disks, v.leaseType, v.cloud)
}

// Normally a single worker can handle 20-40 nodes. If we find this is
// insufficient we can bump it up.
const numNodesPerWorker = 20

// setup sets up the parameters that can be manually set up without having the
// cluster spec. finishSetup below sets up the remainder after the cluster is
// defined.
func setupMetamorphic(p perturbation) variations {
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
	v.cloud = cloudSets[rng.Intn(len(cloudSets))]
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
	v.cloud = registry.OnlyGCE
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
	v.cloud = registry.AllClouds
	v.perturbation = p
	return v
}

func registerLatencyTests(r registry.Registry) {
	// NB: If these tests fail because they are flaky, increase the numbers
	// until they pass. Additionally add the seed (from the log) that caused
	// them to fail as a comment in the test.
	addMetamorphic(r, restart{}, 1000)
	addMetamorphic(r, partition{}, 1000)
	addMetamorphic(r, addNode{}, 2.0)
	addMetamorphic(r, decommission{}, 2.0)

	// NB: If these tests fail, it likely signals a regression. Investigate the
	// history of the test on roachperf to see what changed.
	addFull(r, restart{}, 1000)
	addFull(r, partition{}, 1000)
	addFull(r, addNode{}, 2.0)
	addFull(r, decommission{}, 2.0)

	// NB: These tests will never fail and are not enabled, but they are useful
	// for development.
	addDev(r, restart{}, math.Inf(1))
	addDev(r, partition{}, math.Inf(1))
	addDev(r, addNode{}, math.Inf(1))
	addDev(r, decommission{}, math.Inf(1))
}

func (v variations) makeClusterSpec() spec.ClusterSpec {
	// NB: We use low memory to force non-cache disk reads earlier.
	return spec.MakeClusterSpec(v.numNodes+v.numWorkloadNodes, spec.CPU(v.vcpu), spec.SSD(v.disks), spec.Mem(spec.Low))
}

func addMetamorphic(r registry.Registry, p perturbation, acceptableChange float64) {
	v := setupMetamorphic(p)
	v.acceptableChange = acceptableChange
	// TODO(baptist): Make the cloud be metamorphic for repeatable results with
	// a given seed.
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/metamorphic/%s", reflect.TypeOf(p).Name()),
		CompatibleClouds: v.cloud,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerKV,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Randomized:       true,
		Run:              v.runTest,
	})
}

func addFull(r registry.Registry, p perturbation, acceptableChange float64) {
	v := setupFull(p)
	v.acceptableChange = acceptableChange
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/full/%s", reflect.TypeOf(p).Name()),
		CompatibleClouds: v.cloud,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerKV,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Run:              v.runTest,
	})
}

func addDev(r registry.Registry, p perturbation, acceptableChange float64) {
	v := setupDev(p)
	v.acceptableChange = acceptableChange
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/dev/%s", reflect.TypeOf(p).Name()),
		CompatibleClouds: v.cloud,
		Suites:           registry.ManualOnly,
		Owner:            registry.OwnerKV,
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
	v.Stop(ctx, l, gracefulOpts, v.targetNodes())
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
	targetIPs, err := v.InternalIP(ctx, l, v.targetNodes())
	if err != nil {
		panic(err)
	}

	if !v.IsLocal() {
		v.Run(
			ctx,
			v.withPartitionedNodes(v),
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
	if !v.IsLocal() {
		v.Run(ctx, v.withPartitionedNodes(v), `sudo iptables -F`)
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
	waitForRebalanceToStop(ctx, l, v)
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
		v.Run(ctx, option.WithNodes(v.Node(node)), drainCmd)
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
		v.Run(ctx, option.WithNodes(v.Node(node)), decommissionCmd)
	}

	l.Printf("stopping decommissioned nodes")
	v.Stop(ctx, l, option.DefaultStopOpts(), v.targetNodes())
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

// interval is a time interval.
type interval struct {
	start time.Time
	end   time.Time
}

func (i interval) String() string {
	return fmt.Sprintf("%s -> %s", i.start, i.end)
}

func intervalSince(d time.Duration) interval {
	now := timeutil.Now()
	return interval{start: now.Add(-d), end: now}
}

type workloadData struct {
	score scoreCalculator
	data  map[string]map[time.Time]trackedStat
}

func (w workloadData) String() string {
	var outputStr strings.Builder
	for name, stats := range w.data {
		outputStr.WriteString(name + "\n")
		keys := sortedTimeKeys(stats)
		for _, key := range keys {
			outputStr.WriteString(fmt.Sprintf("%s: %s\n", key, stats[key]))
		}
	}
	return outputStr.String()
}

func sortedTimeKeys(stats map[time.Time]trackedStat) []time.Time {
	keys := make([]time.Time, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Before(keys[j])
	})
	return keys
}

func (w workloadData) addTicks(ticks []cli.Tick) {
	for _, tick := range ticks {
		if _, ok := w.data[tick.Type]; !ok {
			w.data[tick.Type] = make(map[time.Time]trackedStat)
		}
		stat := w.convert(tick)
		if _, ok := w.data[tick.Type][tick.Time]; ok {
			w.data[tick.Type][tick.Time] = w.data[tick.Type][tick.Time].merge(stat, w.score)
		} else {
			w.data[tick.Type][tick.Time] = stat
		}
	}
}

// calculateScore calculates the score for a given tick. It can be interpreted
// as the single core operation latency.
func (v variations) calculateScore(t cli.Tick) time.Duration {
	if t.Throughput == 0 {
		// Use a non-infinite score that is still very high if there was a period of no throughput.
		return time.Hour
	}
	return time.Duration(math.Sqrt((float64(v.numNodes*v.vcpu) * float64((t.P50+t.P99)/2)) / t.Throughput * float64(time.Second)))
}

func (w workloadData) convert(t cli.Tick) trackedStat {
	// Align to the second boundary to make the stats from different nodes overlap.
	t.Time = t.Time.Truncate(time.Second)
	return trackedStat{Tick: t, score: w.score(t)}
}

// worstStats returns the worst stats for a given interval for each of the
// tracked data types.
func (w workloadData) worstStats(i interval) map[string]trackedStat {
	m := make(map[string]trackedStat)
	for name, stats := range w.data {
		for time, stat := range stats {
			if time.After(i.start) && time.Before(i.end) {
				if cur, ok := m[name]; ok {
					if stat.score > cur.score {
						m[name] = stat
					}
				} else {
					m[name] = stat
				}
			}
		}
	}
	return m
}

// runTest is the main entry point for all the tests. Its ste
func (v variations) runTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	v.Cluster = c
	t.L().Printf("test variations are: %+v", v)
	t.Status("T0: starting nodes")

	// Track the three operations that we are sending in this test.
	m := c.NewMonitor(ctx, v.stableNodes())

	// Start the stable nodes and let the perturbation start the target node(s).
	v.startNoBackup(ctx, t.L(), v.stableNodes())
	v.perturbation.startTargetNode(ctx, t.L(), v)

	func() {
		// TODO(baptist): Remove this block once #120073 is fixed.
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		if _, err := db.ExecContext(ctx,
			`SET CLUSTER SETTING kv.lease.reject_on_leader_unknown.enabled = true`); err != nil {
			t.Fatal(err)
		}
		// This isn't strictly necessary, but it would be nice if this test passed at 10s (or lower).
		if _, err := db.ExecContext(ctx,
			`SET CLUSTER SETTING server.time_after_store_suspect = '10s'`); err != nil {
			t.Fatal(err)
		}
		// Avoid stores up-replicating away from the target node, reducing the
		// backlog of work.
		if _, err := db.ExecContext(
			ctx,
			fmt.Sprintf(
				`SET CLUSTER SETTING server.time_until_store_dead = '%s'`, v.perturbationDuration+time.Minute)); err != nil {
			t.Fatal(err)
		}
	}()

	// Wait for rebalancing to finish before starting to fill. This minimizes the time to finish.
	waitForRebalanceToStop(ctx, t.L(), v)
	require.NoError(t, v.workload.initWorkload(ctx, v))

	// Capture the stable rate near the last 1/4 of the fill process.
	clusterMaxRate := make(chan int)
	m.Go(func(ctx context.Context) error {
		waitDuration(ctx, 3*v.fillDuration/4)
		clusterMaxRate <- v.measureQPS(ctx, t.L(), v.fillDuration/4)
		return nil
	})
	// Start filling the system without a rate.
	t.Status("T1: filling at full rate")
	_, err := v.workload.runWorkload(ctx, v, v.fillDuration, 0)
	require.NoError(t, err)

	// Start the consistent workload and begin collecting profiles.
	stableRatePerNode := int(float64(<-clusterMaxRate) * v.ratioOfMax / float64(v.numWorkloadNodes))
	t.Status("T2: running workload at stable rate")
	var data *workloadData
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		if data, err = v.workload.runWorkload(ctx, v, 0, stableRatePerNode); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	// Begin profiling halfway through the workload.
	waitDuration(ctx, v.validationDuration/2)
	require.NoError(t, profileTopStatements(ctx, c, t.L(), "target"))
	// Let enough data be written to all nodes in the cluster, then induce the perturbation.
	waitDuration(ctx, v.validationDuration/2)

	// Collect the baseline after the workload has stabilized.
	baselineInterval := intervalSince(v.validationDuration / 2)
	// Now start the perturbation.
	t.Status("T3: inducing perturbation")
	perturbationDuration := v.perturbation.startPerturbation(ctx, t.L(), v)
	perturbationInterval := intervalSince(perturbationDuration)

	t.Status("T4: recovery from the perturbation")
	afterDuration := v.perturbation.endPerturbation(ctx, t.L(), v)
	afterInterval := intervalSince(afterDuration)

	t.L().Printf("Baseline interval     : %s", baselineInterval)
	t.L().Printf("Perturbation interval : %s", perturbationInterval)
	t.L().Printf("Recovery interval     : %s", afterInterval)

	cancelWorkload()
	require.NoError(t, m.WaitE())

	baselineStats := data.worstStats(baselineInterval)
	perturbationStats := data.worstStats(perturbationInterval)
	afterStats := data.worstStats(afterInterval)

	t.L().Printf("%s\n", prettyPrint("Baseline stats", baselineStats))
	t.L().Printf("%s\n", prettyPrint("Perturbation stats", perturbationStats))
	t.L().Printf("%s\n", prettyPrint("Recovery stats", afterStats))

	t.Status("T5: validating results")

	// collect files
	// check intervals
	require.NoError(t, downloadProfiles(ctx, c, t.L(), t.ArtifactsDir()))

	t.L().Printf("validating stats during the perturbation")
	duringOK := isAcceptableChange(t.L(), baselineStats, perturbationStats, v.acceptableChange)
	t.L().Printf("validating stats after the perturbation")
	afterOK := isAcceptableChange(t.L(), baselineStats, afterStats, v.acceptableChange)
	require.True(t, duringOK && afterOK)
}

// trackedStat is a collection of the relevant values from the histogram. The
// score is a unitless composite measure representing the throughput and latency
// of a histogram. Lower scores are better.
type trackedStat struct {
	cli.Tick
	score time.Duration
}

type scoreCalculator func(cli.Tick) time.Duration

func (t trackedStat) String() string {
	return fmt.Sprintf("%s: score: %s, qps: %d, p50: %s, p99: %s, pMax: %s",
		t.Time, t.score, int(t.Throughput), t.P50, t.P99, t.PMax)
}

// merge two stats together. Note that this isn't really a merge of the P99, but
// the other merges are fairly accurate.
func (t trackedStat) merge(o trackedStat, c scoreCalculator) trackedStat {
	tick := cli.Tick{
		Time:       t.Time,
		Throughput: t.Throughput + o.Throughput,
		P50:        (t.P50 + o.P50) / 2,
		P99:        (t.P99 + o.P99) / 2,
		PMax:       max(t.PMax, o.PMax),
	}
	return trackedStat{
		Tick:  tick,
		score: max(t.score, o.score),
	}
}

// minAcceptableLatencyThreshold is the threshold below which we consider the
// latency acceptable regardless of any relative change from the baseline.
const minAcceptableLatencyThreshold = 10 * time.Millisecond

// isAcceptableChange determines if a change from the baseline is acceptable.
// It compares all the metrics rather than failing fast. Normally multiple
// metrics will fail at once if a test is going to fail and it is helpful to see
// all the differences.
func isAcceptableChange(
	logger *logger.Logger, baseline, other map[string]trackedStat, acceptableChange float64,
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
		if float64(otherStat.P99) > float64(minAcceptableLatencyThreshold) && increase > acceptableChange {
			logger.Printf("FAILURE: %-15s: Increase %.4f > %.4f BASE: %v SCORE: %v\n", name, increase, acceptableChange, baseStat.score, otherStat.score)
			allPassed = false
		} else {
			logger.Printf("PASSED : %-15s: Increase %.4f <= %.4f BASE: %v SCORE: %v\n", name, increase, acceptableChange, baseStat.score, otherStat.score)
		}
	}
	return allPassed
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
		v.Start(ctx, l, opts, install.MakeClusterSettings(), v.Node(node))
	}
}

func (v variations) workloadNodes() option.NodeListOption {
	return v.Range(v.numNodes+1, v.numNodes+v.numWorkloadNodes)
}

func (v variations) stableNodes() option.NodeListOption {
	return v.Range(1, v.numNodes-1)
}

// Note this is always only a single node today. If we have perturbations that
// require multiple targets we could add multi-target support.
func (v variations) targetNodes() option.NodeListOption {
	return v.Node(v.numNodes)
}

// measureQPS will measure the approx QPS at the time this command is run. The
// duration is the interval to measure over. Setting too short of an interval
// can mean inaccuracy in results. Setting too long of an interval may mean the
// impact is blurred out.
func (v variations) measureQPS(ctx context.Context, l *logger.Logger, duration time.Duration) int {
	stableNodes := v.stableNodes()

	totalOpsCompleted := func() int {
		// NB: We can't hold the connection open during the full duration.
		var dbs []*gosql.DB
		for _, nodeId := range stableNodes {
			db := v.Conn(ctx, l, nodeId)
			defer db.Close()
			dbs = append(dbs, db)
		}
		rates := make(chan int, len(dbs))
		// Count the inserts before sleeping.
		for _, db := range dbs {
			db := db
			go func() {
				var v float64
				if err := db.QueryRowContext(
					ctx, `SELECT sum(value) FROM crdb_internal.node_metrics WHERE name in ('sql.select.count', 'sql.insert.count')`,
				).Scan(&v); err != nil {
					panic(err)
				}
				rates <- int(v)
			}()
		}
		var total int
		for range dbs {
			total += <-rates
		}
		return total
	}

	// Measure the current time and the QPS now.
	startTime := timeutil.Now()
	beforeOps := totalOpsCompleted()
	// Wait for the duration minus the first query time.
	select {
	case <-ctx.Done():
		return 0
	case <-time.After(duration - timeutil.Since(startTime)):
		afterOps := totalOpsCompleted()
		return int(float64(afterOps-beforeOps) / duration.Seconds())
	}
}

type workloadType interface {
	operations() []string
	initWorkload(ctx context.Context, v variations) error
	runWorkload(ctx context.Context, v variations, duration time.Duration, maxRate int) (*workloadData, error)
}

type kvWorkload struct{}

func (w kvWorkload) operations() []string {
	return []string{"write", "read", "follower-read"}
}

func (w kvWorkload) initWorkload(ctx context.Context, v variations) error {
	initCmd := fmt.Sprintf("./cockroach workload init kv --db target --splits %d {pgurl:1}", v.splits)
	return v.RunE(ctx, option.WithNodes(v.Node(1)), initCmd)
}

// Don't run a workload against the node we're going to shut down.
func (w kvWorkload) runWorkload(
	ctx context.Context, v variations, duration time.Duration, maxRate int,
) (*workloadData, error) {
	runCmd := fmt.Sprintf(
		"./cockroach workload run kv --db target --display-format=incremental-json --duration=%s --max-rate=%d --tolerate-errors --max-block-bytes=%d --read-percent=50 --follower-read-percent=50 --concurrency=500 {pgurl%s}",
		duration, maxRate, v.maxBlockBytes, v.stableNodes())
	allOutput, err := v.RunWithDetails(ctx, nil, option.WithNodes(v.workloadNodes()), runCmd)
	if err != nil {
		return nil, err
	}
	wd := workloadData{
		score: v.calculateScore,
		data:  make(map[string]map[time.Time]trackedStat),
	}
	for _, output := range allOutput {
		stdout := output.Stdout
		ticks := cli.ParseOutput(strings.NewReader(stdout))
		wd.addTicks(ticks)
	}
	return &wd, nil
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
