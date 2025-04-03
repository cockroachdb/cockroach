// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/cli"
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
type variations struct {
	// cluster is set up at the start of the test run.
	cluster.Cluster

	// These fields are set up during construction.
	seed                 int64
	fillDuration         time.Duration
	blockSize            int
	perturbationDuration time.Duration
	validationDuration   time.Duration
	ratioOfMax           float64
	splits               int
	numNodes             int
	numWorkloadNodes     int
	vcpu                 int
	disks                int
	mem                  spec.MemPerCPU
	leaseType            registry.LeaseType
	perturbation         perturbation
	workload             workloadType
	acceptableChange     float64
	cloud                registry.CloudSet
	acMode               admissionControlMode
	diskBandwidthLimit   string
	profileOptions       []roachtestutil.ProfileOptionFunc
	specOptions          []spec.Option
	clusterSettings      map[string]string
}

const NUM_REGIONS = 3

var durationOptions = []time.Duration{10 * time.Second, 10 * time.Minute, 30 * time.Minute}
var splitOptions = []int{1, 100, 10000}
var blockSize = []int{1, 1024, 4096}
var numNodes = []int{5, 12, 30}
var numVCPUs = []int{4, 8, 16, 32}
var numDisks = []int{1, 2}
var memOptions = []spec.MemPerCPU{spec.Low, spec.Standard, spec.High}
var cloudSets = []registry.CloudSet{registry.OnlyAWS, registry.OnlyGCE, registry.OnlyAzure}
var admissionControlOptions = []admissionControlMode{elasticOnlyBoth, fullNormalElasticRepl, fullBoth}
var diskBandwidthLimitOptions = []string{"0", "350MiB"}

var leases = []registry.LeaseType{
	registry.EpochLeases,
	registry.LeaderLeases,
	registry.ExpirationLeases,
}

type admissionControlMode int

const (
	// defaultOption uses the releases default settings for admission control.
	defaultOption = admissionControlMode(iota)
	// disabled fully disables admission control.
	disabled
	// elasticOnlyNoRepl applies normal admission control to elastic traffic, no
	// replication admission control.
	elasticOnlyNoRepl
	// elasticOnlyBoth applies normal admission control to elastic traffic, and
	// elastic replication admission control.
	elasticOnlyBoth
	// fullNormalElasticRepl applies normal admission control to elastic
	// traffic, and elastic replication admission control.
	fullNormalElasticRepl
	// fullBoth applies admission control to all elastic and normal traffic.
	fullBoth
)

func (a admissionControlMode) String() string {
	switch a {
	case disabled:
		return "none"
	case elasticOnlyNoRepl:
		return "elasticOnlyNoRepl"
	case elasticOnlyBoth:
		return "elasticOnlyBoth"
	case fullNormalElasticRepl:
		return "fullNormalElasticRepl"
	case fullBoth:
		return "fullBoth"
	case defaultOption:
		return "defaultOption"
	default:
		return "unknown"
	}
}

func (a admissionControlMode) getSettings() map[string]string {
	switch a {
	case disabled:
		return map[string]string{
			"admission.kv.enabled":             "false",
			"kvadmission.flow_control.enabled": "false",
		}
	case elasticOnlyNoRepl:
		return map[string]string{
			"admission.kv.bulk_only.enabled":   "true",
			"kvadmission.flow_control.enabled": "false",
		}
	case elasticOnlyBoth:
		return map[string]string{
			"admission.kv.bulk_only.enabled": "true",
			"kvadmission.flow_control.mode":  "apply_to_elastic",
		}
	case fullNormalElasticRepl:
		return map[string]string{
			"kvadmission.flow_control.mode": "apply_to_elastic",
		}
	case fullBoth:
		return map[string]string{
			"kvadmission.flow_control.mode": "apply_to_all",
		}
	default:
		return map[string]string{}
	}
}

// getParameterMap returns a map of the parameters used for the test in
// stringified format. They can be used in logging to more easily identify the
// test reproduction.
func (v variations) getParameterMap() map[string]string {
	params := map[string]string{}
	params["seed"] = strconv.FormatInt(v.seed, 10)
	params["fillDuration"] = v.fillDuration.String()
	params["blockSize"] = strconv.Itoa(v.blockSize)
	params["perturbationDuration"] = v.perturbationDuration.String()
	params["validationDuration"] = v.validationDuration.String()
	params["ratioOfMax"] = strconv.FormatFloat(v.ratioOfMax, 'f', -1, 64)
	params["splits"] = strconv.Itoa(v.splits)
	params["numNodes"] = strconv.Itoa(v.numNodes)
	params["numWorkloadNodes"] = strconv.Itoa(v.numWorkloadNodes)
	params["vcpu"] = strconv.Itoa(v.vcpu)
	params["disks"] = strconv.Itoa(v.disks)
	params["mem"] = v.mem.String()
	params["leaseType"] = v.leaseType.String()
	params["cloud"] = v.cloud.String()
	params["acMode"] = v.acMode.String()
	params["diskBandwidthLimit"] = v.diskBandwidthLimit
	return params
}

// Normally a single worker can handle 20-40 nodes. If we find this is
// insufficient we can bump it up.
const numNodesPerWorker = 20

// randomize will randomize the test parameters for a metamorphic run.
func (v variations) randomize(rng *rand.Rand) variations {
	v.splits = splitOptions[rng.Intn(len(splitOptions))]
	v.blockSize = blockSize[rng.Intn(len(blockSize))]
	v.perturbationDuration = durationOptions[rng.Intn(len(durationOptions))]
	v.leaseType = leases[rng.Intn(len(leases))]
	v.numNodes = numNodes[rng.Intn(len(numNodes))]
	v.numWorkloadNodes = v.numNodes/numNodesPerWorker + 1
	v.vcpu = numVCPUs[rng.Intn(len(numVCPUs))]
	v.disks = numDisks[rng.Intn(len(numDisks))]
	v.cloud = cloudSets[rng.Intn(len(cloudSets))]
	// TODO(baptist): Temporarily disable the metamorphic tests on other clouds
	// as they have limitations on configurations that can run.
	v.cloud = registry.OnlyGCE
	v.mem = memOptions[rng.Intn(len(memOptions))]
	v.acMode = admissionControlOptions[rng.Intn(len(admissionControlOptions))]
	v.diskBandwidthLimit = diskBandwidthLimitOptions[rng.Intn(len(diskBandwidthLimitOptions))]
	return v
}

// setup sets up the full test with a fixed set of parameters.
func setup(p perturbation, acceptableChange float64) variations {
	v := variations{}
	v.workload = kvWorkload{}
	v.leaseType = registry.EpochLeases
	v.blockSize = 4096
	v.splits = 10000
	v.numNodes = 12
	v.numWorkloadNodes = v.numNodes/numNodesPerWorker + 1
	v.vcpu = 16
	v.disks = 2
	v.fillDuration = 10 * time.Minute
	v.validationDuration = 5 * time.Minute
	v.perturbationDuration = 10 * time.Minute
	v.ratioOfMax = 0.5
	v.cloud = registry.OnlyGCE
	v.mem = spec.Standard
	v.diskBandwidthLimit = "0"
	v.perturbation = p
	v.profileOptions = []roachtestutil.ProfileOptionFunc{
		roachtestutil.ProfDbName("target"),
		roachtestutil.ProfMinimumLatency(30 * time.Millisecond),
		roachtestutil.ProfMinNumExpectedStmts(1000),
		roachtestutil.ProfProbabilityToInclude(0.001),
		roachtestutil.ProfMultipleFromP99(10),
	}
	v.acceptableChange = acceptableChange
	v.clusterSettings = make(map[string]string)
	// Having the io_load_listener logs makes it easier to debug failures.
	v.clusterSettings["server.debug.default_vmodule"] = "io_load_listener=1"
	return v
}

func register(r registry.Registry, p perturbation) {
	// Metamorphic perturbation tests are currently disabled. See
	// https://github.com/cockroachdb/cockroach/issues/142148.
	// addMetamorphic(r, p)
	addFull(r, p)
	addDev(r, p)
}

func RegisterTests(r registry.Registry) {
	// NB: If these tests fail because they are flaky, increase the numbers
	// until they pass. Additionally add the seed (from the log) that caused
	// them to fail as a comment in the test.
	register(r, restart{})
	register(r, partition{})
	register(r, addNode{})
	register(r, decommission{})
	register(r, backfill{})
	register(r, &slowDisk{})
	register(r, elasticWorkload{})
	register(r, intents{})
	register(r, backup{})
}

func (v variations) makeClusterSpec() spec.ClusterSpec {
	opts := append(v.specOptions, spec.CPU(v.vcpu), spec.SSD(v.disks), spec.Mem(v.mem), spec.TerminateOnMigration())
	return spec.MakeClusterSpec(v.numNodes+v.numWorkloadNodes, opts...)
}

func (v variations) perturbationName() string {
	t := reflect.TypeOf(v.perturbation)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// finishSetup completes initialization of the variations.
func (v variations) finishSetup() variations {
	// Apply any environment variable overrides first.
	if overrides, found := os.LookupEnv("PERTURBATION_OVERRIDE"); found {
		for _, override := range strings.Split(overrides, ",") {
			parts := strings.Split(override, "=")
			if err := v.applyEnvOverride(parts[0], parts[1]); err != nil {
				panic(fmt.Sprintf("can't apply override: %s: %v", override, err))
			}
		}
	}

	for k, val := range v.acMode.getSettings() {
		v.clusterSettings[k] = val
	}
	// Enable raft tracing. Remove this once raft tracing is the default.
	v.clusterSettings["kv.raft.max_concurrent_traces"] = "10"
	v.clusterSettings["kvadmission.store.provisioned_bandwidth"] = v.diskBandwidthLimit
	return v
}

// applyEnvOverride applies a single override to the test.
func (v *variations) applyEnvOverride(key string, val string) (err error) {
	switch key {
	case "fillDuration":
		v.fillDuration, err = time.ParseDuration(val)
	case "blockSize":
		v.blockSize, err = strconv.Atoi(val)
	case "perturbationDuration":
		v.perturbationDuration, err = time.ParseDuration(val)
	case "validationDuration":
		v.validationDuration, err = time.ParseDuration(val)
	case "ratioOfMax":
		v.ratioOfMax, err = strconv.ParseFloat(val, 64)
	case "splits":
		v.splits, err = strconv.Atoi(val)
	case "numNodes":
		v.numNodes, err = strconv.Atoi(val)
	case "numWorkloadNodes":
		v.numWorkloadNodes, err = strconv.Atoi(val)
	case "vcpu":
		v.vcpu, err = strconv.Atoi(val)
	case "disks":
		v.disks, err = strconv.Atoi(val)
	case "mem":
		v.mem = spec.ParseMemCPU(val)
		if v.mem == -1 {
			err = errors.Errorf("unknown memory setting: %s", val)
		}
	case "diskBandwidthLimit":
		v.diskBandwidthLimit = val
	case "leaseType":
		for _, l := range leases {
			if l.String() == val {
				v.leaseType = l
				return nil
			}
		}
		return errors.Errorf("unknown lease type: %s", val)
	case "cloud":
		for _, c := range cloudSets {
			if c.String() == val {
				v.cloud = c
				return nil
			}
		}
		return errors.Errorf("unknown cloud: %s", val)
	case "acMode":
		for _, a := range admissionControlOptions {
			if a.String() == val {
				v.acMode = a
				return nil
			}
		}
		return errors.Errorf("unknown admission control mode: %s", val)
	default:
		return errors.Errorf("unknown key: %s", key)
	}
	return err
}

//lint:ignore U1000 unused
func addMetamorphic(r registry.Registry, p perturbation) {
	rng, seed := randutil.NewPseudoRand()
	v := p.setupMetamorphic(rng)
	v.seed = seed
	v = v.finishSetup()
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/metamorphic/%s", v.perturbationName()),
		CompatibleClouds: v.cloud,
		Suites:           registry.Suites(registry.Perturbation),
		Owner:            registry.OwnerKV,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Randomized:       true,
		Run:              v.runTest,
	})
}

func addFull(r registry.Registry, p perturbation) {
	v := p.setup()
	v = v.finishSetup()
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/full/%s", v.perturbationName()),
		CompatibleClouds: v.cloud,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerKV,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Benchmark:        true,
		Run:              v.runTest,
	})
}

func addDev(r registry.Registry, p perturbation) {
	v := p.setup()
	// Dev tests never fail on latency increases.
	v.acceptableChange = math.Inf(1)
	// Make the tests faster for development.
	v.splits = 1
	v.numNodes = 5
	v.numWorkloadNodes = 1
	v.vcpu = 4
	v.disks = 1
	v.fillDuration = 20 * time.Second
	v.validationDuration = 10 * time.Second
	v.perturbationDuration = 30 * time.Second
	// We want to collect some profiles during the dev test, so make it more
	// aggressive at collecting profiles.
	v.profileOptions = []roachtestutil.ProfileOptionFunc{
		roachtestutil.ProfDbName("target"),
		roachtestutil.ProfMinimumLatency(20 * time.Millisecond),
		roachtestutil.ProfMinNumExpectedStmts(100),
		roachtestutil.ProfProbabilityToInclude(0.01),
		roachtestutil.ProfMultipleFromP99(10),
	}

	// Allow the test to run on dev machines.
	v.cloud = registry.AllClouds
	v = v.finishSetup()
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/dev/%s", v.perturbationName()),
		CompatibleClouds: v.cloud,
		Suites:           registry.ManualOnly,
		Owner:            registry.OwnerKV,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Run:              v.runTest,
	})
}

type perturbation interface {
	// setup is called to create the standard variations for the perturbation.
	setup() variations

	// setupMetamorphic is called at the start of the test to randomize the perturbation.
	setupMetamorphic(rng *rand.Rand) variations

	// startTargetNode is called for custom logic starting the target node(s).
	// Some of the perturbations need special logic for starting the target
	// node.
	startTargetNode(ctx context.Context, t test.Test, v variations)

	// startPerturbation begins the system change and blocks until it is
	// finished. It returns the duration looking backwards to collect
	// performance stats.
	startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration

	// endPerturbation ends the system change. Not all perturbations do anything on stop.
	// It returns the duration looking backwards to collect performance stats.
	endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration
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
	params := v.getParameterMap()
	for k, val := range params {
		t.AddParam(k, val)
		t.L().Printf("%s: %s", k, val)
	}
	t.Status("T0: starting nodes")

	// Track the three operations that we are sending in this test.
	m := c.NewMonitor(ctx, v.stableNodes())

	// Start the stable nodes and let the perturbation start the target node(s).
	v.startNoBackup(ctx, t, v.stableNodes())
	v.applyClusterSettings(ctx, t)
	v.perturbation.startTargetNode(ctx, t, v)

	// Wait for rebalancing to finish before starting to fill. This minimizes
	// the time to finish.
	v.waitForRebalanceToStop(ctx, t)
	require.NoError(t, v.workload.initWorkload(ctx, v))

	// Capture the stable rate near the last 1/4 of the fill process.
	clusterMaxRate := make(chan int)
	m.Go(func(ctx context.Context) error {
		// Wait for the first 3/4 of the duration and then measure the QPS in
		// the last 1/4.
		waitDuration(ctx, v.fillDuration*3/4)
		clusterMaxRate <- int(roachtestutil.MeasureQPS(ctx, t, c, v.fillDuration*1/4, v.stableNodes()))
		return nil
	})
	// Start filling the system without a rate.
	t.Status("T1: filling at full rate")
	_, err := v.workload.runWorkload(ctx, v, v.fillDuration, 0)
	require.NoError(t, err)

	// Start the consistent workload and begin collecting profiles.
	var stableRatePerNode int
	select {
	case rate := <-clusterMaxRate:
		stableRatePerNode = int(float64(rate) * v.ratioOfMax / float64(v.numWorkloadNodes))
		t.Status(fmt.Sprintf("T2: running workload at stable rate of %d per node", stableRatePerNode))
	case <-ctx.Done(): // closes when the caller cancels the ctx
		t.Fatal("failed to get cluster max rate")
	}
	var data *workloadData
	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		if data, err = v.workload.runWorkload(ctx, v, 0, stableRatePerNode); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	// Begin profiling halfway through the workload.
	waitDuration(ctx, v.validationDuration/2)
	t.L().Printf("profiling slow statements")
	require.NoError(t, roachtestutil.ProfileTopStatements(ctx, c, t.L(), roachtestutil.ProfDbName("target")))
	waitDuration(ctx, v.validationDuration/2)

	// Collect the baseline after the workload has stabilized.
	baselineInterval := intervalSince(v.validationDuration / 2)
	// Now start the perturbation.
	t.Status("T3: inducing perturbation")
	perturbationDuration := v.perturbation.startPerturbation(ctx, t, v)
	perturbationInterval := intervalSince(perturbationDuration)

	t.Status("T4: recovery from the perturbation")
	afterDuration := v.perturbation.endPerturbation(ctx, t, v)
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
	require.NoError(t, roachtestutil.DownloadProfiles(ctx, c, t.L(), t.ArtifactsDir()))
	require.NoError(t, v.writePerfArtifacts(ctx, t, baselineStats, perturbationStats, afterStats))

	t.L().Printf("validating stats during the perturbation")
	failures := isAcceptableChange(t.L(), baselineStats, perturbationStats, v.acceptableChange)
	t.L().Printf("validating stats after the perturbation")
	failures = append(failures, isAcceptableChange(t.L(), baselineStats, afterStats, v.acceptableChange)...)
	require.True(t, len(failures) == 0, strings.Join(failures, "\n"))
	// TODO(baptist): Look at the time for token return in actual tests to
	// determine if this can be lowered further.
	tokenReturnTime := 10 * time.Minute
	// TODO(#137017): Increase the return time if disk bandwidth limit is set.
	if v.diskBandwidthLimit != "0" {
		tokenReturnTime = 1 * time.Hour
	}
	roachtestutil.ValidateTokensReturned(ctx, t, v, v.stableNodes(), tokenReturnTime)
}

func (v variations) applyClusterSettings(ctx context.Context, t test.Test) {
	db := v.Conn(ctx, t.L(), 1)
	defer db.Close()
	for key, value := range v.clusterSettings {
		setCmd := fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", key, value)
		t.L().Printf(setCmd)
		if _, err := db.ExecContext(ctx, setCmd); err != nil {
			t.Fatal(err)
		}
	}
}

// trackedStat is a collection of the relevant values from the histogram. The
// score is computed based on the time per operation per core and blended
// latency of P50 and P99. Lower scores are better.
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

// isAcceptableChange determines if a change from the baseline is acceptable.
// It compares all the metrics rather than failing fast. Normally multiple
// metrics will fail at once if a test is going to fail and it is helpful to see
// all the differences.
// This returns an array of strings with the reason(s) the change was too large.
func isAcceptableChange(
	logger *logger.Logger, baseline, other map[string]trackedStat, acceptableChange float64,
) []string {
	// This can happen if we aren't measuring one of the phases.
	var failures []string
	if len(other) == 0 {
		return failures
	}
	keys := sortedStringKeys(baseline)

	for _, name := range keys {
		baseStat := baseline[name]
		otherStat := other[name]
		increase := float64(otherStat.score) / float64(baseStat.score)
		if increase > acceptableChange {
			failure := fmt.Sprintf("FAILURE: %-15s: Increase %.4f > %.4f BASE: %v SCORE: %v\n", name, increase, acceptableChange, baseStat.score, otherStat.score)
			logger.Printf(failure)
			failures = append(failures, failure)
		} else {
			logger.Printf("PASSED : %-15s: Increase %.4f <= %.4f BASE: %v SCORE: %v\n", name, increase, acceptableChange, baseStat.score, otherStat.score)
		}
	}
	return failures
}

// startNoBackup starts the nodes without enabling backup.
func (v variations) startNoBackup(
	ctx context.Context, t test.Test, nodes option.NodeListOption, extraArgs ...string,
) {
	nodesPerRegion := v.numNodes / NUM_REGIONS
	for _, node := range nodes {
		// Don't start a backup schedule because this test is timing sensitive.
		opts := option.NewStartOpts(option.NoBackupSchedule)
		opts.RoachprodOpts.StoreCount = v.disks
		opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--locality=region=fake-%d", (node-1)/nodesPerRegion))
		opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs, extraArgs...)
		settings := install.MakeClusterSettings(install.EnvOption([]string{"GODEBUG=gctrace=1"}))
		v.Start(ctx, t.L(), opts, settings, v.Node(node))
	}
}

// waitForRebalanceToStop polls the system.rangelog every second to see if there
// have been any transfers in the last 5 seconds. It returns once the system
// stops transferring replicas.
func (v variations) waitForRebalanceToStop(ctx context.Context, t test.Test) {
	db := v.Conn(ctx, t.L(), 1)
	defer db.Close()
	q := `SELECT extract_duration(seconds FROM now()-timestamp) FROM system.rangelog WHERE "eventType" = 'add_voter' ORDER BY timestamp DESC LIMIT 1`

	opts := retry.Options{
		InitialBackoff: 1 * time.Second,
		Multiplier:     1,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if row := db.QueryRowContext(ctx, q); row != nil {
			var secondsSinceLastEvent int
			if err := row.Scan(&secondsSinceLastEvent); err != nil && !errors.Is(err, gosql.ErrNoRows) {
				t.Fatal(err)
			}
			if secondsSinceLastEvent > 5 {
				return
			}
		}
	}
	// This loop should never end until success or fatal.
	t.FailNow()
}

// waitForIOOverloadToEnd polls the system.metrics every second to see if there
// is any IO overload on the target nodes. It returns once the overload ends.
func (v variations) waitForIOOverloadToEnd(ctx context.Context, t test.Test) {
	q := `SELECT value FROM crdb_internal.node_metrics WHERE name = 'admission.io.overload'`

	opts := retry.Options{
		InitialBackoff: 1 * time.Second,
		Multiplier:     1,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		anyOverloaded := false
		for _, nodeId := range v.targetNodes() {
			db := v.Conn(ctx, t.L(), nodeId)
			if row := db.QueryRowContext(ctx, q); row != nil {
				var overload float64
				if err := row.Scan(&overload); err != nil && !errors.Is(err, gosql.ErrNoRows) {
					db.Close()
					t.Fatal(err)
					return
				}
				if overload > 0.01 {
					anyOverloaded = true
				}
			}
			db.Close()
		}
		if !anyOverloaded {
			return
		}
	}
	// This loop should never end until success or fatal.
	t.FailNow()
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

type workloadType interface {
	operations() []string
	initWorkload(ctx context.Context, v variations) error
	runWorkload(ctx context.Context, v variations, duration time.Duration, maxRate int) (*workloadData, error)
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

// writePerfArtifacts writes the stats file in the right format to node 1 so it
// can be picked up by roachperf. Currently it only writes the write stats since
// there would be too many lines on the graph otherwise.
func (v variations) writePerfArtifacts(
	ctx context.Context, t test.Test, baseline, perturbation, recovery map[string]trackedStat,
) error {

	exporter := roachtestutil.CreateWorkloadHistogramExporter(t, v)

	reg := histogram.NewRegistryWithExporter(
		time.Second,
		histogram.MockWorkloadName,
		exporter,
	)

	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)
	exporter.Init(&writer)
	defer roachtestutil.CloseExporter(ctx, exporter, t, v, bytesBuf, v.Node(1), "")

	reg.GetHandle().Get("baseline").Record(baseline["write"].score)
	reg.GetHandle().Get("perturbation").Record(perturbation["write"].score)
	reg.GetHandle().Get("recovery").Record(recovery["write"].score)

	var err error
	reg.Tick(func(tick histogram.Tick) {
		err = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
	})
	if err != nil {
		return err
	}
	return nil
}
