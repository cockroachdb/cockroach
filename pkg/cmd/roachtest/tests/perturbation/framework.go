// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/artifactsutil"
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
	impact               acceptableImpact
	cloud                registry.CloudSet
	acMode               admissionControlMode
	diskBandwidthLimit   string
	histogramArgs        string // set at test start for workload histogram export
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
var cloudSets = []registry.CloudSet{
	registry.OnlyAWS,
	registry.OnlyGCE,
	registry.OnlyAzure,
	registry.OnlyIBM,
}
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

// acceptableImpact defines max-impact-ratio pass/fail criteria. Each ratio
// mirrors the roachperf top-line metric so the threshold is directly
// comparable to the number shown on the dashboard.
//
// All ratios are derived from per-second workload measurements ("ticks").
// Each tick records the p50 and p99 latency and throughput (ops/s) observed
// during that one-second window. We drop the worst 5% of ticks for each
// series before comparing the worst measurements (i.e. we compare their
// p95s). This filters out brief outlier seconds while still catching
// sustained degradation, striking a balance between spurious test failures
// caused e.g. by infrastructure hiccups and learning about significant
// slowdowns proactively with a full set of artifacts in place.
//
// A ratio of ~1.0 means no measurable latency change. The test fails if
// any of the computed ratios exceeds the corresponding value in the fields
// below.
//
// Regardless of whether a particular run fails due to the impact thresholds
// below, it will show up on roachperf.
type acceptableImpact struct {
	// maxP99Impact is the maximum allowed p99 latency impact ratio between
	// the current interval (perturbation active or the recovery interval
	// following the perturbation) and the pre-perturbation baseline interval.
	//
	// Numerator: collect the p99 latency from every one-second tick during
	// the measured interval, sort them, and drop the worst 5% of seconds.
	// The value at the boundary is the numerator.
	// Denominator: same computation over the baseline interval.
	//
	// Example: baseline ticks have p99s [2ms 3ms 2ms 4ms ...]; after
	// dropping the worst 5% of seconds, the boundary value is 5ms.
	// Perturbation ticks have p99s [10ms 20ms 8ms ...]; after dropping the
	// worst 5%, the boundary is 25ms. Ratio = 25/5 = 5.0.
	maxP99Impact float64

	// maxP50Impact is the maximum allowed p50 (median) latency impact ratio.
	// Computed identically to maxP99Impact but using per-second p50 latencies
	// instead. Less noisy than p99, useful for catching sustained latency
	// shifts that affect all operations rather than just outliers.
	maxP50Impact float64

	// maxThroughputImpact is the maximum allowed throughput impact ratio
	// between the current interval (perturbation active or the recovery
	// interval following the perturbation) and the pre-perturbation baseline
	// interval.
	//
	// Numerator: collect the throughput (ops/s) from every one-second tick
	// during the baseline interval, sort them, and drop the worst 5% of
	// seconds. The value at the boundary is the numerator.
	// Denominator: same computation over the measured interval.
	//
	// The ratio is inverted (baseline / measured) so that the direction
	// matches latency: higher = worse.
	//
	// Example: baseline ticks have throughputs [900 950 920 ...] ops/s;
	// after dropping the worst 5% of seconds, the boundary is 850.
	// Perturbation ticks have [400 500 450 ...]; after dropping the worst
	// 5%, the boundary is 380. Ratio = 850/380 ≈ 2.2, meaning throughput
	// dropped to ~45% of baseline.
	maxThroughputImpact float64
}

// noImpactThresholds returns thresholds that never fail.
func noImpactThresholds() acceptableImpact {
	return acceptableImpact{
		maxP99Impact:        math.Inf(1),
		maxP50Impact:        math.Inf(1),
		maxThroughputImpact: math.Inf(1),
	}
}

// defaultThresholds returns the conservative pass/fail criteria applied to
// production perturbation tests. Latency gates are disabled (p99 in particular
// is too noisy with current sample sizes), but a throughput floor is enforced:
// the test fails if measured throughput drops below 80% of baseline (i.e. the
// throughput impact ratio exceeds 1.25).
func defaultThresholds() acceptableImpact {
	return acceptableImpact{
		maxP99Impact:        math.Inf(1),
		maxP50Impact:        math.Inf(1),
		maxThroughputImpact: 1.25,
	}
}

// aggregatedStat holds summary statistics for per-tick metrics over a
// measurement interval. Used for display/logging and pass/fail validation.
type aggregatedStat struct {
	medianP99  time.Duration // median of per-second p99 latencies
	medianTput float64       // median of per-second throughput
	p99        time.Duration // p95 of per-second p99 latencies
	p50        time.Duration // p95 of per-second p50 latencies
	throughput float64       // p5 of per-second throughput
	ticks      int           // number of 1s ticks
}

func (s aggregatedStat) String() string {
	return fmt.Sprintf(
		"median(p99/s): %v, median(tput/s): %.0f ops/s, "+
			"p95(p99/s): %v, p95(p50/s): %v, p5(tput/s): %.0f ops/s (%d ticks)",
		s.medianP99, s.medianTput, s.p99, s.p50, s.throughput, s.ticks)
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
func setup(p perturbation, impact acceptableImpact) variations {
	v := variations{}
	v.workload = kvWorkload{}
	v.leaseType = registry.LeaderLeases
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
	v.impact = impact
	v.clusterSettings = make(map[string]string)
	// Having the io_load_listener logs makes it easier to debug failures.
	v.clusterSettings["server.debug.default_vmodule"] = "io_load_listener=1"
	return v
}

func register(r registry.Registry, p perturbation, skipReason string) {
	// All metamorphic perturbation tests are currently disabled. See
	// https://github.com/cockroachdb/cockroach/issues/142148.
	addMetamorphic(r, p, "#142148")
	addFull(r, p, skipReason)
	addDev(r, p)
}

func RegisterTests(r registry.Registry) {
	const notSkipped = ""
	const skippedByBankruptcy = "#149662"

	register(r, restart{}, notSkipped)
	addLong(r, restart{})
	register(r, backup{}, notSkipped)

	// TODO(ssd): We skipped the majority of these tests so that we can focus on
	// one at a time. These are vaguely ordered by their previous pass rate
	// (highest first).
	register(r, intents{}, skippedByBankruptcy)
	register(r, decommission{}, skippedByBankruptcy)
	register(r, elasticWorkload{}, skippedByBankruptcy)
	register(r, partition{}, skippedByBankruptcy)
	register(r, backfill{}, notSkipped)
	register(r, &slowDisk{}, skippedByBankruptcy)
	register(r, addNode{}, skippedByBankruptcy)
}

func (v variations) makeClusterSpec() spec.ClusterSpec {
	opts := append(v.specOptions, spec.CPU(v.vcpu), spec.Disks(v.disks), spec.Mem(v.mem), spec.TerminateOnMigration())
	// Disable cluster reuse to avoid potential cgroup side effects.
	if v.perturbationName() == "slowDisk" {
		opts = append(opts, spec.ReuseNone())
		// TODO(darryl): Enable FIPS once we can upgrade to Ubuntu 22 and use cgroups v2 for disk stalls.
		opts = append(opts, spec.Arch(spec.AllExceptFIPS))
	}
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

var perturbationDefaultProcessFunction = func(test string, histograms *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
	meanMetrics := make(map[string]roachtestutil.MetricPoint)

	for _, summary := range histograms.Summaries {
		meanMetrics[summary.Name] = summary.Values[0].Mean
	}

	var aggregatedMeanMetrics roachtestutil.AggregatedPerfMetrics
	for key, value := range meanMetrics {
		aggregatedMeanMetrics = append(aggregatedMeanMetrics, &roachtestutil.AggregatedMetric{
			Name:             fmt.Sprintf("%s_%s_mean", test, key),
			Value:            value / 1e6,
			Unit:             "x baseline",
			IsHigherBetter:   false,
			AdditionalLabels: nil,
		})
	}

	return aggregatedMeanMetrics, nil
}

//lint:ignore U1000 unused
func addMetamorphic(r registry.Registry, p perturbation, skipReason string) {
	rng, seed := randutil.NewPseudoRand()
	v := p.setupMetamorphic(rng)
	v.seed = seed
	v = v.finishSetup()
	r.Add(registry.TestSpec{
		Name:                   fmt.Sprintf("perturbation/metamorphic/%s", v.perturbationName()),
		Skip:                   skipReason,
		CompatibleClouds:       v.cloud,
		Suites:                 registry.Suites(registry.Perturbation),
		Owner:                  registry.OwnerKV,
		Cluster:                v.makeClusterSpec(),
		Leases:                 v.leaseType,
		Randomized:             true,
		PostProcessPerfMetrics: perturbationDefaultProcessFunction,
		Run:                    v.runTest,
	})
}

func addFull(r registry.Registry, p perturbation, skipReason string) {
	v := p.setup()
	v = v.finishSetup()
	r.Add(registry.TestSpec{
		Name:                   fmt.Sprintf("perturbation/full/%s", v.perturbationName()),
		Skip:                   skipReason,
		CompatibleClouds:       v.cloud,
		Suites:                 registry.Suites(registry.Nightly),
		Owner:                  registry.OwnerKV,
		Cluster:                v.makeClusterSpec(),
		Leases:                 v.leaseType,
		Benchmark:              true,
		PostProcessPerfMetrics: perturbationDefaultProcessFunction,
		Run:                    v.runTest,
	})
}

// addLong registers a heavyweight variant of a perturbation test that runs
// significantly longer than addFull. It is intended for cases where the
// perturbation only becomes interesting at scale (e.g. a restart that needs a
// non-trivial backlog to surface IO-overload behavior on recovery), and
// belongs to the Weekly suite to keep the nightly footprint small. Individual
// callers can override variations fields after p.setup() to tune what "long"
// means for their perturbation; the timeout below is sized for the longest
// expected fill-plus-perturbation we currently configure.
//
// What the long fill buys, and what it does not:
//
// The 2h fill duration grows the LSM and range count on the surviving nodes
// so that ongoing compactions, snapshot generation, and lease bookkeeping at
// the time of the perturbation reflect a non-trivial steady-state cluster,
// rather than a freshly-initialized one. It does NOT enlarge the raft gap
// the perturbed node has to close on recovery -- that gap is set by
// perturbationDuration (default 10 minutes) and the cluster's write rate
// during the measurement phase.
//
// Napkin math for the restart variant on the default 12-node, 16 vcpu,
// localSSD spec with 50/50 r/w (50% follower-reads), 4 KiB blocks, and
// splits=10000: cluster max throughput measures around 80k ops/s, so at
// ratioOfMax=0.5 the measurement workload sustains ~20k writes/s. A 10
// minute downtime therefore produces ~12 GiB of cluster-wide raft data
// destined for the down node's ranges. With ~25% of replicas on a
// 12-node RF=3 cluster across 10000 splits, the down node owns ~2500
// ranges, so the average per-range raft log accumulated against it is
// ~5 MiB. That sits below RaftLogTruncationThreshold (16 MiB; see
// pkg/base/config.go), so most ranges should recover via log replay
// rather than raft snapshots; if the per-range write skew, the cluster
// throughput, or perturbationDuration grow, the average will cross that
// threshold and the cost mix shifts toward snapshot ingest.
func addLong(r registry.Registry, p perturbation) {
	v := p.setup()
	v.fillDuration = 2 * time.Hour
	v = v.finishSetup()
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("perturbation/long/%s", v.perturbationName()),
		CompatibleClouds: v.cloud,
		Suites:           registry.Suites(registry.Weekly),
		Owner:            registry.OwnerKV,
		Cluster:          v.makeClusterSpec(),
		Leases:           v.leaseType,
		Benchmark:        true,
		// The expected runtime is around 2h45m (2h fill + 10m perturbation +
		// validation/recovery windows + teardown). The timeout is set well
		// above that — it is a backstop for pathological cases (everything
		// seizing up in a way that does not surface as a test failure) and
		// not a target.
		Timeout: 6 * time.Hour,
		// The 2h fill produces enough data that the post-test replica
		// divergence check exceeds its 20m budget, mirroring the carve-out
		// for large block sizes in pkg/cmd/roachtest/tests/kv.go (see
		// #141007). The divergence-check timeout is logged but does not
		// fail the test; we skip it explicitly to keep the artifact clean.
		SkipPostValidations:    registry.PostValidationReplicaDivergence,
		PostProcessPerfMetrics: perturbationDefaultProcessFunction,
		Run:                    v.runTest,
	})
}

func addDev(r registry.Registry, p perturbation) {
	v := p.setup()
	// Dev tests never fail on latency increases.
	v.impact = noImpactThresholds()
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
		Name:                   fmt.Sprintf("perturbation/dev/%s", v.perturbationName()),
		CompatibleClouds:       v.cloud,
		Suites:                 registry.ManualOnly,
		Owner:                  registry.OwnerKV,
		Cluster:                v.makeClusterSpec(),
		Leases:                 v.leaseType,
		Benchmark:              true,
		PostProcessPerfMetrics: perturbationDefaultProcessFunction,
		Run:                    v.runTest,
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

func prettyPrint(title string, stats map[string]aggregatedStat) string {
	var outputStr strings.Builder
	outputStr.WriteString(title + "\n")
	for _, name := range slices.Sorted(maps.Keys(stats)) {
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
	data map[string]map[time.Time]trackedStat
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
			w.data[tick.Type][tick.Time] = w.data[tick.Type][tick.Time].merge(stat)
		} else {
			w.data[tick.Type][tick.Time] = stat
		}
	}
}

func (w workloadData) convert(t cli.Tick) trackedStat {
	// Align to the second boundary to make the stats from different nodes overlap.
	t.Time = t.Time.Truncate(time.Second)
	return trackedStat{Tick: t}
}

// ticksInInterval returns per-second ticks grouped by operation type for the
// given time interval.
func (w workloadData) ticksInInterval(i interval) map[string][]trackedStat {
	result := make(map[string][]trackedStat)
	for name, stats := range w.data {
		for t, stat := range stats {
			if t.After(i.start) && t.Before(i.end) {
				result[name] = append(result[name], stat)
			}
		}
	}
	return result
}

// aggregateStats computes summary statistics for each operation type within
// the given interval. Used for display/logging only.
func (w workloadData) aggregateStats(i interval) map[string]aggregatedStat {
	ticksByType := w.ticksInInterval(i)

	result := make(map[string]aggregatedStat)
	for name, ticks := range ticksByType {
		p50s := make([]time.Duration, len(ticks))
		p99s := make([]time.Duration, len(ticks))
		tputs := make([]float64, len(ticks))
		for i, t := range ticks {
			p50s[i] = t.P50
			p99s[i] = t.P99
			tputs[i] = t.Throughput
		}
		slices.Sort(p50s)
		slices.Sort(p99s)
		slices.Sort(tputs)

		result[name] = aggregatedStat{
			medianP99:  p99s[percentileIdx(len(p99s), 0.50)],
			medianTput: tputs[percentileIdx(len(tputs), 0.50)],
			p99:        p99s[percentileIdx(len(p99s), 0.95)],
			p50:        p50s[percentileIdx(len(p50s), 0.95)],
			throughput: tputs[percentileIdx(len(tputs), 0.05)],
			ticks:      len(ticks),
		}
	}
	return result
}

// percentileIdx returns the index corresponding to the given percentile in a
// sorted slice of length n.
func percentileIdx(n int, pct float64) int {
	return min(int(float64(n)*pct), n-1)
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
	m := c.NewDeprecatedMonitor(ctx, v.stableNodes())

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

	// Start the consistent workload and begin collecting profiles. Enable
	// histogram export for this workload (not the fill) and disable the
	// temp file so that data is written directly — the measurement workload
	// is cancelled via context, and the temp-file-then-rename pattern would
	// lose all data on cancellation.
	v.histogramArgs = " " + roachtestutil.GetWorkloadHistogramString(t, c, nil, true /* disableTempFile */)
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

	// Write phase boundaries so roachperf can annotate workload charts.
	writePhases(ctx, t, v, baselineInterval, perturbationInterval, afterInterval)

	cancelWorkload()
	require.NoError(t, m.WaitE())

	baselineStats := data.aggregateStats(baselineInterval)
	perturbationStats := data.aggregateStats(perturbationInterval)
	afterStats := data.aggregateStats(afterInterval)

	t.L().Printf("%s\n", prettyPrint("Baseline stats", baselineStats))
	t.L().Printf("%s\n", prettyPrint("Perturbation stats", perturbationStats))
	t.L().Printf("%s\n", prettyPrint("Recovery stats", afterStats))

	t.Status("T5: validating results")
	require.NoError(t, roachtestutil.DownloadProfiles(ctx, c, t.L(), t.ArtifactsDir()))
	require.NoError(t, v.writePerfArtifacts(ctx, t, baselineStats, perturbationStats, afterStats))

	t.L().Printf("validating stats during the perturbation")
	failures := isAcceptableChange(t.L(), baselineStats, perturbationStats, v.impact)
	t.L().Printf("validating stats after the perturbation")
	failures = append(failures, isAcceptableChange(t.L(), baselineStats, afterStats, v.impact)...)
	if len(failures) > 0 {
		// Collect perf artifacts before failing so that roachperf still gets
		// the data for this run.
		artifactsutil.CollectPerfArtifacts(ctx, t, c)
		require.Fail(t, strings.Join(failures, "\n"))
	}
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

// trackedStat wraps a cli.Tick to support merging stats from multiple nodes at
// the same timestamp.
type trackedStat struct {
	cli.Tick
}

func (t trackedStat) String() string {
	return fmt.Sprintf("%s: qps: %d, p50: %s, p99: %s, pMax: %s",
		t.Time, int(t.Throughput), t.P50, t.P99, t.PMax)
}

// merge combines stats from two nodes at the same timestamp. Throughput is
// summed, latency percentiles are averaged (an approximation, but workable
// given similar node profiles), and pMax takes the worst case.
func (t trackedStat) merge(o trackedStat) trackedStat {
	return trackedStat{Tick: cli.Tick{
		Time:       t.Time,
		Throughput: t.Throughput + o.Throughput,
		P50:        (t.P50 + o.P50) / 2,
		P99:        (t.P99 + o.P99) / 2,
		PMax:       max(t.PMax, o.PMax),
	}}
}

// isAcceptableChange checks whether the impact ratios (same values shown on
// roachperf) stay within the configured limits. For each operation type it
// compares the p95(p99/s), p95(p50/s), and p5(tput/s) of the measured
// interval against the baseline.
func isAcceptableChange(
	logger *logger.Logger, baseline, other map[string]aggregatedStat, limits acceptableImpact,
) []string {
	var failures []string
	for _, name := range slices.Sorted(maps.Keys(baseline)) {
		stats, ok := other[name]
		if !ok {
			continue
		}
		base := baseline[name]

		check := func(label, baseStr, gotStr string, ratio, limit float64) {
			if ratio > limit {
				f := fmt.Sprintf(
					"FAILURE: %-15s: %s ratio %.2fx exceeds limit %.1fx (baseline: %s, measured: %s)",
					name, label, ratio, limit, baseStr, gotStr)
				logger.Printf(f)
				failures = append(failures, f)
				return
			}
			logger.Printf("PASSED : %-15s: %s ratio %.2fx within limit %.1fx", name, label, ratio, limit)
		}

		// Latency impact uses the p95(p99/s) and p95(p50/s) ratios; throughput
		// uses p5(tput/s) inverted so higher = worse.
		if base.p99 > 0 {
			check("p99", base.p99.String(), stats.p99.String(),
				float64(stats.p99)/float64(base.p99), limits.maxP99Impact)
		}
		if base.p50 > 0 {
			check("p50", base.p50.String(), stats.p50.String(),
				float64(stats.p50)/float64(base.p50), limits.maxP50Impact)
		}
		if base.throughput > 0 {
			check("throughput",
				fmt.Sprintf("%.0f ops/s", base.throughput),
				fmt.Sprintf("%.0f ops/s", stats.throughput),
				base.throughput/nonZero(stats.throughput), limits.maxThroughputImpact)
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

// writePhases writes a phases.json file alongside the workload's stats.json so
// that roachperf can annotate charts with phase boundaries. The timestamps use
// RFC3339Nano, matching the "Now" field in the workload's stats.json output.
func writePhases(
	ctx context.Context, t test.Test, v variations, baseline, perturbation, after interval,
) {
	type phaseInterval struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}
	phases := map[string]phaseInterval{
		"baseline":     {baseline.start.Format(time.RFC3339Nano), baseline.end.Format(time.RFC3339Nano)},
		"perturbation": {perturbation.start.Format(time.RFC3339Nano), perturbation.end.Format(time.RFC3339Nano)},
		"recovery":     {after.start.Format(time.RFC3339Nano), after.end.Format(time.RFC3339Nano)},
	}
	buf, err := json.Marshal(phases)
	if err != nil {
		t.L().Printf("failed to marshal phases: %v", err)
		return
	}
	dest := filepath.Join(t.PerfArtifactsDir(), "phases.json")
	if err := v.PutString(ctx, string(buf), dest, 0644, v.workloadNodes()); err != nil {
		t.L().Printf("failed to write phases.json: %v", err)
	}
}

// writePerfArtifacts writes the stats file in the right format to node 1 so
// it can be picked up by roachperf. For each operation type and phase, it
// records threshold-independent impact ratios:
//
//   - p95(p99/s) ratio: p95 of per-second p99 latencies / baseline median p99.
//     ~1.0 when healthy, higher = worse. The p95 filters single-second spikes
//     while catching sustained degradation.
//   - p5(tput/s) ratio: baseline median throughput / p5 of per-second throughput.
//     ~1.0 when healthy, higher = worse (inverted so direction matches latency).
//
// To pass ratios through the HDR histogram pipeline (which expects
// time.Duration nanoseconds) and roachperf's perturbationValues (which
// divides by 1e6), we record time.Duration(ratio * float64(time.Millisecond)).
// The 1e6 cancels out, yielding the raw ratio value.
func (v variations) writePerfArtifacts(
	ctx context.Context, t test.Test, baseline, perturbation, recovery map[string]aggregatedStat,
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

	for _, op := range slices.Sorted(maps.Keys(baseline)) {
		base := baseline[op]

		// p95(p99/s) ratio relative to baseline p95(p99/s). ~1.0 = no change.
		if base.p99 > 0 {
			for phase, stats := range map[string]aggregatedStat{
				"perturbation": perturbation[op],
				"recovery":     recovery[op],
			} {
				ratio := float64(stats.p99) / float64(base.p99)
				reg.GetHandle().Get(op + "/p99_ratio/" + phase).Record(
					time.Duration(ratio * float64(time.Millisecond)))
			}
		}

		// p95(p50/s) ratio relative to baseline p95(p50/s). Less noisy than
		// the p99 ratio, useful for detecting sustained latency shifts.
		if base.p50 > 0 {
			for phase, stats := range map[string]aggregatedStat{
				"perturbation": perturbation[op],
				"recovery":     recovery[op],
			} {
				ratio := float64(stats.p50) / float64(base.p50)
				reg.GetHandle().Get(op + "/p50_ratio/" + phase).Record(
					time.Duration(ratio * float64(time.Millisecond)))
			}
		}

		// p5(tput/s) ratio relative to baseline p5(tput/s). ~1.0 = no change,
		// >1 = throughput dropped.
		if base.throughput > 0 {
			for phase, stats := range map[string]aggregatedStat{
				"perturbation": perturbation[op],
				"recovery":     recovery[op],
			} {
				ratio := base.throughput / nonZero(stats.throughput)
				reg.GetHandle().Get(op + "/tput_ratio/" + phase).Record(
					time.Duration(ratio * float64(time.Millisecond)))
			}
		}
	}

	var err error
	reg.Tick(func(tick histogram.Tick) {
		err = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
	})
	return err
}

// nonZero returns v if positive, or 1e-9 to avoid division by zero.
func nonZero(v float64) float64 {
	if v > 0 {
		return v
	}
	return 1e-9
}
