// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/require"
)

const (
	// defaultAllocBenchDuration is the default duration of workloads.
	defaultAllocBenchDuration = 30 * time.Minute
	// defaultStartRecord is by default how long after the cluster began
	// running before the benchmark starts recording stats to apply penalties.
	defaultStartRecord = 10 * time.Minute
	// defaultAllocBenchConcurrency is the default concurrency limit for
	// workloads in allocation bench.
	defaultAllocBenchConcurrency = 512
	// defaultAllocBenchDuration is the number of times that the spec is re-run
	// in order to find a summary run value.
	defaultBenchSamples = 5
)

type allocationBenchSpec struct {
	nodes, cpus int
	load        allocBenchLoad

	startRecord time.Duration
	samples     int
}

type allocBenchLoad struct {
	desc   string
	events []allocBenchLoadEvent
}

type allocBenchLoadEvent struct {
	run       allocBenchEventRunner
	startTime time.Duration
}

// allocBenchEventRunner declares a run method, which may be implemented by
// different events in order to execute workloads, background operations or
// likewise on the cluster.
type allocBenchEventRunner interface {
	run(context.Context, cluster.Cluster, test.Test) error
}

// kvAllocBenchEventRunner implements the allocBenchEventRunner interface.
type kvAllocBenchEventRunner struct {
	readPercent int
	rate        int
	blockSize   int
	splits      int
	skew        bool
	span        bool
	insertCount int

	name string
}

var (
	// NB: 35% CPU expected on 7 node cluster.
	smallReads = kvAllocBenchEventRunner{
		readPercent: 95,
		blockSize:   1,
		rate:        16500,
		splits:      210,
		insertCount: 10000000,
		name:        "smallreads",
	}

	// NB: 30% CPU expected on 7 node cluster.
	bigReads = kvAllocBenchEventRunner{
		readPercent: 75,
		blockSize:   1 << 10,
		rate:        850,
		splits:      10,
		span:        true,
		insertCount: 1000000,
		name:        "bigreads",
	}

	// NB: 40% CPU, 25% Write Bandwidth expected on a 7 node cluster.
	smallWrites = kvAllocBenchEventRunner{
		readPercent: 0,
		blockSize:   1 << 10,
		rate:        8000,
		splits:      60,
		insertCount: 100000,
		name:        "smallwrites",
	}

	// NB: 20% CPU, 40% Write Bandwidth expected on a 7 node cluster.
	bigWrites = kvAllocBenchEventRunner{
		readPercent: 0,
		blockSize:   32 << 10,
		rate:        550,
		splits:      10,
		insertCount: 1000,
		name:        "bigwrites",
	}
)

func (r kvAllocBenchEventRunner) skewed() kvAllocBenchEventRunner {
	r.skew = true
	return r
}

func (r kvAllocBenchEventRunner) addname(add string) kvAllocBenchEventRunner {
	r.name += add
	return r
}

func (r kvAllocBenchEventRunner) run(ctx context.Context, c cluster.Cluster, t test.Test) error {
	name := r.name
	setupCmd := fmt.Sprintf("./cockroach workload init kv --db=%s", name)
	if r.insertCount > 0 {
		setupCmd += fmt.Sprintf(" --insert-count=%d --min-block-bytes=%d --max-block-bytes=%d", r.insertCount, r.blockSize, r.blockSize)
		if r.skew {
			setupCmd += " --zipfian"
		}
	}
	setupCmd += " {pgurl:1}"
	err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), setupCmd)
	if err != nil {
		return err
	}

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	runCmd := fmt.Sprintf(
		"./cockroach workload run kv --db=%s --read-percent=%d --min-block-bytes=%d --max-block-bytes=%d --max-rate=%d",
		name, r.readPercent, r.blockSize, r.blockSize, r.rate)
	if r.skew {
		runCmd += " --zipfian"
	}

	// When span is specifed, add in 20% spans to the workload with 10k
	// spanning limit batched in sizes of 10. This puts a read (cpu) intensive
	// load on the cluster.
	if r.span {
		runCmd += " --span-percent=20 --span-limit=10000 --batch=10"
	}

	runCmd = fmt.Sprintf(
		"%s --tolerate-errors --concurrency=%d --duration=%s {pgurl%s}",
		runCmd, defaultAllocBenchConcurrency, defaultAllocBenchDuration.String(), c.CRDBNodes())

	t.Status("running kv workload", runCmd)
	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd)
}
func registerAllocationBench(r registry.Registry) {
	for _, spec := range []allocationBenchSpec{
		// TODO(kvoli): Add a background event runner and implement events for
		// import and index backfills.
		{
			// NB: Skewed read operation workload, 20k ops/s vs 700 ops/s,
			// where each workload uses approx. the same amount of resources in
			// aggregate.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "kv/r=95/ops=skew",
				events: []allocBenchLoadEvent{
					{run: smallReads},
					{run: bigReads},
				},
			},
		},
		{
			// NB: Skewed write operation workload, 8k ops/s vs 500 ops/s,
			// where each workload uses approx. the same amount of resources in
			// aggregate.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "kv/r=0/ops=skew",
				events: []allocBenchLoadEvent{
					{run: smallWrites},
					{run: bigWrites},
				},
			},
		},
		{
			// NB: Skewed read-write operation workload, 20k ops/s vs 500
			// ops/s.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "kv/r=50/ops=skew",
				events: []allocBenchLoadEvent{
					{run: smallReads},
					{run: bigWrites},
				},
			},
		},
		{
			// NB: Skewed read access workload, all ops are uniform in load,
			// however occur much more frequently on certain keys (power law).
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "kv/r=95/access=skew",
				events: []allocBenchLoadEvent{
					{run: smallReads.skewed().addname("1")},
					{run: smallReads.skewed().addname("2")},
				},
			},
		},
		{
			// NB: Skewed write access workload, all ops are uniform in load,
			// however occur much more frequently on certain keys (power law).
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "kv/r=0/access=skew",
				events: []allocBenchLoadEvent{
					{run: bigWrites.skewed().addname("1")},
					{run: bigWrites.skewed().addname("2")},
				},
			},
		},
	} {
		registerAllocationBenchSpec(r, spec)
	}
}

func registerAllocationBenchSpec(r registry.Registry, allocSpec allocationBenchSpec) {
	specOptions := []spec.Option{spec.CPU(allocSpec.cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(allocSpec.cpus)}
	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("allocbench/nodes=%d/cpu=%d/%s", allocSpec.nodes, allocSpec.cpus, allocSpec.load.desc),
		Owner:     registry.OwnerKV,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			allocSpec.nodes+1,
			specOptions...,
		),
		Timeout:           time.Duration(allocSpec.samples) * time.Hour,
		NonReleaseBlocker: true,
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAllocationBench(ctx, t, c, allocSpec)
		},
	})
}

func setupAllocationBench(
	ctx context.Context, t test.Test, c cluster.Cluster, spec allocationBenchSpec,
) (clusterstats.StatCollector, func(context.Context)) {
	t.Status("starting cluster")
	for i := 1; i <= spec.nodes; i++ {
		// Don't start a backup schedule as this test reports to roachperf.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			"--vmodule=store_rebalancer=2,allocator=2,replicate_queue=2")
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	return setupStatCollector(ctx, t, c, spec)
}

func setupStatCollector(
	ctx context.Context, t test.Test, c cluster.Cluster, spec allocationBenchSpec,
) (clusterstats.StatCollector, func(context.Context)) {
	t.Status("setting up prometheus and grafana")

	// Setup the prometheus instance and client.
	cfg := (&prometheus.Config{}).
		WithCluster(c.CRDBNodes().InstallNodes()).
		WithPrometheusNode(c.WorkloadNode().InstallNodes()[0])

	err := c.StartGrafana(ctx, t.L(), cfg)
	require.NoError(t, err)

	cleanupFunc := func(ctx context.Context) {
		if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
		}
		c.Wipe(ctx)
	}

	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), cfg)
	require.NoError(t, err)

	// Setup the stats collector for the prometheus client.
	statCollector := clusterstats.NewStatsCollector(ctx, promClient)
	return statCollector, cleanupFunc
}

func runAllocationBenchEvent(
	ctx context.Context, t test.Test, c cluster.Cluster, loadConf *allocBenchLoad, idx int,
) error {
	// Grab at load event ticket and wait until the start time of the ticket
	// before running.
	load := loadConf.events[idx]
	time.Sleep(load.startTime)
	return load.run.run(ctx, c, t)
}

func runAllocationBench(
	ctx context.Context, t test.Test, c cluster.Cluster, spec allocationBenchSpec,
) {
	if spec.startRecord == 0 {
		spec.startRecord = defaultStartRecord
	}
	if spec.samples == 0 {
		spec.samples = defaultBenchSamples
	}
	samples := make([]*clusterstats.ClusterStatRun, spec.samples)

	for i := 0; i < spec.samples; i++ {
		statCollector, cleanupFunc := setupAllocationBench(ctx, t, c, spec)
		stats, err := runAllocationBenchSample(ctx, t, c, spec, statCollector)
		if err != nil {
			t.L().PrintfCtx(ctx, "unable to collect allocation bench sample %s", err.Error())
		} else {
			samples[i] = stats
		}
		// Completely wipe the cluster after each go. This avoid spurious
		// results where prior information / statistics could influence the
		// results of future runs.
		cleanupFunc(ctx)
	}
	// Find the middle run to export. The test has high variation due to
	// generally compounding effects of bad decisions earlier on that are
	// random. We also add an additional summary, the stddev of a stat over
	// the sampled runs. This helps gauge whether there is a large variance in
	// results and whilst being susecptible to outliers is indicative of
	// worst/best case outcomes.
	result, sampleStddev := findMinDistanceClusterStatRun(t, samples)
	for tag, value := range sampleStddev {
		result.Total[fmt.Sprintf("std_%s", tag)] = value
	}
	if result == nil {
		t.L().PrintfCtx(ctx, "no samples found for allocation bench run, won't put any artifacts")
		return
	}
	if err := result.SerializeOutRun(ctx, t, c, t.ExportOpenmetrics()); err != nil {
		t.L().PrintfCtx(ctx, "error putting run artifacts, %v", err)
	}
}

func runAllocationBenchSample(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	spec allocationBenchSpec,
	statCollector clusterstats.StatCollector,
) (*clusterstats.ClusterStatRun, error) {
	startTime := timeutil.Now()
	// Run the workload, by spinning up a group of monitored goroutines which
	// will sleep until the start time of their ticketed event. When all
	// workloads have completed, or one has errored, the monitor will stop
	// blocking.
	specLoad := &spec.load
	m := c.NewMonitor(ctx, c.Nodes(1, spec.nodes))
	for i := range spec.load.events {
		m.Go(func(ctx context.Context) error {
			return runAllocationBenchEvent(ctx, t, c, specLoad, i)
		})
	}
	err := m.WaitE()
	require.NoError(t, err)

	endTime := timeutil.Now()
	startTime = startTime.Add(spec.startRecord)

	// Dry run the stat collector to get the penalties and statistics of the
	// sample.
	return statCollector.Exporter().Export(
		ctx,
		c,
		t,
		true, /* dryRun */
		startTime, endTime,
		joinSummaryQueries(resourceMinMaxSummary, overloadMaxSummary, rebalanceCostSummary),
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			ret, name := 0.0, "cpu(%)"
			if stat, ok := stats[cpuStat.Query]; ok {
				ret = roundFraction(arithmeticMean(stat.Value), 1, 2)
			}
			return name, ret
		},
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			ret, name := 0.0, "write(%)"
			if stat, ok := stats[ioWriteStat.Query]; ok {
				ret = roundFraction(arithmeticMean(stat.Value), 1, 2)
			}
			return name, ret
		},
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			rebalanceMb := 0.0
			values := stats[rebalanceSnapshotSentStat.Query].Value
			if len(values) > 0 {
				startMB, endMB := values[0], values[len(values)-1]
				rebalanceMb = roundFraction(endMB-startMB, 1024, 2)
			}
			return "cost(gb)", rebalanceMb
		},
	)
}

// findMinDistanceClusterStatRun finds the sample among the given samples,
// which has the minimum sum of pairwise distances to all other samples. The
// distance is calculated across all dimensions given. This also returns the
// stddev of samples. e.g. ((a.cpu - b.cpu)^2 + (a.io - b.io)^2 + ... )^0.5
func findMinDistanceClusterStatRun(
	t test.Test, samples []*clusterstats.ClusterStatRun,
) (*clusterstats.ClusterStatRun, map[string]float64) {
	if len(samples) < 1 {
		return nil, nil
	}
	if len(samples) == 1 {
		return samples[0], nil
	}

	// Construct a matrix of results which looks like [sample][tag].
	tags := []string{}
	for tag := range samples[0].Total {
		tags = append(tags, tag)
	}
	n, m := len(samples), len(tags)
	sort.SliceStable(tags, func(i, j int) bool { return tags[i] < tags[j] })
	resultMatrix := make([][]float64, n)
	for i := 0; i < n; i++ {
		resultMatrix[i] = make([]float64, m)
		for j, tag := range tags {
			resultMatrix[i][j] = samples[i].Total[tag]
		}
	}

	// Gather information on the dispersion of samples and the maximum for
	// scaling below.
	maxVals := make([]float64, m)
	minMaxs := map[string]float64{}
	stddevs := map[string]float64{}
	for j := 0; j < m; j++ {
		statSamples := make([]float64, n)
		for i := 0; i < n; i++ {
			statSamples[i] = resultMatrix[i][j]
		}
		if n > 0 {
			max, _ := stats.Max(statSamples)
			min, _ := stats.Min(statSamples)
			stddev, _ := stats.StandardDeviation(statSamples)

			maxVals[j] = max
			minMaxs[tags[j]] = max - min
			stddevs[tags[j]] = stddev
		}
	}

	// Normalize the values by the maximum, assuming that the minimum possible
	// is 0.
	for j := 0; j < m; j++ {
		for i := 0; i < n; i++ {
			// Handle the div by 0 case.
			if maxVals[j] == 0 {
				resultMatrix[i][j] = 0
			} else {
				resultMatrix[i][j] /= maxVals[j]
			}
		}
	}

	pairWiseDistanceSum := func(e int) float64 {
		total := 0.0
		for i := 0; i < n; i++ {
			if i == e {
				continue
			}
			localDistance := 0.0
			for j := 0; j < m; j++ {
				localDistance += math.Pow(math.Abs(resultMatrix[e][j]-resultMatrix[i][j]), 2 /* pow */)
			}
			if localDistance > 0 {
				total += math.Sqrt(localDistance)
			} else {
				total += 0
			}
		}
		return total
	}

	minSample := 0
	minSampleVal := math.MaxFloat64
	for i := 0; i < n; i++ {
		if dist := pairWiseDistanceSum(i); dist < minSampleVal {
			minSample = i
			minSampleVal = dist
		}
	}

	t.L().Printf("Selected row(%d) %v from samples (normalized) %v", minSample, samples[minSample].Total, resultMatrix)
	t.L().Printf("Sample range %v", minMaxs)
	t.L().Printf("Sample stddev %v", stddevs)
	for _, sample := range samples {
		t.L().Printf("%v", sample.Total)
	}
	return samples[minSample], stddevs
}
