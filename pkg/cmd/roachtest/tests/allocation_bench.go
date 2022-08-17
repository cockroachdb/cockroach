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
	"sync/atomic"
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
	"github.com/stretchr/testify/require"
)

const (
	// defaultAllocBenchDuration is the default duration of workloads.
	defaultAllocBenchDuration = 20 * time.Minute
	// defaultStartRecord is by default how long after the cluster began
	// running before the benchmark starts recording stats to apply penalties.
	defaultStartRecord = 10 * time.Minute
	// defaultAllocBenchConcurrency is the default concurrency limit for
	// workloads in allocation bench.
	defaultAllocBenchConcurrency = 512
	// ACIOThreshold is the threshold (%), that when any admission control IO
	// overload exceeds it, a penalty is applied to the benchmark run.
	ACIOThreshold = 100
	// ACWaitThreshold is the threshold (seconds), that when any admission
	// control average wait duration exceeds it, a penalty is applied to the
	// benchmark run.
	ACWaitThreshold = 30
	// CPUBalanceThreshold is the threshold (%), that when max-min cpu exceeds
	// it, a penalty is applies to the benchmark. e.g.
	//   [40,50,35] = no penalty (50-35 <= 15)
	//   [50,70,55] = penalty    (70-50 >  15)
	CPUBalanceThreshold = 15
	// WriteBalanceThreshold is the threshold (%), that when the max-min write
	// bandwidth utilization exceeds it, a penalty is applied to the benchmark.
	// e.g.
	//   [60,80,80] = no penalty (80-60 <= 30)
	//   [50,80,85] = penalty    (85-50 >  30)
	WriteBalanceThreshold = 30
)

type allocationBenchSpec struct {
	nodes, cpus int
	load        allocBenchLoad

	nodeAttrs   map[int]string
	startRecord time.Duration
}

type allocBenchLoad struct {
	desc         string
	events       []allocBenchLoadEvent
	eventCounter *int32
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

	name string

	pinnedAttribute string
	replFactor      int
}

var (
	// NB: 35% CPU expected on 7 node cluster.
	smallReads = kvAllocBenchEventRunner{
		readPercent: 95,
		blockSize:   1,
		rate:        21000,
		splits:      210,
		name:        "smallreads",
	}

	// NB: 30% CPU expected on 7 node cluster.
	bigReads = kvAllocBenchEventRunner{
		readPercent: 75,
		blockSize:   1 << 10,
		rate:        500,
		splits:      10,
		span:        true,
		name:        "bigreads",
	}

	// NB: 35% CPU, 20% Write Bandwidth on a 7 node cluster.
	smallWrites = kvAllocBenchEventRunner{
		readPercent: 0,
		blockSize:   1 << 10,
		rate:        6000,
		splits:      60,
		name:        "smallwrites",
	}

	// NB: 15% CPU, 37.5% Write Bandwidth on a 7 node cluster.
	bigWrites = kvAllocBenchEventRunner{
		readPercent: 0,
		blockSize:   32 << 10,
		rate:        500,
		splits:      10,
		name:        "bigwrites",
	}
)

func (r kvAllocBenchEventRunner) pinned(attr string) kvAllocBenchEventRunner {
	r.pinnedAttribute = attr
	return r
}

func (r kvAllocBenchEventRunner) skewed() kvAllocBenchEventRunner {
	r.skew = true
	return r
}

func (r kvAllocBenchEventRunner) addname(add string) kvAllocBenchEventRunner {
	r.name += add
	return r
}

func (r kvAllocBenchEventRunner) run(ctx context.Context, c cluster.Cluster, t test.Test) error {
	workloadNode := c.Spec().NodeCount
	name := r.name
	setupCmd := fmt.Sprintf("./workload init kv --db=%s {pgurl:1}", name)
	err := c.RunE(ctx, c.Node(workloadNode), setupCmd)
	if err != nil {
		return err
	}

	if r.replFactor == 0 {
		r.replFactor = 3
	}

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Set the replication factor of the database to match the spec.
	stmt := fmt.Sprintf("alter database %s configure zone using num_replicas=%d",
		name, r.replFactor)
	if r.pinnedAttribute != "" {
		stmt = fmt.Sprintf("%s, constraints='[+%s]'", stmt, r.pinnedAttribute)
	}
	stmt += ";"
	if _, err = db.ExecContext(ctx, stmt); err != nil {
		return err
	}

	runCmd := fmt.Sprintf(
		"./workload run kv --db=%s --read-percent=%d --min-block-bytes=%d --max-block-bytes=%d --max-rate=%d",
		name, r.readPercent, r.blockSize, r.blockSize, r.rate)
	if r.skew {
		runCmd += " --zipfian"
	}

	// When span is specifed, add in 10% spans to the workload with 10k
	// spanning limit batched in sizes of 10. This puts a read (cpu) intensive
	// load on the cluster.
	if r.span {
		runCmd += " --span-percent=20 --span-limit=10000 --batch=10"
	}

	runCmd = fmt.Sprintf(
		"%s --tolerate-errors --concurrency=%d --duration=%s {pgurl:1-%d}",
		runCmd, defaultAllocBenchConcurrency, defaultAllocBenchDuration.String(), workloadNode-1)

	t.Status("running kv workload %s", runCmd)
	return c.RunE(ctx, c.Node(workloadNode), runCmd)
}

// clampResourceEventRunner implements the allocBenchEventRunner interface.
type clampResourceEventRunner struct {
	node           int
	cpu            float64
	writeBandwidth int
}

// clampCPUCmd clamps the usable amount of cpu time by the cockroach service,
// on a node. It sets the usable amount of cpu time to be a fracton of 1 cpu
// second * cpus on the node e.g. with 8 cpus, 50% would be 4 seconds of cpu
// time per second.
func clampCPUCmd(fraction float64, cpus int) string {
	period := int64(time.Second / time.Microsecond)
	quota := int64(fraction * float64(period) * float64(cpus))
	return fmt.Sprintf(
		"sudo bash -c 'echo \"%d\" > /sys/fs/cgroup/cpu/system.slice/cockroach.service/cpu.cfs_quota_us'"+
			"&& sudo bash -c 'echo \"%d\" > /sys/fs/cgroup/cpu/system.slice/cockroach.service/cpu.cfs_period_us'",
		quota, period)
}

// clampDiskWriteBandwidthCmd clamps the usable amount of wrte bandwidth the
// cockroach.service may use per second, in bytes.
func clampDiskWriteBandwidthCmd(bytes int) string {
	return fmt.Sprintf(
		"sudo bash -c 'echo \"259:0  %d\" > /sys/fs/cgroup/blkio/system.slice/cockroach.service/blkio.throttle.write_bps_device'",
		bytes)
}

func (crr *clampResourceEventRunner) run(
	ctx context.Context, c cluster.Cluster, t test.Test,
) error {
	if crr.cpu > 0 {
		err := c.RunE(ctx, c.Node(crr.node), clampCPUCmd(crr.cpu, c.Spec().CPUs))
		if err != nil {
			return err
		}
	}
	if crr.writeBandwidth > 0 {
		err := c.RunE(ctx, c.Node(crr.node), clampDiskWriteBandwidthCmd(crr.writeBandwidth))
		if err != nil {
			return err
		}
	}
	return nil
}

func registerAllocationBench(r registry.Registry) {
	for _, spec := range []allocationBenchSpec{
		// TODO(kvoli): Add a background event runner and implement events for
		// import and index backfills.
		{
			// NB: Skewed read operation workload, 20k ops/s vs 650 ops/s,
			// where each workload uses approx. the same amount of resources in
			// aggregate.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "rw=95/ops=skew",
				events: []allocBenchLoadEvent{
					{run: smallReads},
					{run: bigReads},
				},
			},
		},
		{
			// NB: Skewed write operation workload, 20k ops/s vs 200 ops/s,
			// where each workload uses approx. the same amount of resources in
			// aggregate.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "rw=0/ops=skew",
				events: []allocBenchLoadEvent{
					{run: smallWrites},
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
				desc: "rw=95/access=skew",
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
				desc: "rw=0/access=skew",
				events: []allocBenchLoadEvent{
					{run: bigWrites.skewed().addname("1")},
					{run: bigWrites.skewed().addname("2")},
				},
			},
		},
		{
			// NB: Uniform read heavy workload. Where n7 has reduced cpu
			// capacity to 50% of the original. In aggregate the cluster is
			// overprovisioned.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "rw=95/clamp=cpu",
				events: []allocBenchLoadEvent{
					{run: smallReads.addname("1")},
					{run: smallReads.addname("2")},
					{run: &clampResourceEventRunner{
						node: 7,
						cpu:  0.5,
					}},
				},
			},
		},
		{
			// NB: Uniform write heavy workload. Where n7 has reduced write
			// bandwidth capacity to 50% of the original (400mb/s GCE). In
			// aggregate the cluster is overprovisioned.
			nodes: 7,
			cpus:  8,
			load: allocBenchLoad{
				desc: "rw=0/clamp=write",
				events: []allocBenchLoadEvent{
					{run: smallWrites.addname("1")},
					{run: smallWrites.addname("2")},
					{run: &clampResourceEventRunner{
						node:           7,
						writeBandwidth: 200 << 20,
					}},
				},
			},
		},
		{
			// NB: Skewed read operation workload. Where one workload is pinned
			// to nodes 1,2,3. The remaining load is not pinned. In aggregate,
			// with the pinned load being immovable, the cluster is
			// overprovisioned.
			nodes:     7,
			cpus:      8,
			nodeAttrs: map[int]string{1: "hot", 2: "hot", 3: "hot"},
			load: allocBenchLoad{
				desc: "rw=95/ops=skew/pin",
				events: []allocBenchLoadEvent{
					{run: smallReads},
					{run: bigReads.pinned("hot")},
				},
			},
		},
		{
			// NB: Skewed write operation workload. Where one workload is pinned
			// to nodes 1,2,3. The remaining load is not pinned. In aggregate,
			// with the pinned load being immovable, the cluster is
			// overprovisioned.
			nodes:     7,
			cpus:      8,
			nodeAttrs: map[int]string{1: "hot", 2: "hot", 3: "hot"},
			load: allocBenchLoad{
				desc: "rw=0/ops=skew/pin",
				events: []allocBenchLoadEvent{
					{run: smallWrites},
					{run: bigWrites.pinned("hot")},
				},
			},
		},
	} {
		registerAllocationBenchSpec(r, spec)
	}
}

func registerAllocationBenchSpec(r registry.Registry, allocSpec allocationBenchSpec) {
	specOptions := []spec.Option{spec.CPU(allocSpec.cpus)}
	r.Add(registry.TestSpec{
		Name:  fmt.Sprintf("allocbench/nodes=%d/cpu=%d/%s", allocSpec.nodes, allocSpec.cpus, allocSpec.load.desc),
		Owner: registry.OwnerKV,
		Cluster: r.MakeClusterSpec(
			allocSpec.nodes+1,
			specOptions...,
		),
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAllocationBench(ctx, t, c, allocSpec)
		},
	})
}

func setupAllocationBench(
	ctx context.Context, t test.Test, c cluster.Cluster, spec allocationBenchSpec,
) (clusterstats.StatCollector, func()) {
	workloadNode := c.Spec().NodeCount
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))
	t.Status("starting cluster")
	for i := 1; i <= spec.nodes; i++ {
		startOpts := option.DefaultStartOpts()
		if attr, ok := spec.nodeAttrs[i]; ok {
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				fmt.Sprintf("--attrs=%s", attr))
		}
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			"--vmodule=store_rebalancer=2,allocator=2,replicate_queue=2")
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	return setupStatCollector(ctx, t, c, spec)
}

func setupStatCollector(
	ctx context.Context, t test.Test, c cluster.Cluster, spec allocationBenchSpec,
) (clusterstats.StatCollector, func()) {
	t.Status("setting up prometheus and grafana")

	// Setup the prometheus instance and client.
	clusNodes := c.Range(1, spec.nodes)
	promNode := c.Node(c.Spec().NodeCount)
	cfg := (&prometheus.Config{}).
		WithCluster(clusNodes.InstallNodes()).
		WithPrometheusNode(promNode.InstallNodes()[0])

	err := c.StartGrafana(ctx, t.L(), cfg)
	require.NoError(t, err)

	cleanupFunc := func() {
		if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
		}
	}

	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), cfg)
	require.NoError(t, err)

	// Setup the stats collector for the prometheus client.
	statCollector := clusterstats.NewStatsCollector(ctx, promClient)
	return statCollector, cleanupFunc
}

func runAllocationBenchEvent(
	ctx context.Context, t test.Test, c cluster.Cluster, loadConf *allocBenchLoad,
) error {
	// Grab at load event ticket and wait until the start time of the ticket
	// before running.
	idx := atomic.AddInt32(loadConf.eventCounter, -1)
	load := loadConf.events[idx]
	time.Sleep(load.startTime)
	return load.run.run(ctx, c, t)
}

func runAllocationBench(
	ctx context.Context, t test.Test, c cluster.Cluster, spec allocationBenchSpec,
) {
	statCollector, cleanupFunc := setupAllocationBench(ctx, t, c, spec)
	defer cleanupFunc()

	startTime := timeutil.Now()
	// Run the workload, by spinning up a group of monitored goroutines which
	// will sleep until the start time of their ticketed event. When all
	// workloads have completed, or one has errored, the monitor will stop
	// blocking.
	eventCounter := int32(len(spec.load.events))
	spec.load.eventCounter = &eventCounter
	specLoad := &spec.load
	m := c.NewMonitor(ctx, c.Nodes(1, spec.nodes))
	for range spec.load.events {
		m.Go(func(ctx context.Context) error {
			return runAllocationBenchEvent(ctx, t, c, specLoad)
		})
	}
	err := m.WaitE()
	require.NoError(t, err)

	endTime := timeutil.Now()
	if spec.startRecord == 0 {
		spec.startRecord = defaultStartRecord
	}
	startTime.Add(spec.startRecord)

	// create an array of boolean flags which indicate for each tick, whether
	// the stat value reported, exceeds the threshold argument given.
	penaltyArray := func(stat clusterstats.StatSummary, threshold float64) []bool {
		n := len(stat.Time)
		penalties := make([]bool, n)
		for i := range stat.Time {
			if stat.Value[i] > threshold {
				penalties[i] = true
			}
		}
		return penalties
	}

	// conbinePenalties collects the penalty arrays for each threshold given.
	// If any threshold is exceeded during a tick, then a penalty is applied
	// equal to the tick duration. This penalty is cumulative.
	combinePenalties := func(stats map[string]clusterstats.StatSummary, thresholds map[string]float64, tickDuration float64) float64 {
		penaltyMatrix := [][]bool{}
		m := -1
		for query, threshold := range thresholds {
			stat, ok := stats[query]
			if !ok || len(stat.Time) == 0 {
				t.L().Printf("unable to find tag for stat collection %s, available tags: %+v", query, stats)
				continue
			}
			penArray := penaltyArray(stat, threshold)
			if m == -1 || len(penArray) < m {
				m = len(penArray)
			}
			penaltyMatrix = append(penaltyMatrix, penArray)
		}

		n := len(penaltyMatrix)
		if n == 0 {
			t.L().Printf("unable to find any tags in stat collection, available tags: %+v", stats)
			return 0
		}
		penalty := 0.0
		for column := 0; column < m; column++ {
			for row := 0; row < n; row++ {
				if penaltyMatrix[row][column] {
					penalty += tickDuration
					break
				}
			}
		}
		return penalty
	}

	if err := statCollector.Exporter().Export(
		ctx,
		c,
		t,
		startTime, endTime,
		joinSummaryQueries(resourceMinMaxSummary, overloadMaxSummary, rebalanceCostSummary),
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			return "cpu(s) imbalance", combinePenalties(
				stats,
				map[string]float64{cpuStat.Query: CPUBalanceThreshold},
				10, /* tick duration */
			)
		},
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			return "write(s) imbalance", combinePenalties(
				stats,
				map[string]float64{ioWriteStat.Query: WriteBalanceThreshold},
				10, /* tick duration */
			)
		},
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			return "overload(s)", combinePenalties(
				stats,
				map[string]float64{admissionControlIOOverload.Query: ACIOThreshold, admissionAvgWaitSecs.Query: ACWaitThreshold},
				10, /* tick duration */
			)
		},
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			rebalanceMb := 0.0
			for _, mbs := range stats[rebalanceSnapshotSentStat.Query].Value {
				rebalanceMb += mbs
			}
			return "cost(mb)", rebalanceMb

		},
	); err != nil {
		t.L().PrintfCtx(ctx, "unable to serialize perf artifacts %s", err.Error())
	}
}
