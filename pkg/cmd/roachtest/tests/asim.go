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
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	asimworkload "github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// asim does not assert on any tests, instead it is checked in so that real
// cluster data may be collected easily, in the same format as the simulator
// for validation. By default, the real cluster is never run, only the
// simulation. In order to generate new "real cluster" statistics, add the
// `enableRealCluster` to the spec declaration.

const (
	defaultCPU          = 8
	defaultAsimDuration = 30 * time.Minute
	defaultCycleLength  = 100000
	asimSeed            = 42
	manualAsimTestOnly  = "This config can be used to perform manual allocation" +
		"simulator validation and is not meant to be run on a nightly basis"
)

var (
	// asimStatSummary represents the cluster stats that correspond to
	// collected stats in the allocation simulator.
	asimStatSummary = []clusterstats.AggQuery{
		{Stat: rpsStat, Query: "sum(rebalancing_readspersecond)"},
		{Stat: rbpsStat, Query: "sum(rebalancing_readbytespersecond)"},
		{Stat: wpsStat, Query: "sum(rebalancing_writespersecond)"},
		{Stat: wbpsStat, Query: "sum(rebalancing_writebytespersecond)"},
		{Stat: qpsStat, Query: "sum(rebalancing_queriespersecond)"},
		{Stat: leasesStat, Query: "sum(replicas_leaseholders)"},
		{Stat: replicasStat, Query: "sum(replicas)"},
		{Stat: rebalanceSnapshotSentStat, Query: "sum(range_snapshots_rebalancing_sent_bytes)"},
		{Stat: rebalanceSnapshotRcvdStat, Query: "sum(range_snapshots_rebalancing_rcvd_bytes)"},
		{Stat: leaseTransferStat, Query: "sum(rebalancing_lease_transfers)"},
		{Stat: rangeRebalancesStat, Query: "sum(rebalancing_range_rebalances)"},
		{Stat: rangeSplitStat, Query: "sum(range_splits)"},
	}
	// queryMap maps prometheus queries to simulator metrics tags.
	queryMap = map[string]string{
		qpsStat.Query:                   "qps",
		wpsStat.Query:                   "write",
		wbpsStat.Query:                  "write_b",
		rpsStat.Query:                   "read",
		rbpsStat.Query:                  "read_b",
		replicasStat.Query:              "replicas",
		leasesStat.Query:                "leases",
		leaseTransferStat.Query:         "lease_moves",
		rangeRebalancesStat.Query:       "replica_moves",
		rebalanceSnapshotRcvdStat.Query: "replica_b_rcvd",
		rebalanceSnapshotSentStat.Query: "replica_b_sent",
		rangeSplitStat.Query:            "range_splits",
	}
)

type asimWorkloadSpec struct {
	readRatio                  int
	rate                       int
	minBlockSize, maxBlockSize int
}
type asimSpec struct {
	desc              string
	duration          time.Duration
	skip              string
	enableRealCluster bool

	nodes       int
	cpus        int
	splits      int
	cycleLength int
	skew        bool
	load        asimWorkloadSpec
	settings    config.SimulationSettings
}

type asimRunStats map[string][][]float64

func registerAsim(r registry.Registry) {
	for _, asimSpec := range []asimSpec{
		{
			desc:     "kv95",
			nodes:    3,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 95, rate: 4000, minBlockSize: 128, maxBlockSize: 128},
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv0",
			nodes:    3,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 0, rate: 4000, minBlockSize: 128, maxBlockSize: 128},
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv95/zipf",
			nodes:    3,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 95, rate: 4000, minBlockSize: 128, maxBlockSize: 128},
			skew:     true,
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv0/zipf",
			nodes:    3,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 0, rate: 4000, minBlockSize: 128, maxBlockSize: 128},
			skew:     true,
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv95",
			nodes:    7,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 95, rate: 10000, minBlockSize: 128, maxBlockSize: 128},
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv0",
			nodes:    7,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 0, rate: 10000, minBlockSize: 128, maxBlockSize: 128},
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv95/zipf",
			nodes:    7,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 95, rate: 10000, minBlockSize: 128, maxBlockSize: 128},
			skew:     true,
			settings: *config.DefaultSimulationSettings(),
		},
		{
			desc:     "kv0/zipf",
			nodes:    7,
			duration: 60 * time.Minute,
			load:     asimWorkloadSpec{readRatio: 0, rate: 10000, minBlockSize: 128, maxBlockSize: 128},
			skew:     true,
			settings: *config.DefaultSimulationSettings(),
		},
	} {
		registerAsimSpec(r, asimSpec)
	}
}

func registerAsimSpec(r registry.Registry, asimSpec asimSpec) {
	if asimSpec.cpus == 0 {
		asimSpec.cpus = defaultCPU
	}

	if asimSpec.duration == 0 {
		asimSpec.duration = defaultAsimDuration
	}

	if asimSpec.cycleLength == 0 {
		asimSpec.cycleLength = defaultCycleLength
	}

	specOptions := []spec.Option{spec.CPU(asimSpec.cpus)}

	r.Add(registry.TestSpec{
		Name:  fmt.Sprintf("asim/nodes=%d/cpu=%d/%s", asimSpec.nodes, asimSpec.cpus, asimSpec.desc),
		Owner: registry.OwnerKV,
		Cluster: r.MakeClusterSpec(
			asimSpec.nodes+1,
			specOptions...,
		),
		Timeout:           2 * asimSpec.duration,
		NonReleaseBlocker: true,
		Skip:              asimSpec.skip,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSim(ctx, t, c, asimSpec)
		},
	})
}

func runSim(ctx context.Context, t test.Test, c cluster.Cluster, spec asimSpec) {
	if spec.enableRealCluster {
		runRealCluster(ctx, t, c, spec)
	}
	err := runSimCluster(ctx, t, c, spec)
	if err != nil {
		t.Fatalf("unable to run sim cluster: %s", err.Error())
	}
}

// collectPromStats collects prometheus stats and converts them into the
// simulator data format.
func collectPromStats(
	ctx context.Context,
	l *logger.Logger,
	interval clusterstats.Interval,
	exporter clusterstats.StatExporter,
) *bytes.Buffer {
	summaries, _ := exporter.CollectSummaries(ctx, l, interval, asimStatSummary)
	stats := make(asimRunStats)
	ticks := math.MaxInt
	stores := 0
	tickArray := []int64{}
	for query, summary := range summaries {
		if len(summary.Time) < ticks {
			ticks = len(summary.Time)
			tickArray = summary.Time
		}
		stores = len(summary.Tagged)
		stats[queryMap[query]] = clusterstats.ConvertEqualLengthMapToMat(summary.Tagged)
	}

	metricsBuf := bytes.NewBuffer([]byte{})
	metricsTracker := metrics.NewStoreMetricsTracker(metricsBuf)
	for tick := 0; tick < ticks; tick++ {
		sms := []metrics.StoreMetrics{}
		for store := 0; store < stores; store++ {
			sm := metrics.StoreMetrics{
				Tick:               timeutil.Unix(0, tickArray[tick]),
				StoreID:            int64(store + 1),
				QPS:                int64(stats["qps"][tick][store]),
				ReadKeys:           int64(stats["read"][tick][store]),
				ReadBytes:          int64(stats["read_b"][tick][store]),
				WriteKeys:          int64(stats["write"][tick][store]),
				WriteBytes:         int64(stats["write_b"][tick][store]),
				Replicas:           int64(stats["replicas"][tick][store]),
				Leases:             int64(stats["leases"][tick][store]),
				LeaseTransfers:     int64(stats["lease_moves"][tick][store]),
				Rebalances:         int64(stats["replica_moves"][tick][store]),
				RebalanceSentBytes: int64(stats["replica_b_sent"][tick][store]),
				RebalanceRcvdBytes: int64(stats["replica_b_rcvd"][tick][store]),
				RangeSplits:        int64(stats["range_splits"][tick][store]),
			}
			sms = append(sms, sm)
		}
		metricsTracker.Listen(ctx, sms)
	}

	return metricsBuf
}

func runRealCluster(ctx context.Context, t test.Test, c cluster.Cluster, spec asimSpec) {
	workloadNode := spec.nodes + 1
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))

	for i := 1; i < spec.nodes+1; i++ {
		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--attrs=node%d", i),
			"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
	}

	// Setup the prometheus instance and client.
	clusNodes := c.Range(1, c.Spec().NodeCount-1)
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
	defer cleanupFunc()

	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), cfg)
	require.NoError(t, err)

	// Setup the stats collector for the prometheus client.
	statCollector := clusterstats.NewStatsCollector(ctx, promClient)

	runWorkload := func(workloadSpec asimWorkloadSpec) (time.Time, time.Time) {
		setupCmd := "./cockroach workload init kv"
		if spec.splits > 0 {
			setupCmd = fmt.Sprintf("%s --splits=%d", setupCmd, spec.splits)
		}
		setupCmd = fmt.Sprintf("%s {pgurl:1}", setupCmd)

		runCmd := "./cockroach workload run kv"
		if spec.skew {
			runCmd = fmt.Sprintf("%s --zipfian", runCmd)
		}
		runCmd = fmt.Sprintf(
			"%s --min-block-bytes=%d --max-block-bytes=%d --read-percent=%d --duration=%s --max-rate=%d --cycle-length=100000 {pgurl:1-%d}",
			runCmd,
			workloadSpec.minBlockSize,
			workloadSpec.maxBlockSize,
			workloadSpec.readRatio,
			spec.duration.String(),
			workloadSpec.rate,
			spec.nodes,
		)

		c.Run(ctx, c.Node(workloadNode), setupCmd)

		startTime := timeutil.Now()
		c.Run(ctx, c.Node(workloadNode), runCmd)
		endTime := timeutil.Now()
		return startTime, endTime
	}

	time.Sleep(10 * time.Second)
	start, end := runWorkload(spec.load)
	buffer := collectPromStats(ctx, t.L(), clusterstats.Interval{From: start, To: end.Add(-time.Second * 10)}, statCollector.Exporter())
	_ = writeOutSimBuf(ctx, t, c, buffer, "store_metrics_real.csv")
}

func runSimCluster(ctx context.Context, t test.Test, c cluster.Cluster, spec asimSpec) error {
	settings := config.DefaultSimulationSettings()
	settings.Duration = spec.duration

	state := state.NewTestStateEvenDistribution(spec.nodes, spec.splits, 3, spec.cycleLength)

	var keyGen asimworkload.KeyGenerator
	if spec.skew {
		keyGen = asimworkload.NewZipfianKeyGen(int64(spec.cycleLength), 1.1, 1, rand.New(rand.NewSource(asimSeed)))
	} else {
		keyGen = asimworkload.NewUniformKeyGen(int64(spec.cycleLength), rand.New(rand.NewSource(asimSeed)))

	}
	wgs := []asimworkload.Generator{asimworkload.NewRandomGenerator(
		settings.Start,
		asimSeed,
		keyGen,
		float64(spec.load.rate),
		float64(spec.load.readRatio)/100,
		spec.load.maxBlockSize,
		spec.load.minBlockSize)}

	metricsBuf := bytes.NewBuffer([]byte{})
	metricsTracker := metrics.NewTracker(
		metrics.NewRatedStoreMetricListener(metrics.NewStoreMetricsTracker(metricsBuf)),
	)

	sim := asim.NewSimulator(wgs, state, settings, metricsTracker)
	sim.RunSim(ctx)
	require.NoError(t, writeOutSimBuf(ctx, t, c, metricsBuf, "store_metrics.csv"))

	return nil
}

func writeOutSimBuf(
	ctx context.Context, t test.Test, c cluster.Cluster, buffer *bytes.Buffer, filename string,
) error {
	l := t.L()
	dest := filepath.Join(t.ArtifactsDir(), "asim")
	if err := os.MkdirAll(dest, os.ModePerm); err != nil {
		l.ErrorfCtx(ctx, "failed to create sim dir: %+v", err)
		return err
	}
	dest = filepath.Join(dest, filename)
	if err := os.WriteFile(dest, buffer.Bytes(), 0755); err != nil {
		l.ErrorfCtx(ctx, "failed to upload sim artifacts to node: %s", err.Error())
		return err
	}
	return nil
}
