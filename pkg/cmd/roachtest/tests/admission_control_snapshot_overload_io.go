// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// This test aims to test the behavior of range snapshots under heavy load. It
// sets up a 3 node cluster, where the cluster is pre-populated with about 500GB
// of data. Then, a foreground kv workload is run, and shortly after that, n3 is
// brought down. Upon restart, n3 starts to receive large amounts of snapshot
// data. It is expected that l0 sublevel counts and p99 latencies remain stable.
func registerSnapshotOverloadIO(r registry.Registry) {
	spec := func(subtest string, cfg admissionControlSnapshotOverloadIOOpts) registry.TestSpec {
		return registry.TestSpec{
			Name:             "admission-control/snapshot-overload-io/" + subtest,
			Owner:            registry.OwnerAdmissionControl,
			Benchmark:        true,
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Weekly),
			Cluster: r.MakeClusterSpec(
				4,
				spec.CPU(4),
				spec.WorkloadNode(),
				spec.VolumeSize(cfg.volumeSize),
				spec.ReuseNone(),
				spec.DisableLocalSSD(),
			),
			Leases:  registry.MetamorphicLeases,
			Timeout: 12 * time.Hour,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runAdmissionControlSnapshotOverloadIO(ctx, t, c, cfg)
			},
		}
	}

	// This tests the ability of the storage engine to handle a high rate of
	// snapshots while maintaining a healthy LSM shape and stable p99 latencies.
	r.Add(spec("excise", admissionControlSnapshotOverloadIOOpts{
		// The test uses a large volume size to ensure high provisioned bandwidth
		// from the cloud provider.
		volumeSize: 2000,
		// COCKROACH_CONCURRENT_COMPACTIONS is set to 1 since we want to ensure
		// that snapshot ingests don't result in LSM inversion even with a very
		// low compaction rate. With Pebble's IngestAndExcise all the ingested
		// sstables should ingest into L6.
		limitCompactionConcurrency: true,
		limitDiskBandwidth:         false,
		readPercent:                75,
		workloadBlockBytes:         12288,
		rebalanceRate:              "256MiB",
	}))

	// This tests the behaviour of snapshot ingestion in bandwidth constrained
	// environments.
	r.Add(spec("bandwidth", admissionControlSnapshotOverloadIOOpts{
		// 2x headroom from the ~500GB pre-population of the test.
		volumeSize:                 1000,
		limitCompactionConcurrency: false,
		limitDiskBandwidth:         true,
		readPercent:                20,
		workloadBlockBytes:         1024,
		rebalanceRate:              "1GiB",
	}))

}

type admissionControlSnapshotOverloadIOOpts struct {
	volumeSize                 int
	limitCompactionConcurrency bool
	limitDiskBandwidth         bool
	readPercent                int
	workloadBlockBytes         int
	rebalanceRate              string
}

func runAdmissionControlSnapshotOverloadIO(
	ctx context.Context, t test.Test, c cluster.Cluster, cfg admissionControlSnapshotOverloadIOOpts,
) {
	if c.Spec().NodeCount < 4 {
		t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
	}

	envOptions := install.EnvOption{
		// COCKROACH_RAFT_LOG_TRUNCATION_THRESHOLD is reduced so that there is
		// certainty that the restarted node will be caught up via snapshots,
		// and not via raft log replay.
		fmt.Sprintf("COCKROACH_RAFT_LOG_TRUNCATION_THRESHOLD=%d", 512<<10 /* 512KiB */),
		// COCKROACH_CONCURRENT_SNAPSHOT* is increased so that the rate of
		// snapshot application is high.
		"COCKROACH_CONCURRENT_SNAPSHOT_APPLY_LIMIT=100",
		"COCKROACH_CONCURRENT_SNAPSHOT_SEND_LIMIT=100",
	}

	if cfg.limitCompactionConcurrency {
		envOptions = append(envOptions, "COCKROACH_CONCURRENT_COMPACTIONS=1")
	}

	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
	roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
	settings := install.MakeClusterSettings(envOptions)
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	db := c.Conn(ctx, t.L(), len(c.CRDBNodes()))
	defer db.Close()

	t.Status(fmt.Sprintf("configuring cluster settings (<%s)", 30*time.Second))
	{
		// Defensive, since admission control is enabled by default.
		roachtestutil.SetAdmissionControl(ctx, t, c, true)
		// Ensure ingest splits and excises are enabled. (Enabled by default in v24.1+)
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING kv.snapshot_receiver.excise.enabled = 'true'"); err != nil {
			t.Fatalf("failed to set kv.snapshot_receiver.excise.enabled: %v", err)
		}
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING storage.ingest_split.enabled = 'true'"); err != nil {
			t.Fatalf("failed to set storage.ingest_split.enabled: %v", err)
		}

		// Set rebalance rate.
		if _, err := db.ExecContext(
			ctx, fmt.Sprintf("SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '%s'", cfg.rebalanceRate)); err != nil {
			t.Fatalf("failed to set kv.snapshot_rebalance.max_rate: %v", err)
		}
	}

	// Setup the prometheus instance and client.
	t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
	var statCollector clusterstats.StatCollector
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0]).
		WithNodeExporter(c.CRDBNodes().InstallNodes()).
		WithCluster(c.CRDBNodes().InstallNodes()).
		WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
	err := c.StartGrafana(ctx, t.L(), promCfg)
	require.NoError(t, err)
	cleanupFunc := func() {
		if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
		}
	}
	defer cleanupFunc()
	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
	require.NoError(t, err)
	statCollector = clusterstats.NewStatsCollector(ctx, promClient)

	// Initialize the kv database,
	t.Status(fmt.Sprintf("initializing kv dataset (<%s)", 2*time.Hour))
	c.Run(ctx, option.WithNodes(c.WorkloadNode()),
		"./cockroach workload init kv --drop --insert-count=40000000 "+
			"--max-block-bytes=12288 --min-block-bytes=12288 {pgurl:1-3}")

	// Now set disk bandwidth limits
	if cfg.limitDiskBandwidth {
		const bandwidthLimit = 128 << 20 // 128 MiB
		t.Status(fmt.Sprintf("limiting disk bandwidth to %d bytes/s", bandwidthLimit))
		staller := roachtestutil.MakeCgroupDiskStaller(t, c,
			false /* readsToo */, false /* logsToo */)
		staller.Setup(ctx)
		staller.Slow(ctx, c.CRDBNodes(), bandwidthLimit)

		if _, err := db.ExecContext(
			ctx, fmt.Sprintf("SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '%dMiB'", bandwidthLimit)); err != nil {
			t.Fatalf("failed to set kvadmission.store.provisioned_bandwidth: %v", err)
		}
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING kvadmission.store.snapshot_ingest_bandwidth_control.enabled = 'true'"); err != nil {
			t.Fatalf("failed to set kvadmission.store.snapshot_ingest_bandwidth_control.enabled: %v", err)
		}
	}

	t.Status(fmt.Sprintf("starting kv workload thread (<%s)", time.Minute))
	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {

		labels := map[string]string{
			"concurrency":  "4000",
			"read-percent": strconv.Itoa(cfg.readPercent),
		}
		c.Run(ctx, option.WithNodes(c.WorkloadNode()),
			fmt.Sprintf("./cockroach workload run kv --tolerate-errors "+
				"--splits=1000 %s --read-percent=%d "+
				"--max-rate=600 --max-block-bytes=%d --min-block-bytes=%d "+
				"--concurrency=4000 --duration=%s {pgurl:1-2}",
				roachtestutil.GetWorkloadHistogramArgs(t, c, labels),
				cfg.readPercent,
				cfg.workloadBlockBytes,
				cfg.workloadBlockBytes,
				(6*time.Hour).String(),
			),
		)
		return nil
	})

	t.Status(fmt.Sprintf("waiting for data build up (<%s)", time.Hour))
	time.Sleep(time.Hour)

	t.Status(fmt.Sprintf("killing node 3... (<%s)", time.Minute))
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(3))

	t.Status(fmt.Sprintf("waiting for increased snapshot data and raft log truncation (<%s)", 2*time.Hour))
	time.Sleep(2 * time.Hour)

	t.Status(fmt.Sprintf("starting node 3... (<%s)", time.Minute))
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(envOptions), c.Node(3))

	t.Status(fmt.Sprintf("waiting for snapshot transfers to finish %s", 2*time.Hour))
	m.Go(func(ctx context.Context) error {
		t.Status(fmt.Sprintf("starting monitoring thread (<%s)", time.Minute))
		getMetricVal := func(query string, label string) (float64, error) {
			point, err := statCollector.CollectPoint(ctx, t.L(), timeutil.Now(), query)
			if err != nil {
				t.L().Errorf("could not query prom %s", err.Error())
				return 0, err
			}
			val := point[label]
			for storeID, v := range val {
				t.L().Printf("%s(store=%s): %f", query, storeID, v.Value)
				// We only assert on the 3rd store.
				if storeID == "3" {
					return v.Value, nil
				}
			}
			// Unreachable.
			panic("unreachable")
		}
		getHistMetricVal := func(query string) (float64, error) {
			at := timeutil.Now()
			fromVal, warnings, err := promClient.Query(ctx, query, at)
			if err != nil {
				return 0, err
			}
			if len(warnings) > 0 {
				return 0, errors.Newf("found warnings querying prometheus: %s", warnings)
			}

			fromVec := fromVal.(model.Vector)
			if len(fromVec) == 0 {
				return 0, errors.Newf("Empty vector result for query %s @ %s (%v)", query, at.Format(time.RFC3339), fromVal)
			}
			return float64(fromVec[0].Value), nil
		}

		// Assert on l0 sublevel count and p99 latencies.
		//
		// TODO(aaditya): Add disk bandwidth assertion once
		// https://github.com/cockroachdb/cockroach/pull/133310 lands.
		latencyMetric := divQuery("histogram_quantile(0.99, sum by(le) (rate(sql_service_latency_bucket[2m])))", 1<<20 /* 1ms */)
		const latencyThreshold = 100 // 100ms since the metric is scaled to 1ms above.
		const sublevelMetric = "storage_l0_sublevels"
		const sublevelThreshold = 20
		var l0SublevelCount []float64
		const sampleCountForL0Sublevel = 12
		const collectionIntervalSeconds = 10.0
		// Loop for ~120 minutes.
		const numIterations = int(120 / (collectionIntervalSeconds / 60))
		numErrors := 0
		numSuccesses := 0
		for i := 0; i < numIterations; i++ {
			time.Sleep(collectionIntervalSeconds * time.Second)
			val, err := getHistMetricVal(latencyMetric)
			if err != nil {
				numErrors++
				continue
			}
			if val > latencyThreshold {
				t.Fatalf("sql p99 latency %f exceeded threshold", val)
			}
			val, err = getMetricVal(sublevelMetric, "store")
			if err != nil {
				numErrors++
				continue
			}
			l0SublevelCount = append(l0SublevelCount, val)
			// We want to use the mean of the last 2m of data to avoid short-lived
			// spikes causing failures.
			if len(l0SublevelCount) >= sampleCountForL0Sublevel {
				latestSampleMeanL0Sublevels := roachtestutil.GetMeanOverLastN(sampleCountForL0Sublevel, l0SublevelCount)
				if latestSampleMeanL0Sublevels > sublevelThreshold {
					t.Fatalf("sub-level mean %f over last %d iterations exceeded threshold", latestSampleMeanL0Sublevels, sampleCountForL0Sublevel)
				}
			}
			numSuccesses++
		}
		t.Status(fmt.Sprintf("done monitoring, errors: %d successes: %d", numErrors, numSuccesses))
		if numErrors > numSuccesses {
			t.Fatalf("too many errors retrieving metrics")
		}
		return nil
	})

	m.Wait()
}
