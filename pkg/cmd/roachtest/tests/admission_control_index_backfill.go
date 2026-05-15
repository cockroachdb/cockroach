// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

var indexBackfillQPS = clusterstats.ClusterStat{
	LabelName: "node",
	Query:     "rate(sql_query_count[2m])",
}

var indexBackfillQPSAgg = clusterstats.AggQuery{
	Stat:  indexBackfillQPS,
	Query: "sum(rate(sql_query_count[2m]))",
	Tag:   "Total QPS",
}

type indexBackfillVariant struct {
	nameSuffix               string
	snapshotPrefix           string
	withCgroupLimiting       bool
	withProvisionedBandwidth bool
}

var indexBackfillVariants = []indexBackfillVariant{
	{
		nameSuffix: "/no-disk-limit",
		// Keep the snapshot prefix short: GCE caps snapshot names at 63 chars,
		// and snapshotName() appends a version+node infix that consumes ~22
		// chars on top of the versionedSnapshotPrefix(t) prefix.
		snapshotPrefix: "index-backfill-tpce-100k-nodl",
	},
	{
		nameSuffix:         "/provisioned-bandwidth=false",
		snapshotPrefix:     "index-backfill-tpce-100k-nobw",
		withCgroupLimiting: true,
	},
	{
		nameSuffix:               "/provisioned-bandwidth=true",
		snapshotPrefix:           "index-backfill-tpce-100k-bw",
		withCgroupLimiting:       true,
		withProvisionedBandwidth: true,
	},
}

// versionedSnapshotPrefix returns a snapshot prefix that includes the major
// version (e.g., "index-backfill-tpce-100k-bw-v26-2") so that each major
// release uses its own snapshots. This is needed because snapshot data may
// not be compatible across major versions. Dots are replaced with dashes to
// match the GCE snapshot naming convention used by snapshotName().
func versionedSnapshotPrefix(t test.Test) string {
	v := t.BuildVersion()
	major := strings.ReplaceAll(v.Major().String(), ".", "-")
	return fmt.Sprintf("%s-%s", t.SnapshotPrefix(), major)
}

func registerIndexBackfill(r registry.Registry) {
	for _, v := range indexBackfillVariants {
		clusterOpts := []spec.Option{
			spec.CPU(8),
			spec.WorkloadNode(),
			// The use of snapshots requires workload nodes to also have an attached disk.
			// See: https://github.com/cockroachdb/cockroach/issues/156760
			spec.WorkloadRequiresDisk(),
			spec.WorkloadNodeCPU(8),
			spec.VolumeSize(500),
			spec.VolumeType("pd-ssd"),
			spec.GCEMachineType("n2-standard-8"),
			spec.GCEZones("us-east1-b"),
			spec.ReuseNone(),
		}
		if v.withCgroupLimiting {
			// The cgroup disk staller relies on `lsblk -o NAME,MAJ:MIN,MOUNTPOINTS`,
			// which requires util-linux >= 2.37 (Ubuntu 22.04+). FIPS images run
			// Ubuntu 20.04, so exclude FIPS for cgroup-limiting variants. See #169098.
			clusterOpts = append(clusterOpts, spec.Arch(spec.AllExceptFIPS))
		}
		clusterSpec := r.MakeClusterSpec(10 /* nodeCount */, clusterOpts...)

		r.Add(registry.TestSpec{
			Name:             "admission-control/index-backfill" + v.nameSuffix,
			Timeout:          12 * time.Hour,
			Owner:            registry.OwnerAdmissionControl,
			Benchmark:        true,
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Nightly),
			Cluster:          clusterSpec,
			SnapshotPrefix:   v.snapshotPrefix,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runIndexBackfill(ctx, t, c, clusterSpec, v.withCgroupLimiting, v.withProvisionedBandwidth)
			},
		})
	}
}

const indexBackfillDiskBandwidth = 128 << 20 // 128 MiB/s

func runIndexBackfill(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	clusterSpec spec.ClusterSpec,
	withCgroupLimiting bool,
	withProvisionedBandwidth bool,
) {
	snapshotPrefix := versionedSnapshotPrefix(t)
	snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
		NamePrefix: snapshotPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) == 0 {
		t.L().Printf("no existing snapshots found for %s (%s), doing pre-work",
			t.Name(), snapshotPrefix)

		// Set up TPC-E with 100k customers. Do so using a published
		// CRDB release, since we'll use this state to generate disk
		// snapshots.
		runTPCE(ctx, t, c, tpceOptions{
			start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				pred, err := release.LatestPredecessor(t.BuildVersion())
				if err != nil {
					t.Fatal(err)
				}

				path, err := clusterupgrade.UploadCockroach(
					ctx, t, t.L(), c, c.All(), clusterupgrade.MustParseVersion(pred),
				)
				if err != nil {
					t.Fatal(err)
				}

				// Save the current binary before overwriting with the
				// predecessor. We restore it after snapshot creation so
				// the workload phase runs the current version.
				c.Run(ctx, option.WithNodes(c.All()), "cp ./cockroach ./cockroach.current")

				// Copy over the binary to ./cockroach and run it from
				// there. This test captures disk snapshots, which are
				// fingerprinted using the binary version found in this
				// path. The reason it can't just poke at the running
				// CRDB process is because when grabbing snapshots, CRDB
				// is not running.
				c.Run(ctx, option.WithNodes(c.All()), fmt.Sprintf("cp %s ./cockroach", path))
				settings := install.MakeClusterSettings(install.NumRacksOption(len(c.CRDBNodes())))
				startOpts := option.NewStartOpts(option.NoBackupSchedule)
				roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
				if err := c.StartE(ctx, t.L(), startOpts, settings, c.CRDBNodes()); err != nil {
					t.Fatal(err)
				}
			},
			customers:          100_000,
			disablePrometheus:  true,
			setupType:          usingTPCEInit,
			estimatedSetupTime: 4 * time.Hour,
			nodes:              len(c.CRDBNodes()),
			cpus:               clusterSpec.CPUs,
			ssds:               1,
			onlySetup:          true,
		})

		// Stop all nodes before capturing cluster snapshots.
		c.Stop(ctx, t.L(), option.DefaultStopOpts())

		// Create the aforementioned snapshots.
		snapshots, err = c.CreateSnapshot(ctx, snapshotPrefix)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				// Another concurrent run may have already created snapshots
				// with the same name.
				t.L().Printf("snapshot creation hit 'already exists' error, proceeding: %v", err)
			} else {
				t.Fatal(err)
			}
		} else {
			t.L().Printf("created %d new snapshot(s) with prefix %q, using this state",
				len(snapshots), snapshotPrefix)
		}

		// Restore the current binary so the workload phase runs the
		// current version instead of the predecessor.
		c.Run(ctx, option.WithNodes(c.All()), "cp ./cockroach.current ./cockroach")
	} else {
		t.L().Printf("using %d pre-existing snapshot(s) with prefix %q",
			len(snapshots), snapshotPrefix)

		if !t.SkipInit() {
			roachtestutil.CopySnapshotDataToNodes(ctx, t, c, snapshots)
		}
	}

	// Set up prometheus for metrics collection.
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0]).
		WithNodeExporter(c.CRDBNodes().InstallNodes()).
		WithCluster(c.CRDBNodes().InstallNodes()).
		WithGrafanaDashboard("https://go.crdb.dev/p/index-admission-control-grafana").
		WithScrapeConfigs(
			prometheus.MakeWorkloadScrapeConfig("workload", "/",
				makeWorkloadScrapeNodes(
					c.WorkloadNode().InstallNodes()[0],
					[]workloadInstance{{nodes: c.WorkloadNode()}},
				),
			),
		)
	if err := c.StartGrafana(ctx, t.L(), promCfg); err != nil {
		t.Fatal(err)
	}

	// Set up prometheus client and stat collector for roachperf export.
	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
	require.NoError(t, err)
	statCollector := clusterstats.NewStatsCollector(ctx, promClient)

	// Start the cluster.
	t.Status("starting cluster for workload")
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
	roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
	settings := install.MakeClusterSettings(install.NumRacksOption(len(c.CRDBNodes())))
	if err := c.StartE(ctx, t.L(), startOpts, settings, c.CRDBNodes()); err != nil {
		t.Fatal(err)
	}

	// Configure cluster settings before workload begins.
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Work around https://github.com/cockroachdb/cockroach/issues/98311.
	// TPC-E tables use a 300s GC TTL. When admission control delays
	// AddSSTable requests beyond this TTL, the GC threshold advances
	// past the batch timestamp and the request fails.
	if _, err := db.ExecContext(ctx,
		"SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = false",
	); err != nil {
		t.Fatal(err)
	}

	if withCgroupLimiting {
		// Optionally tell admission control the disk's provisioned
		// bandwidth so it can manage elastic vs. foreground work.
		if withProvisionedBandwidth {
			if _, err := db.ExecContext(ctx,
				"SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '128MiB'",
			); err != nil {
				t.Fatal(err)
			}
			// Allow elastic work to fully utilize the provisioned
			// bandwidth.
			if _, err := db.ExecContext(ctx,
				"SET CLUSTER SETTING kvadmission.store.elastic_disk_bandwidth_max_util = 1.0",
			); err != nil {
				t.Fatal(err)
			}
		}

		// Limit disk bandwidth to 128 MiB/s via cgroups so that disk I/O
		// becomes the bottleneck. Without this, pd-ssd disks are fast enough
		// that admission control has nothing meaningful to manage. This must
		// run after c.StartE, since the cgroup path for the cockroach systemd
		// service only exists after the service has been created.
		t.Status(fmt.Sprintf("limiting disk bandwidth to %d bytes/s via cgroups",
			indexBackfillDiskBandwidth))
		staller := roachtestutil.MakeCgroupDiskStaller(t, c,
			true /* readsToo */, false /* logsToo */, false /* disableStateValidation */)
		staller.Setup(ctx)
		staller.Slow(ctx, c.CRDBNodes(), indexBackfillDiskBandwidth)
	}

	// Initialize TPC-E spec for running workload commands.
	tpceSpec, err := initTPCESpec(ctx, t.L(), c)
	require.NoError(t, err)

	// Run TPC-E workload and schema changes concurrently, collecting
	// disk bandwidth metrics during the schema changes.
	const (
		workloadDuration = 4 * time.Hour
		baselineWait     = 5 * time.Minute
	)
	var backfillDuration time.Duration
	var pkChangeDuration time.Duration
	var totalBWSamples []float64
	var metricsStart, metricsEnd time.Time

	// Run the TPC-E workload with a cancelable context so we can stop it
	// once the schema changes complete.
	cancelWorkload := t.GoWithCancel(func(ctx context.Context, l *logger.Logger) error {
		t.Status(fmt.Sprintf("starting TPC-E workload with 20000 active customers (<%s)",
			workloadDuration))
		runOptions := tpceCmdOptions{
			customers:       100_000,
			activeCustomers: 20_000,
			racks:           len(c.CRDBNodes()),
			duration:        workloadDuration,
			threads:         400,
			skipCleanup:     true,
			connectionOpts:  defaultTPCEConnectionOpts(),
		}
		result, err := tpceSpec.run(ctx, t, c, runOptions)
		if err != nil {
			// Context cancellation is expected when we stop the workload
			// after schema changes complete.
			if ctx.Err() != nil {
				l.Printf("TPC-E workload stopped (schema changes completed)")
				return nil
			}
			l.Printf("TPC-E workload error: %v", err)
			return err
		}
		l.Printf("TPC-E workload output:\n%s\n", result.Stdout)
		return nil
	}, task.Name("tpce-workload"))

	// Run schema changes in a separate group so we can wait for them
	// independently of the workload.
	g := t.NewGroup(task.WithContext(ctx))

	// Goroutine 1: Run index creation after baseline period.
	g.Go(func(ctx context.Context, l *logger.Logger) error {
		t.Status(fmt.Sprintf("recording baseline performance (<%s)", baselineWait))
		time.Sleep(baselineWait)

		t.Status("starting index creation on tpce.cash_transaction")
		indexName := fmt.Sprintf("index_%s", timeutil.Now().Format("20060102_T150405"))

		backfillStart := timeutil.Now()
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE INDEX %s ON tpce.cash_transaction (ct_dts)", indexName),
		)
		backfillDuration = timeutil.Since(backfillStart)
		if err != nil {
			l.Printf("index creation error: %v", err)
			return err
		}
		l.Printf("index backfill completed in %s", backfillDuration)
		t.Status("finished index creation")
		return nil
	}, task.Name("index-backfill"))

	// Goroutine 2: Run primary key change, starting 10 minutes after
	// test start (5 minutes after the index backfill starts).
	g.Go(func(ctx context.Context, l *logger.Logger) error {
		time.Sleep(baselineWait + 5*time.Minute)

		t.Status("starting primary key change on tpce.holding_history")
		pkChangeStart := timeutil.Now()
		_, err := db.ExecContext(ctx,
			"ALTER TABLE tpce.holding_history ALTER PRIMARY KEY USING COLUMNS (hh_h_t_id ASC, hh_t_id ASC, hh_before_qty ASC)",
		)
		pkChangeDuration = timeutil.Since(pkChangeStart)
		if err != nil {
			l.Printf("primary key change error: %v", err)
			return err
		}
		l.Printf("primary key change completed in %s", pkChangeDuration)
		t.Status("finished primary key change")
		return nil
	}, task.Name("pk-change"))

	// Collect disk bandwidth metrics in a separate goroutine outside the
	// group, so g.Wait() only blocks on the workload and schema changes.
	stopMetrics := make(chan struct{})
	metricsDone := make(chan struct{})
	t.Go(func(ctx context.Context, l *logger.Logger) error {
		defer close(metricsDone)

		// Wait for baseline period before starting collection.
		time.Sleep(baselineWait)

		metricsStart = timeutil.Now()
		defer func() { metricsEnd = timeutil.Now() }()

		// Use avg() across nodes to get per-node average bandwidth,
		// which is comparable to the 128 MiB/s cgroup limit.
		writeBWQuery := divQuery("avg(rate(sys_host_disk_write_bytes[1m]))", 1<<20)
		readBWQuery := divQuery("avg(rate(sys_host_disk_read_bytes[1m]))", 1<<20)

		getMetricVal := func(query string) (float64, error) {
			point, err := statCollector.CollectPoint(ctx, t.L(), timeutil.Now(), query)
			if err != nil {
				return 0, err
			}
			for _, v := range point[""] {
				return v.Value, nil
			}
			return 0, fmt.Errorf("no data for query %s", query)
		}

		l.Printf("=== DISK BANDWIDTH METRICS COLLECTION STARTED (1m interval) ===")
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		iteration := 0
		for {
			select {
			case <-ticker.C:
				iteration++
				writeBW, writeErr := getMetricVal(writeBWQuery)
				readBW, readErr := getMetricVal(readBWQuery)
				if writeErr != nil || readErr != nil {
					l.Printf("[metrics %d] error collecting: write=%v, read=%v",
						iteration, writeErr, readErr)
					continue
				}
				totalBW := writeBW + readBW
				totalBWSamples = append(totalBWSamples, totalBW)
				l.Printf("[metrics %d] disk bandwidth: read=%.2f MiB/s, write=%.2f MiB/s, total=%.2f MiB/s",
					iteration, readBW, writeBW, totalBW)
			case <-stopMetrics:
				l.Printf("=== DISK BANDWIDTH METRICS COLLECTION STOPPED ===")
				return nil
			case <-ctx.Done():
				return nil
			}
		}
	}, task.Name("metrics-collector"))

	// Wait for schema changes to complete, then stop the workload and
	// metrics collection.
	g.Wait()
	cancelWorkload()
	close(stopMetrics)
	<-metricsDone

	t.L().Printf("index backfill duration: %s", backfillDuration)
	t.L().Printf("primary key change duration: %s", pkChangeDuration)

	// Compute mean total bandwidth from the samples collected during
	// schema changes.
	var avgTotalBW float64
	if len(totalBWSamples) > 0 {
		var sum float64
		for _, s := range totalBWSamples {
			sum += s
		}
		avgTotalBW = sum / float64(len(totalBWSamples))
		t.L().Printf("average total disk bandwidth during schema changes: %.2f MiB/s (%d samples)",
			avgTotalBW, len(totalBWSamples))
	}

	// Export metrics to roachperf.
	if !metricsStart.IsZero() && !metricsEnd.IsZero() {
		exportingStats, err := statCollector.Exporter().Export(
			ctx, c, t, true, /* dryRun */
			metricsStart, metricsEnd,
			[]clusterstats.AggQuery{sqlServiceLatencyAgg, indexBackfillQPSAgg},
			func(stats map[string]clusterstats.StatSummary) *roachtestutil.AggregatedMetric {
				return &roachtestutil.AggregatedMetric{
					Name:           "index_backfill_duration",
					Value:          roachtestutil.MetricPoint(backfillDuration.Seconds()),
					Unit:           "s",
					IsHigherBetter: false,
				}
			},
			func(stats map[string]clusterstats.StatSummary) *roachtestutil.AggregatedMetric {
				return &roachtestutil.AggregatedMetric{
					Name:           "primary_key_change_duration",
					Value:          roachtestutil.MetricPoint(pkChangeDuration.Seconds()),
					Unit:           "s",
					IsHigherBetter: false,
				}
			},
			func(stats map[string]clusterstats.StatSummary) *roachtestutil.AggregatedMetric {
				if avgTotalBW == 0 {
					return nil
				}
				return &roachtestutil.AggregatedMetric{
					Name:           "mean_total_bandwidth",
					Value:          roachtestutil.MetricPoint(avgTotalBW),
					Unit:           "MiB/s",
					IsHigherBetter: true,
				}
			},
			func(stats map[string]clusterstats.StatSummary) *roachtestutil.AggregatedMetric {
				return meanNonNaN(stats, sqlServiceLatency.Query, "mean_p99_latency", "ms", false)
			},
			func(stats map[string]clusterstats.StatSummary) *roachtestutil.AggregatedMetric {
				return meanNonNaN(stats, indexBackfillQPS.Query, "mean_qps", "queries/s", true)
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		// Replace NaN values to avoid JSON serialization errors.
		// histogram_quantile returns NaN when there are no queries
		// in the rate window.
		cleanNaN(exportingStats, sqlServiceLatency.Query)
		cleanNaN(exportingStats, indexBackfillQPS.Query)

		if err := exportingStats.SerializeOutRun(
			ctx, t, c, t.ExportOpenmetrics(),
		); err != nil {
			t.Fatal(err)
		}
	}

	t.Status("test completed successfully")
}

// meanNonNaN computes the mean of non-NaN values in a StatSummary's
// Value timeseries and returns it as an AggregatedMetric. Returns nil
// if no valid data points exist.
func meanNonNaN(
	stats map[string]clusterstats.StatSummary, key, name, unit string, isHigherBetter bool,
) *roachtestutil.AggregatedMetric {
	summary, ok := stats[key]
	if !ok || len(summary.Value) == 0 {
		return nil
	}
	var sum float64
	var count int
	for _, v := range summary.Value {
		if !math.IsNaN(v) {
			sum += v
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return &roachtestutil.AggregatedMetric{
		Name:           name,
		Value:          roachtestutil.MetricPoint(sum / float64(count)),
		Unit:           unit,
		IsHigherBetter: isHigherBetter,
	}
}

// cleanNaN replaces NaN values with 0 in a StatSummary's Value array
// to avoid JSON serialization errors.
func cleanNaN(stats *clusterstats.ClusterStatRun, key string) {
	summary, ok := stats.Stats[key]
	if !ok {
		return
	}
	for i, val := range summary.Value {
		if math.IsNaN(val) {
			summary.Value[i] = 0
		}
	}
	stats.Stats[key] = summary
}
