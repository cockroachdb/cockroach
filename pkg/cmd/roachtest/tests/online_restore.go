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

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	crdbworkload "github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/require"
)

// sqlServiceLatencyP95Agg is the 10s avg P95 latency of foreground SQL traffic
// across all nodes measured in milliseconds.
var sqlServiceLatencyP95Agg = clusterstats.AggQuery{
	Stat:  sqlServiceLatency,
	Query: "histogram_quantile(0.95, sum by(le) (rate(sql_service_latency_bucket[30s]))) / (1000*1000)",
	Tag:   "P95 Foreground Latency (ms)",
}

var queriesThroughput = clusterstats.ClusterStat{Query: "rate(sql_query_count[30s])", LabelName: "node"}

var queriesThroughputAgg = clusterstats.AggQuery{
	Stat:  queriesThroughput,
	Query: applyAggQuery("sum", queriesThroughput.Query),
	Tag:   "Queries over Time",
}

type onlineRestoreSpecs struct {
	restoreSpecs

	// workload defines the workload that will run during the download phase of
	// Online Restore. If set, must match the fixture that is being restored.
	workload restoreWorkload
	// linkPhaseTimeout is the timeout for the link phase of the restore, if set.
	linkPhaseTimeout time.Duration
	// downloadPhaseTimeout is the timeout for the download phase of the restore, if set.
	downloadPhaseTimeout time.Duration
	// compactionConcurrency overrides the default
	// storage.max_download_compaction_concurrency cluster setting.
	compactionConcurrency int
}

// restoreWorkload describes the workload that will run during the download
// phase of Online Restore.
type restoreWorkload interface {
	// Run begins a workload that runs indefinitely until the passed context is
	// canceled.
	Run(ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs) error
}

type tpccRunOpts struct {
	workers        int
	maxOps         int
	maxRate        int
	waitFraction   float64
	queryTraceFile string
	seed           uint64
	fakeTime       uint32
}

type tpccRestore struct {
	opts tpccRunOpts
}

var _ restoreWorkload = &tpccRestore{}

func (tpcc tpccRestore) Run(
	ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs,
) error {
	crdbNodes := sp.getCRDBNodes()
	cmd := roachtestutil.NewCommand(`./cockroach workload run tpcc`).
		MaybeFlag(tpcc.opts.workers > 0, "workers", tpcc.opts.workers).
		MaybeFlag(tpcc.opts.waitFraction != 1, "wait", tpcc.opts.waitFraction).
		MaybeFlag(tpcc.opts.maxOps != 0, "max-ops", tpcc.opts.maxOps).
		MaybeFlag(tpcc.opts.maxRate != 0, "max-rate", tpcc.opts.maxRate).
		MaybeFlag(tpcc.opts.seed != 0, "seed", tpcc.opts.seed).
		MaybeFlag(tpcc.opts.fakeTime != 0, "fake-time", tpcc.opts.fakeTime).
		MaybeFlag(tpcc.opts.queryTraceFile != "", "query-trace-file", tpcc.opts.queryTraceFile).
		Arg("{pgurl:%d-%d}", crdbNodes[0], crdbNodes[len(crdbNodes)-1])
	return c.RunE(ctx, option.WithNodes([]int{sp.getWorkloadNode()}), cmd.String())
}

func registerOnlineRestorePerf(r registry.Registry) {
	// This driver creates a variety of roachtests to benchmark online restore
	// performance with the prefix
	// restore/{online,offline}/<workload/scale>). For each
	// online restore roachtest (prefix restore/online/*), the driver creates a
	// corresponding roachtest that runs a conventional restore over the same
	// cluster topology and workload in order to measure post restore query
	// latency relative to online restore (prefix restore/control/*).
	//
	// Performance optimizations to reduce download phase time require further
	// investigation.
	for _, sp := range []onlineRestoreSpecs{
		{
			// 350 GB tpcc Online Restore
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
				backup: backupSpecs{
					cloud:   spec.GCE,
					fixture: SmallFixture,
				},
				fullBackupOnly:  true,
				skipFingerprint: true,
				timeout:         1 * time.Hour,
				suites:          registry.Suites(registry.Nightly),
			},
			workload: tpccRestore{
				opts: tpccRunOpts{waitFraction: 0, workers: 100, maxRate: 300},
			},
			linkPhaseTimeout:     45 * time.Second, // typically takes 20 seconds
			downloadPhaseTimeout: 20 * time.Minute, // typically takes 10 minutes.
		},
		{
			// 350 GB tpcc Online Restore with 48 incrementals
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
				backup: backupSpecs{
					cloud:   spec.GCE,
					fixture: SmallFixture,
				},
				fullBackupOnly: false,
				timeout:        1 * time.Hour,
				suites:         registry.Suites(registry.Nightly),
			},
			workload: tpccRestore{
				opts: tpccRunOpts{waitFraction: 0, workers: 100, maxRate: 300},
			},
			linkPhaseTimeout:     45 * time.Second, // typically takes 20 seconds
			downloadPhaseTimeout: 20 * time.Minute, // typically takes 10 minutes.
		},
		// OR Benchmarking tests
		// See benchmark plan here: https://docs.google.com/spreadsheets/d/1uPcQ1YPohXKxwFxWWDUMJrYLKQOuqSZKVrI8SJam5n8
		{
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{
					nodes: 10, volumeSize: 1500, workloadNode: true,
				}),
				backup: backupSpecs{
					cloud:   spec.GCE,
					fixture: MediumFixture,
				},
				timeout:         3 * time.Hour,
				suites:          registry.Suites(registry.Nightly),
				fullBackupOnly:  true,
				skipFingerprint: true,
			},
			workload: tpccRestore{
				opts: tpccRunOpts{waitFraction: 0, workers: 100, maxRate: 1000},
			},
			linkPhaseTimeout:     10 * time.Minute, // typically takes 5 minutes
			downloadPhaseTimeout: 4 * time.Hour,    // typically takes 2 hours.
		},
		{
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{
					nodes: 10, volumeSize: 1500, workloadNode: true,
				}),
				backup: backupSpecs{
					cloud:   spec.GCE,
					fixture: MediumFixture,
				},
				timeout:         3 * time.Hour,
				suites:          registry.Suites(registry.Nightly),
				fullBackupOnly:  true,
				skipFingerprint: true,
			},
			workload: tpccRestore{
				opts: tpccRunOpts{waitFraction: 0, workers: 100, maxRate: 1000},
			},
			linkPhaseTimeout:      10 * time.Minute,
			downloadPhaseTimeout:  4 * time.Hour,
			compactionConcurrency: 32,
		},
		{
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{
					nodes: 10, volumeSize: 1500, workloadNode: true, ebsIOPS: 15_000, ebsThroughput: 800,
				}),
				backup: backupSpecs{
					cloud:   spec.AWS,
					fixture: MediumFixture,
				},
				timeout:         3 * time.Hour,
				suites:          registry.Suites(registry.Nightly),
				fullBackupOnly:  true,
				skipFingerprint: true,
			},
			workload: tpccRestore{
				opts: tpccRunOpts{waitFraction: 0, workers: 100, maxRate: 1000},
			},
			linkPhaseTimeout:      10 * time.Minute,
			downloadPhaseTimeout:  4 * time.Hour,
			compactionConcurrency: 32,
		},
	} {
		for _, runOnline := range []bool{true, false} {
			for _, useWorkarounds := range []bool{true, false} {
				for _, runWorkload := range []bool{true, false} {
					clusterSettings := []string{
						// TODO(dt): what's the right value for this? How do we tune this
						// on the fly automatically during the restore instead of by-hand?
						// Context: We expect many operations to take longer than usual
						// when some or all of the data they touch is remote. For now this
						// is being blanket set to 1h manually, and a user's run-book
						// would need to do this by hand before an online restore and
						// reset it manually after, but ideally the queues would be aware
						// of remote-ness when they pick their own timeouts and pick
						// accordingly.
						"kv.queue.process.guaranteed_time_budget='1h'",
						// TODO(dt): AC appears periodically reduce the workload to 0 QPS
						// during the download phase (sudden jumps from 0 to 2k qps to 0).
						// Disable for now until we figure out how to smooth this out.
						"admission.disk_bandwidth_tokens.elastic.enabled=false",
						"admission.kv.enabled=false",
						"admission.sql_kv_response.enabled=false",
						//"kv.consistency_queue.enabled=false",
						"kv.range_merge.skip_external_bytes.enabled=true",
					}

					if runOnline {
						sp.namePrefix = "online/"
					} else {
						sp.namePrefix = "offline/"
						sp.skip = "used for ad hoc experiments"
					}
					if !runWorkload && sp.skipFingerprint {
						sp.skip = "used for ad hoc experiments"
					}

					sp.namePrefix = sp.namePrefix + fmt.Sprintf("workload=%t", runWorkload)
					if !useWorkarounds {
						clusterSettings = []string{}
						sp.skip = "used for ad hoc experiments"
						sp.namePrefix = sp.namePrefix + fmt.Sprintf("/workarounds=%t", useWorkarounds)
					}

					if sp.compactionConcurrency != 0 {
						sp.namePrefix = sp.namePrefix + fmt.Sprintf(
							"/compaction-concurrency=%d", sp.compactionConcurrency,
						)
						clusterSettings = append(
							clusterSettings,
							fmt.Sprintf(
								"storage.max_download_compaction_concurrency=%d", sp.compactionConcurrency,
							),
						)
						sp.skip = "used for ad hoc experiments"
					}

					if sp.skip == "" && !backuptestutils.IsOnlineRestoreSupported() {
						sp.skip = "online restore is only tested on development branch"
					}

					// For the sake of simplicity, we only fingerprint when no active
					// workload is running during the download phase so that we do not
					// need to account for changes to the database after the restore.
					doFingerprint := !sp.skipFingerprint && runOnline && !runWorkload

					sp.initTestName()
					r.Add(registry.TestSpec{
						Name:      sp.testName,
						Owner:     registry.OwnerDisasterRecovery,
						Benchmark: true,
						Cluster:   sp.hardware.makeClusterSpecs(r),
						Timeout:   sp.timeout,
						// These tests measure performance. To ensure consistent perf,
						// disable metamorphic encryption.
						EncryptionSupport:         registry.EncryptionAlwaysDisabled,
						CompatibleClouds:          sp.backup.CompatibleClouds(),
						Suites:                    sp.suites,
						TestSelectionOptOutSuites: sp.suites,
						Skip:                      sp.skip,
						// Takes 10 minutes on OR tests for some reason.
						SkipPostValidations: registry.PostValidationReplicaDivergence,
						Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
							rd := makeRestoreDriver(ctx, t, c, sp.restoreSpecs)
							rd.prepareCluster(ctx)

							restoreStats := runRestore(
								ctx, t, c, sp, rd, runOnline, runWorkload, clusterSettings...,
							)
							if doFingerprint {
								rd.maybeValidateFingerprint(ctx)
							}
							if runOnline {
								require.NoError(t, postRestoreValidation(
									ctx,
									c,
									t.L(),
									sp.backup.fixture.DatabaseName(),
									restoreStats.downloadEndTimeLowerBound,
								))
							}
							if runWorkload {
								if err := exportStats(ctx, rd, restoreStats); err != nil {
									t.L().Printf("failed to export stats: %s", err.Error())
								}
							}
						},
					})
				}
			}
		}
	}
}

// maybeAddSomeEmptyTables adds some empty tables to the cluster to exercise
// prefix rewrite rules.
func maybeAddSomeEmptyTables(ctx context.Context, rd restoreDriver) error {
	if rd.rng.Intn(2) == 0 {
		return nil
	}
	rd.t.L().Printf("adding some empty tables")
	db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE DATABASE empty`); err != nil {
		return err
	}
	numTables := rd.rng.Intn(10)
	for i := 0; i < numTables; i++ {
		if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE empty.t%d (a INT)`, i)); err != nil {
			return err
		}
	}
	return nil
}

func registerOnlineRestoreCorrectness(r registry.Registry) {
	sp := onlineRestoreSpecs{
		restoreSpecs: restoreSpecs{
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			backup: backupSpecs{
				cloud:   spec.AWS,
				fixture: TinyFixture,
			},
			timeout:    15 * time.Minute,
			suites:     registry.Suites(registry.Nightly),
			namePrefix: "online/correctness",
			skip:       "skip for now - flaky",
		},
		workload: tpccRestore{
			opts: tpccRunOpts{workers: 1, waitFraction: 0, maxOps: 1000},
		},
	}
	sp.initTestName()
	r.Add(
		registry.TestSpec{
			Name:                      sp.testName,
			Owner:                     registry.OwnerDisasterRecovery,
			Cluster:                   sp.hardware.makeClusterSpecs(r),
			Timeout:                   sp.timeout,
			CompatibleClouds:          sp.backup.CompatibleClouds(),
			Suites:                    sp.suites,
			TestSelectionOptOutSuites: sp.suites,
			SkipPostValidations:       registry.PostValidationReplicaDivergence,
			Skip:                      sp.skip,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				defaultSeed := crdbworkload.NewUint64RandomSeed().Seed()
				var defaultFakeTime uint32 = 1713818229 // Set to a fixed value for reproducibility
				regRestoreSpecs, regWorkload := initCorrectnessRestoreSpecs(
					t, sp, defaultSeed, defaultFakeTime, "-reg.trace",
				)
				orSpecs, onlineWorkload := initCorrectnessRestoreSpecs(
					t, sp, defaultSeed, defaultFakeTime, "-online.trace",
				)

				rd := makeRestoreDriver(ctx, t, c, sp.restoreSpecs)
				rd.prepareCluster(ctx)

				runRestore(ctx, t, c, regRestoreSpecs, rd, false /* runOnline */, true /* runWorkload */)
				details, err := c.RunWithDetails(
					ctx,
					t.L(),
					option.WithNodes([]int{regRestoreSpecs.hardware.getWorkloadNode()}),
					fmt.Sprintf("cat %s", regWorkload.opts.queryTraceFile),
				)
				require.NoError(t, err, "failed to retrieve query trace for regular restore")
				regQueryTrace := details[0].Stdout

				c.Wipe(ctx)
				rd.prepareCluster(ctx)

				runRestore(ctx, t, c, orSpecs, rd, true /* runOnline */, true /* runWorkload */)
				details, err = c.RunWithDetails(
					ctx,
					t.L(),
					option.WithNodes([]int{orSpecs.hardware.getWorkloadNode()}),
					fmt.Sprintf("cat %s", onlineWorkload.opts.queryTraceFile),
				)
				require.NoError(t, err, "failed to retrieve query trace for online restore")
				onlineQueryTrace := details[0].Stdout

				// Compare the query traces.
				var longerOutput, shorterOutput string
				if len(regQueryTrace) > len(onlineQueryTrace) {
					longerOutput = regQueryTrace
					shorterOutput = onlineQueryTrace
				} else {
					longerOutput = onlineQueryTrace
					shorterOutput = regQueryTrace
				}

				require.True(
					t,
					strings.HasPrefix(longerOutput, shorterOutput),
					"query trace of online restore does not match regular restore",
				)
			},
		},
	)
}

func postRestoreValidation(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	dbName string,
	approxDownloadJobEndTime time.Time,
) error {
	downloadJobHLC := hlc.Timestamp{WallTime: approxDownloadJobEndTime.UnixNano()}
	conn, err := c.ConnE(ctx, l, c.Node(1)[0])
	if err != nil {
		return err
	}
	var statsJobCount int

	// Ensure there are no automatic jobs acting on restoring tables before the
	// download job completes.
	if err := conn.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT count(*) FROM [SHOW AUTOMATIC JOBS] AS OF SYSTEM TIME '%s' WHERE description ~ '%s';`, downloadJobHLC.AsOfSystemTime(), dbName)).Scan(&statsJobCount); err != nil {
		return err
	}
	if statsJobCount > 0 {
		return errors.Newf("stats jobs found during download job on %s database", dbName)
	}
	return nil
}

func createStatCollector(
	ctx context.Context, rd restoreDriver,
) (clusterstats.StatCollector, error) {
	if rd.c.IsLocal() {
		rd.t.L().Printf("Local test. Don't setup grafana")
		return nil, nil
	}

	// TODO(msbutler): figure out why we need to
	// pass a grafana dashboard for this to work
	promCfg := (&prometheus.Config{}).
		WithPrometheusNode(rd.c.Node(rd.sp.hardware.getWorkloadNode()).InstallNodes()[0]).
		WithCluster(rd.sp.hardware.getCRDBNodes().InstallNodes()).
		WithNodeExporter(rd.sp.hardware.getCRDBNodes().InstallNodes()).
		WithGrafanaDashboard("https://go.crdb.dev/p/changefeed-roachtest-grafana-dashboard")

	// StartGrafana clutters the test.log. Try logging setup to a separate file.
	promLog, err := rd.t.L().ChildLogger("prom_setup", logger.QuietStderr, logger.QuietStdout)
	if err != nil {
		promLog = rd.t.L()
	}
	require.NoError(rd.t, rd.c.StartGrafana(ctx, promLog, promCfg))
	rd.t.L().Printf("Prom has started")

	client, err := clusterstats.SetupCollectorPromClient(ctx, rd.c, rd.t.L(), promCfg)
	if err != nil {
		return nil, err
	}

	return clusterstats.NewStatsCollector(ctx, client), nil
}

// exportStats creates an artifacts file for roachperf. Online Restore
// roachtests will export QPS and p95 sql latency stats over the download job to
// roachperf. The top line metric is the duration between the restore stmt
// execution time and the timestamp we reach and maintain an "acceptable" p99
// latency. We define an acceptable latency as within 1.25x of the latency
// observed 1 minute after the download job completed.
func exportStats(ctx context.Context, rd restoreDriver, restoreStats restoreStats) error {
	endTime := timeutil.Now()
	latencyQueryKey := sqlServiceLatency.Query
	statsCollector := restoreStats.collector
	qpsQueryKey := queriesThroughput.Query
	exportingStats, err := statsCollector.Exporter().Export(ctx, rd.c, rd.t, true, /* dryRun */
		restoreStats.workloadStartTime,
		endTime,
		[]clusterstats.AggQuery{sqlServiceLatencyP95Agg, queriesThroughputAgg},
		func(stats map[string]clusterstats.StatSummary) *roachtestutil.AggregatedMetric {
			var timeToHealth time.Time
			healthyLatencyRatio := 1.25
			n := len(stats[latencyQueryKey].Value)
			rd.t.L().Printf("aggregating latency over %d data points", n)
			if n == 0 {
				return nil // Return nil for no data points
			}
			healthyLatency := stats[latencyQueryKey].Value[n-1]
			latestHealthyValue := healthyLatency

			for i := 0; i < n; i++ {
				if stats[latencyQueryKey].Value[i] < healthyLatency*healthyLatencyRatio && timeToHealth.IsZero() && stats[latencyQueryKey].Value[i] != 0 {
					timeToHealth = timeutil.Unix(0, stats[latencyQueryKey].Time[i])
					latestHealthyValue = stats[latencyQueryKey].Value[i]
				} else if !timeToHealth.IsZero() {
					// Latency increased after it was previously acceptable.
					timeToHealth = time.Time{}
				}
			}
			if timeToHealth.IsZero() {
				timeToHealth = endTime
			}
			rto := timeToHealth.Sub(restoreStats.restoreStartTime).Minutes()
			fullRestoreTime := endTime.Sub(restoreStats.restoreStartTime).Minutes()
			description := "Time to within 1.25x of regular p95 latency (mins)"
			rd.t.L().Printf("%s: %.2f minutes, compared to link + download phase time %.2f", description, rto, fullRestoreTime)
			rd.t.L().Printf("Latency at Recovery Time %.0f ms; at end of test %.0f ms", latestHealthyValue, healthyLatency)

			return &roachtestutil.AggregatedMetric{
				Name:             description,
				Value:            roachtestutil.MetricPoint(rto),
				Unit:             "minutes",
				IsHigherBetter:   false,
				AdditionalLabels: nil,
			}
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to construct stats")
	}

	outlier, err := stats.Percentile(exportingStats.Stats[latencyQueryKey].Value, 95)
	if err != nil {
		return errors.Wrap(err, "could not compute latency outliers")
	}

	medianQPS, err := stats.Median(exportingStats.Stats[qpsQueryKey].Value)
	if err != nil {
		return errors.Wrap(err, "could not compute median QPS")
	}
	if medianQPS < 10 {
		return errors.Errorf("median QPS %.2f < 10. Check test.log if workload silently failed", medianQPS)
	}

	for i, val := range exportingStats.Stats[latencyQueryKey].Value {
		// Json cannot serialize Nan's so convert them to 0 (I tried suffixing the
		// prom query with "or vector(0)", which does not work. These nans appear at
		// the beginning of the workload when 0 queries run in the interval.
		if math.IsNaN(val) {
			exportingStats.Stats[latencyQueryKey].Value[i] = 0
		}
		// Remove outliers from stats to improve roachperf graph quality by reducing
		// the y-axis bounds.
		if val >= outlier {
			exportingStats.Stats[latencyQueryKey].Value[i] = 0
		}
	}
	if err := exportingStats.SerializeOutRun(ctx, rd.t, rd.c, rd.t.ExportOpenmetrics()); err != nil {
		return errors.Wrap(err, "failed to export stats")
	}
	return nil
}

func waitForDownloadJob(
	ctx context.Context, c cluster.Cluster, l *logger.Logger,
) (time.Time, error) {
	l.Printf(`Begin tracking online restore download phase completion`)
	// Wait for the job to succeed.
	var downloadJobEndTimeLowerBound time.Time
	pollingInterval := time.Minute
	succeededJobTick := time.NewTicker(pollingInterval)
	defer succeededJobTick.Stop()
	done := ctx.Done()
	conn, err := c.ConnE(ctx, l, c.Node(1)[0])
	if err != nil {
		return downloadJobEndTimeLowerBound, err
	}
	defer conn.Close()
	for {
		select {
		case <-done:
			return downloadJobEndTimeLowerBound, ctx.Err()
		case <-succeededJobTick.C:
			var status string
			if err := conn.QueryRow(`SELECT status FROM [SHOW JOBS] WHERE job_type = 'RESTORE' ORDER BY created DESC LIMIT 1`).Scan(&status); err != nil {
				return downloadJobEndTimeLowerBound, err
			}
			if status == string(jobs.StateSucceeded) {
				var externalBytes uint64
				if err := conn.QueryRow(jobutils.GetExternalBytesForConnectedTenant).Scan(&externalBytes); err != nil {
					return downloadJobEndTimeLowerBound, errors.Wrapf(err, "could not get external bytes")
				}
				if externalBytes != 0 {
					return downloadJobEndTimeLowerBound, errors.Newf(" not all data downloaded. %d external bytes still in cluster", externalBytes)
				}
				postDownloadDelay := time.Minute
				l.Printf("Download job completed; let workload run for %.2f minute before proceeding", postDownloadDelay.Minutes())
				time.Sleep(postDownloadDelay)
				downloadJobEndTimeLowerBound = timeutil.Now().Add(-pollingInterval).Add(-postDownloadDelay)
				return downloadJobEndTimeLowerBound, nil
			} else if status == string(jobs.StateRunning) {
				l.Printf("Download job still running")
			} else {
				return downloadJobEndTimeLowerBound, errors.Newf("job unexpectedly found in %s state", status)
			}
		}
	}
}

// initCorrectnessRestoreSpecs initializes the restoreSpecs for correctness testing based on the
// base restore spec by setting the workload seed, fake time, and trace file name
func initCorrectnessRestoreSpecs(
	t test.Test, baseSp onlineRestoreSpecs, seed uint64, fakeTime uint32, traceSuffix string,
) (onlineRestoreSpecs, tpccRestore) {
	t.Helper()
	tpccWorkload, ok := baseSp.workload.(tpccRestore)
	if !ok {
		require.Fail(t, "only tpcc workloads are supported for correctness testing")
	}
	baseSp.timeout = time.Duration(baseSp.timeout.Seconds()*0.45) * time.Second
	tpccWorkload.opts.queryTraceFile = strings.ReplaceAll(baseSp.testName, "/", "-") + traceSuffix
	if tpccWorkload.opts.seed == 0 {
		tpccWorkload.opts.seed = seed
	}
	if tpccWorkload.opts.fakeTime == 0 {
		tpccWorkload.opts.fakeTime = fakeTime
	}
	baseSp.workload = tpccWorkload
	return baseSp, tpccWorkload
}

type restoreStats struct {
	collector                 clusterstats.StatCollector
	restoreStartTime          time.Time
	restoreEndTime            time.Time
	downloadEndTimeLowerBound time.Time
	workloadStartTime         time.Time
	workloadEndTime           time.Time
}

// runRestore runs restore based on the provided specs.
//
// If runOnline is set, online restore is run, otherwise a conventional restore
// is run.
//
// If runWorkload is set, the workload is run during the download phase of the
// restore.
//
// clusterSettings is a list of key=value pairs of cluster settings to set
// before performing the restore.
func runRestore(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	sp onlineRestoreSpecs,
	rd restoreDriver,
	runOnline, runWorkload bool,
	clusterSettings ...string,
) restoreStats {
	testStartTime := timeutil.Now()

	statsCollector, err := createStatCollector(ctx, rd)
	require.NoError(t, err)

	restoreStartTime, restoreEndTime, err := executeTestRestorePhase(
		ctx, t, c, sp, rd, runOnline, clusterSettings...,
	)
	require.NoError(t, err, "failed to execute restore phase")

	downloadEndTimeLowerBound, workloadStartTime, workloadEndTime, err := executeTestDownloadPhase(
		ctx, t, c, sp, rd, runOnline, runWorkload, testStartTime,
	)
	require.NoError(t, err, "failed to execute download phase")

	return restoreStats{
		collector:                 statsCollector,
		restoreStartTime:          restoreStartTime,
		restoreEndTime:            restoreEndTime,
		workloadStartTime:         workloadStartTime,
		workloadEndTime:           workloadEndTime,
		downloadEndTimeLowerBound: downloadEndTimeLowerBound,
	}
}

// executeTestRestorePhase executes the restore phase of the online restore
// roachtests. If `runOnline` is not set, a conventional restore is run instead.
// The start time and end time of the online restore link phase are returned (or
// in the case of conventional restore, the start and end time of the entire
// restore job).
func executeTestRestorePhase(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	sp onlineRestoreSpecs,
	rd restoreDriver,
	runOnline bool,
	clusterSettings ...string,
) (time.Time, time.Time, error) {
	db, err := rd.c.ConnE(ctx, t.L(), rd.c.Node(1)[0])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	defer db.Close()
	for _, setting := range clusterSettings {
		if _, err := db.Exec(fmt.Sprintf("SET CLUSTER SETTING %s", setting)); err != nil {
			return time.Time{}, time.Time{}, errors.Wrapf(err, "failed to set cluster setting %s", setting)
		}
	}
	opts := "WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
	if runOnline {
		opts = "WITH EXPERIMENTAL DEFERRED COPY, UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
	}
	if err := maybeAddSomeEmptyTables(ctx, rd); err != nil {
		return time.Time{}, time.Time{}, errors.Wrapf(err, "failed to add some empty tables")
	}
	restoreStartTime := timeutil.Now()
	restoreCmd := rd.restoreCmd(ctx, fmt.Sprintf("DATABASE %s", sp.backup.fixture.DatabaseName()), opts)
	if _, err = db.ExecContext(ctx, restoreCmd); err != nil {
		return time.Time{}, time.Time{}, err
	}
	restoreEndTime := timeutil.Now()
	if runOnline && sp.linkPhaseTimeout != 0 && sp.linkPhaseTimeout < restoreEndTime.Sub(restoreStartTime) {
		return restoreStartTime, restoreEndTime, errors.Newf(
			"link phase took too long: %s greater than timeout %s",
			timeutil.Since(restoreStartTime), sp.linkPhaseTimeout,
		)
	}
	return restoreStartTime, restoreEndTime, err
}

// executeTestDownloadPhase executes the download phase of the online restore
// roachtest. `runWorkload` indicates whether a workload should be running
// during the download phase. If `runOnline` is not set, no wait for the
// download phase is performed, but the workload is still run for 5 minutes (or
// the remaining time in the test, whichever is shorter).
// The lower bound of the download job end time is returned, along with the
// start and end time of the workload, if it was run.
func executeTestDownloadPhase(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	sp onlineRestoreSpecs,
	rd restoreDriver,
	runOnline bool,
	runWorkload bool,
	testStartTime time.Time,
) (time.Time, time.Time, time.Time, error) {
	mon := t.NewErrorGroup(task.Logger(t.L()))

	var workloadStartTime, workloadEndTime time.Time
	workloadCancel := mon.GoWithCancel(func(ctx context.Context, logger *logger.Logger) error {
		if !runWorkload {
			logger.Printf("roachtest configured to skip running the foreground workload")
			return nil
		}
		workloadStartTime = timeutil.Now()
		// We expect the workload to return a context cancelled error because
		// the roachtest driver cancels the monitor's context after the download job completes
		if err := sp.workload.Run(ctx, t, c, sp.hardware); err != nil && ctx.Err() == nil {
			// Implies the workload context was not cancelled and the workload cmd returned a
			// different error.
			return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
		}
		logger.Printf("workload successfully finished")
		return nil
	})

	var downloadEndTimeLowerBound time.Time
	downloadStartTime := timeutil.Now()
	mon.Go(func(ctx context.Context, logger *logger.Logger) error {
		defer workloadCancel()
		if runOnline {
			var err error
			if downloadEndTimeLowerBound, err = waitForDownloadJob(ctx, c, logger); err != nil {
				return err
			}
			downloadTime := downloadEndTimeLowerBound.Sub(downloadStartTime)
			if sp.downloadPhaseTimeout != 0 && sp.downloadPhaseTimeout < downloadTime {
				return errors.Newf(
					"download phase took too long: %s greater than timeout %s",
					downloadTime, sp.downloadPhaseTimeout,
				)
			}
		}

		if runWorkload {
			// Remaining workload duration is capped by the test timeout
			testRunTime := timeutil.Since(testStartTime)
			testTimeoutRemaining := sp.timeout - (testRunTime + time.Minute)

			// Run the workload for at most 5 more minutes.
			maxWorkloadDuration := time.Minute * 5

			workloadDuration := min(testTimeoutRemaining, maxWorkloadDuration)
			logger.Printf("let workload run for another %.2f minutes", workloadDuration.Minutes())
			time.Sleep(workloadDuration)
		}
		return nil
	})

	if err := mon.WaitE(); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	return downloadEndTimeLowerBound, workloadStartTime, workloadEndTime, nil
}
