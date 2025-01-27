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
	// linkPhaseTimeout is the timeout for the link phase of the restore, if set.
	linkPhaseTimeout time.Duration
	// downloadPhaseTimeout is the timeout for the download phase of the restore, if set.
	downloadPhaseTimeout time.Duration
}

func registerOnlineRestorePerf(r registry.Registry) {
	// This driver creates a variety of roachtests to benchmark online restore
	// performance with the prefix
	// restore/{online,offline}/<workload/scale>). For each
	// online restore roachtest (prefix restore/online/*), the driver creates a
	// corresponding roachtest that runs a conventional restore over the same
	// cluster topology and workload in order to measure post restore query
	// latency relative to online restore (prefix restore/control/*).
	for _, sp := range []onlineRestoreSpecs{
		{
			// 400GB tpce Online Restore
			restoreSpecs: restoreSpecs{
				hardware:               makeHardwareSpecs(hardwareSpecs{ebsThroughput: 1000 /* MB/s */, workloadNode: true}),
				backup:                 makeRestoringBackupSpecs(backupSpecs{nonRevisionHistory: true, version: fixtureFromMasterVersion, numBackupsInChain: 5}),
				timeout:                1 * time.Hour,
				suites:                 registry.Suites(registry.Nightly),
				restoreUptoIncremental: 1,
				skip:                   "fails because of #118283",
			},
		},
		{
			// 350 GB tpcc Online Restore
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
				backup: makeRestoringBackupSpecs(backupSpecs{
					nonRevisionHistory: true,
					cloud:              spec.GCE,
					version:            "24.1",
					workload:           tpccRestore{tpccRestoreOptions{warehouses: 5000, waitFraction: 0, workers: 100, maxRate: 300}},
					customFixtureDir:   `'gs://cockroach-fixtures-us-east1/backups/tpc-c/v24.1/db/warehouses=5k?AUTH=implicit'`}),
				timeout:                1 * time.Hour,
				suites:                 registry.Suites(registry.Nightly),
				restoreUptoIncremental: 0,
			},
			linkPhaseTimeout:     30 * time.Second, // typically takes 15 seconds
			downloadPhaseTimeout: 20 * time.Minute, // typically takes 10 minutes. Should get faster once we address #124767.
		},
		{
			// 8.5TB tpcc Online Restore
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 1500, workloadNode: true}),
				backup: makeRestoringBackupSpecs(backupSpecs{
					nonRevisionHistory: true,
					cloud:              spec.GCE,
					version:            "24.1",
					workload:           tpccRestore{tpccRestoreOptions{warehouses: 150000, waitFraction: 0, workers: 100, maxRate: 1000}},
					customFixtureDir:   `'gs://cockroach-fixtures-us-east1/backups/tpc-c/v24.1/db/warehouses=150k?AUTH=implicit'`}),
				timeout:                3 * time.Hour,
				suites:                 registry.Suites(registry.Nightly),
				restoreUptoIncremental: 0,
			},
			linkPhaseTimeout:     10 * time.Minute, // typically takes 5 minutes
			downloadPhaseTimeout: 4 * time.Hour,    // typically takes 2 hours. Should get faster once we address #124767.
		},
	} {
		for _, runOnline := range []bool{true, false} {
			for _, useWorkarounds := range []bool{true, false} {
				for _, runWorkload := range []bool{true, false} {
					sp := sp
					runOnline := runOnline
					runWorkload := runWorkload
					useWorkarounds := useWorkarounds

					if runOnline {
						sp.namePrefix = "online/"
					} else {
						sp.namePrefix = "offline/"
						sp.skip = "used for ad hoc experiments"
					}
					if !runWorkload {
						sp.skip = "used for ad hoc experiments"
					}

					sp.namePrefix = sp.namePrefix + fmt.Sprintf("workload=%t", runWorkload)
					if !useWorkarounds {
						sp.skip = "used for ad hoc experiments"
						sp.namePrefix = sp.namePrefix + fmt.Sprintf("/workarounds=%t", useWorkarounds)
					}

					if sp.skip == "" && !backuptestutils.IsOnlineRestoreSupported() {
						sp.skip = "online restore is only tested on development branch"
					}

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
							rd := makeRestoreDriver(t, c, sp.restoreSpecs)
							rd.prepareCluster(ctx)

							restoreStats := runRestore(ctx, t, c, sp, rd, runOnline, runWorkload, useWorkarounds)
							if runOnline {
								require.NoError(t, postRestoreValidation(
									ctx,
									c,
									t.L(),
									sp.backup.workload.DatabaseName(),
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
			backup: makeRestoringBackupSpecs(backupSpecs{
				nonRevisionHistory: true,
				version:            fixtureFromMasterVersion,
				workload:           tpccRestore{opts: tpccRestoreOptions{warehouses: 10, workers: 1, waitFraction: 0, maxOps: 1000}}}),
			timeout:                15 * time.Minute,
			suites:                 registry.Suites(registry.Nightly),
			restoreUptoIncremental: 1,
			namePrefix:             "online/correctness",
			skip:                   "skip for now - flaky",
		}}
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

				rd := makeRestoreDriver(t, c, sp.restoreSpecs)
				rd.prepareCluster(ctx)

				runRestore(
					ctx, t, c, regRestoreSpecs, rd,
					false /* runOnline */, true /* runWorkload */, false, /* useWorkarounds */
				)
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

				runRestore(
					ctx, t, c, orSpecs, rd,
					true /* runOnline */, true /* runWorkload */, false, /* useWorkarounds */
				)
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
		func(stats map[string]clusterstats.StatSummary) (string, float64) {
			var timeToHealth time.Time
			healthyLatencyRatio := 1.25
			n := len(stats[latencyQueryKey].Value)
			rd.t.L().Printf("aggregating latency over %d data points", n)
			if n == 0 {
				return "", 0
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
			return description, rto
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
	tpccWorkload, ok := baseSp.backup.workload.(tpccRestore)
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
	baseSp.backup.workload = tpccWorkload
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

func runRestore(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	sp onlineRestoreSpecs,
	rd restoreDriver,
	runOnline, runWorkload, useWorkarounds bool,
) restoreStats {
	testStartTime := timeutil.Now()

	statsCollector, err := createStatCollector(ctx, rd)
	require.NoError(t, err)

	m := c.NewMonitor(ctx, sp.hardware.getCRDBNodes())
	var restoreStartTime, restoreEndTime time.Time
	m.Go(func(ctx context.Context) error {
		db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
		if err != nil {
			return err
		}
		defer db.Close()
		if useWorkarounds {
			// TODO(dt): what's the right value for this? How do we tune this
			// on the fly automatically during the restore instead of by-hand?
			// Context: We expect many operations to take longer than usual
			// when some or all of the data they touch is remote. For now this
			// is being blanket set to 1h manually, and a user's run-book
			// would need to do this by hand before an online restore and
			// reset it manually after, but ideally the queues would be aware
			// of remote-ness when they pick their own timeouts and pick
			// accordingly.
			if _, err := db.Exec("SET CLUSTER SETTING kv.queue.process.guaranteed_time_budget='1h'"); err != nil {
				return err
			}
			// TODO(dt): AC appears periodically reduce the workload to 0 QPS
			// during the download phase (sudden jumps from 0 to 2k qps to 0).
			// Disable for now until we figure out how to smooth this out.
			if _, err := db.Exec("SET CLUSTER SETTING admission.disk_bandwidth_tokens.elastic.enabled=false"); err != nil {
				return err
			}
			if _, err := db.Exec("SET CLUSTER SETTING admission.kv.enabled=false"); err != nil {
				return err
			}
			if _, err := db.Exec("SET CLUSTER SETTING admission.sql_kv_response.enabled=false"); err != nil {
				return err
			}
			if _, err := db.Exec("SET CLUSTER SETTING kv.consistency_queue.enabled=false"); err != nil {
				return err
			}
			if _, err := db.Exec("SET CLUSTER SETTING kv.range_merge.skip_external_bytes.enabled=true"); err != nil {
				return err
			}
		}
		opts := "WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
		if runOnline {
			opts = "WITH EXPERIMENTAL DEFERRED COPY, UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
		}
		if err := maybeAddSomeEmptyTables(ctx, rd); err != nil {
			return errors.Wrapf(err, "failed to add some empty tables")
		}
		restoreStartTime = timeutil.Now()
		if runOnline && sp.linkPhaseTimeout != 0 {
			timer := time.AfterFunc(sp.linkPhaseTimeout, func() {
				c.CaptureSideEyeSnapshot(ctx)
			})
			defer timer.Stop()
		}

		restoreCmd := rd.restoreCmd(fmt.Sprintf("DATABASE %s", sp.backup.workload.DatabaseName()), opts)
		t.L().Printf("Running %s", restoreCmd)
		if _, err = db.ExecContext(ctx, restoreCmd); err != nil {
			return err
		}
		if runOnline && sp.linkPhaseTimeout != 0 && sp.linkPhaseTimeout < timeutil.Since(restoreStartTime) {
			return errors.Newf("link phase took too long: %s greater than timeout %s", timeutil.Since(restoreStartTime), sp.linkPhaseTimeout)
		}
		return nil
	})
	m.Wait()
	restoreEndTime = timeutil.Now()

	workloadCtx, workloadCancel := context.WithCancel(ctx)
	mDownload := c.NewMonitor(workloadCtx, sp.hardware.getCRDBNodes())

	var workloadStartTime, workloadEndTime time.Time
	mDownload.Go(func(ctx context.Context) error {
		if !runWorkload {
			fmt.Printf("roachtest configured to skip running the foreground workload")
			return nil
		}
		workloadStartTime = timeutil.Now()
		err := sp.backup.workload.run(ctx, t, c, sp.hardware)
		// We expect the workload to return a context cancelled error because
		// the roachtest driver cancels the monitor's context after the download job completes
		if err != nil && ctx.Err() == nil {
			// Implies the workload context was not cancelled and the workload cmd returned a
			// different error.
			return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
		}
		rd.t.L().Printf("workload successfully finished")
		return nil
	})
	var downloadEndTimeLowerBound time.Time
	mDownload.Go(func(ctx context.Context) error {
		defer workloadCancel()
		if runOnline {
			downloadEndTimeLowerBound, err = waitForDownloadJob(ctx, c, t.L())
			if err != nil {
				return err
			}
			if sp.downloadPhaseTimeout != 0 && sp.downloadPhaseTimeout < timeutil.Since(restoreEndTime) {
				return errors.Newf("download phase took too long: %s greater than timeout %s", timeutil.Since(restoreEndTime), sp.downloadPhaseTimeout)
			}
		}
		if runWorkload {
			// Run the workload for at most 5 minutes.
			testRuntime := timeutil.Since(testStartTime)
			workloadDuration := sp.timeout - (testRuntime + time.Minute)
			maxWorkloadDuration := time.Minute * 5
			if workloadDuration > maxWorkloadDuration {
				workloadDuration = maxWorkloadDuration
			}
			t.L().Printf("let workload run for another %.2f minutes", workloadDuration.Minutes())
			time.Sleep(workloadDuration)
		}
		return nil
	})
	mDownload.Wait()
	if runWorkload {
		workloadEndTime = timeutil.Now()
	}
	return restoreStats{
		collector:                 statsCollector,
		restoreStartTime:          restoreStartTime,
		restoreEndTime:            restoreEndTime,
		workloadStartTime:         workloadStartTime,
		workloadEndTime:           workloadEndTime,
		downloadEndTimeLowerBound: downloadEndTimeLowerBound,
	}
}
