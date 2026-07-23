// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/cli"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerOnlineRestorePerfBreakdown(r registry.Registry) {
	for _, useFullBackupAOST := range []bool{true, false} {
		sp := onlineRestoreSpecs{
			restoreSpecs: restoreSpecs{
				hardware: makeHardwareSpecs(hardwareSpecs{nodes: 5}),
				backup: backupSpecs{
					cloud:   spec.GCE,
					fixture: SmallFixture,
				},
				fullBackupOnly:  true,
				skipFingerprint: true,
				timeout:         2 * time.Hour,
				suites:          registry.Suites(registry.Nightly),
			},
			workload: tpccRestore{
				opts: tpccRunOpts{waitFraction: 0, workers: 100, maxRate: 300},
			},
		}
		if useFullBackupAOST {
			sp.namePrefix = "online/perf-breakdown/full-only"
		} else {
			sp.namePrefix = "online/perf-breakdown/with-inc"
		}
		sp.initTestName()
		useFullBackup := useFullBackupAOST
		r.Add(registry.TestSpec{
			Name:                      sp.testName,
			Owner:                     registry.OwnerDisasterRecovery,
			Benchmark:                 true,
			Cluster:                   sp.hardware.makeClusterSpecs(r),
			Timeout:                   sp.timeout,
			EncryptionSupport:         registry.EncryptionAlwaysDisabled,
			CompatibleClouds:          sp.backup.CompatibleClouds(),
			Suites:                    sp.suites,
			TestSelectionOptOutSuites: sp.suites,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runOnlineRestorePerfBreakdown(ctx, t, c, sp, useFullBackup)
			},
		})
	}
}

// stallPoint defines a download percentage at which to pause and measure.
type stallPoint struct {
	targetPct float64
	label     string
}

var defaultStallPoints = []stallPoint{
	{targetPct: 0, label: "download_0"},
	{targetPct: 0.30, label: "download_30"},
	{targetPct: 0.60, label: "download_60"},
	{targetPct: 0.90, label: "download_90"},
	{targetPct: 1.0, label: "download_100"},
}

// querySnapshot captures per-query-type latency at a stall point.
type querySnapshot struct {
	Type         string  `json:"type"`
	P50Ms        float64 `json:"p50_ms"`
	P95Ms        float64 `json:"p95_ms"`
	P99Ms        float64 `json:"p99_ms"`
	PMaxMs       float64 `json:"pMax_ms"`
	Throughput   float64 `json:"qps"`
	Errors       int     `json:"errors"`
	TickCount    int     `json:"tick_count"`
	StalledTicks int     `json:"stalled_ticks"`
}

// clusterMetricsSnapshot captures cluster-level metrics at a stall point.
type clusterMetricsSnapshot struct {
	LogicalBytes  uint64 `json:"logical_bytes"`
	ExternalBytes uint64 `json:"external_bytes"`
	RangeCount    int    `json:"range_count"`
	L0SizeBytes   int64  `json:"l0_size_bytes"`
	L6SizeBytes   int64  `json:"l6_size_bytes"`
	ReadAmp       int    `json:"read_amp"`
}

// stallPointReport is the output for a single stall point measurement.
type stallPointReport struct {
	Label              string                  `json:"label"`
	DownloadPct        float64                 `json:"download_pct"`
	Queries            []querySnapshot         `json:"queries"`
	BundlesCollected   int                     `json:"bundles_collected"`
	MeasurementTimeUTC string                  `json:"measurement_time_utc"`
	ClusterMetrics     *clusterMetricsSnapshot `json:"cluster_metrics,omitempty"`
}

// breakdownReport is the full output of the test.
type breakdownReport struct {
	StallPoints []stallPointReport `json:"stall_points"`
}

func runOnlineRestorePerfBreakdown(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	sp onlineRestoreSpecs,
	useFullBackupAOST bool,
) {
	rd := makeRestoreDriver(ctx, t, c, sp.restoreSpecs)
	rd.prepareCluster(ctx)

	db, err := c.ConnE(ctx, t.L(), c.Node(1)[0])
	require.NoError(t, err)
	defer db.Close()

	// Override the AOST to use the full backup end time if requested,
	// ensuring a single-layer restore with no incremental overlap.
	if useFullBackupAOST {
		defer disableUseBackupsWithIDs(ctx, t, db)()
		aostCmd := sp.restoreSpecs.getFullBackupEndTimeCmd(ctx, t, rd.collectionURI)
		var fullAOST string
		require.NoError(t, db.QueryRowContext(ctx, aostCmd).Scan(&fullAOST))
		rd.aost = fullAOST
		t.L().Printf("overriding AOST to full backup end time: %s", fullAOST)
	}

	clusterSettings := []string{
		// Speed up job adoption so resume/pause transitions are fast.
		"jobs.registry.interval.adopt='5s'",
	}
	for _, setting := range clusterSettings {
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING %s", setting))
		require.NoError(t, err)
	}

	// Set pausepoint so the download job pauses before any downloads begin.
	_, err = db.ExecContext(ctx,
		"SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")
	require.NoError(t, err)

	// Run the online restore (link phase).
	opts := "WITH EXPERIMENTAL DEFERRED COPY, UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
	restoreCmd := rd.restoreCmd(ctx,
		fmt.Sprintf("DATABASE %s", sp.backup.fixture.DatabaseName()), opts)
	t.L().Printf("starting online restore: %s", restoreCmd)
	_, err = db.ExecContext(ctx, restoreCmd)
	if err != nil {
		rd.markFixtureWithFailure(ctx)
		t.Fatal(err)
	}
	t.L().Printf("link phase complete")

	// Find the download job.
	var downloadJobID int
	err = db.QueryRowContext(ctx,
		`SELECT job_id FROM [SHOW JOBS]
		 WHERE description LIKE '%Background Data Download%'
		   AND job_type = 'RESTORE'
		 ORDER BY created DESC LIMIT 1`).Scan(&downloadJobID)
	require.NoError(t, err)
	t.L().Printf("download job ID: %d", downloadJobID)

	// Wait for the job to actually be paused at the pausepoint.
	require.NoError(t, waitForJobStatus(ctx, t.L(), db, downloadJobID, jobs.StatePaused))

	// Clear the pausepoint so future resumes don't re-pause at it.
	_, err = db.ExecContext(ctx,
		"SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	require.NoError(t, err)

	// Capture initial external bytes at 0% downloaded. We use this as the
	// denominator when computing download fraction from live external byte
	// counts, which update in real-time (unlike fraction_completed which
	// only flushes every minute).
	var initialExternalBytes uint64
	err = db.QueryRowContext(ctx, jobutils.GetExternalBytesForConnectedTenant).Scan(&initialExternalBytes)
	require.NoError(t, err)
	t.L().Printf("initial external bytes: %d", initialExternalBytes)

	crdbNodes := sp.hardware.getCRDBNodes()
	workloadNode := crdbNodes[0]
	workloadOutputFile := "/tmp/workload_output.json"
	bundleBaseDir := "/tmp/bundles"

	tpccFixture, ok := sp.backup.fixture.(TpccFixture)
	if !ok {
		t.Fatal("expected TpccFixture")
	}
	warehouses := tpccFixture.WorkloadWarehouses

	// Use enough workers that max-rate is the bottleneck, not worker
	// count. This keeps offered load constant so latency measurements
	// are independent of throughput (open-loop).
	workloadCmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Flag("display-format", "incremental-json").
		Flag("warehouses", warehouses).
		Flag("workers", 1000).
		Flag("wait", 0).
		Flag("max-rate", 300).
		Option("tolerate-errors").
		Arg("{pgurl:%d-%d}", crdbNodes[0], crdbNodes[len(crdbNodes)-1]).
		String()
	workloadCmd += fmt.Sprintf(" > %s 2>&1", workloadOutputFile)

	t.L().Printf("starting workload: %s", workloadCmd)
	mon := t.NewErrorGroup()
	workloadCancel := mon.GoWithCancel(
		func(ctx context.Context, l *logger.Logger) error {
			return c.RunE(ctx,
				option.WithNodes(c.Node(workloadNode)),
				workloadCmd)
		})
	defer workloadCancel()

	// Give the workload time to initialize connections and start
	// generating queries before resolving fingerprints.
	time.Sleep(60 * time.Second)
	fingerprints, err := resolveTpccFingerprints(ctx, db)
	if err != nil {
		t.L().Printf("warning: could not resolve fingerprints: %v", err)
	}
	t.L().Printf("resolved %d TPCC fingerprints", len(fingerprints))

	var report breakdownReport
	// Track which bundle IDs we've already downloaded so we don't
	// re-download at the next stall point.
	downloadedBundleIDs := make(map[int]bool)

	for _, stallPt := range defaultStallPoints {
		t.L().Printf("=== measuring at stall point: %s (target %.0f%%) ===",
			stallPt.label, stallPt.targetPct*100)

		if stallPt.targetPct == 0 {
			// Already paused at 0% from the pausepoint -- nothing to do.
		} else if stallPt.targetPct >= 1.0 {
			// Job was resumed at end of previous stall point. Wait for it
			// to finish.
			require.NoError(t, waitForJobStatus(ctx, t.L(), db, downloadJobID, jobs.StateSucceeded))
		} else {
			// Job was either resumed at end of previous stall (running) or
			// paused at pausepoint (0% case). Ensure it's running, then
			// poll until we hit the target and pause.
			var status string
			_ = db.QueryRowContext(ctx,
				`SELECT status FROM [SHOW JOB $1]`, downloadJobID).Scan(&status)
			if status != string(jobs.StateRunning) {
				_, err = db.ExecContext(ctx, "RESUME JOB $1", downloadJobID)
				require.NoError(t, err)
				require.NoError(t, waitForJobStatus(ctx, t.L(), db, downloadJobID, jobs.StateRunning))
			}
			actualPct, err := waitForDownloadPctByExternalBytes(
				ctx, t.L(), db, initialExternalBytes, stallPt.targetPct)
			require.NoError(t, err)
			t.L().Printf("download reached %.1f%%, pausing", actualPct*100)
			_, err = db.ExecContext(ctx, "PAUSE JOB $1", downloadJobID)
			require.NoError(t, err)
			require.NoError(t, waitForJobStatus(ctx, t.L(), db, downloadJobID, jobs.StatePaused))
		}

		// Brief settle, then request bundles early so they start collecting
		// during the measurement window.
		time.Sleep(5 * time.Second)
		requestBundles(ctx, t.L(), db, fingerprints)

		// Read the actual download percentage from live external bytes.
		actualPct := stallPt.targetPct
		if stallPt.targetPct < 1.0 && initialExternalBytes > 0 {
			var currentBytes uint64
			if err := db.QueryRowContext(ctx,
				jobutils.GetExternalBytesForConnectedTenant).Scan(&currentBytes); err == nil {
				actualPct = 1.0 - float64(currentBytes)/float64(initialExternalBytes)
			}
		}

		// Also log job fraction_completed for comparison.
		var jobFrac float64
		if err := db.QueryRowContext(ctx,
			`SELECT fraction_completed FROM [SHOW JOB $1]`,
			downloadJobID).Scan(&jobFrac); err == nil {
			t.L().Printf("external bytes pct: %.1f%%, job fraction_completed: %.1f%%",
				actualPct*100, jobFrac*100)
		}

		// Collect cluster metrics.
		clusterMetrics := collectClusterMetrics(ctx, t.L(), db)

		// Measurement window: wait 30s for ticks to accumulate.
		t.L().Printf("waiting 30s for measurement window")
		time.Sleep(30 * time.Second)

		// Tail workload output and aggregate over ~30 ticks per op type.
		queries, err := captureWorkloadSnapshot(ctx, t.L(), c, workloadNode, workloadOutputFile)
		if err != nil {
			t.L().Printf("warning: failed to capture workload snapshot: %v", err)
		}

		stallReport := stallPointReport{
			Label:              stallPt.label,
			DownloadPct:        actualPct,
			Queries:            queries,
			MeasurementTimeUTC: timeutil.Now().UTC().Format(time.RFC3339),
			ClusterMetrics:     clusterMetrics,
		}

		// Resume download job before collecting bundles so download
		// progress overlaps with bundle collection.
		if stallPt.targetPct < 1.0 {
			_, _ = db.ExecContext(ctx, "RESUME JOB $1", downloadJobID)
		}

		// Download bundles on the workload node (in parallel, while
		// download job runs toward next stall point).
		bundleCount := downloadBundlesToRemote(ctx, t, c, db, workloadNode,
			bundleBaseDir, stallPt.label, downloadedBundleIDs)
		stallReport.BundlesCollected = bundleCount

		report.StallPoints = append(report.StallPoints, stallReport)

		t.L().Printf("stall point %s: captured %d query types, %d bundles",
			stallPt.label, len(queries), bundleCount)
	}

	// Stop workload.
	workloadCancel()

	// Fetch all bundles from the workload node in one shot.
	fetchBundlesFromRemote(ctx, t, c, workloadNode, bundleBaseDir)

	// Write summary report.
	writeBreakdownReport(t, report)

	// Write perf summary stats for the snowflake pipeline.
	writePerfSummaryStats(t, c, report)
}

// resolveTpccFingerprints queries statement statistics to find TPCC query
// fingerprints that have been executed.
func resolveTpccFingerprints(ctx context.Context, db *gosql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT metadata->>'query'
		 FROM crdb_internal.statement_statistics
		 WHERE app_name = 'tpcc'
		   AND metadata->>'query' NOT LIKE '%crdb_internal%'
		   AND metadata->>'query' NOT LIKE '%SET %'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fingerprints []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		fingerprints = append(fingerprints, fp)
	}
	return fingerprints, rows.Err()
}

// requestBundles requests a statement bundle for each fingerprint.
func requestBundles(ctx context.Context, l *logger.Logger, db *gosql.DB, fingerprints []string) {
	for _, fp := range fingerprints {
		_, err := db.ExecContext(ctx,
			`SELECT crdb_internal.request_statement_bundle($1, 0::FLOAT, '0s'::INTERVAL, '5m'::INTERVAL)`,
			fp)
		if err != nil {
			l.Printf("warning: failed to request bundle for %q: %v",
				truncate(fp, 60), err)
		}
	}
	l.Printf("requested bundles for %d fingerprints", len(fingerprints))
}

// captureWorkloadSnapshot tails the workload JSON output and aggregates
// the last ~30 ticks per operation type, computing the median of each
// per-tick percentile. This smooths out single-tick noise.
func captureWorkloadSnapshot(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, workloadNode int, outputFile string,
) ([]querySnapshot, error) {
	// 5 op types × 30 ticks = 150 lines, grab extra for safety.
	result, err := c.RunWithDetails(ctx, l,
		option.WithNodes(c.Node(workloadNode)),
		fmt.Sprintf("tail -250 %s", outputFile))
	if err != nil {
		return nil, errors.Wrap(err, "tailing workload output")
	}
	if len(result) == 0 || result[0].Stdout == "" {
		return nil, errors.New("no workload output captured")
	}

	ticks := cli.ParseOutput(strings.NewReader(result[0].Stdout))
	if len(ticks) == 0 {
		return nil, errors.New("no valid ticks parsed from workload output")
	}

	// Group ticks by operation type, keeping the last 30 per type.
	ticksByType := make(map[string][]cli.Tick)
	for _, tick := range ticks {
		ticksByType[tick.Type] = append(ticksByType[tick.Type], tick)
	}

	var snapshots []querySnapshot
	for opType, opTicks := range ticksByType {
		// Keep last 30 ticks.
		if len(opTicks) > 30 {
			opTicks = opTicks[len(opTicks)-30:]
		}

		// Separate active ticks (non-zero throughput) from stalled ticks.
		var active []cli.Tick
		stalledCount := 0
		for _, t := range opTicks {
			if t.Throughput > 0 {
				active = append(active, t)
			} else {
				stalledCount++
			}
		}

		// Compute medians from active ticks only.
		p50s := make([]float64, len(active))
		p95s := make([]float64, len(active))
		p99s := make([]float64, len(active))
		pmaxs := make([]float64, len(active))
		qps := make([]float64, len(active))
		totalErrs := 0
		for i, t := range active {
			p50s[i] = t.P50.Seconds() * 1000
			p95s[i] = t.P95.Seconds() * 1000
			p99s[i] = t.P99.Seconds() * 1000
			pmaxs[i] = t.PMax.Seconds() * 1000
			qps[i] = t.Throughput
			totalErrs = t.Errs
		}

		snapshots = append(snapshots, querySnapshot{
			Type:         opType,
			P50Ms:        median(p50s),
			P95Ms:        median(p95s),
			P99Ms:        median(p99s),
			PMaxMs:       median(pmaxs),
			Throughput:   median(qps),
			Errors:       totalErrs,
			TickCount:    len(active),
			StalledTicks: stalledCount,
		})
		l.Printf("  %s: %d active ticks (%d stalled), median p50=%.1fms p99=%.1fms qps=%.0f",
			opType, len(active), stalledCount, median(p50s), median(p99s), median(qps))
	}
	return snapshots, nil
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

// downloadBundlesToRemote downloads statement bundles to a directory on the
// workload node, in parallel. The bundles are fetched to the local artifacts
// directory later in one batch via fetchBundlesFromRemote.
func downloadBundlesToRemote(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	db *gosql.DB,
	workloadNode int,
	bundleBaseDir string,
	stallLabel string,
	alreadyDownloaded map[int]bool,
) int {
	rows, err := db.QueryContext(ctx,
		`SELECT id FROM system.statement_diagnostics ORDER BY collected_at DESC`)
	if err != nil {
		t.L().Printf("warning: could not query diagnostics: %v", err)
		return 0
	}
	defer rows.Close()

	var bundleIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			continue
		}
		if alreadyDownloaded[id] {
			continue
		}
		bundleIDs = append(bundleIDs, id)
	}
	if len(bundleIDs) == 0 {
		return 0
	}

	// Build a shell script that downloads all bundles in parallel.
	remoteDir := fmt.Sprintf("%s/%s", bundleBaseDir, stallLabel)
	var sb strings.Builder
	fmt.Fprintf(&sb, "mkdir -p %s && ", remoteDir)
	for _, id := range bundleIDs {
		fmt.Fprintf(&sb,
			"./cockroach statement-diag download %d --url={pgurl:1} %s/bundle_%d.zip & ",
			id, remoteDir, id)
	}
	sb.WriteString("wait")

	if err := c.RunE(ctx,
		option.WithNodes(c.Node(workloadNode)),
		sb.String()); err != nil {
		t.L().Printf("warning: failed to download bundles: %v", err)
		return 0
	}

	for _, id := range bundleIDs {
		alreadyDownloaded[id] = true
	}
	t.L().Printf("downloaded %d bundles to %s on workload node", len(bundleIDs), remoteDir)
	return len(bundleIDs)
}

// fetchBundlesFromRemote tars the bundle directory on the workload node and
// fetches it to the local artifacts directory in a single transfer.
func fetchBundlesFromRemote(
	ctx context.Context, t test.Test, c cluster.Cluster, workloadNode int, bundleBaseDir string,
) {
	remoteTar := "/tmp/bundles.tar.gz"
	tarCmd := fmt.Sprintf("tar czf %s -C %s . 2>/dev/null || true", remoteTar, bundleBaseDir)
	if err := c.RunE(ctx, option.WithNodes(c.Node(workloadNode)), tarCmd); err != nil {
		t.L().Printf("warning: failed to tar bundles: %v", err)
		return
	}

	localTar := filepath.Join(t.ArtifactsDir(), "bundles.tar.gz")
	if err := c.Get(ctx, t.L(), remoteTar, localTar, c.Node(workloadNode)); err != nil {
		t.L().Printf("warning: failed to fetch bundles tarball: %v", err)
		return
	}

	// Extract into artifacts directory.
	bundleDir := filepath.Join(t.ArtifactsDir(), "bundles")
	if err := os.MkdirAll(bundleDir, 0755); err != nil {
		t.L().Printf("warning: could not create bundles dir: %v", err)
		return
	}

	cmd := fmt.Sprintf("tar xzf %s -C %s", localTar, bundleDir)
	if out, err := runLocalCmd(cmd); err != nil {
		t.L().Printf("warning: failed to extract bundles: %v (output: %s)", err, out)
		return
	}
	t.L().Printf("fetched and extracted bundles to %s", bundleDir)
}

// waitForDownloadPctByExternalBytes polls external bytes every 6s to
// compute download progress with real-time precision (as opposed to
// fraction_completed which only flushes every minute).
func waitForDownloadPctByExternalBytes(
	ctx context.Context,
	l *logger.Logger,
	db *gosql.DB,
	initialExternalBytes uint64,
	targetPct float64,
) (float64, error) {
	const pollInterval = 6 * time.Second

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
			var currentBytes uint64
			if err := db.QueryRowContext(ctx,
				jobutils.GetExternalBytesForConnectedTenant).Scan(&currentBytes); err != nil {
				return 0, errors.Wrap(err, "querying external bytes")
			}
			if currentBytes == 0 {
				return 1.0, nil
			}
			frac := 1.0 - float64(currentBytes)/float64(initialExternalBytes)
			l.Printf("download progress: %.1f%% (target: %.0f%%)",
				frac*100, targetPct*100)
			if frac >= targetPct {
				return frac, nil
			}
		}
	}
}

// waitForJobStatus polls until the job reaches the desired status.
func waitForJobStatus(
	ctx context.Context, l *logger.Logger, db *gosql.DB, jobID int, target jobs.State,
) error {
	const pollInterval = 2 * time.Second
	const timeout = 10 * time.Minute

	deadline := timeutil.Now().Add(timeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if timeutil.Now().After(deadline) {
				return errors.Newf("timed out waiting for job %d to reach %s", jobID, target)
			}
			var status string
			if err := db.QueryRowContext(ctx,
				`SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status); err != nil {
				return errors.Wrap(err, "querying job status")
			}
			if status == string(target) {
				return nil
			}
		}
	}
}

// writeBreakdownReport writes the full report as JSON to the test artifacts.
func writeBreakdownReport(t test.Test, report breakdownReport) {
	reportPath := filepath.Join(t.ArtifactsDir(), "summary_report.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.L().Printf("warning: failed to marshal report: %v", err)
		return
	}
	if err := os.WriteFile(reportPath, data, 0644); err != nil {
		t.L().Printf("warning: failed to write report: %v", err)
		return
	}
	t.L().Printf("wrote breakdown report to %s", reportPath)
}

// writePerfSummaryStats emits key metrics to the snowflake pipeline via
// WritePerfSummaryStats.
func writePerfSummaryStats(t test.Test, c cluster.Cluster, report breakdownReport) {
	if len(report.StallPoints) < 2 {
		return
	}

	var stats roachtestutil.AggregatedPerfMetrics

	for _, sp := range report.StallPoints {
		for _, q := range sp.Queries {
			stats = append(stats, &roachtestutil.AggregatedMetric{
				Name:  fmt.Sprintf("%s_p50_%s", q.Type, sp.Label),
				Value: roachtestutil.MetricPoint(q.P50Ms), Unit: "ms",
			})
			stats = append(stats, &roachtestutil.AggregatedMetric{
				Name:  fmt.Sprintf("%s_p95_%s", q.Type, sp.Label),
				Value: roachtestutil.MetricPoint(q.P95Ms), Unit: "ms",
			})
			stats = append(stats, &roachtestutil.AggregatedMetric{
				Name:  fmt.Sprintf("%s_p99_%s", q.Type, sp.Label),
				Value: roachtestutil.MetricPoint(q.P99Ms), Unit: "ms",
			})
			stats = append(stats, &roachtestutil.AggregatedMetric{
				Name:  fmt.Sprintf("%s_qps_%s", q.Type, sp.Label),
				Value: roachtestutil.MetricPoint(q.Throughput), Unit: "qps",
				IsHigherBetter: true,
			})
		}
		if sp.ClusterMetrics != nil {
			m := sp.ClusterMetrics
			stats = append(stats,
				&roachtestutil.AggregatedMetric{
					Name:  fmt.Sprintf("l0_size_mb_%s", sp.Label),
					Value: roachtestutil.MetricPoint(float64(m.L0SizeBytes) / (1024 * 1024)), Unit: "MB",
				},
				&roachtestutil.AggregatedMetric{
					Name:  fmt.Sprintf("l6_size_mb_%s", sp.Label),
					Value: roachtestutil.MetricPoint(float64(m.L6SizeBytes) / (1024 * 1024)), Unit: "MB",
				},
				&roachtestutil.AggregatedMetric{
					Name:  fmt.Sprintf("read_amp_%s", sp.Label),
					Value: roachtestutil.MetricPoint(float64(m.ReadAmp)),
				},
				&roachtestutil.AggregatedMetric{
					Name:  fmt.Sprintf("external_gb_%s", sp.Label),
					Value: roachtestutil.MetricPoint(float64(m.ExternalBytes) / (1024 * 1024 * 1024)), Unit: "GB",
				},
			)
		}
	}

	if len(stats) == 0 {
		return
	}
	if err := roachtestutil.WritePerfSummaryStats(t, c, stats); err != nil {
		t.L().Printf("warning: failed to write perf summary stats: %v", err)
	}
}

// collectClusterMetrics grabs cluster-level stats from crdb_internal.
func collectClusterMetrics(
	ctx context.Context, l *logger.Logger, db *gosql.DB,
) *clusterMetricsSnapshot {
	m := &clusterMetricsSnapshot{}

	_ = db.QueryRowContext(ctx, jobutils.GetExternalBytesForConnectedTenant).Scan(&m.ExternalBytes)

	_ = db.QueryRowContext(ctx,
		`SELECT sum(range_count)::INT FROM crdb_internal.kv_store_status`).Scan(&m.RangeCount)

	_ = db.QueryRowContext(ctx,
		`SELECT sum(logical_bytes)::INT FROM crdb_internal.kv_store_status`).Scan(&m.LogicalBytes)

	_ = db.QueryRowContext(ctx,
		`SELECT COALESCE(sum((metrics->>'storage.l0-level-size')::FLOAT)::INT, 0)
		 FROM crdb_internal.kv_store_status`).Scan(&m.L0SizeBytes)

	_ = db.QueryRowContext(ctx,
		`SELECT COALESCE(sum((metrics->>'storage.l6-level-size')::FLOAT)::INT, 0)
		 FROM crdb_internal.kv_store_status`).Scan(&m.L6SizeBytes)

	_ = db.QueryRowContext(ctx,
		`SELECT COALESCE(max((metrics->>'rocksdb.read-amplification')::FLOAT)::INT, 0)
		 FROM crdb_internal.kv_store_status`).Scan(&m.ReadAmp)

	l.Printf("cluster metrics: external_bytes=%d logical_bytes=%d ranges=%d l0=%dMB l6=%dMB read_amp=%d",
		m.ExternalBytes, m.LogicalBytes, m.RangeCount,
		m.L0SizeBytes/(1024*1024), m.L6SizeBytes/(1024*1024), m.ReadAmp)
	return m
}

func runLocalCmd(cmd string) (string, error) {
	out, err := exec.Command("bash", "-c", cmd).CombinedOutput()
	return string(out), err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
