// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/stretchr/testify/require"
)

func registerInspectThoughput(r registry.Registry) {
	// Short run: 12 nodes × 8 CPUs, 500M rows, 1 index check, ~1 hour (in v25.4)
	r.Add(makeInspectThroughputTest(r, 12, 8, 500_000_000, 3*time.Hour, 1))

	// Long run: 12 nodes × 8 CPUs, 1B rows, 2 index checks (runs INSPECT twice: 1 index, then 2 indexes), ~5 hours (in v25.4)
	const indexesForLongRun = 2
	r.Add(makeInspectThroughputTest(r, 12, 8, 1_000_000_000, 11*time.Hour, indexesForLongRun))
}

// initInspectHistograms creates a histogram registry with multiple named metrics.
// All metrics share the same registry and are ticked together.
func initInspectHistograms(
	timeout time.Duration, t test.Test, e exporter.Exporter, metricNames []string,
) (*histogram.Registry, *bytes.Buffer) {
	reg := histogram.NewRegistryWithExporter(
		timeout,
		histogram.MockWorkloadName,
		e,
	)

	// Register all metric names in the same registry
	for _, name := range metricNames {
		reg.GetHandle().Get(name)
	}

	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)
	e.Init(&writer)

	return reg, bytesBuf
}

func makeInspectThroughputTest(
	r registry.Registry, numNodes, numCPUs, numRows int, length time.Duration, numChecks int,
) registry.TestSpec {
	// Define index names that will be created.
	indexNames := []string{
		"bulkingest_c_b_a_idx",
		"bulkingest_b_a_c_idx",
	}

	// Cap numChecks to the number of available indexes.
	const maxChecks = 2
	if numChecks > maxChecks {
		numChecks = maxChecks
	}
	if numChecks < 1 {
		numChecks = 1
	}

	return registry.TestSpec{
		Name:                fmt.Sprintf("inspect/throughput/bulkingest/nodes=%d/cpu=%d/rows=%d/checks=%d", numNodes, numCPUs, numRows, numChecks),
		Owner:               registry.OwnerSQLFoundations,
		Benchmark:           true,
		Cluster:             r.MakeClusterSpec(numNodes, spec.WorkloadNode(), spec.CPU(numCPUs)),
		CompatibleClouds:    registry.AllExceptAWS,
		Suites:              registry.Suites(registry.Nightly),
		Leases:              registry.LeaderLeases,
		Timeout:             length,
		SkipPostValidations: registry.PostValidationInspect,
		PostProcessPerfMetrics: func(test string, histogram *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
			// This callback is invoked once with all histogram summaries.
			// histogram.Summaries contains one entry for each named metric (checks=1, checks=2).
			metrics := roachtestutil.AggregatedPerfMetrics{}

			for _, summary := range histogram.Summaries {
				// Each summary corresponds to one INSPECT run with a specific check count.
				totalElapsed := summary.TotalElapsed
				if totalElapsed == 0 {
					continue
				}

				// Calculate throughput in rows/sec per CPU.
				// TotalElapsed is in milliseconds, convert to seconds.
				inspectDuration := totalElapsed / 1000
				if inspectDuration == 0 {
					inspectDuration = 1 // Avoid division by zero.
				}
				totalCPUs := int64(numNodes * numCPUs)
				throughput := roachtestutil.MetricPoint(float64(numRows) / float64(totalCPUs*inspectDuration))

				// Use the summary name (e.g., "checks=1") as part of the metric name.
				metrics = append(metrics, &roachtestutil.AggregatedMetric{
					Name:           fmt.Sprintf("%s_%s_throughput", test, summary.Name),
					Value:          throughput,
					Unit:           "rows/s/cpu",
					IsHigherBetter: true,
				})
			}

			return metrics, nil
		},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Configure column a to have sequential ascending values. The payload
			// column will be randomized and thus uncorrelated with the primary key
			// (a, b, c).
			bNum := 1000
			cNum := 1000
			aNum := numRows / (bNum * cNum)
			if c.IsLocal() {
				aNum = 100000
				bNum = 1
				cNum = 1
			}
			payloadBytes := 40

			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			disableRowCountValidation(t, db)

			// Import bulkingest data without the default index. We'll create custom
			// indexes based on the checks parameter.
			cmdImport := fmt.Sprintf(
				"./cockroach workload fixtures import bulkingest {pgurl:1} --a %d --b %d --c %d --payload-bytes %d --index-b-c-a=false",
				aNum, bNum, cNum, payloadBytes,
			)

			t.L().Printf("Importing bulkingest data")
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdImport)

			// Set up a single histogram registry with metrics for the requested number of checks.
			var metricNames []string
			for i := 1; i <= numChecks; i++ {
				metricNames = append(metricNames, fmt.Sprintf("checks=%d", i))
			}
			exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
			reg, perfBuf := initInspectHistograms(length*2, t, exporter, metricNames)
			defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, c.Node(1), "")

			// Helper to tick a specific named histogram.
			tickHistogram := func(name string) {
				reg.Tick(func(tick histogram.Tick) {
					if tick.Name == name {
						_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
					}
				})
			}

			// Build index lists for each check count.
			type checkConfig struct {
				metricName   string
				indexList    []string
				indexListSQL string
			}
			var configs []checkConfig
			for checks := 1; checks <= numChecks; checks++ {
				var indexList []string
				for i := 0; i < checks; i++ {
					indexList = append(indexList, indexNames[i])
				}
				configs = append(configs, checkConfig{
					metricName:   metricNames[checks-1],
					indexList:    indexList,
					indexListSQL: strings.Join(indexList, ", "),
				})
			}

			// Create the requested number of indexes with explicit names.
			allIndexDefinitions := []struct {
				name       string
				definition string
			}{
				{indexNames[0], "(c, b, a)"},
				{indexNames[1], "(b, a, c)"},
			}
			indexDefinitions := allIndexDefinitions[:numChecks]
			for i, idx := range indexDefinitions {
				indexSQL := fmt.Sprintf("CREATE INDEX %s ON bulkingest.bulkingest %s", idx.name, idx.definition)
				t.L().Printf("Creating index %d/%d: %s", i+1, len(indexDefinitions), indexSQL)
				if _, err := db.Exec(indexSQL); err != nil {
					t.Fatal(err)
				}
			}

			t.L().Printf("Computing table statistics manually")
			if _, err := db.Exec("CREATE STATISTICS stats FROM bulkingest.bulkingest"); err != nil {
				t.Fatal(err)
			}

			if _, err := db.Exec("USE bulkingest;"); err != nil {
				t.Fatal(err)
			}

			// Run INSPECT, ticking the corresponding histogram.
			for i, cfg := range configs {
				checks := i + 1
				t.L().Printf("Running INSPECT with %d check(s): %s", checks, cfg.indexListSQL)

				// Tick before starting INSPECT for this specific metric.
				tickHistogram(cfg.metricName)
				before := timeutil.Now()

				inspectSQL := fmt.Sprintf("INSPECT TABLE bulkingest.bulkingest WITH OPTIONS INDEX (%s)", cfg.indexListSQL)
				jobID := runInspectInBackground(ctx, t, db, inspectSQL)

				// Tick after INSPECT completes to capture elapsed time for this specific metric.
				tickHistogram(cfg.metricName)
				duration := timeutil.Since(before)
				t.L().Printf("INSPECT with %d check(s) took %v\n", checks, duration)

				// Query the job progress to get the total check count. Since each span
				// handles all indexes, we divide by the number of checks to get the
				// actual span count. This is important for debugging because INSPECT
				// concurrency is based on spans. So, knowing the number of spans vs
				// CPUs helps understand how well INSPECT parallelized this job.
				var jobTotalCheckCount int64
				querySQL := `
					SELECT coalesce(
						(crdb_internal.pb_to_json(
							'cockroach.sql.jobs.jobspb.Progress',
							value
						)->'inspect'->>'jobTotalCheckCount')::INT8,
						0
					)
					FROM system.job_info
					WHERE job_id = $1
					AND info_key = 'legacy_progress'`
				err := db.QueryRow(querySQL, jobID).Scan(&jobTotalCheckCount)
				if err != nil {
					t.L().Printf("Warning: failed to query job total check count: %v", err)
				} else {
					// Each span handles all indexes, so divide by number of checks to get span count
					spanCount := int(jobTotalCheckCount) / checks
					totalCPUs := numNodes * numCPUs
					spansPerCPU := float64(spanCount) / float64(totalCPUs)
					t.L().Printf("INSPECT completed %d checks across %d spans (%.2f spans/CPU with %d total CPUs)\n",
						jobTotalCheckCount, spanCount, spansPerCPU, totalCPUs)
				}
			}
		},
	}
}

// disableRowCountValidation disables automatic row count validation during import.
// This is necessary for INSPECT tests because row count validation uses INSPECT
// behind the covers, and we want to control when INSPECT runs.
func disableRowCountValidation(t test.Test, db *gosql.DB) {
	t.Helper()
	t.L().Printf("Disabling automatic row count validation")
	_, err := db.Exec("SET CLUSTER SETTING bulkio.import.row_count_validation.mode = 'off'")
	if err != nil {
		t.Fatal(err)
	}
}

// runInspectInBackground runs an INSPECT command with a short statement timeout,
// forcing it to run as a background job. It then polls the job until completion,
// reporting progress at 10% intervals. Returns the job ID.
func runInspectInBackground(
	ctx context.Context, t test.Test, db *gosql.DB, inspectSQL string,
) (jobID int64) {
	// Set a short statement timeout to force INSPECT to run as a background job.
	_, err := db.Exec("SET statement_timeout = '5s'")
	require.NoError(t, err)

	// Reset statement timeout after we're done.
	defer func() {
		_, resetErr := db.Exec("RESET statement_timeout")
		require.NoError(t, resetErr)
	}()

	_, err = db.Exec(inspectSQL)

	// This may fail if the INSPECT took longer than the statement_timeout to run.
	// So, we tolerate only statement timeout errors here.
	if err != nil {
		require.ErrorContains(t, err, "statement timeout")
	}

	// Get the INSPECT job ID.
	getJobIDSQL := `
		SELECT job_id
		FROM [SHOW JOBS]
		WHERE job_type = 'INSPECT'
		ORDER BY created DESC
		LIMIT 1`
	if err := db.QueryRow(getJobIDSQL).Scan(&jobID); err != nil {
		t.Fatalf("failed to get INSPECT job ID: %v", err)
	}
	t.L().Printf("INSPECT job ID: %d", jobID)

	// Poll the job until it completes, reporting progress at 10% intervals.
	const pollInterval = 5 * time.Second
	lastReportedThreshold := -1
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled while waiting for INSPECT job %d", jobID)
		case <-ticker.C:
			var status jobs.State
			var fractionCompleted float64
			checkJobSQL := `
				SELECT status, fraction_completed
				FROM [SHOW JOBS]
				WHERE job_id = $1`
			if err := db.QueryRow(checkJobSQL, jobID).Scan(&status, &fractionCompleted); err != nil {
				t.Fatalf("failed to query job %d status: %v", jobID, err)
			}

			// Report progress at 10% thresholds (0%, 10%, 20%, ..., 90%).
			currentThreshold := int(fractionCompleted * 10)
			if currentThreshold > lastReportedThreshold && currentThreshold < 10 {
				t.L().Printf("INSPECT job %d: %d%% complete", jobID, currentThreshold*10)
				lastReportedThreshold = currentThreshold
			}

			// Check if job is complete.
			switch status {
			case jobs.StateSucceeded:
				t.L().Printf("INSPECT job %d: 100%% complete (succeeded)", jobID)
				return jobID
			case jobs.StateFailed, jobs.StateCanceled:
				t.Fatalf("INSPECT job %d finished with status: %s", jobID, status)
			}
		}
	}
}
