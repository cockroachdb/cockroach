// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
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
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/lib/pq"
)

func registerInspectThoughput(r registry.Registry) {
	// Short run: 12 nodes × 8 CPUs, 500M rows, 1 index check, ~1 hour (in v25.4)
	r.Add(makeInspectThroughputTest(r, 12, 8, 500_000_000, 3*time.Hour, 1))

	// Long run: 12 nodes × 8 CPUs, 1B rows, 2 index checks (runs INSPECT twice: 1 index, then 2 indexes), ~5 hours (in v25.4)
	// TODO(148365): Until we have INSPECT syntax, we cannot execute the INSPECT
	// job on a subset of indexes. Set this to 2 when INSPECT SQL is available.
	const indexesForLongRun = 1
	r.Add(makeInspectThroughputTest(r, 12, 8, 1_000_000_000, 8*time.Hour, indexesForLongRun))
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
		Name:             fmt.Sprintf("inspect/throughput/bulkingest/nodes=%d/cpu=%d/rows=%d/checks=%d", numNodes, numCPUs, numRows, numChecks),
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(numNodes, spec.WorkloadNode(), spec.CPU(numCPUs)),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.LeaderLeases,
		Timeout:          length,
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

			// Enable scrub jobs for EXPERIMENTAL SCRUB to use job system.
			if _, err := db.Exec("SET enable_scrub_job = on"); err != nil {
				t.Fatal(err)
			}

			// Run INSPECT, ticking the corresponding histogram.
			for i, cfg := range configs {
				checks := i + 1
				t.L().Printf("Running INSPECT with %d check(s): %s", checks, cfg.indexListSQL)

				// Tick before starting INSPECT for this specific metric.
				tickHistogram(cfg.metricName)
				before := timeutil.Now()

				// TODO(148365): Update to use INSPECT syntax when SQL is ready.
				scrubSQL := fmt.Sprintf("EXPERIMENTAL SCRUB TABLE bulkingest.bulkingest WITH OPTIONS INDEX (%s)", cfg.indexListSQL)
				if _, err := db.Exec(scrubSQL); err != nil {
					t.Fatal(err)
				}

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
					WHERE job_id = (
						SELECT job_id
						FROM [SHOW JOBS]
						WHERE job_type = 'INSPECT'
						ORDER BY created DESC
						LIMIT 1
					)
					AND info_key = 'legacy_progress'`
				err := db.QueryRow(querySQL).Scan(&jobTotalCheckCount)
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
	_, err := db.Exec("SET CLUSTER SETTING bulkio.import.row_count_validation.unsafe.mode = 'off'")
	// If we get an error, it's because the cluster setting is considered unsafe.
	// So, we need extract the interlock key from the error.
	if err != nil {
		var pqErr *pq.Error
		if !errors.As(err, &pqErr) {
			t.Fatalf("expected pq.Error, got %T: %v", err, err)
		}
		if !strings.HasPrefix(pqErr.Detail, "key: ") {
			t.Fatalf("expected error detail to start with 'key: ', got: %s", pqErr.Detail)
		}
		interlockKey := strings.TrimPrefix(pqErr.Detail, "key: ")

		// Set the interlock key and retry.
		if _, err := db.Exec("SET unsafe_setting_interlock_key = $1", interlockKey); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("SET CLUSTER SETTING bulkio.import.row_count_validation.unsafe.mode = 'off'"); err != nil {
			t.Fatal(err)
		}
	}
}
