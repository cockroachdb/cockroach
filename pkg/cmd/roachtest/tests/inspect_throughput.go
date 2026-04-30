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
)

func registerInspectThoughput(r registry.Registry) {
	// Short run: 12 nodes × 8 CPUs, 500M rows, 1 index check, ~1 hour (in v25.4)
	r.Add(makeInspectThroughputTest(r, 12, 8, 500_000_000, 3*time.Hour, 1, true))

	// Long run: 12 nodes × 8 CPUs, 1B rows, 2 index checks (runs INSPECT twice: 1 index, then 2 indexes), ~5 hours (in v25.4)
	const indexesForLongRun = 2
	r.Add(makeInspectThroughputTest(r, 12, 8, 1_000_000_000, 11*time.Hour, indexesForLongRun, true))

	// Variants with admission control disabled, allowing INSPECT throughput
	// to be measured independently of elastic CPU control.
	r.Add(makeInspectThroughputTest(r, 12, 8, 500_000_000, 3*time.Hour, 1, false))
	r.Add(makeInspectThroughputTest(r, 12, 8, 1_000_000_000, 11*time.Hour, indexesForLongRun, false))

	// Multi-region: 12 CRDB nodes (4 per region) × 8 CPUs, 500M rows, 1 index check.
	// Measures INSPECT throughput on a geo-distributed cluster to establish a
	// cross-region performance baseline.

	// Non-complex key case: PK = (crdb_region, unique_val, filler_val)
	// unique_val immediately follows crdb_region.
	r.Add(makeInspectMultiRegionThroughputTest(r, 4, 8, 500_000_000, 5*time.Hour, true, false /* complexKey */))

	// Complex key cases: PK = (crdb_region, filler_val, unique_val)
	// unique_val does not immediately follow crdb_region.
	r.Add(makeInspectMultiRegionThroughputTest(r, 4, 8, 500_000_000, 5*time.Hour, true, true /* complexKey */))
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
	r registry.Registry,
	numNodes, numCPUs, numRows int,
	length time.Duration,
	numChecks int,
	admissionControl bool,
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

	name := fmt.Sprintf("inspect/throughput/bulkingest/nodes=%d/cpu=%d/rows=%d/checks=%d", numNodes, numCPUs, numRows, numChecks)
	if !admissionControl {
		name += "/elastic=false"
	}

	return registry.TestSpec{
		Name:                name,
		Owner:               registry.OwnerSQLQueries,
		Benchmark:           true,
		Cluster:             r.MakeClusterSpec(numNodes, spec.WorkloadNode(), spec.CPU(numCPUs)),
		CompatibleClouds:    registry.OnlyGCE,
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
				aNum = 100_000
				bNum = 1
				cNum = 1
			}
			payloadBytes := 40

			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			disableRowCountValidation(t, db)

			if !admissionControl {
				t.L().Printf("Disabling admission control for INSPECT")
				if _, err := db.ExecContext(ctx, "SET CLUSTER SETTING sql.inspect.admission_control.enabled = false"); err != nil {
					t.Fatal(err)
				}
			}

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

				inspectSQL := fmt.Sprintf("INSPECT TABLE bulkingest.bulkingest WITH OPTIONS INDEX (%s), DETACHED", cfg.indexListSQL)
				jobID := runInspectInBackground(ctx, t, db, inspectSQL, false /* expectIssue */)

				// Tick after INSPECT completes to capture elapsed time for this specific metric.
				tickHistogram(cfg.metricName)
				duration := timeutil.Since(before)
				t.L().Printf("INSPECT with %d check(s) took %v\n", checks, duration)

				logInspectJobStats(t, db, jobID, checks, numNodes*numCPUs)
			}
		},
	}
}

// makeInspectMultiRegionThroughputTest creates a benchmark that measures INSPECT
// throughput on a three-region cluster.
func makeInspectMultiRegionThroughputTest(
	r registry.Registry,
	nodesPerRegion, numCPUs, numRows int,
	length time.Duration,
	admissionControl bool,
	complexKey bool,
) registry.TestSpec {
	type regionSpec struct {
		name string
		zone string
	}
	regions := []regionSpec{
		{name: "us-east1", zone: "us-east1-b"},
		{name: "us-west1", zone: "us-west1-b"},
		{name: "europe-west2", zone: "europe-west2-b"},
	}
	numRegions := len(regions)
	numCRDBNodes := nodesPerRegion * numRegions

	var zones []string
	for _, reg := range regions {
		for j := 0; j < nodesPerRegion; j++ {
			zones = append(zones, reg.zone)
		}
	}
	zones = append(zones, regions[0].zone) // workload node in primary region

	name := fmt.Sprintf(
		"inspect/throughput/bulkingest/nodes=%d/cpu=%d/rows=%d/checks=1/unique/complex=%t",
		numCRDBNodes, numCPUs, numRows, complexKey,
	)
	if !admissionControl {
		name += "/elastic=false"
	}

	return registry.TestSpec{
		Name:      name,
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			numCRDBNodes+1, // +1 for workload node
			spec.CPU(numCPUs),
			spec.WorkloadNode(),
			spec.Geo(),
			spec.GCEZones(strings.Join(zones, ",")),
		),
		CompatibleClouds:    registry.OnlyGCE,
		Suites:              registry.Suites(registry.Nightly),
		Leases:              registry.LeaderLeases,
		Timeout:             length,
		SkipPostValidations: registry.PostValidationInspect,
		PostProcessPerfMetrics: func(
			test string, histogram *roachtestutil.HistogramMetric,
		) (roachtestutil.AggregatedPerfMetrics, error) {
			metrics := roachtestutil.AggregatedPerfMetrics{}

			for _, summary := range histogram.Summaries {
				totalElapsed := summary.TotalElapsed
				if totalElapsed == 0 {
					continue
				}

				inspectDuration := totalElapsed / 1000
				if inspectDuration == 0 {
					inspectDuration = 1
				}
				totalCPUs := int64(numCRDBNodes * numCPUs)
				throughput := roachtestutil.MetricPoint(
					float64(numRows) / float64(totalCPUs*inspectDuration),
				)

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
			bNum := 1000
			cNum := 1000
			aNum := numRows / (bNum * cNum)
			if c.IsLocal() {
				aNum = 100_000
				bNum = 1
				cNum = 1
			}
			payloadBytes := 40

			settings := install.MakeClusterSettings()

			for i, reg := range regions {
				startNode := i*nodesPerRegion + 1
				endNode := startNode + nodesPerRegion - 1
				nodes := c.Range(startNode, endNode)
				startOpts := option.DefaultStartOpts()
				startOpts.RoachprodOpts.ExtraArgs = append(
					startOpts.RoachprodOpts.ExtraArgs,
					fmt.Sprintf("--locality=region=%s,zone=%s", reg.name, reg.zone),
				)
				c.Start(ctx, t.L(), startOpts, settings, nodes)
			}

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			disableRowCountValidation(t, db)

			if !admissionControl {
				t.L().Printf("Disabling admission control for INSPECT")
				if _, err := db.ExecContext(ctx,
					"SET CLUSTER SETTING sql.inspect.admission_control.enabled = false",
				); err != nil {
					t.Fatal(err)
				}
			}

			// Set up the MR database.
			t.L().Printf("Creating a MR bulkingest database")
			if _, err := db.ExecContext(ctx, "CREATE DATABASE bulkingest"); err != nil {
				t.Fatal(err)
			}

			primaryRegion := regions[0].name
			if _, err := db.ExecContext(ctx, fmt.Sprintf(
				`ALTER DATABASE bulkingest SET PRIMARY REGION '%s'`, primaryRegion,
			)); err != nil {
				t.Fatal(err)
			}
			for _, reg := range regions[1:] {
				t.L().Printf("Adding region %s", reg.name)
				if _, err := db.ExecContext(ctx, fmt.Sprintf(
					`ALTER DATABASE bulkingest ADD REGION '%s'`, reg.name,
				)); err != nil {
					t.Fatal(err)
				}
			}

			// Create the MR table with a bulkingest friendly schema.
			//
			// Complex: PK becomes (crdb_region, a, unique_val) where unique_val
			//          does not immediately follow crdb_region.
			// Non-complex: PK becomes (crdb_region, unique_val, a) where unique_val
			//              immediately follows crdb_region.
			t.L().Printf("Creating REGIONAL BY ROW table with final schema (complexKey=%t)", complexKey)
			var createTableSQL string
			if complexKey {
				createTableSQL = `CREATE TABLE IF NOT EXISTS bulkingest.bulkingest (
					a INT,
					b INT,
					c INT,
					payload STRING,
					unique_val INT NOT VISIBLE DEFAULT unique_rowid(),
					PRIMARY KEY (a, unique_val)
				) LOCALITY REGIONAL BY ROW`
			} else {
				createTableSQL = `CREATE TABLE IF NOT EXISTS bulkingest.bulkingest (
					a INT,
					b INT,
					c INT,
					payload STRING,
					unique_val INT NOT VISIBLE DEFAULT unique_rowid(),
					PRIMARY KEY (unique_val, a)
				) LOCALITY REGIONAL BY ROW`
			}
			if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
				t.Fatal(err)
			}

			// Import will use the existing table and distribute the data.
			cmdImport := fmt.Sprintf(
				"./cockroach workload fixtures import bulkingest"+
					" {pgurl:1} --a %d --b %d --c %d --payload-bytes %d --index-b-c-a=false",
				aNum, bNum, cNum, payloadBytes,
			)
			t.L().Printf("Importing bulkingest data (will be distributed across %d regions)", numRegions)
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdImport)

			bulkDB := c.Conn(ctx, t.L(), 1, option.DBName("bulkingest"))
			defer bulkDB.Close()

			t.L().Printf("Computing table statistics")
			if _, err := bulkDB.Exec("CREATE STATISTICS stats FROM bulkingest"); err != nil {
				t.Fatal(err)
			}

			// Introduce test uniqueness violations
			t.L().Printf("Introducing test uniqueness violations")

			// Sample a unique_val from the first region
			var sampleUniqueVal int64
			err := bulkDB.QueryRow(`
					SELECT unique_val FROM bulkingest
					WHERE crdb_region = (SELECT min(crdb_region) FROM bulkingest)
					LIMIT 1
				`).Scan(&sampleUniqueVal)
			if err != nil {
				t.Fatal(err)
			}

			// Duplicate it to another region
			_, err = bulkDB.Exec(`
					UPDATE bulkingest
					SET unique_val = $1
					WHERE crdb_region > (SELECT min(crdb_region) FROM bulkingest)
					LIMIT 1
				`, sampleUniqueVal)
			if err != nil {
				t.Fatal(err)
			}

			t.L().Printf("Introduced violation with unique_val=%d", sampleUniqueVal)

			if complexKey {
				enableComplexKeyUniquenessValidation(t, bulkDB)
			}

			metricName := "inspect"
			exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
			reg, perfBuf := initInspectHistograms(
				length*2, t, exporter, []string{metricName},
			)
			defer roachtestutil.CloseExporter(
				ctx, exporter, t, c, perfBuf, c.Node(1), "",
			)

			tickHistogram := func(name string) {
				reg.Tick(func(tick histogram.Tick) {
					if tick.Name == name {
						_ = tick.Exporter.SnapshotAndWrite(
							tick.Hist, tick.Now, tick.Elapsed, &tick.Name,
						)
					}
				})
			}

			inspectSQL := "INSPECT TABLE bulkingest WITH OPTIONS DETACHED"

			t.L().Printf("Running INSPECT on multi-region cluster")
			tickHistogram(metricName)
			before := timeutil.Now()

			jobID := runInspectInBackground(ctx, t, bulkDB, inspectSQL, true /* expectIssue */)

			tickHistogram(metricName)
			duration := timeutil.Since(before)
			t.L().Printf("INSPECT on multi-region cluster took %v\n", duration)

			logInspectJobStats(t, bulkDB, jobID, 0 /* numChecks */, numCRDBNodes*numCPUs)

			// Verify that INSPECT found the introduced uniqueness violation.
			var issueCount int
			err = bulkDB.QueryRow(`
					SELECT count(*)
					FROM system.inspect_errors
					WHERE job_id = $1 AND error_type = 'duplicate_unique_value'
				`, jobID).Scan(&issueCount)
			if err != nil {
				t.Fatal(err)
			}
			if issueCount != 1 {
				t.Fatalf("Expected INSPECT to find one duplicate_unique_value issue, but found %d", issueCount)
			}
			t.L().Printf("INSPECT found %d duplicate_unique_value issue(s) as expected", issueCount)
		},
	}
}

// logInspectJobStats queries and logs the span count and parallelism metrics
// for a completed INSPECT job.
func logInspectJobStats(t test.Test, db *gosql.DB, jobID int64, numChecks int, totalCPUs int) {
	t.Helper()
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
		return
	}

	var spanCount int
	if numChecks > 0 {
		// Each span handles all indexes, so divide by number of checks to get span count.
		spanCount = int(jobTotalCheckCount) / numChecks
	} else {
		spanCount = int(jobTotalCheckCount)
	}
	spansPerCPU := float64(spanCount) / float64(totalCPUs)
	t.L().Printf(
		"INSPECT completed %d checks across %d spans (%.2f spans/CPU with %d total CPUs)\n",
		jobTotalCheckCount, spanCount, spansPerCPU, totalCPUs,
	)
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

// enableComplexKeyUniquenessValidation enables validation of complex keys (i.e.
// the unique column does not immediately follow the region column) during the
// uniqueness check.
func enableComplexKeyUniquenessValidation(t test.Test, db *gosql.DB) {
	t.Helper()
	t.L().Printf("Enabling complex key uniqueness validation")
	_, err := db.Exec("SET CLUSTER SETTING sql.inspect.uniqueness_check.complex_keys.enabled = true;")
	if err != nil {
		t.Fatal(err)
	}
}

// runInspectInBackground runs an INSPECT DETACHED command, which starts the
// job in the background and returns immediately. It then looks up the job ID
// and polls until completion, reporting progress at 10% intervals. Returns the
// job ID.
func runInspectInBackground(
	ctx context.Context, t test.Test, db *gosql.DB, inspectSQL string, expectIssue bool,
) (jobID int64) {
	// INSPECT ... DETACHED starts the job in the background and returns
	// immediately, avoiding the need for a statement timeout hack.
	if _, err := db.Exec(inspectSQL); err != nil {
		t.Fatalf("failed to start INSPECT job: %v", err)
	}

	// Look up the INSPECT job ID.
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
				if status == jobs.StateFailed && expectIssue {
					t.L().Printf("INSPECT job %d finished with status: %s (expected)", jobID, status)
					return jobID
				}
				t.Fatalf("INSPECT job %d finished with status: %s", jobID, status)
			}
		}
	}
}
