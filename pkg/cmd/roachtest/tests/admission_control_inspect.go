// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	roachtestgrafana "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
)

func registerInspectAdmissionControl(r registry.Registry) {
	r.Add(makeInspectAdmissionControlTest(r, 4, 8, 25_000_000, 5*time.Hour))
}

// makeInspectAdmissionControlTest creates a test that sets up a CRDB cluster, loads it
// up with bulkingest data, and sets up a foreground read-only workload. It then
// runs INSPECT twice: once with the default low QoS priority and once with
// normal priority, to measure the impact on foreground latency.
//
// The test ensures sufficient ranges are created so that INSPECT work is
// well-distributed across the CPUs.
func makeInspectAdmissionControlTest(
	r registry.Registry, numCRDBNodes, numCPUs, numRows int, timeout time.Duration,
) registry.TestSpec {
	// totalNodes includes CRDB nodes + 1 workload node
	totalNodes := numCRDBNodes + 1

	return registry.TestSpec{
		Name:             fmt.Sprintf("inspect/admission-control/nodes=%d/cpu=%d/rows=%d", numCRDBNodes, numCPUs, numRows),
		Timeout:          timeout,
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          r.MakeClusterSpec(totalNodes, spec.CPU(numCPUs), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		PostProcessPerfMetrics: func(test string, histogram *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
			metrics := roachtestutil.AggregatedPerfMetrics{}

			for _, summary := range histogram.Summaries {
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
				totalCPUs := int64(numCRDBNodes * numCPUs)
				throughput := roachtestutil.MetricPoint(float64(numRows) / float64(totalCPUs*inspectDuration))

				// Use the summary name (e.g., "admission_control_enabled") as part of the metric name.
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
			// Adjust for local testing
			rowsToImport := numRows
			targetRanges := numCRDBNodes * numCPUs * 2
			if c.IsLocal() {
				rowsToImport = 100_000
				targetRanges = 4
			}

			// Calculate bulkingest parameters to achieve target row count
			// We'll use b × c = 1000, so a = numRows / 1000
			bNum := 1000
			cNum := 1000
			aNum := rowsToImport / (bNum * cNum)

			// For local, use simpler values
			if c.IsLocal() {
				aNum = 100000
				bNum = 1
				cNum = 1
			}

			payloadBytes := 40

			c.Start(
				ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule),
				install.MakeClusterSettings(), c.CRDBNodes(),
			)

			{
				promCfg := &prometheus.Config{}
				promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0])
				promCfg.WithNodeExporter(c.All().InstallNodes())
				promCfg.WithCluster(c.CRDBNodes().InstallNodes())
				promCfg.WithGrafanaDashboardJSON(roachtestgrafana.SnapshotAdmissionControlGrafanaJSON)
				promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
					"/", makeWorkloadScrapeNodes(c.WorkloadNode().InstallNodes()[0], []workloadInstance{
						{nodes: c.WorkloadNode()},
					})))
				_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, []workloadInstance{{nodes: c.WorkloadNode()}})
				defer cleanupFunc()
			}

			baselineDuration, err := time.ParseDuration(roachtestutil.IfLocal(c, "30s", "5m"))
			if err != nil {
				t.Fatal(err)
			}

			// Set up histogram tracking for throughput metrics
			metricNames := []string{"calibration", "admission_control_enabled", "admission_control_disabled"}
			exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
			reg, perfBuf := initInspectHistograms(timeout*2, t, exporter, metricNames)
			defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, c.Node(1), "")

			// Helper to tick a specific named histogram
			tickHistogram := func(name string) {
				reg.Tick(func(tick histogram.Tick) {
					if tick.Name == name {
						_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
					}
				})
			}

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			// Helper to add Grafana annotations.
			addAnnotation := func(text string, startTime time.Time) {
				err := c.AddGrafanaAnnotation(ctx, t.L(), grafana.AddAnnotationRequest{
					Text:      text,
					StartTime: startTime.UnixMilli(),
					EndTime:   timeutil.Now().UnixMilli(),
				})
				if err != nil {
					t.L().Printf("failed to add grafana annotation: %v", err)
				}
			}

			// Helper to run INSPECT with timing, metrics, and logging.
			runInspect := func(description, histogramName, annotationText string) time.Duration {
				t.Status("running " + description)
				tickHistogram(histogramName)
				startTime := timeutil.Now()
				if _, err := db.ExecContext(ctx, "SET enable_inspect_command = true"); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, "INSPECT TABLE bulkingest.bulkingest"); err != nil {
					t.Fatal(err)
				}
				tickHistogram(histogramName)
				duration := timeutil.Since(startTime)
				addAnnotation(annotationText, startTime)

				totalCPUs := numCRDBNodes * numCPUs
				throughput := float64(rowsToImport) / float64(totalCPUs) / duration.Seconds()
				t.L().Printf("%s completed in %v (%.2f rows/s/cpu)\n", description, duration, throughput)

				return duration
			}

			if !t.SkipInit() {
				t.Status("importing bulkingest dataset")

				// Disable automatic row count validation during import
				disableRowCountValidation(t, db)

				// Import bulkingest data with the default secondary index
				cmdImport := fmt.Sprintf(
					"./cockroach workload fixtures import bulkingest {pgurl:1} --a %d --b %d --c %d --payload-bytes %d",
					aNum, bNum, cNum, payloadBytes,
				)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdImport)

				// Split the table into the target number of ranges
				t.Status(fmt.Sprintf("splitting table into ~%d ranges", targetRanges))
				splitSQL := fmt.Sprintf("ALTER TABLE bulkingest.bulkingest SPLIT AT SELECT (i * %d) // %d FROM generate_series(1, %d-1) AS i",
					aNum, targetRanges, targetRanges)
				if _, err := db.Exec(splitSQL); err != nil {
					t.Fatal(err)
				}

				// Scatter the ranges to ensure even distribution before INSPECT.
				t.Status("scattering ranges across cluster")
				if _, err := db.Exec("ALTER TABLE bulkingest.bulkingest SCATTER"); err != nil {
					t.Fatal(err)
				}

				// Initialize kv workload for foreground traffic
				t.Status("initializing kv workload")
				splits := roachtestutil.IfLocal(c, " --splits=3", " --splits=100")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv"+splits+" {pgurl:1}")
			}

			// Run calibration INSPECT without workload to determine how long the
			// workload should run.
			calibrationDuration := runInspect(
				"calibration INSPECT (no workload, admission control enabled)",
				"calibration",
				"INSPECT (AC, no load)",
			)

			// Estimate total workload duration:
			// - INSPECT with admission control (~9.5× calibrationDuration)
			// - INSPECT without admission control (~0.5× calibrationDuration)
			// - baseline overhead before and in between runs (2× baselineDuration)
			// Add a 25% safety buffer.
			estimatedDuration := baselineDuration*2 + calibrationDuration*10
			workloadDuration := time.Duration(float64(estimatedDuration) * 1.25)
			t.L().Printf("Setting workload duration to %v (based on calibration: %v)\n", workloadDuration, calibrationDuration)

			// Run a read-only kv workload to match INSPECT’s read-only behavior.
			// This avoids write pressure, keeps the dataset static, and ensures
			// comparisons reflect load differences, not data drift.
			t.Status(fmt.Sprintf("starting read-only kv workload for %v", workloadDuration))
			m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())

			m.Go(func(ctx context.Context) error {
				concurrency := roachtestutil.IfLocal(c, "8", "256")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()),
					fmt.Sprintf("./cockroach workload run kv --read-percent=100 --duration=%s --concurrency=%s {pgurl%s}",
						workloadDuration, concurrency, c.CRDBNodes()),
				)
				return nil
			})

			t.Status(fmt.Sprintf("waiting %v for workload to run before starting INSPECT", baselineDuration))
			time.Sleep(baselineDuration)

			_ = runInspect(
				"INSPECT under load with admission control enabled",
				"admission_control_enabled",
				"INSPECT (AC, load)",
			)

			t.Status(fmt.Sprintf("waiting %v between INSPECTs", baselineDuration))
			time.Sleep(baselineDuration)

			// Disable admission control for second INSPECT. This should have more of an
			// impact to the foreground workload.
			t.Status("disabling admission control for INSPECT")
			if _, err := db.ExecContext(ctx, "SET CLUSTER SETTING sql.inspect.admission_control.enabled = false"); err != nil {
				t.Fatal(err)
			}

			_ = runInspect(
				"INSPECT under load with admission control disabled",
				"admission_control_disabled",
				"INSPECT (no AC, load)",
			)

			// Wait for workload to complete.
			m.Wait()
		},
	}
}
