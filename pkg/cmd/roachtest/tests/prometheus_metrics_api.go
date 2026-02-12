// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/pkg/errors"
)

// registerPrometheusMetricsTest registers a test that sets up a cluster with Prometheus
// and runs assertions on collected metrics.
func registerPrometheusMetricsTest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "metrics/prometheus",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(4), spec.WorkloadNodeCount(1), spec.PreferLocalSSD()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPrometheusMetricsTest(ctx, t, c)
		},
	})
}

// runPrometheusMetricsTest is the implementation of the Prometheus metrics test.
func runPrometheusMetricsTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		// Check if Prometheus is explicitly enabled for local testing
		if os.Getenv("RUN_LOCAL_PROMETHEUS") != "1" {
			t.L().Printf("Local cluster detected, skipping Prometheus setup. Set RUN_LOCAL_PROMETHEUS=1 to enable local Prometheus.")
			return
		}

		// Check if we have a Prometheus address for connecting to an existing instance
		if os.Getenv("PROMETHEUS_ADDR") == "" {
			t.Skip("skipping prometheus metrics test on local cluster without PROMETHEUS_ADDR environment variable. Set PROMETHEUS_ADDR to connect to an existing Prometheus instance.")
			return
		}
	}

	// Start the cluster
	t.Status("starting cluster")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	// Set up Prometheus configuration
	t.Status("setting up prometheus configuration")
	workloadNode := c.WorkloadNode().InstallNodes()[0]
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(workloadNode)
	promCfg.WithNodeExporter(c.CRDBNodes().InstallNodes())
	promCfg.WithCluster(c.CRDBNodes().InstallNodes())

	// Explicitly disable Grafana
	promCfg.Grafana.Enabled = false

	// Start Prometheus (without Grafana) - but only for non-local testing
	// or if explicitly enabled for local testing
	var api metrics.MetricsAPI

	if !c.IsLocal() || os.Getenv("RUN_LOCAL_PROMETHEUS") == "1" {
		t.Status("starting prometheus")
		quietLogger, err := t.L().ChildLogger("start-prometheus", logger.QuietStdout, logger.QuietStderr)
		if err != nil {
			t.Fatal(err)
		}

		if err := c.StartGrafana(ctx, quietLogger, promCfg); err != nil {
			t.Fatal(err)
		}

		// Cleanup function for Prometheus
		defer func() {
			if t.IsDebug() {
				return // Skip cleanup in debug mode
			}
			t.Status("stopping prometheus")
			if err := c.StopGrafana(context.Background(), quietLogger, t.ArtifactsDir()); err != nil {
				t.L().Printf("error shutting down prometheus: %s", err)
			}
		}()
	}

	// Create metrics API instance
	t.Status("initializing metrics API")
	var err error
	api, err = metrics.CreateMetricsAPI(ctx, t, c, promCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := api.Close(); err != nil {
			t.L().Printf("error closing metrics API: %v", err)
		}
	}()

	// Enable verbose logging for metrics API
	t.Status("enabling verbose logging for metrics API")
	api.EnableVerboseLogging(true)

	// Initialize TPCC workload with 50 warehouses
	t.Status("initializing TPCC workload with 50 warehouses")
	warehouses := 50
	cmd := fmt.Sprintf("./cockroach workload init tpcc --warehouses=%d {pgurl:1-3}", warehouses)
	if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd); err != nil {
		t.Fatal(err)
	}

	// Run TPCC workload for 5 minutes
	t.Status("running TPCC workload for 5 minutes")
	duration := 5 * time.Minute
	cmd = fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --duration=%s {pgurl:1-3}", warehouses, duration)

	// Create an error group for managing concurrent tasks
	g := t.NewErrorGroup(task.WithContext(ctx))

	// Add the workload task to the error group
	g.Go(func(ctx context.Context, l *logger.Logger) error {
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
	})

	// Wait for 1 minute to let metrics accumulate
	t.Status("waiting for metrics to accumulate")
	select {
	case <-ctx.Done():
		return
	case <-time.After(1 * time.Minute):
		// Continue with the test
	}

	// Only check metrics if we have a valid API
	if api != nil {
		// Enable verbose logging to see query details
		api.EnableVerboseLogging(true)
		t.L().Printf("Prometheus URL: %s", os.Getenv("PROMETHEUS_ADDR"))

		// Start testing metrics while the workload is running
		t.Status("verifying basic system metrics")

		// Only checking metrics that we've confirmed exist in the Prometheus instance
		testMetrics := []struct {
			name        string
			query       func()
			description string
		}{
			{
				name: "liveness_livenodes",
				query: func() {
					api.Query("liveness_livenodes").AssertHasValue(t, float64(len(c.CRDBNodes())))
				},
				description: "live node count",
			},
			{
				name: "sql_query_count",
				query: func() {
					api.Query("sql_query_count").
						WithLabel("job", "cockroach-n1").
						OverLast("2m").
						Rate().
						Sum().
						AssertHasValueAtLeast(t, 10)
				},
				description: "SQL query rate",
			},
			{
				name: "sys_host_net_recv_bytes",
				query: func() {
					api.Query("sys_host_net_recv_bytes").
						OverLast("2m").
						Rate().
						Sum().
						AssertHasValueAtLeast(t, 100)
				},
				description: "network traffic",
			},
			{
				name: "sql_query_count_node1",
				query: func() {
					api.Query("sql_query_count").
						ForNode("1").
						OverLast("2m").
						Rate().
						AssertHasValueAtLeast(t, 0.1)
				},
				description: "SQL queries on node 1",
			},
			{
				name: "sql_query_count_node2",
				query: func() {
					api.Query("sql_query_count").
						ForNode("2").
						OverLast("2m").
						Rate().
						Avg().
						AssertHasValueAtLeast(t, 1)
				},
				description: "SQL queries on node 2",
			},
			{
				name: "sql_query_count_node3",
				query: func() {
					api.Query("sql_query_count").
						ForNode("3").
						OverLast("2m").
						Rate().
						Max().
						AssertHasValueAtLeast(t, 1)
				},
				description: "SQL queries on node 3",
			},
		}

		// Since we're using direct assertion with the testing object,
		// we don't need to collect errors anymore.
		for _, testMetric := range testMetrics {
			t.Status(fmt.Sprintf("checking %s", testMetric.description))
			testMetric.query()
			t.L().Printf("Test succeeded: %s", testMetric.name)
		}

		// Example of using direct assertions with Fataler
		t.Status("checking SQL query rate (direct assertion)")
		api.Query("sql_query_count").
			WithLabel("job", "cockroach-n1").
			OverLast("2m").
			Rate().
			Sum().
			AssertHasValueAtLeast(t, 10)

		// Example: Check max CPU usage - using direct value query
		t.Status("checking max CPU usage")
		api.Query("sys_cpu_combined_percent_normalized").
			AssertHasValueAtMost(t, 0.95)

		// Example of using the AssertEventually methods
		t.Status("demonstrating recovery assertions")

		// Get baseline QPS to use as reference - using Rate() and Avg() here
		baselineQPS, err := api.Query("sql_query_count").
			ForNode("1").
			OverLast("30s").
			Rate().
			Avg().
			Value()
		if err != nil {
			t.L().Printf("Failed to get baseline QPS: %v, using default value", err)
			baselineQPS = 10 // Use default if we can't get real value
		}
		t.L().Printf("Baseline QPS: %f", baselineQPS)

		// Example 1: Wait for a metric to reach a threshold using simple predicate
		t.Status("example: waiting for network traffic to exceed threshold")
		api.Query("sys_host_net_recv_bytes").
			OverLast("1m").
			Rate().
			Sum().
			AssertEventually(t, func(v float64) bool {
				return v >= 50 // Network traffic should reach 50 bytes/sec within timeout
			}, 30*time.Second)

		// Example 2: Use AssertRecoversTo for simpler recovery testing
		t.Status("example: verify CPU usage won't exceed threshold for long")
		// First verify CPU is currently normal (under threshold)
		normalCPU := 0.9 // 90% is our threshold for "normal" CPU
		api.Query("sys_cpu_combined_percent_normalized").
			ForNode("1").
			AssertHasValueAtMost(t, normalCPU)

		// If CPU briefly spiked, it should recover within 30 seconds
		t.Status("example: verify CPU would recover if it spiked")
		api.Query("sys_cpu_combined_percent_normalized").
			ForNode("1").
			AssertDropsBelow(t, normalCPU, 30*time.Second)

		// Example 3: Assert that QPS remains above threshold
		t.Status("example: verify QPS remains above minimum threshold")
		minExpectedQPS := baselineQPS * 0.5 // 50% of baseline
		api.Query("sql_query_count").
			ForNode("1").
			OverLast("30s").
			Rate().
			Sum().
			AssertRecoversTo(t, minExpectedQPS, 30*time.Second)

		// Examples of using the Scan function for more complex data extraction
		t.Status("examples: using Scan function for complex data extraction")

		// Example 1: Scan a simple value into a variable
		var qpsValue float64
		err = api.Query("sql_query_count").
			ForNode("1").
			OverLast("1m").
			Rate().
			Avg().
			Scan(&qpsValue)
		if err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan QPS value"))
		} else {
			t.L().Printf("Current average QPS: %f", qpsValue)
		}

		// Example 2: Scan into a map (for label-based results)
		var nodeMetrics map[string]float64
		err = api.Query("liveness_livenodes").
			Scan(&nodeMetrics)
		if err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan node metrics"))
		} else {
			for node, value := range nodeMetrics {
				t.L().Printf("Node %s: %f", node, value)
			}
		}

		// Example 2b: Scan max CPU usage by node into a map
		var nodeCPUMetrics map[string]float64
		err = api.Query("sys_cpu_combined_percent_normalized").
			Scan(&nodeCPUMetrics)
		if err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan CPU metrics"))
		} else {
			for node, cpuMax := range nodeCPUMetrics {
				t.L().Printf("Node %s max CPU: %.1f%%", node, cpuMax)
			}
		}

		// Example 3: Scan into a custom struct for more complex processing
		type TimeSeriesPoint struct {
			Timestamp time.Time
			Value     float64
		}
		var timeSeriesData []TimeSeriesPoint
		err = api.Query("sql_query_count").
			OverLast("10m").
			Scan(&timeSeriesData)
		if err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan time series data"))
		} else {
			t.L().Printf("Retrieved %d data points", len(timeSeriesData))
			// Process time series data
			if len(timeSeriesData) > 0 {
				latestPoint := timeSeriesData[len(timeSeriesData)-1]
				t.L().Printf("Latest data point at %v: %f",
					latestPoint.Timestamp.Format(time.RFC3339), latestPoint.Value)
			}
		}

		t.Status("prometheus metrics test completed successfully")
	} else {
		t.Status("skipping metrics checks for local test without Prometheus")
	}

	// Wait for workload to complete
	t.Status("waiting for workload to complete")
	if err := g.WaitE(); err != nil {
		t.Fatal(err)
	}

	t.Status("prometheus metrics test completed successfully")
}
