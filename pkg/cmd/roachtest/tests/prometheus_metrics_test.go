// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

// registerPrometheusMetricsTest registers a test that sets up a cluster with Prometheus
// and runs assertions on collected metrics.
func registerPrometheusMetricsTest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "metrics/prometheus",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(4), spec.WorkloadNodeCount(1), spec.PreferLocalSSD()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPrometheusMetricsTest(ctx, t, c)
		},
	})
}

func runPrometheusMetricsTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		// For local testing, we'll still run but use environment variables
		if os.Getenv("RUN_LOCAL_PROMETHEUS") != "1" {
			t.L().Printf("For local testing, set RUN_LOCAL_PROMETHEUS=1 to enable. Will skip Prometheus checks.")
		}
	}

	// Stage the cockroach binary
	t.Status("staging cockroach binary")
	if err := c.Stage(ctx, t.L(), "cockroach", "", "", c.All()); err != nil {
		t.Fatal(err)
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
			if err := c.StopGrafana(ctx, quietLogger, t.ArtifactsDir()); err != nil {
				t.L().ErrorfCtx(ctx, "error shutting down prometheus: %s", err)
			}
		}()

		// Create metrics API instance
		t.Status("initializing metrics API")
		api, err = metrics.GetOrCreateMetricsAPI(ctx, t, c, promCfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := api.Close(); err != nil {
				t.L().Printf("error closing metrics API: %v", err)
			}
		}()

		// Enable debug mode for metrics API
		t.Status("enabling debug mode for metrics API")
		api.EnableDebugMode(true)
	}

	// Initialize TPCC workload with 50 warehouses
	t.Status("initializing TPCC workload with 50 warehouses")
	warehouses := 50
	cmd := fmt.Sprintf("./cockroach workload init tpcc --warehouses=%d {pgurl:1}", warehouses)
	if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd); err != nil {
		t.Fatal(err)
	}

	// Run TPCC workload for 4 minutes
	t.Status("running TPCC workload for 4 minutes")
	duration := 4 * time.Minute
	cmd = fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --duration=%s {pgurl:1}", warehouses, duration)

	// Create a context that can be cancelled to clean up the goroutine
	workloadCtx, workloadCancel := context.WithCancel(ctx)
	defer workloadCancel() // Ensure the goroutine is cleaned up no matter how we exit

	errCh := make(chan error, 1)
	go func() {
		select {
		case <-workloadCtx.Done():
			// Context cancelled, exit the goroutine
			errCh <- workloadCtx.Err()
		default:
			// Run the workload
			errCh <- c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		}
	}()

	// Wait for 1 minute to let metrics accumulate
	t.Status("waiting for metrics to accumulate")
	time.Sleep(1 * time.Minute)

	// Only check metrics if we have a valid API
	if api != nil {
		// Enable debug mode to see query details
		api.EnableDebugMode(true)
		t.L().Printf("DEBUG: Prometheus URL: %s", os.Getenv("PROMETHEUS_ADDR"))

		// Start testing metrics while the workload is running
		t.Status("verifying basic system metrics")

		// Only checking metrics that we've confirmed exist in the Prometheus instance
		testMetrics := []struct {
			name        string
			query       func() error
			description string
		}{
			{
				name: "liveness_livenodes",
				query: func() error {
					return api.Query("liveness_livenodes").AssertHasValue(float64(len(c.CRDBNodes())))
				},
				description: "live node count",
			},
			{
				name: "sql_query_count",
				query: func() error {
					return api.Query("sql_query_count").
						WithLabel("job", "cockroach-n1").
						OverLast("2m").
						Rate().
						Sum().
						AssertHasValueAtLeast(10)
				},
				description: "SQL query rate",
			},
			{
				name: "sys_host_net_recv_bytes",
				query: func() error {
					return api.Query("sys_host_net_recv_bytes").
						OverLast("2m").
						Rate().
						Sum().
						AssertHasValueAtLeast(100)
				},
				description: "network traffic",
			},
			{
				name: "sql_query_count_node1",
				query: func() error {
					return api.Query("sql_query_count").
						ForNode("1").
						OverLast("2m").
						AssertHasValueAtLeast(1)
				},
				description: "SQL queries on node 1",
			},
			{
				name: "sql_query_count_node2",
				query: func() error {
					return api.Query("sql_query_count").
						ForNode("2").
						OverLast("2m").
						AssertHasValueAtLeast(1)
				},
				description: "SQL queries on node 2",
			},
			{
				name: "sql_query_count_node3",
				query: func() error {
					return api.Query("sql_query_count").
						ForNode("3").
						OverLast("2m").
						AssertHasValueAtLeast(1)
				},
				description: "SQL queries on node 3",
			},
		}

		// Run each test and log detailed debug info on failure
		var testFailures []string
		for _, testMetric := range testMetrics {
			t.Status(fmt.Sprintf("checking %s", testMetric.description))
			t.L().Printf("DEBUG: Testing metric %s", testMetric.name)

			err := testMetric.query()
			if err != nil {
				t.L().Printf("DEBUG: Metric test failed: %v", err)
				t.L().Printf("DEBUG: Last query: %s", api.GetLastQuery())
				testFailures = append(testFailures, fmt.Sprintf("%s: %v", testMetric.name, err))
			} else {
				t.L().Printf("DEBUG: Test succeeded: %s = %f", api.GetLastQuery(), api.GetLastQueryResult())
			}
		}

		// If any tests failed, report them all at once instead of failing on the first one
		if len(testFailures) > 0 {
			t.Fatal(fmt.Sprintf("Metric tests failed: %s", strings.Join(testFailures, "; ")))
		}

		t.Status("prometheus metrics test completed successfully")
	} else {
		t.Status("skipping metrics checks for local test without Prometheus")
	}

	// Wait for workload to complete
	t.Status("waiting for workload to complete")
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	t.Status("prometheus metrics test completed successfully")
}
