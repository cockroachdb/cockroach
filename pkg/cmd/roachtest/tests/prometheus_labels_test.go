// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// TestPrometheusLabels is a manual test that can be run to examine available Prometheus labels
// This should NOT be run in CI but can be manually run to debug Prometheus query issues
func TestPrometheusLabels(t *testing.T) {
	// Skip test during automated test runs
	t.Skip("This test is for manual verification of Prometheus labels only")

	// Create a mock API directly for testing
	ctx := context.Background()
	api := metrics.CreateMockMetricsAPIForTesting(ctx)
	api.EnableDebugMode(true)

	// Output what queries would look like - this won't actually query Prometheus
	// but shows what the query string would be
	testQueries := []struct {
		name       string
		queryBuild func() error
	}{
		{
			name: "Basic metric query",
			queryBuild: func() error {
				return api.AssertThat("sql_txn_count").HasValueAtLeast(10)
			},
		},
		{
			name: "Query with node filter",
			queryBuild: func() error {
				return api.AssertThat("liveness_heartbeats_successful").ForNode("1").HasValueAtLeast(1)
			},
		},
		{
			name: "Rate query",
			queryBuild: func() error {
				return api.AssertThat("sql_select_count").OverLast("30s").Rate().HasRateAtLeast(0.1)
			},
		},
	}

	for _, tq := range testQueries {
		t.Run(tq.name, func(t *testing.T) {
			err := tq.queryBuild()
			require.NoError(t, err)
			t.Logf("Generated query: %s", api.GetLastQuery())
		})
	}

	t.Log("Note: To run an actual query against Prometheus, register the PrometheusLabelsTest using:")
	t.Log("func init() { registry.Tests = append(registry.Tests, PrometheusLabelsTest{}) }")
}

// PrometheusLabelsTest is a roachtest that connects to an actual Prometheus instance
// and lists available metrics and their labels
type PrometheusLabelsTest struct{}

// Name implements the Test interface.
func (t PrometheusLabelsTest) Name() string {
	return "prometheus-labels"
}

// Owner implements the Test interface.
func (t PrometheusLabelsTest) Owner() string {
	return "gourav"
}

// Run implements the Test interface.
func (t PrometheusLabelsTest) Run(ctx context.Context, tr test.Test, c Cluster) {
	// Set up Prometheus if needed
	promNode := c.Node(1)
	// Note: Depending on your cluster setup, you might need to customize this part

	// Connect to Prometheus
	promIP, err := c.ExternalIP(ctx, tr.L(), promNode)
	if err != nil {
		tr.Fatal(err)
	}

	tr.L().Printf("Connecting to Prometheus at %s:9090", promIP[0])

	client, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s:9090", promIP[0]),
	})
	if err != nil {
		tr.Fatal(err)
	}

	v1api := promv1.NewAPI(client)

	// List all available metrics
	labelNames, err := v1api.LabelNames(ctx, nil, nil, time.Now())
	if err != nil {
		tr.Fatal(err)
	}

	tr.L().Printf("Available label names: %v", labelNames)

	// Check if "node" is a valid label
	nodeValues, err := v1api.LabelValues(ctx, "node", nil, nil, time.Now())
	if err != nil {
		tr.L().Printf("Error querying 'node' label values: %v", err)
		// Continue execution even if this fails
	} else {
		tr.L().Printf("Values for 'node' label: %v", nodeValues)
	}

	// Get a list of metrics
	metrics, err := v1api.Series(ctx, []string{"{__name__=~'.+'}"},
		time.Now().Add(-1*time.Hour), time.Now())
	if err != nil {
		tr.Fatal(err)
	}

	// Create a map of metrics and their labels
	metricLabels := make(map[string]map[string][]string)
	for _, m := range metrics {
		name := string(m["__name__"])
		if _, ok := metricLabels[name]; !ok {
			metricLabels[name] = make(map[string][]string)
		}

		for labelName, labelValue := range m {
			if labelName == "__name__" {
				continue
			}

			values := metricLabels[name][labelName]
			found := false
			for _, v := range values {
				if v == string(labelValue) {
					found = true
					break
				}
			}

			if !found {
				metricLabels[name][labelName] = append(metricLabels[name][labelName], string(labelValue))
			}
		}
	}

	// Print out metrics with "node" label specifically
	tr.L().Printf("Metrics with 'node' label:")
	metricsWithNode := []string{}
	for name, labels := range metricLabels {
		if _, hasNode := labels["node"]; hasNode {
			metricsWithNode = append(metricsWithNode, name)
		}
	}

	sort.Strings(metricsWithNode)
	for _, name := range metricsWithNode {
		tr.L().Printf("  - %s (node values: %v)", name, metricLabels[name]["node"])
	}

	// Test a specific query with node label
	result, warnings, err := v1api.Query(ctx, `liveness_heartbeats_successful{node="1"}`, time.Now())
	if err != nil {
		tr.L().Printf("Error running test query: %v", err)
	} else {
		if len(warnings) > 0 {
			tr.L().Printf("Query warnings: %v", warnings)
		}
		tr.L().Printf("Query result: %v", result)
	}
}

// listMetricsByLabel lists all metrics that have a specific label
func listMetricsByLabel(label string, v1api promv1.API, ctx context.Context, tr test.Test) {
	query := fmt.Sprintf("{%s=~'.+'}", label)
	result, warnings, err := v1api.Query(ctx, query, time.Now())
	if err != nil {
		tr.L().Printf("Error querying for metrics with label %s: %v", label, err)
		return
	}

	if len(warnings) > 0 {
		tr.L().Printf("Query warnings: %v", warnings)
	}

	if vector, ok := result.(model.Vector); ok {
		tr.L().Printf("Found %d metrics with label %s", len(vector), label)
		for _, sample := range vector {
			metricName := string(sample.Metric["__name__"])
			labelValue := string(sample.Metric[model.LabelName(label)])
			tr.L().Printf("  - %s: %s = %s", metricName, label, labelValue)
		}
	} else {
		tr.L().Printf("Unexpected result type for label query: %T", result)
	}
}
