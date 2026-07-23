// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// datadoggen generates Datadog dashboards from various sources.
//
// Usage:
//
//	datadoggen convert-grafana <input.json> [-o output.json]
//	datadoggen from-metrics --prefix <prefix> [--yaml <metrics.yaml>] [-o output.json]
//
// Query format (following Datadog best practices from OI-Querying self-hosted DB metrics):
//   - Counter: sum:cockroachdb.metric{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)
//   - Gauge: avg:cockroachdb.metric{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)
//   - Histogram: p99:cockroachdb.metric{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)
//
// Datadog Query Guidelines:
//   - Rollup interval: 10s (matches CockroachDB scrape interval for both Cloud and tsdump)
//   - Counter metrics: Always use as_rate() with MAX rollup aggregation
//   - Gauge metrics: Use AVG rollup aggregation (or MAX/MIN for spikes/dips)
//   - Histogram/Percentile metrics: Use MAX rollup aggregation (percentiles represent upper bounds)
//
// Important Notes:
//   - Datadog charts limit to 1500 data points; interval may be ignored for large ranges
//   - Self-hosted uploaded histograms use different naming (e.g., crdb.tsdump.sql.service.latency_p99)
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "datadoggen",
		Short: "Generate Datadog dashboards from Grafana dashboards or CockroachDB metrics",
		Example: `  # Convert Grafana dashboard (output: grafana_dashboard_datadog.json)
  datadoggen convert-grafana grafana_dashboard.json

  # Generate from Go metrics (output: mma_metrics_datadog.json)
  datadoggen from-metrics -g pkg/kv/kvserver/allocator/mmaprototype/mma_metrics.go

  # Filter by prefixes (output: mma_rebalancing_datadog.json)
  datadoggen from-metrics --prefix mma,rebalancing

  # Search for metrics (output: latency_datadog.json)
  datadoggen from-metrics --search "latency"

  # Get queries to stdout (for piping)
  datadoggen from-metrics --search "sql.service" -q --quiet

  # Generate for self-hosted tsdump format (crdb.tsdump.* prefix, $upload_id tag)
  datadoggen from-metrics --search "sql.service" --tsdump

  # Convert Grafana dashboard for tsdump
  datadoggen convert-grafana grafana_dashboard.json --tsdump`,
	}

	rootCmd.AddCommand(
		newConvertGrafanaCmd(),
		newFromMetricsCmd(),
	)

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
