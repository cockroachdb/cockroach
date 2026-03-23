// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestBuildDatadogQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Set up the global rollupInterval for testing
	rollupInterval = 10

	testCases := []struct {
		name             string
		metricName       string
		metricsNameMap   map[string]string
		metricTypeMap    map[string]string
		groupByStorage   bool
		expectedQuery    string
		expectedContains []string
	}{
		{
			name:           "basic counter metric without prefix",
			metricName:     "sql.conns",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.conns": "COUNTER"},
			groupByStorage: false,
			expectedContains: []string{
				"sum:cockroachdb.sql_conns",
				"$region,$cluster,$host,$store,$node_id",
				".as_rate()",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "basic gauge metric without prefix",
			metricName:     "sys.cpu.user.percent",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sys.cpu.user.percent": "GAUGE"},
			groupByStorage: false,
			expectedContains: []string{
				"sum:cockroachdb.sys_cpu_user_percent",
				"$region,$cluster,$host,$store,$node_id",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "node metric with groupByStorage",
			metricName:     "cr.node.sql.conns",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.conns": "COUNTER"},
			groupByStorage: true,
			expectedContains: []string{
				"sum:cockroachdb.sql_conns",
				"$region,$cluster,$host,$store,$node_id",
				" by {node_id}",
				".as_rate()",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "store metric with groupByStorage",
			metricName:     "cr.store.capacity",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"capacity": "GAUGE"},
			groupByStorage: true,
			expectedContains: []string{
				"sum:cockroachdb.capacity",
				"$region,$cluster,$host,$store,$node_id",
				" by {store}",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "node metric without groupByStorage",
			metricName:     "cr.node.sql.conns",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.conns": "COUNTER"},
			groupByStorage: false,
			expectedContains: []string{
				"sum:cockroachdb.sql_conns",
				"$region,$cluster,$host,$store,$node_id",
				".as_rate()",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "histogram metric with p99 percentile",
			metricName:     "sql.service.latency-p99",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.service.latency": "HISTOGRAM"},
			groupByStorage: false,
			expectedContains: []string{
				"p99:cockroachdb.sql_service_latency",
				"$region,$cluster,$host,$store,$node_id",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "histogram metric with p99.9 percentile",
			metricName:     "sql.service.latency-p99.9",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.service.latency": "HISTOGRAM"},
			groupByStorage: false,
			expectedContains: []string{
				"p99.9:cockroachdb.sql_service_latency",
				"$region,$cluster,$host,$store,$node_id",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "histogram metric with p50 percentile",
			metricName:     "sql.service.latency-p50",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.service.latency": "HISTOGRAM"},
			groupByStorage: false,
			expectedContains: []string{
				"p50:cockroachdb.sql_service_latency",
				"$region,$cluster,$host,$store,$node_id",
				".rollup(sum, 10)",
			},
		},
		{
			name:       "metric with datadog name mapping",
			metricName: "sql.conns",
			metricsNameMap: map[string]string{
				"sql_conns": "sql.connections.total",
			},
			metricTypeMap:  map[string]string{"sql.conns": "COUNTER"},
			groupByStorage: false,
			expectedContains: []string{
				"sum:cockroachdb.sql.connections.total",
				"$region,$cluster,$host,$store,$node_id",
				".as_rate()",
				".rollup(sum, 10)",
			},
		},
		{
			name:       "metric already with cockroachdb prefix in mapping",
			metricName: "sql.conns",
			metricsNameMap: map[string]string{
				"sql_conns": "cockroachdb.sql_conns",
			},
			metricTypeMap:  map[string]string{"sql.conns": "COUNTER"},
			groupByStorage: false,
			expectedContains: []string{
				"sum:cockroachdb.sql_conns",
				"$region,$cluster,$host,$store,$node_id",
				".as_rate()",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "counter metric without rate",
			metricName:     "txn.commits",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"txn.commits": "COUNTER"},
			groupByStorage: false,
			expectedContains: []string{
				"sum:cockroachdb.txn_commits",
				"$region,$cluster,$host,$store,$node_id",
				".as_rate()",
				".rollup(sum, 10)",
			},
		},
		{
			name:           "node metric with percentile and groupByStorage",
			metricName:     "cr.node.sql.service.latency-p99",
			metricsNameMap: map[string]string{},
			metricTypeMap:  map[string]string{"sql.service.latency": "HISTOGRAM"},
			groupByStorage: true,
			expectedContains: []string{
				"p99:cockroachdb.sql_service_latency",
				"$region,$cluster,$host,$store,$node_id",
				" by {node_id}",
				".rollup(sum, 10)",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := buildDatadogQuery(tc.metricName, tc.metricsNameMap, tc.metricTypeMap, tc.groupByStorage)

			// Check that all expected substrings are present
			for _, expected := range tc.expectedContains {
				require.Contains(t, query, expected, "query should contain: %s", expected)
			}

			// Verify gauge metrics don't have .as_rate()
			if tc.metricTypeMap[stripPercentileSuffix(tc.metricName)] == "GAUGE" {
				require.NotContains(t, query, ".as_rate()", "gauge metrics should not have .as_rate()")
			}

			// Verify counter metrics have .as_rate()
			if tc.metricTypeMap[stripPercentileSuffix(tc.metricName)] == "COUNTER" {
				require.Contains(t, query, ".as_rate()", "counter metrics should have .as_rate()")
			}

			// Verify histogram metrics use correct percentile aggregation
			if tc.metricTypeMap[stripPercentileSuffix(tc.metricName)] == "HISTOGRAM" {
				percentileValue := extractPercentileValue(tc.metricName)
				if percentileValue != "" {
					expectedAgg := percentileValue + ":"
					require.Contains(t, query, expectedAgg, "histogram with percentile should use correct aggregation")
				}
			}

			// Verify groupByStorage behavior
			if tc.groupByStorage {
				// Should have either " by {node_id}" or " by {store}"
				hasNodeGroupBy := Contains(query, " by {node_id}")
				hasStoreGroupBy := Contains(query, " by {store}")
				require.True(t, hasNodeGroupBy || hasStoreGroupBy, "groupByStorage=true should add group by clause")
			}
		})
	}
}

// Contains is a helper function to check if a string contains a substring
func Contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestBuildDatadogQueryWithDifferentRollupIntervals(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name           string
		rollupInterval int
		expectedRollup string
	}{
		{
			name:           "rollup interval 10",
			rollupInterval: 10,
			expectedRollup: ".rollup(sum, 10)",
		},
		{
			name:           "rollup interval 30",
			rollupInterval: 30,
			expectedRollup: ".rollup(sum, 30)",
		},
		{
			name:           "rollup interval 60",
			rollupInterval: 60,
			expectedRollup: ".rollup(sum, 60)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set the global rollupInterval
			rollupInterval = tc.rollupInterval

			query := buildDatadogQuery(
				"sql.conns",
				map[string]string{},
				map[string]string{"sql.conns": "COUNTER"},
				false,
			)

			require.Contains(t, query, tc.expectedRollup, "query should contain correct rollup interval")
		})
	}

	// Reset to default
	rollupInterval = 10
}

func TestExtractPercentileValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		metricName    string
		expectedValue string
	}{
		{
			name:          "p99 percentile",
			metricName:    "sql.service.latency-p99",
			expectedValue: "p99",
		},
		{
			name:          "p99.9 percentile",
			metricName:    "sql.service.latency-p99.9",
			expectedValue: "p99.9",
		},
		{
			name:          "p50 percentile",
			metricName:    "sql.service.latency-p50",
			expectedValue: "p50",
		},
		{
			name:          "p90 percentile",
			metricName:    "sql.service.latency-p90",
			expectedValue: "p90",
		},
		{
			name:          "max percentile",
			metricName:    "sql.service.latency-max",
			expectedValue: "max",
		},
		{
			name:          "no percentile",
			metricName:    "sql.conns",
			expectedValue: "",
		},
		{
			name:          "metric with p in name but not percentile",
			metricName:    "sql.parsing.count",
			expectedValue: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractPercentileValue(tc.metricName)
			require.Equal(t, tc.expectedValue, result)
		})
	}
}

func TestBuildPrometheusQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name             string
		metricName       string
		metricTypeMap    map[string]string
		expectedContains []string
	}{
		{
			name:       "basic counter metric",
			metricName: "sql.conns",
			metricTypeMap: map[string]string{
				"sql.conns": "COUNTER",
			},
			expectedContains: []string{
				"sum(rate(sql_conns{",
				`job="cockroachdb"`,
				`cluster=~"$cluster"`,
				`node_id=~"$node|.*"`,
				`store=~"$store|.*"`,
				"[$__rate_interval]))",
			},
		},
		{
			name:       "basic gauge metric",
			metricName: "sys.cpu.user.percent",
			metricTypeMap: map[string]string{
				"sys.cpu.user.percent": "GAUGE",
			},
			expectedContains: []string{
				"sum(sys_cpu_user_percent{",
				`job="cockroachdb"`,
				`cluster=~"$cluster"`,
				`node_id=~"$node|.*"`,
				`store=~"$store|.*"`,
				"})",
			},
		},
		{
			name:       "histogram with p99 percentile",
			metricName: "sql.service.latency-p99",
			metricTypeMap: map[string]string{
				"sql.service.latency": "HISTOGRAM",
			},
			expectedContains: []string{
				"histogram_quantile(0.99, rate(sql_service_latency_bucket{",
				`job="cockroachdb"`,
				`cluster=~"$cluster"`,
				`node_id=~"$node|.*"`,
				`store=~"$store|.*"`,
				"}[$__rate_interval]))",
			},
		},
		{
			name:       "histogram with p99.9 percentile",
			metricName: "sql.service.latency-p99.9",
			metricTypeMap: map[string]string{
				"sql.service.latency": "HISTOGRAM",
			},
			expectedContains: []string{
				"histogram_quantile(0.999, rate(sql_service_latency_bucket{",
				`job="cockroachdb"`,
				"}[$__rate_interval]))",
			},
		},
		{
			name:       "histogram with p50 percentile",
			metricName: "sql.service.latency-p50",
			metricTypeMap: map[string]string{
				"sql.service.latency": "HISTOGRAM",
			},
			expectedContains: []string{
				"histogram_quantile(0.50, rate(sql_service_latency_bucket{",
				"}[$__rate_interval]))",
			},
		},
		{
			name:       "histogram with max percentile",
			metricName: "sql.service.latency-max",
			metricTypeMap: map[string]string{
				"sql.service.latency": "HISTOGRAM",
			},
			expectedContains: []string{
				"histogram_quantile(1, rate(sql_service_latency_bucket{",
				"}[$__rate_interval]))",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := buildPrometheusQuery(tc.metricName, tc.metricTypeMap)

			for _, expected := range tc.expectedContains {
				require.Contains(t, query, expected, "query should contain: %s", expected)
			}

			// Verify counter metrics use rate()
			if tc.metricTypeMap[stripPercentileSuffix(tc.metricName)] == "COUNTER" {
				require.Contains(t, query, "rate(", "counter metrics should use rate()")
			}

			// Verify gauge metrics use sum() without rate()
			if tc.metricTypeMap[stripPercentileSuffix(tc.metricName)] == "GAUGE" {
				require.Contains(t, query, "sum(", "gauge metrics should use sum()")
				require.NotContains(t, query, "rate(", "gauge metrics should not use rate()")
			}

			// Verify histogram metrics use histogram_quantile()
			if tc.metricTypeMap[stripPercentileSuffix(tc.metricName)] == "HISTOGRAM" {
				require.Contains(t, query, "histogram_quantile(", "histogram metrics should use histogram_quantile()")
				require.Contains(t, query, "_bucket{", "histogram metrics should query _bucket")
			}
		})
	}
}

func TestExtractPrometheusPercentile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name           string
		percentileVal  string
		expectedResult string
	}{
		{
			name:           "p99 to 0.99",
			percentileVal:  "p99",
			expectedResult: "0.99",
		},
		{
			name:           "p99.9 to 0.999",
			percentileVal:  "p99.9",
			expectedResult: "0.999",
		},
		{
			name:           "p50 to 0.50",
			percentileVal:  "p50",
			expectedResult: "0.50",
		},
		{
			name:           "p90 to 0.90",
			percentileVal:  "p90",
			expectedResult: "0.90",
		},
		{
			name:           "max to 1",
			percentileVal:  "max",
			expectedResult: "1",
		},
		{
			name:           "99 without p prefix",
			percentileVal:  "99",
			expectedResult: "0.99",
		},
		{
			name:           "50 without p prefix",
			percentileVal:  "50",
			expectedResult: "0.50",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractPrometheusPercentile(tc.percentileVal)
			require.Equal(t, tc.expectedResult, result)
		})
	}
}

func TestGetGrafanaUnit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name         string
		units        string
		expectedUnit string
	}{
		{
			name:         "Duration unit",
			units:        "Duration",
			expectedUnit: "ns",
		},
		{
			name:         "DurationSeconds unit",
			units:        "DurationSeconds",
			expectedUnit: "s",
		},
		{
			name:         "Bytes unit",
			units:        "Bytes",
			expectedUnit: "bytes",
		},
		{
			name:         "Percentage unit",
			units:        "Percentage",
			expectedUnit: "percentunit",
		},
		{
			name:         "Count unit",
			units:        "Count",
			expectedUnit: "short",
		},
		{
			name:         "Unknown unit defaults to short",
			units:        "UnknownUnit",
			expectedUnit: "short",
		},
		{
			name:         "Empty unit defaults to short",
			units:        "",
			expectedUnit: "short",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getGrafanaUnit(tc.units)
			require.Equal(t, tc.expectedUnit, result)
		})
	}
}

func TestGetLegend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name           string
		title          string
		registry       string
		groupByStorage bool
		expectedLegend string
	}{
		{
			name:           "no grouping returns title only",
			title:          "Latency",
			registry:       "node",
			groupByStorage: false,
			expectedLegend: "Latency",
		},
		{
			name:           "node metric with grouping",
			title:          "SQL Latency",
			registry:       "node",
			groupByStorage: true,
			expectedLegend: "SQL Latency-n$node",
		},
		{
			name:           "store metric with grouping",
			title:          "Storage Capacity",
			registry:       "store",
			groupByStorage: true,
			expectedLegend: "Storage Capacity-s$store",
		},
		{
			name:           "empty registry defaults to node with grouping",
			title:          "Throughput",
			registry:       "",
			groupByStorage: true,
			expectedLegend: "Throughput-n$",
		},
		{
			name:           "unknown registry defaults to node with grouping",
			title:          "Throughput",
			registry:       "unknown",
			groupByStorage: true,
			expectedLegend: "Throughput-n$unknown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getLegend(tc.title, tc.registry, tc.groupByStorage)
			require.Equal(t, tc.expectedLegend, result)
		})
	}
}

func TestConvertGraphToGrafanaPanel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		graph       GraphConfig
		metricTypes map[string]string
		panelID     int
		yPos        int
	}{
		{
			name: "single counter metric",
			graph: GraphConfig{
				Title:   "SQL Connections",
				Tooltip: "Number of SQL connections",
				Axis: AxisConfig{
					Label: "Connections",
					Units: "Count",
				},
				Metrics: []MetricConfig{
					{
						Name:  "sql.conns",
						Title: "Connections",
					},
				},
				GroupByStorage: false,
			},
			metricTypes: map[string]string{
				"sql.conns": "COUNTER",
			},
			panelID: 1,
			yPos:    0,
		},
		{
			name: "multiple metrics with different types",
			graph: GraphConfig{
				Title:   "SQL Performance",
				Tooltip: "SQL performance metrics",
				Axis: AxisConfig{
					Label: "Latency",
					Units: "Duration",
				},
				Metrics: []MetricConfig{
					{
						Name:  "sql.service.latency-p99",
						Title: "P99 Latency",
					},
					{
						Name:  "sql.service.latency-p50",
						Title: "P50 Latency",
					},
				},
				GroupByStorage: false,
			},
			metricTypes: map[string]string{
				"sql.service.latency": "HISTOGRAM",
			},
			panelID: 2,
			yPos:    8,
		},
		{
			name: "node metric with grouping",
			graph: GraphConfig{
				Title:   "Node CPU",
				Tooltip: "CPU usage per node",
				Axis: AxisConfig{
					Label: "CPU %",
					Units: "Percentage",
				},
				Metrics: []MetricConfig{
					{
						Name:  "cr.node.sys.cpu.user.percent",
						Title: "User CPU",
					},
				},
				GroupByStorage: true,
			},
			metricTypes: map[string]string{
				"sys.cpu.user.percent": "GAUGE",
			},
			panelID: 3,
			yPos:    16,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			panel := convertGraphToGrafanaPanel(tc.graph, tc.metricTypes, tc.panelID, tc.yPos)

			// Verify basic panel properties
			require.Equal(t, tc.graph.Title, panel.Title)
			require.Equal(t, tc.graph.Tooltip, panel.Description)
			require.Equal(t, tc.panelID, panel.ID)
			require.Equal(t, "timeseries", panel.Type)

			// Verify grid position
			require.Equal(t, tc.yPos, panel.GridPos.Y)
			require.Equal(t, 8, panel.GridPos.H)
			require.Equal(t, 12, panel.GridPos.W)

			// Verify number of targets matches number of metrics
			require.Equal(t, len(tc.graph.Metrics), len(panel.Targets))

			// Verify datasource configuration
			require.NotNil(t, panel.Datasource)
			require.Equal(t, "prometheus", panel.Datasource.Type)
			require.Equal(t, "${DS_PROMETHEUS}", panel.Datasource.UID)

			// Verify field config
			require.NotNil(t, panel.FieldConfig)
			expectedUnit := getGrafanaUnit(tc.graph.Axis.Units)
			require.Equal(t, expectedUnit, panel.FieldConfig.Defaults.Unit)
			require.Equal(t, tc.graph.Axis.Label, panel.FieldConfig.Defaults.Custom.Axis.Label)

			// Verify panel options
			require.NotNil(t, panel.Options)
			require.True(t, panel.Options.Legend.ShowLegend)

			// Verify targets have correct RefIDs (1, 2, 3, ...)
			for i, target := range panel.Targets {
				expectedRefID := fmt.Sprintf("%d", i+1)
				require.Equal(t, expectedRefID, target.RefID)
				require.Equal(t, "code", target.EditorMode)
				require.True(t, target.Exemplar)
				require.True(t, target.Range)
			}
		})
	}
}

// TestGenDashboardE2E tests the end-to-end CLI invocation of gen dashboard command.
// This test generates dashboard JSON files and compares them against golden files.
func TestGenDashboardE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "gen_dashboard", "basic"), func(t *testing.T, d *datadriven.TestData) string {
		c := NewCLITest(TestCLIParams{T: t, NoServer: true})
		defer c.Cleanup()

		// Map command to tool type
		var tool string
		switch d.Cmd {
		case "gen-datadog":
			tool = "datadog"
		case "gen-grafana":
			tool = "grafana"
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}

		// Create temporary output file
		tmpDir := t.TempDir()
		outputFile := filepath.Join(tmpDir, fmt.Sprintf("%s_dashboard.json", tool))

		// Build the command based on tool type
		var cmd string
		if tool == "datadog" {
			// Parse rollup interval if provided (Datadog only)
			rollupInterval := 10
			if d.HasArg("rollup-interval") {
				d.ScanArgs(t, "rollup-interval", &rollupInterval)
			}
			cmd = fmt.Sprintf("gen dashboard --tool=%s --rollup-interval=%d --output=%s",
				tool, rollupInterval, outputFile)
		} else {
			// Grafana doesn't use rollup interval
			cmd = fmt.Sprintf("gen dashboard --tool=%s --output=%s",
				tool, outputFile)
		}

		// Run the gen dashboard command
		output, err := c.RunWithCapture(cmd)
		if err != nil {
			return fmt.Sprintf("ERROR: %v\nOUTPUT: %s", err, output)
		}

		// Read the generated JSON
		data, err := os.ReadFile(outputFile)
		if err != nil {
			return fmt.Sprintf("ERROR reading file: %v\nCommand output: %s", err, output)
		}

		return string(data)
	})
}
