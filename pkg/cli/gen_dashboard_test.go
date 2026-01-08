// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
