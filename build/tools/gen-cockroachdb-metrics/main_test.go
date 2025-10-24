// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePythonDict(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		dictName string
		expected map[string]string
		wantErr  bool
	}{
		{
			name: "basic dictionary",
			input: `
METRIC_MAP = {
    'metric_one': 'datadog.metric.one',
    'metric_two': 'datadog.metric.two',
}`,
			dictName: "METRIC_MAP",
			expected: map[string]string{
				"metric_one": "datadog.metric.one",
				"metric_two": "datadog.metric.two",
			},
		},
		{
			name: "empty dictionary",
			input: `
METRIC_MAP = {
}`,
			dictName: "METRIC_MAP",
			expected: map[string]string{},
		},
		{
			name: "dictionary not found",
			input: `
OTHER_MAP = {
    'metric_one': 'datadog.metric.one',
}`,
			dictName: "METRIC_MAP",
			expected: map[string]string{},
		},
		{
			name: "nested braces",
			input: `
METRIC_MAP = {
    'metric_one': 'datadog.metric.one',
    'nested': {'inner': 'value'},
    'metric_two': 'datadog.metric.two',
}`,
			dictName: "METRIC_MAP",
			expected: map[string]string{
				"metric_one": "datadog.metric.one",
				"nested":     "{'inner': 'value'}",
				"metric_two": "datadog.metric.two",
			},
		},
		{
			name: "double quotes - not parsed (requires single quotes)",
			input: `
METRIC_MAP = {
    "metric_one": "datadog.metric.one",
    "metric_two": "datadog.metric.two",
}`,
			dictName: "METRIC_MAP",
			expected: map[string]string{},
		},
		{
			name: "mixed quotes",
			input: `
METRIC_MAP = {
    'metric_one': "datadog.metric.one",
    "metric_two": 'datadog.metric.two',
}`,
			dictName: "METRIC_MAP",
			expected: map[string]string{
				"metric_one": "datadog.metric.one",
				"metric_two": "datadog.metric.two",
			},
		},
		{
			name:     "empty input",
			input:    "",
			dictName: "METRIC_MAP",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := strings.Split(tt.input, "\n")
			result, err := parsePythonDict(lines, tt.dictName)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseDatadogMappings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
		wantErr  bool
	}{
		{
			name: "both dictionaries present",
			input: `
METRIC_MAP = {
    'metric_one': 'datadog.metric.one',
}

OMV2_METRIC_MAP = {
    'metric_two': 'datadog.metric.two',
}`,
			expected: map[string]string{
				"metric_one": "datadog.metric.one",
				"metric_two": "datadog.metric.two",
			},
		},
		{
			name: "only METRIC_MAP",
			input: `
METRIC_MAP = {
    'metric_one': 'datadog.metric.one',
}`,
			expected: map[string]string{
				"metric_one": "datadog.metric.one",
			},
		},
		{
			name: "only OMV2_METRIC_MAP",
			input: `
OMV2_METRIC_MAP = {
    'metric_two': 'datadog.metric.two',
}`,
			expected: map[string]string{
				"metric_two": "datadog.metric.two",
			},
		},
		{
			name:     "empty input",
			input:    "",
			expected: map[string]string{},
		},
		{
			name: "overlapping keys - last wins",
			input: `
METRIC_MAP = {
    'metric_one': 'datadog.metric.old',
}

OMV2_METRIC_MAP = {
    'metric_one': 'datadog.metric.new',
}`,
			expected: map[string]string{
				"metric_one": "datadog.metric.new",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := parseDatadogMappings(reader)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldSkipMetric(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		expected   bool
	}{
		// Should skip
		{
			name:       "auth prefix",
			metricName: "auth_login_attempts",
			expected:   true,
		},
		{
			name:       "distsender_rpc_err_errordetailtype prefix",
			metricName: "distsender_rpc_err_errordetailtype_someerror",
			expected:   true,
		},
		{
			name:       "gossip_callbacks prefix",
			metricName: "gossip_callbacks_count",
			expected:   true,
		},
		{
			name:       "logical_replication prefix",
			metricName: "logical_replication_events",
			expected:   true,
		},
		{
			name:       "sql_crud prefix",
			metricName: "sql_crud_operations",
			expected:   true,
		},
		{
			name:       "storage_l6 pattern",
			metricName: "storage_l6_bytes",
			expected:   true,
		},
		{
			name:       "storage_sstable_compression prefix",
			metricName: "storage_sstable_compression_ratio",
			expected:   true,
		},
		// Should not skip
		{
			name:       "normal metric",
			metricName: "sql_query_count",
			expected:   false,
		},
		{
			name:       "empty string",
			metricName: "",
			expected:   false,
		},
		{
			name:       "contains but doesn't start with auth",
			metricName: "system_auth_count",
			expected:   false,
		},
		{
			name:       "storage without level number",
			metricName: "storage_bytes_written",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldSkipMetric(tt.metricName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMapMetricsToDatadog(t *testing.T) {
	datadogMappings := map[string]string{
		"sql_query_count": "cockroachdb.sql.query.count",
		"sys_cpu_percent": "cockroachdb.sys.cpu.percent",
	}

	tests := []struct {
		name     string
		metrics  []MetricInfo
		expected map[string]string
	}{
		{
			name:     "empty metrics",
			metrics:  []MetricInfo{},
			expected: map[string]string{},
		},
		{
			name: "simple metric with mapping",
			metrics: []MetricInfo{
				{Name: "sql.query.count", Type: "COUNTER"},
			},
			expected: map[string]string{
				"sql_query_count": "cockroachdb.sql.query.count",
			},
		},
		{
			name: "metric without mapping",
			metrics: []MetricInfo{
				{Name: "custom.metric", Type: "GAUGE"},
			},
			expected: map[string]string{
				"custom_metric": "custom.metric",
			},
		},
		{
			name: "metric with hyphens normalized",
			metrics: []MetricInfo{
				{Name: "some-metric-name", Type: "GAUGE"},
			},
			expected: map[string]string{
				"some_metric_name": "some_metric_name",
			},
		},
		{
			name: "histogram metric",
			metrics: []MetricInfo{
				{Name: "request.latency", Type: "HISTOGRAM"},
			},
			expected: map[string]string{
				"request_latency":        "request.latency",
				"request_latency_bucket": "request.latency.bucket",
				"request_latency_count":  "request.latency.count",
				"request_latency_sum":    "request.latency.sum",
			},
		},
		{
			name: "skipped metric",
			metrics: []MetricInfo{
				{Name: "auth.login.count", Type: "COUNTER"},
			},
			expected: map[string]string{},
		},
		{
			name: "multiple metrics mixed",
			metrics: []MetricInfo{
				{Name: "sql.query.count", Type: "COUNTER"},
				{Name: "auth.login.count", Type: "COUNTER"}, // skipped
				{Name: "custom.metric", Type: "GAUGE"},
				{Name: "request.latency", Type: "HISTOGRAM"},
			},
			expected: map[string]string{
				"sql_query_count":        "cockroachdb.sql.query.count",
				"custom_metric":          "custom.metric",
				"request_latency":        "request.latency",
				"request_latency_bucket": "request.latency.bucket",
				"request_latency_count":  "request.latency.count",
				"request_latency_sum":    "request.latency.sum",
			},
		},
		{
			name: "special characters in name",
			metrics: []MetricInfo{
				{Name: "metric-with.dots_and-dashes", Type: "GAUGE"},
			},
			expected: map[string]string{
				"metric_with_dots_and_dashes": "metric_with.dots_and_dashes",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapMetricsToDatadog(tt.metrics, datadogMappings)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMergeLegacyMappings(t *testing.T) {
	tests := []struct {
		name           string
		legacyMappings map[string]string
		newMappings    map[string]string
		expected       map[string]string
	}{
		{
			name:           "both empty",
			legacyMappings: map[string]string{},
			newMappings:    map[string]string{},
			expected:       map[string]string{},
		},
		{
			name: "only legacy",
			legacyMappings: map[string]string{
				"old_metric": "legacy.old.metric",
			},
			newMappings: map[string]string{},
			expected: map[string]string{
				"old_metric": "legacy.old.metric",
			},
		},
		{
			name:           "only new",
			legacyMappings: map[string]string{},
			newMappings: map[string]string{
				"new_metric": "new.metric",
			},
			expected: map[string]string{
				"new_metric": "new.metric",
			},
		},
		{
			name: "no overlap",
			legacyMappings: map[string]string{
				"old_metric": "legacy.old.metric",
			},
			newMappings: map[string]string{
				"new_metric": "new.metric",
			},
			expected: map[string]string{
				"old_metric": "legacy.old.metric",
				"new_metric": "new.metric",
			},
		},
		{
			name: "overlapping keys - new wins",
			legacyMappings: map[string]string{
				"metric": "legacy.metric",
				"old":    "legacy.old",
			},
			newMappings: map[string]string{
				"metric": "new.metric",
				"new":    "new.new",
			},
			expected: map[string]string{
				"metric": "new.metric",
				"old":    "legacy.old",
				"new":    "new.new",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeLegacyMappings(tt.legacyMappings, tt.newMappings)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCollectAllCRDBMetrics(t *testing.T) {
	tests := []struct {
		name                      string
		yamlOutput                *YAMLOutput
		runtimeConditionalMetrics []MetricInfo
		expected                  int // expected total count
	}{
		{
			name: "empty yaml and runtime conditional",
			yamlOutput: &YAMLOutput{
				Layers: []Layer{},
			},
			runtimeConditionalMetrics: []MetricInfo{},
			expected:                  0,
		},
		{
			name: "only yaml metrics",
			yamlOutput: &YAMLOutput{
				Layers: []Layer{
					{
						Name: "layer1",
						Categories: []Category{
							{
								Name: "cat1",
								Metrics: []MetricInfo{
									{Name: "metric1"},
									{Name: "metric2"},
								},
							},
						},
					},
				},
			},
			runtimeConditionalMetrics: []MetricInfo{},
			expected:                  2,
		},
		{
			name: "only runtime conditional metrics",
			yamlOutput: &YAMLOutput{
				Layers: []Layer{},
			},
			runtimeConditionalMetrics: []MetricInfo{
				{Name: "conditional1"},
				{Name: "conditional2"},
			},
			expected: 2,
		},
		{
			name: "combined yaml and runtime conditional",
			yamlOutput: &YAMLOutput{
				Layers: []Layer{
					{
						Name: "layer1",
						Categories: []Category{
							{
								Name: "cat1",
								Metrics: []MetricInfo{
									{Name: "metric1"},
								},
							},
							{
								Name: "cat2",
								Metrics: []MetricInfo{
									{Name: "metric2"},
									{Name: "metric3"},
								},
							},
						},
					},
					{
						Name: "layer2",
						Categories: []Category{
							{
								Name: "cat3",
								Metrics: []MetricInfo{
									{Name: "metric4"},
								},
							},
						},
					},
				},
			},
			runtimeConditionalMetrics: []MetricInfo{
				{Name: "conditional1"},
				{Name: "conditional2"},
			},
			expected: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collectAllCRDBMetrics(tt.yamlOutput, tt.runtimeConditionalMetrics)
			assert.Equal(t, tt.expected, len(result))
		})
	}
}
