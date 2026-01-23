// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

func TestConvertMetricName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "metric with dots (from metrics.yaml)",
			input:    "kv.allocator.load_based_lease_transfers.cannot_find_better_candidate",
			expected: "cockroachdb.kv.allocator.load_based_lease_transfers.cannot_find_better_candidate",
		},
		{
			name:     "metric with dots and underscores",
			input:    "sql.service.latency",
			expected: "cockroachdb.sql.service.latency",
		},
		{
			name:     "single word metric",
			input:    "goroutines",
			expected: "cockroachdb.goroutines",
		},
		{
			name:     "metric with hyphens (converted to underscores)",
			input:    "round-trip-latency",
			expected: "cockroachdb.round_trip_latency",
		},
		{
			name:     "mma metric from metrics.yaml",
			input:    "mma.change.external.lease.failure",
			expected: "cockroachdb.mma.change.external.lease.failure",
		},
		{
			name:     "metric with mixed hyphens and dots",
			input:    "storage.iterator.category-abort-span.block-load.latency",
			expected: "cockroachdb.storage.iterator.category_abort_span.block_load.latency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertMetricName(tt.input)
			if result != tt.expected {
				t.Errorf("ConvertMetricName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestConvertMetricNameWithLookup tests ConvertMetricName with the Datadog lookup loaded.
// This simulates how from-metrics works when cockroachdb_datadog_metrics.yaml is available.
func TestConvertMetricNameWithLookup(t *testing.T) {
	// Load Datadog-specific metric lookup using datapathutils for correct Bazel paths
	datadogPath := datapathutils.RewritableDataPath(t, "pkg", "cli", "files", "cockroachdb_datadog_metrics.yaml")
	if err := LoadDatadogMetricLookup(datadogPath); err != nil {
		skip.IgnoreLint(t, "Could not load cockroachdb_datadog_metrics.yaml for test")
	}

	// Clear the lookup after test to not affect other tests
	defer func() { datadogMetricLookup = nil }()

	// Input format matches metrics.yaml (with hyphens), expected format uses Datadog lookup
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Round trip metrics - these use the Datadog lookup
		{
			name:     "round-trip-latency from metrics.yaml",
			input:    "round-trip-latency",
			expected: "cockroachdb.round_trip.latency",
		},
		{
			name:     "round-trip-default-class-latency from metrics.yaml",
			input:    "round-trip-default-class-latency",
			expected: "cockroachdb.round_trip_default_class_latency",
		},
		{
			name:     "round-trip-raft-class-latency from metrics.yaml",
			input:    "round-trip-raft-class-latency",
			expected: "cockroachdb.round_trip_raft_class_latency",
		},
		// SQL service latency
		{
			name:     "sql.service.latency from metrics.yaml",
			input:    "sql.service.latency",
			expected: "cockroachdb.sql.service.latency",
		},
		// sys.cpu metric - YAML has percent.normalized (dot before normalized)
		{
			name:     "sys.cpu.combined.percent-normalized from metrics.yaml",
			input:    "sys.cpu.combined.percent-normalized",
			expected: "cockroachdb.sys.cpu.combined.percent.normalized",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertMetricName(tt.input)
			if result != tt.expected {
				t.Errorf("ConvertMetricName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestBuildCounterQuery(t *testing.T) {
	tests := []struct {
		name       string
		metric     string
		labels     string
		aggregator string
		groupBy    string
		expected   string
	}{
		{
			name:       "default aggregator and groupBy",
			metric:     "cockroachdb.sql.select.count",
			labels:     "$cluster,$node_id,$store",
			aggregator: "",
			groupBy:    "",
			expected:   "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},
		{
			name:       "custom aggregator",
			metric:     "cockroachdb.sql.select.count",
			labels:     "$cluster,$node_id,$store",
			aggregator: "avg",
			groupBy:    "",
			expected:   "avg:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},
		{
			name:       "custom groupBy",
			metric:     "cockroachdb.storage.bytes",
			labels:     "$cluster,$node_id,$store",
			aggregator: "sum",
			groupBy:    "store",
			expected:   "sum:cockroachdb.storage.bytes{$cluster,$node_id,$store} by {store}.as_rate().rollup(max, 10)",
		},
		{
			name:       "custom aggregator and groupBy",
			metric:     "cockroachdb.sql.txn.count",
			labels:     "$cluster",
			aggregator: "max",
			groupBy:    "instance",
			expected:   "max:cockroachdb.sql.txn.count{$cluster} by {instance}.as_rate().rollup(max, 10)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildCounterQuery(tt.metric, tt.labels, tt.aggregator, tt.groupBy)
			if result != tt.expected {
				t.Errorf("BuildCounterQuery() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildGaugeQuery(t *testing.T) {
	tests := []struct {
		name       string
		metric     string
		labels     string
		aggregator string
		groupBy    string
		expected   string
	}{
		{
			name:       "default aggregator and groupBy",
			metric:     "cockroachdb.sys.cpu.percent",
			labels:     "$cluster,$node_id,$store",
			aggregator: "",
			groupBy:    "",
			expected:   "avg:cockroachdb.sys.cpu.percent{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
		},
		{
			name:       "max aggregator",
			metric:     "cockroachdb.sys.memory.used",
			labels:     "$cluster,$node_id,$store",
			aggregator: "max",
			groupBy:    "",
			expected:   "max:cockroachdb.sys.memory.used{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
		},
		{
			name:       "min aggregator with custom groupBy",
			metric:     "cockroachdb.ranges.available",
			labels:     "$cluster,$node_id,$store",
			aggregator: "min",
			groupBy:    "store",
			expected:   "min:cockroachdb.ranges.available{$cluster,$node_id,$store} by {store}.rollup(min, 10)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildGaugeQuery(tt.metric, tt.labels, tt.aggregator, tt.groupBy)
			if result != tt.expected {
				t.Errorf("BuildGaugeQuery() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildHistogramQuery(t *testing.T) {
	tests := []struct {
		name             string
		metricBase       string
		labels           string
		percentilePrefix string
		groupBy          string
		expected         string
	}{
		{
			name:             "p99 histogram",
			metricBase:       "cockroachdb.sql.service.latency",
			labels:           "$cluster,$node_id,$store",
			percentilePrefix: "p99",
			groupBy:          "",
			expected:         "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
		},
		{
			name:             "p50 histogram",
			metricBase:       "cockroachdb.sql.service.latency",
			labels:           "$cluster,$node_id,$store",
			percentilePrefix: "p50",
			groupBy:          "instance",
			expected:         "p50:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {instance}.rollup(max, 10)",
		},
		{
			name:             "default percentile",
			metricBase:       "cockroachdb.kv.request.latency",
			labels:           "$cluster,$node_id,$store",
			percentilePrefix: "",
			groupBy:          "",
			expected:         "p99:cockroachdb.kv.request.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
		},
		{
			name:             "strip bucket suffix",
			metricBase:       "cockroachdb.sql.service.latency.bucket",
			labels:           "$cluster,$node_id,$store",
			percentilePrefix: "p99",
			groupBy:          "",
			expected:         "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildHistogramQuery(tt.metricBase, tt.labels, tt.percentilePrefix, tt.groupBy)
			if result != tt.expected {
				t.Errorf("BuildHistogramQuery() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		name     string
		metric   MetricDef
		expected string
	}{
		{
			name: "counter metric",
			metric: MetricDef{
				Name: "sql.select.count",
				Type: MetricTypeCounter,
			},
			expected: "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},
		{
			name: "gauge metric",
			metric: MetricDef{
				Name: "sys.cpu.percent",
				Type: MetricTypeGauge,
			},
			expected: "avg:cockroachdb.sys.cpu.percent{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
		},
		{
			name: "histogram metric",
			metric: MetricDef{
				Name: "sql.service.latency",
				Type: MetricTypeHistogram,
			},
			expected: "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
		},
		{
			name: "unknown type defaults to gauge",
			metric: MetricDef{
				Name: "custom.metric",
				Type: "",
			},
			expected: "avg:cockroachdb.custom.metric{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildQuery(tt.metric)
			if result != tt.expected {
				t.Errorf("BuildQuery() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestFormatPercentile(t *testing.T) {
	tests := []struct {
		name     string
		quantile float64
		expected string
	}{
		{
			name:     "p50",
			quantile: 0.50,
			expected: "p50",
		},
		{
			name:     "p90",
			quantile: 0.90,
			expected: "p90",
		},
		{
			name:     "p99",
			quantile: 0.99,
			expected: "p99",
		},
		{
			name:     "p99.9",
			quantile: 0.999,
			expected: "p99.9",
		},
		{
			name:     "p99.99",
			quantile: 0.9999,
			expected: "p99.99",
		},
		{
			name:     "max (1.0)",
			quantile: 1.0,
			expected: "max",
		},
		{
			name:     "p75",
			quantile: 0.75,
			expected: "p75",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatPercentile(tt.quantile)
			if result != tt.expected {
				t.Errorf("FormatPercentile(%v) = %q, want %q", tt.quantile, result, tt.expected)
			}
		})
	}
}

func TestIsCounterMetric(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		expected   bool
	}{
		// Counter suffixes
		{name: "count suffix underscore", metricName: "sql_select_count", expected: true},
		{name: "count suffix dot", metricName: "sql.select.count", expected: true},
		{name: "total suffix underscore", metricName: "requests_total", expected: true},
		{name: "total suffix dot", metricName: "requests.total", expected: true},
		{name: "sum suffix underscore", metricName: "values_sum", expected: true},
		{name: "sum suffix dot", metricName: "values.sum", expected: true},
		{name: "bucket suffix underscore", metricName: "latency_bucket", expected: true},
		{name: "bucket suffix dot", metricName: "latency.bucket", expected: true},
		{name: "bytes suffix underscore", metricName: "storage_bytes", expected: true},
		{name: "bytes suffix dot", metricName: "storage.bytes", expected: true},
		{name: "ops suffix underscore", metricName: "read_ops", expected: true},
		{name: "ops suffix dot", metricName: "read.ops", expected: true},

		// Non-counter metrics
		{name: "gauge metric", metricName: "sys_cpu_percent", expected: false},
		{name: "histogram metric", metricName: "sql_service_latency", expected: false},
		{name: "generic metric", metricName: "goroutines", expected: false},
		{name: "metric with count in middle", metricName: "sql_count_per_second", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCounterMetric(tt.metricName)
			if result != tt.expected {
				t.Errorf("IsCounterMetric(%q) = %v, want %v", tt.metricName, result, tt.expected)
			}
		})
	}
}

func TestDefaultTemplateVariables(t *testing.T) {
	vars := DefaultTemplateVariables()

	if len(vars) != 3 {
		t.Errorf("Expected 3 template variables, got %d", len(vars))
	}

	expectedVars := []struct {
		name   string
		prefix string
	}{
		{"cluster", "cluster"},
		{"node_id", "node_id"},
		{"store", "store"},
	}

	for i, expected := range expectedVars {
		if vars[i].Name != expected.name {
			t.Errorf("Variable %d name = %q, want %q", i, vars[i].Name, expected.name)
		}
		if vars[i].Prefix != expected.prefix {
			t.Errorf("Variable %d prefix = %q, want %q", i, vars[i].Prefix, expected.prefix)
		}
		if vars[i].Default != "*" {
			t.Errorf("Variable %d default = %q, want %q", i, vars[i].Default, "*")
		}
	}
}

func TestNewDashboard(t *testing.T) {
	title := "Test Dashboard"
	description := "Test Description"

	dashboard := NewDashboard(title, description)

	if dashboard.Title != title {
		t.Errorf("Dashboard title = %q, want %q", dashboard.Title, title)
	}
	if dashboard.Description != description {
		t.Errorf("Dashboard description = %q, want %q", dashboard.Description, description)
	}
	if dashboard.LayoutType != "ordered" {
		t.Errorf("Dashboard layout_type = %q, want %q", dashboard.LayoutType, "ordered")
	}
	if dashboard.IsReadOnly {
		t.Error("Dashboard is_read_only should be false")
	}
	if len(dashboard.TemplateVariables) != 3 {
		t.Errorf("Expected 3 template variables, got %d", len(dashboard.TemplateVariables))
	}
	if len(dashboard.Widgets) != 0 {
		t.Errorf("Expected 0 widgets, got %d", len(dashboard.Widgets))
	}
}

func TestCreateTimeseriesWidget(t *testing.T) {
	metric := MetricDef{
		Name: "sql.select.count",
		Type: MetricTypeCounter,
	}

	widget := CreateTimeseriesWidget(metric, 0)

	if widget.Definition.Type != "timeseries" {
		t.Errorf("Widget type = %q, want %q", widget.Definition.Type, "timeseries")
	}
	if widget.Definition.TitleSize != "16" {
		t.Errorf("Widget title_size = %q, want %q", widget.Definition.TitleSize, "16")
	}
	if len(widget.Definition.Requests) != 1 {
		t.Fatalf("Expected 1 request, got %d", len(widget.Definition.Requests))
	}

	req := widget.Definition.Requests[0]
	if req.ResponseFormat != "timeseries" {
		t.Errorf("Request response_format = %q, want %q", req.ResponseFormat, "timeseries")
	}
	if req.DisplayType != "line" {
		t.Errorf("Request display_type = %q, want %q", req.DisplayType, "line")
	}
	if len(req.Queries) != 1 {
		t.Fatalf("Expected 1 query, got %d", len(req.Queries))
	}
	if req.Queries[0].DataSource != "metrics" {
		t.Errorf("Query data_source = %q, want %q", req.Queries[0].DataSource, "metrics")
	}
	if req.Queries[0].Name != "q0_0" {
		t.Errorf("Query name = %q, want %q", req.Queries[0].Name, "q0_0")
	}
}

func TestCreateGroupWidget(t *testing.T) {
	title := "Test Group"
	widgets := []Widget{
		CreateTimeseriesWidget(MetricDef{Name: "metric1", Type: MetricTypeGauge}, 0),
		CreateTimeseriesWidget(MetricDef{Name: "metric2", Type: MetricTypeCounter}, 1),
	}

	group := CreateGroupWidget(title, widgets)

	if group.Definition.Type != "group" {
		t.Errorf("Widget type = %q, want %q", group.Definition.Type, "group")
	}
	if group.Definition.Title != title {
		t.Errorf("Widget title = %q, want %q", group.Definition.Title, title)
	}
	if group.Definition.LayoutType != "ordered" {
		t.Errorf("Widget layout_type = %q, want %q", group.Definition.LayoutType, "ordered")
	}
	if !group.Definition.ShowTitle {
		t.Error("Widget show_title should be true")
	}
	if len(group.Definition.Widgets) != 2 {
		t.Errorf("Expected 2 widgets, got %d", len(group.Definition.Widgets))
	}
}

func TestCreateNoteWidget(t *testing.T) {
	content := "Test note content"
	widget := CreateNoteWidget(content)

	if widget.Definition.Type != "note" {
		t.Errorf("Widget type = %q, want %q", widget.Definition.Type, "note")
	}
	if widget.Definition.Content != content {
		t.Errorf("Widget content = %q, want %q", widget.Definition.Content, content)
	}
	if widget.Definition.BackgroundColor != "yellow" {
		t.Errorf("Widget background_color = %q, want %q", widget.Definition.BackgroundColor, "yellow")
	}
	if widget.Layout == nil {
		t.Fatal("Widget layout should not be nil")
	}
	if widget.Layout.Width != 3 || widget.Layout.Height != 2 {
		t.Errorf("Widget layout = %dx%d, want 3x2", widget.Layout.Width, widget.Layout.Height)
	}
}

// TestRealWorldQueryExamples tests query building against patterns from actual Datadog dashboards.
// These test cases are derived from example.json which contains real production dashboard queries.
func TestRealWorldQueryExamples(t *testing.T) {
	t.Run("counter queries", func(t *testing.T) {
		// Real examples from example.json:
		// sum:cockroachdb.sql.query.count{$cluster, $node_id, $store} by {cluster}.rollup(max, 30).as_rate()
		// sum:cockroachdb.rocksdb.flushes.total{$cluster, $node_id, $store} by {store}.as_rate()

		tests := []struct {
			name       string
			metric     string
			labels     string
			aggregator string
			groupBy    string
			expected   string
		}{
			{
				name:       "sql query count by cluster",
				metric:     "cockroachdb.sql.query.count",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "cluster",
				expected:   "sum:cockroachdb.sql.query.count{$cluster,$node_id,$store} by {cluster}.as_rate().rollup(max, 10)",
			},
			{
				name:       "rocksdb flushes total by store",
				metric:     "cockroachdb.rocksdb.flushes.total",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.rocksdb.flushes.total{$cluster,$node_id,$store} by {store}.as_rate().rollup(max, 10)",
			},
			{
				name:       "exec success by cluster",
				metric:     "cockroachdb.exec.success",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "cluster",
				expected:   "sum:cockroachdb.exec.success{$cluster,$node_id,$store} by {cluster}.as_rate().rollup(max, 10)",
			},
			{
				name:       "distsender batches total",
				metric:     "cockroachdb.distsender.batches.total",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "cluster",
				expected:   "sum:cockroachdb.distsender.batches.total{$cluster,$node_id,$store} by {cluster}.as_rate().rollup(max, 10)",
			},
			{
				name:       "storage wal bytes in by store",
				metric:     "cockroachdb.storage.wal.bytes_in",
				labels:     "$cluster",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.storage.wal.bytes_in{$cluster} by {store}.as_rate().rollup(max, 10)",
			},
			{
				name:       "rocksdb flushed bytes by store",
				metric:     "cockroachdb.rocksdb.flushed_bytes",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.rocksdb.flushed_bytes{$cluster,$node_id,$store} by {store}.as_rate().rollup(max, 10)",
			},
			{
				name:       "rocksdb compactions total",
				metric:     "cockroachdb.rocksdb.compactions.total",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.rocksdb.compactions.total{$cluster,$node_id,$store} by {store}.as_rate().rollup(max, 10)",
			},
			{
				name:       "block cache misses",
				metric:     "cockroachdb.rocksdb.block.cache.misses",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.rocksdb.block.cache.misses{$cluster,$node_id,$store} by {store}.as_rate().rollup(max, 10)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := BuildCounterQuery(tt.metric, tt.labels, tt.aggregator, tt.groupBy)
				if result != tt.expected {
					t.Errorf("BuildCounterQuery() =\n  %q\nwant:\n  %q", result, tt.expected)
				}
			})
		}
	})

	t.Run("gauge queries", func(t *testing.T) {
		// Real examples from example.json:
		// avg:cockroachdb.sys.cpu.combined.percent.normalized{$cluster, $node_id, $store} by {node_id,friendly_id}
		// max:cockroachdb.sys.rss{$cluster, $node_id, $store} by {node_id,friendly_id}
		// max:cockroachdb.ranges{$cluster, $node_id, $store} by {node_id,store}

		tests := []struct {
			name       string
			metric     string
			labels     string
			aggregator string
			groupBy    string
			expected   string
		}{
			{
				name:       "cpu combined percent normalized",
				metric:     "cockroachdb.sys.cpu.combined.percent.normalized",
				labels:     "$cluster,$node_id,$store",
				aggregator: "avg",
				groupBy:    "node_id,friendly_id",
				expected:   "avg:cockroachdb.sys.cpu.combined.percent.normalized{$cluster,$node_id,$store} by {node_id,friendly_id}.rollup(avg, 10)",
			},
			{
				name:       "sys rss max",
				metric:     "cockroachdb.sys.rss",
				labels:     "$cluster,$node_id,$store",
				aggregator: "max",
				groupBy:    "node_id,friendly_id",
				expected:   "max:cockroachdb.sys.rss{$cluster,$node_id,$store} by {node_id,friendly_id}.rollup(max, 10)",
			},
			{
				name:       "sys totalmem max",
				metric:     "cockroachdb.sys.totalmem",
				labels:     "$cluster,$node_id,$store",
				aggregator: "max",
				groupBy:    "node_id,friendly_id",
				expected:   "max:cockroachdb.sys.totalmem{$cluster,$node_id,$store} by {node_id,friendly_id}.rollup(max, 10)",
			},
			{
				name:       "ranges by node and store",
				metric:     "cockroachdb.ranges",
				labels:     "$cluster,$node_id,$store",
				aggregator: "max",
				groupBy:    "node_id,store",
				expected:   "max:cockroachdb.ranges{$cluster,$node_id,$store} by {node_id,store}.rollup(max, 10)",
			},
			{
				name:       "goroutines by node_id",
				metric:     "cockroachdb.sys.goroutines",
				labels:     "$cluster",
				aggregator: "max",
				groupBy:    "node_id",
				expected:   "max:cockroachdb.sys.goroutines{$cluster} by {node_id}.rollup(max, 10)",
			},
			{
				name:       "rocksdb memtable size",
				metric:     "cockroachdb.rocksdb.memtable.total.size",
				labels:     "$cluster",
				aggregator: "max",
				groupBy:    "store",
				expected:   "max:cockroachdb.rocksdb.memtable.total.size{$cluster} by {store}.rollup(max, 10)",
			},
			{
				name:       "capacity used",
				metric:     "cockroachdb.capacity.used",
				labels:     "$cluster,$node_id,$store",
				aggregator: "max",
				groupBy:    "store",
				expected:   "max:cockroachdb.capacity.used{$cluster,$node_id,$store} by {store}.rollup(max, 10)",
			},
			{
				name:       "totalbytes",
				metric:     "cockroachdb.totalbytes",
				labels:     "$cluster,$node_id,$store",
				aggregator: "max",
				groupBy:    "store",
				expected:   "max:cockroachdb.totalbytes{$cluster,$node_id,$store} by {store}.rollup(max, 10)",
			},
			{
				name:       "l0 sublevels",
				metric:     "cockroachdb.storage.l0_sublevels",
				labels:     "$cluster,$store",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.storage.l0_sublevels{$cluster,$store} by {store}.rollup(sum, 10)",
			},
			{
				name:       "read amplification",
				metric:     "cockroachdb.rocksdb.read.amplification",
				labels:     "$cluster,$node_id,$store",
				aggregator: "sum",
				groupBy:    "store",
				expected:   "sum:cockroachdb.rocksdb.read.amplification{$cluster,$node_id,$store} by {store}.rollup(sum, 10)",
			},
			{
				name:       "jobs backup running",
				metric:     "cockroachdb.jobs.backup.currently_running",
				labels:     "$cluster",
				aggregator: "max",
				groupBy:    "node_id",
				expected:   "max:cockroachdb.jobs.backup.currently_running{$cluster} by {node_id}.rollup(max, 10)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := BuildGaugeQuery(tt.metric, tt.labels, tt.aggregator, tt.groupBy)
				if result != tt.expected {
					t.Errorf("BuildGaugeQuery() =\n  %q\nwant:\n  %q", result, tt.expected)
				}
			})
		}
	})

	t.Run("histogram/percentile queries", func(t *testing.T) {
		// Real examples from example.json:
		// p99:cockroachdb.exec.latency{$cluster} by {cluster}
		// p99.99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}
		// p99.9:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}
		// p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}
		// p90:cockroachdb.exec.latency{$cluster,$node_id,$store} by {node_id}
		// p50:cockroachdb.raft.replication.latency{$cluster,$node_id,$store}

		tests := []struct {
			name             string
			metricBase       string
			labels           string
			percentilePrefix string
			groupBy          string
			expected         string
		}{
			{
				name:             "exec latency p99 by cluster",
				metricBase:       "cockroachdb.exec.latency",
				labels:           "$cluster",
				percentilePrefix: "p99",
				groupBy:          "cluster",
				expected:         "p99:cockroachdb.exec.latency{$cluster} by {cluster}.rollup(max, 10)",
			},
			{
				name:             "sql service latency p99.99",
				metricBase:       "cockroachdb.sql.service.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p99.99",
				groupBy:          "node_id",
				expected:         "p99.99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "sql service latency p99.9",
				metricBase:       "cockroachdb.sql.service.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p99.9",
				groupBy:          "node_id",
				expected:         "p99.9:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "sql service latency p99",
				metricBase:       "cockroachdb.sql.service.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p99",
				groupBy:          "node_id",
				expected:         "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "exec latency p90",
				metricBase:       "cockroachdb.exec.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p90",
				groupBy:          "node_id",
				expected:         "p90:cockroachdb.exec.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "exec latency p50",
				metricBase:       "cockroachdb.exec.latency",
				labels:           "$cluster,$host",
				percentilePrefix: "p50",
				groupBy:          "node_id",
				expected:         "p50:cockroachdb.exec.latency{$cluster,$host} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "raft replication latency p50",
				metricBase:       "cockroachdb.raft.replication.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p50",
				groupBy:          "node_id",
				expected:         "p50:cockroachdb.raft.replication.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "raft replication latency p90",
				metricBase:       "cockroachdb.raft.replication.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p90",
				groupBy:          "node_id",
				expected:         "p90:cockroachdb.raft.replication.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
			{
				name:             "wal fsync latency max",
				metricBase:       "cockroachdb.storage.wal.fsync.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "max",
				groupBy:          "store",
				expected:         "max:cockroachdb.storage.wal.fsync.latency{$cluster,$node_id,$store} by {store}.rollup(max, 10)",
			},
			{
				name:             "raft logcommit latency max",
				metricBase:       "cockroachdb.raft.process.logcommit.latency",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "max",
				groupBy:          "store",
				expected:         "max:cockroachdb.raft.process.logcommit.latency{$cluster,$node_id,$store} by {store}.rollup(max, 10)",
			},
			{
				name:             "ebpf bio latency p99",
				metricBase:       "ebpf_exporter.bio_latency_seconds",
				labels:           "$cluster,operation:write",
				percentilePrefix: "p99",
				groupBy:          "host",
				expected:         "p99:ebpf_exporter.bio_latency_seconds{$cluster,operation:write} by {host}.rollup(max, 10)",
			},
			{
				name:             "kvflowcontrol eval wait duration",
				metricBase:       "cockroachdb.kvflowcontrol.eval_wait.elastic.duration",
				labels:           "$cluster,$node_id,$store",
				percentilePrefix: "p99",
				groupBy:          "node_id",
				expected:         "p99:cockroachdb.kvflowcontrol.eval_wait.elastic.duration{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := BuildHistogramQuery(tt.metricBase, tt.labels, tt.percentilePrefix, tt.groupBy)
				if result != tt.expected {
					t.Errorf("BuildHistogramQuery() =\n  %q\nwant:\n  %q", result, tt.expected)
				}
			})
		}
	})
}

// TestConvertExportedMetricName tests that exported metric names (Prometheus-style with underscores)
// convert correctly to Datadog format using the metrics.yaml lookup.
func TestConvertExportedMetricName(t *testing.T) {
	// Load Datadog-specific metric lookup (preferred) using datapathutils for correct Bazel paths
	datadogPath := datapathutils.RewritableDataPath(t, "pkg", "cli", "files", "cockroachdb_datadog_metrics.yaml")
	if err := LoadDatadogMetricLookup(datadogPath); err != nil {
		// Not fatal - we'll try the fallback
		t.Logf("Note: Could not load cockroachdb_datadog_metrics.yaml: %v", err)
	}

	// Load metrics.yaml as fallback using datapathutils
	metricsPath := datapathutils.RewritableDataPath(t, "docs", "generated", "metrics", "metrics.yaml")
	if err := LoadMetricNameLookup(metricsPath); err != nil {
		skip.IgnoreLint(t, "Could not load metrics.yaml for test")
	}

	// These are actual metrics from example.json (exported_name format)
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Storage metrics
		{name: "wal bytes in", input: "storage_wal_bytes_in", expected: "cockroachdb.storage.wal.bytes_in"},
		{name: "wal fsync latency", input: "storage_wal_fsync_latency", expected: "cockroachdb.storage.wal.fsync.latency"},

		// SQL metrics
		{name: "sql service latency", input: "sql_service_latency", expected: "cockroachdb.sql.service.latency"},

		// System metrics
		{name: "sys cpu percent", input: "sys_cpu_combined_percent_normalized", expected: "cockroachdb.sys.cpu.combined.percent.normalized"},
		{name: "sys rss", input: "sys_rss", expected: "cockroachdb.sys.rss"},

		// Ranges metrics
		{name: "ranges", input: "ranges", expected: "cockroachdb.ranges"},

		// Metrics with _bucket suffix should strip it
		{name: "sql service latency bucket", input: "sql_service_latency_bucket", expected: "cockroachdb.sql.service.latency"},

		// Round trip metrics - mixed patterns
		{name: "round trip latency", input: "round_trip_latency", expected: "cockroachdb.round_trip.latency"},
		{name: "round trip default class latency", input: "round_trip_default_class_latency", expected: "cockroachdb.round_trip_default_class_latency"},
		{name: "round trip raft class latency", input: "round_trip_raft_class_latency", expected: "cockroachdb.round_trip_raft_class_latency"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertExportedMetricName(tt.input)
			if result != tt.expected {
				t.Errorf("ConvertExportedMetricName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestTsdumpMode tests query generation in tsdump (self-hosted) mode.
func TestTsdumpMode(t *testing.T) {
	// Save original state and restore after test
	originalMode := TsdumpMode
	defer func() { TsdumpMode = originalMode }()

	// Load Datadog-specific metric lookup using datapathutils for correct Bazel paths
	datadogPath := datapathutils.RewritableDataPath(t, "pkg", "cli", "files", "cockroachdb_datadog_metrics.yaml")
	if err := LoadDatadogMetricLookup(datadogPath); err != nil {
		skip.IgnoreLint(t, "Could not load cockroachdb_datadog_metrics.yaml for test")
	}
	// Clear the lookup after test to not affect other tests
	defer func() { datadogMetricLookup = nil }()

	t.Run("GetMetricPrefix", func(t *testing.T) {
		TsdumpMode = false
		if got := GetMetricPrefix(); got != "cockroachdb" {
			t.Errorf("GetMetricPrefix() in normal mode = %q, want %q", got, "cockroachdb")
		}

		TsdumpMode = true
		if got := GetMetricPrefix(); got != "crdb.tsdump" {
			t.Errorf("GetMetricPrefix() in tsdump mode = %q, want %q", got, "crdb.tsdump")
		}
	})

	t.Run("GetDefaultTags", func(t *testing.T) {
		TsdumpMode = false
		if got := GetDefaultTags(); got != "$cluster,$node_id,$store" {
			t.Errorf("GetDefaultTags() in normal mode = %q, want %q", got, "$cluster,$node_id,$store")
		}

		TsdumpMode = true
		if got := GetDefaultTags(); got != "$upload_id,$node_id" {
			t.Errorf("GetDefaultTags() in tsdump mode = %q, want %q", got, "$upload_id,$node_id")
		}
	})

	t.Run("ConvertMetricName_tsdump", func(t *testing.T) {
		TsdumpMode = true
		tests := []struct {
			input    string
			expected string
		}{
			{"sql.service.latency", "crdb.tsdump.sql.service.latency"},
			{"sys.cpu.combined.percent-normalized", "crdb.tsdump.sys.cpu.combined.percent.normalized"},
			{"sql.txns.open", "crdb.tsdump.sql.txns.open"},
			{"ranges.decommissioning", "crdb.tsdump.ranges.decommissioning"},
		}

		for _, tt := range tests {
			result := ConvertMetricName(tt.input)
			if result != tt.expected {
				t.Errorf("ConvertMetricName(%q) in tsdump mode = %q, want %q", tt.input, result, tt.expected)
			}
		}
	})

	t.Run("BuildHistogramQuery_tsdump", func(t *testing.T) {
		TsdumpMode = true
		// In tsdump mode, histograms use suffix format: avg:metric_p90{tags}
		result := BuildHistogramQuery("crdb.tsdump.sql.txn.latency", "$upload_id,$node_id", "p90", "node_id")
		expected := "avg:crdb.tsdump.sql.txn.latency_p90{$upload_id,$node_id} by {node_id}.rollup(max, 10)"
		if result != expected {
			t.Errorf("BuildHistogramQuery() in tsdump mode = %q, want %q", result, expected)
		}
	})

	t.Run("BuildQuery_histogram_tsdump", func(t *testing.T) {
		TsdumpMode = true
		metric := MetricDef{
			Name: "sql.txn.latency",
			Type: MetricTypeHistogram,
		}
		result := BuildQuery(metric)
		// BuildQuery returns p90 for tsdump histograms (single query)
		expected := "avg:crdb.tsdump.sql.txn.latency_p90{$upload_id,$node_id} by {node_id}.rollup(max, 10)"
		if result != expected {
			t.Errorf("BuildQuery() histogram in tsdump mode = %q, want %q", result, expected)
		}
	})

	t.Run("BuildQueries_histogram_tsdump", func(t *testing.T) {
		TsdumpMode = true
		metric := MetricDef{
			Name: "sql.txn.latency",
			Type: MetricTypeHistogram,
		}
		results := BuildQueries(metric)
		// BuildQueries returns p50, p90, p99 for tsdump histograms
		if len(results) != 3 {
			t.Errorf("BuildQueries() histogram in tsdump mode returned %d queries, want 3", len(results))
		}
		expectedQueries := []string{
			"avg:crdb.tsdump.sql.txn.latency_p50{$upload_id,$node_id} by {node_id}.rollup(max, 10)",
			"avg:crdb.tsdump.sql.txn.latency_p90{$upload_id,$node_id} by {node_id}.rollup(max, 10)",
			"avg:crdb.tsdump.sql.txn.latency_p99{$upload_id,$node_id} by {node_id}.rollup(max, 10)",
		}
		for i, expected := range expectedQueries {
			if results[i] != expected {
				t.Errorf("BuildQueries()[%d] in tsdump mode = %q, want %q", i, results[i], expected)
			}
		}
	})

	t.Run("BuildQuery_gauge_tsdump", func(t *testing.T) {
		TsdumpMode = true
		metric := MetricDef{
			Name: "sql.txns.open",
			Type: MetricTypeGauge,
		}
		result := BuildQuery(metric)
		expected := "avg:crdb.tsdump.sql.txns.open{$upload_id,$node_id} by {node_id}.rollup(avg, 10)"
		if result != expected {
			t.Errorf("BuildQuery() gauge in tsdump mode = %q, want %q", result, expected)
		}
	})

	t.Run("BuildQuery_counter_tsdump", func(t *testing.T) {
		TsdumpMode = true
		metric := MetricDef{
			Name: "sql.statements.count",
			Type: MetricTypeCounter,
		}
		result := BuildQuery(metric)
		expected := "sum:crdb.tsdump.sql.statements.count{$upload_id,$node_id} by {node_id}.as_rate().rollup(max, 10)"
		if result != expected {
			t.Errorf("BuildQuery() counter in tsdump mode = %q, want %q", result, expected)
		}
	})

	t.Run("DefaultTemplateVariables_tsdump", func(t *testing.T) {
		TsdumpMode = true
		vars := DefaultTemplateVariables()
		if len(vars) != 2 {
			t.Errorf("DefaultTemplateVariables() in tsdump mode has %d vars, want 2", len(vars))
		}
		if vars[0].Name != "upload_id" {
			t.Errorf("First template var in tsdump mode = %q, want %q", vars[0].Name, "upload_id")
		}
		if vars[1].Name != "node_id" {
			t.Errorf("Second template var in tsdump mode = %q, want %q", vars[1].Name, "node_id")
		}
	})
}
