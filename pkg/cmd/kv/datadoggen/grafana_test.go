// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
)

func TestConvertPromQLToDatadog(t *testing.T) {
	// Load Datadog-specific metric lookup (preferred) using datapathutils for correct Bazel paths
	datadogPath := datapathutils.RewritableDataPath(t, "pkg", "cli", "files", "cockroachdb_datadog_metrics.yaml")
	if err := LoadDatadogMetricLookup(datadogPath); err != nil {
		t.Logf("Note: Could not load cockroachdb_datadog_metrics.yaml: %v", err)
	}
	// Load metrics.yaml as fallback using datapathutils
	metricsPath := datapathutils.RewritableDataPath(t, "docs", "generated", "metrics", "metrics.yaml")
	if err := LoadMetricNameLookup(metricsPath); err != nil {
		t.Logf("Note: Could not load metrics.yaml: %v", err)
	}
	tests := []struct {
		name     string
		promql   string
		expected string
	}{
		// histogram_quantile patterns
		{
			name:     "histogram_quantile with sum by le",
			promql:   `histogram_quantile(0.99, sum by (instance,le) (rate(sql_service_latency_bucket{cluster="$cluster"}[$__rate_interval])))`,
			expected: "p99:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {instance}.rollup(max, 10)",
		},
		{
			name:     "histogram_quantile p50",
			promql:   `histogram_quantile(0.50, sum by (le) (rate(sql_service_latency_bucket{cluster="$cluster"}[$__rate_interval])))`,
			expected: "p50:cockroachdb.sql.service.latency{$cluster,$node_id,$store} by {node_id}.rollup(max, 10)",
		},
		{
			name:     "histogram_quantile p99.9",
			promql:   `histogram_quantile(0.999, sum by (instance,le) (rate(kv_request_latency_bucket{cluster="$cluster"}[$__rate_interval])))`,
			expected: "p99.9:cockroachdb.kv.request.latency{$cluster,$node_id,$store} by {instance}.rollup(max, 10)",
		},

		// sum by rate patterns (counters)
		{
			name:     "sum by rate",
			promql:   `sum by (instance) (rate(sql_select_count{cluster="$cluster"}[$__rate_interval]))`,
			expected: "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {instance}.as_rate().rollup(max, 10)",
		},
		{
			name:     "sum rate no group by",
			promql:   `sum(rate(sql_select_count{cluster="$cluster"}[$__rate_interval]))`,
			expected: "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},

		// avg by patterns (gauges)
		{
			name:     "avg by metric",
			promql:   `avg by (instance) (sys_cpu_combined_percent_normalized{cluster="$cluster"})`,
			expected: "avg:cockroachdb.sys.cpu.combined.percent.normalized{$cluster,$node_id,$store} by {instance}.rollup(avg, 10)",
		},
		{
			name:     "avg by rate (counter)",
			promql:   `avg by (instance) (rate(sql_txn_count{cluster="$cluster"}[$__rate_interval]))`,
			expected: "avg:cockroachdb.sql.txn.count{$cluster,$node_id,$store} by {instance}.as_rate().rollup(max, 10)",
		},

		// Simple metric patterns
		{
			name:     "simple metric with labels",
			promql:   `goroutines{cluster="$cluster"}`,
			expected: "avg:cockroachdb.goroutines{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
		},
		{
			name:     "simple counter metric",
			promql:   `sql_select_count{cluster="$cluster"}`,
			expected: "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},

		// rate() patterns
		{
			name:     "simple rate",
			promql:   `rate(sql_select_count{cluster="$cluster"}[$__rate_interval])`,
			expected: "avg:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},

		// avg_over_time patterns
		{
			name:     "avg_over_time",
			promql:   `avg_over_time(sys_cpu_percent{cluster="$cluster"}[$__range])`,
			expected: "avg:cockroachdb.sys.cpu.percent{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
		},
		{
			name:   "max by avg_over_time",
			promql: `max by (instance) (avg_over_time(sys_cpu_percent{cluster="$cluster"}[$__range]))`,
			// Note: Currently this matches the simpler avg_over_time pattern first
			// In the future, could reorder patterns to match more specific patterns first
			expected: "avg:cockroachdb.sys.cpu.percent{$cluster,$node_id,$store} by {node_id}.rollup(avg, 10)",
		},

		// sum by metric (no rate)
		{
			name:     "sum by metric",
			promql:   `sum by (instance) (ranges_available{cluster="$cluster"})`,
			expected: "sum:cockroachdb.ranges.available{$cluster,$node_id,$store} by {instance}.rollup(sum, 10)",
		},
		{
			name:     "sum metric counter",
			promql:   `sum(sql_select_count{cluster="$cluster"})`,
			expected: "sum:cockroachdb.sql.select.count{$cluster,$node_id,$store} by {node_id}.as_rate().rollup(max, 10)",
		},

		// Empty/invalid inputs
		{
			name:     "empty string",
			promql:   "",
			expected: "",
		},
		{
			name:     "whitespace only",
			promql:   "   ",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertPromQLToDatadog(tt.promql)
			if result != tt.expected {
				t.Errorf("convertPromQLToDatadog(%q)\ngot:  %q\nwant: %q", tt.promql, result, tt.expected)
			}
		})
	}
}

func TestExtractAllMetricsAsQueries(t *testing.T) {
	tests := []struct {
		name          string
		expr          string
		expectedCount int
		containsRate  bool
	}{
		{
			name:          "single metric",
			expr:          `sql_select_count{cluster="$cluster"}`,
			expectedCount: 1,
			containsRate:  false,
		},
		{
			name:          "division expression with two metrics",
			expr:          `rate(metric_a{cluster="$cluster"}[$__rate_interval]) / rate(metric_b{cluster="$cluster"}[$__rate_interval])`,
			expectedCount: 2,
			containsRate:  true,
		},
		{
			name:          "rate expression",
			expr:          `rate(sql_select_count{cluster="$cluster"}[$__rate_interval])`,
			expectedCount: 1,
			containsRate:  true,
		},
		{
			name:          "empty expression",
			expr:          "",
			expectedCount: 0,
			containsRate:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractAllMetricsAsQueries(tt.expr)
			if len(result) != tt.expectedCount {
				t.Errorf("extractAllMetricsAsQueries() returned %d queries, want %d", len(result), tt.expectedCount)
			}
			if tt.containsRate && len(result) > 0 {
				// Check that counter queries have as_rate()
				for _, q := range result {
					if !strings.Contains(q, ".as_rate()") {
						t.Errorf("Expected query to contain .as_rate(), got: %s", q)
					}
				}
			}
		})
	}
}

func TestExtractMetricAndLabels(t *testing.T) {
	tests := []struct {
		name           string
		expr           string
		expectedMetric string
		expectedLabels string
	}{
		{
			name:           "metric with labels",
			expr:           `sql_select_count{cluster="$cluster",instance="$node"}`,
			expectedMetric: "sql_select_count",
			expectedLabels: DefaultTags,
		},
		{
			name:           "metric without labels",
			expr:           "goroutines",
			expectedMetric: "goroutines",
			expectedLabels: DefaultTags,
		},
		{
			name:           "metric with empty labels",
			expr:           "metric_name{}",
			expectedMetric: "metric_name",
			expectedLabels: DefaultTags,
		},
		{
			name:           "complex expression - extracts nothing",
			expr:           "rate(metric[$__rate_interval])",
			expectedMetric: "",
			expectedLabels: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric, labels := extractMetricAndLabels(tt.expr)
			if metric != tt.expectedMetric {
				t.Errorf("extractMetricAndLabels() metric = %q, want %q", metric, tt.expectedMetric)
			}
			if labels != tt.expectedLabels {
				t.Errorf("extractMetricAndLabels() labels = %q, want %q", labels, tt.expectedLabels)
			}
		})
	}
}

func TestCleanGroupBy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "remove le from group by",
			input:    "instance,le",
			expected: "instance",
		},
		{
			name:     "le only",
			input:    "le",
			expected: "node_id",
		},
		{
			name:     "no le",
			input:    "instance,store",
			expected: "instance,store",
		},
		{
			name:     "with spaces",
			input:    " instance , le ",
			expected: "instance",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "node_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanGroupBy(tt.input)
			if result != tt.expected {
				t.Errorf("cleanGroupBy(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCleanLabels(t *testing.T) {
	// cleanLabels currently always returns DefaultTags
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "any labels returns default tags",
			input:    `cluster="$cluster",instance="$node"`,
			expected: DefaultTags,
		},
		{
			name:     "empty labels returns default tags",
			input:    "",
			expected: DefaultTags,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanLabels(tt.input)
			if result != tt.expected {
				t.Errorf("cleanLabels(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		a, b     int
		expected int
	}{
		{1, 2, 2},
		{5, 3, 5},
		{0, 0, 0},
		{-1, 1, 1},
		{10, 10, 10},
	}

	for _, tt := range tests {
		result := max(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("max(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestHasQueries(t *testing.T) {
	tests := []struct {
		name     string
		widget   Widget
		expected bool
	}{
		{
			name: "widget with queries",
			widget: Widget{
				Definition: WidgetDefinition{
					Type: "timeseries",
					Requests: []WidgetRequest{
						{
							Queries: []WidgetQuery{
								{Query: "test"},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "widget without queries",
			widget: Widget{
				Definition: WidgetDefinition{
					Type:     "timeseries",
					Requests: []WidgetRequest{},
				},
			},
			expected: false,
		},
		{
			name: "note widget",
			widget: Widget{
				Definition: WidgetDefinition{
					Type:    "note",
					Content: "test",
				},
			},
			expected: true,
		},
		{
			name: "widget with empty queries array",
			widget: Widget{
				Definition: WidgetDefinition{
					Type: "timeseries",
					Requests: []WidgetRequest{
						{
							Queries: []WidgetQuery{},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasQueries(&tt.widget)
			if result != tt.expected {
				t.Errorf("hasQueries() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestConvertGrafanaToDadog(t *testing.T) {
	grafana := &GrafanaDashboard{
		Title: "Test Dashboard",
		Panels: []GrafanaPanel{
			{
				Type:  "row",
				Title: "Test Row",
			},
			{
				Type:  "timeseries",
				Title: "CPU Usage",
				Targets: []GrafanaTarget{
					{
						Expr:  `avg by (instance) (sys_cpu_percent{cluster="$cluster"})`,
						RefID: "A",
					},
				},
			},
		},
	}

	dashboard := convertGrafanaToDadog(grafana)

	if dashboard.Title != "Test Dashboard" {
		t.Errorf("Dashboard title = %q, want %q", dashboard.Title, "Test Dashboard")
	}
	if len(dashboard.TemplateVariables) != 3 {
		t.Errorf("Expected 3 template variables, got %d", len(dashboard.TemplateVariables))
	}
}

func TestConvertPanelToWidget(t *testing.T) {
	tests := []struct {
		name           string
		panel          GrafanaPanel
		expectedType   string
		expectsQueries bool
	}{
		{
			name: "timeseries panel",
			panel: GrafanaPanel{
				Type:  "timeseries",
				Title: "Test Panel",
				Targets: []GrafanaTarget{
					{
						Expr:  `goroutines{cluster="$cluster"}`,
						RefID: "A",
					},
				},
			},
			expectedType:   "timeseries",
			expectsQueries: true,
		},
		{
			name: "heatmap panel",
			panel: GrafanaPanel{
				Type:  "heatmap",
				Title: "Heatmap",
				Targets: []GrafanaTarget{
					{
						Expr:  `goroutines{cluster="$cluster"}`,
						RefID: "A",
					},
				},
			},
			expectedType:   "timeseries",
			expectsQueries: true,
		},
		{
			name: "unsupported panel type",
			panel: GrafanaPanel{
				Type:  "stat",
				Title: "Stat Panel",
			},
			expectedType:   "note",
			expectsQueries: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			widget := convertPanelToWidget(&tt.panel)
			if widget == nil {
				t.Fatal("Expected widget, got nil")
			}
			if widget.Definition.Type != tt.expectedType {
				t.Errorf("Widget type = %q, want %q", widget.Definition.Type, tt.expectedType)
			}
		})
	}
}

func TestConvertTimeseriesPanel(t *testing.T) {
	tests := []struct {
		name            string
		panel           GrafanaPanel
		expectedDisplay string
	}{
		{
			name: "default line display",
			panel: GrafanaPanel{
				Type:  "timeseries",
				Title: "Test",
				Targets: []GrafanaTarget{
					{Expr: `goroutines{cluster="$cluster"}`, RefID: "A"},
				},
			},
			expectedDisplay: "line",
		},
		{
			name: "state-timeline becomes bars",
			panel: GrafanaPanel{
				Type:  "state-timeline",
				Title: "Test",
				Targets: []GrafanaTarget{
					{Expr: `goroutines{cluster="$cluster"}`, RefID: "A"},
				},
			},
			expectedDisplay: "bars",
		},
		{
			name: "heatmap becomes area",
			panel: GrafanaPanel{
				Type:  "heatmap",
				Title: "Test",
				Targets: []GrafanaTarget{
					{Expr: `goroutines{cluster="$cluster"}`, RefID: "A"},
				},
			},
			expectedDisplay: "area",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			widget := convertTimeseriesPanel(&tt.panel)
			if len(widget.Definition.Requests) == 0 {
				t.Fatal("Expected at least one request")
			}
			if widget.Definition.Requests[0].DisplayType != tt.expectedDisplay {
				t.Errorf("DisplayType = %q, want %q", widget.Definition.Requests[0].DisplayType, tt.expectedDisplay)
			}
		})
	}
}
