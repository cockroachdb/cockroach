// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFilterMetricsByPrefix(t *testing.T) {
	yamlMetrics := map[string]YAMLMetric{
		"sql.select.count": {
			Name: "sql.select.count",
			Type: "COUNTER",
		},
		"sql.insert.count": {
			Name: "sql.insert.count",
			Type: "COUNTER",
		},
		"sql.service.latency": {
			Name: "sql.service.latency",
			Type: "HISTOGRAM",
		},
		"kv.rangefeed.count": {
			Name: "kv.rangefeed.count",
			Type: "COUNTER",
		},
		"sys.cpu.percent": {
			Name: "sys.cpu.percent",
			Type: "GAUGE",
		},
	}

	tests := []struct {
		name          string
		prefixes      []string
		expectedCount int
		expectedNames []string
	}{
		{
			name:          "single prefix",
			prefixes:      []string{"sql"},
			expectedCount: 3,
			expectedNames: []string{"sql.insert.count", "sql.select.count", "sql.service.latency"},
		},
		{
			name:          "multiple prefixes",
			prefixes:      []string{"sql", "kv"},
			expectedCount: 4,
			expectedNames: []string{"kv.rangefeed.count", "sql.insert.count", "sql.select.count", "sql.service.latency"},
		},
		{
			name:          "specific prefix",
			prefixes:      []string{"sql.service"},
			expectedCount: 1,
			expectedNames: []string{"sql.service.latency"},
		},
		{
			name:          "no matches",
			prefixes:      []string{"nonexistent"},
			expectedCount: 0,
			expectedNames: []string{},
		},
		{
			name:          "all metrics",
			prefixes:      []string{"sql", "kv", "sys"},
			expectedCount: 5,
			expectedNames: []string{"kv.rangefeed.count", "sql.insert.count", "sql.select.count", "sql.service.latency", "sys.cpu.percent"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterMetricsByPrefix(yamlMetrics, tt.prefixes)
			if len(result) != tt.expectedCount {
				t.Errorf("filterMetricsByPrefix() returned %d metrics, want %d", len(result), tt.expectedCount)
			}

			// Check that all expected names are present
			resultNames := make(map[string]bool)
			for _, m := range result {
				resultNames[m.Name] = true
			}
			for _, name := range tt.expectedNames {
				if !resultNames[name] {
					t.Errorf("Expected metric %q not found in result", name)
				}
			}
		})
	}
}

func TestFilterMetricsByPrefixTypes(t *testing.T) {
	yamlMetrics := map[string]YAMLMetric{
		"counter.metric": {Name: "counter.metric", Type: "COUNTER"},
		"gauge.metric":   {Name: "gauge.metric", Type: "GAUGE"},
		"hist.metric":    {Name: "hist.metric", Type: "HISTOGRAM"},
		"unknown.metric": {Name: "unknown.metric", Type: "UNKNOWN"},
	}

	result := filterMetricsByPrefix(yamlMetrics, []string{"counter", "gauge", "hist", "unknown"})

	typeMap := make(map[string]MetricType)
	for _, m := range result {
		typeMap[m.Name] = m.Type
	}

	if typeMap["counter.metric"] != MetricTypeCounter {
		t.Errorf("counter.metric type = %q, want %q", typeMap["counter.metric"], MetricTypeCounter)
	}
	if typeMap["gauge.metric"] != MetricTypeGauge {
		t.Errorf("gauge.metric type = %q, want %q", typeMap["gauge.metric"], MetricTypeGauge)
	}
	if typeMap["hist.metric"] != MetricTypeHistogram {
		t.Errorf("hist.metric type = %q, want %q", typeMap["hist.metric"], MetricTypeHistogram)
	}
	if typeMap["unknown.metric"] != MetricTypeGauge {
		t.Errorf("unknown.metric type = %q, want %q (defaults to gauge)", typeMap["unknown.metric"], MetricTypeGauge)
	}
}

func TestGroupMetricsByPrefix(t *testing.T) {
	metrics := []MetricDef{
		{Name: "sql.select.count"},
		{Name: "sql.insert.count"},
		{Name: "sql.service.latency"},
		{Name: "kv.rangefeed.count"},
		{Name: "sys.cpu.percent"},
	}

	tests := []struct {
		name           string
		depth          int
		expectedGroups int
		expectedKeys   []string
	}{
		{
			name:           "depth 1",
			depth:          1,
			expectedGroups: 3,
			expectedKeys:   []string{"kv", "sql", "sys"},
		},
		{
			name:           "depth 2",
			depth:          2,
			expectedGroups: 5,
			expectedKeys:   []string{"kv.rangefeed", "sql.insert", "sql.select", "sql.service", "sys.cpu"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupMetricsByPrefix(metrics, tt.depth)
			if len(result) != tt.expectedGroups {
				t.Errorf("groupMetricsByPrefix() returned %d groups, want %d", len(result), tt.expectedGroups)
			}
		})
	}
}

func TestSortedKeys(t *testing.T) {
	m := map[string][]MetricDef{
		"zebra":  {},
		"alpha":  {},
		"middle": {},
		"beta":   {},
	}

	result := sortedKeys(m)

	expected := []string{"alpha", "beta", "middle", "zebra"}
	if len(result) != len(expected) {
		t.Fatalf("sortedKeys() returned %d keys, want %d", len(result), len(expected))
	}

	for i, key := range expected {
		if result[i] != key {
			t.Errorf("sortedKeys()[%d] = %q, want %q", i, result[i], key)
		}
	}
}

func TestCreateDashboard(t *testing.T) {
	metrics := []MetricDef{
		{Name: "sql.select.count", Type: MetricTypeCounter},
		{Name: "sql.insert.count", Type: MetricTypeCounter},
		{Name: "kv.rangefeed.count", Type: MetricTypeCounter},
	}

	t.Run("with grouping", func(t *testing.T) {
		dashboard := createDashboard(metrics, "Test Dashboard", true)

		if dashboard.Title != "Test Dashboard" {
			t.Errorf("Dashboard title = %q, want %q", dashboard.Title, "Test Dashboard")
		}
		if dashboard.LayoutType != "ordered" {
			t.Errorf("Dashboard layout_type = %q, want %q", dashboard.LayoutType, "ordered")
		}

		// Should have 2 groups: kv and sql
		if len(dashboard.Widgets) != 2 {
			t.Errorf("Expected 2 widget groups, got %d", len(dashboard.Widgets))
		}

		// Verify groups are created correctly
		for _, widget := range dashboard.Widgets {
			if widget.Definition.Type != "group" {
				t.Errorf("Expected group widget, got %q", widget.Definition.Type)
			}
		}
	})

	t.Run("without grouping", func(t *testing.T) {
		dashboard := createDashboard(metrics, "Test Dashboard", false)

		if dashboard.Title != "Test Dashboard" {
			t.Errorf("Dashboard title = %q, want %q", dashboard.Title, "Test Dashboard")
		}

		// Should have 3 individual widgets (no grouping)
		if len(dashboard.Widgets) != 3 {
			t.Errorf("Expected 3 individual widgets, got %d", len(dashboard.Widgets))
		}

		// Verify widgets are timeseries (not groups)
		for _, widget := range dashboard.Widgets {
			if widget.Definition.Type != "timeseries" {
				t.Errorf("Expected timeseries widget, got %q", widget.Definition.Type)
			}
		}
	})
}

func TestLoadMetricsYAML(t *testing.T) {
	// Create a temporary YAML file for testing
	yamlContent := `metrics:
  - name: test.counter.metric
    type: COUNTER
    description: A test counter metric
    unit: COUNT
  - name: test.gauge.metric
    type: GAUGE
    description: A test gauge metric
    unit: BYTES
  - name: test.histogram.metric
    type: HISTOGRAM
    description: A test histogram metric
    unit: NANOSECONDS
`
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "test_metrics.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test YAML file: %v", err)
	}

	metrics, err := loadMetricsYAML(yamlPath)
	if err != nil {
		t.Fatalf("loadMetricsYAML() error = %v", err)
	}

	if len(metrics) != 3 {
		t.Errorf("loadMetricsYAML() returned %d metrics, want 3", len(metrics))
	}

	// Check counter metric
	if m, ok := metrics["test.counter.metric"]; ok {
		if m.Type != "COUNTER" {
			t.Errorf("counter metric type = %q, want %q", m.Type, "COUNTER")
		}
	} else {
		t.Error("test.counter.metric not found")
	}

	// Check gauge metric
	if m, ok := metrics["test.gauge.metric"]; ok {
		if m.Type != "GAUGE" {
			t.Errorf("gauge metric type = %q, want %q", m.Type, "GAUGE")
		}
	} else {
		t.Error("test.gauge.metric not found")
	}

	// Check histogram metric
	if m, ok := metrics["test.histogram.metric"]; ok {
		if m.Type != "HISTOGRAM" {
			t.Errorf("histogram metric type = %q, want %q", m.Type, "HISTOGRAM")
		}
	} else {
		t.Error("test.histogram.metric not found")
	}
}

func TestLoadMetricsYAMLFileNotFound(t *testing.T) {
	_, err := loadMetricsYAML("/nonexistent/path/metrics.yaml")
	if err == nil {
		t.Error("Expected error for nonexistent file, got nil")
	}
}

func TestLoadMetricsYAMLEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "empty.yaml")
	if err := os.WriteFile(yamlPath, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to write test YAML file: %v", err)
	}

	metrics, err := loadMetricsYAML(yamlPath)
	if err != nil {
		t.Fatalf("loadMetricsYAML() error = %v", err)
	}

	if len(metrics) != 0 {
		t.Errorf("loadMetricsYAML() returned %d metrics for empty file, want 0", len(metrics))
	}
}

func TestLoadMetricsYAMLWithDescription(t *testing.T) {
	yamlContent := `  - name: test.metric
    type: GAUGE
    description: 'This is a test metric description'
    unit: COUNT
`
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test YAML file: %v", err)
	}

	metrics, err := loadMetricsYAML(yamlPath)
	if err != nil {
		t.Fatalf("loadMetricsYAML() error = %v", err)
	}

	if m, ok := metrics["test.metric"]; ok {
		if m.Description != "This is a test metric description" {
			t.Errorf("metric description = %q, want %q", m.Description, "This is a test metric description")
		}
	} else {
		t.Error("test.metric not found")
	}
}

func TestPrintAvailablePrefixes(t *testing.T) {
	// This test just ensures the function doesn't panic
	yamlMetrics := map[string]YAMLMetric{
		"sql.select.count": {Name: "sql.select.count", Type: "COUNTER"},
		"kv.rangefeed":     {Name: "kv.rangefeed", Type: "COUNTER"},
	}

	// Redirect stdout to prevent printing during tests
	// Just verify it doesn't panic
	printAvailablePrefixes(yamlMetrics, 10)
}

func TestYAMLMetricStruct(t *testing.T) {
	metric := YAMLMetric{
		Name:        "test.metric",
		Type:        "COUNTER",
		Description: "Test description",
		Unit:        "COUNT",
		YAxisLabel:  "Count",
	}

	if metric.Name != "test.metric" {
		t.Errorf("Name = %q, want %q", metric.Name, "test.metric")
	}
	if metric.Type != "COUNTER" {
		t.Errorf("Type = %q, want %q", metric.Type, "COUNTER")
	}
	if metric.Description != "Test description" {
		t.Errorf("Description = %q, want %q", metric.Description, "Test description")
	}
	if metric.Unit != "COUNT" {
		t.Errorf("Unit = %q, want %q", metric.Unit, "COUNT")
	}
	if metric.YAxisLabel != "Count" {
		t.Errorf("YAxisLabel = %q, want %q", metric.YAxisLabel, "Count")
	}
}

func TestFilterMetricsByPrefixSortedOrder(t *testing.T) {
	// Test that results are sorted alphabetically
	yamlMetrics := map[string]YAMLMetric{
		"sql.zebra":  {Name: "sql.zebra", Type: "GAUGE"},
		"sql.alpha":  {Name: "sql.alpha", Type: "GAUGE"},
		"sql.middle": {Name: "sql.middle", Type: "GAUGE"},
	}

	result := filterMetricsByPrefix(yamlMetrics, []string{"sql"})

	if len(result) != 3 {
		t.Fatalf("Expected 3 metrics, got %d", len(result))
	}

	// Results should be sorted
	expected := []string{"sql.alpha", "sql.middle", "sql.zebra"}
	for i, name := range expected {
		if result[i].Name != name {
			t.Errorf("result[%d].Name = %q, want %q", i, result[i].Name, name)
		}
	}
}

func TestFilterMetricsByPrefixExactMatch(t *testing.T) {
	yamlMetrics := map[string]YAMLMetric{
		"exact":       {Name: "exact", Type: "GAUGE"},
		"exact.child": {Name: "exact.child", Type: "GAUGE"},
		"exactprefix": {Name: "exactprefix", Type: "GAUGE"}, // Should NOT match "exact"
	}

	result := filterMetricsByPrefix(yamlMetrics, []string{"exact"})

	if len(result) != 2 {
		t.Errorf("Expected 2 metrics (exact and exact.child), got %d", len(result))
	}

	names := make(map[string]bool)
	for _, m := range result {
		names[m.Name] = true
	}

	if !names["exact"] {
		t.Error("Expected 'exact' to be included")
	}
	if !names["exact.child"] {
		t.Error("Expected 'exact.child' to be included")
	}
	if names["exactprefix"] {
		t.Error("'exactprefix' should NOT be included (not a prefix match)")
	}
}

func TestFilterMetricsBySearch(t *testing.T) {
	yamlMetrics := map[string]YAMLMetric{
		"sql.select.count": {
			Name: "sql.select.count",
			Type: "COUNTER",
		},
		"sql.insert.count": {
			Name: "sql.insert.count",
			Type: "COUNTER",
		},
		"sql.service.latency": {
			Name: "sql.service.latency",
			Type: "HISTOGRAM",
		},
		"kv.rangefeed.latency": {
			Name: "kv.rangefeed.latency",
			Type: "HISTOGRAM",
		},
		"sys.cpu.percent": {
			Name: "sys.cpu.percent",
			Type: "GAUGE",
		},
		"storage.wal.fsync.latency": {
			Name: "storage.wal.fsync.latency",
			Type: "HISTOGRAM",
		},
	}

	tests := []struct {
		name          string
		searchTerms   []string
		expectedCount int
		expectedNames []string
	}{
		{
			name:          "search for latency",
			searchTerms:   []string{"latency"},
			expectedCount: 3,
			expectedNames: []string{"kv.rangefeed.latency", "sql.service.latency", "storage.wal.fsync.latency"},
		},
		{
			name:          "search for count",
			searchTerms:   []string{"count"},
			expectedCount: 2,
			expectedNames: []string{"sql.insert.count", "sql.select.count"},
		},
		{
			name:          "search case insensitive",
			searchTerms:   []string{"LATENCY"},
			expectedCount: 3,
			expectedNames: []string{"kv.rangefeed.latency", "sql.service.latency", "storage.wal.fsync.latency"},
		},
		{
			name:          "search multiple terms",
			searchTerms:   []string{"cpu", "wal"},
			expectedCount: 2,
			expectedNames: []string{"storage.wal.fsync.latency", "sys.cpu.percent"},
		},
		{
			name:          "search partial match",
			searchTerms:   []string{"range"},
			expectedCount: 1,
			expectedNames: []string{"kv.rangefeed.latency"},
		},
		{
			name:          "search no match",
			searchTerms:   []string{"nonexistent"},
			expectedCount: 0,
			expectedNames: []string{},
		},
		{
			name:          "search sql.service",
			searchTerms:   []string{"sql.service"},
			expectedCount: 1,
			expectedNames: []string{"sql.service.latency"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterMetricsBySearch(yamlMetrics, tt.searchTerms)

			if len(result) != tt.expectedCount {
				t.Errorf("filterMetricsBySearch() returned %d metrics, want %d", len(result), tt.expectedCount)
			}

			// Check that all expected names are present
			resultNames := make(map[string]bool)
			for _, m := range result {
				resultNames[m.Name] = true
			}

			for _, expectedName := range tt.expectedNames {
				if !resultNames[expectedName] {
					t.Errorf("Expected metric %q not found in results", expectedName)
				}
			}
		})
	}
}

func TestFilterMetricsBySearchTypes(t *testing.T) {
	yamlMetrics := map[string]YAMLMetric{
		"test.counter": {Name: "test.counter", Type: "COUNTER"},
		"test.gauge":   {Name: "test.gauge", Type: "GAUGE"},
		"test.hist":    {Name: "test.hist", Type: "HISTOGRAM"},
		"test.unknown": {Name: "test.unknown", Type: "UNKNOWN"},
	}

	result := filterMetricsBySearch(yamlMetrics, []string{"test"})

	if len(result) != 4 {
		t.Fatalf("Expected 4 metrics, got %d", len(result))
	}

	typeMap := make(map[string]MetricType)
	for _, m := range result {
		typeMap[m.Name] = m.Type
	}

	if typeMap["test.counter"] != MetricTypeCounter {
		t.Errorf("test.counter type = %q, want %q", typeMap["test.counter"], MetricTypeCounter)
	}
	if typeMap["test.gauge"] != MetricTypeGauge {
		t.Errorf("test.gauge type = %q, want %q", typeMap["test.gauge"], MetricTypeGauge)
	}
	if typeMap["test.hist"] != MetricTypeHistogram {
		t.Errorf("test.hist type = %q, want %q", typeMap["test.hist"], MetricTypeHistogram)
	}
	if typeMap["test.unknown"] != MetricTypeGauge {
		t.Errorf("test.unknown type = %q, want %q (default)", typeMap["test.unknown"], MetricTypeGauge)
	}
}

func TestLoadMetricsFromGoFile(t *testing.T) {
	// Create a temporary Go file with metric definitions
	goContent := `package test

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaReplicaCount = metric.Metadata{
		Name:        "replicas",
		Help:        "Number of replicas",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}

	metaLatency = metric.Metadata{
		Name:        "request.latency",
		Help:        "Request latency in nanoseconds",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaQueueSize = metric.Metadata{
		Name:        "queue.size",
		Help:        "Current queue size",
		Measurement: "Queue Size",
		Unit:        metric.Unit_COUNT,
	}
)

// Usage in struct
type Metrics struct {
	ReplicaCount *metric.Counter
	Latency      *metric.Histogram
	QueueSize    *metric.Gauge
}

func newMetrics() *Metrics {
	return &Metrics{
		ReplicaCount: metric.NewCounter(metaReplicaCount),
		Latency:      metric.NewHistogram(metaLatency),
		QueueSize:    metric.NewGauge(metaQueueSize),
	}
}
`

	// Write to temp file
	tmpFile, err := os.CreateTemp("", "test_metrics_*.go")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err := tmpFile.WriteString(goContent); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpFile.Close()

	// Parse the Go file
	metrics, err := loadMetricsFromGoFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("loadMetricsFromGoFile failed: %v", err)
	}

	// Verify we found the expected metrics
	if len(metrics) != 3 {
		t.Errorf("Expected 3 metrics, got %d", len(metrics))
	}

	// Check replicas metric
	if m, ok := metrics["replicas"]; !ok {
		t.Error("Expected to find 'replicas' metric")
	} else {
		if m.Description != "Number of replicas" {
			t.Errorf("replicas.Help = %q, want %q", m.Description, "Number of replicas")
		}
		if m.YAxisLabel != "Replicas" {
			t.Errorf("replicas.Measurement = %q, want %q", m.YAxisLabel, "Replicas")
		}
		// Type detection from NewCounter usage
		if m.Type != "COUNTER" {
			t.Errorf("replicas.Type = %q, want %q", m.Type, "COUNTER")
		}
	}

	// Check latency metric
	if m, ok := metrics["request.latency"]; !ok {
		t.Error("Expected to find 'request.latency' metric")
	} else {
		// Should be HISTOGRAM due to Unit_NANOSECONDS
		if m.Type != "HISTOGRAM" {
			t.Errorf("request.latency.Type = %q, want %q", m.Type, "HISTOGRAM")
		}
	}

	// Check queue size metric
	if m, ok := metrics["queue.size"]; !ok {
		t.Error("Expected to find 'queue.size' metric")
	} else {
		// Should be GAUGE from NewGauge usage
		if m.Type != "GAUGE" {
			t.Errorf("queue.size.Type = %q, want %q", m.Type, "GAUGE")
		}
	}
}

func TestGetMetricPrefix(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"replicas", "replicas"},
		{"replicas.leaders", "replicas"},
		{"queue.replicate.pending", "queue"},
		{"kv.replica.read.batch.evaluate.latency", "kv"},
		{"admission.io.overload", "admission"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getMetricPrefix(tt.name)
			if got != tt.want {
				t.Errorf("getMetricPrefix(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestGroupQueriesByPrefix(t *testing.T) {
	metrics := []MetricDef{
		{Name: "replicas", Type: MetricTypeGauge},
		{Name: "replicas.leaders", Type: MetricTypeGauge},
		{Name: "queue.replicate.pending", Type: MetricTypeGauge},
		{Name: "queue.gc.pending", Type: MetricTypeGauge},
		{Name: "kv.replica.latency", Type: MetricTypeHistogram},
	}

	grouped := groupQueriesByPrefix(metrics)

	if len(grouped) != 3 {
		t.Errorf("Expected 3 groups, got %d", len(grouped))
	}

	if len(grouped["replicas"]) != 2 {
		t.Errorf("Expected 2 metrics in 'replicas' group, got %d", len(grouped["replicas"]))
	}

	if len(grouped["queue"]) != 2 {
		t.Errorf("Expected 2 metrics in 'queue' group, got %d", len(grouped["queue"]))
	}

	if len(grouped["kv"]) != 1 {
		t.Errorf("Expected 1 metric in 'kv' group, got %d", len(grouped["kv"]))
	}
}

func TestDetermineMetricType(t *testing.T) {
	tests := []struct {
		name         string
		varName      string
		blockContent string
		fullContent  string
		want         string
	}{
		{
			name:         "counter in variable name",
			varName:      "metaMyCounter",
			blockContent: `Name: "my.metric"`,
			fullContent:  "",
			want:         "COUNTER",
		},
		{
			name:         "count in variable name",
			varName:      "metaRequestCount",
			blockContent: `Name: "request.count"`,
			fullContent:  "",
			want:         "COUNTER",
		},
		{
			name:         "histogram in variable name",
			varName:      "metaLatencyHistogram",
			blockContent: `Name: "request.latency"`,
			fullContent:  "",
			want:         "HISTOGRAM",
		},
		{
			name:         "latency in variable name",
			varName:      "metaRequestLatency",
			blockContent: `Name: "request.latency"`,
			fullContent:  "",
			want:         "HISTOGRAM",
		},
		{
			name:         "duration in variable name",
			varName:      "metaOperationDuration",
			blockContent: `Name: "operation.duration"`,
			fullContent:  "",
			want:         "HISTOGRAM",
		},
		{
			name:         "nanoseconds unit indicates histogram",
			varName:      "metaGeneric",
			blockContent: `Name: "generic.metric", Unit: metric.Unit_NANOSECONDS`,
			fullContent:  "",
			want:         "HISTOGRAM",
		},
		{
			name:         "NewCounter usage",
			varName:      "metaGeneric",
			blockContent: `Name: "generic.metric"`,
			fullContent:  "Counter: metric.NewCounter(metaGeneric)",
			want:         "COUNTER",
		},
		{
			name:         "NewGauge usage",
			varName:      "metaGeneric",
			blockContent: `Name: "generic.metric"`,
			fullContent:  "Gauge: metric.NewGauge(metaGeneric)",
			want:         "GAUGE",
		},
		{
			name:         "NewHistogram usage",
			varName:      "metaGeneric",
			blockContent: `Name: "generic.metric"`,
			fullContent:  "Histogram: metric.NewHistogram(metaGeneric)",
			want:         "HISTOGRAM",
		},
		{
			name:         "default to gauge",
			varName:      "metaGeneric",
			blockContent: `Name: "generic.metric"`,
			fullContent:  "",
			want:         "GAUGE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := determineMetricType(tt.varName, tt.blockContent, tt.fullContent)
			if got != tt.want {
				t.Errorf("determineMetricType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGenerateDefaultOutputFilename(t *testing.T) {
	tests := []struct {
		name        string
		goFiles     []string
		searchTerms []string
		prefixes    []string
		want        string
	}{
		{
			name:    "single go file",
			goFiles: []string{"pkg/kv/kvserver/metrics.go"},
			want:    "kvserver_metrics_datadog.json",
		},
		{
			name:    "multiple go files",
			goFiles: []string{"pkg/kv/kvserver/metrics.go", "pkg/kv/kvserver/rangefeed/metrics.go"},
			want:    "kvserver_metrics_rangefeed_metrics_datadog.json",
		},
		{
			name:        "search terms",
			searchTerms: []string{"sql.service", "latency"},
			want:        "sql_service_latency_datadog.json",
		},
		{
			name:     "prefixes",
			prefixes: []string{"mma", "raft"},
			want:     "mma_raft_datadog.json",
		},
		{
			name:        "mixed go file and search",
			goFiles:     []string{"pkg/kv/kvserver/metrics.go"},
			searchTerms: []string{"replica"},
			want:        "kvserver_metrics_replica_datadog.json",
		},
		{
			name: "empty input",
			want: "metrics_datadog.json",
		},
		{
			name:        "more than 3 parts truncated",
			goFiles:     []string{"a.go", "b.go"},
			searchTerms: []string{"c", "d"},
			want:        "a_b_c_datadog.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateDefaultOutputFilename(tt.goFiles, tt.searchTerms, tt.prefixes)
			if got != tt.want {
				t.Errorf("generateDefaultOutputFilename() = %q, want %q", got, tt.want)
			}
		})
	}
}
