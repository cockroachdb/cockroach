// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Datadog configuration constants.
const (
	// MetricPrefix is the prefix for all CockroachDB metrics in Datadog.
	MetricPrefix = "cockroachdb"

	// DefaultTags are the default template variable tags for filtering.
	// No spaces - Datadog is sensitive to whitespace in tag filters.
	DefaultTags = "$cluster,$node_id,$store"

	// RollupInterval is the default rollup interval in seconds.
	// Matches CockroachDB scrape interval (both Cloud and self-hosted tsdump).
	RollupInterval = 10
)

// MetricType represents the type of a metric.
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
)

// MetricDef represents a metric definition.
type MetricDef struct {
	Name        string
	Help        string
	Measurement string
	Unit        string
	Type        MetricType
	LabeledName string
	Aggregation string // Hints at preferred aggregation (AVG, SUM, etc.)
}

// DatadogDashboard represents a Datadog dashboard.
type DatadogDashboard struct {
	Title             string             `json:"title"`
	Description       string             `json:"description"`
	LayoutType        string             `json:"layout_type"`
	IsReadOnly        bool               `json:"is_read_only"`
	TemplateVariables []TemplateVariable `json:"template_variables"`
	Widgets           []Widget           `json:"widgets"`
}

// TemplateVariable represents a Datadog template variable.
type TemplateVariable struct {
	Name            string   `json:"name"`
	Prefix          string   `json:"prefix"`
	AvailableValues []string `json:"available_values"`
	Default         string   `json:"default"`
}

// Widget represents a Datadog widget.
type Widget struct {
	Definition WidgetDefinition `json:"definition"`
	Layout     *WidgetLayout    `json:"layout,omitempty"`
}

// WidgetDefinition represents a widget definition.
// Field order matters for JSON output - matches Datadog's expected format.
type WidgetDefinition struct {
	Type            string           `json:"type"`
	LayoutType      string           `json:"layout_type,omitempty"` // For group widgets - must come before title
	Title           string           `json:"title,omitempty"`
	TitleSize       string           `json:"title_size,omitempty"`
	ShowTitle       bool             `json:"show_title,omitempty"` // For group widgets
	Widgets         []Widget         `json:"widgets,omitempty"`    // For group widgets
	Requests        []WidgetRequest  `json:"requests,omitempty"`   // For timeseries widgets
	Content         string           `json:"content,omitempty"`
	BackgroundColor string           `json:"background_color,omitempty"`
	FontSize        string           `json:"font_size,omitempty"`
	TextAlign       string           `json:"text_align,omitempty"`
	ShowTick        bool             `json:"show_tick,omitempty"`
	TickPos         string           `json:"tick_pos,omitempty"`
	TickEdge        string           `json:"tick_edge,omitempty"`
	YAxis           *YAxisDefinition `json:"yaxis,omitempty"`
}

// WidgetRequest represents a widget request.
type WidgetRequest struct {
	ResponseFormat string        `json:"response_format,omitempty"`
	Queries        []WidgetQuery `json:"queries,omitempty"`
	Formulas       []Formula     `json:"formulas,omitempty"`
	DisplayType    string        `json:"display_type,omitempty"`
	Style          *RequestStyle `json:"style,omitempty"`
}

// WidgetQuery represents a query in a widget.
type WidgetQuery struct {
	DataSource string `json:"data_source"`
	Name       string `json:"name"`
	Query      string `json:"query"`
}

// Formula represents a formula in a widget request.
type Formula struct {
	Formula      string        `json:"formula"`
	Alias        string        `json:"alias,omitempty"`
	NumberFormat *NumberFormat `json:"number_format,omitempty"`
}

// NumberFormat specifies the unit formatting for a formula.
type NumberFormat struct {
	Unit *UnitFormat `json:"unit,omitempty"`
}

// UnitFormat specifies the unit type and name for Datadog.
type UnitFormat struct {
	Type     string `json:"type"`      // "canonical_unit"
	UnitName string `json:"unit_name"` // e.g., "nanosecond", "byte"
}

// ConvertUnitToDatadog converts a metrics.yaml unit to Datadog's unit format.
// Returns empty string if the unit doesn't have a Datadog equivalent.
func ConvertUnitToDatadog(unit string) string {
	switch strings.ToUpper(unit) {
	case "NANOSECONDS", "TIMESTAMP_NS":
		return "nanosecond"
	case "MICROSECONDS":
		return "microsecond"
	case "MILLISECONDS":
		return "millisecond"
	case "SECONDS", "TIMESTAMP_SEC":
		return "second"
	case "BYTES":
		return "byte"
	case "PERCENT":
		return "percent"
	case "COUNT", "CONST":
		return "" // No unit display needed for counts
	default:
		return ""
	}
}

// RequestStyle represents styling for a request.
type RequestStyle struct {
	LineType  string `json:"line_type,omitempty"`
	LineWidth string `json:"line_width,omitempty"`
}

// YAxisDefinition represents y-axis configuration.
type YAxisDefinition struct {
	Scale string `json:"scale,omitempty"`
	Min   string `json:"min,omitempty"`
	Max   string `json:"max,omitempty"`
}

// WidgetLayout represents widget layout.
type WidgetLayout struct {
	X      int `json:"x"`
	Y      int `json:"y"`
	Width  int `json:"width"`
	Height int `json:"height"`
}

// DefaultTemplateVariables returns the default template variables for a dashboard.
func DefaultTemplateVariables() []TemplateVariable {
	return []TemplateVariable{
		{Name: "cluster", Prefix: "cluster", AvailableValues: []string{}, Default: "*"},
		{Name: "node_id", Prefix: "node_id", AvailableValues: []string{}, Default: "*"},
		{Name: "store", Prefix: "store", AvailableValues: []string{}, Default: "*"},
	}
}

// metricNameLookup maps exported_name (underscore format) to name (dot format).
// This is lazily loaded from metrics.yaml when needed.
var metricNameLookup map[string]string

// metricUnitLookup maps metric name (dot format) to its unit.
var metricUnitLookup map[string]string

// LoadMetricNameLookup loads the exported_name → name mapping from metrics.yaml.
// This enables accurate conversion of Prometheus-style names (sql_service_latency)
// to Datadog-style names (sql.service.latency).
func LoadMetricNameLookup(yamlPath string) error {
	nameLookup := make(map[string]string)
	unitLookup := make(map[string]string)

	file, err := os.Open(yamlPath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var currentName, currentExportedName, currentUnit string

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "- name:") {
			// Save previous mappings if we have both
			if currentName != "" && currentExportedName != "" {
				nameLookup[currentExportedName] = currentName
			}
			if currentName != "" && currentUnit != "" {
				unitLookup[currentName] = currentUnit
			}
			currentName = strings.TrimSpace(strings.TrimPrefix(trimmed, "- name:"))
			currentExportedName = ""
			currentUnit = ""
		} else if strings.HasPrefix(trimmed, "exported_name:") {
			currentExportedName = strings.TrimSpace(strings.TrimPrefix(trimmed, "exported_name:"))
		} else if strings.HasPrefix(trimmed, "unit:") {
			currentUnit = strings.TrimSpace(strings.TrimPrefix(trimmed, "unit:"))
		}
	}

	// Don't forget the last one
	if currentName != "" && currentExportedName != "" {
		nameLookup[currentExportedName] = currentName
	}
	if currentName != "" && currentUnit != "" {
		unitLookup[currentName] = currentUnit
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	metricNameLookup = nameLookup
	metricUnitLookup = unitLookup
	return nil
}

// LookupMetricUnit looks up the unit for a metric name (dot format).
// Returns the Datadog-compatible unit name, or empty string if not found.
func LookupMetricUnit(metricName string) string {
	if metricUnitLookup == nil {
		return ""
	}
	// Strip the cockroachdb. prefix if present
	name := strings.TrimPrefix(metricName, MetricPrefix+".")
	if unit, ok := metricUnitLookup[name]; ok {
		return ConvertUnitToDatadog(unit)
	}
	return ""
}

// ConvertMetricName converts a metric name to Datadog format.
// This is used when reading from metrics.yaml where names already have the correct format.
// Example: "kv.allocator.load_based_lease_transfers.should_transfer"
//
//	-> "cockroachdb.kv.allocator.load_based_lease_transfers.should_transfer"
func ConvertMetricName(name string) string {
	// Replace hyphens with underscores (for names like sys.cpu.combined.percent-normalized)
	ddMetric := strings.ReplaceAll(name, "-", "_")
	return fmt.Sprintf("%s.%s", MetricPrefix, ddMetric)
}

// ConvertExportedMetricName converts a Prometheus/exported metric name to Datadog format.
// Uses the lookup table to find the correct name with dots in the right places.
// Example: "sql_service_latency" -> "cockroachdb.sql.service.latency"
// Example: "kv_allocator_load_based_lease_transfers_should_transfer"
//
//	-> "cockroachdb.kv.allocator.load_based_lease_transfers.should_transfer"
func ConvertExportedMetricName(exportedName string) string {
	// Strip common suffixes that aren't part of the base metric name
	baseName := exportedName
	suffix := ""
	for _, s := range []string{"_bucket", "_count", "_sum", "_total"} {
		if strings.HasSuffix(baseName, s) {
			baseName = strings.TrimSuffix(baseName, s)
			// Don't keep _bucket suffix - Datadog histograms don't need it
			if s != "_bucket" {
				suffix = strings.TrimPrefix(s, "_")
			}
			break
		}
	}

	// Try lookup first
	if metricNameLookup != nil {
		if name, ok := metricNameLookup[baseName]; ok {
			result := ConvertMetricName(name)
			if suffix != "" {
				result += "." + suffix
			}
			return result
		}
	}

	// Fallback: convert all underscores to dots (less accurate but works)
	ddMetric := strings.ReplaceAll(baseName, "_", ".")
	result := fmt.Sprintf("%s.%s", MetricPrefix, ddMetric)
	if suffix != "" {
		result += "." + suffix
	}
	return result
}

// BuildCounterQuery builds a Datadog query for counter metrics.
//
// Format: sum:metric{tags} by {group}.as_rate().rollup(max, 10)
//
// Per Datadog guidelines (OI-Querying self-hosted DB metrics on Datadog):
//   - Always use as_rate() for monotonically increasing counters to calculate rate accurately
//   - Use MAX aggregation in rollup to ensure highest counter value within each interval,
//     accurately reflecting the rate
//   - Use 10s rollup interval to match CockroachDB scrape interval (both Cloud and tsdump)
//   - Note: Datadog charts limit to 1500 data points; interval may be ignored for large ranges
func BuildCounterQuery(metric, labels string, aggregator string, groupBy string) string {
	if aggregator == "" {
		aggregator = "sum"
	}
	if groupBy == "" {
		groupBy = "node_id"
	}
	return fmt.Sprintf("%s:%s{%s} by {%s}.as_rate().rollup(max, %d)",
		aggregator, metric, labels, groupBy, RollupInterval)
}

// BuildGaugeQuery builds a Datadog query for gauge metrics.
//
// Format: avg:metric{tags} by {group}.rollup(avg, 10)
//
// Per Datadog guidelines (OI-Querying self-hosted DB metrics on Datadog):
//   - Use AVG aggregation for stability; optionally MAX/MIN for spikes/dips
//   - Explicit rollup ensures consistent aggregation regardless of zoom level
//   - Use 10s rollup interval to match CockroachDB scrape interval (both Cloud and tsdump)
//   - Note: Datadog charts limit to 1500 data points; interval may be ignored for large ranges
func BuildGaugeQuery(metric, labels string, aggregator string, groupBy string) string {
	if aggregator == "" {
		aggregator = "avg"
	}
	if groupBy == "" {
		groupBy = "node_id"
	}
	return fmt.Sprintf("%s:%s{%s} by {%s}.rollup(%s, %d)",
		aggregator, metric, labels, groupBy, aggregator, RollupInterval)
}

// BuildHistogramQuery builds a Datadog query for histogram/percentile metrics.
//
// Format: p99:metric{tags} by {group}.rollup(max, 10)
//
// Per Datadog guidelines (OI-Querying self-hosted DB metrics on Datadog):
//   - Histograms are emitted as Gauges of specific percentiles
//   - Percentile metrics represent upper bounds; using MAX ensures peak latency values aren't missed
//   - Use 10s rollup interval to match CockroachDB scrape interval (both Cloud and tsdump)
//   - Note: Self-hosted uploads use different naming (e.g., crdb.tsdump.sql.service.latency_p99)
//   - Note: Datadog charts limit to 1500 data points; interval may be ignored for large ranges
func BuildHistogramQuery(metricBase, labels string, percentilePrefix string, groupBy string) string {
	if percentilePrefix == "" {
		percentilePrefix = "p99"
	}
	if groupBy == "" {
		groupBy = "node_id"
	}
	// Remove .bucket suffix if present
	metricBase = strings.TrimSuffix(metricBase, ".bucket")

	// Format: p99:metric{labels} by {group}.rollup(max, 10)
	// This works for Datadog distribution metrics with percentiles enabled.
	// Note: If you get "percentiles not enabled" error, the metric may need
	// to be queried as max:metric_p99{labels} instead (for tsdump metrics).
	return fmt.Sprintf("%s:%s{%s} by {%s}.rollup(max, %d)",
		percentilePrefix, metricBase, labels, groupBy, RollupInterval)
}

// BuildQuery builds a Datadog query based on metric type.
func BuildQuery(metric MetricDef) string {
	ddName := ConvertMetricName(metric.Name)
	tags := DefaultTags

	switch metric.Type {
	case MetricTypeCounter:
		return BuildCounterQuery(ddName, tags, "sum", "node_id")
	case MetricTypeHistogram:
		// For histograms, check the Aggregation hint from the YAML.
		// If AVG is specified, the metric may not support p99: percentile queries
		// (not configured as a Datadog Distribution metric), so use avg: instead.
		if strings.ToUpper(metric.Aggregation) == "AVG" {
			return BuildGaugeQuery(ddName, tags, "avg", "node_id")
		}
		return BuildHistogramQuery(ddName, tags, "p99", "node_id")
	default:
		return BuildGaugeQuery(ddName, tags, "avg", "node_id")
	}
}

// FormatPercentile formats a quantile value as a Datadog percentile prefix.
// Examples:
//
//	0.90 -> p90
//	0.99 -> p99
//	0.999 -> p99.9
//	0.9999 -> p99.99
//	1 -> max
func FormatPercentile(quantile float64) string {
	if quantile == 1 {
		return "max"
	}

	// Convert to percentage
	pct := quantile * 100

	// Check if it's a clean integer
	if pct == float64(int(pct)) {
		return fmt.Sprintf("p%d", int(pct))
	}
	// Format with appropriate decimal places
	return fmt.Sprintf("p%g", pct)
}

// IsCounterMetric determines if a metric is a counter based on naming conventions.
func IsCounterMetric(metricName string) bool {
	counterSuffixes := []string{
		"_count", "_total", "_sum", "_bucket",
		".count", ".total", ".sum", ".bucket",
		"_bytes", ".bytes", "_ops", ".ops",
	}

	metricLower := strings.ToLower(metricName)
	for _, suffix := range counterSuffixes {
		if strings.HasSuffix(metricLower, suffix) {
			return true
		}
	}
	return false
}

// CreateTimeseriesWidget creates a timeseries widget for a metric.
func CreateTimeseriesWidget(metric MetricDef, index int) Widget {
	query := BuildQuery(metric)

	// Create a readable title from the metric name
	title := strings.ReplaceAll(metric.Name, ".", " ")
	title = strings.ReplaceAll(title, "_", " ")
	title = strings.Title(title)

	// Create formula with unit if available
	formula := Formula{Formula: fmt.Sprintf("q%d", index)}
	if metric.Unit != "" {
		ddUnit := ConvertUnitToDatadog(metric.Unit)
		if ddUnit != "" {
			formula.NumberFormat = &NumberFormat{
				Unit: &UnitFormat{
					Type:     "canonical_unit",
					UnitName: ddUnit,
				},
			}
		}
	}

	return Widget{
		Definition: WidgetDefinition{
			Type:      "timeseries",
			Title:     title,
			TitleSize: "16",
			Requests: []WidgetRequest{
				{
					ResponseFormat: "timeseries",
					Queries: []WidgetQuery{
						{
							DataSource: "metrics",
							Name:       fmt.Sprintf("q%d", index),
							Query:      query,
						},
					},
					Formulas:    []Formula{formula},
					DisplayType: "line",
				},
			},
		},
	}
}

// CreateGroupWidget creates a group widget containing other widgets.
func CreateGroupWidget(title string, widgets []Widget) Widget {
	return Widget{
		Definition: WidgetDefinition{
			Type:       "group",
			LayoutType: "ordered",
			Title:      title,
			ShowTitle:  true,
			Widgets:    widgets,
		},
	}
}

// CreateNoteWidget creates a note widget.
func CreateNoteWidget(content string) Widget {
	return Widget{
		Definition: WidgetDefinition{
			Type:            "note",
			Content:         content,
			BackgroundColor: "yellow",
			FontSize:        "14",
			TextAlign:       "left",
			ShowTick:        false,
			TickPos:         "50%",
			TickEdge:        "left",
		},
		Layout: &WidgetLayout{X: 0, Y: 0, Width: 3, Height: 2},
	}
}

// NewDashboard creates a new Datadog dashboard with default configuration.
func NewDashboard(title, description string) *DatadogDashboard {
	return &DatadogDashboard{
		Title:             title,
		Description:       description,
		LayoutType:        "ordered",
		IsReadOnly:        false,
		TemplateVariables: DefaultTemplateVariables(),
		Widgets:           []Widget{},
	}
}
