// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	grafanaOutputFile string
)

func newConvertGrafanaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "convert-grafana <input.json>",
		Short: "Convert a Grafana dashboard JSON to Datadog format",
		Long: `Convert a Grafana dashboard JSON to Datadog dashboard JSON.

Handles all query patterns found in CockroachDB Grafana dashboards including:
  - histogram_quantile() expressions
  - sum by / avg by aggregations
  - rate() conversions
  - Division expressions with multiple metrics

Query formats follow Datadog best practices:
  - Counter: sum:metric{tags} by {group}.as_rate().rollup(max, 10)
  - Gauge: avg:metric{tags} by {group}.rollup(avg, 10)
  - Histogram: pXX:metric{tags} by {group}.rollup(max, 10)`,
		Example: `  # Default output (output: grafana_dashboard_datadog.json)
  datadoggen convert-grafana grafana_dashboard.json

  # Custom output file
  datadoggen convert-grafana input.json -o custom_output.json

  # Generate for self-hosted tsdump format (crdb.tsdump.* prefix, $upload_id tag)
  datadoggen convert-grafana grafana_dashboard.json --tsdump

  # Combine tsdump with custom output
  datadoggen convert-grafana input.json --tsdump -o my_tsdump_dashboard.json`,
		Args: cobra.ExactArgs(1),
		RunE: runConvertGrafana,
	}

	cmd.Flags().StringVarP(&grafanaOutputFile, "output", "o", "", "Output file (default: <input>_datadog.json)")
	cmd.Flags().BoolVar(&TsdumpMode, "tsdump", false, "Generate queries for self-hosted tsdump format (crdb.tsdump.* prefix, $upload_id tag)")

	return cmd
}

// GrafanaPanel represents a panel in a Grafana dashboard.
type GrafanaPanel struct {
	Type        string          `json:"type"`
	Title       string          `json:"title"`
	Description string          `json:"description,omitempty"`
	Targets     []GrafanaTarget `json:"targets,omitempty"`
	Panels      []GrafanaPanel  `json:"panels,omitempty"` // For row panels with collapsed content
	Collapsed   bool            `json:"collapsed,omitempty"`
	GridPos     *GridPos        `json:"gridPos,omitempty"`
	FieldConfig *FieldConfig    `json:"fieldConfig,omitempty"`
}

// GrafanaTarget represents a query target in a Grafana panel.
type GrafanaTarget struct {
	Expr         string `json:"expr"`
	RefID        string `json:"refId"`
	LegendFormat string `json:"legendFormat,omitempty"`
}

// GridPos represents grid position in Grafana.
type GridPos struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

// FieldConfig represents field configuration.
type FieldConfig struct {
	Defaults FieldDefaults `json:"defaults,omitempty"`
}

// FieldDefaults represents default field settings.
type FieldDefaults struct {
	Unit   string       `json:"unit,omitempty"`
	Custom CustomConfig `json:"custom,omitempty"`
}

// CustomConfig represents custom configuration.
type CustomConfig struct {
	DrawStyle string         `json:"drawStyle,omitempty"`
	Stacking  StackingConfig `json:"stacking,omitempty"`
}

// StackingConfig represents stacking configuration.
type StackingConfig struct {
	Mode string `json:"mode,omitempty"`
}

// GrafanaDashboard represents a Grafana dashboard.
type GrafanaDashboard struct {
	Title  string         `json:"title"`
	Panels []GrafanaPanel `json:"panels"`
}

const defaultYAMLPathGrafana = "docs/generated/metrics/metrics.yaml"
const defaultDatadogYAMLPath = "pkg/cli/files/cockroachdb_datadog_metrics.yaml"
const defaultBaseMetricsYAMLPath = "pkg/roachprod/agents/opentelemetry/files/cockroachdb_metrics_base.yaml"
const defaultFullMetricsYAMLPath = "pkg/roachprod/agents/opentelemetry/files/cockroachdb_metrics.yaml"

func runConvertGrafana(cmd *cobra.Command, args []string) error {
	inputFile := args[0]

	// Generate output filename if not provided
	outputFile := grafanaOutputFile
	if outputFile == "" {
		outputFile = strings.TrimSuffix(inputFile, ".json") + "_datadog.json"
	}

	// Load Datadog-specific metric lookup for accurate name conversion (preferred)
	if err := LoadDatadogMetricLookup(defaultDatadogYAMLPath); err != nil {
		// Not fatal - we'll try the fallback
		fmt.Printf("Note: Could not load cockroachdb_datadog_metrics.yaml: %v\n", err)
	}

	// Load base metrics lookup (legacy/runtime conditional metrics)
	if err := LoadBaseMetricLookup(defaultBaseMetricsYAMLPath); err != nil {
		// Not fatal - we'll try other fallbacks
		fmt.Printf("Note: Could not load cockroachdb_metrics_base.yaml: %v\n", err)
	}

	// Load full metrics lookup (comprehensive auto-generated file)
	if err := LoadFullMetricLookup(defaultFullMetricsYAMLPath); err != nil {
		// Not fatal - we'll try other fallbacks
		fmt.Printf("Note: Could not load cockroachdb_metrics.yaml: %v\n", err)
	}

	// Load metric name lookup as fallback
	if err := LoadMetricNameLookup(defaultYAMLPathGrafana); err != nil {
		// Not fatal - we'll fall back to simple underscore conversion
		fmt.Printf("Note: Could not load metrics.yaml for name lookup: %v\n", err)
		fmt.Println("      Falling back to simple underscore-to-dot conversion.")
	}

	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("GRAFANA TO DATADOG DASHBOARD CONVERTER")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("\nInput:  %s\n", inputFile)
	fmt.Printf("Output: %s\n", outputFile)
	fmt.Printf("\nDatadog query configuration:\n")
	fmt.Printf("  Metric prefix: %s\n", MetricPrefix)
	fmt.Printf("  Default tags: %s\n", GetDefaultTags())
	fmt.Printf("  Rollup interval: %ds (matches scrape interval)\n", RollupInterval)
	fmt.Printf("  Counter format: sum:metric{tags} by {group}.as_rate().rollup(max, %d)\n", RollupInterval)
	fmt.Printf("  Gauge format: avg:metric{tags} by {group}.rollup(avg, %d)\n", RollupInterval)
	fmt.Printf("  Histogram format: pXX:metric{tags} by {group}.rollup(max, %d)\n", RollupInterval)

	// Load Grafana dashboard
	fmt.Printf("\n%s\n", strings.Repeat("-", 70))
	fmt.Println("LOADING GRAFANA DASHBOARD")
	fmt.Println(strings.Repeat("-", 70))

	data, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	var grafana GrafanaDashboard
	if err := json.Unmarshal(data, &grafana); err != nil {
		return fmt.Errorf("failed to parse Grafana JSON: %w", err)
	}

	fmt.Printf("✓ Successfully loaded: %s\n", inputFile)
	fmt.Printf("  Dashboard title: %s\n", grafana.Title)
	fmt.Printf("  Total panels: %d\n", len(grafana.Panels))

	// Convert
	fmt.Printf("\n%s\n", strings.Repeat("-", 70))
	fmt.Println("CONVERTING PANELS")
	fmt.Println(strings.Repeat("-", 70))

	dashboard := convertGrafanaToDadog(&grafana)

	// Write output
	fmt.Printf("\n%s\n", strings.Repeat("-", 70))
	fmt.Println("WRITING OUTPUT")
	fmt.Println(strings.Repeat("-", 70))

	outputData, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	if err := os.WriteFile(outputFile, outputData, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	fmt.Printf("✓ Dashboard written to: %s\n", outputFile)
	fmt.Println(strings.Repeat("=", 70))

	// Print summary of metrics not found in lookup
	PrintMissingMetricsSummary()

	return nil
}

func convertGrafanaToDadog(grafana *GrafanaDashboard) *DatadogDashboard {
	dashboard := NewDashboard(
		grafana.Title,
		fmt.Sprintf("Converted from Grafana dashboard: %s", grafana.Title),
	)

	var currentGroup *Widget
	var groupWidgets []Widget

	for _, panel := range grafana.Panels {
		if panel.Type == "row" {
			// Save previous group if exists
			if currentGroup != nil && len(groupWidgets) > 0 {
				currentGroup.Definition.Widgets = groupWidgets
				dashboard.Widgets = append(dashboard.Widgets, *currentGroup)
			}

			// Start new group
			group := CreateGroupWidget(panel.Title, nil)
			currentGroup = &group
			groupWidgets = nil
			fmt.Printf("  Group: %s\n", panel.Title)

			// Handle collapsed rows with nested panels
			if panel.Collapsed && len(panel.Panels) > 0 {
				fmt.Printf("    (collapsed row with %d nested panels)\n", len(panel.Panels))
				for _, nested := range panel.Panels {
					widget := convertPanelToWidget(&nested)
					if widget != nil && hasQueries(widget) {
						groupWidgets = append(groupWidgets, *widget)
					}
				}
			}
		} else {
			widget := convertPanelToWidget(&panel)
			if widget != nil && hasQueries(widget) {
				if currentGroup != nil {
					groupWidgets = append(groupWidgets, *widget)
				} else {
					dashboard.Widgets = append(dashboard.Widgets, *widget)
				}
			}
		}
	}

	// Save last group
	if currentGroup != nil && len(groupWidgets) > 0 {
		currentGroup.Definition.Widgets = groupWidgets
		dashboard.Widgets = append(dashboard.Widgets, *currentGroup)
	}

	return dashboard
}

func hasQueries(widget *Widget) bool {
	for _, req := range widget.Definition.Requests {
		if len(req.Queries) > 0 {
			return true
		}
	}
	return widget.Definition.Type == "note"
}

func convertPanelToWidget(panel *GrafanaPanel) *Widget {
	switch panel.Type {
	case "timeseries", "state-timeline", "heatmap":
		return convertTimeseriesPanel(panel)
	default:
		// Create a note for unsupported panel types
		note := CreateNoteWidget(fmt.Sprintf("Panel: %s\nType: %s (not converted)", panel.Title, panel.Type))
		return &note
	}
}

func convertTimeseriesPanel(panel *GrafanaPanel) *Widget {
	var queries []WidgetQuery
	var formulas []Formula
	queryNames := make(map[string]bool)

	displayType := "line"
	if panel.Type == "state-timeline" {
		displayType = "bars"
	} else if panel.Type == "heatmap" {
		displayType = "area"
	} else if panel.FieldConfig != nil {
		if panel.FieldConfig.Defaults.Custom.DrawStyle == "bars" {
			displayType = "bars"
		} else if panel.FieldConfig.Defaults.Custom.Stacking.Mode == "normal" {
			displayType = "area"
		}
	}

	for i, target := range panel.Targets {
		if target.Expr == "" || strings.TrimSpace(target.Expr) == "" {
			continue
		}

		refID := target.RefID
		if refID == "" {
			refID = string(rune('a' + i))
		}
		refID = strings.ToLower(refID)

		// Convert PromQL to Datadog query
		ddQuery := convertPromQLToDatadog(target.Expr)
		if ddQuery == "" {
			ddQueries := extractAllMetricsAsQueries(target.Expr)
			if len(ddQueries) == 0 {
				continue
			}
			ddQuery = ddQueries[0]
		}

		// Handle division expressions which may have multiple metrics
		var ddQueries []string
		if strings.Contains(target.Expr, "/") {
			ddQueries = extractAllMetricsAsQueries(target.Expr)
		} else {
			ddQueries = []string{ddQuery}
		}

		for idx, q := range ddQueries {
			queryName := refID
			if idx > 0 {
				queryName = fmt.Sprintf("%s_%d", refID, idx)
			}

			// Ensure unique query name
			counter := 1
			for queryNames[queryName] {
				queryName = fmt.Sprintf("%s_%d_%d", refID, idx, counter)
				counter++
			}
			queryNames[queryName] = true

			queries = append(queries, WidgetQuery{
				DataSource: "metrics",
				Name:       queryName,
				Query:      q,
			})

			formula := Formula{Formula: queryName}
			if target.LegendFormat != "" && target.LegendFormat != "__auto" && idx == 0 {
				alias := strings.ReplaceAll(target.LegendFormat, "{{", "")
				alias = strings.ReplaceAll(alias, "}}", "")
				alias = strings.ReplaceAll(alias, "instance", "$instance")
				formula.Alias = alias
			}

			// Use Grafana panel's unit if available, otherwise look up from metrics.yaml
			var ddUnit string
			if panel.FieldConfig != nil && panel.FieldConfig.Defaults.Unit != "" {
				ddUnit = convertGrafanaUnitToDatadog(panel.FieldConfig.Defaults.Unit)
			}
			if ddUnit == "" {
				ddUnit = lookupUnitFromQuery(q)
			}
			if ddUnit != "" {
				formula.NumberFormat = &NumberFormat{
					Unit: &UnitFormat{
						Type:     "canonical_unit",
						UnitName: ddUnit,
					},
				}
			}

			formulas = append(formulas, formula)
		}
	}

	widget := Widget{
		Definition: WidgetDefinition{
			Type:      "timeseries",
			Title:     panel.Title,
			TitleSize: "16",
			Requests: []WidgetRequest{
				{
					ResponseFormat: "timeseries",
					Queries:        queries,
					Formulas:       formulas,
					DisplayType:    displayType,
				},
			},
		},
	}

	// Add layout if grid position exists
	if panel.GridPos != nil {
		widget.Layout = &WidgetLayout{
			X:      panel.GridPos.X / 2,
			Y:      panel.GridPos.Y / 2,
			Width:  max(panel.GridPos.W/2, 2),
			Height: max(panel.GridPos.H/2, 2),
		}
	}

	// Handle percentage unit
	if panel.FieldConfig != nil && panel.FieldConfig.Defaults.Unit == "percentunit" {
		widget.Definition.YAxis = &YAxisDefinition{
			Scale: "linear",
			Min:   "0",
			Max:   "1",
		}
	}

	return &widget
}

// PromQL to Datadog conversion patterns and functions

var promqlFuncs = map[string]bool{
	"rate": true, "sum": true, "avg": true, "max": true, "min": true,
	"histogram_quantile": true, "avg_over_time": true, "min_over_time": true,
	"max_over_time": true, "stddev": true, "ignoring": true,
	"group_left": true, "group_right": true, "by": true, "without": true,
}

func convertPromQLToDatadog(expr string) string {
	if expr == "" || strings.TrimSpace(expr) == "" {
		return ""
	}

	expr = strings.TrimSpace(expr)

	// Remove time range parameters
	expr = regexp.MustCompile(`\[\$__rate_interval\]`).ReplaceAllString(expr, "")
	expr = regexp.MustCompile(`\[\$__range\]`).ReplaceAllString(expr, "")
	expr = regexp.MustCompile(`\[\d+[smhd]\]`).ReplaceAllString(expr, "")

	// Pattern 1: histogram_quantile with sum by (labels,le) (rate(...))
	if match := regexp.MustCompile(`histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)\)`).FindStringSubmatch(expr); match != nil {
		quantile, _ := strconv.ParseFloat(match[1], 64)
		groupBy := cleanGroupBy(match[2])
		metric, labels := extractMetricAndLabels(match[3])
		if metric != "" {
			pPrefix := FormatPercentile(quantile)
			ddMetric := ConvertExportedMetricName(metric)
			return BuildHistogramQuery(ddMetric, labels, pPrefix, groupBy)
		}
	}

	// Pattern 2: histogram_quantile with sum(rate(...)) by (labels)
	if match := regexp.MustCompile(`histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\(rate\(([^)]+)\)\)\s*by\s*\(([^)]+)\)\)`).FindStringSubmatch(expr); match != nil {
		quantile, _ := strconv.ParseFloat(match[1], 64)
		inner := match[2]
		groupBy := cleanGroupBy(match[3])
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			pPrefix := FormatPercentile(quantile)
			ddMetric := ConvertExportedMetricName(metric)
			return BuildHistogramQuery(ddMetric, labels, pPrefix, groupBy)
		}
	}

	// Pattern 2b: histogram_quantile with sum(rate(...)) - no group by
	if match := regexp.MustCompile(`histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\(rate\(([^)]+)\)\)\)`).FindStringSubmatch(expr); match != nil {
		quantile, _ := strconv.ParseFloat(match[1], 64)
		inner := match[2]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			pPrefix := FormatPercentile(quantile)
			ddMetric := ConvertExportedMetricName(metric)
			return BuildHistogramQuery(ddMetric, labels, pPrefix, "")
		}
	}

	// Pattern 3: sum by (label) (rate(metric{labels})) - Counter
	if match := regexp.MustCompile(`sum\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)`).FindStringSubmatch(expr); match != nil {
		groupBy := match[1]
		inner := match[2]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			ddMetric := ConvertExportedMetricName(metric)
			return BuildCounterQuery(ddMetric, labels, "sum", groupBy)
		}
	}

	// Pattern 4: sum by (label) (metric{labels}) - no rate
	if match := regexp.MustCompile(`sum\s+by\s*\(([^)]+)\)\s*\(([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\)`).FindStringSubmatch(expr); match != nil {
		groupBy := match[1]
		metric := match[2]
		labels := cleanLabels(match[3])
		ddMetric := ConvertExportedMetricName(metric)
		if IsCounterMetric(metric) {
			return BuildCounterQuery(ddMetric, labels, "sum", groupBy)
		}
		return BuildGaugeQuery(ddMetric, labels, "sum", groupBy)
	}

	// Pattern 5: sum(rate(metric{labels})) - no group by - Counter
	if match := regexp.MustCompile(`sum\(rate\(([^)]+)\)\)`).FindStringSubmatch(expr); match != nil {
		inner := match[1]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			ddMetric := ConvertExportedMetricName(metric)
			return BuildCounterQuery(ddMetric, labels, "sum", "")
		}
	}

	// Pattern 6: sum(metric{labels})
	if match := regexp.MustCompile(`sum\(([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\)`).FindStringSubmatch(expr); match != nil {
		metric := match[1]
		labels := cleanLabels(match[2])
		ddMetric := ConvertExportedMetricName(metric)
		if IsCounterMetric(metric) {
			return BuildCounterQuery(ddMetric, labels, "sum", "")
		}
		return BuildGaugeQuery(ddMetric, labels, "sum", "")
	}

	// Pattern 7: avg by (label) (metric{labels}) - Gauge
	if match := regexp.MustCompile(`avg\s+by\s*\(([^)]+)\)\s*\(([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}\)`).FindStringSubmatch(expr); match != nil {
		groupBy := match[1]
		metric := match[2]
		labels := cleanLabels(match[3])
		ddMetric := ConvertExportedMetricName(metric)
		return BuildGaugeQuery(ddMetric, labels, "avg", groupBy)
	}

	// Pattern 8: avg by (label) (rate(metric{labels})) - Counter
	if match := regexp.MustCompile(`avg\s+by\s*\(([^)]+)\)\s*\(rate\(([^)]+)\)\)`).FindStringSubmatch(expr); match != nil {
		groupBy := match[1]
		inner := match[2]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			ddMetric := ConvertExportedMetricName(metric)
			return BuildCounterQuery(ddMetric, labels, "avg", groupBy)
		}
	}

	// Pattern 9: rate(metric{labels}) - Counter
	if match := regexp.MustCompile(`^rate\(([^)]+)\)$`).FindStringSubmatch(expr); match != nil {
		inner := match[1]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			ddMetric := ConvertExportedMetricName(metric)
			return BuildCounterQuery(ddMetric, labels, "avg", "")
		}
	}

	// Pattern 10: avg_over_time(metric{labels}) - Gauge
	if match := regexp.MustCompile(`avg_over_time\(([^)]+)\)`).FindStringSubmatch(expr); match != nil {
		inner := match[1]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			ddMetric := ConvertExportedMetricName(metric)
			return BuildGaugeQuery(ddMetric, labels, "avg", "")
		}
	}

	// Pattern 11: max by (label) (avg_over_time(metric{labels})) - Gauge
	if match := regexp.MustCompile(`max\s+by\s*\(([^)]+)\)\s*\(avg_over_time\(([^)]+)\)\)`).FindStringSubmatch(expr); match != nil {
		groupBy := match[1]
		inner := match[2]
		metric, labels := extractMetricAndLabels(inner)
		if metric != "" {
			ddMetric := ConvertExportedMetricName(metric)
			return BuildGaugeQuery(ddMetric, labels, "max", groupBy)
		}
	}

	// Pattern 12: Simple metric{labels}
	if match := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}$`).FindStringSubmatch(expr); match != nil {
		metric := match[1]
		labels := cleanLabels(match[2])
		ddMetric := ConvertExportedMetricName(metric)
		if IsCounterMetric(metric) {
			return BuildCounterQuery(ddMetric, labels, "", "")
		}
		return BuildGaugeQuery(ddMetric, labels, "", "")
	}

	// Pattern 13: Just metric name without labels
	if match := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)$`).FindStringSubmatch(expr); match != nil {
		metric := match[1]
		ddMetric := ConvertExportedMetricName(metric)
		if IsCounterMetric(metric) {
			return BuildCounterQuery(ddMetric, GetDefaultTags(), "", "")
		}
		return BuildGaugeQuery(ddMetric, GetDefaultTags(), "", "")
	}

	// Fallback: Try to extract the main metric from complex expressions
	if match := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}`).FindStringSubmatch(expr); match != nil {
		metric := match[1]
		if !promqlFuncs[strings.ToLower(metric)] {
			labels := cleanLabels(match[2])
			ddMetric := ConvertExportedMetricName(metric)
			hasRate := strings.Contains(strings.ToLower(expr), "rate(")
			if hasRate || IsCounterMetric(metric) {
				return BuildCounterQuery(ddMetric, labels, "", "")
			}
			return BuildGaugeQuery(ddMetric, labels, "", "")
		}
	}

	return ""
}

func extractAllMetricsAsQueries(expr string) []string {
	if expr == "" || strings.TrimSpace(expr) == "" {
		return nil
	}

	// Remove time range parameters
	expr = regexp.MustCompile(`\[\$__rate_interval\]`).ReplaceAllString(expr, "")
	expr = regexp.MustCompile(`\[\$__range\]`).ReplaceAllString(expr, "")
	expr = regexp.MustCompile(`\[\d+[smhd]\]`).ReplaceAllString(expr, "")

	hasRate := strings.Contains(strings.ToLower(expr), "rate(")

	// Find all metric{labels} patterns
	matches := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}`).FindAllStringSubmatch(expr, -1)

	var queries []string
	seenMetrics := make(map[string]bool)

	for _, match := range matches {
		metric := match[1]
		labels := match[2]

		if promqlFuncs[strings.ToLower(metric)] {
			continue
		}
		if seenMetrics[metric] {
			continue
		}
		seenMetrics[metric] = true

		cleanLabel := cleanLabels(labels)
		ddMetric := ConvertExportedMetricName(metric)

		if hasRate || IsCounterMetric(metric) {
			queries = append(queries, BuildCounterQuery(ddMetric, cleanLabel, "", ""))
		} else {
			queries = append(queries, BuildGaugeQuery(ddMetric, cleanLabel, "", ""))
		}
	}

	return queries
}

func extractMetricAndLabels(expr string) (string, string) {
	// Match metric_name{labels}
	if match := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\{([^}]*)\}`).FindStringSubmatch(strings.TrimSpace(expr)); match != nil {
		return match[1], cleanLabels(match[2])
	}

	// Match just metric_name
	if match := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)$`).FindStringSubmatch(strings.TrimSpace(expr)); match != nil {
		return match[1], GetDefaultTags()
	}

	return "", ""
}

func cleanLabels(labelsStr string) string {
	// Always return default tags for now, as the Python version does
	return GetDefaultTags()
}

func cleanGroupBy(groupBy string) string {
	// Remove 'le' from group by (used for histogram buckets in Prometheus)
	groupBy = strings.ReplaceAll(groupBy, "le", "")
	groupBy = strings.Trim(groupBy, " ,")
	if groupBy == "" {
		return "node_id"
	}
	return groupBy
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// lookupUnitFromQuery extracts the metric name from a Datadog query and looks up its unit.
// Query format: "agg:cockroachdb.metric.name{tags} by {group}.rollup(...)"
func lookupUnitFromQuery(query string) string {
	// Extract metric name from query
	// Pattern: "agg:cockroachdb.metric.name{" or "agg:cockroachdb.metric.name "
	re := regexp.MustCompile(`(?:avg|sum|max|min|p\d+(?:\.\d+)?):([a-zA-Z0-9_.]+)`)
	match := re.FindStringSubmatch(query)
	if len(match) < 2 {
		return ""
	}
	metricName := match[1]
	return LookupMetricUnit(metricName)
}

// convertGrafanaUnitToDatadog converts Grafana unit names to Datadog canonical unit names.
func convertGrafanaUnitToDatadog(grafanaUnit string) string {
	switch grafanaUnit {
	// Time units
	case "ns":
		return "nanosecond"
	case "µs", "us":
		return "microsecond"
	case "ms":
		return "millisecond"
	case "s":
		return "second"
	case "m":
		return "minute"
	case "h":
		return "hour"
	case "d":
		return "day"
	// Data units
	case "bytes", "decbytes":
		return "byte"
	case "bits", "decbits":
		return "bit"
	case "kbytes":
		return "kibibyte"
	case "mbytes":
		return "mebibyte"
	case "gbytes":
		return "gibibyte"
	case "Bps", "binBps", "decBps":
		return "byte"
	case "binbps", "bps", "decbps":
		return "bit"
	case "KBs":
		return "kibibyte"
	case "MBs":
		return "mebibyte"
	case "GBs":
		return "gibibyte"
	// Percentage
	case "percent", "percentunit":
		return "percent"
	// Throughput
	case "ops", "opm", "ops/s", "reqps", "rps", "wps", "iops":
		return "operation"
	case "reqpm", "rpm":
		return "operation"
	// Count
	case "short", "none":
		return ""
	default:
		return ""
	}
}
