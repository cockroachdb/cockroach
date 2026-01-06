// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dashboard

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/util/yamlutil"
)

//go:embed data
var dashboardConfigs embed.FS

// TypeScriptTemplate contains the Go template for generating TypeScript dashboard files.
const TypeScriptTemplate = `// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// THIS FILE IS GENERATED. DO NOT EDIT.
// To regenerate: ./dev generate dashboards

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
{{if .Config.Imports}}{{range .Config.Imports}}{{.}};
{{end}}{{else}}
{{end}}import { Axis, Metric } from "src/views/shared/components/metricQuery";

import {
  GraphDashboardProps,
  nodeDisplayName,
  storeIDsForNode,
} from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    storeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    storeIDsByNodeID,
    tenantSource,
  } = props;

  return [
{{range $i, $chart := .Config.Charts}}{{if $i}},

{{end}}    <LineGraph
      title="{{$chart.Title}}"
      isKvGraph={{eq (getChartGraphType $chart) "kv"}}
      {{if getSources $chart}}sources={{getSources $chart}}{{end}}
      tenantSource={tenantSource}
      {{if renderTooltip $chart.Tooltip}}tooltip={{renderTooltip $chart.Tooltip}}{{end}}
      showMetricsInTooltip={{getShowMetricsInTooltip $chart}}
      preCalcGraphSize={{getPreCalcGraphSize $chart}}
    >
      <Axis{{getAxisProps $chart.Axis}}>
{{renderMetrics $chart.Metrics}}
      </Axis>
    </LineGraph>{{end}}
  ];
}`

// TemplateData holds the data passed to the template during generation.
type TemplateData struct {
	Config GeneratedDashboard
}

// MetricsYAMLStructure represents the top-level structure of metrics.yaml
type MetricsYAMLStructure struct {
	Layers []struct {
		Name       string `yaml:"name"`
		Categories []struct {
			Name    string                   `yaml:"name"`
			Metrics []map[string]interface{} `yaml:"metrics"`
		} `yaml:"categories"`
	} `yaml:"layers"`
}

// MetricMetadata contains enriched metadata about a metric from metrics.yaml
type MetricMetadata struct {
	Name        string
	Description string
	YAxisLabel  string
	Unit        string
	Type        string
	ChartConfig map[string]MetricChartConfigData
}

// MetricChartConfigData represents the top-level config for a metric in a specific dashboard
type MetricChartConfigData struct {
	Title   string                 `yaml:"title"`   // Metric's title/legend in the chart
	Options map[string]interface{} `yaml:"options"` // Metric-level options (e.g., rate)
	Chart   ChartConfigData        `yaml:"chart"`   // Chart-level configuration
}

// ChartConfigData represents chart configuration for a specific dashboard
type ChartConfigData struct {
	Title        string                 `yaml:"title"`
	Type         string                 `yaml:"type"`
	Units        string                 `yaml:"units"`
	AxisLabel    string                 `yaml:"axis_label"`
	Tooltip      interface{}            `yaml:"tooltip"` // Can be string or map with "text" and "note" fields
	Options      map[string]interface{} `yaml:"options"`
	RecordedName string                 `yaml:"recorded_name"`
}

// Generator provides dashboard generation functionality.
type Generator struct {
	metricsLookup map[string]*MetricMetadata
}

// NewGenerator creates a new dashboard generator.
func NewGenerator() *Generator {
	return &Generator{
		metricsLookup: make(map[string]*MetricMetadata),
	}
}

// LoadMetricsYAML loads and indexes metrics from metrics.yaml
func (g *Generator) LoadMetricsYAML(metricsYAMLPath string) error {
	data, err := os.ReadFile(metricsYAMLPath)
	if err != nil {
		return fmt.Errorf("reading metrics.yaml: %w", err)
	}

	var metricsYAML MetricsYAMLStructure
	if err := yamlutil.UnmarshalStrict(data, &metricsYAML); err != nil {
		return fmt.Errorf("parsing metrics.yaml: %w", err)
	}

	// Build lookup map
	for _, layer := range metricsYAML.Layers {
		for _, category := range layer.Categories {
			for _, metricData := range category.Metrics {
				name, _ := metricData["name"].(string)
				if name == "" {
					continue
				}

				metadata := &MetricMetadata{
					Name: name,
				}

				if desc, ok := metricData["description"].(string); ok {
					metadata.Description = desc
				}
				if yAxisLabel, ok := metricData["y_axis_label"].(string); ok {
					metadata.YAxisLabel = yAxisLabel
				}
				if unit, ok := metricData["unit"].(string); ok {
					metadata.Unit = unit
				}
				if metricType, ok := metricData["type"].(string); ok {
					metadata.Type = metricType
				}

				// Parse chart_config if present
				if chartConfigs, ok := metricData["chart_config"].(map[string]interface{}); ok {
					metadata.ChartConfig = make(map[string]MetricChartConfigData)

					for dashboardKey, metricChartConfigListRaw := range chartConfigs {
						// Handle new MetricConfigList structure with configs array
						var configMaps []map[string]interface{}

						// The new structure is a list of configs
						if configsArray, ok := metricChartConfigListRaw.([]interface{}); ok {
							// New structure: array of MetricConfig
							for _, configRaw := range configsArray {
								if configMap, ok := configRaw.(map[string]interface{}); ok {
									configMaps = append(configMaps, configMap)
								}
							}
						} else if metricChartConfigMap, ok := metricChartConfigListRaw.(map[string]interface{}); ok {
							// Old structure: single MetricConfig map (for backward compatibility)
							configMaps = append(configMaps, metricChartConfigMap)
						}

						// Process each config
						for _, metricChartConfigMap := range configMaps {
							metricChartConfig := MetricChartConfigData{}

							// Metric title and options
							metricChartConfig.Title, _ = metricChartConfigMap["title"].(string)
							if options, ok := metricChartConfigMap["options"].(map[string]interface{}); ok {
								metricChartConfig.Options = options
							}

							// Parse nested chart configuration
							if chartMap, ok := metricChartConfigMap["chart"].(map[string]interface{}); ok {
								chartData := ChartConfigData{}

								chartData.Title, _ = chartMap["title"].(string)
								chartData.Type, _ = chartMap["type"].(string)
								chartData.Units, _ = chartMap["units"].(string)
								chartData.AxisLabel, _ = chartMap["axis_label"].(string)

								// Tooltip can be either a string or a map with "text" and "note" fields
								if tooltipStr, ok := chartMap["tooltip"].(string); ok {
									chartData.Tooltip = tooltipStr
								} else if tooltipMap, ok := chartMap["tooltip"].(map[string]interface{}); ok {
									// Convert map[string]interface{} to map[string]string
									tooltip := make(map[string]string)
									if text, ok := tooltipMap["text"].(string); ok {
										tooltip["text"] = text
									}
									if note, ok := tooltipMap["note"].(string); ok {
										tooltip["note"] = note
									}
									chartData.Tooltip = tooltip
								}

								chartData.RecordedName, _ = chartMap["recorded_name"].(string)

								// Chart-level options
								if chartOptions, ok := chartMap["options"].(map[string]interface{}); ok {
									chartData.Options = chartOptions
								}

								metricChartConfig.Chart = chartData
							}

							// Store each config with a unique key: dashboardKey:chartTitle
							// This allows a metric to appear in multiple charts within the same dashboard
							storeKey := dashboardKey + ":" + metricChartConfig.Chart.Title
							metadata.ChartConfig[storeKey] = metricChartConfig
						}
					}
				}

				// Store metric metadata for lookup
				g.metricsLookup[name] = metadata
			}
		}
	}

	return nil
}

// buildChartFromMetrics constructs a Chart by aggregating all metrics that belong to the specified chart in the dashboard
func (g *Generator) buildChartFromMetrics(dashboardKey, chartTitle string) (Chart, error) {
	// Find all metrics that have chart_config for this dashboard and chart title
	var chartMetrics []Metric
	var chartConfig *ChartConfigData
	seenMetrics := make(map[string]bool) // Track metrics we've already added to avoid duplicates

	// First collect all matching metric names, then sort them for deterministic ordering
	var metricNames []string
	for name := range g.metricsLookup {
		metricNames = append(metricNames, name)
	}

	// Sort metric names alphabetically for deterministic ordering
	for i := 0; i < len(metricNames); i++ {
		for j := i + 1; j < len(metricNames); j++ {
			if metricNames[i] > metricNames[j] {
				metricNames[i], metricNames[j] = metricNames[j], metricNames[i]
			}
		}
	}

	// Build the lookup key using dashboardKey:chartTitle
	lookupKey := dashboardKey + ":" + chartTitle

	for _, name := range metricNames {
		metadata := g.metricsLookup[name]
		if metadata.ChartConfig == nil {
			continue
		}

		metricChartConfig, hasConfig := metadata.ChartConfig[lookupKey]
		if !hasConfig {
			continue
		}

		// Skip if we've already added this metric
		if seenMetrics[metadata.Name] {
			continue
		}
		seenMetrics[metadata.Name] = true

		// Save chart config (all metrics in the same chart should have the same chart config)
		if chartConfig == nil {
			chartConfig = &metricChartConfig.Chart
		}

		// Check if percentile option exists
		percentileStr, hasPercentile := metricChartConfig.Chart.Options["percentile"].(string)
		if hasPercentile && percentileStr != "" {
			// Split percentiles by comma (handles both single and comma-separated values)
			percentiles := strings.Split(percentileStr, ",")
			for _, p := range percentiles {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}

				// Create a copy of options for this specific percentile
				metricOptions := make(map[string]interface{})
				// First copy metric-level options
				for k, v := range metricChartConfig.Options {
					metricOptions[k] = v
				}
				metricOptions["percentile"] = p

				// Build metric with percentile suffix in the name
				metric := Metric{
					Name:    metricChartConfig.Chart.RecordedName + "-" + p,
					Title:   metricChartConfig.Title,
					Options: metricOptions,
				}

				chartMetrics = append(chartMetrics, metric)
			}
		} else {
			// Create metric options by merging metric-level and chart-level options
			metricOptions := make(map[string]interface{})
			// First copy metric-level options
			for k, v := range metricChartConfig.Options {
				metricOptions[k] = v
			}

			// Build metric with recorded name and title
			metric := Metric{
				Name:    metricChartConfig.Chart.RecordedName, // Use recorded name from metrics.yaml
				Title:   metricChartConfig.Title,
				Options: metricOptions,
			}

			chartMetrics = append(chartMetrics, metric)
		}
	}

	if chartConfig == nil {
		return Chart{}, fmt.Errorf("no metrics found for chart %q in dashboard %q", chartTitle, dashboardKey)
	}

	// Convert units from metrics.yaml format to dashboard format
	units := strings.ToLower(chartConfig.Units)
	switch units {
	case "nanoseconds", "duration":
		units = "duration"
	case "bytes":
		units = "bytes"
	case "percent":
		units = "percentage"
	default:
		units = "count"
	}

	// Build the chart
	chart := Chart{
		Title: chartConfig.Title,
		Type:  chartConfig.Type,
		Axis: Axis{
			Label: chartConfig.AxisLabel,
			Units: units,
		},
		Tooltip: chartConfig.Tooltip,
		Metrics: chartMetrics,
		Options: chartConfig.Options,
	}

	return chart, nil
}

// getAllChartTitlesForDashboard returns all unique chart titles for a dashboard from metrics.yaml
func (g *Generator) getAllChartTitlesForDashboard(dashboardKey string) []string {
	chartTitleSet := make(map[string]bool)

	// Iterate through all metrics and their chart configs
	for _, metadata := range g.metricsLookup {
		if metadata.ChartConfig == nil {
			continue
		}

		// Look for all configs that match this dashboard
		// Since keys are stored as "dashboardKey:chartTitle", we need to check all keys
		for configKey, metricChartConfig := range metadata.ChartConfig {
			// Check if this config key starts with the dashboard key
			if strings.HasPrefix(configKey, dashboardKey+":") {
				chartTitleSet[metricChartConfig.Chart.Title] = true
			}
		}
	}

	// Convert map to slice
	chartTitles := make([]string, 0, len(chartTitleSet))
	for title := range chartTitleSet {
		chartTitles = append(chartTitles, title)
	}

	return chartTitles
}

// sortChartsByConfigOrder sorts chart titles based on the order defined in dashboards.yaml
// Charts specified in configOrder appear first in that order, followed by any remaining charts
func (g *Generator) sortChartsByConfigOrder(
	allChartTitles []string, configOrder []string,
) []string {
	// Create a map for quick lookup of position in configOrder
	orderIndex := make(map[string]int)
	for i, title := range configOrder {
		orderIndex[title] = i
	}

	// Separate charts into ordered and unordered groups
	var orderedCharts []string
	var unorderedCharts []string

	for _, title := range allChartTitles {
		if _, hasOrder := orderIndex[title]; hasOrder {
			orderedCharts = append(orderedCharts, title)
		} else {
			unorderedCharts = append(unorderedCharts, title)
		}
	}

	// Sort the ordered charts based on their position in configOrder
	for i := 0; i < len(orderedCharts); i++ {
		for j := i + 1; j < len(orderedCharts); j++ {
			if orderIndex[orderedCharts[i]] > orderIndex[orderedCharts[j]] {
				orderedCharts[i], orderedCharts[j] = orderedCharts[j], orderedCharts[i]
			}
		}
	}

	// Combine ordered charts followed by unordered charts
	result := make([]string, 0, len(allChartTitles))
	result = append(result, orderedCharts...)
	result = append(result, unorderedCharts...)

	return result
}

// GetEmbeddedConfigs returns the embedded dashboard configuration files.
func (g *Generator) GetEmbeddedConfigs() embed.FS {
	return dashboardConfigs
}

// GenerateAllTypeScriptDashboards generates TypeScript dashboard files for all embedded YAML configurations.
func (g *Generator) GenerateAllTypeScriptDashboards(outputDir string) error {
	configFiles, err := dashboardConfigs.ReadDir("data")
	if err != nil {
		return fmt.Errorf("reading embedded configs: %w", err)
	}

	for _, file := range configFiles {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".yaml") {
			continue
		}

		yamlData, err := dashboardConfigs.ReadFile("data/" + file.Name())
		if err != nil {
			return fmt.Errorf("reading embedded file %s: %w", file.Name(), err)
		}

		// Try parsing as multi-dashboard format (dashboards.yaml)
		var dashboardsConfig DashboardsConfig
		if err := yamlutil.UnmarshalStrict(yamlData, &dashboardsConfig); err == nil && len(dashboardsConfig.Dashboards) > 0 {
			for _, dashConfig := range dashboardsConfig.Dashboards {
				if err := g.GenerateDashboard(dashConfig, outputDir); err != nil {
					return fmt.Errorf("generating dashboard %q from %s: %w", dashConfig.Name, file.Name(), err)
				}
			}
		}
	}

	return nil
}

// GenerateDashboard generates a TypeScript dashboard file from a DashboardConfig.
func (g *Generator) GenerateDashboard(dashConfig DashboardConfig, outputDir string) error {
	// Convert dashboard name to lowercase for filename
	dashboardName := strings.ToLower(strings.ReplaceAll(dashConfig.Name, " ", "_"))

	// Use uppercase dashboard name for lookup (e.g., "overview" -> "OVERVIEW")
	dashboardKey := strings.ToUpper(dashboardName)

	// Build full GeneratedDashboard by looking up chart metadata from metrics.yaml
	generatedDash := GeneratedDashboard{
		Name:        dashConfig.Name,
		Description: dashConfig.Description,
		Charts:      []Chart{},
		Imports:     dashConfig.Imports,
	}

	// Get all unique chart titles for this dashboard from metrics.yaml
	allChartTitles := g.getAllChartTitlesForDashboard(dashboardKey)

	// Sort charts based on order in dashboards.yaml
	sortedChartTitles := g.sortChartsByConfigOrder(allChartTitles, dashConfig.Charts)

	// Build charts in sorted order
	for _, chartTitle := range sortedChartTitles {
		chart, err := g.buildChartFromMetrics(dashboardKey, chartTitle)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: skipping chart %q: %v\n", chartTitle, err)
			continue
		}
		generatedDash.Charts = append(generatedDash.Charts, chart)
	}

	// Generate TypeScript file
	return g.generateTypeScriptFile(generatedDash, outputDir, dashboardName)
}

// generateTypeScriptFile generates a TypeScript dashboard file from a GeneratedDashboard.
func (g *Generator) generateTypeScriptFile(
	generatedDashboard GeneratedDashboard, outputDir, dashboardName string,
) error {
	// Create template with helper functions
	tmpl := template.New("dashboard").Funcs(template.FuncMap{
		"getSources":              getSources,
		"renderTooltip":           renderTooltip,
		"getShowMetricsInTooltip": getShowMetricsInTooltip,
		"getPreCalcGraphSize":     getPreCalcGraphSize,
		"getAxisProps":            getAxisProps,
		"renderMetrics":           renderMetrics,
		"eq":                      eq,
		// DB Console-specific helper functions
		"getChartGraphType":            GetChartGraphType,
		"getChartSources":              GetChartSources,
		"getChartShowMetricsInTooltip": GetChartShowMetricsInTooltip,
		"getChartPreCalcGraphSize":     GetChartPreCalcGraphSize,
		"getMetricRate":                GetMetricRate,
		"getMetricPerNode":             GetMetricPerNode,
		"getMetricSourcesType":         GetMetricSourcesType,
		"getMetricAggregation":         GetMetricAggregation,
	})

	tmpl, err := tmpl.Parse(TypeScriptTemplate)
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	// Generate output filename
	outputFile := fmt.Sprintf("%s.tsx", dashboardName)
	outputPath := filepath.Join(outputDir, outputFile)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer outFile.Close()

	// Execute template
	data := TemplateData{
		Config: generatedDashboard,
	}

	if err := tmpl.Execute(outFile, data); err != nil {
		return fmt.Errorf("executing template: %w", err)
	}

	return nil
}

// Helper functions for template

func getAxisUnits(units string) string {
	switch units {
	case "duration":
		return "AxisUnits.Duration"
	case "bytes":
		return "AxisUnits.Bytes"
	case "percentage":
		return "AxisUnits.Percentage"
	case "count":
		fallthrough
	default:
		return "AxisUnits.Count"
	}
}

func getSources(chart Chart) string {
	sources := GetChartSources(chart)
	switch sources {
	case "stores":
		return "{storeSources}"
	case "nodes":
		return "{nodeSources}"
	default:
		return ""
	}
}

func renderTooltip(tooltip any) string {
	switch v := tooltip.(type) {
	case string:
		if v == "" {
			return ""
		}
		if v == "capacity_graph_tooltip" {
			return "{<CapacityGraphTooltip tooltipSelection={tooltipSelection} />}"
		} else if v == "available_disc_capacity_graph_tooltip" {
			return "{<AvailableDiscCapacityGraphTooltip />}"
		}
		escaped := strings.ReplaceAll(v, "{tooltipSelection}", "${tooltipSelection}")
		return fmt.Sprintf("{<div>%s</div>}", escaped)
	case map[string]string:
		text, hasText := v["text"]
		note, hasNote := v["note"]
		if hasText && hasNote {
			text = strings.ReplaceAll(text, "{tooltipSelection}", "${tooltipSelection}")
			return fmt.Sprintf(`{
        <div>
          %s&nbsp;
          <em>
            %s
          </em>
        </div>
      }`, text, note)
		} else if hasText {
			return fmt.Sprintf("{`%s`}", text)
		}
	}
	if tooltip == nil {
		return ""
	}
	return fmt.Sprintf("{%v}", tooltip)
}

func getShowMetricsInTooltip(chart Chart) string {
	val := GetChartShowMetricsInTooltip(chart)
	if val == nil || *val {
		return "{true}"
	}
	return "{false}"
}

func getPreCalcGraphSize(chart Chart) string {
	val := GetChartPreCalcGraphSize(chart)
	if val != nil && *val {
		return "{true}"
	}
	return "{false}"
}

func getAxisProps(axis Axis) string {
	props := fmt.Sprintf(` units={%s}`, getAxisUnits(axis.Units))
	props += fmt.Sprintf(` label="%s"`, axis.Label)

	return props
}

func renderMetrics(metrics []Metric) string {
	var result strings.Builder

	for i, metric := range metrics {
		if i > 0 {
			result.WriteString("\n")
		}

		perNode := GetMetricPerNode(metric)
		if perNode != nil && *perNode {
			result.WriteString("        {nodeIDs.map(nid => (\n")
			result.WriteString("          <Metric\n")
			result.WriteString(fmt.Sprintf("            key=%s\n", GetKey(metric)))
			result.WriteString(fmt.Sprintf("            name=\"%s\"\n", metric.Name))
			result.WriteString(fmt.Sprintf("            title=%s\n", GetTitle(metric)))

			sourcesType := GetMetricSourcesType(metric)
			if sourcesType == "stores_for_node" {
				result.WriteString("            sources={storeIDsForNode(storeIDsByNodeID, nid)}\n")
			} else {
				result.WriteString("            sources={[nid]}\n")
			}

			rate := GetMetricRate(metric)
			if rate != nil && *rate {
				result.WriteString("            nonNegativeRate\n")
			}
			aggregation := GetMetricAggregation(metric)
			if aggregation != "" {
				result.WriteString(fmt.Sprintf("            %s\n", aggregation))
			}
			result.WriteString("          />\n")
			result.WriteString("        ))}")
		} else {
			result.WriteString("        <Metric\n")
			result.WriteString(fmt.Sprintf("          name=\"%s\"\n", metric.Name))
			result.WriteString(fmt.Sprintf("          title=\"%s\"\n", metric.Title))

			rate := GetMetricRate(metric)
			if rate != nil && *rate {
				result.WriteString("          nonNegativeRate\n")
			}
			aggregation := GetMetricAggregation(metric)
			if aggregation == "max" {
				result.WriteString("          downsampleMax\n")
			}

			result.WriteString("        />")
		}
	}

	return result.String()
}

func eq(a, b string) string {
	if a == b {
		return "{true}"
	}
	return "{false}"
}
