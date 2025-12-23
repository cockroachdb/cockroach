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
import map from "lodash/map";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { CapacityGraphTooltip } from "src/views/cluster/containers/nodeGraphs/dashboards/graphTooltips";
import { Axis, Metric } from "src/views/shared/components/metricQuery";

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
      tooltip={{renderTooltip $chart.Tooltip}}
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
	Config DashboardConfig
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
	Name                       string
	Description                string
	YAxisLabel                 string
	Unit                       string
	Type                       string
	MetricsVisualisationConfig *MetricsVisualisationConfigData
}

// MetricsVisualisationConfigData holds visualization configuration from metrics.yaml
type MetricsVisualisationConfigData struct {
	Title       string                     `yaml:"title"`
	Options     map[string]interface{}     `yaml:"options"`
	ChartConfig map[string]ChartConfigData `yaml:"chart_config"`
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

				// Parse metrics_visualisation_config if present
				if visConfig, ok := metricData["metrics_visualisation_config"].(map[interface{}]interface{}); ok {
					vizData := &MetricsVisualisationConfigData{
						ChartConfig: make(map[string]ChartConfigData),
					}

					// title is required
					vizData.Title, _ = visConfig["title"].(string)

					// options is optional (map)
					if options, ok := visConfig["options"].(map[interface{}]interface{}); ok {
						vizData.Options = make(map[string]interface{})
						for k, v := range options {
							if keyStr, ok := k.(string); ok {
								vizData.Options[keyStr] = v
							}
						}
					}

					// chart_config is optional (map of dashboard name to Chart)
					if chartConfigs, ok := visConfig["chart_config"].(map[interface{}]interface{}); ok {
						for dashboardKey, chartConfigRaw := range chartConfigs {
							dashboardName, _ := dashboardKey.(string)
							if chartConfigMap, ok := chartConfigRaw.(map[interface{}]interface{}); ok {
								chartData := ChartConfigData{}

								// All Chart fields are required except options
								chartData.Title, _ = chartConfigMap["title"].(string)
								chartData.Type, _ = chartConfigMap["type"].(string)
								chartData.Units, _ = chartConfigMap["units"].(string)
								chartData.AxisLabel, _ = chartConfigMap["axis_label"].(string)

								// Tooltip can be either a string or a map with "text" and "note" fields
								if tooltipStr, ok := chartConfigMap["tooltip"].(string); ok {
									chartData.Tooltip = tooltipStr
								} else if tooltipMap, ok := chartConfigMap["tooltip"].(map[interface{}]interface{}); ok {
									// Convert map[interface{}]interface{} to map[string]string
									tooltip := make(map[string]string)
									if text, ok := tooltipMap["text"].(string); ok {
										tooltip["text"] = text
									}
									if note, ok := tooltipMap["note"].(string); ok {
										tooltip["note"] = note
									}
									chartData.Tooltip = tooltip
								}

								chartData.RecordedName, _ = chartConfigMap["recorded_name"].(string)

								// options is optional
								if options, ok := chartConfigMap["options"].(map[interface{}]interface{}); ok {
									chartData.Options = make(map[string]interface{})
									for k, v := range options {
										if keyStr, ok := k.(string); ok {
											chartData.Options[keyStr] = v
										}
									}
								}

								vizData.ChartConfig[dashboardName] = chartData
							}
						}
					}

					metadata.MetricsVisualisationConfig = vizData
				}

				// Store with multiple key formats for lookup
				g.metricsLookup[name] = metadata
				g.metricsLookup["cr.node."+name] = metadata
				g.metricsLookup["cr.store."+name] = metadata
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

	for metricName, metadata := range g.metricsLookup {
		// Skip if this is a duplicate key (cr.node.* or cr.store.* variants)
		if strings.HasPrefix(metricName, "cr.node.") || strings.HasPrefix(metricName, "cr.store.") {
			continue
		}

		if metadata.MetricsVisualisationConfig == nil {
			continue
		}

		config, hasConfig := metadata.MetricsVisualisationConfig.ChartConfig[dashboardKey]
		if !hasConfig || config.Title != chartTitle {
			continue
		}

		// Skip if we've already added this metric
		if seenMetrics[metadata.Name] {
			continue
		}
		seenMetrics[metadata.Name] = true

		// Save chart config (all metrics in the same chart should have the same chart config)
		if chartConfig == nil {
			chartConfig = &config
		}

		// Check if percentile option contains comma-separated values
		percentileStr, hasPercentile := config.Options["percentile"].(string)
		if hasPercentile && strings.Contains(percentileStr, ",") {
			// Split percentiles and create a metric for each
			percentiles := strings.Split(percentileStr, ",")
			for _, p := range percentiles {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}

				// Create a copy of options for this specific percentile
				metricOptions := make(map[string]interface{})
				for k, v := range metadata.MetricsVisualisationConfig.Options {
					metricOptions[k] = v
				}
				metricOptions["percentile"] = p

				// Build metric with percentile suffix in the name
				metric := Metric{
					Name:    config.RecordedName + "-" + p,
					Title:   metadata.MetricsVisualisationConfig.Title,
					Options: metricOptions,
				}

				chartMetrics = append(chartMetrics, metric)
			}
		} else {
			// Build metric with recorded name and title from metrics_visualisation_config
			metric := Metric{
				Name:    config.RecordedName, // Use recorded name from metrics.yaml
				Title:   metadata.MetricsVisualisationConfig.Title,
				Options: metadata.MetricsVisualisationConfig.Options,
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

		dashboardName := strings.TrimSuffix(file.Name(), ".yaml")
		if err := g.GenerateTypeScript(yamlData, outputDir, dashboardName); err != nil {
			return fmt.Errorf("generating dashboard for %s: %w", file.Name(), err)
		}
	}

	return nil
}

// GenerateTypeScript generates a TypeScript dashboard file from YAML configuration.
func (g *Generator) GenerateTypeScript(yamlData []byte, outputDir, dashboardName string) error {
	// Parse the dashboard config (just name, description, and list of chart titles)
	var simplifiedConfig struct {
		Name        string   `yaml:"name"`
		Description string   `yaml:"description"`
		Charts      []string `yaml:"charts"`
	}
	if err := yamlutil.UnmarshalStrict(yamlData, &simplifiedConfig); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	// Build full DashboardConfig by looking up chart metadata from metrics.yaml
	config := DashboardConfig{
		Name:        simplifiedConfig.Name,
		Description: simplifiedConfig.Description,
		Charts:      []Chart{},
	}

	// Use uppercase dashboard name for lookup (e.g., "overview" -> "OVERVIEW")
	dashboardKey := strings.ToUpper(dashboardName)

	// Build charts from metrics.yaml based on requested chart titles
	for _, chartTitle := range simplifiedConfig.Charts {
		chart, err := g.buildChartFromMetrics(dashboardKey, chartTitle)
		if err != nil {
			// Skip charts that don't exist in metrics.yaml yet
			fmt.Fprintf(os.Stderr, "Warning: skipping chart %q: %v\n", chartTitle, err)
			continue
		}
		config.Charts = append(config.Charts, chart)
	}

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
	outputFile := fmt.Sprintf("%s_generated.tsx", dashboardName)
	outputPath := filepath.Join(outputDir, outputFile)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer outFile.Close()

	// Execute template
	data := TemplateData{
		Config: config,
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
		if v == "capacity_graph_tooltip" {
			return "{<CapacityGraphTooltip tooltipSelection={tooltipSelection} />}"
		}
		escaped := strings.ReplaceAll(v, "{tooltipSelection}", "${tooltipSelection}")
		return fmt.Sprintf("{`%s`}", escaped)
	case map[string]string:
		text, hasText := v["text"]
		note, hasNote := v["note"]
		if hasText && hasNote {
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
	return fmt.Sprintf("{`%v`}", tooltip)
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
	if val == nil || *val {
		return "{true}"
	}
	return "{false}"
}

func getAxisProps(axis Axis) string {
	props := fmt.Sprintf(` label="%s"`, axis.Label)
	if axis.Units != "count" {
		props += fmt.Sprintf(` units={%s}`, getAxisUnits(axis.Units))
	}
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
			result.WriteString("        {map(nodeIDs, node => (\n")
			result.WriteString("          <Metric\n")
			result.WriteString("            key={node}\n")
			result.WriteString(fmt.Sprintf("            name=\"%s\"\n", metric.Name))
			result.WriteString("            title={nodeDisplayName(nodeDisplayNameByID, node)}\n")

			sourcesType := GetMetricSourcesType(metric)
			if sourcesType == "stores_for_node" {
				result.WriteString("            sources={storeIDsForNode(storeIDsByNodeID, node)}\n")
			} else {
				result.WriteString("            sources={[node]}\n")
			}

			rate := GetMetricRate(metric)
			if rate != nil && *rate {
				result.WriteString("            nonNegativeRate\n")
			}
			aggregation := GetMetricAggregation(metric)
			if aggregation == "max" {
				result.WriteString("            downsampleMax\n")
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
