// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/util/yamlutil"
	"github.com/spf13/cobra"
)

//go:embed files/dashboard.yaml
var dashboardYAML []byte

const (
	Datadog = "datadog"
	Grafana = "grafana"
)

var percentileSuffixes = []string{"-p99.999", "-p99.99", "-p99.9", "-p99", "-p90", "-p75", "-p50", "-max"}

// DashboardConfig represents the YAML configuration for a dashboard
type DashboardConfig struct {
	Graphs     []GraphConfig `yaml:"graphs,omitempty"`
	Dashboards []Dashboard   `yaml:"dashboards,omitempty"`
}

// Dashboard represents a dashboard section containing multiple graphs
type Dashboard struct {
	Title  string        `yaml:"title"`
	Graphs []GraphConfig `yaml:"graphs"`
}

// GraphConfig represents a single graph/chart in the dashboard
type GraphConfig struct {
	Title          string         `yaml:"title"`
	Tooltip        string         `yaml:"tooltip"`
	Axis           AxisConfig     `yaml:"axis"`
	Metrics        []MetricConfig `yaml:"metrics"`
	GroupByStorage bool           `yaml:"group_by_storage,omitempty"`
}

// AxisConfig represents the axis configuration for a graph
type AxisConfig struct {
	Label string `yaml:"label"`
	Units string `yaml:"units"`
}

// MetricConfig represents a single metric in a graph
type MetricConfig struct {
	Name  string `yaml:"name"`
	Title string `yaml:"title"`
}

// DatadogDashboard represents the top-level Datadog dashboard structure
type DatadogDashboard struct {
	Title             string                    `json:"title"`
	Description       string                    `json:"description"`
	LayoutType        string                    `json:"layout_type"`
	Type              string                    `json:"type,omitempty"`
	Widgets           []DatadogWidget           `json:"widgets"`
	TemplateVariables []DatadogTemplateVariable `json:"template_variables,omitempty"`
}

// DatadogTemplateVariable represents a template variable
type DatadogTemplateVariable struct {
	Name    string `json:"name"`
	Prefix  string `json:"prefix,omitempty"`
	Default string `json:"default"`
}

// DatadogWidget represents a widget in the dashboard
type DatadogWidget struct {
	Definition DatadogWidgetDefinition `json:"definition"`
	Layout     *DatadogLayout          `json:"layout,omitempty"`
}

// DatadogLayout represents the layout configuration for a widget
type DatadogLayout struct {
	X      int `json:"x"`
	Y      int `json:"y"`
	Width  int `json:"width"`
	Height int `json:"height"`
}

// DatadogWidgetDefinition represents the definition of a widget
type DatadogWidgetDefinition struct {
	Type          string           `json:"type"`
	Title         string           `json:"title,omitempty"`
	ShowTitle     bool             `json:"show_title,omitempty"`
	ShowLegend    bool             `json:"show_legend,omitempty"`
	LegendLayout  string           `json:"legend_layout,omitempty"`
	LegendColumns []string         `json:"legend_columns,omitempty"`
	LayoutType    string           `json:"layout_type,omitempty"`
	Widgets       []DatadogWidget  `json:"widgets,omitempty"`
	Requests      []DatadogRequest `json:"requests,omitempty"`
	YAxis         *DatadogYAxis    `json:"yaxis,omitempty"`
	// Image widget fields
	URL           string `json:"url,omitempty"`
	URLDarkTheme  string `json:"url_dark_theme,omitempty"`
	Sizing        string `json:"sizing,omitempty"`
	HasBackground bool   `json:"has_background,omitempty"`
	HasBorder     bool   `json:"has_border,omitempty"`
	// Note widget fields
	Content         string `json:"content,omitempty"`
	BackgroundColor string `json:"background_color,omitempty"`
	FontSize        string `json:"font_size,omitempty"`
	TextAlign       string `json:"text_align,omitempty"`
	VerticalAlign   string `json:"vertical_align,omitempty"`
	ShowTick        bool   `json:"show_tick,omitempty"`
	TickPos         string `json:"tick_pos,omitempty"`
	TickEdge        string `json:"tick_edge,omitempty"`
	HasPadding      bool   `json:"has_padding,omitempty"`
	// Group widget fields
	BannerImg       string `json:"banner_img,omitempty"`
	HorizontalAlign string `json:"horizontal_align,omitempty"`
}

// DatadogRequest represents a request for metrics data
type DatadogRequest struct {
	Formulas       []DatadogFormula `json:"formulas,omitempty"`
	Queries        []DatadogQuery   `json:"queries,omitempty"`
	ResponseFormat string           `json:"response_format,omitempty"`
	Style          *DatadogStyle    `json:"style,omitempty"`
	DisplayType    string           `json:"display_type,omitempty"`
}

// DatadogFormula represents a formula in a request
type DatadogFormula struct {
	Alias        string               `json:"alias,omitempty"`
	Formula      string               `json:"formula"`
	NumberFormat *DatadogNumberFormat `json:"number_format,omitempty"`
}

// DatadogNumberFormat specifies how numbers should be formatted
type DatadogNumberFormat struct {
	Unit *DatadogUnit `json:"unit,omitempty"`
}

// DatadogUnit specifies the unit for a number format
type DatadogUnit struct {
	Type        string `json:"type"`
	UnitName    string `json:"unit_name,omitempty"`
	PerUnitName string `json:"per_unit_name,omitempty"`
}

// DatadogQuery represents a metric query
type DatadogQuery struct {
	DataSource string `json:"data_source"`
	Name       string `json:"name"`
	Query      string `json:"query"`
}

// DatadogStyle specifies the visual style for a request
type DatadogStyle struct {
	Palette   string `json:"palette,omitempty"`
	LineType  string `json:"line_type,omitempty"`
	LineWidth string `json:"line_width,omitempty"`
}

// DatadogYAxis represents the Y-axis configuration
type DatadogYAxis struct {
	IncludeZero bool   `json:"include_zero,omitempty"`
	Scale       string `json:"scale,omitempty"`
	Min         string `json:"min,omitempty"`
	Max         string `json:"max,omitempty"`
}

var rollupInterval int
var outputFile string

var genDashboardCmd = &cobra.Command{
	Use:   "dashboard [datadog|grafana]",
	Short: "generate dashboard from embedded dashboard configuration",
	Long: `Generate a dashboard JSON file from the embedded CockroachDB dashboard YAML configuration.

This command reads the embedded dashboard.yaml file and generates a dashboard JSON file
in the format specified (datadog or grafana).

Examples:
  cockroach gen dashboard datadog
  cockroach gen dashboard grafana
  cockroach gen dashboard datadog --rollup-interval=30
  cockroach gen dashboard datadog --output=my-dashboard.json
  cockroach gen dashboard datadog --output=/path/to/dashboard.json --rollup-interval=60
`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"datadog", "grafana"},
	RunE:      clierrorplus.MaybeDecorateError(runGenDashboardCmd),
}

func init() {
	genDashboardCmd.Flags().IntVar(&rollupInterval, "rollup-interval", 10, "interval in seconds for metric rollup (default: 10)")
	genDashboardCmd.Flags().StringVar(&outputFile, "output", "", "output file path for the generated dashboard (must have .json extension)")
}

func runGenDashboardCmd(_ *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dashboardType := args[0]

	// Validate output file extension if specified
	if outputFile != "" && !strings.HasSuffix(outputFile, ".json") {
		return fmt.Errorf("output file must have .json extension: %s", outputFile)
	}

	// Generate metrics name mapping using the same logic as tsdump_upload
	metricsNameMap, err := generateMetricsNameMap(ctx)
	if err != nil {
		return fmt.Errorf("generating metrics name map: %w", err)
	}

	// Load metric type map to determine counter vs histogram types
	metricTypeMap, err := loadMetricTypesMap(ctx)
	if err != nil {
		return fmt.Errorf("generating metric type map: %w", err)
	}

	// Parse the embedded YAML file
	var dashConfig DashboardConfig
	if err := yamlutil.UnmarshalStrict(dashboardYAML, &dashConfig); err != nil {
		return fmt.Errorf("parsing dashboard config: %w", err)
	}

	fmt.Printf("Processing dashboard configuration for %s...\n", dashboardType)

	switch dashboardType {
	case Datadog:
		return generateDatadogDashboard(dashConfig, metricsNameMap, metricTypeMap)
	case Grafana:
		return fmt.Errorf("grafana dashboard generation not yet implemented")
	default:
		return fmt.Errorf("unsupported dashboard type: %s (must be 'datadog' or 'grafana')", dashboardType)
	}
}

func generateDatadogDashboard(
	dashConfig DashboardConfig, metricsNameMap map[string]string, metricTypeMap map[string]string,
) error {
	// Create template variables for filtering
	templateVars := []DatadogTemplateVariable{
		{Name: "region", Prefix: "region", Default: "*"},
		{Name: "cluster", Prefix: "cluster", Default: "*"},
		{Name: "host", Prefix: "host", Default: "*"},
		{Name: "node_id", Prefix: "node_id", Default: "*"},
		{Name: "store", Prefix: "store", Default: "*"},
	}

	// Create a single combined dashboard
	dashboard := DatadogDashboard{
		Title:             "CockroachDB Dashboard",
		Description:       "CockroachDB metrics dashboard combining all metric categories",
		TemplateVariables: templateVars,
		Widgets:           []DatadogWidget{},
		LayoutType:        "ordered",
	}

	// Add static header widget at the beginning
	headerWidget := createStaticHeaderWidget()
	dashboard.Widgets = append(dashboard.Widgets, headerWidget)

	// Process flat graphs (backward compatible)
	for _, graph := range dashConfig.Graphs {
		widget := convertGraphToDatadogWidget(graph, metricsNameMap, metricTypeMap)
		dashboard.Widgets = append(dashboard.Widgets, widget)
	}

	// Process dashboard sections (new feature)
	for _, dashboardSection := range dashConfig.Dashboards {
		dashboardWidget := convertDashboardToDatadogWidget(dashboardSection, metricsNameMap, metricTypeMap)
		dashboard.Widgets = append(dashboard.Widgets, dashboardWidget)
	}

	return writeDashboardToFile(dashboard, Datadog)
}

// writeDashboardToFile marshals the dashboard to JSON and writes it to a file
func writeDashboardToFile(dashboard DatadogDashboard, dashboardType string) error {
	// Marshal to JSON
	jsonData, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling dashboard: %w", err)
	}

	// Determine output path
	outputPath := outputFile
	if outputPath == "" {
		// Use default filename if no output specified
		outputPath = fmt.Sprintf("cockroachdb_%s_dashboard.json", dashboardType)
	}

	// Write to file
	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("writing dashboard file: %w", err)
	}

	fmt.Printf("Successfully generated %s dashboard: %s\n", dashboardType, outputPath)
	return nil
}

// convertDashboardToDatadogWidget converts a Dashboard section to a Datadog group widget
func convertDashboardToDatadogWidget(
	dashboardSection Dashboard, metricsNameMap map[string]string, metricTypeMap map[string]string,
) DatadogWidget {
	var dashboardWidgets []DatadogWidget

	// Convert each graph in the dashboard section to a widget
	for _, graph := range dashboardSection.Graphs {
		widget := convertGraphToDatadogWidget(graph, metricsNameMap, metricTypeMap)
		dashboardWidgets = append(dashboardWidgets, widget)
	}

	// Create dashboard group widget definition
	definition := DatadogWidgetDefinition{
		Type:       "group",
		Title:      dashboardSection.Title,
		ShowTitle:  true,
		LayoutType: "ordered",
		Widgets:    dashboardWidgets,
	}

	return DatadogWidget{
		Definition: definition,
	}
}

// convertGraphToDatadogWidget converts a GraphConfig to a Datadog widget
func convertGraphToDatadogWidget(
	graph GraphConfig, metricsNameMap map[string]string, metricTypeMap map[string]string,
) DatadogWidget {
	var queries []DatadogQuery
	var formulas []DatadogFormula

	// Build queries and formulas for each metric
	for i, metric := range graph.Metrics {
		queryName := fmt.Sprintf("query%d", i+1)

		// Build Datadog query
		query := DatadogQuery{
			DataSource: "metrics",
			Name:       queryName,
			Query:      buildDatadogQuery(metric.Name, metricsNameMap, metricTypeMap, graph.GroupByStorage),
		}
		queries = append(queries, query)

		// Create formula
		formulaStr := queryName
		if graph.Axis.Units == "Percentage" {
			formulaStr = queryName + " * 100"
		}
		formula := DatadogFormula{
			Alias:   metric.Title,
			Formula: formulaStr,
		}

		// Add number format based on units
		if numberFormat := getDatadogNumberFormat(graph.Axis.Units); numberFormat != nil {
			formula.NumberFormat = numberFormat
		}

		formulas = append(formulas, formula)
	}

	// Create request
	request := DatadogRequest{
		Formulas:       formulas,
		Queries:        queries,
		ResponseFormat: "timeseries",
		Style: &DatadogStyle{
			Palette:   "dog_classic",
			LineType:  "solid",
			LineWidth: "normal",
		},
		DisplayType: "line",
	}

	// Create widget definition
	definition := DatadogWidgetDefinition{
		Type:          "timeseries",
		Title:         graph.Title,
		ShowLegend:    true,
		LegendLayout:  "auto",
		LegendColumns: []string{"avg", "min", "max", "value", "sum"},
		Requests:      []DatadogRequest{request},
		YAxis: &DatadogYAxis{
			IncludeZero: true,
			Scale:       "linear",
			Min:         "0",
			Max:         "auto",
		},
	}

	return DatadogWidget{
		Definition: definition,
	}
}

// stripPercentileSuffix removes percentile suffixes like -p50, -p75, -p90, -p99, -p99.9, -p99.99, -p99.999 from a metric name.
func stripPercentileSuffix(name string) string {
	for _, suffix := range percentileSuffixes {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix)
		}
	}
	return name
}

// extractPercentileValue extracts the percentile value from a metric name with percentile suffix.
// For example: "sql.service.latency-p99" returns "99", "sql.service.latency-p99.9" returns "99.9"
func extractPercentileValue(metricName string) string {
	for _, suffix := range percentileSuffixes {
		if strings.HasSuffix(metricName, suffix) {
			// Extract the percentile value by removing the "-" prefix
			return strings.TrimPrefix(suffix, "-")
		}
	}
	return ""
}

// buildDatadogQuery constructs a Datadog query string from a metric name
func buildDatadogQuery(
	metricName string,
	metricsNameMap map[string]string,
	metricTypeMap map[string]string,
	groupByStorage bool,
) string {
	// Step 1: Extract and strip node/store prefix using reCrStoreNode regex
	// reCrStoreNode is defined in tsdump.go and matches "cr.(node|store).(.*)"
	sl := reCrStoreNode.FindStringSubmatch(metricName)
	var registry string
	if len(sl) >= 3 {
		registry = sl[1]   // "node" or "store"
		metricName = sl[2] // rest of the metric name
	}

	// Step 2: Strip percentile suffix and extract percentile value
	// Use stripPercentileSuffix() from gen.go
	baseMetricName := stripPercentileSuffix(metricName)
	percentileValue := extractPercentileValue(metricName)

	// Step 3: Convert to Prometheus format
	// prometheusNameReplaceRE is defined in tsdump_upload.go
	promName := prometheusNameReplaceRE.ReplaceAllString(baseMetricName, "_")

	// Step 4: Lookup Datadog name in metricsNameMap
	datadogMetricName := promName
	if ddName, ok := metricsNameMap[promName]; ok {
		datadogMetricName = ddName
	}

	// Ensure cockroachdb prefix
	if !strings.HasPrefix(datadogMetricName, "cockroachdb.") {
		datadogMetricName = "cockroachdb." + datadogMetricName
	}

	// Step 5: Look up metric type
	metricType := metricTypeMap[baseMetricName]

	// Step 6: Generate query based on metric type
	// Determine aggregation (default to sum)
	aggregation := "sum"
	if metricType == "HISTOGRAM" && percentileValue != "" {
		aggregation = percentileValue
	}

	// Build tags - use region, cluster, and host as base tags
	tags := "$region,$cluster,$host,$store,$node_id"
	var groupByClause string

	// Add node/store tags based on original metric prefix only if groupByStorage is true
	if groupByStorage {
		if registry == "node" {
			groupByClause = " by {node_id}"
		} else if registry == "store" {
			groupByClause = " by {store}"
		}
	}

	// Build base query
	query := fmt.Sprintf("%s:%s{%s}%s", aggregation, datadogMetricName, tags, groupByClause)

	// Apply rate() for COUNTER metrics
	if metricType == "COUNTER" {
		query += ".as_rate()"
	}

	query += fmt.Sprintf(".rollup(sum, %d)", rollupInterval)
	return query
}

// getDatadogNumberFormat returns number format configuration for a given unit
func getDatadogNumberFormat(units string) *DatadogNumberFormat {
	switch units {
	case "Duration":
		return &DatadogNumberFormat{
			Unit: &DatadogUnit{
				Type:     "canonical_unit",
				UnitName: "nanosecond",
			},
		}
	case "DurationSeconds":
		return &DatadogNumberFormat{
			Unit: &DatadogUnit{
				Type:     "canonical_unit",
				UnitName: "second",
			},
		}
	case "Bytes":
		return &DatadogNumberFormat{
			Unit: &DatadogUnit{
				Type:     "canonical_unit",
				UnitName: "byte_in_binary_bytes_family",
			},
		}
	case "Percentage":
		return &DatadogNumberFormat{
			Unit: &DatadogUnit{
				Type:     "canonical_unit",
				UnitName: "percent",
			},
		}
	case "Count":
		// For counts, return nil to use default formatting
		// Datadog doesn't require explicit unit configuration for counts
		return nil
	default:
		return nil
	}
}

// createStaticHeaderWidget creates the static header group widget containing
// the CockroachDB logo and description text
func createStaticHeaderWidget() DatadogWidget {
	// Create image widget for CockroachDB logo
	imageWidget := DatadogWidget{
		Definition: DatadogWidgetDefinition{
			Type:            "image",
			URL:             "/static/images/logos/cockroachdb_large.svg",
			URLDarkTheme:    "/static/images/logos/cockroachdb_reversed_large.svg",
			Sizing:          "cover",
			HasBackground:   true,
			HasBorder:       true,
			VerticalAlign:   "center",
			HorizontalAlign: "center",
		},
	}

	// Create description note widget
	descriptionWidget := DatadogWidget{
		Definition: DatadogWidgetDefinition{
			Type:            "note",
			Content:         "This dashboard provides a high-level view of your CockroachDB cluster, including:\n- A high-level view of SQL performance & latency.\n- Information about resource consumption to help aid in capacity planning.",
			BackgroundColor: "transparent",
			FontSize:        "14",
			TextAlign:       "left",
			VerticalAlign:   "center",
			ShowTick:        false,
			TickPos:         "100%",
			TickEdge:        "left",
			HasPadding:      false,
		},
	}

	// Create configuration instructions note widget
	configWidget := DatadogWidget{
		Definition: DatadogWidgetDefinition{
			Type: "note",
			Content: "#### Optimize your CockroachDB integration configuration for monitoring\n1. Configure `cluster` and `region` tags in your `cockroachdb.d/conf.yaml`" +
				" [file](https://github.com/DataDog/integrations-core/blob/7f091cbd40dfa13ec713d00d53caa2273a6cc7f6/cockroachdb/datadog_checks/cockroachdb/data/conf.yaml.example#L583-L591):\n\n " +
				"   ```\n    instances:\n" +
				"      - openmetrics_endpoint: http://localhost:8080/_status/vars\n        tags: [\"cluster:prod\", \"region:us-east-1\"]\n" +
				"      - openmetrics_endpoint: http://localhost:8082/_status/vars\n        tags: [\"cluster:dev\", \"region:us-east-2\"]\n    ```",
			BackgroundColor: "white",
			FontSize:        "14",
			TextAlign:       "left",
			VerticalAlign:   "center",
			ShowTick:        false,
			TickPos:         "100%",
			TickEdge:        "left",
			HasPadding:      true,
		},
	}

	// Create the group widget containing all three widgets
	groupWidget := DatadogWidget{
		Definition: DatadogWidgetDefinition{
			Type:       "group",
			Title:      "Description",
			BannerImg:  "",
			ShowTitle:  false,
			LayoutType: "ordered",
			Widgets:    []DatadogWidget{imageWidget, descriptionWidget, configWidget},
		},
	}

	return groupWidget
}
