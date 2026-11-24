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

// GrafanaDashboard represents the top-level Grafana dashboard structure
type GrafanaDashboard struct {
	Inputs        []GrafanaInput     `json:"__inputs"`
	Elements      map[string]any     `json:"__elements"`
	Requires      []GrafanaRequire   `json:"__requires"`
	Annotations   GrafanaAnnotations `json:"annotations"`
	Editable      bool               `json:"editable"`
	Panels        []GrafanaPanel     `json:"panels"`
	Refresh       string             `json:"refresh"`
	SchemaVersion int                `json:"schemaVersion"`
	Tags          []string           `json:"tags"`
	Templating    GrafanaTemplating  `json:"templating"`
	Time          GrafanaTime        `json:"time"`
	Timepicker    map[string]any     `json:"timepicker"`
	Timezone      string             `json:"timezone"`
	Title         string             `json:"title"`
	UID           string             `json:"uid"`
	Version       int                `json:"version"`
	ID            *int               `json:"id"`
}

// GrafanaInput represents an input parameter for the dashboard
type GrafanaInput struct {
	Name       string `json:"name"`
	Label      string `json:"label"`
	Type       string `json:"type"`
	PluginID   string `json:"pluginId"`
	PluginName string `json:"pluginName"`
}

// GrafanaRequire represents a requirement for the dashboard
type GrafanaRequire struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

// GrafanaAnnotations represents annotations configuration
type GrafanaAnnotations struct {
	List []GrafanaAnnotation `json:"list"`
}

// GrafanaAnnotation represents a single annotation
type GrafanaAnnotation struct {
	BuiltIn    int               `json:"builtIn"`
	Datasource GrafanaDatasource `json:"datasource"`
	Enable     bool              `json:"enable"`
	Hide       bool              `json:"hide"`
	IconColor  string            `json:"iconColor"`
	Name       string            `json:"name"`
	Target     GrafanaTarget2    `json:"target"`
	Type       string            `json:"type"`
}

// GrafanaTarget2 is used for annotation targets
type GrafanaTarget2 struct {
	Limit    int      `json:"limit"`
	MatchAny bool     `json:"matchAny"`
	Tags     []string `json:"tags"`
	Type     string   `json:"type"`
}

// GrafanaTemplating represents dashboard templating configuration
type GrafanaTemplating struct {
	List []GrafanaTemplateVariable `json:"list"`
}

// GrafanaTemplateVariable represents a template variable
type GrafanaTemplateVariable struct {
	Current    any                `json:"current,omitempty"`
	Datasource *GrafanaDatasource `json:"datasource,omitempty"`
	Definition string             `json:"definition,omitempty"`
	IncludeAll bool               `json:"includeAll"`
	Label      string             `json:"label,omitempty"`
	Name       string             `json:"name"`
	Options    any                `json:"options,omitempty"`
	Query      any                `json:"query,omitempty"`
	Refresh    int                `json:"refresh"`
	Regex      string             `json:"regex,omitempty"`
	Sort       int                `json:"sort,omitempty"`
	Type       string             `json:"type"`
	Auto       *bool              `json:"auto,omitempty"`
	AutoCount  *int               `json:"auto_count,omitempty"`
	AutoMin    *string            `json:"auto_min,omitempty"`
}

// GrafanaTime represents time range configuration
type GrafanaTime struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// GrafanaPanel represents a panel in the dashboard
type GrafanaPanel struct {
	Collapsed     bool                 `json:"collapsed,omitempty"`
	GridPos       GrafanaGridPos       `json:"gridPos"`
	ID            int                  `json:"id"`
	Panels        []GrafanaPanel       `json:"panels,omitempty"`
	Title         string               `json:"title"`
	Type          string               `json:"type"`
	Datasource    *GrafanaDatasource   `json:"datasource,omitempty"`
	Description   string               `json:"description,omitempty"`
	FieldConfig   *GrafanaFieldConfig  `json:"fieldConfig,omitempty"`
	Options       *GrafanaPanelOptions `json:"options,omitempty"`
	PluginVersion string               `json:"pluginVersion,omitempty"`
	Targets       []GrafanaTarget      `json:"targets,omitempty"`
}

// GrafanaGridPos represents the position and size of a panel
type GrafanaGridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// GrafanaDatasource represents a datasource reference
type GrafanaDatasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

// GrafanaFieldConfig represents field configuration for a panel
type GrafanaFieldConfig struct {
	Defaults  GrafanaFieldDefaults `json:"defaults"`
	Overrides []any                `json:"overrides"`
}

// GrafanaFieldDefaults represents default field configuration
type GrafanaFieldDefaults struct {
	Color      GrafanaColor      `json:"color"`
	Custom     GrafanaCustom     `json:"custom"`
	Mappings   []any             `json:"mappings"`
	Min        *float64          `json:"min,omitempty"`
	Thresholds GrafanaThresholds `json:"thresholds"`
	Unit       string            `json:"unit"`
}

// GrafanaColor represents color configuration
type GrafanaColor struct {
	Mode string `json:"mode"`
}

// GrafanaAxis represents axis configuration
type GrafanaAxis struct {
	BorderShow   bool   `json:"axisBorderShow"`
	CenteredZero bool   `json:"axisCenteredZero"`
	ColorMode    string `json:"axisColorMode"`
	Label        string `json:"axisLabel"`
	Placement    string `json:"axisPlacement"`
}

// GrafanaCustom represents custom field configuration
type GrafanaCustom struct {
	Axis              GrafanaAxis              `json:",inline"`
	BarAlignment      int                      `json:"barAlignment"`
	BarWidthFactor    float64                  `json:"barWidthFactor"`
	DrawStyle         string                   `json:"drawStyle"`
	FillOpacity       int                      `json:"fillOpacity"`
	GradientMode      string                   `json:"gradientMode"`
	HideFrom          GrafanaHideFrom          `json:"hideFrom"`
	InsertNulls       bool                     `json:"insertNulls"`
	LineInterpolation string                   `json:"lineInterpolation"`
	LineWidth         int                      `json:"lineWidth"`
	PointSize         int                      `json:"pointSize"`
	ScaleDistribution GrafanaScaleDistribution `json:"scaleDistribution"`
	ShowPoints        string                   `json:"showPoints"`
	ShowValues        bool                     `json:"showValues"`
	SpanNulls         bool                     `json:"spanNulls"`
	Stacking          GrafanaStacking          `json:"stacking"`
	ThresholdsStyle   GrafanaThresholdsStyle   `json:"thresholdsStyle"`
}

// GrafanaHideFrom represents hide configuration
type GrafanaHideFrom struct {
	Legend  bool `json:"legend"`
	Tooltip bool `json:"tooltip"`
	Viz     bool `json:"viz"`
}

// GrafanaScaleDistribution represents scale distribution configuration
type GrafanaScaleDistribution struct {
	Type string `json:"type"`
}

// GrafanaStacking represents stacking configuration
type GrafanaStacking struct {
	Group string `json:"group"`
	Mode  string `json:"mode"`
}

// GrafanaThresholdsStyle represents thresholds style configuration
type GrafanaThresholdsStyle struct {
	Mode string `json:"mode"`
}

// GrafanaThresholds represents thresholds configuration
type GrafanaThresholds struct {
	Mode  string                 `json:"mode"`
	Steps []GrafanaThresholdStep `json:"steps"`
}

// GrafanaThresholdStep represents a single threshold step
type GrafanaThresholdStep struct {
	Color string   `json:"color"`
	Value *float64 `json:"value"`
}

// GrafanaPanelOptions represents panel options
type GrafanaPanelOptions struct {
	AlertThreshold bool           `json:"alertThreshold"`
	Legend         GrafanaLegend  `json:"legend"`
	Tooltip        GrafanaTooltip `json:"tooltip"`
}

// GrafanaLegend represents legend configuration
type GrafanaLegend struct {
	Calcs       []string `json:"calcs"`
	DisplayMode string   `json:"displayMode"`
	Placement   string   `json:"placement"`
	ShowLegend  bool     `json:"showLegend"`
}

// GrafanaTooltip represents tooltip configuration
type GrafanaTooltip struct {
	HideZeros bool   `json:"hideZeros"`
	Mode      string `json:"mode"`
	Sort      string `json:"sort"`
}

// GrafanaTarget represents a query target
type GrafanaTarget struct {
	Datasource     GrafanaDatasource `json:"datasource"`
	EditorMode     string            `json:"editorMode"`
	Exemplar       bool              `json:"exemplar"`
	Expr           string            `json:"expr"`
	Interval       string            `json:"interval"`
	LegendFormat   string            `json:"legendFormat"`
	Range          bool              `json:"range"`
	RefID          string            `json:"refId"`
	IntervalFactor *int              `json:"intervalFactor,omitempty"`
}

var rollupInterval int
var outputFile string
var dashboardTool string

var genDashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "generate dashboard from embedded dashboard configuration",
	Long: `Generate a dashboard JSON file from the embedded CockroachDB dashboard YAML configuration.

This command reads the embedded dashboard.yaml file and generates a dashboard JSON file
in the format specified by the --tool flag (datadog or grafana).

Examples:
  cockroach gen dashboard --tool=datadog
  cockroach gen dashboard --tool=grafana
  cockroach gen dashboard --tool=datadog --rollup-interval=30
  cockroach gen dashboard --tool=datadog --output=my-dashboard.json
  cockroach gen dashboard --tool=grafana --output=/path/to/dashboard.json --rollup-interval=60
`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(runGenDashboardCmd),
}

func init() {
	genDashboardCmd.Flags().StringVar(&dashboardTool, "tool", "", "dashboard tool to generate for (datadog or grafana)")
	genDashboardCmd.Flags().IntVar(&rollupInterval, "rollup-interval", 10, "interval in seconds for metric rollup (default: 10)")
	genDashboardCmd.Flags().StringVar(&outputFile, "output", "", "output file path for the generated dashboard")
}

func runGenDashboardCmd(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dashboardType := strings.ToLower(dashboardTool)

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

	switch dashboardType {
	case Datadog:
		return generateDatadogDashboard(dashConfig, metricsNameMap, metricTypeMap)
	case Grafana:
		return generateGrafanaDashboard(dashConfig, metricTypeMap)
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
func writeDashboardToFile(dashboard any, dashboardType string) error {
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

// buildPrometheusQuery constructs a Prometheus query string from a metric name
func buildPrometheusQuery(metricName string, metricTypeMap map[string]string) string {
	baseMetricName := stripPercentileSuffix(metricName)
	percentileValue := extractPercentileValue(metricName)

	promName := prometheusNameReplaceRE.ReplaceAllString(baseMetricName, "_")
	metricType := metricTypeMap[baseMetricName]

	labelMatchers := `job="cockroachdb",cluster=~"$cluster",node_id=~"$node|.*",store=~"$store|.*"`

	var query string
	if metricType == "HISTOGRAM" && percentileValue != "" {
		// For histogram with percentile, use histogram_quantile
		percentile := extractPrometheusPercentile(percentileValue)
		query = fmt.Sprintf("histogram_quantile(%s, rate(%s_bucket{%s}[$__rate_interval]))", percentile, promName, labelMatchers)
	} else if metricType == "COUNTER" {
		query = fmt.Sprintf("sum(rate(%s{%s}[$__rate_interval]))", promName, labelMatchers)
	} else {
		query = fmt.Sprintf("sum(%s{%s})", promName, labelMatchers)
	}
	return query
}

// extractPrometheusPercentile converts percentile value to Prometheus format
// e.g., "p99" -> "0.99", "p99.9" -> "0.999", "max" -> "1"
func extractPrometheusPercentile(percentileValue string) string {
	// Remove the 'p' prefix if present
	percentileValue = strings.TrimPrefix(percentileValue, "p")

	// Handle special case for "max" and "100"
	if percentileValue == "max" || percentileValue == "100" {
		return "1"
	}

	// Convert percentile to decimal by moving decimal point 2 places left
	// This avoids floating point division precision issues
	// e.g., "99" -> "0.99", "99.9" -> "0.999", "50" -> "0.50"
	percentileValue = strings.Replace(percentileValue, ".", "", 1)
	return "0." + percentileValue
}

// getGrafanaUnit returns Grafana unit string for a given unit type
func getGrafanaUnit(units string) string {
	switch units {
	case "Duration":
		return "ns"
	case "DurationSeconds":
		return "s"
	case "Bytes":
		return "bytes"
	case "Percentage":
		return "percentunit"
	default:
		return "short"
	}
}

// convertGraphToGrafanaPanel converts a GraphConfig to a Grafana panel
func convertGraphToGrafanaPanel(
	graph GraphConfig, metricTypeMap map[string]string, panelID int, yPos int,
) GrafanaPanel {
	var targets []GrafanaTarget
	datasource := GrafanaDatasource{
		Type: "prometheus",
		UID:  "${DS_PROMETHEUS}",
	}

	// Build targets for each metric
	for i, metric := range graph.Metrics {
		// Step 1: Extract and strip node/store prefix using reCrStoreNode regex
		registry := ""
		metricName := metric.Name
		sl := reCrStoreNode.FindStringSubmatch(metric.Name)
		if len(sl) >= 3 {
			registry = sl[1]   // "node" or "store"
			metricName = sl[2] // rest of the metric name
		}

		target := GrafanaTarget{
			Datasource:   datasource,
			EditorMode:   "code",
			Exemplar:     true,
			Expr:         buildPrometheusQuery(metricName, metricTypeMap),
			Interval:     "",
			LegendFormat: getLegend(metric.Title, registry, graph.GroupByStorage),
			Range:        true,
			RefID:        fmt.Sprintf("%d", i+1),
		}

		targets = append(targets, target)
	}

	// Determine grid position based on number of metrics and chart type
	gridPos := GrafanaGridPos{
		H: 8,
		W: 12,
		X: (panelID % 2) * 12, // Alternate between left (0) and right (12)
		Y: yPos,
	}

	// Create field configuration
	minVal := 0.0
	fieldConfig := &GrafanaFieldConfig{
		Defaults: GrafanaFieldDefaults{
			Color: GrafanaColor{
				Mode: "palette-classic",
			},
			Custom: GrafanaCustom{
				Axis: GrafanaAxis{
					BorderShow:   false,
					CenteredZero: false,
					ColorMode:    "text",
					Label:        graph.Axis.Label,
					Placement:    "auto",
				},
				BarAlignment:   0,
				BarWidthFactor: 0.6,
				DrawStyle:      "line",
				FillOpacity:    10,
				GradientMode:   "none",
				HideFrom: GrafanaHideFrom{
					Legend:  false,
					Tooltip: false,
					Viz:     false,
				},
				InsertNulls:       false,
				LineInterpolation: "linear",
				LineWidth:         1,
				PointSize:         5,
				ScaleDistribution: GrafanaScaleDistribution{
					Type: "linear",
				},
				ShowPoints: "never",
				ShowValues: false,
				SpanNulls:  false,
				Stacking: GrafanaStacking{
					Group: "A",
					Mode:  "none",
				},
				ThresholdsStyle: GrafanaThresholdsStyle{
					Mode: "off",
				},
			},
			Mappings: []any{},
			Min:      &minVal,
			Thresholds: GrafanaThresholds{
				Mode: "absolute",
				Steps: []GrafanaThresholdStep{
					{
						Color: "green",
						Value: nil,
					},
					{
						Color: "red",
						Value: func() *float64 { v := 80.0; return &v }(),
					},
				},
			},
			Unit: getGrafanaUnit(graph.Axis.Units),
		},
		Overrides: []any{},
	}

	// Create panel options
	options := &GrafanaPanelOptions{
		AlertThreshold: true,
		Legend: GrafanaLegend{
			Calcs:       []string{},
			DisplayMode: "list",
			Placement:   "bottom",
			ShowLegend:  true,
		},
		Tooltip: GrafanaTooltip{
			HideZeros: false,
			Mode:      "multi",
			Sort:      "none",
		},
	}

	return GrafanaPanel{
		Datasource:    &datasource,
		Description:   graph.Tooltip,
		FieldConfig:   fieldConfig,
		GridPos:       gridPos,
		ID:            panelID,
		Options:       options,
		PluginVersion: "12.3.1",
		Targets:       targets,
		Title:         graph.Title,
		Type:          "timeseries",
	}
}

// getLegend generates the legend format string for Grafana panels based on grouping preferences.
// When groupByStorage is false, returns just the metric title.
// When groupByStorage is true, appends a prefix and template variable to distinguish series:
//   - For node metrics: "title-n$node_id" (e.g., "Latency-n$node_id")
//   - For store metrics: "title-s$store" (e.g., "Latency-s$store")
//   - For metrics without registry: defaults to node format
func getLegend(title, registry string, groupByStorage bool) string {
	if !groupByStorage {
		return title
	}

	// Use "n" prefix for node metrics, "s" prefix for store metrics
	prefix := "n"
	if registry == "store" {
		prefix = "s"
	}
	// Return legend format with template variable for Grafana to substitute
	// e.g., "Latency-n$node_id" or "Throughput-s$store"
	return fmt.Sprintf("%s-%s$%s", title, prefix, registry)
}

// generateGrafanaDashboard generates a Grafana dashboard from the dashboard config
func generateGrafanaDashboard(dashConfig DashboardConfig, metricTypeMap map[string]string) error {
	// Create template variables
	datasource := &GrafanaDatasource{
		Type: "prometheus",
		UID:  "${DS_PROMETHEUS}",
	}

	templateVars := []GrafanaTemplateVariable{
		// Datasource variable
		{
			Name:       "DS_PROMETHEUS",
			Type:       "datasource",
			Label:      "datasource",
			Query:      "prometheus",
			Refresh:    1,
			IncludeAll: false,
		},
		// Cluster variable
		{
			Name:       "cluster",
			Type:       "query",
			Label:      "Cluster",
			Datasource: datasource,
			Definition: `sys_uptime{job="cockroachdb"}`,
			Query: map[string]any{
				"query": `sys_uptime{job="cockroachdb"}`,
				"refId": "Prometheus-cluster-Variable-Query",
			},
			Regex:      `/cluster="([^"]+)"/`,
			Refresh:    1,
			Sort:       1,
			IncludeAll: false,
		},
		// Node variable
		{
			Name:       "node",
			Type:       "query",
			Label:      "node",
			Datasource: datasource,
			Definition: `label_values(sys_uptime{job="cockroachdb", cluster="$cluster"},node_id)`,
			Query: map[string]any{
				"qryType": 1,
				"query":   `label_values(sys_uptime{job="cockroachdb", cluster="$cluster"},node_id)`,
				"refId":   "PrometheusVariableQueryEditor-VariableQuery",
			},
			Refresh:    1,
			Sort:       1,
			IncludeAll: true,
		},
		// Store variable
		{
			Name:       "store",
			Type:       "query",
			Label:      "store",
			Datasource: datasource,
			Definition: `label_values(replicas{job="cockroachdb", cluster="$cluster"},store)`,
			Query: map[string]any{
				"qryType": 1,
				"query":   `label_values(replicas{job="cockroachdb", cluster="$cluster"},store)`,
				"refId":   "PrometheusVariableQueryEditor-VariableQuery",
			},
			Refresh:    1,
			Sort:       1,
			IncludeAll: true,
		},
	}

	// Create the dashboard
	var panels []GrafanaPanel
	panelID := 1
	yPos := 0

	// Process dashboard sections
	for _, dashboardSection := range dashConfig.Dashboards {
		// Create a row panel for the section
		rowPanel := GrafanaPanel{
			Type:      "row",
			Title:     dashboardSection.Title,
			Collapsed: false,
			ID:        panelID,
			GridPos: GrafanaGridPos{
				H: 1,
				W: 24,
				X: 0,
				Y: yPos,
			},
			Panels: []GrafanaPanel{},
		}
		panels = append(panels, rowPanel)
		panelID++
		yPos++

		// Convert each graph to a panel
		for _, graph := range dashboardSection.Graphs {
			panel := convertGraphToGrafanaPanel(graph, metricTypeMap, panelID, yPos)
			panels = append(panels, panel)
			panelID++

			// Update yPos for next row
			if panelID%2 == 1 { // After adding a right panel
				yPos += 8
			}
		}

		// Ensure we start the next section on a new row
		if panelID%2 == 0 {
			yPos += 8
		}
	}

	// Create dashboard
	dashboard := GrafanaDashboard{
		Inputs: []GrafanaInput{
			{
				Name:       "DS_PROMETHEUS",
				Label:      "prometheus",
				Type:       "datasource",
				PluginID:   "prometheus",
				PluginName: "Prometheus",
			},
		},
		Elements: map[string]any{},
		Requires: []GrafanaRequire{
			{
				Type: "panel",
				ID:   "timeseries",
				Name: "Time series",
			},
		},
		Annotations: GrafanaAnnotations{
			List: []GrafanaAnnotation{
				{
					BuiltIn: 1,
					Datasource: GrafanaDatasource{
						Type: "datasource",
						UID:  "grafana",
					},
					Enable:    true,
					Hide:      true,
					IconColor: "rgba(0, 211, 255, 1)",
					Name:      "Annotations & Alerts",
					Target: GrafanaTarget2{
						Limit:    100,
						MatchAny: false,
						Tags:     []string{},
						Type:     "dashboard",
					},
					Type: "dashboard",
				},
			},
		},
		Editable:      true,
		Panels:        panels,
		Refresh:       "30s",
		SchemaVersion: 42,
		Tags:          []string{},
		Templating: GrafanaTemplating{
			List: templateVars,
		},
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Timepicker: map[string]any{},
		Title:      "CockroachDB Dashboard",
		UID:        "crdb-console",
		Version:    12,
		ID:         nil,
	}

	return writeDashboardToFile(dashboard, Grafana)
}
