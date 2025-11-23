// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// gen-cockroachdb-metrics generates the cockroachdb_metrics.go file for roachprod's OpenTelemetry
// integration, enabling proper metric forwarding to Datadog from CockroachDB clusters.
//
// This generator creates a comprehensive mapping of CockroachDB metric names to Datadog-compatible
// metric names by merging metrics from three distinct sources:
//
// 1. Live metrics from test server
//   - The primary source containing all metrics emitted by the current CRDB version
//   - Collected by starting a test server and querying the ChartCatalog RPC endpoint
//   - Represents the complete set of metrics available in the codebase
//
// 2. cockroachdb_metrics_base.yaml - runtime_conditional_metrics section
//   - Contains metrics that are only emitted when specific runtime conditions are met
//   - These metrics are processed through the same mapping logic as server metrics
//
// 3. cockroachdb_metrics_base.yaml - legacy_metrics section
//   - Contains legacy/stale metrics no longer emitted by the latest CRDB version
//   - Preserved for backward compatibility with older roachprod clusters running older CRDB versions
//   - These mappings are used as-is
//   - Ensures consistent metric names across different CRDB versions
//
// The generator performs the following steps:
//
//	a) Starts a test server and collects all available metrics via RPC
//	b) Loads Datadog metric mapping rules from a local file (downloaded via Bazel)
//	c) Reads runtime_conditional_metrics and legacy_metrics from base YAML file
//	d) Processes server metrics and runtime_conditional_metrics, converting them to Prometheus format
//	   and applying Datadog naming conventions (including histogram variants like _bucket, _count, _sum)
//	e) Merges the processed mappings with legacy_metrics
//	f) Generates cockroachdb_metrics.go containing the final consolidated mapping

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// MetricInfo represents a metric from the metrics.yaml file
type MetricInfo struct {
	Name         string `yaml:"name"`
	ExportedName string `yaml:"exported_name"`
	LabeledName  string `yaml:"labeled_name,omitempty"`
	Description  string `yaml:"description"`
	YAxisLabel   string `yaml:"y_axis_label"`
	Type         string `yaml:"type"`
	Unit         string `yaml:"unit"`
	Aggregation  string `yaml:"aggregation"`
	Derivative   string `yaml:"derivative"`
	HowToUse     string `yaml:"how_to_use,omitempty"`
	Essential    bool   `yaml:"essential,omitempty"`
}

// Category represents a category of metrics
type Category struct {
	Name    string       `yaml:"name"`
	Metrics []MetricInfo `yaml:"metrics"`
}

// Layer represents a layer containing categories
type Layer struct {
	Name       string     `yaml:"name"`
	Categories []Category `yaml:"categories"`
}

// YAMLOutput represents the structure of metrics.yaml
type YAMLOutput struct {
	Layers []Layer `yaml:"layers"`
}

// BaseMappingsYAML represents the structure of cockroachdb_metrics_base.yaml
// This file contains two types of metrics not in the standard metrics.yaml:
//   - RuntimeConditionalMetrics: Metrics emitted only under specific runtime conditions
//     (e.g., cert auth, license installed). Processed through Datadog mapping rules.
//   - LegacyMetrics: Deprecated metrics from older CRDB versions, preserved for backward
//     compatibility.
type BaseMappingsYAML struct {
	RuntimeConditionalMetrics []MetricInfo      `yaml:"runtime_conditional_metrics"`
	LegacyMetrics             map[string]string `yaml:"legacy_metrics"`
}

var (
	// Skip patterns - metrics that we haven't included for historical reasons
	skipPatterns = []*regexp.Regexp{
		regexp.MustCompile(`^auth_`),
		regexp.MustCompile(`^distsender_rpc_err_errordetailtype_`),
		regexp.MustCompile(`^gossip_callbacks_`),
		regexp.MustCompile(`^jobs_auto_config_env_runner_`),
		regexp.MustCompile(`^jobs_update_table_`),
		regexp.MustCompile(`^logical_replication_`),
		regexp.MustCompile(`^sql_crud_`),
		regexp.MustCompile(`^storage_l\d_`),
		regexp.MustCompile(`^storage_sstable_compression_`),
	}

	// Regex to convert CRDB metric names to Prometheus format
	prometheusNameRegex = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

// Go code template for generating the cockroachdb_metrics.go file
const goTemplate = `// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// DO NOT EDIT THIS FILE MANUALLY
//
// This file is auto-generated when running "./dev generate".
// Manual edits will be overwritten.
//
// Generator: build/tools/gen-cockroachdb-metrics
//
// This file is generated by merging metrics from three sources:
//   1. Live metrics from test server
//      - Collected by starting a test server and querying metrics via RPC
//      - Processed through Datadog mapping rules
//   2. runtime_conditional_metrics from cockroachdb_metrics_base.yaml
//      - Conditionally emitted metrics not available from test server
//      - Processed through Datadog mapping rules
//   3. legacy_metrics from cockroachdb_metrics_base.yaml
//      - Legacy metrics from older CRDB versions for backward compatibility
//      - Used as-is

package metrics_config

// CockroachdbMetrics is a mapping of CockroachDB metric names to cockroachdb
// Datadog integration metric names. This allows CockroachDB metrics to comply
// with the naming requirements for the cockroachdb Datadog integration listed
// in its metadata.csv.
// - https://github.com/DataDog/integrations-core/blob/master/cockroachdb/metadata.csv

var CockroachdbMetrics = map[string]string{
{{- range $key, $value := .Metrics }}
    "{{ $key }}": "{{ $value }}",
{{- end }}
}
`

// loadDatadogMappings loads the Datadog metrics mapping from a local file
func loadDatadogMappings(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	return parseDatadogMappings(file)
}

// parseDatadogMappings parses the Python file to extract dictionaries
func parseDatadogMappings(r io.Reader) (map[string]string, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading content: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	metrics := make(map[string]string)

	// CRDB-Datadog mappings are stored as python dictionaries in the following file:
	// https://github.com/DataDog/integrations-core/blob/master/cockroachdb/datadog_checks/cockroachdb/metrics.py
	//   - METRIC_MAP: represents the raw CRDB-Datadog metric name mapping.
	//   - OMV2_METRIC_MAP: represents the metric in OpenMetrics V2 format.
	// E.g.
	// 'admission_errored_sql_kv_response': 'admission.errored.sql_kv.response'
	// here the key is the CRDB metric name in prometheus format, and the value is the corresponding metric name visible in Datadog.
	// Both maps are mutually exclusive. Parse both dictionaries to get the complete mapping.

	for _, mapName := range []string{"METRIC_MAP", "OMV2_METRIC_MAP"} {
		mapMetrics, err := parsePythonDict(lines, mapName)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", mapName, err)
		}
		for k, v := range mapMetrics {
			metrics[k] = v
		}
	}

	return metrics, nil
}

// parsePythonDict parses a specific Python dictionary (METRIC_MAP or OMV2_METRIC_MAP)
func parsePythonDict(lines []string, dictName string) (map[string]string, error) {
	metrics := make(map[string]string)
	inDict := false
	braceDepth := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if strings.Contains(trimmed, dictName+" = {") {
			inDict = true
			braceDepth = 1
			continue
		}

		if !inDict {
			continue
		}

		braceDepth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
		if braceDepth <= 0 {
			break
		}

		// Parse metric mapping line: 'key': 'value',
		if strings.Contains(trimmed, ":") && strings.Contains(trimmed, "'") {
			parts := strings.SplitN(trimmed, ":", 2)
			if len(parts) == 2 {
				key := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
				value := strings.Trim(strings.TrimSpace(parts[1]), "',\"")
				if key != "" && value != "" {
					metrics[strings.ToLower(key)] = value
				}
			}
		}
	}

	return metrics, nil
}

// loadBaseMappings reads the base mappings from a YAML file
func loadBaseMappings(path string) (*BaseMappingsYAML, error) {
	if path == "" {
		return &BaseMappingsYAML{
			RuntimeConditionalMetrics: []MetricInfo{},
			LegacyMetrics:             make(map[string]string),
		}, nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &BaseMappingsYAML{
				RuntimeConditionalMetrics: []MetricInfo{},
				LegacyMetrics:             make(map[string]string),
			}, nil
		}
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var mappings BaseMappingsYAML
	if err := yaml.Unmarshal(content, &mappings); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	return &mappings, nil
}

// collectMetricsFromServer starts a test server and retrieves all metrics via RPC
func collectMetricsFromServer(ctx context.Context) ([]MetricInfo, error) {
	sArgs := base.TestServerArgs{
		Insecure:          true,
		DefaultTestTenant: base.ExternalTestTenantAlwaysEnabled,
	}
	s, err := server.TestServerFactory.New(sArgs)
	if err != nil {
		return nil, err
	}
	srv := s.(serverutils.TestServerInterfaceRaw)
	defer srv.Stopper().Stop(ctx)

	// We want to only return after the server is ready.
	readyCh := make(chan struct{})
	srv.SetReadyFn(func(bool) {
		close(readyCh)
	})

	// Start the server.
	if err := srv.Start(ctx); err != nil {
		return nil, err
	}

	// Wait until the server is ready to action.
	select {
	case <-readyCh:
	case <-time.After(5 * time.Second):
		return nil, errors.AssertionFailedf("could not initialize server in time")
	}

	var sections []catalog.ChartSection

	// Retrieve the chart catalog (metric list) for the system tenant over RPC.
	retrieve := func(layer serverutils.ApplicationLayerInterface, predicate func(catalog.MetricLayer) bool) error {
		conn, err := layer.RPCClientConnE(username.RootUserName())
		if err != nil {
			return err
		}
		client := conn.NewAdminClient()

		resp, err := client.ChartCatalog(ctx, &serverpb.ChartCatalogRequest{})
		if err != nil {
			return err
		}
		for _, section := range resp.Catalog {
			if !predicate(section.MetricLayer) {
				continue
			}
			sections = append(sections, section)
		}
		return nil
	}

	// Retrieve metrics from both system and application layers
	if err := retrieve(srv, func(layer catalog.MetricLayer) bool {
		return layer != catalog.MetricLayer_APPLICATION
	}); err != nil {
		return nil, err
	}
	if err := retrieve(srv.TestTenant(), func(layer catalog.MetricLayer) bool {
		return layer == catalog.MetricLayer_APPLICATION
	}); err != nil {
		return nil, err
	}

	// Sort by layer then category name.
	sort.Slice(sections, func(i, j int) bool {
		return sections[i].MetricLayer < sections[j].MetricLayer ||
			(sections[i].MetricLayer == sections[j].MetricLayer &&
				sections[i].Title < sections[j].Title)
	})

	// Convert catalog sections to MetricInfo
	var allMetrics []MetricInfo
	for _, section := range sections {
		for _, chart := range section.Charts {
			// There are many charts, but only 1 metric per chart.
			metric := MetricInfo{
				Name:         chart.Metrics[0].Name,
				ExportedName: chart.Metrics[0].ExportedName,
				LabeledName:  chart.Metrics[0].LabeledName,
				Description:  strings.TrimSpace(chart.Metrics[0].Help),
				YAxisLabel:   chart.AxisLabel,
				Type:         chart.Metrics[0].MetricType.String(),
				Unit:         chart.Units.String(),
				Aggregation:  chart.Aggregator.String(),
				Derivative:   chart.Derivative.String(),
				HowToUse:     strings.TrimSpace(chart.Metrics[0].HowToUse),
				Essential:    chart.Metrics[0].Essential,
			}
			allMetrics = append(allMetrics, metric)
		}
	}

	return allMetrics, nil
}

// shouldSkipMetric checks if a metric name matches any skip patterns
func shouldSkipMetric(promName string) bool {
	for _, pattern := range skipPatterns {
		if pattern.MatchString(promName) {
			return true
		}
	}
	return false
}

// mapMetricsToDatadog processes the CRDB metrics and maps them to Datadog names
func mapMetricsToDatadog(metrics []MetricInfo, datadogMappings map[string]string) map[string]string {
	result := make(map[string]string)

	for _, metric := range metrics {
		// Convert to prometheus format (replace non-alphanumeric with underscore)
		promName := prometheusNameRegex.ReplaceAllString(metric.Name, "_")

		if shouldSkipMetric(promName) {
			continue
		}

		// Get Datadog name from mapping, default to normalized CRDB name
		datadogName := metric.Name
		if ddName, exists := datadogMappings[strings.ToLower(promName)]; exists {
			datadogName = ddName
		} else {
			// Normalize metric name for Datadog by replacing hyphens with underscores
			// Datadog metric names should not contain hyphens
			datadogName = strings.ReplaceAll(metric.Name, "-", "_")
		}

		result[promName] = datadogName

		// Add histogram variants if applicable
		if metric.Type == "HISTOGRAM" {
			result[promName+"_bucket"] = datadogName + ".bucket"
			result[promName+"_count"] = datadogName + ".count"
			result[promName+"_sum"] = datadogName + ".sum"
		}
	}

	return result
}

// mergeLegacyMappings combines legacy mappings with new mappings, giving priority to new mappings
func mergeLegacyMappings(legacyMappings, newMappings map[string]string) map[string]string {
	result := make(map[string]string)

	for k, v := range legacyMappings {
		result[k] = v
	}

	for k, v := range newMappings {
		result[k] = v
	}

	return result
}

// writeGoFile generates the cockroachdb_metrics.go file
func writeGoFile(outputPath string, metrics map[string]string) error {
	tmpl, err := template.New("cockroachdb_metrics").Parse(goTemplate)
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(metrics))
	for k := range metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sortedMetrics := make(map[string]string)
	for _, k := range keys {
		sortedMetrics[k] = metrics[k]
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	data := struct {
		Metrics map[string]string
	}{
		Metrics: sortedMetrics,
	}

	if err := tmpl.Execute(file, data); err != nil {
		return fmt.Errorf("executing template: %w", err)
	}

	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <output.go> <datadog-mappings.py> [base.yaml]\n", os.Args[0])
		os.Exit(1)
	}

	outputPath := os.Args[1]
	datadogMappingsPath := os.Args[2]

	// Optional third argument: base YAML file with extra and legacy metrics
	basePath := ""
	if len(os.Args) >= 4 {
		basePath = os.Args[3]
	}

	ctx := context.Background()

	fmt.Println("Starting test server to collect metrics...")
	serverMetrics, err := collectMetricsFromServer(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR collecting metrics from server:", err)
		os.Exit(1)
	}
	fmt.Printf("Collected %d metrics from server\n", len(serverMetrics))

	datadogMappings, err := loadDatadogMappings(datadogMappingsPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}

	baseMetrics, err := loadBaseMappings(basePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}

	// Combine server metrics with runtime conditional metrics from base file
	allMetrics := append(serverMetrics, baseMetrics.RuntimeConditionalMetrics...)
	newMappings := mapMetricsToDatadog(allMetrics, datadogMappings)
	finalMappings := mergeLegacyMappings(baseMetrics.LegacyMetrics, newMappings)

	if err := writeGoFile(outputPath, finalMappings); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}

	// Only print summary if not in exec/tool configuration to avoid duplicate logs.
	// Bazel builds the tool in both target and exec configurations, causing duplicate output.
	if !strings.Contains(outputPath, "-exec-") && !strings.Contains(outputPath, "for tool") {
		fmt.Printf("Generated %d metric mappings:\n", len(finalMappings))
		fmt.Printf("  - %d from server\n", len(serverMetrics))
		fmt.Printf("  - %d runtime conditional metrics\n", len(baseMetrics.RuntimeConditionalMetrics))
		fmt.Printf("  - %d legacy mappings\n", len(baseMetrics.LegacyMetrics))
		fmt.Printf("Output written to: %s\n", outputPath)
	}
}
