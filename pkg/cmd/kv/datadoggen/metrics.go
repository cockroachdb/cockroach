// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	metricsPrefix      string
	metricsSearch      string
	metricsYAMLFile    string
	metricsGoFiles     []string
	metricsOutputFile  string
	metricsQueriesOnly bool
	metricsQuiet       bool
	metricsNoGrouping  bool
)

const defaultYAMLPath = "docs/generated/metrics/metrics.yaml"
const defaultDatadogYAMLPathMetrics = "pkg/cli/files/cockroachdb_datadog_metrics.yaml"
const defaultBaseMetricsYAMLPathMetrics = "pkg/roachprod/agents/opentelemetry/files/cockroachdb_metrics_base.yaml"
const defaultFullMetricsYAMLPathMetrics = "pkg/roachprod/agents/opentelemetry/files/cockroachdb_metrics.yaml"

func newFromMetricsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "from-metrics",
		Short: "Generate Datadog queries from metrics.yaml or Go source files",
		Long: `Generate Datadog queries from CockroachDB metrics.yaml or Go source files.

Filter metrics by prefix or search for metrics containing specific text.
By default, output is grouped by metric prefix (part before first dot).

Output behavior:
  - Dashboard JSON: writes to {input}_datadog.json by default
  - Queries only (-q): writes to stdout for piping

Query formats follow Datadog best practices:
  - Counter: sum:metric{tags} by {node_id}.as_rate().rollup(max, 10)
  - Gauge: avg:metric{tags} by {node_id}.rollup(avg, 10)
  - Histogram: p99:metric{tags} by {node_id}.rollup(max, 10)

Examples:
  # Search for metrics (output: sql_service_datadog.json)
  datadoggen from-metrics --search "sql.service"

  # Filter by prefix (output: mma_rebalancing_datadog.json)
  datadoggen from-metrics --prefix mma,rebalancing

  # Generate from Go files (output: kvserver_metrics_rangefeed_metrics_datadog.json)
  datadoggen from-metrics -g pkg/kv/kvserver/metrics.go -g pkg/kv/kvserver/rangefeed/metrics.go

  # Get queries only (output: stdout for piping)
  datadoggen from-metrics -g pkg/kv/kvserver/metrics.go -q --quiet

  # Custom output file
  datadoggen from-metrics -g pkg/kv/kvserver/metrics.go -o my_dashboard.json

  # Disable grouping (flat list)
  datadoggen from-metrics --search "sql" --no-group

  # Generate for self-hosted tsdump format (crdb.tsdump.* prefix, $upload_id tag)
  datadoggen from-metrics --search "sql.service" --tsdump

  # Combine tsdump with Go file input
  datadoggen from-metrics -g pkg/kv/kvserver/metrics.go --tsdump -o my_tsdump_dashboard.json`,
		RunE: runFromMetrics,
	}

	cmd.Flags().StringVarP(&metricsPrefix, "prefix", "p", "", "Comma-separated metric prefixes to filter")
	cmd.Flags().StringVarP(&metricsSearch, "search", "s", "", "Comma-separated search terms (matches anywhere in metric name)")
	cmd.Flags().StringVar(&metricsYAMLFile, "yaml", "", fmt.Sprintf("Path to metrics.yaml (default: %s)", defaultYAMLPath))
	cmd.Flags().StringArrayVarP(&metricsGoFiles, "go-file", "g", nil, "Path to Go source file(s) containing metric definitions (can be repeated)")
	cmd.Flags().StringVarP(&metricsOutputFile, "output", "o", "", "Output file (default: auto-generated from input)")
	cmd.Flags().BoolVarP(&metricsQueriesOnly, "queries-only", "q", false, "Output only queries to stdout (for piping)")
	cmd.Flags().BoolVar(&metricsQuiet, "quiet", false, "Suppress info messages, output only results")
	cmd.Flags().BoolVar(&metricsNoGrouping, "no-group", false, "Disable grouping by prefix (flat list)")
	cmd.Flags().BoolVar(&TsdumpMode, "tsdump", false, "Generate queries for self-hosted tsdump format (crdb.tsdump.* prefix, $upload_id tag)")

	return cmd
}

// YAMLMetric represents a metric definition from metrics.yaml.
type YAMLMetric struct {
	Name        string
	Type        string
	Description string
	Unit        string
	YAxisLabel  string
	Aggregation string // AVG, SUM, etc. - hints at query aggregation type
}

func runFromMetrics(cmd *cobra.Command, args []string) error {
	// Check that at least one source is provided
	if len(metricsGoFiles) == 0 && metricsPrefix == "" && metricsSearch == "" {
		return fmt.Errorf("either --go-file, --prefix, or --search is required")
	}

	// Load Datadog-specific metric lookup for accurate name conversion (preferred)
	if err := LoadDatadogMetricLookup(defaultDatadogYAMLPathMetrics); err != nil {
		// Not fatal - we'll use the fallback
		if !metricsQuiet {
			fmt.Fprintf(os.Stderr, "Note: Could not load cockroachdb_datadog_metrics.yaml: %v\n", err)
		}
	}

	// Load base metrics lookup (legacy/runtime conditional metrics)
	if err := LoadBaseMetricLookup(defaultBaseMetricsYAMLPathMetrics); err != nil {
		// Not fatal - we'll try other fallbacks
		if !metricsQuiet {
			fmt.Fprintf(os.Stderr, "Note: Could not load cockroachdb_metrics_base.yaml: %v\n", err)
		}
	}

	// Load full metrics lookup (comprehensive auto-generated file)
	if err := LoadFullMetricLookup(defaultFullMetricsYAMLPathMetrics); err != nil {
		// Not fatal - we'll try other fallbacks
		if !metricsQuiet {
			fmt.Fprintf(os.Stderr, "Note: Could not load cockroachdb_metrics.yaml: %v\n", err)
		}
	}

	// Load metric name lookup as fallback
	yamlFile := metricsYAMLFile
	if yamlFile == "" {
		yamlFile = defaultYAMLPath
	}
	if err := LoadMetricNameLookup(yamlFile); err != nil {
		// Not fatal - we'll fall back to simple conversion
		if !metricsQuiet {
			fmt.Fprintf(os.Stderr, "Note: Could not load metrics.yaml for name lookup: %v\n", err)
		}
	}

	// Parse search terms and prefixes
	var searchTerms, prefixes []string
	if metricsSearch != "" {
		searchTerms = strings.Split(metricsSearch, ",")
		for i := range searchTerms {
			searchTerms[i] = strings.TrimSpace(searchTerms[i])
		}
	}
	if metricsPrefix != "" {
		prefixes = strings.Split(metricsPrefix, ",")
		for i := range prefixes {
			prefixes[i] = strings.TrimSpace(prefixes[i])
		}
	}

	// Determine output file - generate default if not specified
	outputToStdout := metricsOutputFile == "" && metricsQueriesOnly
	outputFile := metricsOutputFile
	if outputFile == "" && !metricsQueriesOnly {
		// Generate default output filename based on input
		outputFile = generateDefaultOutputFilename(metricsGoFiles, searchTerms, prefixes)
	}

	// Helper function for conditional logging
	logf := func(format string, args ...interface{}) {
		if !metricsQuiet {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}
	logln := func(args ...interface{}) {
		if !metricsQuiet {
			fmt.Fprintln(os.Stderr, args...)
		}
	}

	logln(strings.Repeat("=", 70))
	logln("METRICS TO DATADOG QUERY GENERATOR")
	logln(strings.Repeat("=", 70))

	var yamlMetrics map[string]YAMLMetric
	var err error

	// Load metrics from Go file(s) or YAML
	if len(metricsGoFiles) > 0 {
		logf("\nGo source files: %s\n", strings.Join(metricsGoFiles, ", "))
		logf("\n%s\n", strings.Repeat("-", 70))
		logln("PARSING GO SOURCE FILES")
		logln(strings.Repeat("-", 70))

		// Also load metrics.yaml to expand template metrics (e.g., %s placeholders)
		yamlFile := metricsYAMLFile
		if yamlFile == "" {
			yamlFile = defaultYAMLPath
		}
		allYamlMetrics, yamlErr := loadMetricsYAML(yamlFile)
		if yamlErr != nil {
			logf("  Note: Could not load metrics.yaml for template expansion: %v\n", yamlErr)
			allYamlMetrics = make(map[string]YAMLMetric)
		}

		yamlMetrics = make(map[string]YAMLMetric)
		for _, goFile := range metricsGoFiles {
			fileMetrics, loadErr := loadMetricsFromGoFile(goFile)
			if loadErr != nil {
				return fmt.Errorf("failed to parse Go file %s: %w", goFile, loadErr)
			}
			logf("  Parsed %d metrics from %s\n", len(fileMetrics), goFile)
			// Merge metrics, expanding template metrics using YAML
			for name, metric := range fileMetrics {
				if strings.Contains(name, "%") {
					// Template metric - find matching metrics in YAML
					expanded := expandTemplateMetric(name, metric, allYamlMetrics)
					for expName, expMetric := range expanded {
						yamlMetrics[expName] = expMetric
					}
				} else {
					// Check if YAML has this metric with additional info (like Aggregation, Unit)
					if yamlDef, ok := allYamlMetrics[name]; ok {
						// Inherit Aggregation from YAML if not set in Go
						if metric.Aggregation == "" && yamlDef.Aggregation != "" {
							metric.Aggregation = yamlDef.Aggregation
						}
						// Inherit Type from YAML if available (more accurate)
						if yamlDef.Type != "" {
							metric.Type = yamlDef.Type
						}
						// Inherit Unit from YAML if not set in Go (or Go extraction failed)
						if metric.Unit == "" && yamlDef.Unit != "" {
							metric.Unit = yamlDef.Unit
						}
					}
					yamlMetrics[name] = metric
				}
			}
		}
		logf("  Total: %d unique metrics from %d file(s)\n", len(yamlMetrics), len(metricsGoFiles))
	} else {
		// Use default YAML path if not specified
		yamlFile := metricsYAMLFile
		if yamlFile == "" {
			yamlFile = defaultYAMLPath
		}
		logf("\nMetrics YAML:    %s\n", yamlFile)
		logf("\n%s\n", strings.Repeat("-", 70))
		logln("LOADING METRICS FROM YAML")
		logln(strings.Repeat("-", 70))

		yamlMetrics, err = loadMetricsYAML(yamlFile)
		if err != nil {
			return fmt.Errorf("failed to load metrics YAML: %w", err)
		}
		logf("  Loaded %d total metrics from YAML\n", len(yamlMetrics))
	}

	if len(searchTerms) > 0 {
		logf("Search terms:    %s\n", strings.Join(searchTerms, ", "))
	}
	if len(prefixes) > 0 {
		logf("Metric prefixes: %s\n", strings.Join(prefixes, ", "))
	}
	if metricsQueriesOnly {
		logf("Output mode:     queries only\n")
	} else if outputToStdout {
		logf("Output:          stdout (dashboard JSON)\n")
	} else {
		logf("Output:          %s\n", metricsOutputFile)
	}

	// Filter metrics by search terms and/or prefix
	var metrics []MetricDef
	filterDesc := ""

	// If go-file is provided without search/prefix, include all metrics
	if len(metricsGoFiles) > 0 && len(searchTerms) == 0 && len(prefixes) == 0 {
		// Convert all YAML metrics to MetricDef
		var names []string
		for name := range yamlMetrics {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			yamlDef := yamlMetrics[name]
			var metricType MetricType
			switch strings.ToUpper(yamlDef.Type) {
			case "COUNTER":
				metricType = MetricTypeCounter
			case "HISTOGRAM":
				metricType = MetricTypeHistogram
			default:
				metricType = MetricTypeGauge
			}
			metrics = append(metrics, MetricDef{
				Name:        name,
				Help:        yamlDef.Description,
				Measurement: yamlDef.YAxisLabel,
				Unit:        yamlDef.Unit,
				Type:        metricType,
				Aggregation: yamlDef.Aggregation,
			})
		}
		filterDesc = fmt.Sprintf("all metrics from %d Go file(s)", len(metricsGoFiles))
	} else {
		if len(searchTerms) > 0 {
			metrics = filterMetricsBySearch(yamlMetrics, searchTerms)
			filterDesc = fmt.Sprintf("search term(s): %s", strings.Join(searchTerms, ", "))
		}
		if len(prefixes) > 0 {
			prefixMetrics := filterMetricsByPrefix(yamlMetrics, prefixes)
			if len(metrics) > 0 {
				// Merge with search results (union)
				metricSet := make(map[string]MetricDef)
				for _, m := range metrics {
					metricSet[m.Name] = m
				}
				for _, m := range prefixMetrics {
					metricSet[m.Name] = m
				}
				metrics = make([]MetricDef, 0, len(metricSet))
				for _, m := range metricSet {
					metrics = append(metrics, m)
				}
				sort.Slice(metrics, func(i, j int) bool {
					return metrics[i].Name < metrics[j].Name
				})
				filterDesc += fmt.Sprintf(" + prefix(es): %s", strings.Join(prefixes, ", "))
			} else {
				metrics = prefixMetrics
				filterDesc = fmt.Sprintf("prefix(es): %s", strings.Join(prefixes, ", "))
			}
		}
	}

	logf("  Found %d metrics matching %s\n", len(metrics), filterDesc)

	if len(metrics) == 0 {
		fmt.Fprintf(os.Stderr, "\nERROR: No metrics found matching %s\n", filterDesc)
		fmt.Fprintln(os.Stderr, "\nAvailable metric prefixes (first 20):")
		printAvailablePrefixesTo(os.Stderr, yamlMetrics, 20)
		return fmt.Errorf("no metrics found")
	}

	// Count by type
	var counterCount, gaugeCount, histogramCount int
	for _, m := range metrics {
		switch m.Type {
		case MetricTypeCounter:
			counterCount++
		case MetricTypeGauge:
			gaugeCount++
		case MetricTypeHistogram:
			histogramCount++
		}
	}

	logf("\n  Metric types:\n")
	logf("    - Counters: %d\n", counterCount)
	logf("    - Gauges: %d\n", gaugeCount)
	logf("    - Histograms: %d\n", histogramCount)

	// If queries-only mode, just output the queries and exit
	if metricsQueriesOnly {
		logf("\n%s\n", strings.Repeat("-", 70))
		logln("QUERIES")
		logln(strings.Repeat("-", 70))

		if !metricsNoGrouping {
			// Group metrics by prefix (part before first dot) - default behavior
			grouped := groupQueriesByPrefix(metrics)
			prefixList := sortedKeys(grouped)

			for _, prefix := range prefixList {
				fmt.Printf("\n# %s\n", prefix)
				for _, metric := range grouped[prefix] {
					fmt.Println(BuildQuery(metric))
				}
			}
		} else {
			// Flat list (no grouping)
			for _, metric := range metrics {
				fmt.Println(BuildQuery(metric))
			}
		}

		logf("\n✓ Output %d queries\n", len(metrics))
		logln(strings.Repeat("=", 70))
		return nil
	}

	// Print discovered metrics
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("DISCOVERED METRICS")
	logln(strings.Repeat("-", 70))

	displayLimit := 50
	for i, metric := range metrics {
		if i >= displayLimit {
			break
		}
		typeIndicator := "?"
		switch metric.Type {
		case MetricTypeCounter:
			typeIndicator = "C"
		case MetricTypeGauge:
			typeIndicator = "G"
		case MetricTypeHistogram:
			typeIndicator = "H"
		}
		logf("  [%s] %s\n", typeIndicator, metric.Name)
	}
	if len(metrics) > displayLimit {
		logf("  ... and %d more\n", len(metrics)-displayLimit)
	}

	// Generate dashboard title
	var titleParts []string
	titleParts = append(titleParts, searchTerms...)
	titleParts = append(titleParts, prefixes...)
	dashboardTitle := strings.ReplaceAll(strings.Join(titleParts, ", "), ".", " ")
	dashboardTitle = strings.ReplaceAll(dashboardTitle, "_", " ")
	dashboardTitle = cases.Title(language.English).String(dashboardTitle) + " Metrics"

	// Create dashboard
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("GENERATING DASHBOARD")
	logln(strings.Repeat("-", 70))

	dashboard := createDashboard(metrics, dashboardTitle, !metricsNoGrouping)

	// Count widgets
	totalWidgets := 0
	for _, w := range dashboard.Widgets {
		if w.Definition.Type == "group" {
			totalWidgets += len(w.Definition.Widgets)
		} else {
			totalWidgets++
		}
	}

	logf("  Dashboard title: %s\n", dashboardTitle)
	logf("  Groups: %d\n", len(dashboard.Widgets))
	logf("  Total widgets: %d\n", totalWidgets)

	// Print sample queries
	logf("\nSample queries generated:\n")
	for i, metric := range metrics {
		if i >= 5 {
			break
		}
		logf("  %s\n", BuildQuery(metric))
	}
	if len(metrics) > 5 {
		logf("  ... and %d more\n", len(metrics)-5)
	}

	// Write output
	logf("\n%s\n", strings.Repeat("-", 70))
	logln("WRITING OUTPUT")
	logln(strings.Repeat("-", 70))

	outputData, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	if outputToStdout {
		fmt.Println(string(outputData))
		logln("✓ Dashboard written to stdout")
	} else {
		if err := os.WriteFile(outputFile, outputData, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		logf("✓ Dashboard written to: %s\n", outputFile)
	}

	logln(strings.Repeat("=", 70))

	// Print summary of metrics not found in lookup
	PrintMissingMetricsSummary()

	return nil
}

// generateDefaultOutputFilename creates a default output filename based on input sources.
// Examples:
//   - Go files: metrics.go -> metrics_datadog.json
//   - Multiple Go files: metrics.go, rangefeed/metrics.go -> metrics_rangefeed_datadog.json
//   - Search terms: "sql,kv" -> sql_kv_datadog.json
//   - Prefixes: "mma,raft" -> mma_raft_datadog.json
func generateDefaultOutputFilename(goFiles, searchTerms, prefixes []string) string {
	var parts []string

	// Extract base names from Go files (without .go extension)
	for _, f := range goFiles {
		base := filepath.Base(f)
		// Remove .go extension
		name := strings.TrimSuffix(base, ".go")
		// Also include parent dir if it's not the base name
		dir := filepath.Base(filepath.Dir(f))
		if dir != "." && dir != name {
			name = dir + "_" + name
		}
		parts = append(parts, name)
	}

	// Add search terms
	for _, term := range searchTerms {
		// Clean up the term for filename
		clean := strings.ReplaceAll(term, ".", "_")
		clean = strings.ReplaceAll(clean, " ", "_")
		parts = append(parts, clean)
	}

	// Add prefixes
	for _, prefix := range prefixes {
		clean := strings.ReplaceAll(prefix, ".", "_")
		parts = append(parts, clean)
	}

	// If no parts, use "metrics" as default
	if len(parts) == 0 {
		return "metrics_datadog.json"
	}

	// Join parts and add suffix
	// Limit to first 3 parts to avoid very long filenames
	if len(parts) > 3 {
		parts = parts[:3]
	}

	return strings.Join(parts, "_") + "_datadog.json"
}

// normalizeForSearch normalizes a string for search comparison by:
// - converting to lowercase
// - treating hyphens and underscores as equivalent
func normalizeForSearch(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

// filterMetricsBySearch filters metrics by checking if any search term is contained in the metric name.
// Hyphens and underscores are treated as equivalent (e.g., "round_trip" matches "round-trip-latency").
func filterMetricsBySearch(yamlMetrics map[string]YAMLMetric, searchTerms []string) []MetricDef {
	var metrics []MetricDef

	// Get sorted keys for consistent ordering
	var names []string
	for name := range yamlMetrics {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		yamlDef := yamlMetrics[name]

		// Check if metric matches any search term (case-insensitive, hyphen/underscore equivalent)
		matches := false
		nameNormalized := normalizeForSearch(name)
		for _, term := range searchTerms {
			if strings.Contains(nameNormalized, normalizeForSearch(term)) {
				matches = true
				break
			}
		}

		if !matches {
			continue
		}

		// Convert YAML type to MetricType
		var metricType MetricType
		switch strings.ToUpper(yamlDef.Type) {
		case "COUNTER":
			metricType = MetricTypeCounter
		case "HISTOGRAM":
			metricType = MetricTypeHistogram
		default:
			metricType = MetricTypeGauge
		}

		metrics = append(metrics, MetricDef{
			Name:        name,
			Help:        yamlDef.Description,
			Measurement: yamlDef.YAxisLabel,
			Unit:        yamlDef.Unit,
			Type:        metricType,
			Aggregation: yamlDef.Aggregation,
		})
	}

	return metrics
}

func loadMetricsYAML(yamlPath string) (map[string]YAMLMetric, error) {
	file, err := os.Open(yamlPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	metrics := make(map[string]YAMLMetric)
	scanner := bufio.NewScanner(file)

	var currentMetric YAMLMetric
	var inMetric bool

	// Simple line-by-line parser for metrics.yaml
	// Format is typically:
	// - name: metric.name
	//   type: COUNTER|GAUGE|HISTOGRAM
	//   description: ...
	//   unit: ...
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "- name:") {
			// Save previous metric if exists
			if inMetric && currentMetric.Name != "" && currentMetric.Type != "" {
				metrics[currentMetric.Name] = currentMetric
			}

			// Start new metric
			currentMetric = YAMLMetric{}
			inMetric = true
			currentMetric.Name = strings.TrimSpace(strings.TrimPrefix(trimmed, "- name:"))
			continue
		}

		if !inMetric {
			continue
		}

		if match := regexp.MustCompile(`^\s*type:\s*(.+)$`).FindStringSubmatch(line); match != nil {
			currentMetric.Type = strings.TrimSpace(match[1])
		} else if match := regexp.MustCompile(`^\s*description:\s*['"]?(.+?)['"]?$`).FindStringSubmatch(line); match != nil {
			currentMetric.Description = strings.TrimSpace(match[1])
		} else if match := regexp.MustCompile(`^\s*unit:\s*(.+)$`).FindStringSubmatch(line); match != nil {
			currentMetric.Unit = strings.TrimSpace(match[1])
		} else if match := regexp.MustCompile(`^\s*y_axis_label:\s*(.+)$`).FindStringSubmatch(line); match != nil {
			currentMetric.YAxisLabel = strings.TrimSpace(match[1])
		} else if match := regexp.MustCompile(`^\s*aggregation:\s*(.+)$`).FindStringSubmatch(line); match != nil {
			currentMetric.Aggregation = strings.TrimSpace(match[1])
		}
	}

	// Don't forget the last metric
	if inMetric && currentMetric.Name != "" && currentMetric.Type != "" {
		metrics[currentMetric.Name] = currentMetric
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return metrics, nil
}

// expandTemplateMetric expands a template metric (containing %s, %d, etc.) by finding
// matching metrics in the YAML metrics map.
// For example, "kv.rangefeed.scheduler.%s.latency" would match:
//   - kv.rangefeed.scheduler.normal.latency
//   - kv.rangefeed.scheduler.system.latency
func expandTemplateMetric(
	templateName string, templateMetric YAMLMetric, yamlMetrics map[string]YAMLMetric,
) map[string]YAMLMetric {
	result := make(map[string]YAMLMetric)

	// Convert template to regex pattern: replace %s, %d, etc. with .+
	pattern := templateName
	pattern = strings.ReplaceAll(pattern, ".", "\\.")
	pattern = regexp.MustCompile(`%[sdvfx]`).ReplaceAllString(pattern, ".+")
	pattern = "^" + pattern + "$"

	re, err := regexp.Compile(pattern)
	if err != nil {
		// If pattern compilation fails, skip this template
		return result
	}

	// Find all matching metrics in YAML
	for name, yamlDef := range yamlMetrics {
		if re.MatchString(name) {
			// Use YAML definition but inherit type from Go template if YAML doesn't have it
			metric := yamlDef
			if metric.Type == "" && templateMetric.Type != "" {
				metric.Type = templateMetric.Type
			}
			result[name] = metric
		}
	}

	return result
}

// loadMetricsFromGoFile parses a Go source file for metric.Metadata definitions.
// It extracts the Name, Help, and determines the metric type based on context.
func loadMetricsFromGoFile(goFilePath string) (map[string]YAMLMetric, error) {
	data, err := os.ReadFile(goFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Go file: %w", err)
	}

	content := string(data)
	metrics := make(map[string]YAMLMetric)

	// Pattern to match metric.Metadata blocks
	// Matches: variableName = metric.Metadata{ ... }
	// Uses (?s) for DOTALL mode so . matches newlines
	metadataPattern := regexp.MustCompile(`(?s)(\w+)\s*=\s*metric\.Metadata\s*\{([^}]+)\}`)

	matches := metadataPattern.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}

		varName := match[1]
		blockContent := match[2]

		// Extract Name field
		nameMatch := regexp.MustCompile(`Name:\s*"([^"]+)"`).FindStringSubmatch(blockContent)
		if nameMatch == nil {
			continue
		}
		metricName := nameMatch[1]

		// Extract Help field (description)
		help := ""
		helpMatch := regexp.MustCompile(`Help:\s*"([^"]+)"`).FindStringSubmatch(blockContent)
		if helpMatch != nil {
			help = helpMatch[1]
		}

		// Extract Measurement field
		measurement := ""
		measurementMatch := regexp.MustCompile(`Measurement:\s*"([^"]+)"`).FindStringSubmatch(blockContent)
		if measurementMatch != nil {
			measurement = measurementMatch[1]
		}

		// Extract Unit field (e.g., metric.Unit_NANOSECONDS -> NANOSECONDS)
		unit := ""
		unitMatch := regexp.MustCompile(`Unit:\s*metric\.Unit_(\w+)`).FindStringSubmatch(blockContent)
		if unitMatch != nil {
			unit = unitMatch[1]
		}

		// Determine metric type based on variable name prefix or usage context
		metricType := determineMetricType(varName, blockContent, content)

		metrics[metricName] = YAMLMetric{
			Name:        metricName,
			Type:        metricType,
			Description: help,
			Unit:        unit,
			YAxisLabel:  measurement,
		}
	}

	return metrics, nil
}

// determineMetricType tries to infer the metric type from the variable name and context.
func determineMetricType(varName, blockContent, fullContent string) string {
	// Check for explicit type hints in variable name
	lowerVar := strings.ToLower(varName)
	if strings.Contains(lowerVar, "counter") || strings.Contains(lowerVar, "count") {
		return "COUNTER"
	}
	if strings.Contains(lowerVar, "histogram") || strings.Contains(lowerVar, "latency") || strings.Contains(lowerVar, "duration") {
		return "HISTOGRAM"
	}

	// Check unit field - NANOSECONDS often indicates histogram
	if strings.Contains(blockContent, "Unit_NANOSECONDS") {
		return "HISTOGRAM"
	}

	// Check how the metric is used in the file
	// Look for NewCounter, NewGauge, NewHistogram patterns
	usagePattern := regexp.MustCompile(fmt.Sprintf(`New(Counter|Gauge|Histogram)[^\(]*\([^)]*%s`, regexp.QuoteMeta(varName)))
	usageMatch := usagePattern.FindStringSubmatch(fullContent)
	if usageMatch != nil {
		return strings.ToUpper(usageMatch[1])
	}

	// Also check for patterns like Counter: metric.NewCounter(meta...)
	usagePattern2 := regexp.MustCompile(fmt.Sprintf(`(Counter|Gauge|Histogram):\s*metric\.New\w+\([^)]*%s`, regexp.QuoteMeta(varName)))
	usageMatch2 := usagePattern2.FindStringSubmatch(fullContent)
	if usageMatch2 != nil {
		return strings.ToUpper(usageMatch2[1])
	}

	// Default to GAUGE
	return "GAUGE"
}

func filterMetricsByPrefix(yamlMetrics map[string]YAMLMetric, prefixes []string) []MetricDef {
	var metrics []MetricDef

	// Get sorted keys for consistent ordering
	var names []string
	for name := range yamlMetrics {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		yamlDef := yamlMetrics[name]

		// Check if metric matches any prefix
		matches := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(name, prefix+".") || name == prefix {
				matches = true
				break
			}
		}

		if !matches {
			continue
		}

		// Convert YAML type to MetricType
		var metricType MetricType
		switch strings.ToUpper(yamlDef.Type) {
		case "COUNTER":
			metricType = MetricTypeCounter
		case "HISTOGRAM":
			metricType = MetricTypeHistogram
		default:
			metricType = MetricTypeGauge
		}

		metrics = append(metrics, MetricDef{
			Name:        name,
			Help:        yamlDef.Description,
			Measurement: yamlDef.YAxisLabel,
			Unit:        yamlDef.Unit,
			Type:        metricType,
			Aggregation: yamlDef.Aggregation,
		})
	}

	return metrics
}

func createDashboard(metrics []MetricDef, title string, groupByPrefix bool) *DatadogDashboard {
	dashboard := NewDashboard(title, fmt.Sprintf("Auto-generated dashboard for %s", title))

	if groupByPrefix {
		// Group metrics by top-level prefix (part before first dot)
		groups := groupQueriesByPrefix(metrics)

		for _, groupName := range sortedKeys(groups) {
			groupMetrics := groups[groupName]
			var groupWidgets []Widget

			for i, metric := range groupMetrics {
				widget := CreateTimeseriesWidget(metric, i)
				groupWidgets = append(groupWidgets, widget)
			}

			groupTitle := strings.ReplaceAll(groupName, ".", " ")
			groupTitle = strings.ReplaceAll(groupTitle, "_", " ")
			groupTitle = cases.Title(language.English).String(groupTitle)

			groupWidget := CreateGroupWidget(groupTitle, groupWidgets)
			dashboard.Widgets = append(dashboard.Widgets, groupWidget)
		}
	} else {
		// Flat list - all metrics as individual timeseries widgets
		for i, metric := range metrics {
			widget := CreateTimeseriesWidget(metric, i)
			dashboard.Widgets = append(dashboard.Widgets, widget)
		}
	}

	return dashboard
}

func groupMetricsByPrefix(metrics []MetricDef, depth int) map[string][]MetricDef {
	groups := make(map[string][]MetricDef)

	for _, metric := range metrics {
		parts := strings.Split(metric.Name, ".")
		var groupKey string
		if len(parts) >= depth {
			groupKey = strings.Join(parts[:depth], ".")
		} else {
			groupKey = parts[0]
		}

		groups[groupKey] = append(groups[groupKey], metric)
	}

	return groups
}

// groupQueriesByPrefix groups metrics by their prefix (part before the first dot).
func groupQueriesByPrefix(metrics []MetricDef) map[string][]MetricDef {
	groups := make(map[string][]MetricDef)

	for _, metric := range metrics {
		prefix := getMetricPrefix(metric.Name)
		groups[prefix] = append(groups[prefix], metric)
	}

	return groups
}

// getMetricPrefix extracts the prefix from a metric name (part before first dot).
func getMetricPrefix(name string) string {
	if idx := strings.Index(name, "."); idx != -1 {
		return name[:idx]
	}
	return name
}

func sortedKeys(m map[string][]MetricDef) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func printAvailablePrefixes(yamlMetrics map[string]YAMLMetric, limit int) {
	printAvailablePrefixesTo(os.Stdout, yamlMetrics, limit)
}

func printAvailablePrefixesTo(w *os.File, yamlMetrics map[string]YAMLMetric, limit int) {
	prefixes := make(map[string]bool)

	for name := range yamlMetrics {
		parts := strings.Split(name, ".")
		if len(parts) >= 2 {
			prefixes[parts[0]+"."+parts[1]] = true
		} else {
			prefixes[parts[0]] = true
		}
	}

	var sortedPrefixes []string
	for p := range prefixes {
		sortedPrefixes = append(sortedPrefixes, p)
	}
	sort.Strings(sortedPrefixes)

	for i, p := range sortedPrefixes {
		if i >= limit {
			break
		}
		fmt.Fprintf(w, "  - %s\n", p)
	}
}
