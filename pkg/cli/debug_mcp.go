// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed debug_agent_instructions.md
var debugAgentInstructions string

//go:embed metrics.yaml
var metricsYAML string

// MetricMetadata represents a single metric's metadata
type MetricMetadata struct {
	Name         string `yaml:"name"`
	ExportedName string `yaml:"exported_name"`
	LabeledName  string `yaml:"labeled_name"`
	Description  string `yaml:"description"`
	YAxisLabel   string `yaml:"y_axis_label"`
	Type         string `yaml:"type"`
	Unit         string `yaml:"unit"`
	Aggregation  string `yaml:"aggregation"`
	Derivative   string `yaml:"derivative"`
	HowToUse     string `yaml:"how_to_use"`
	Essential    bool   `yaml:"essential"`
	Category     string `yaml:"-"` // Will be populated from parent
	Layer        string `yaml:"-"` // Will be populated from parent
}

// MetricsCategory represents a category of metrics
type MetricsCategory struct {
	Name    string           `yaml:"name"`
	Metrics []MetricMetadata `yaml:"metrics"`
}

// MetricsLayer represents a layer containing categories
type MetricsLayer struct {
	Name       string            `yaml:"name"`
	Categories []MetricsCategory `yaml:"categories"`
}

// MetricsYAML represents the top-level structure
type MetricsYAML struct {
	Layers []MetricsLayer `yaml:"layers"`
}

// parseMetrics parses the embedded metrics YAML and returns flattened metrics
func parseMetrics() ([]MetricMetadata, error) {
	var metricsData MetricsYAML
	if err := yaml.Unmarshal([]byte(metricsYAML), &metricsData); err != nil {
		return nil, fmt.Errorf("failed to parse metrics YAML: %w", err)
	}

	// Flatten the hierarchy and populate category/layer info
	var allMetrics []MetricMetadata
	for _, layer := range metricsData.Layers {
		for _, category := range layer.Categories {
			for _, metric := range category.Metrics {
				metric.Layer = layer.Name
				metric.Category = category.Name
				allMetrics = append(allMetrics, metric)
			}
		}
	}

	return allMetrics, nil
}

// searchMetrics searches for metrics matching keywords
func searchMetrics(metrics []MetricMetadata, keywords []string) []MetricMetadata {
	if len(keywords) == 0 {
		return metrics
	}

	var results []MetricMetadata
	for _, metric := range metrics {
		// Create searchable text from all fields
		searchText := strings.ToLower(strings.Join([]string{
			metric.Name,
			metric.ExportedName,
			metric.LabeledName,
			metric.Description,
			metric.Category,
			metric.Layer,
			metric.Type,
			metric.Unit,
			metric.HowToUse,
		}, " "))

		// Check if all keywords match
		allMatch := true
		for _, keyword := range keywords {
			if !strings.Contains(searchText, strings.ToLower(keyword)) {
				allMatch = false
				break
			}
		}

		if allMatch {
			results = append(results, metric)
		}
	}

	return results
}

// computeTimeSeriesStats computes statistics for a time series
func computeTimeSeriesStats(datapoints [][2]any) map[string]any {
	if len(datapoints) == 0 {
		return map[string]any{"count": 0}
	}

	// First pass: compute min, max, sum
	var min, max, sum float64
	var startTime, endTime int64
	values := make([]float64, len(datapoints))

	for i, dp := range datapoints {
		ts := dp[0].(int64)
		val := dp[1].(float64)
		values[i] = val

		if i == 0 {
			min = val
			max = val
			startTime = ts
			endTime = ts
		} else {
			if val < min {
				min = val
			}
			if val > max {
				max = val
			}
			if ts < startTime {
				startTime = ts
			}
			if ts > endTime {
				endTime = ts
			}
		}
		sum += val
	}

	count := len(datapoints)
	mean := sum / float64(count)

	// Second pass: compute standard deviation
	var variance float64
	for _, val := range values {
		diff := val - mean
		variance += diff * diff
	}
	variance /= float64(count)
	stdDev := math.Sqrt(variance)

	// Compute percentiles
	sortedValues := make([]float64, len(values))
	copy(sortedValues, values)
	sort.Float64s(sortedValues)

	percentile := func(p float64) float64 {
		index := int(math.Ceil(p*float64(count))) - 1
		if index < 0 {
			index = 0
		}
		if index >= count {
			index = count - 1
		}
		return sortedValues[index]
	}

	durationSeconds := float64(endTime-startTime) / 1e9

	return map[string]any{
		"count":            count,
		"min":              min,
		"max":              max,
		"mean":             mean,
		"std_dev":          stdDev,
		"p50":              percentile(0.50),
		"p90":              percentile(0.90),
		"p95":              percentile(0.95),
		"p99":              percentile(0.99),
		"start_time":       startTime,
		"end_time":         endTime,
		"duration_seconds": durationSeconds,
	}
}

// detectAnomalies detects various anomalies in time series data
func detectAnomalies(datapoints [][2]any, stats map[string]any) map[string]any {
	if len(datapoints) == 0 {
		return map[string]any{}
	}

	mean := stats["mean"].(float64)
	stdDev := stats["std_dev"].(float64)

	// Outlier detection (> 3 standard deviations from mean)
	var outlierCount int
	var outlierSamples [][2]any
	threshold := mean + 3*stdDev

	for _, dp := range datapoints {
		val := dp[1].(float64)
		if math.Abs(val-mean) > 3*stdDev {
			outlierCount++
			if len(outlierSamples) < 5 {
				outlierSamples = append(outlierSamples, dp)
			}
		}
	}

	// Spike detection (>50% change between consecutive points)
	var spikeCount int
	var spikeSamples [][3]any // [timestamp, prev_value, curr_value]

	for i := 1; i < len(datapoints); i++ {
		prevVal := datapoints[i-1][1].(float64)
		currVal := datapoints[i][1].(float64)
		ts := datapoints[i][0].(int64)

		if prevVal != 0 {
			change := math.Abs((currVal - prevVal) / prevVal)
			if change > 0.5 {
				spikeCount++
				if len(spikeSamples) < 5 {
					spikeSamples = append(spikeSamples, [3]any{ts, prevVal, currVal})
				}
			}
		}
	}

	// Flatline detection (very low variance)
	flatlineDetected := stdDev < (mean * 0.01) // Less than 1% coefficient of variation

	// Gap detection (time gaps > 2x median interval)
	var gaps []int64
	if len(datapoints) > 2 {
		intervals := make([]int64, len(datapoints)-1)
		for i := 1; i < len(datapoints); i++ {
			intervals[i-1] = datapoints[i][0].(int64) - datapoints[i-1][0].(int64)
		}

		sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
		medianInterval := intervals[len(intervals)/2]
		gapThreshold := medianInterval * 2

		var largestGap int64
		for _, interval := range intervals {
			if interval > gapThreshold {
				if interval > largestGap {
					largestGap = interval
				}
			}
		}

		if largestGap > 0 {
			gaps = []int64{largestGap}
		}
	}

	anomalies := map[string]any{
		"outliers": map[string]any{
			"count":         outlierCount,
			"threshold":     threshold,
			"sample_points": outlierSamples,
		},
		"spikes": map[string]any{
			"count":         spikeCount,
			"sample_spikes": spikeSamples,
		},
		"flatline": map[string]any{
			"detected": flatlineDetected,
		},
	}

	if len(gaps) > 0 {
		anomalies["gaps"] = map[string]any{
			"detected":            true,
			"largest_gap_seconds": float64(gaps[0]) / 1e9,
		}
	} else {
		anomalies["gaps"] = map[string]any{
			"detected": false,
		}
	}

	return anomalies
}

// toJSON converts a value to JSON string
func toJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf(`{"error": "failed to marshal JSON: %v"}`, err)
	}
	return string(data)
}

// SearchMetricsInput defines the input for the search-metrics tool
type SearchMetricsInput struct {
	Query string  `json:"query" jsonschema:"Search query with space-separated keywords (e.g. 'cpu usage' 'changefeed error' 'gc ttl')"`
	Limit float64 `json:"limit,omitempty" jsonschema:"Maximum number of results to return (default and max: 50)"`
}

// QueryTSDumpInput defines the input for the query-tsdump tool
type QueryTSDumpInput struct {
	File    string   `json:"file" jsonschema:"Path to the tsdump file to query"`
	Metric  string   `json:"metric,omitempty" jsonschema:"Single metric name (for backward compatibility)"`
	Metrics []string `json:"metrics,omitempty" jsonschema:"Array of metric names to retrieve in a single scan (recommended for multiple metrics)"`
}

// KapaConfig holds configuration for Kapa.AI integration
type KapaConfig struct {
	apiKey          string
	projectID       string
	currentThreadID string
}

// debugMCPState holds the server state
type debugMCPState struct {
	metrics    []MetricMetadata
	kapaConfig *KapaConfig
}

// searchMetricsTool handles the search-metrics tool
func (s *debugMCPState) searchMetricsTool(
	ctx context.Context, req *mcp.CallToolRequest, input SearchMetricsInput,
) (*mcp.CallToolResult, any, error) {
	if input.Query == "" {
		return nil, nil, fmt.Errorf("query parameter is required")
	}

	// Split query into keywords
	keywords := strings.Fields(input.Query)

	// Search for matching metrics
	results := searchMetrics(s.metrics, keywords)

	// Limit results to avoid overwhelming response
	maxResults := 50
	if input.Limit > 0 && int(input.Limit) < maxResults {
		maxResults = int(input.Limit)
	}

	if len(results) > maxResults {
		results = results[:maxResults]
	}

	// Format results
	formattedResults := make([]map[string]any, len(results))
	for i, m := range results {
		formattedResults[i] = map[string]any{
			"name":        m.Name,
			"category":    fmt.Sprintf("%s/%s", m.Layer, m.Category),
			"type":        m.Type,
			"unit":        m.Unit,
			"description": m.Description,
			"essential":   m.Essential,
		}
		// Include how_to_use only if non-empty
		if m.HowToUse != "" {
			formattedResults[i]["how_to_use"] = m.HowToUse
		}
	}

	result := map[string]any{
		"total_found": len(searchMetrics(s.metrics, keywords)),
		"returned":    len(results),
		"query":       input.Query,
		"keywords":    keywords,
		"metrics":     formattedResults,
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: toJSON(result)}},
	}, nil, nil
}

// queryTSDumpTool handles the query-tsdump tool
func (s *debugMCPState) queryTSDumpTool(
	ctx context.Context, req *mcp.CallToolRequest, input QueryTSDumpInput,
) (*mcp.CallToolResult, any, error) {
	if input.File == "" {
		return nil, nil, fmt.Errorf("file parameter is required")
	}

	// Support both single metric (string) and multiple metrics (array)
	var metricNames []string
	if len(input.Metrics) > 0 {
		metricNames = input.Metrics
	} else if input.Metric != "" {
		metricNames = []string{input.Metric}
	}

	if len(metricNames) == 0 {
		return nil, nil, fmt.Errorf("metric or metrics parameter is required")
	}

	var dec *gob.Decoder

	initDecoder := func() error {
		f, err := getFileReader(input.File)
		if err != nil {
			return err
		}
		dec = gob.NewDecoder(f)
		return nil
	}
	if err := initDecoder(); err != nil {
		return nil, nil, errors.Wrap(err, "failed to create decoder")
	}

	// Skip metadata if present
	_, metadataErr := tsdumpmeta.Read(dec)
	if metadataErr != nil {
		if err := initDecoder(); err != nil {
			return nil, nil, errors.Wrap(err, "failed to recreate decoder")
		}
	}

	// Track which metrics we need to find and which we've found
	metricsToFind := make(map[string]bool)
	for _, name := range metricNames {
		metricsToFind[name] = true
	}
	foundMetrics := make(map[string]any)

	// Scan sequentially through file to find all requested metrics
	for {
		var v roachpb.KeyValue
		err := dec.Decode(&v)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "decode error")
		}

		var data *tspb.TimeSeriesData
		dumper := ts.DefaultDumper{Send: func(d *tspb.TimeSeriesData) error {
			data = d
			return nil
		}}
		if err := dumper.Dump(&v); err != nil {
			continue
		}
		if data == nil {
			continue
		}

		// Check if this metric matches any of the requested metrics
		for metricName := range metricsToFind {
			if strings.HasSuffix(data.Name, metricName) {
				// Found a requested metric
				datapoints := make([][2]any, 0, len(data.Datapoints))
				for _, dp := range data.Datapoints {
					datapoints = append(datapoints, [2]any{dp.TimestampNanos, dp.Value})
				}

				// Compute statistics and detect anomalies
				stats := computeTimeSeriesStats(datapoints)
				anomalies := detectAnomalies(datapoints, stats)

				foundMetrics[data.Name] = map[string]any{
					"metric_name": data.Name,
					"source":      data.Source,
					"data_points": datapoints,
					"stats":       stats,
					"anomalies":   anomalies,
				}

				delete(metricsToFind, metricName)

				// If we've found all requested metrics, we can stop scanning
				if len(metricsToFind) == 0 {
					goto done
				}
			}
		}
	}

done:
	// Report results
	if len(foundMetrics) == 0 {
		return nil, nil, fmt.Errorf("none of the requested metrics found in file: %v", metricNames)
	}

	// If only one metric was requested, return it directly (backward compatibility)
	if len(metricNames) == 1 {
		for _, result := range foundMetrics {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: toJSON(result)}},
			}, nil, nil
		}
	}

	// Multiple metrics: return as a map
	notFound := make([]string, 0)
	for metricName := range metricsToFind {
		notFound = append(notFound, metricName)
	}

	result := map[string]any{
		"metrics":   foundMetrics,
		"requested": len(metricNames),
		"found":     len(foundMetrics),
	}
	if len(notFound) > 0 {
		result["not_found"] = notFound
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: toJSON(result)}},
	}, nil, nil
}

// SearchDocsInput defines the input for the search-docs tool
type SearchDocsInput struct {
	Query string `json:"query" jsonschema:"Question or query about CockroachDB"`
}

// searchDocsTool handles the search-docs tool
func (s *debugMCPState) searchDocsTool(
	ctx context.Context, req *mcp.CallToolRequest, input SearchDocsInput,
) (*mcp.CallToolResult, any, error) {
	if input.Query == "" {
		return nil, nil, fmt.Errorf("query parameter is required")
	}

	// Build request body
	reqBody := map[string]string{"query": input.Query}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Determine URL based on whether we have a thread ID
	var url string
	if s.kapaConfig.currentThreadID == "" {
		url = fmt.Sprintf("https://api.kapa.ai/query/v1/projects/%s/chat/", s.kapaConfig.projectID)
	} else {
		url = fmt.Sprintf("https://api.kapa.ai/query/v1/threads/%s/chat/", s.kapaConfig.currentThreadID)
	}

	// Create HTTP request
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqJSON))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-API-KEY", s.kapaConfig.apiKey)

	// Make the request
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Extract and store thread_id if present
	if threadID, ok := response["thread_id"].(string); ok && threadID != "" {
		s.kapaConfig.currentThreadID = threadID
	}

	// Extract only answer and is_certain fields
	result := map[string]any{}
	if answer, ok := response["answer"]; ok {
		result["answer"] = answer
	}
	if isCertain, ok := response["is_certain"]; ok {
		result["is_certain"] = isCertain
	}

	// Return only the filtered fields
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: toJSON(result)}},
	}, nil, nil
}

func runDebugMCP(cmd *cobra.Command, args []string) error {
	// Parse embedded metrics
	metrics, err := parseMetrics()
	if err != nil {
		return fmt.Errorf("failed to parse metrics: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Loaded %d metrics into memory\n", len(metrics))

	state := &debugMCPState{metrics: metrics}

	// Load instructions (default embedded, or from file if env var set)
	instructions := debugAgentInstructions
	if instructionsPath := os.Getenv("COCKROACH_DEBUG_MCP_INSTRUCTIONS"); instructionsPath != "" {
		customInstructions, err := os.ReadFile(instructionsPath)
		if err != nil {
			return fmt.Errorf("failed to read custom instructions from %s: %w", instructionsPath, err)
		}
		instructions = string(customInstructions)
		fmt.Fprintf(os.Stderr, "Loaded custom instructions from %s\n", instructionsPath)
	}

	// Create MCP server
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "cockroach-debug",
		Version: "1.0.0",
	}, &mcp.ServerOptions{
		Instructions: instructions,
	})

	// Register search-metrics tool
	mcp.AddTool(server, &mcp.Tool{
		Name:        "search-metrics",
		Description: "Search for CockroachDB metrics by keywords. Searches across metric names, descriptions, categories, and other metadata.",
	}, state.searchMetricsTool)

	// Register query-tsdump tool
	mcp.AddTool(server, &mcp.Tool{
		Name:        "query-tsdump",
		Description: "Query a tsdump file for one or more metrics by name. Efficiently scans the file once to retrieve all requested metrics.",
	}, state.queryTSDumpTool)

	// Conditionally register Kapa.AI tool if API key is provided
	kapaAPIKey, _ := cmd.Flags().GetString("kapa-api-key")
	if kapaAPIKey != "" {
		kapaProjectID, _ := cmd.Flags().GetString("kapa-project-id")
		state.kapaConfig = &KapaConfig{
			apiKey:    kapaAPIKey,
			projectID: kapaProjectID,
		}

		mcp.AddTool(server, &mcp.Tool{
			Name:        "search-docs",
			Description: "Search CockroachDB documentation and support knowledge base for conceptual understanding and configuration guidance. Maintains conversation context across multiple queries.",
		}, state.searchDocsTool)

		fmt.Fprintf(os.Stderr, "Kapa.AI integration enabled\n")
	}

	// Run the server with stdio transport
	return server.Run(context.Background(), &mcp.StdioTransport{})
}
