// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/errors"
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

// MCPMessage represents a JSON-RPC 2.0 message
type MCPMessage struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      *int      `json:"id,omitempty"`
	Method  string    `json:"method,omitempty"`
	Params  any       `json:"params,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *MCPError `json:"error,omitempty"`
}

// MCPError represents a JSON-RPC 2.0 error
type MCPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// MCPServer is a minimal MCP server implementation
type MCPServer struct {
	name         string
	version      string
	instructions string
	tools        map[string]MCPTool
	metrics      []MetricMetadata
}

// MCPTool represents an MCP tool
type MCPTool struct {
	Name        string                                      `json:"name"`
	Description string                                      `json:"description"`
	InputSchema map[string]any                              `json:"inputSchema"`
	Handler     func(params map[string]any) (string, error) `json:"-"`
}

// NewMCPServer creates a new minimal MCP server
func NewMCPServer(name, version string) *MCPServer {
	return &MCPServer{
		name:         name,
		version:      version,
		instructions: debugAgentInstructions,
		tools:        make(map[string]MCPTool),
	}
}

// AddTool adds a tool to the server
func (s *MCPServer) AddTool(tool MCPTool) {
	s.tools[tool.Name] = tool
}

// Run starts the MCP server with stdio transport
func (s *MCPServer) Run(ctx context.Context) error {
	fmt.Fprintf(os.Stderr, "Starting MCP server: %s v%s\n", s.name, s.version)
	fmt.Fprintf(os.Stderr, "Available tools: %v\n", s.getToolNames())

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		response := s.handleMessage(line)
		if response != nil {
			if err := s.writeMessage(response); err != nil {
				return fmt.Errorf("failed to write response: %w", err)
			}
		}
	}

	return scanner.Err()
}

func (s *MCPServer) getToolNames() []string {
	names := make([]string, 0, len(s.tools))
	for name := range s.tools {
		names = append(names, name)
	}
	return names
}

func (s *MCPServer) handleMessage(line string) *MCPMessage {
	var msg MCPMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		errMsg := fmt.Sprintf("Parse error: %v", err)
		return &MCPMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   &MCPError{Code: -32700, Message: errMsg},
		}
	}

	switch msg.Method {
	case "initialize":
		return s.handleInitialize(&msg)
	case "tools/list":
		return s.handleToolsList(&msg)
	case "tools/call":
		return s.handleToolsCall(&msg)
	default:
		return &MCPMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   &MCPError{Code: -32601, Message: "Method not found"},
		}
	}
}

func (s *MCPServer) handleInitialize(msg *MCPMessage) *MCPMessage {
	serverInfo := map[string]any{
		"name":    s.name,
		"version": s.version,
	}

	// Include instructions if available
	if s.instructions != "" {
		serverInfo["instructions"] = s.instructions
	}

	// Build tools list for capabilities
	toolsList := make([]string, 0, len(s.tools))
	for name := range s.tools {
		toolsList = append(toolsList, name)
	}

	return &MCPMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools": map[string]any{
					"listChanged": false,
				},
			},
			"serverInfo": serverInfo,
		},
	}
}

func (s *MCPServer) handleToolsList(msg *MCPMessage) *MCPMessage {
	tools := make([]map[string]any, 0, len(s.tools))
	for _, tool := range s.tools {
		tools = append(tools, map[string]any{
			"name":        tool.Name,
			"description": tool.Description,
			"inputSchema": tool.InputSchema,
		})
	}

	return &MCPMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]any{
			"tools": tools,
		},
	}
}

func (s *MCPServer) handleToolsCall(msg *MCPMessage) *MCPMessage {
	params, ok := msg.Params.(map[string]any)
	if !ok {
		return &MCPMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   &MCPError{Code: -32602, Message: "Invalid params"},
		}
	}

	toolName, ok := params["name"].(string)
	if !ok {
		return &MCPMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   &MCPError{Code: -32602, Message: "Missing tool name"},
		}
	}

	tool, exists := s.tools[toolName]
	if !exists {
		return &MCPMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   &MCPError{Code: -32602, Message: "Tool not found"},
		}
	}

	arguments, _ := params["arguments"].(map[string]any)
	result, err := tool.Handler(arguments)
	if err != nil {
		return &MCPMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   &MCPError{Code: -32000, Message: err.Error()},
		}
	}

	return &MCPMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]any{
			"content": []map[string]any{{"type": "text", "text": result}},
		},
	}
}

func (s *MCPServer) writeMessage(msg *MCPMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(os.Stdout, "%s\n", data)
	return err
}

// queryTSDumpTool queries a tsdump file for one or more metrics
func (s *MCPServer) queryTSDumpTool(params map[string]any) (string, error) {
	filePath, ok := params["file"].(string)
	if !ok || filePath == "" {
		return "", fmt.Errorf("file parameter is required")
	}

	// Support both single metric (string) and multiple metrics (array)
	var metricNames []string
	if metricParam, ok := params["metrics"]; ok {
		// Array of metrics
		if metricsArray, ok := metricParam.([]any); ok {
			for _, m := range metricsArray {
				if metricStr, ok := m.(string); ok && metricStr != "" {
					metricNames = append(metricNames, metricStr)
				}
			}
		}
	} else if metricParam, ok := params["metric"]; ok {
		// Single metric (backward compatibility)
		if metricStr, ok := metricParam.(string); ok && metricStr != "" {
			metricNames = []string{metricStr}
		}
	}

	if len(metricNames) == 0 {
		// return toJSON(map[string]string{"error": "metric or metrics parameter is required"})
		return "", fmt.Errorf("metric or metrics parameter is required")
	}

	var dec *gob.Decoder

	initDecoder := func() error {
		f, err := getFileReader(filePath)
		if err != nil {
			return err
		}
		dec = gob.NewDecoder(f)
		return nil
	}
	if err := initDecoder(); err != nil {
		return "", errors.Wrap(err, "failed to create decoder")
	}

	// Skip metadata if present
	_, metadataErr := tsdumpmeta.Read(dec)
	if metadataErr != nil {
		if err := initDecoder(); err != nil {
			return "", errors.Wrap(err, "failed to recreate decoder")
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
			return "", errors.Wrap(err, "decode error")
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
		return "", fmt.Errorf("none of the requested metrics found in file: %v", metricNames)
	}

	// If only one metric was requested, return it directly (backward compatibility)
	if len(metricNames) == 1 {
		for _, result := range foundMetrics {
			return toJSON(result), nil
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

	return toJSON(result), nil
}

// toJSON converts a value to JSON string
func toJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf(`{"error": "failed to marshal JSON: %v"}`, err)
	}
	return string(data)
}

// searchMetricsTool searches for metrics matching keywords
func (s *MCPServer) searchMetricsTool(params map[string]any) (string, error) {
	query, ok := params["query"].(string)
	if !ok || query == "" {
		return "", fmt.Errorf("query parameter is required")
	}

	// Split query into keywords
	keywords := strings.Fields(query)

	// Search for matching metrics
	results := searchMetrics(s.metrics, keywords)

	// Limit results to avoid overwhelming response
	maxResults := 50
	limit, ok := params["limit"].(float64)
	if ok && int(limit) > 0 && int(limit) < maxResults {
		maxResults = int(limit)
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

	return toJSON(map[string]any{
		"total_found": len(searchMetrics(s.metrics, keywords)),
		"returned":    len(results),
		"query":       query,
		"keywords":    keywords,
		"metrics":     formattedResults,
	}), nil
}

func runDebugMCP(cmd *cobra.Command, args []string) error {
	server := NewMCPServer("cockroach-debug", "1.0.0")

	// Parse embedded metrics
	metrics, err := parseMetrics()
	if err != nil {
		return fmt.Errorf("failed to parse metrics: %w", err)
	}
	server.metrics = metrics
	fmt.Fprintf(os.Stderr, "Loaded %d metrics into memory\n", len(metrics))

	server.AddTool(MCPTool{
		Name:        "search-metrics",
		Description: "Search for CockroachDB metrics by keywords. Searches across metric names, descriptions, categories, and other metadata.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{
					"type":        "string",
					"description": "Search query with space-separated keywords (e.g., 'cpu usage', 'changefeed error', 'gc ttl')",
				},
				"limit": map[string]any{
					"type":        "number",
					"description": "Maximum number of results to return (default and max: 50)",
				},
			},
			"required": []string{"query"},
		},
		Handler: server.searchMetricsTool,
	})

	server.AddTool(MCPTool{
		Name:        "query-tsdump",
		Description: "Query a tsdump file for one or more metrics by name. Efficiently scans the file once to retrieve all requested metrics.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"file": map[string]any{
					"type":        "string",
					"description": "Path to the tsdump file to query",
				},
				"metric": map[string]any{
					"type":        "string",
					"description": "Single metric name (for backward compatibility)",
				},
				"metrics": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "string",
					},
					"description": "Array of metric names to retrieve in a single scan (recommended for multiple metrics)",
				},
			},
			"required": []string{"file"},
		},
		Handler: server.queryTSDumpTool,
	})

	return server.Run(context.Background())
}
