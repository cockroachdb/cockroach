// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dashboard

import "strconv"

// Helper functions for extracting DB Console-specific options from the generic Chart and Metric structs.

// getBoolOption is a utility to extract a boolean value from options map.
// It uses strconv.ParseBool to convert string representations to boolean.
func getBoolOption(options map[string]interface{}, key string) *bool {
	if options == nil {
		return nil
	}

	val, ok := options[key]
	if !ok {
		return nil
	}

	// Try parsing as string using strconv.ParseBool
	if strVal, ok := val.(string); ok {
		if boolVal, err := strconv.ParseBool(strVal); err == nil {
			return &boolVal
		}
	}

	return nil
}

// GetChartGraphType extracts the graph_type option from a Chart.
func GetChartGraphType(chart Chart) string {
	if chart.Options != nil {
		if val, ok := chart.Options["graph_type"].(string); ok {
			return val
		}
	}
	return ""
}

// GetChartSources extracts the sources option from a Chart.
func GetChartSources(chart Chart) string {
	if chart.Options != nil {
		if val, ok := chart.Options["sources"].(string); ok {
			return val
		}
	}
	return ""
}

// GetChartShowMetricsInTooltip extracts the show_metrics_in_tooltip option from a Chart.
func GetChartShowMetricsInTooltip(chart Chart) *bool {
	return getBoolOption(chart.Options, "show_metrics_in_tooltip")
}

// GetChartPreCalcGraphSize extracts the precalc_graph_size option from a Chart.
func GetChartPreCalcGraphSize(chart Chart) *bool {
	return getBoolOption(chart.Options, "precalc_graph_size")
}

// GetMetricRate extracts the rate option from a Metric.
func GetMetricRate(metric Metric) *bool {
	return getBoolOption(metric.Options, "rate")
}

// GetMetricPerNode extracts the per_node option from a Metric.
func GetMetricPerNode(metric Metric) *bool {
	return getBoolOption(metric.Options, "per_node")
}

// GetMetricSourcesType extracts the sources_type option from a Metric.
func GetMetricSourcesType(metric Metric) string {
	if metric.Options != nil {
		if val, ok := metric.Options["sources_type"].(string); ok {
			return val
		}
	}
	return ""
}

// GetMetricAggregation extracts the aggregation option from a Metric.
func GetMetricAggregation(metric Metric) string {
	if metric.Options != nil {
		if val, ok := metric.Options["aggregation"].(string); ok {
			return val
		}
	}
	return ""
}
