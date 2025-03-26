// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package metrics provides a fluent API for querying and asserting on metrics
// in roachtests.
package metrics

import (
	"time"
)

// MetricsAPI provides a fluent interface for querying and asserting on metrics.
type MetricsAPI interface {
	// AssertThat starts a query chain for the given metric.
	AssertThat(metricName string) MetricQuery

	// Close releases any resources held by the metrics API.
	Close() error

	// CompareValuesOverTime creates a metric comparison between two time points.
	CompareValuesOverTime(metricName string, startTime, endTime time.Time) MetricComparison

	// EnableDebugMode enables debug mode, which logs queries and their results.
	EnableDebugMode(enabled bool)

	// GetLastQuery returns the last query that was executed, useful for testing and debugging.
	GetLastQuery() string

	// GetLastQueryResult returns the result of the last query, useful for testing and debugging.
	GetLastQueryResult() float64
}

// MetricQuery represents an in-progress metric query with assertion capabilities.
type MetricQuery interface {
	// Filtering methods
	ForService(serviceName string) MetricQuery
	ForNode(nodeID string) MetricQuery
	WithLabel(name, value string) MetricQuery

	// Time range specification
	OverLast(duration string) MetricQuery
	Between(start, end time.Time) MetricQuery

	// Aggregation operations
	Sum() MetricQuery
	Avg() MetricQuery
	Max() MetricQuery
	Rate() MetricQuery

	// Assertion methods
	HasValueAtLeast(threshold float64) error
	HasValueAtMost(threshold float64) error
	HasRateAtLeast(threshold float64) error
	HasRateAtMost(threshold float64) error
	HasPercentile(percentile int, threshold float64) error

	// Value retrieval
	Value() (float64, error)

	// GetBuiltQuery returns the PromQL query string that would be executed by this query.
	// This is useful for testing and debugging, and does not actually execute the query.
	GetBuiltQuery() string
}

// MetricComparison allows comparing metrics between two time points.
type MetricComparison interface {
	// HasIncreasedByLessThan asserts that the metric hasn't grown by more than the given percentage.
	HasIncreasedByLessThan(percentage float64) error

	// HasIncreasedByAtLeast asserts that the metric has grown by at least the given percentage.
	HasIncreasedByAtLeast(percentage float64) error

	// HasChangedByLessThan asserts that the metric hasn't changed by more than the given percentage.
	HasChangedByLessThan(percentage float64) error
}
