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

// MetricQuery represents a query that can be executed or asserted on
type MetricQuery interface {
	// Query building methods
	ForService(serviceName string) MetricQuery
	ForNode(nodeID string) MetricQuery
	WithLabel(name, value string) MetricQuery
	OverLast(duration string) MetricQuery
	Between(start, end time.Time) MetricQuery
	Sum() MetricQuery
	Avg() MetricQuery
	Max() MetricQuery
	Rate() MetricQuery

	// Execution methods
	Scan(dest interface{}) error
	Value() (float64, error)

	// Assertion methods
	AssertHasValue(expected float64) error
	AssertHasValueAtLeast(threshold float64) error
	AssertHasValueAtMost(threshold float64) error
	AssertHasRateAtLeast(threshold float64) error
	AssertHasRateAtMost(threshold float64) error
	AssertHasPercentile(percentile int, threshold float64) error
}

// MetricsAPI provides methods to query and assert on metrics
type MetricsAPI interface {
	// Query methods
	Query(metricName string) MetricQuery
	CompareValuesOverTime(metricName string, startTime, endTime time.Time) MetricComparison

	// Debug methods
	EnableDebugMode(enabled bool)
	GetLastQuery() string
	GetLastQueryResult() float64

	// Cleanup
	Close() error
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
