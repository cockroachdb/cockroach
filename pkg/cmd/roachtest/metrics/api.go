// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package metrics provides a fluent API for querying and asserting on metrics
// in roachtests.
package metrics

import "time"

// Fataler is an interface for test implementations that can fail a test.
type Fataler interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

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

	// Assertion methods that directly fail the test via Fataler
	// If Fataler is nil, the method will panic with a meaningful error
	AssertHasValue(t Fataler, expected float64)
	AssertHasValueAtLeast(t Fataler, threshold float64)
	AssertHasValueAtMost(t Fataler, threshold float64)
	AssertHasRateAtLeast(t Fataler, threshold float64)
	AssertHasRateAtMost(t Fataler, threshold float64)
	AssertHasPercentile(t Fataler, percentile int, threshold float64)

	// AssertEventually polls the metric until the predicate returns true or timeout is reached
	AssertEventually(t Fataler, predicate func(float64) bool, timeout time.Duration)

	// AssertRecoversTo asserts a metric returns to at least threshold within timeout
	AssertRecoversTo(t Fataler, threshold float64, timeout time.Duration)

	// AssertDropsBelow asserts a metric drops below threshold within timeout
	AssertDropsBelow(t Fataler, threshold float64, timeout time.Duration)
}

// MetricsAPI provides methods to query and assert on metrics
type MetricsAPI interface {
	// Query methods
	Query(metricName string) MetricQuery
	CompareValuesOverTime(metricName string, startTime, endTime time.Time) MetricComparison

	// Logging configuration
	EnableVerboseLogging(enabled bool)

	// Cleanup
	Close() error
}

// MetricComparison allows comparing metrics between two time points.
type MetricComparison interface {
	// AssertIncreasedByLessThan asserts that the metric hasn't grown by more than the given percentage.
	AssertIncreasedByLessThan(t Fataler, percentage float64)

	// AssertIncreasedByAtLeast asserts that the metric has grown by at least the given percentage.
	AssertIncreasedByAtLeast(t Fataler, percentage float64)

	// AssertChangedByLessThan asserts that the metric hasn't changed by more than the given percentage.
	AssertChangedByLessThan(t Fataler, percentage float64)
}
