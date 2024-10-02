// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

// LogMetrics enables the registration and recording of metrics
// within the log package.
//
// Because the log package is imported by nearly every package
// within CRDB, it's difficult to add new dependencies to the
// log package without introducing a circular dependency.
//
// The LogMetrics interface provides us with a way to still
// make use of the metrics library within the log package via
// dependency injection, allowing the implementation to live
// elsewhere (e.g. the metrics package).
type LogMetrics interface {
	// IncrementCounter increments the Counter metric associated with the
	// provided MetricName by the given amount, assuming the
	// metric has been registered.
	//
	// The LogMetrics implementation must have metadata defined
	// for the given MetricName within its own scope. See
	// pkg/util/log/logmetrics for details.
	IncrementCounter(metric Metric, amount int64)
}

// Metric is the enum representation of each metric supported within the log package.
// NB: The metric also needs to be added to pkg/util/log/logmetrics, where we
// need to register the metric and define its metadata.
type Metric int

const (
	FluentSinkConnectionAttempt Metric = iota
	FluentSinkConnectionError
	FluentSinkWriteAttempt
	FluentSinkWriteError
	BufferedSinkMessagesDropped
	LogMessageCount
)
