// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	IncrementCounter(metric MetricName, amount int64)
}

// MetricName represents the name of a metric registered &
// used within the log package, available to use in the LogMetrics
// interface.
type MetricName string

// FluentSinkConnectionError is the MetricName for the metric
// used to count fluent-server log sink connection errors. Please
// refer to its metric metadata for more details (hint: see usages).
const FluentSinkConnectionError MetricName = "fluent.sink.conn.errors"
