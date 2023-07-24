// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logmetrics

import (
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var (
	// logMetricsReg is a singleton instance of the LogMetricsRegistry.
	logMetricsReg        = newLogMetricsRegistry()
	FluentSinkConnErrors = metric.Metadata{
		Name:        string(log.FluentSinkConnectionError),
		Help:        "Number of connection errors experienced by fluent-server logging sinks",
		Measurement: "fluent-server log sink connection errors",
		Unit:        metric.Unit_COUNT,
	}
)

// Inject our singleton LogMetricsRegistry into the logging
// package. This ensures that the LogMetrics implementation within the
// log package is always defined. This should only be called once from
// a single init function.
//
// Since the primary user of the eventual metric.Registry's that come
// from LogMetricsRegistry is the MetricsRecorder, we trigger this
// init function via an import in pkg/util/log/logmetrics/metrics.go.
func init() {
	log.SetLogMetrics(logMetricsReg)
}

// logMetricsStruct is a struct used to contain all metrics
// tracked by the LogMetricsRegistry. This container is necessary
// to register all the metrics with the Registry internal to the
// LogMetricsRegistry.
type logMetricsStruct struct {
	FluentSinkConnErrors *metric.Counter
}

// LogMetricsRegistry is a log.LogMetrics implementation used in the
// logging package to give it access to metrics without introducing a
// circular dependency.
//
// All metrics meant to be available to the logging package must be
// registered at the time of initialization.
//
// LogMetricsRegistry is thread-safe.
type LogMetricsRegistry struct {
	mu struct {
		syncutil.Mutex
		// metricsStruct holds the same metrics as the below structures, but
		// provides an easy way to inject them into metric.Registry's on demand
		// in NewRegistry().
		metricsStruct logMetricsStruct
		counters      map[log.MetricName]*metric.Counter
	}
}

var _ log.LogMetrics = (*LogMetricsRegistry)(nil)

func newLogMetricsRegistry() *LogMetricsRegistry {
	registry := &LogMetricsRegistry{}
	registry.registerCounters()
	return registry
}

func (l *LogMetricsRegistry) registerCounters() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.counters = make(map[log.MetricName]*metric.Counter)
	// Create the metrics struct for us to add to registries as they're
	// requested.
	l.mu.metricsStruct = logMetricsStruct{
		FluentSinkConnErrors: metric.NewCounter(FluentSinkConnErrors),
	}
	// Be sure to also add the metrics to our internal store, for
	// recall in functions such as IncrementCounter.
	l.mu.counters[log.MetricName(FluentSinkConnErrors.Name)] = l.mu.metricsStruct.FluentSinkConnErrors
}

// NewRegistry initializes and returns a new metric.Registry, populated with metrics
// tracked by the LogMetricsRegistry. While the metrics tracked by the logmetrics package
// are global, they may be shared by multiple servers, test servers, etc. Therefore, we
// need the means to label the metrics separately depending on the server, tenant, etc.
// serving them. For this reason, we provide the ability to track the same log metrics
// across multiple registries.
func NewRegistry() *metric.Registry {
	if logMetricsReg == nil {
		panic(errors.AssertionFailedf("LogMetricsRegistry was not initialized"))
	}
	reg := metric.NewRegistry()
	logMetricsReg.mu.Lock()
	defer logMetricsReg.mu.Unlock()
	reg.AddMetricStruct(logMetricsReg.mu.metricsStruct)
	return reg
}

// IncrementCounter increments thegi Counter held by the given alias. If a log.MetricName
// is provided as an argument, but is not registered with the LogMetricsRegistry, this function
// panics.
func (l *LogMetricsRegistry) IncrementCounter(metric log.MetricName, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	counter, ok := l.mu.counters[metric]
	if !ok {
		panic(errors.AssertionFailedf("MetricName not registered in LogMetricsRegistry: %q", string(metric)))
	}
	counter.Inc(amount)
}
