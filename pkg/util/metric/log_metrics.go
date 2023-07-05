// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var (
	FluentSinkConnErrors = Metadata{
		Name:        string(log.FluentSinkConnectionError),
		Help:        "Number of connection errors experienced by fluent-server logging sinks",
		Measurement: "fluent-server log sink connection errors",
		Unit:        Unit_COUNT,
	}
)

// logMetricsStruct is a struct used to contain all metrics
// tracked by the LogMetricsRegistry. This container is necessary
// to register all the metrics with the Registry internal to the
// LogMetricsRegistry.
type logMetricsStruct struct {
	FluentSinkConnErrors *Counter
}

// LogMetricsRegistry is a log.LogMetrics implementation used in the
// logging package to give it access to metrics without introducing a
// circular dependency.
//
// All metrics meant to be available to the logging package must be
// registered at the time of initialization.
type LogMetricsRegistry struct {
	mu struct {
		syncutil.Mutex
		reg      *Registry
		counters map[log.LogMetricName]*Counter
	}
}

var _ log.LogMetrics = (*LogMetricsRegistry)(nil)

func NewLogMetricsRegistry() *LogMetricsRegistry {
	registry := &LogMetricsRegistry{}
	registry.mu.counters = make(map[log.LogMetricName]*Counter)
	registry.mu.reg = NewRegistry()
	registry.registerCounters()
	return registry
}

func (l *LogMetricsRegistry) Registry() interface{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.reg
}

func (l *LogMetricsRegistry) IncrementCounter(metric log.LogMetricName, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if counter, ok := l.mu.counters[metric]; ok {
		counter.Inc(amount)
	}
}

func (l *LogMetricsRegistry) registerCounters() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Create the metrics struct for us to add to the registry.
	metrics := logMetricsStruct{
		FluentSinkConnErrors: NewCounter(FluentSinkConnErrors),
	}
	l.mu.reg.AddMetricStruct(metrics)
	// Be sure to also add the metrics to our internal store, for
	// recall in functions such as IncrementCounter.
	l.mu.counters[log.LogMetricName(FluentSinkConnErrors.Name)] = metrics.FluentSinkConnErrors
}
