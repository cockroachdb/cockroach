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
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var (
	// logMetricsReg is a singleton instance of the LogMetricsRegistry.
	logMetricsReg = newLogMetricsRegistry()
	// metricToMeta stores, for each log.Metric type supported, the metric's metadata.
	// All metrics intended to be used within the LogMetricsRegistry must be contained
	// within this map.
	metricToMeta = map[log.Metric]metric.Metadata{
		log.FluentSinkConnectionAttempt: {
			Name:        "log.fluent.sink.conn.attempts",
			Help:        "Number of connection attempts experienced by fluent-server logging sinks",
			Measurement: "Attempts",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		},
		log.FluentSinkConnectionError: {
			Name:        "log.fluent.sink.conn.errors",
			Help:        "Number of connection errors experienced by fluent-server logging sinks",
			Measurement: "Errors",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		},
		log.FluentSinkWriteAttempt: {
			Name:        "log.fluent.sink.write.attempts",
			Help:        "Number of write attempts experienced by fluent-server logging sinks",
			Measurement: "Attempts",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		},
		log.FluentSinkWriteError: {
			Name:        "log.fluent.sink.write.errors",
			Help:        "Number of write errors experienced by fluent-server logging sinks",
			Measurement: "Errors",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		},
		log.BufferedSinkMessagesDropped: {
			Name:        "log.buffered.messages.dropped",
			Help:        "Count of log messages that are dropped by buffered log sinks. When CRDB attempts to buffer a log message in a buffered log sink whose buffer is already full, it drops the oldest buffered messages to make space for the new message",
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		},
		log.LogMessageCount: {
			Name:        "log.messages.count",
			Help:        "Count of messages logged on the node since startup. Note that this does not measure the fan-out of single log messages to the various configured logging sinks.",
			Measurement: "Messages",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		},
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

// LogMetricsRegistry is a log.LogMetrics implementation used in the
// logging package to give it access to metrics without introducing a
// circular dependency.
//
// LogMetricsRegistry is thread-safe.
type LogMetricsRegistry struct {
	// counters contains references to all the counters
	// tracked by LogMetricsRegistry, keyed by metric type.
	counters map[log.Metric]*metric.Counter
}

var _ log.LogMetrics = (*LogMetricsRegistry)(nil)

func newLogMetricsRegistry() *LogMetricsRegistry {
	registry := &LogMetricsRegistry{}
	registry.registerCounters()
	return registry
}

func (l *LogMetricsRegistry) registerCounters() {
	l.counters = make(map[log.Metric]*metric.Counter, len(metricToMeta))
	for metricType, meta := range metricToMeta {
		l.counters[metricType] = metric.NewCounter(meta)
	}
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
	for _, m := range logMetricsReg.counters {
		reg.AddMetric(m)
	}
	return reg
}

// IncrementCounter increments the Counter held by the given log.Metric type. If a log.Metric
// is provided as an argument, but is not registered with the LogMetricsRegistry, this function
// panics.
func (l *LogMetricsRegistry) IncrementCounter(metric log.Metric, amount int64) {
	if _, ok := l.counters[metric]; !ok {
		panic(errors.AssertionFailedf("attempted to increment unregistered logging metric with ordinal %d", metric))
	}
	l.counters[metric].Inc(amount)
}
