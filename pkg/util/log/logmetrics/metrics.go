// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logmetrics

import (
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_model/go"
)

var (
	// logMetricsReg is a singleton instance of the LogMetricsRegistry.
	logMetricsReg          = newLogMetricsRegistry()
	FluentSinkConnAttempts = metric.Metadata{
		Name:        "log.fluent.sink.conn.attempts",
		Help:        "Number of connection attempts experienced by fluent-server logging sinks",
		Measurement: "Attempts",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	FluentSinkConnErrors = metric.Metadata{
		Name:        "log.fluent.sink.conn.errors",
		Help:        "Number of connection errors experienced by fluent-server logging sinks",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	FluentSinkWriteAttempts = metric.Metadata{
		Name:        "log.fluent.sink.write.attempts",
		Help:        "Number of write attempts experienced by fluent-server logging sinks",
		Measurement: "Attempts",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	FluentSinkWriteErrors = metric.Metadata{
		Name:        "log.fluent.sink.write.errors",
		Help:        "Number of write errors experienced by fluent-server logging sinks",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	BufferedSinkMessagesDropped = metric.Metadata{
		Name:        "log.buffered.messages.dropped",
		Help:        "Count of log messages that are dropped by buffered log sinks. When CRDB attempts to buffer a log message in a buffered log sink whose buffer is already full, it drops the oldest buffered messages to make space for the new message",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	LogMessageCount = metric.Metadata{
		Name:        "log.messages.count",
		Help:        "Count of messages logged on the node since startup. Note that this does not measure the fan-out of single log messages to the various configured logging sinks.",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
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
//
// NB: If adding metrics to this struct, be sure to also add in
// (*LogMetricsRegistry).registerCounters
type logMetricsStruct struct {
	FluentSinkConnAttempts      *metric.Counter
	FluentSinkConnErrors        *metric.Counter
	FluentSinkWriteAttempts     *metric.Counter
	FluentSinkWriteErrors       *metric.Counter
	BufferedSinkMessagesDropped *metric.Counter
	LogMessageCount             *metric.Counter
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
	// metricsStruct holds the same metrics as the below structures, but
	// provides an easy way to inject them into metric.Registry's on demand
	// in NewRegistry().
	metricsStruct logMetricsStruct
	mu            struct {
		syncutil.Mutex
		counters []*metric.Counter
	}
}

var _ log.LogMetrics = (*LogMetricsRegistry)(nil)

func newLogMetricsRegistry() *LogMetricsRegistry {
	registry := &LogMetricsRegistry{}
	registry.registerCounters()
	return registry
}

func (l *LogMetricsRegistry) registerCounters() {
	l.mu.counters = make([]*metric.Counter, len(log.Metrics))
	// Create the metrics struct for us to add to registries as they're
	// requested.
	l.metricsStruct = logMetricsStruct{
		FluentSinkConnAttempts:      metric.NewCounter(FluentSinkConnAttempts),
		FluentSinkConnErrors:        metric.NewCounter(FluentSinkConnErrors),
		FluentSinkWriteAttempts:     metric.NewCounter(FluentSinkWriteAttempts),
		FluentSinkWriteErrors:       metric.NewCounter(FluentSinkWriteErrors),
		BufferedSinkMessagesDropped: metric.NewCounter(BufferedSinkMessagesDropped),
		LogMessageCount:             metric.NewCounter(LogMessageCount),
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// Be sure to also add the metrics to our internal store, for
	// recall in functions such as IncrementCounter.
	l.mu.counters[log.FluentSinkConnectionAttempt] = l.metricsStruct.FluentSinkConnAttempts
	l.mu.counters[log.FluentSinkConnectionError] = l.metricsStruct.FluentSinkConnErrors
	l.mu.counters[log.FluentSinkWriteAttempt] = l.metricsStruct.FluentSinkWriteAttempts
	l.mu.counters[log.FluentSinkWriteError] = l.metricsStruct.FluentSinkWriteErrors
	l.mu.counters[log.BufferedSinkMessagesDropped] = l.metricsStruct.BufferedSinkMessagesDropped
	l.mu.counters[log.LogMessageCount] = l.metricsStruct.LogMessageCount
	for _, c := range l.mu.counters {
		if c == nil {
			panic(errors.AssertionFailedf("Failed to register log metric in LogMetricsRegistry"))
		}
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
	reg.AddMetricStruct(logMetricsReg.metricsStruct)
	return reg
}

// IncrementCounter increments thegi Counter held by the given alias. If a log.MetricName
// is provided as an argument, but is not registered with the LogMetricsRegistry, this function
// panics.
func (l *LogMetricsRegistry) IncrementCounter(metric log.Metric, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.counters[metric].Inc(amount)
}
