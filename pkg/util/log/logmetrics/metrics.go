// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logmetrics

import (
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var (
	// logMetricsReg is a singleton instance of the logMetricsRegistry.
	logMetricsReg          = newLogMetricsRegistry()
	fluentSinkConnAttempts = metric.Metadata{
		Name:        "log.fluent.sink.conn.attempts",
		Help:        "Number of connection attempts experienced by fluent-server logging sinks",
		Measurement: "Attempts",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	fluentSinkConnErrors = metric.Metadata{
		Name:        "log.fluent.sink.conn.errors",
		Help:        "Number of connection errors experienced by fluent-server logging sinks",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	fluentSinkWriteAttempts = metric.Metadata{
		Name:        "log.fluent.sink.write.attempts",
		Help:        "Number of write attempts experienced by fluent-server logging sinks",
		Measurement: "Attempts",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	fluentSinkWriteErrors = metric.Metadata{
		Name:        "log.fluent.sink.write.errors",
		Help:        "Number of write errors experienced by fluent-server logging sinks",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	bufferedSinkMessagesDropped = metric.Metadata{
		Name:        "log.buffered.messages.dropped",
		Help:        "Count of log messages that are dropped by buffered log sinks. When CRDB attempts to buffer a log message in a buffered log sink whose buffer is already full, it drops the oldest buffered messages to make space for the new message",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	logMessageCount = metric.Metadata{
		Name:        "log.messages.count",
		Help:        "Count of messages logged on the node since startup. Note that this does not measure the fan-out of single log messages to the various configured logging sinks.",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)

// Inject our singleton logMetricsRegistry into the logging
// package. This ensures that the LogMetrics implementation within the
// log package is always defined. This should only be called once from
// a single init function.
//
// Since the primary user of the eventual metric.Registry's that come
// from logMetricsRegistry is the MetricsRecorder, we trigger this
// init function via an import in pkg/util/log/logmetrics/metrics.go.
func init() {
	log.SetLogMetrics(logMetricsReg)
}

// logMetricsRegistry is a log.LogMetrics implementation used in the
// logging package to give it access to metrics without introducing a
// circular dependency.
//
// logMetricsRegistry is thread-safe.
type logMetricsRegistry struct {
	// counters contains references to all the counters tracked by logMetricsRegistry,
	// indexed by the log.Metric type.
	counters []*metric.Counter
}

var _ log.LogMetrics = (*logMetricsRegistry)(nil)

func newLogMetricsRegistry() *logMetricsRegistry {
	return &logMetricsRegistry{
		counters: []*metric.Counter{
			log.FluentSinkConnectionAttempt: metric.NewCounter(fluentSinkConnAttempts),
			log.FluentSinkConnectionError:   metric.NewCounter(fluentSinkConnErrors),
			log.FluentSinkWriteAttempt:      metric.NewCounter(fluentSinkWriteAttempts),
			log.FluentSinkWriteError:        metric.NewCounter(fluentSinkWriteErrors),
			log.BufferedSinkMessagesDropped: metric.NewCounter(bufferedSinkMessagesDropped),
			log.LogMessageCount:             metric.NewCounter(logMessageCount),
		},
	}
}

// NewRegistry initializes and returns a new metric.Registry, populated with metrics
// tracked by the logMetricsRegistry. While the metrics tracked by the logmetrics package
// are global, they may be shared by multiple servers, test servers, etc. Therefore, we
// need the means to label the metrics separately depending on the server, tenant, etc.
// serving them. For this reason, we provide the ability to track the same log metrics
// across multiple registries.
func NewRegistry() *metric.Registry {
	if logMetricsReg == nil {
		panic(errors.AssertionFailedf("logMetricsRegistry was not initialized"))
	}
	reg := metric.NewRegistry()
	for _, c := range logMetricsReg.counters {
		reg.AddMetric(c)
	}
	return reg
}

// IncrementCounter increments the Counter held by the given log.Metric type. If a log.Metric
// is provided as an argument, but is not registered with the logMetricsRegistry, this function
// panics.
func (l *logMetricsRegistry) IncrementCounter(metric log.Metric, amount int64) {
	l.counters[metric].Inc(amount)
}
