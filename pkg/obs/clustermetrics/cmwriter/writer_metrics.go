// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaFlushLatency = metric.InitMetadata(metric.Metadata{
		Name:        "obs.clustermetrics.flush.latency",
		Help:        "Latency of cluster metrics flushes to storage",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	})
	metaFlushCount = metric.InitMetadata(metric.Metadata{
		Name:        "obs.clustermetrics.flush.count",
		Help:        "Number of cluster metrics flush operations",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	})
	metaFlushErrors = metric.InitMetadata(metric.Metadata{
		Name:        "obs.clustermetrics.flush.errors",
		Help:        "Number of flush errors (write or delete failures)",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	})
	metaMetricsWritten = metric.InitMetadata(metric.Metadata{
		Name:        "obs.clustermetrics.flush.metrics_written",
		Help:        "Number of individual metrics written per flush",
		Measurement: "Metrics",
		Unit:        metric.Unit_COUNT,
	})
	metaMetricsDeleted = metric.InitMetadata(metric.Metadata{
		Name:        "obs.clustermetrics.flush.metrics_deleted",
		Help:        "Number of individual metrics deleted per flush",
		Measurement: "Metrics",
		Unit:        metric.Unit_COUNT,
	})
)

// WriterMetrics holds operational metrics for the Writer.
type WriterMetrics struct {
	FlushLatency   *metric.Gauge
	FlushCount     *metric.Counter
	FlushErrors    *metric.Counter
	MetricsWritten *metric.Counter
	MetricsDeleted *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (WriterMetrics) MetricStruct() {}

// NewWriterMetrics creates a new WriterMetrics instance.
func NewWriterMetrics() *WriterMetrics {
	return &WriterMetrics{
		FlushLatency:   metric.NewGauge(metaFlushLatency),
		FlushCount:     metric.NewCounter(metaFlushCount),
		FlushErrors:    metric.NewCounter(metaFlushErrors),
		MetricsWritten: metric.NewCounter(metaMetricsWritten),
		MetricsDeleted: metric.NewCounter(metaMetricsDeleted),
	}
}
