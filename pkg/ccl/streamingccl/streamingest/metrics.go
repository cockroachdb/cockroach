// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaStreamingEventsIngested = metric.Metadata{
		Name:        "streaming.events_ingested",
		Help:        "Events ingested by all ingestion jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamingResolvedEventsIngested = metric.Metadata{
		Name:        "streaming.resolved_events_ingested",
		Help:        "Resolved events ingested by all ingestion jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamingIngestedBytes = metric.Metadata{
		Name:        "streaming.ingested_bytes",
		Help:        "Bytes ingested by all ingestion jobs",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaStreamingFlushes = metric.Metadata{
		Name:        "streaming.flushes",
		Help:        "Total flushes across all ingestion jobs",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}

	metaStreamingFlushHistNanos = metric.Metadata{
		Name:        "streaming.flush_hist_nanos",
		Help:        "Time spent flushing messages across all replication streams",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStreamingCommitLatency = metric.Metadata{
		Name: "streaming.commit_latency",
		Help: "Event commit latency: a difference between event MVCC timestamp " +
			"and the time it was flushed into disk. If we batch events, then the difference " +
			"between the oldest event in the batch and flush is recorded",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStreamingAdmitLatency = metric.Metadata{
		Name: "streaming.admit_latency",
		Help: "Event admission latency: a difference between event MVCC timestamp " +
			"and the time it was admitted into ingestion processor",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStreamsRunning = metric.Metadata{
		Name:        "streaming.running",
		Help:        "Number of currently running replication streams",
		Measurement: "Replication Streams",
		Unit:        metric.Unit_COUNT,
	}
	metaEarliestDataCheckpointSpan = metric.Metadata{
		Name:        "streaming.earliest_data_checkpoint_span",
		Help:        "The earliest timestamp of the last checkpoint forwarded by an ingestion data processor",
		Measurement: "Timestamp",
		Unit:        metric.Unit_TIMESTAMP_NS,
	}
	metaLatestDataCheckpointSpan = metric.Metadata{
		Name:        "streaming.latest_data_checkpoint_span",
		Help:        "The latest timestamp of the last checkpoint forwarded by an ingestion data processor",
		Measurement: "Timestamp",
		Unit:        metric.Unit_TIMESTAMP_NS,
	}
	metaDataCheckpointSpanCount = metric.Metadata{
		Name:        "streaming.data_checkpoint_span_count",
		Help:        "The number of resolved spans in the last checkpoint forwarded by an ingestion data processor",
		Measurement: "Resolved Spans",
		Unit:        metric.Unit_COUNT,
	}
	metaFrontierCheckpointSpanCount = metric.Metadata{
		Name:        "streaming.frontier_checkpoint_span_count",
		Help:        "The number of resolved spans last persisted to the ingestion job's checkpoint record",
		Measurement: "Resolved Spans",
		Unit:        metric.Unit_COUNT,
	}
	metaFrontierLagSeconds = metric.Metadata{
		Name:        "replication.frontier_lag_seconds",
		Help:        "Time the replication frontier lags",
		Measurement: "Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaJobProgressUpdates = metric.Metadata{
		Name:        "streaming.job_progress_updates",
		Help:        "Total number of updates to the ingestion job progress",
		Measurement: "Job Updates",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of stream ingestion jobs.
type Metrics struct {
	IngestedEvents              *metric.Counter
	IngestedBytes               *metric.Counter
	Flushes                     *metric.Counter
	JobProgressUpdates          *metric.Counter
	ResolvedEvents              *metric.Counter
	FlushHistNanos              *metric.Histogram
	CommitLatency               *metric.Histogram
	AdmitLatency                *metric.Histogram
	RunningCount                *metric.Gauge
	EarliestDataCheckpointSpan  *metric.Gauge
	LatestDataCheckpointSpan    *metric.Gauge
	DataCheckpointSpanCount     *metric.Gauge
	FrontierCheckpointSpanCount *metric.Gauge
	FrontierLagSeconds          *metric.GaugeFloat64
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for stream ingestion job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		IngestedEvents:     metric.NewCounter(metaStreamingEventsIngested),
		IngestedBytes:      metric.NewCounter(metaStreamingIngestedBytes),
		Flushes:            metric.NewCounter(metaStreamingFlushes),
		ResolvedEvents:     metric.NewCounter(metaStreamingResolvedEventsIngested),
		JobProgressUpdates: metric.NewCounter(metaJobProgressUpdates),
		FlushHistNanos: metric.NewHistogram(metaStreamingFlushHistNanos,
			histogramWindow, metric.BatchProcessLatencyBuckets),
		CommitLatency: metric.NewHistogram(metaStreamingCommitLatency,
			histogramWindow, metric.BatchProcessLatencyBuckets),
		AdmitLatency: metric.NewHistogram(metaStreamingAdmitLatency,
			histogramWindow, metric.BatchProcessLatencyBuckets),
		RunningCount:                metric.NewGauge(metaStreamsRunning),
		EarliestDataCheckpointSpan:  metric.NewGauge(metaEarliestDataCheckpointSpan),
		LatestDataCheckpointSpan:    metric.NewGauge(metaLatestDataCheckpointSpan),
		DataCheckpointSpanCount:     metric.NewGauge(metaDataCheckpointSpanCount),
		FrontierCheckpointSpanCount: metric.NewGauge(metaFrontierCheckpointSpanCount),
		FrontierLagSeconds:          metric.NewGaugeFloat64(metaFrontierLagSeconds),
	}
	return m
}

func init() {
	jobs.MakeStreamIngestMetricsHook = MakeMetrics
}
