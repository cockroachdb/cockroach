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

const (
	streamingFlushHistMaxLatency   = 1 * time.Minute
	streamingAdmitLatencyMaxValue  = 3 * time.Minute
	streamingCommitLatencyMaxValue = 10 * time.Minute
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
		Measurement: "Streams",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of stream ingestion jobs.
type Metrics struct {
	IngestedEvents *metric.Counter
	IngestedBytes  *metric.Counter
	Flushes        *metric.Counter
	ResolvedEvents *metric.Counter
	FlushHistNanos *metric.Histogram
	CommitLatency  *metric.Histogram
	AdmitLatency   *metric.Histogram
	RunningCount   *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for stream ingestion job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		IngestedEvents: metric.NewCounter(metaStreamingEventsIngested),
		IngestedBytes:  metric.NewCounter(metaStreamingIngestedBytes),
		Flushes:        metric.NewCounter(metaStreamingFlushes),
		ResolvedEvents: metric.NewCounter(metaStreamingResolvedEventsIngested),
		FlushHistNanos: metric.NewHistogram(metaStreamingFlushHistNanos,
			histogramWindow, streamingFlushHistMaxLatency.Nanoseconds(), 1),
		CommitLatency: metric.NewHistogram(metaStreamingCommitLatency,
			histogramWindow, streamingCommitLatencyMaxValue.Nanoseconds(), 1),
		AdmitLatency: metric.NewHistogram(metaStreamingAdmitLatency,
			histogramWindow, streamingAdmitLatencyMaxValue.Nanoseconds(), 1),
		RunningCount: metric.NewGauge(metaStreamsRunning),
	}
	return m
}

func init() {
	jobs.MakeStreamIngestMetricsHook = MakeMetrics
}
