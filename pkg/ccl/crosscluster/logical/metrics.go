// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	// Top-line metrics.
	metaAppliedRowUpdates = metric.Metadata{
		Name:        "logical_replication.events_ingested",
		Help:        "Events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaAppliedLogicalBytes = metric.Metadata{
		Name:        "logical_replication.logical_bytes",
		Help:        "Logical bytes (sum of keys + values) ingested by all replication jobs",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCommitToCommitLatency = metric.Metadata{
		Name: "logical_replication.commit_latency",
		Help: "Event commit latency: a difference between event MVCC timestamp " +
			"and the time it was flushed into disk. If we batch events, then the difference " +
			"between the oldest event in the batch and flush is recorded",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicatedTimeSeconds = metric.Metadata{
		Name:        "logical_replication.replicated_time_seconds",
		Help:        "The replicated time of the logical replication stream in seconds since the unix epoch.",
		Measurement: "Seconds",
		Unit:        metric.Unit_SECONDS,
	}

	// User-visible health and ops metrics.
	metaApplyBatchNanosHist = metric.Metadata{
		Name:        "logical_replication.batch_hist_nanos",
		Help:        "Time spent flushing a batch",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// Internal metrics.
	metaCheckpointEvents = metric.Metadata{
		Name:        "logical_replication.checkpoint_events_ingested",
		Help:        "Checkpoint events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamBatchRowsHist = metric.Metadata{
		Name:        "logical_replication.flush_row_count",
		Help:        "Number of rows in a given flush",
		Measurement: "Rows",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamBatchBytesHist = metric.Metadata{
		Name:        "logical_replication.flush_bytes",
		Help:        "Number of bytes in a given flush",
		Measurement: "Logical bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaStreamBatchNanosHist = metric.Metadata{
		Name:        "logical_replication.flush_hist_nanos",
		Help:        "Time spent flushing messages across all replication streams",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaOptimisticInsertConflictCount = metric.Metadata{
		Name:        "logical_replication.optimistic_insert_conflict_count",
		Help:        "Total number of times the optimistic insert encountered a conflict",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of logical replication jobs.
type Metrics struct {
	// Top-line user-facing numbers that how many events and how much data are
	// bring moved and applied/rejected/etc.
	AppliedRowUpdates     *metric.Counter
	AppliedLogicalBytes   *metric.Counter
	CommitToCommitLatency metric.IHistogram
	ReplicatedTimeSeconds *metric.Gauge

	// User-surfaced information about the health/operation of the stream; this
	// should be a narrow subset of numbers that are actually relevant to a user
	// such as the latency of application as that could be their supplied UDF.
	ApplyBatchNanosHist metric.IHistogram

	// Internal numbers that are useful for determining why a stream is behaving
	// a specific way.
	CheckpointEvents *metric.Counter
	// TODO(dt): are these stream batch size or latency numbers useful?
	StreamBatchRowsHist           metric.IHistogram
	StreamBatchBytesHist          metric.IHistogram
	StreamBatchNanosHist          metric.IHistogram
	OptimisticInsertConflictCount *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for logical replication job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	return &Metrics{
		AppliedRowUpdates:   metric.NewCounter(metaAppliedRowUpdates),
		AppliedLogicalBytes: metric.NewCounter(metaAppliedLogicalBytes),
		CommitToCommitLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaCommitToCommitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.LongRunning60mLatencyBuckets,
		}),
		ReplicatedTimeSeconds: metric.NewGauge(metaReplicatedTimeSeconds),
		ApplyBatchNanosHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaApplyBatchNanosHist,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		CheckpointEvents: metric.NewCounter(metaCheckpointEvents),
		StreamBatchRowsHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaStreamBatchRowsHist,
			Duration:     histogramWindow,
			BucketConfig: metric.DataCount16MBuckets,
		}),
		StreamBatchBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaStreamBatchBytesHist,
			Duration:     histogramWindow,
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		StreamBatchNanosHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaStreamBatchNanosHist,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		OptimisticInsertConflictCount: metric.NewCounter(metaOptimisticInsertConflictCount),
	}
}
