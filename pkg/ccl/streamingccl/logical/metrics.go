// Copyright 2021 The Cockroach Authors.
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
	metaReplicationEventsIngested = metric.Metadata{
		Name:        "logical_replication.events_ingested",
		Help:        "Events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationCheckpointEventsIngested = metric.Metadata{
		Name:        "logical_replication.checkpoint_events_ingested",
		Help:        "Checkpoint events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationIngestedBytes = metric.Metadata{
		Name:        "logical_replication.logical_bytes",
		Help:        "Logical bytes (sum of keys + values) ingested by all replication jobs",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaReplicationFlushes = metric.Metadata{
		Name:        "logical_replication.flushes",
		Help:        "Total flushes across all replication jobs",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationFlushHistNanos = metric.Metadata{
		Name:        "logical_replication.flush_hist_nanos",
		Help:        "Time spent flushing messages across all replication streams",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicationCommitLatency = metric.Metadata{
		Name: "logical_replication.commit_latency",
		Help: "Event commit latency: a difference between event MVCC timestamp " +
			"and the time it was flushed into disk. If we batch events, then the difference " +
			"between the oldest event in the batch and flush is recorded",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicationAdmitLatency = metric.Metadata{
		Name: "logical_replication.admit_latency",
		Help: "Event admission latency: a difference between event MVCC timestamp " +
			"and the time it was admitted into ingestion processor",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStreamsRunning = metric.Metadata{
		Name:        "logical_replication.running",
		Help:        "Number of currently running replication streams",
		Measurement: "Replication Streams",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicatedTimeSeconds = metric.Metadata{
		Name:        "logical_replication.replicated_time_seconds",
		Help:        "The replicated time of the logical replication stream in seconds since the unix epoch.",
		Measurement: "Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaJobProgressUpdates = metric.Metadata{
		Name:        "logical_replication.job_progress_updates",
		Help:        "Total number of updates to the ingestion job progress",
		Measurement: "Job Updates",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationFlushRowCountHist = metric.Metadata{
		Name:        "logical_replication.flush_row_count",
		Help:        "Number of rows in a given flush",
		Measurement: "Rows",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationFlushBytesHist = metric.Metadata{
		Name:        "logical_replication.flush_bytes",
		Help:        "Number of bytes in a given flush",
		Measurement: "Logical bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaReplicationFlushWaitHistNanos = metric.Metadata{
		Name:        "logical_replication.flush_wait_nanos",
		Help:        "Time spenting waiting for an in-progress flush",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicationFlushOnSize = metric.Metadata{
		Name:        "logical_replication.flush_on_size",
		Help:        "Number of flushes caused by hitting the buffer size limit",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationFlushOnTime = metric.Metadata{
		Name:        "logical_replication.flush_on_time",
		Help:        "Number of flushes caused by hitting the time limit",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationBatchBytes = metric.Metadata{
		Name:        "logical_replication.batch_bytes",
		Help:        "Number of bytes in a given batch",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaReplicationBatchHistNanos = metric.Metadata{
		Name:        "logical_replication.batch_hist_nanos",
		Help:        "Time spent flushing a batch",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaDistSQLReplanCount = metric.Metadata{
		Name:        "logical_replication.distsql_replan_count",
		Help:        "Total number of dist sql replanning events",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of logical replication jobs.
type Metrics struct {
	IngestedEvents        *metric.Counter
	IngestedLogicalBytes  *metric.Counter
	Flushes               *metric.Counter
	JobProgressUpdates    *metric.Counter
	CheckpointEvents      *metric.Counter
	ReplanCount           *metric.Counter
	FlushRowCountHist     metric.IHistogram
	FlushBytesHist        metric.IHistogram
	FlushHistNanos        metric.IHistogram
	FlushWaitHistNanos    metric.IHistogram
	FlushOnSize           *metric.Counter
	FlushOnTime           *metric.Counter
	BatchBytesHist        metric.IHistogram
	BatchHistNanos        metric.IHistogram
	CommitLatency         metric.IHistogram
	AdmitLatency          metric.IHistogram
	RunningCount          *metric.Gauge
	ReplicatedTimeSeconds *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for logical replication job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	return &Metrics{
		IngestedEvents:       metric.NewCounter(metaReplicationEventsIngested),
		IngestedLogicalBytes: metric.NewCounter(metaReplicationIngestedBytes),
		Flushes:              metric.NewCounter(metaReplicationFlushes),
		CheckpointEvents:     metric.NewCounter(metaReplicationCheckpointEventsIngested),
		JobProgressUpdates:   metric.NewCounter(metaJobProgressUpdates),
		ReplanCount:          metric.NewCounter(metaDistSQLReplanCount),
		FlushHistNanos: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationFlushHistNanos,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		CommitLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationCommitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.LongRunning60mLatencyBuckets,
		}),
		AdmitLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationAdmitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		FlushRowCountHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationFlushRowCountHist,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		FlushBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationFlushBytesHist,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		FlushWaitHistNanos: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationFlushWaitHistNanos,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		FlushOnSize: metric.NewCounter(metaReplicationFlushOnSize),
		FlushOnTime: metric.NewCounter(metaReplicationFlushOnTime),
		BatchBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationBatchBytes,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		BatchHistNanos: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaReplicationBatchHistNanos,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		RunningCount:          metric.NewGauge(metaStreamsRunning),
		ReplicatedTimeSeconds: metric.NewGauge(metaReplicatedTimeSeconds),
	}
}
