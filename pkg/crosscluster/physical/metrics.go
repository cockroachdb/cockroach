// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

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
	metaReplicationEventsIngested = metric.Metadata{
		Name:        "physical_replication.events_ingested",
		Help:        "Events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationResolvedEventsIngested = metric.Metadata{
		Name:        "physical_replication.resolved_events_ingested",
		Help:        "Resolved events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicationIngestedBytes = metric.Metadata{
		Name:        "physical_replication.logical_bytes",
		Help:        "Logical bytes (sum of keys + values) ingested by all replication jobs",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaReplicationFlushes = metric.Metadata{
		Name:        "physical_replication.flushes",
		Help:        "Total flushes across all replication jobs",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}

	metaReplicationFlushHistNanos = metric.Metadata{
		Name:        "physical_replication.flush_hist_nanos",
		Help:        "Time spent flushing messages across all replication streams",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicationCommitLatency = metric.Metadata{
		Name: "physical_replication.commit_latency",
		Help: "Event commit latency: a difference between event MVCC timestamp " +
			"and the time it was flushed into disk. If we batch events, then the difference " +
			"between the oldest event in the batch and flush is recorded",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaReplicationAdmitLatency = metric.Metadata{
		Name: "physical_replication.admit_latency",
		Help: "Event admission latency: a difference between event MVCC timestamp " +
			"and the time it was admitted into ingestion processor",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaStreamsRunning = metric.Metadata{
		Name:        "physical_replication.running",
		Help:        "Number of currently running replication streams",
		Measurement: "Replication Streams",
		Unit:        metric.Unit_COUNT,
	}

	metaReplicatedTimeSeconds = metric.Metadata{
		Name:        "physical_replication.replicated_time_seconds",
		Help:        "The replicated time of the physical replication stream in seconds since the unix epoch.",
		Measurement: "Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	// This metric would be 0 until cutover begins, and then it will be updated to
	// the total number of ranges that need to be reverted, and then gradually go
	// down to 0 again. NB: that the number of ranges is the total number of
	// ranges left to be reverted, but some may not have writes and therefore the
	// revert will be a no-op for those ranges.
	metaReplicationCutoverProgress = metric.Metadata{
		Name:        "physical_replication.failover_progress",
		Help:        "The number of ranges left to revert in order to complete an inflight cutover",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSQLReplanCount = metric.Metadata{
		Name:        "physical_replication.distsql_replan_count",
		Help:        "Total number of dist sql replanning events",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of stream ingestion jobs.
type Metrics struct {
	IngestedEvents             *metric.Counter
	IngestedLogicalBytes       *metric.Counter
	Flushes                    *metric.Counter
	ResolvedEvents             *metric.Counter
	ReplanCount                *metric.Counter
	FlushHistNanos             metric.IHistogram
	CommitLatency              metric.IHistogram
	AdmitLatency               metric.IHistogram
	RunningCount               *metric.Gauge
	ReplicatedTimeSeconds      *metric.Gauge
	ReplicationCutoverProgress *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for stream ingestion job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		IngestedEvents:       metric.NewCounter(metaReplicationEventsIngested),
		IngestedLogicalBytes: metric.NewCounter(metaReplicationIngestedBytes),
		Flushes:              metric.NewCounter(metaReplicationFlushes),
		ResolvedEvents:       metric.NewCounter(metaReplicationResolvedEventsIngested),
		ReplanCount:          metric.NewCounter(metaDistSQLReplanCount),
		FlushHistNanos: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaReplicationFlushHistNanos,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
			MaxVal:       streamingFlushHistMaxLatency.Nanoseconds(),
			SigFigs:      1,
		}),
		CommitLatency: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaReplicationCommitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
			MaxVal:       streamingCommitLatencyMaxValue.Nanoseconds(),
			SigFigs:      1,
		}),
		AdmitLatency: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaReplicationAdmitLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.BatchProcessLatencyBuckets,
			MaxVal:       streamingAdmitLatencyMaxValue.Nanoseconds(),
			SigFigs:      1,
		}),
		RunningCount:               metric.NewGauge(metaStreamsRunning),
		ReplicatedTimeSeconds:      metric.NewGauge(metaReplicatedTimeSeconds),
		ReplicationCutoverProgress: metric.NewGauge(metaReplicationCutoverProgress),
	}
	return m
}

func init() {
	jobs.MakeStreamIngestMetricsHook = MakeMetrics
}
