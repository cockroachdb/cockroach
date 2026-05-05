// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

const (
	streamingFlushHistMaxLatency   = 1 * time.Minute
	streamingAdmitLatencyMaxValue  = 3 * time.Minute
	streamingCommitLatencyMaxValue = 10 * time.Minute

	streamingSourceTenantLabelName      = "source_tenant_name"
	streamingDestinationTenantLabelName = "destination_tenant_name"
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
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_CROSS_CLUSTER_REPLICATION,
		Unit:        metric.Unit_BYTES,
		HowToUse:    "Track PCR throughput",
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
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_CROSS_CLUSTER_REPLICATION,
		Unit:        metric.Unit_SECONDS,
		HowToUse:    "Track replication lag via current time - physical_replication.replicated_time_seconds",
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

	metaScanningRanges = metric.Metadata{
		Name:        "physical_replication.scanning_ranges",
		Help:        "Source side ranges undergoing an initial scan",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaCatchupRanges = metric.Metadata{
		Name:        "physical_replication.catchup_ranges",
		Help:        "Source side ranges undergoing catch up scans",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaReceiveWaitNanos = metric.Metadata{
		Name:        "physical_replication.receive_wait_nanos",
		Help:        "Cumulative time spent waiting to receive events from producer; use rate() to compare against flush_wait_nanos",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaFlushWaitNanos = metric.Metadata{
		Name:        "physical_replication.flush_wait_nanos",
		Help:        "Cumulative time spent waiting to send buffer to flush loop; use rate() to compare against receive_wait_nanos",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaClusterReplicatedTimeSeconds = metric.Metadata{
		Name:        "physical_replication.cluster.replicated_time_seconds",
		Help:        "The replicated time of the physical replication stream in seconds since the unix epoch.",
		Measurement: "Seconds",
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_CROSS_CLUSTER_REPLICATION,
		Unit:        metric.Unit_SECONDS,
		HowToUse:    "Track replication lag via current time - physical_replication.cluster.replicated_time_seconds",
	}
)

// Metrics holds per-node metrics for stream ingestion jobs.
type Metrics struct {
	FlushHistNanos metric.IHistogram
	CommitLatency  metric.IHistogram
	AdmitLatency   metric.IHistogram

	IngestedEvents       *metric.Counter
	ResolvedEvents       *metric.Counter
	IngestedLogicalBytes *metric.Counter
	Flushes              *metric.Counter
	ReceiveWaitNanos     *metric.Counter
	FlushWaitNanos       *metric.Counter

	// ReplicatedTimeSeconds is a per-node variant of the identical field in ClusterMetrics,
	// kept here to maintain backwards compatability with existing customer monitoring stacks.
	// This field could likely be removed in the future.
	ReplicatedTimeSeconds *metric.Gauge
}

// ClusterMetrics holds per-cluster metrics for stream ingestion jobs.
type ClusterMetrics struct {
	ReplanCount                *clustermetrics.CounterVec
	RunningCount               *clustermetrics.GaugeVec
	ReplicatedTimeSeconds      *clustermetrics.GaugeVec
	ScanningRanges             *clustermetrics.GaugeVec
	CatchupRanges              *clustermetrics.GaugeVec
	ReplicationCutoverProgress *clustermetrics.GaugeVec
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MetricStruct implements the metric.Struct interface.
func (*ClusterMetrics) MetricStruct() {}

// labeledClusterMetrics pairs a ClusterMetrics with precomputed labels
// for a specific ingest job.
type labeledClusterMetrics struct {
	*ClusterMetrics
	labels map[string]string
}

// MakeMetrics constructs the per-node metrics for stream ingestion.
func MakeMetrics() *Metrics {
	histogramWindow := base.DefaultHistogramWindowInterval()
	return &Metrics{
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

		IngestedEvents:       metric.NewCounter(metaReplicationEventsIngested),
		ResolvedEvents:       metric.NewCounter(metaReplicationResolvedEventsIngested),
		IngestedLogicalBytes: metric.NewCounter(metaReplicationIngestedBytes),
		Flushes:              metric.NewCounter(metaReplicationFlushes),
		ReceiveWaitNanos:     metric.NewCounter(metaReceiveWaitNanos),
		FlushWaitNanos:       metric.NewCounter(metaFlushWaitNanos),

		ReplicatedTimeSeconds: metric.NewGauge(metaReplicatedTimeSeconds),
	}
}

// MakeClusterMetrics constructs the per-cluster metrics for stream
// ingestion. Must be called from an init() function: clustermetrics
// constructors panic in test builds when called outside init.
func MakeClusterMetrics() *ClusterMetrics {
	return &ClusterMetrics{
		ReplanCount: clustermetrics.NewCounterVec(
			metaDistSQLReplanCount,
			streamingSourceTenantLabelName, streamingDestinationTenantLabelName,
		),
		RunningCount: clustermetrics.NewGaugeVec(
			metaStreamsRunning,
			streamingSourceTenantLabelName, streamingDestinationTenantLabelName,
		),
		ReplicatedTimeSeconds: clustermetrics.NewGaugeVec(
			metaClusterReplicatedTimeSeconds,
			streamingSourceTenantLabelName, streamingDestinationTenantLabelName,
		),
		ScanningRanges: clustermetrics.NewGaugeVec(
			metaScanningRanges,
			streamingSourceTenantLabelName, streamingDestinationTenantLabelName,
		),
		CatchupRanges: clustermetrics.NewGaugeVec(
			metaCatchupRanges,
			streamingSourceTenantLabelName, streamingDestinationTenantLabelName,
		),
		ReplicationCutoverProgress: clustermetrics.NewGaugeVec(
			metaReplicationCutoverProgress,
			streamingSourceTenantLabelName, streamingDestinationTenantLabelName,
		),
	}
}
