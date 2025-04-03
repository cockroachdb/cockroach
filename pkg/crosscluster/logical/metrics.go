// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	metaDLQedRowUpdates = metric.Metadata{
		Name:        "logical_replication.events_dlqed",
		Help:        "Row update events sent to DLQ",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaReceivedLogicalBytes = metric.Metadata{
		Name:        "logical_replication.logical_bytes",
		Help:        "Logical bytes (sum of keys + values) received by all replication jobs",
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
	metaRetryQueueBytes = metric.Metadata{
		Name:        "logical_replication.retry_queue_bytes",
		Help:        "Logical bytes (sum of keys+values) in the retry queue",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRetryQueueEvents = metric.Metadata{
		Name:        "logical_replication.retry_queue_events",
		Help:        "Row update events in the retry queue",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaApplyBatchNanosHist = metric.Metadata{
		Name:        "logical_replication.batch_hist_nanos",
		Help:        "Time spent per row flushing a batch",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaInitialApplySuccess = metric.Metadata{
		Name:        "logical_replication.events_initial_success",
		Help:        "Successful applications of an incoming row update",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaInitialApplyFailures = metric.Metadata{
		Name:        "logical_replication.events_initial_failure",
		Help:        "Failed attempts to apply an incoming row update",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaRetriedApplySuccesses = metric.Metadata{
		Name:        "logical_replication.events_retry_success",
		Help:        "Row update events applied after one or more retries",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaRetriedApplyFailures = metric.Metadata{
		Name:        "logical_replication.events_retry_failure",
		Help:        "Failed re-attempts to apply a row update",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}

	metaDLQedDueToAge = metric.Metadata{
		Name:        "logical_replication.events_dlqed_age",
		Help:        "Row update events sent to DLQ due to reaching the maximum time allowed in the retry queue",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaDLQedDueToQueueSpace = metric.Metadata{
		Name:        "logical_replication.events_dlqed_space",
		Help:        "Row update events sent to DLQ due to capacity of the retry queue",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaDLQedDueToErrType = metric.Metadata{
		Name:        "logical_replication.events_dlqed_errtype",
		Help:        "Row update events sent to DLQ due to an error not considered retryable",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}

	// Internal metrics.
	metaCheckpointEvents = metric.Metadata{
		Name:        "logical_replication.checkpoint_events_ingested",
		Help:        "Checkpoint events ingested by all replication jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSQLReplanCount = metric.Metadata{
		Name:        "logical_replication.replan_count",
		Help:        "Total number of dist sql replanning events",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaKVUpdateTooOld = metric.Metadata{
		Name:        "logical_replication.kv.update_too_old",
		Help:        "Total number of updates that were not applied because they were too old",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaKVValueRefreshes = metric.Metadata{
		Name:        "logical_replication.kv.value_refreshes",
		Help:        "Total number of batches that refreshed the previous value",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaScanningRanges = metric.Metadata{
		Name:        "logical_replication.scanning_ranges",
		Help:        "Source side ranges undergoing an initial scan (inaccurate with multiple LDR jobs)",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaCatchupRanges = metric.Metadata{
		Name:        "logical_replication.catchup_ranges",
		Help:        "Source side ranges undergoing catch up scans (inaccurate with multiple LDR jobs)",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}

	// Labeled metrics.
	metaLabeledReplicatedTime = metric.Metadata{
		Name:        "logical_replication.replicated_time_by_label",
		Help:        "Replicated time of the logical replication stream by label",
		Measurement: "Seconds",
		Unit:        metric.Unit_SECONDS,
	}
	metaLabeledEventsIngetsted = metric.Metadata{
		Name:        "logical_replication.events_ingested_by_label",
		Help:        "Events ingested by all replication jobs by label",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaLabeledEventsDLQed = metric.Metadata{
		Name:        "logical_replication.events_dlqed_by_label",
		Help:        "Row update events sent to DLQ by label",
		Measurement: "Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaLabeledScanningRanges = metric.Metadata{
		Name:        "logical_replication.scanning_ranges_by_label",
		Help:        "Source side ranges undergoing an initial scan",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaLabeledCatchupRanges = metric.Metadata{
		Name:        "logical_replication.catchup_ranges_by_label",
		Help:        "Source side ranges undergoing catch up scans",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of logical replication jobs.
type Metrics struct {
	// Top-line user-facing numbers that how many events and how much data are
	// bring moved and applied/rejected/etc.
	AppliedRowUpdates     *metric.Counter
	DLQedRowUpdates       *metric.Counter
	ReceivedLogicalBytes  *metric.Counter
	CommitToCommitLatency metric.IHistogram
	ReplicatedTimeSeconds *metric.Gauge

	// User-surfaced information about the health/operation of the stream; this
	// should be a narrow subset of numbers that are actually relevant to a user
	// such as the latency of application as that could be their supplied UDF.
	RetryQueueBytes     *metric.Gauge
	RetryQueueEvents    *metric.Gauge
	ApplyBatchNanosHist metric.IHistogram

	DLQedDueToAge        *metric.Counter
	DLQedDueToQueueSpace *metric.Counter
	DLQedDueToErrType    *metric.Counter

	InitialApplySuccesses *metric.Counter
	InitialApplyFailures  *metric.Counter
	RetriedApplySuccesses *metric.Counter
	RetriedApplyFailures  *metric.Counter

	ScanningRanges *metric.Gauge
	CatchupRanges  *metric.Gauge

	// Internal numbers that are useful for determining why a stream is behaving
	// a specific way.
	CheckpointEvents *metric.Counter
	ReplanCount      *metric.Counter
	KVValueRefreshes *metric.Counter
	KVUpdateTooOld   *metric.Counter

	// Labeled export-only metrics.
	LabeledReplicatedTime *metric.GaugeVec
	LabeledEventsIngested *metric.CounterVec
	LabeledEventsDLQed    *metric.CounterVec
	LabeledScanningRanges *metric.GaugeVec
	LabeledCatchupRanges  *metric.GaugeVec
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for logical replication job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	return &Metrics{
		AppliedRowUpdates:    metric.NewCounter(metaAppliedRowUpdates),
		DLQedRowUpdates:      metric.NewCounter(metaDLQedRowUpdates),
		ReceivedLogicalBytes: metric.NewCounter(metaReceivedLogicalBytes),
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
		RetryQueueBytes:      metric.NewGauge(metaRetryQueueBytes),
		RetryQueueEvents:     metric.NewGauge(metaRetryQueueEvents),
		DLQedDueToAge:        metric.NewCounter(metaDLQedDueToAge),
		DLQedDueToQueueSpace: metric.NewCounter(metaDLQedDueToQueueSpace),
		DLQedDueToErrType:    metric.NewCounter(metaDLQedDueToErrType),

		InitialApplySuccesses: metric.NewCounter(metaInitialApplySuccess),
		InitialApplyFailures:  metric.NewCounter(metaInitialApplyFailures),
		RetriedApplySuccesses: metric.NewCounter(metaRetriedApplySuccesses),
		RetriedApplyFailures:  metric.NewCounter(metaRetriedApplyFailures),

		CheckpointEvents: metric.NewCounter(metaCheckpointEvents),
		ReplanCount:      metric.NewCounter(metaDistSQLReplanCount),
		KVUpdateTooOld:   metric.NewCounter(metaKVUpdateTooOld),
		KVValueRefreshes: metric.NewCounter(metaKVValueRefreshes),

		ScanningRanges: metric.NewGauge(metaScanningRanges),
		CatchupRanges:  metric.NewGauge(metaCatchupRanges),

		// Labeled export-only metrics.
		LabeledReplicatedTime: metric.NewExportedGaugeVec(metaLabeledReplicatedTime, []string{"label"}),
		LabeledEventsIngested: metric.NewExportedCounterVec(metaLabeledEventsIngetsted, []string{"label"}),
		LabeledEventsDLQed:    metric.NewExportedCounterVec(metaLabeledEventsDLQed, []string{"label"}),
		LabeledScanningRanges: metric.NewExportedGaugeVec(metaLabeledScanningRanges, []string{"label"}),
		LabeledCatchupRanges:  metric.NewExportedGaugeVec(metaLabeledCatchupRanges, []string{"label"}),
	}
}
