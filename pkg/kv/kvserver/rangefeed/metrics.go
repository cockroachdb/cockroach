// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaRangeFeedCatchUpScanNanos = metric.Metadata{
		Name:        "kv.rangefeed.catchup_scan_nanos",
		Help:        "Time spent in RangeFeed catchup scan",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRangeFeedExhausted = metric.Metadata{
		Name:        "kv.rangefeed.budget_allocation_failed",
		Help:        "Number of times RangeFeed failed because memory budget was exceeded",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedBudgetBlocked = metric.Metadata{
		Name:        "kv.rangefeed.budget_allocation_blocked",
		Help:        "Number of times RangeFeed waited for budget availability",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedRegistrations = metric.Metadata{
		Name:        "kv.rangefeed.registrations",
		Help:        "Number of active RangeFeed registrations",
		Measurement: "Registrations",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedClosedTimestampMaxBehindNanos = metric.Metadata{
		Name: "kv.rangefeed.closed_timestamp_max_behind_nanos",
		Help: "Largest latency between realtime and replica max closed timestamp for replicas " +
			"that have active rangeeds on them",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRangefeedSlowClosedTimestampRanges = metric.Metadata{
		Name: "kv.rangefeed.closed_timestamp.slow_ranges",
		Help: "Number of ranges that have a closed timestamp lagging by more than 5x target lag. " +
			"Periodically re-calculated",
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedSlowClosedTimestampCancelledRanges = metric.Metadata{
		Name: "kv.rangefeed.closed_timestamp.slow_ranges.cancelled",
		Help: "Number of rangefeeds that were cancelled due to a chronically " +
			"lagging closed timestamp",
		Measurement: "Cancellation Count",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedProcessorsGO = metric.Metadata{
		Name:        "kv.rangefeed.processors_goroutine",
		Help:        "Number of active RangeFeed processors using goroutines",
		Measurement: "Processors",
		Unit:        metric.Unit_COUNT,
	}
	metaRangeFeedProcessorsScheduler = metric.Metadata{
		Name:        "kv.rangefeed.processors_scheduler",
		Help:        "Number of active RangeFeed processors using scheduler",
		Measurement: "Processors",
		Unit:        metric.Unit_COUNT,
	}
	metaQueueTimeHistogramsTemplate = metric.Metadata{
		Name:        "kv.rangefeed.scheduler.%s.latency",
		Help:        "KV RangeFeed %s scheduler latency",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaQueueSizeTemplate = metric.Metadata{
		Name:        "kv.rangefeed.scheduler.%s.queue_size",
		Help:        "Number of entries in the KV RangeFeed %s scheduler queue",
		Measurement: "Pending Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaQueueTimeout = metric.Metadata{
		Name:        "kv.rangefeed.scheduled_processor.queue_timeout",
		Help:        "Number of times the RangeFeed processor shutdown because of a queue send timeout",
		Measurement: "Failure Count",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of RangeFeeds.
type Metrics struct {
	RangeFeedCatchUpScanNanos                   *metric.Counter
	RangeFeedBudgetExhausted                    *metric.Counter
	RangefeedProcessorQueueTimeout              *metric.Counter
	RangeFeedBudgetBlocked                      *metric.Counter
	RangeFeedSlowClosedTimestampCancelledRanges *metric.Counter
	RangeFeedRegistrations                      *metric.Gauge
	RangeFeedClosedTimestampMaxBehindNanos      *metric.Gauge
	RangeFeedSlowClosedTimestampRanges          *metric.Gauge
	RangeFeedSlowClosedTimestampLogN            log.EveryN
	// RangeFeedSlowClosedTimestampNudgeSem bounds the amount of work that can be
	// spun up on behalf of the RangeFeed nudger. We don't expect to hit this
	// limit, but it's here to limit the effect on stability in case something
	// unexpected happens.
	RangeFeedSlowClosedTimestampNudgeSem chan struct{}
	// Metrics exposing rangefeed processor by type. Those metrics are used to
	// monitor processor switch over. They could be removed when legacy processor
	// is removed.
	RangeFeedProcessorsGO        *metric.Gauge
	RangeFeedProcessorsScheduler *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// NewMetrics makes the metrics for RangeFeeds monitoring.
func NewMetrics() *Metrics {
	return &Metrics{
		RangeFeedCatchUpScanNanos:                   metric.NewCounter(metaRangeFeedCatchUpScanNanos),
		RangefeedProcessorQueueTimeout:              metric.NewCounter(metaQueueTimeout),
		RangeFeedBudgetExhausted:                    metric.NewCounter(metaRangeFeedExhausted),
		RangeFeedBudgetBlocked:                      metric.NewCounter(metaRangeFeedBudgetBlocked),
		RangeFeedSlowClosedTimestampCancelledRanges: metric.NewCounter(metaRangeFeedSlowClosedTimestampCancelledRanges),
		RangeFeedRegistrations:                      metric.NewGauge(metaRangeFeedRegistrations),
		RangeFeedClosedTimestampMaxBehindNanos:      metric.NewGauge(metaRangeFeedClosedTimestampMaxBehindNanos),
		RangeFeedSlowClosedTimestampRanges:          metric.NewGauge(metaRangefeedSlowClosedTimestampRanges),
		RangeFeedSlowClosedTimestampLogN:            log.Every(5 * time.Second),
		RangeFeedSlowClosedTimestampNudgeSem:        make(chan struct{}, 1024),
		RangeFeedProcessorsGO:                       metric.NewGauge(metaRangeFeedProcessorsGO),
		RangeFeedProcessorsScheduler:                metric.NewGauge(metaRangeFeedProcessorsScheduler),
	}
}

// FeedBudgetPoolMetrics holds metrics for RangeFeed budgets for the purpose
// or registration in a metric registry.
type FeedBudgetPoolMetrics struct {
	SystemBytesCount *metric.Gauge
	SharedBytesCount *metric.Gauge
}

// MetricStruct implements metrics.Struct interface.
func (FeedBudgetPoolMetrics) MetricStruct() {}

// NewFeedBudgetMetrics creates new metrics for RangeFeed budgets.
func NewFeedBudgetMetrics(histogramWindow time.Duration) *FeedBudgetPoolMetrics {
	makeMemMetricMetadata := func(name, help string) metric.Metadata {
		return metric.Metadata{
			Name:        "kv.rangefeed.mem_" + name,
			Help:        help,
			Measurement: "Memory",
			Unit:        metric.Unit_BYTES,
		}
	}

	return &FeedBudgetPoolMetrics{
		SystemBytesCount: metric.NewGauge(makeMemMetricMetadata("system",
			"Memory usage by rangefeeds on system ranges")),
		SharedBytesCount: metric.NewGauge(makeMemMetricMetadata("shared",
			"Memory usage by rangefeeds")),
	}
}

// ShardMetrics metrics for individual scheduler shard.
type ShardMetrics struct {
	// QueueTime is time spent by range in scheduler queue.
	QueueTime metric.IHistogram
	// QueueSize is number of elements in the queue recently observed by reader.
	QueueSize *metric.Gauge
}

// MetricStruct implements metrics.Struct interface.
func (*ShardMetrics) MetricStruct() {}

// SchedulerMetrics for production monitoring of rangefeed Scheduler.
type SchedulerMetrics struct {
	SystemPriority *ShardMetrics
	NormalPriority *ShardMetrics
}

// MetricStruct implements metrics.Struct interface.
func (*SchedulerMetrics) MetricStruct() {}

// NewSchedulerMetrics creates metric struct for Scheduler.
func NewSchedulerMetrics(histogramWindow time.Duration) *SchedulerMetrics {
	return &SchedulerMetrics{
		SystemPriority: newSchedulerShardMetrics("system", histogramWindow),
		NormalPriority: newSchedulerShardMetrics("normal", histogramWindow),
	}
}

func newSchedulerShardMetrics(name string, histogramWindow time.Duration) *ShardMetrics {
	expandTemplate := func(template metric.Metadata) metric.Metadata {
		result := template
		result.Name = fmt.Sprintf(template.Name, name)
		result.Help = fmt.Sprintf(template.Help, name)
		return result
	}
	return &ShardMetrics{
		QueueTime: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     expandTemplate(metaQueueTimeHistogramsTemplate),
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		QueueSize: metric.NewGauge(expandTemplate(metaQueueSizeTemplate)),
	}
}
