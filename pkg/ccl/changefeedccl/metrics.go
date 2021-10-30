// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SinkMetrics are sink specific metrics.
type SinkMetrics struct {
	EmittedMessages *metric.Counter
	EmittedBytes    *metric.Counter
	FlushedBytes    *metric.Counter
	BatchHistNanos  *metric.Histogram
	Flushes         *metric.Counter
	FlushHistNanos  *metric.Histogram
	CommitLatency   *metric.Histogram
}

// MetricStruct implements metric.Struct interface.
func (m *SinkMetrics) MetricStruct() {}

// sinkMetrics annotates global SinkMetrics with per-changefeed information,
// such as the state of backfill.
type sinkMetrics struct {
	*SinkMetrics
	backfilling syncutil.AtomicBool
}

// sinkDoesNotCompress is a sentinel value indicating the the sink
// does not compress the data it emits.
const sinkDoesNotCompress = -1

type recordEmittedMessagesCallback func(numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int)

func (m *sinkMetrics) recordEmittedMessages() recordEmittedMessagesCallback {
	if m == nil {
		return func(numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int) {}
	}

	start := timeutil.Now()
	return func(numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int) {
		m.recordEmittedBatch(start, numMessages, mvcc, bytes, compressedBytes)
	}
}

func (m *sinkMetrics) recordEmittedBatch(
	startTime time.Time, numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int,
) {
	if m == nil {
		return
	}
	emitNanos := timeutil.Since(startTime).Nanoseconds()
	m.EmittedMessages.Inc(int64(numMessages))
	m.EmittedBytes.Inc(int64(bytes))
	if compressedBytes == sinkDoesNotCompress {
		compressedBytes = bytes
	}
	m.FlushedBytes.Inc(int64(compressedBytes))
	m.BatchHistNanos.RecordValue(emitNanos)
	if !m.backfilling.Get() {
		m.CommitLatency.RecordValue(timeutil.Since(mvcc.GoTime()).Nanoseconds())
	}
}

func (m *sinkMetrics) recordResolvedCallback() func() {
	if m == nil {
		return func() {}
	}

	start := timeutil.Now()
	return func() {
		emitNanos := timeutil.Since(start).Nanoseconds()
		m.EmittedMessages.Inc(1)
		m.BatchHistNanos.RecordValue(emitNanos)
	}
}

func (m *SinkMetrics) recordFlushRequestCallback() func() {
	if m == nil {
		return func() {}
	}

	start := timeutil.Now()
	return func() {
		flushNanos := timeutil.Since(start).Nanoseconds()
		m.Flushes.Inc(1)
		m.FlushHistNanos.RecordValue(flushNanos)
	}
}

const (
	changefeedCheckpointHistMaxLatency = 30 * time.Second
	changefeedEmitHistMaxLatency       = 30 * time.Second
	changefeedFlushHistMaxLatency      = 1 * time.Minute
	admitLatencyMaxValue               = 60 * time.Second
	commitLatencyMaxValue              = 10 * 60 * time.Second
)

var (
	metaChangefeedEmittedMessages = metric.Metadata{
		Name:        "changefeed.emitted_messages",
		Help:        "Messages emitted by all feeds",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedForwardedResolvedMessages = metric.Metadata{
		Name:        "changefeed.forwarded_resolved_messages",
		Help:        "Resolved timestamps forwarded from the change aggregator to the change frontier",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedEmittedBytes = metric.Metadata{
		Name:        "changefeed.emitted_bytes",
		Help:        "Bytes emitted by all feeds",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaChangefeedFlushedBytes = metric.Metadata{
		Name:        "changefeed.flushed_bytes",
		Help:        "Bytes emitted by all feeds; maybe different from changefeed.emitted_bytes when compression is enabled",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaChangefeedFlushes = metric.Metadata{
		Name:        "changefeed.flushes",
		Help:        "Total flushes across all feeds",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedErrorRetries = metric.Metadata{
		Name:        "changefeed.error_retries",
		Help:        "Total retryable errors encountered by all changefeeds",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedFailures = metric.Metadata{
		Name:        "changefeed.failures",
		Help:        "Total number of changefeed jobs which have failed",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}

	metaEventQueueTime = metric.Metadata{
		Name:        "changefeed.queue_time_nanos",
		Help:        "Time KV event spent waiting to be processed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaAdmitLatency = metric.Metadata{
		Name: "changefeed.admit_latency",
		Help: "Event admission latency: a difference between event MVCC timestamp " +
			"and the time it was admitted into changefeed pipeline; " +
			"Note: this metric includes the time spent waiting until event   can be processed due " +
			"to backpressure or time spent resolving schema descriptors. " +
			"Also note, this metric excludes latency during backfill",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCommitLatency = metric.Metadata{
		Name: "changefeed.commit_latency",
		Help: "Event commit latency: a difference between event MVCC timestamp " +
			"and the time it was acknowledged by the downstream sink.  If the sink batches events, " +
			" then the difference between the oldest event in the batch and acknowledgement is recorded; " +
			"Excludes latency during backfill",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedRunning = metric.Metadata{
		Name:        "changefeed.running",
		Help:        "Number of currently running changefeeds, including sinkless",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_COUNT,
	}

	metaChangefeedCheckpointHistNanos = metric.Metadata{
		Name:        "changefeed.checkpoint_hist_nanos",
		Help:        "Time spent checkpointing changefeed progress",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaChangefeedBatchHistNanos = metric.Metadata{
		Name:        "changefeed.sink_batch_hist_nanos",
		Help:        "Time spent batched in the sink buffer before being being flushed and acknowledged",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaChangefeedFlushHistNanos = metric.Metadata{
		Name:        "changefeed.flush_hist_nanos",
		Help:        "Time spent flushing messages across all changefeeds",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// TODO(dan): This was intended to be a measure of the minimum distance of
	// any changefeed ahead of its gc ttl threshold, but keeping that correct in
	// the face of changing zone configs is much harder, so this will have to do
	// for now.
	metaChangefeedMaxBehindNanos = metric.Metadata{
		Name:        "changefeed.max_behind_nanos",
		Help:        "Largest commit-to-emit duration of any running feed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaChangefeedFrontierUpdates = metric.Metadata{
		Name:        "changefeed.frontier_updates",
		Help:        "Number of change frontier updates across all feeds",
		Measurement: "Updates",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBackfillCount = metric.Metadata{
		Name:        "changefeed.backfill_count",
		Help:        "Number of changefeeds currently executing backfill",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of changefeeds.
type Metrics struct {
	*SinkMetrics
	KVFeedMetrics     kvevent.Metrics
	SchemaFeedMetrics schemafeed.Metrics

	ErrorRetries     *metric.Counter
	Failures         *metric.Counter
	ResolvedMessages *metric.Counter
	BackfillCount    *metric.Gauge

	QueueTimeNanos      *metric.Counter
	AdmitLatency        *metric.Histogram
	CheckpointHistNanos *metric.Histogram
	Running             *metric.Gauge

	FrontierUpdates *metric.Counter
	ThrottleMetrics cdcutils.Metrics

	mu struct {
		syncutil.Mutex
		id       int
		resolved map[int]hlc.Timestamp
	}
	MaxBehindNanos *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for changefeed monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		SinkMetrics: &SinkMetrics{
			EmittedMessages: metric.NewCounter(metaChangefeedEmittedMessages),
			EmittedBytes:    metric.NewCounter(metaChangefeedEmittedBytes),
			FlushedBytes:    metric.NewCounter(metaChangefeedFlushedBytes),
			Flushes:         metric.NewCounter(metaChangefeedFlushes),

			BatchHistNanos: metric.NewHistogram(metaChangefeedBatchHistNanos, histogramWindow,
				changefeedEmitHistMaxLatency.Nanoseconds(), 1),
			FlushHistNanos: metric.NewHistogram(metaChangefeedFlushHistNanos, histogramWindow,
				changefeedFlushHistMaxLatency.Nanoseconds(), 1),

			CommitLatency: metric.NewHistogram(metaCommitLatency, histogramWindow,
				commitLatencyMaxValue.Nanoseconds(), 1),
		},

		KVFeedMetrics:     kvevent.MakeMetrics(histogramWindow),
		SchemaFeedMetrics: schemafeed.MakeMetrics(histogramWindow),
		ErrorRetries:      metric.NewCounter(metaChangefeedErrorRetries),
		ResolvedMessages:  metric.NewCounter(metaChangefeedForwardedResolvedMessages),
		Failures:          metric.NewCounter(metaChangefeedFailures),
		QueueTimeNanos:    metric.NewCounter(metaEventQueueTime),
		AdmitLatency: metric.NewHistogram(metaAdmitLatency, histogramWindow,
			admitLatencyMaxValue.Nanoseconds(), 1),

		CheckpointHistNanos: metric.NewHistogram(metaChangefeedCheckpointHistNanos, histogramWindow,
			changefeedCheckpointHistMaxLatency.Nanoseconds(), 2),

		BackfillCount:   metric.NewGauge(metaChangefeedBackfillCount),
		Running:         metric.NewGauge(metaChangefeedRunning),
		FrontierUpdates: metric.NewCounter(metaChangefeedFrontierUpdates),
		ThrottleMetrics: cdcutils.MakeMetrics(histogramWindow),
	}

	m.mu.resolved = make(map[int]hlc.Timestamp)
	m.mu.id = 1 // start the first id at 1 so we can detect initialization
	m.MaxBehindNanos = metric.NewFunctionalGauge(metaChangefeedMaxBehindNanos, func() int64 {
		now := timeutil.Now()
		var maxBehind time.Duration
		m.mu.Lock()
		for _, resolved := range m.mu.resolved {
			if behind := now.Sub(resolved.GoTime()); behind > maxBehind {
				maxBehind = behind
			}
		}
		m.mu.Unlock()
		return maxBehind.Nanoseconds()
	})
	return m
}

func init() {
	jobs.MakeChangefeedMetricsHook = MakeMetrics
}
