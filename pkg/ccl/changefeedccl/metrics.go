// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// allow creation of per changefeed SLI metrics.
var enableSLIMetrics = envutil.EnvOrDefaultBool(
	"COCKROACH_EXPERIMENTAL_ENABLE_PER_CHANGEFEED_METRICS", false)

// max length for the scope name.
const maxSLIScopeNameLen = 128

// defaultSLIScope is the name of the default SLI scope -- i.e. the set of metrics
// keeping track of all changefeeds which did not have explicit sli scope specified.
const defaultSLIScope = "default"

// AggMetrics are aggregated metrics keeping track of aggregated changefeed performance
// indicators, combined with a limited number of per-changefeed indicators.
type AggMetrics struct {
	EmittedMessages       *aggmetric.AggCounter
	EmittedBytes          *aggmetric.AggCounter
	FlushedBytes          *aggmetric.AggCounter
	BatchHistNanos        *aggmetric.AggHistogram
	Flushes               *aggmetric.AggCounter
	FlushHistNanos        *aggmetric.AggHistogram
	CommitLatency         *aggmetric.AggHistogram
	BackfillCount         *aggmetric.AggGauge
	BackfillPendingRanges *aggmetric.AggGauge
	ErrorRetries          *aggmetric.AggCounter
	AdmitLatency          *aggmetric.AggHistogram
	RunningCount          *aggmetric.AggGauge

	// There is always at least 1 sliMetrics created for defaultSLI scope.
	mu struct {
		syncutil.Mutex
		sliMetrics map[string]*sliMetrics
	}
}

// MetricStruct implements metric.Struct interface.
func (a *AggMetrics) MetricStruct() {}

// sliMetrics holds all SLI related metrics aggregated into AggMetrics.
type sliMetrics struct {
	EmittedMessages       *aggmetric.Counter
	EmittedBytes          *aggmetric.Counter
	FlushedBytes          *aggmetric.Counter
	BatchHistNanos        *aggmetric.Histogram
	Flushes               *aggmetric.Counter
	FlushHistNanos        *aggmetric.Histogram
	CommitLatency         *aggmetric.Histogram
	ErrorRetries          *aggmetric.Counter
	AdmitLatency          *aggmetric.Histogram
	BackfillCount         *aggmetric.Gauge
	BackfillPendingRanges *aggmetric.Gauge
	RunningCount          *aggmetric.Gauge
}

// sinkDoesNotCompress is a sentinel value indicating the sink
// does not compress the data it emits.
const sinkDoesNotCompress = -1

type recordEmittedMessagesCallback func(numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int)

func (m *sliMetrics) recordEmittedMessages() recordEmittedMessagesCallback {
	if m == nil {
		return func(numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int) {}
	}

	start := timeutil.Now()
	return func(numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int) {
		m.recordEmittedBatch(start, numMessages, mvcc, bytes, compressedBytes)
	}
}

func (m *sliMetrics) recordEmittedBatch(
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
	if m.BackfillCount.Value() == 0 {
		m.CommitLatency.RecordValue(timeutil.Since(mvcc.GoTime()).Nanoseconds())
	}
}

func (m *sliMetrics) recordResolvedCallback() func() {
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

func (m *sliMetrics) recordFlushRequestCallback() func() {
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

func (m *sliMetrics) getBackfillCallback() func() func() {
	return func() func() {
		m.BackfillCount.Inc(1)
		return func() {
			m.BackfillCount.Dec(1)
		}
	}
}

// getBackfillRangeCallback returns a backfillRangeCallback that is to be called
// at the beginning of a backfill with the number of ranges that will be scanned
// and returns a two callbacks to decrement the value until all ranges have
// been emitted or clear the number completely if the backfill is cancelled.
// Note: dec() should only be called as many times as the initial value, and
// clear() should be called when there will never be another dec() call.
func (m *sliMetrics) getBackfillRangeCallback() func(int64) (func(), func()) {
	return func(initial int64) (dec func(), clear func()) {
		remaining := initial
		m.BackfillPendingRanges.Inc(initial)
		dec = func() {
			m.BackfillPendingRanges.Dec(1)
			atomic.AddInt64(&remaining, -1)
		}
		clear = func() {
			m.BackfillPendingRanges.Dec(remaining)
			atomic.AddInt64(&remaining, -remaining)
		}
		return
	}
}

const (
	changefeedCheckpointHistMaxLatency = 30 * time.Second
	changefeedBatchHistMaxLatency      = 30 * time.Second
	changefeedFlushHistMaxLatency      = 1 * time.Minute
	admitLatencyMaxValue               = 1 * time.Minute
	commitLatencyMaxValue              = 10 * time.Minute
)

var (
	metaChangefeedForwardedResolvedMessages = metric.Metadata{
		Name:        "changefeed.forwarded_resolved_messages",
		Help:        "Resolved timestamps forwarded from the change aggregator to the change frontier",
		Measurement: "Messages",
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

	metaChangefeedCheckpointHistNanos = metric.Metadata{
		Name:        "changefeed.checkpoint_hist_nanos",
		Help:        "Time spent checkpointing changefeed progress",
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
)

func newAggregateMetrics(histogramWindow time.Duration) *AggMetrics {
	metaChangefeedEmittedMessages := metric.Metadata{
		Name:        "changefeed.emitted_messages",
		Help:        "Messages emitted by all feeds",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedEmittedBytes := metric.Metadata{
		Name:        "changefeed.emitted_bytes",
		Help:        "Bytes emitted by all feeds",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaChangefeedFlushedBytes := metric.Metadata{
		Name:        "changefeed.flushed_bytes",
		Help:        "Bytes emitted by all feeds; maybe different from changefeed.emitted_bytes when compression is enabled",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaChangefeedFlushes := metric.Metadata{
		Name:        "changefeed.flushes",
		Help:        "Total flushes across all feeds",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBatchHistNanos := metric.Metadata{
		Name:        "changefeed.sink_batch_hist_nanos",
		Help:        "Time spent batched in the sink buffer before being flushed and acknowledged",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedFlushHistNanos := metric.Metadata{
		Name:        "changefeed.flush_hist_nanos",
		Help:        "Time spent flushing messages across all changefeeds",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCommitLatency := metric.Metadata{
		Name: "changefeed.commit_latency",
		Help: "Event commit latency: a difference between event MVCC timestamp " +
			"and the time it was acknowledged by the downstream sink.  If the sink batches events, " +
			" then the difference between the oldest event in the batch and acknowledgement is recorded; " +
			"Excludes latency during backfill",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaAdmitLatency := metric.Metadata{
		Name: "changefeed.admit_latency",
		Help: "Event admission latency: a difference between event MVCC timestamp " +
			"and the time it was admitted into changefeed pipeline; " +
			"Note: this metric includes the time spent waiting until event can be processed due " +
			"to backpressure or time spent resolving schema descriptors. " +
			"Also note, this metric excludes latency during backfill",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedBackfillCount := metric.Metadata{
		Name:        "changefeed.backfill_count",
		Help:        "Number of changefeeds currently executing backfill",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBackfillPendingRanges := metric.Metadata{
		Name:        "changefeed.backfill_pending_ranges",
		Help:        "Number of ranges in an ongoing backfill that are yet to be fully emitted",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedRunning := metric.Metadata{
		Name:        "changefeed.running",
		Help:        "Number of currently running changefeeds, including sinkless",
		Measurement: "Changefeeds",
		Unit:        metric.Unit_COUNT,
	}

	// NB: When adding new histograms, use sigFigs = 1.  Older histograms
	// retain significant figures of 2.
	b := aggmetric.MakeBuilder("scope")
	a := &AggMetrics{
		ErrorRetries:    b.Counter(metaChangefeedErrorRetries),
		EmittedMessages: b.Counter(metaChangefeedEmittedMessages),
		EmittedBytes:    b.Counter(metaChangefeedEmittedBytes),
		FlushedBytes:    b.Counter(metaChangefeedFlushedBytes),
		Flushes:         b.Counter(metaChangefeedFlushes),

		BatchHistNanos: b.Histogram(metaChangefeedBatchHistNanos,
			histogramWindow, changefeedBatchHistMaxLatency.Nanoseconds(), 1),
		FlushHistNanos: b.Histogram(metaChangefeedFlushHistNanos,
			histogramWindow, changefeedFlushHistMaxLatency.Nanoseconds(), 2),
		CommitLatency: b.Histogram(metaCommitLatency,
			histogramWindow, commitLatencyMaxValue.Nanoseconds(), 1),
		AdmitLatency: b.Histogram(metaAdmitLatency, histogramWindow,
			admitLatencyMaxValue.Nanoseconds(), 1),
		BackfillCount:         b.Gauge(metaChangefeedBackfillCount),
		BackfillPendingRanges: b.Gauge(metaChangefeedBackfillPendingRanges),
		RunningCount:          b.Gauge(metaChangefeedRunning),
	}
	a.mu.sliMetrics = make(map[string]*sliMetrics)
	_, err := a.getOrCreateScope(defaultSLIScope)
	if err != nil {
		// defaultSLIScope must always exist.
		panic(err)
	}
	return a
}

func (a *AggMetrics) getOrCreateScope(scope string) (*sliMetrics, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	scope = strings.TrimSpace(strings.ToLower(scope))

	if scope == "" {
		scope = defaultSLIScope
	}

	if len(scope) > maxSLIScopeNameLen {
		return nil, pgerror.Newf(pgcode.ConfigurationLimitExceeded,
			"scope name length must be less than %d bytes", maxSLIScopeNameLen)
	}

	if s, ok := a.mu.sliMetrics[scope]; ok {
		return s, nil
	}

	if scope != defaultSLIScope {
		if !enableSLIMetrics {
			return nil, errors.WithHint(
				pgerror.Newf(pgcode.ConfigurationLimitExceeded, "cannot create metrics scope %q", scope),
				"try restarting with COCKROACH_EXPERIMENTAL_ENABLE_PER_CHANGEFEED_METRICS=true",
			)
		}
		const failSafeMax = 1024
		if len(a.mu.sliMetrics) == failSafeMax {
			return nil, pgerror.Newf(pgcode.ConfigurationLimitExceeded,
				"too many metrics labels; max %d", failSafeMax)
		}
	}

	sm := &sliMetrics{
		EmittedMessages:       a.EmittedMessages.AddChild(scope),
		EmittedBytes:          a.EmittedBytes.AddChild(scope),
		FlushedBytes:          a.FlushedBytes.AddChild(scope),
		BatchHistNanos:        a.BatchHistNanos.AddChild(scope),
		Flushes:               a.Flushes.AddChild(scope),
		FlushHistNanos:        a.FlushHistNanos.AddChild(scope),
		CommitLatency:         a.CommitLatency.AddChild(scope),
		ErrorRetries:          a.ErrorRetries.AddChild(scope),
		AdmitLatency:          a.AdmitLatency.AddChild(scope),
		BackfillCount:         a.BackfillCount.AddChild(scope),
		BackfillPendingRanges: a.BackfillPendingRanges.AddChild(scope),
		RunningCount:          a.RunningCount.AddChild(scope),
	}

	a.mu.sliMetrics[scope] = sm
	return sm, nil
}

// Metrics are for production monitoring of changefeeds.
type Metrics struct {
	AggMetrics          *AggMetrics
	KVFeedMetrics       kvevent.Metrics
	SchemaFeedMetrics   schemafeed.Metrics
	Failures            *metric.Counter
	ResolvedMessages    *metric.Counter
	QueueTimeNanos      *metric.Counter
	CheckpointHistNanos *metric.Histogram
	FrontierUpdates     *metric.Counter
	ThrottleMetrics     cdcutils.Metrics

	mu struct {
		syncutil.Mutex
		id       int
		resolved map[int]hlc.Timestamp
	}
	MaxBehindNanos *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// getSLIMetrics retursn SLIMeterics associated with the specified scope.
func (m *Metrics) getSLIMetrics(scope string) (*sliMetrics, error) {
	return m.AggMetrics.getOrCreateScope(scope)
}

// MakeMetrics makes the metrics for changefeed monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		AggMetrics:        newAggregateMetrics(histogramWindow),
		KVFeedMetrics:     kvevent.MakeMetrics(histogramWindow),
		SchemaFeedMetrics: schemafeed.MakeMetrics(histogramWindow),
		ResolvedMessages:  metric.NewCounter(metaChangefeedForwardedResolvedMessages),
		Failures:          metric.NewCounter(metaChangefeedFailures),
		QueueTimeNanos:    metric.NewCounter(metaEventQueueTime),
		CheckpointHistNanos: metric.NewHistogram(metaChangefeedCheckpointHistNanos, histogramWindow,
			changefeedCheckpointHistMaxLatency.Nanoseconds(), 2),
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
