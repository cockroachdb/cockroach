// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type metricsSink struct {
	metrics *Metrics
	wrapped Sink
}

func makeMetricsSink(metrics *Metrics, s Sink) *metricsSink {
	m := &metricsSink{
		metrics: metrics,
		wrapped: s,
	}
	return m
}

// EmitRow implements Sink interface.
func (s *metricsSink) EmitRow(
	ctx context.Context, topic TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	start := timeutil.Now()
	err := s.wrapped.EmitRow(ctx, topic, key, value, updated)
	if err == nil {
		emitNanos := timeutil.Since(start).Nanoseconds()
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(key) + len(value)))
		s.metrics.EmitNanos.Inc(emitNanos)
		s.metrics.EmitHistNanos.RecordValue(emitNanos)
	}
	return err
}

// EmitResolvedTimestamp implements Sink interface.
func (s *metricsSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	start := timeutil.Now()
	err := s.wrapped.EmitResolvedTimestamp(ctx, encoder, resolved)
	if err == nil {
		emitNanos := timeutil.Since(start).Nanoseconds()
		s.metrics.EmittedMessages.Inc(1)
		// TODO(dan): This wasn't correct. The wrapped sink may emit the payload
		// any number of times.
		// s.metrics.EmittedBytes.Inc(int64(len(payload)))
		s.metrics.EmitNanos.Inc(emitNanos)
		s.metrics.EmitHistNanos.RecordValue(emitNanos)
	}
	return err
}

// Flush implements Sink interface.
func (s *metricsSink) Flush(ctx context.Context) error {
	start := timeutil.Now()
	err := s.wrapped.Flush(ctx)
	if err == nil {
		flushNanos := timeutil.Since(start).Nanoseconds()
		s.metrics.Flushes.Inc(1)
		s.metrics.FlushNanos.Inc(flushNanos)
		s.metrics.FlushHistNanos.RecordValue(flushNanos)
	}

	return err
}

// Close implements Sink interface.
func (s *metricsSink) Close() error {
	return s.wrapped.Close()
}

// Dial implements Sink interface.
func (s *metricsSink) Dial() error {
	return s.wrapped.Dial()
}

const (
	changefeedCheckpointHistMaxLatency = 30 * time.Second
	changefeedEmitHistMaxLatency       = 30 * time.Second
	changefeedFlushHistMaxLatency      = 1 * time.Minute
)

var (
	metaChangefeedEmittedMessages = metric.Metadata{
		Name:        "changefeed.emitted_messages",
		Help:        "Messages emitted by all feeds",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedEmittedBytes = metric.Metadata{
		Name:        "changefeed.emitted_bytes",
		Help:        "Bytes emitted by all feeds",
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

	metaChangefeedProcessingNanos = metric.Metadata{
		Name:        "changefeed.processing_nanos",
		Help:        "Time spent processing KV changes into SQL rows",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedEmitNanos = metric.Metadata{
		Name:        "changefeed.emit_nanos",
		Help:        "Total time spent emitting all feeds",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedFlushNanos = metric.Metadata{
		Name:        "changefeed.flush_nanos",
		Help:        "Total time spent flushing all feeds",
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

	// emit_hist_nanos and flush_hist_nanos duplicate information
	// in emit_nanos, emitted_messages, and flush_nanos,
	// flushes. While all of those could be reconstructed from
	// information in the histogram, We've kept the older metrics
	// to avoid breaking historical timeseries data.
	metaChangefeedEmitHistNanos = metric.Metadata{
		Name:        "changefeed.emit_hist_nanos",
		Help:        "Time spent emitting messages across all changefeeds",
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
)

// Metrics are for production monitoring of changefeeds.
type Metrics struct {
	KVFeedMetrics     kvevent.Metrics
	SchemaFeedMetrics schemafeed.Metrics

	EmittedMessages *metric.Counter
	EmittedBytes    *metric.Counter
	Flushes         *metric.Counter
	ErrorRetries    *metric.Counter
	Failures        *metric.Counter

	ProcessingNanos *metric.Counter
	EmitNanos       *metric.Counter
	FlushNanos      *metric.Counter

	CheckpointHistNanos *metric.Histogram
	EmitHistNanos       *metric.Histogram
	FlushHistNanos      *metric.Histogram

	Running *metric.Gauge

	FrontierUpdates *metric.Counter

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
		KVFeedMetrics:     kvevent.MakeMetrics(histogramWindow),
		SchemaFeedMetrics: schemafeed.MakeMetrics(histogramWindow),
		EmittedMessages:   metric.NewCounter(metaChangefeedEmittedMessages),
		EmittedBytes:      metric.NewCounter(metaChangefeedEmittedBytes),
		Flushes:           metric.NewCounter(metaChangefeedFlushes),
		ErrorRetries:      metric.NewCounter(metaChangefeedErrorRetries),
		Failures:          metric.NewCounter(metaChangefeedFailures),

		ProcessingNanos: metric.NewCounter(metaChangefeedProcessingNanos),
		EmitNanos:       metric.NewCounter(metaChangefeedEmitNanos),
		FlushNanos:      metric.NewCounter(metaChangefeedFlushNanos),

		CheckpointHistNanos: metric.NewHistogram(metaChangefeedCheckpointHistNanos, histogramWindow,
			changefeedCheckpointHistMaxLatency.Nanoseconds(), 2),
		EmitHistNanos: metric.NewHistogram(metaChangefeedEmitHistNanos, histogramWindow,
			changefeedEmitHistMaxLatency.Nanoseconds(), 2),
		FlushHistNanos: metric.NewHistogram(metaChangefeedFlushHistNanos, histogramWindow,
			changefeedFlushHistMaxLatency.Nanoseconds(), 2),

		Running:         metric.NewGauge(metaChangefeedRunning),
		FrontierUpdates: metric.NewCounter(metaChangefeedFrontierUpdates),
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
