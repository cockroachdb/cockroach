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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

func (s *metricsSink) EmitRow(
	ctx context.Context, table *sqlbase.TableDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	start := timeutil.Now()
	err := s.wrapped.EmitRow(ctx, table, key, value, updated)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(key) + len(value)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	start := timeutil.Now()
	err := s.wrapped.EmitResolvedTimestamp(ctx, encoder, resolved)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		// TODO(dan): This wasn't correct. The wrapped sink may emit the payload
		// any number of times.
		// s.metrics.EmittedBytes.Inc(int64(len(payload)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) Flush(ctx context.Context) error {
	start := timeutil.Now()
	err := s.wrapped.Flush(ctx)
	if err == nil {
		s.metrics.Flushes.Inc(1)
		s.metrics.FlushNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) Close() error {
	return s.wrapped.Close()
}

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

	metaChangefeedProcessingNanos = metric.Metadata{
		Name:        "changefeed.processing_nanos",
		Help:        "Time spent processing KV changes into SQL rows",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaChangefeedTableMetadataNanos = metric.Metadata{
		Name:        "changefeed.table_metadata_nanos",
		Help:        "Time blocked while verifying table metadata histories",
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
)

// Metrics are for production monitoring of changefeeds.
type Metrics struct {
	KVFeedMetrics   kvfeed.Metrics
	EmittedMessages *metric.Counter
	EmittedBytes    *metric.Counter
	Flushes         *metric.Counter
	ErrorRetries    *metric.Counter

	ProcessingNanos    *metric.Counter
	TableMetadataNanos *metric.Counter
	EmitNanos          *metric.Counter
	FlushNanos         *metric.Counter

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
		KVFeedMetrics:   kvfeed.MakeMetrics(histogramWindow),
		EmittedMessages: metric.NewCounter(metaChangefeedEmittedMessages),
		EmittedBytes:    metric.NewCounter(metaChangefeedEmittedBytes),
		Flushes:         metric.NewCounter(metaChangefeedFlushes),
		ErrorRetries:    metric.NewCounter(metaChangefeedErrorRetries),

		ProcessingNanos:    metric.NewCounter(metaChangefeedProcessingNanos),
		TableMetadataNanos: metric.NewCounter(metaChangefeedTableMetadataNanos),
		EmitNanos:          metric.NewCounter(metaChangefeedEmitNanos),
		FlushNanos:         metric.NewCounter(metaChangefeedFlushNanos),
	}
	m.mu.resolved = make(map[int]hlc.Timestamp)

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
