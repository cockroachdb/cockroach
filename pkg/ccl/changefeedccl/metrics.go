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
	"math"

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

func (s *metricsSink) EmitRow(ctx context.Context, topic string, key, value []byte) error {
	start := timeutil.Now()
	err := s.wrapped.EmitRow(ctx, topic, key, value)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(key) + len(value)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	start := timeutil.Now()
	err := s.wrapped.EmitResolvedTimestamp(ctx, payload)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(payload)))
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
	metaChangefeedEmitNanos = metric.Metadata{
		Name:        "changefeed.emit_nanos",
		Help:        "Total time spent emitting all feeds",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	// This is more naturally a histogram but that creates a lot of timeseries
	// and it's not clear that the additional fidelity is worth it. Revisit if
	// evidence suggests otherwise.
	metaChangefeedFlushes = metric.Metadata{
		Name:        "changefeed.flushes",
		Help:        "Total flushes across all feeds",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
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
	metaChangefeedMinHighWater = metric.Metadata{
		Name:        "changefeed.min_high_water",
		Help:        "Latest high_water timestamp of most behind feed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_TIMESTAMP_NS,
	}
	metaChangefeedSinkErrorRetries = metric.Metadata{
		Name:        "changefeed.sink_error_retries",
		Help:        "Total retryable errors encountered while emitting to sinks",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
)

const noMinHighWaterSentinel = int64(math.MaxInt64)

// Metrics are for production monitoring of changefeeds.
type Metrics struct {
	EmittedMessages  *metric.Counter
	EmittedBytes     *metric.Counter
	EmitNanos        *metric.Counter
	Flushes          *metric.Counter
	FlushNanos       *metric.Counter
	SinkErrorRetries *metric.Counter

	mu struct {
		syncutil.Mutex
		id       int
		resolved map[int]hlc.Timestamp
	}
	MinHighWater *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for changefeed monitoring.
func MakeMetrics() metric.Struct {
	m := &Metrics{
		EmittedMessages:  metric.NewCounter(metaChangefeedEmittedMessages),
		EmittedBytes:     metric.NewCounter(metaChangefeedEmittedBytes),
		EmitNanos:        metric.NewCounter(metaChangefeedEmitNanos),
		Flushes:          metric.NewCounter(metaChangefeedFlushes),
		FlushNanos:       metric.NewCounter(metaChangefeedFlushNanos),
		SinkErrorRetries: metric.NewCounter(metaChangefeedSinkErrorRetries),
	}
	m.mu.resolved = make(map[int]hlc.Timestamp)
	m.MinHighWater = metric.NewFunctionalGauge(metaChangefeedMinHighWater, func() int64 {
		minHighWater := noMinHighWaterSentinel
		m.mu.Lock()
		for _, resolved := range m.mu.resolved {
			if minHighWater == noMinHighWaterSentinel || resolved.WallTime < minHighWater {
				minHighWater = resolved.WallTime
			}
		}
		m.mu.Unlock()
		return minHighWater
	})
	return m
}

func init() {
	jobs.MakeChangefeedMetricsHook = MakeMetrics
}
