// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package timers

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/prometheus/client_golang/prometheus"
)

type Timers struct {
	CheckpointJobProgress     *aggmetric.AggHistogram
	Encode                    *aggmetric.AggHistogram
	EmitRow                   *aggmetric.AggHistogram
	KVFeedWaitForTableEvent   *aggmetric.AggHistogram
	KVFeedCheckCopyBoundary   *aggmetric.AggHistogram
	KVFeedBuffer              *aggmetric.AggHistogram
	RangefeedBufferValue      *aggmetric.AggHistogram
	RangefeedBufferCheckpoint *aggmetric.AggHistogram
}

func New(histogramWindow time.Duration) *Timers {
	metaFor := func(stage string) metric.Metadata {
		baseMeta := metric.Metadata{Unit: metric.Unit_NANOSECONDS, Measurement: "Latency"}
		baseMeta.Name = fmt.Sprintf("changefeed.stage.%s.latency", stage)
		baseMeta.Help = fmt.Sprintf("Latency of the changefeed stage: %s", stage)
		return baseMeta
	}
	histogramOptsFor := func(meta metric.Metadata) metric.HistogramOptions {
		return metric.HistogramOptions{
			Metadata: meta,
			Duration: histogramWindow,
			Buckets:  prometheus.ExponentialBucketsRange(float64(1*time.Microsecond), float64(1*time.Hour), 60),
			Mode:     metric.HistogramModePrometheus,
		}
	}

	b := aggmetric.MakeBuilder("scope")
	return &Timers{
		CheckpointJobProgress:     b.Histogram(histogramOptsFor(metaFor("checkpoint_job_progress"))),
		Encode:                    b.Histogram(histogramOptsFor(metaFor("encode"))),
		EmitRow:                   b.Histogram(histogramOptsFor(metaFor("emit_row"))),
		KVFeedWaitForTableEvent:   b.Histogram(histogramOptsFor(metaFor("kv_feed_wait_for_table_event"))),
		KVFeedCheckCopyBoundary:   b.Histogram(histogramOptsFor(metaFor("kv_feed_check_copy_boundary"))),
		KVFeedBuffer:              b.Histogram(histogramOptsFor(metaFor("kv_feed_buffer"))),
		RangefeedBufferValue:      b.Histogram(histogramOptsFor(metaFor("rangefeed_buffer_value"))),
		RangefeedBufferCheckpoint: b.Histogram(histogramOptsFor(metaFor("rangefeed_buffer_checkpoint"))),
	}
}

func (ts *Timers) GetOrCreateScopedTimers(scope string) *ScopedTimers {
	return &ScopedTimers{
		CheckpointJobProgress:     &timer{ts.CheckpointJobProgress.AddChild(scope)},
		Encode:                    &timer{ts.Encode.AddChild(scope)},
		EmitRow:                   &timer{ts.EmitRow.AddChild(scope)},
		KVFeedWaitForTableEvent:   &timer{ts.KVFeedWaitForTableEvent.AddChild(scope)},
		KVFeedCheckCopyBoundary:   &timer{ts.KVFeedCheckCopyBoundary.AddChild(scope)},
		KVFeedBuffer:              &timer{ts.KVFeedBuffer.AddChild(scope)},
		RangefeedBufferValue:      &timer{ts.RangefeedBufferValue.AddChild(scope)},
		RangefeedBufferCheckpoint: &timer{ts.RangefeedBufferCheckpoint.AddChild(scope)},
	}
}

type ScopedTimers struct {
	CheckpointJobProgress     *timer
	Encode                    *timer
	EmitRow                   *timer
	KVFeedWaitForTableEvent   *timer
	KVFeedCheckCopyBoundary   *timer
	KVFeedBuffer              *timer
	RangefeedBufferValue      *timer
	RangefeedBufferCheckpoint *timer
}

func (ts *ScopedTimers) StartTimer(stage *aggmetric.Histogram) func() {
	start := timeutil.Now()
	return func() {
		stage.RecordValue(timeutil.Since(start).Nanoseconds())
	}
}

type timer struct {
	hist *aggmetric.Histogram
}

func (t *timer) Start() (end func()) {
	start := timeutil.Now()
	return func() {
		t.hist.RecordValue(timeutil.Since(start).Nanoseconds())
	}
}

func (t *timer) Time(cb func()) {
	defer t.Start()()
	cb()
}
