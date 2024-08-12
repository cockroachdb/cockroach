// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package timers

import (
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
	KVFeedBuffer              *aggmetric.AggHistogram
	RangefeedBufferValue      *aggmetric.AggHistogram
	RangefeedBufferCheckpoint *aggmetric.AggHistogram
}

func New(histogramWindow time.Duration) *Timers {
	histogramOptsFor := func(name, desc string) metric.HistogramOptions {
		return metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        name,
				Help:        desc,
				Unit:        metric.Unit_NANOSECONDS,
				Measurement: "Latency",
			},
			Duration: histogramWindow,
			Buckets:  prometheus.ExponentialBucketsRange(float64(1*time.Microsecond), float64(1*time.Hour), 60),
			Mode:     metric.HistogramModePrometheus,
		}
	}

	b := aggmetric.MakeBuilder("scope")
	return &Timers{
		CheckpointJobProgress:     b.Histogram(histogramOptsFor("changefeed.stage.checkpoint_job_progress.latency", "Latency of the changefeed stage: checkpointing job progress")),
		Encode:                    b.Histogram(histogramOptsFor("changefeed.stage.encode.latency", "Latency of the changefeed stage: encoding data")),
		EmitRow:                   b.Histogram(histogramOptsFor("changefeed.stage.emit_row.latency", "Latency of the changefeed stage: emitting row to sink")),
		KVFeedWaitForTableEvent:   b.Histogram(histogramOptsFor("changefeed.stage.kv_feed_wait_for_table_event.latency", "Latency of the changefeed stage: waiting for a table schema event to join to the kv event")),
		KVFeedBuffer:              b.Histogram(histogramOptsFor("changefeed.stage.kv_feed_buffer.latency", "Latency of the changefeed stage: waiting to buffer kv events")),
		RangefeedBufferValue:      b.Histogram(histogramOptsFor("changefeed.stage.rangefeed_buffer_value.latency", "Latency of the changefeed stage: buffering rangefeed value events")),
		RangefeedBufferCheckpoint: b.Histogram(histogramOptsFor("changefeed.stage.rangefeed_buffer_checkpoint.latency", "Latency of the changefeed stage: buffering rangefeed checkpoint events")),
	}
}

func (ts *Timers) GetOrCreateScopedTimers(scope string) *ScopedTimers {
	return &ScopedTimers{
		CheckpointJobProgress:     &timer{ts.CheckpointJobProgress.AddChild(scope)},
		Encode:                    &timer{ts.Encode.AddChild(scope)},
		EmitRow:                   &timer{ts.EmitRow.AddChild(scope)},
		KVFeedWaitForTableEvent:   &timer{ts.KVFeedWaitForTableEvent.AddChild(scope)},
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
