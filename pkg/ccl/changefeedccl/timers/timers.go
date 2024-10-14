// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	DownstreamClientSend      *aggmetric.AggHistogram
	KVFeedWaitForTableEvent   *aggmetric.AggHistogram
	KVFeedBuffer              *aggmetric.AggHistogram
	RangefeedBufferValue      *aggmetric.AggHistogram
	RangefeedBufferCheckpoint *aggmetric.AggHistogram
}

func (*Timers) MetricStruct() {}

var _ metric.Struct = &Timers{}

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
		DownstreamClientSend:      b.Histogram(histogramOptsFor("changefeed.stage.downstream_client_send.latency", "Latency of the changefeed stage: flushing messages from the sink's client to its downstream. This includes sends that failed for most but not all sinks.")),
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
		DownstreamClientSend:      &timer{ts.DownstreamClientSend.AddChild(scope)},
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
	DownstreamClientSend      *timer
	KVFeedWaitForTableEvent   *timer
	KVFeedBuffer              *timer
	RangefeedBufferValue      *timer
	RangefeedBufferCheckpoint *timer
}

var NoopScopedTimers = &ScopedTimers{}

type timer struct {
	hist *aggmetric.Histogram
}

func (t *timer) Start() (end func()) {
	if t == nil {
		return func() {}
	}

	start := timeutil.Now()
	return func() {
		t.hist.RecordValue(timeutil.Since(start).Nanoseconds())
	}
}

func (t *timer) Time(cb func()) {
	defer t.Start()()
	cb()
}
