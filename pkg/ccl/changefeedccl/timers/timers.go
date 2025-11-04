// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timers

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/prometheus/client_golang/prometheus"
)

type Timers struct {
	CheckpointJobProgress     *aggmetric.AggHistogram
	FrontierPersistence       *aggmetric.AggHistogram
	Encode                    *aggmetric.AggHistogram
	EmitRow                   *aggmetric.AggHistogram
	DownstreamClientSend      *aggmetric.AggHistogram
	KVFeedWaitForTableEvent   *aggmetric.AggHistogram
	KVFeedBuffer              *aggmetric.AggHistogram
	RangefeedBufferValue      *aggmetric.AggHistogram
	RangefeedBufferCheckpoint *aggmetric.AggHistogram
	PTSManage                 *aggmetric.AggHistogram
	PTSManageError            *aggmetric.AggHistogram
	PTSCreate                 *aggmetric.AggHistogram
}

func (*Timers) MetricStruct() {}

var _ metric.Struct = &Timers{}

func New(histogramWindow time.Duration) *Timers {
	const (
		stagePrefix    = "changefeed.stage"
		latencySuffix  = "latency"
		ptsSubCategory = "pts"
	)

	histogramOptsFor := func(nameFormat, labeledName, labelName, desc string) metric.HistogramOptions {
		return metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:         fmt.Sprintf(nameFormat, labelName),
				Help:         desc,
				Unit:         metric.Unit_NANOSECONDS,
				Measurement:  "Latency",
				LabeledName:  labeledName,
				StaticLabels: metric.MakeLabelPairs(metric.LabelName, labelName),
			},
			Duration: histogramWindow,
			Buckets:  prometheus.ExponentialBucketsRange(float64(1*time.Microsecond), float64(1*time.Hour), 60),
			Mode:     metric.HistogramModePrometheus,
		}
	}

	stageOpts := func(labeleName, desc string) metric.HistogramOptions {
		nameFormat := fmt.Sprintf("%s.%%s.%s", stagePrefix, latencySuffix)
		labeledName := fmt.Sprintf("%s.%s", stagePrefix, latencySuffix)
		return histogramOptsFor(nameFormat, labeledName, labeleName, desc)
	}

	ptsStageOpts := func(labeleName, desc string) metric.HistogramOptions {
		nameFormat := fmt.Sprintf("%s.%s.%%s.%s", stagePrefix, ptsSubCategory, latencySuffix)
		labeledName := fmt.Sprintf("%s.%s.%s", stagePrefix, ptsSubCategory, latencySuffix)
		return histogramOptsFor(nameFormat, labeledName, labeleName, desc)
	}

	b := aggmetric.MakeBuilder("scope")
	return &Timers{
		CheckpointJobProgress:     b.Histogram(stageOpts("checkpoint_job_progress", "Latency of the changefeed stage: checkpointing job progress")),
		FrontierPersistence:       b.Histogram(stageOpts("frontier_persistence", "Latency of the changefeed stage: persisting frontier to job info")),
		Encode:                    b.Histogram(stageOpts("encode", "Latency of the changefeed stage: encoding data")),
		EmitRow:                   b.Histogram(stageOpts("emit_row", "Latency of the changefeed stage: emitting row to sink")),
		DownstreamClientSend:      b.Histogram(stageOpts("downstream_client_send", "Latency of the changefeed stage: flushing messages from the sink's client to its downstream. This includes sends that failed for most but not all sinks.")),
		KVFeedWaitForTableEvent:   b.Histogram(stageOpts("kv_feed_wait_for_table_event", "Latency of the changefeed stage: waiting for a table schema event to join to the kv event")),
		KVFeedBuffer:              b.Histogram(stageOpts("kv_feed_buffer", "Latency of the changefeed stage: waiting to buffer kv events")),
		RangefeedBufferValue:      b.Histogram(stageOpts("rangefeed_buffer_value", "Latency of the changefeed stage: buffering rangefeed value events")),
		RangefeedBufferCheckpoint: b.Histogram(stageOpts("rangefeed_buffer_checkpoint", "Latency of the changefeed stage: buffering rangefeed checkpoint events")),
		PTSManage:                 b.Histogram(ptsStageOpts("manage", "Latency of the changefeed stage: Time spent successfully managing protected timestamp records on highwater advance, including time spent creating new protected timestamps when needed")),
		PTSManageError:            b.Histogram(ptsStageOpts("manage_error", "Latency of the changefeed stage: Time spent managing protected timestamp when we eventually error")),
		PTSCreate:                 b.Histogram(ptsStageOpts("create", "Latency of the changefeed stage: Time spent creating protected timestamp records on changefeed creation")),
	}
}

func (ts *Timers) GetOrCreateScopedTimers(scope string) *ScopedTimers {
	return &ScopedTimers{
		CheckpointJobProgress:     &timer{ts.CheckpointJobProgress.AddChild(scope)},
		FrontierPersistence:       &timer{ts.FrontierPersistence.AddChild(scope)},
		Encode:                    &timer{ts.Encode.AddChild(scope)},
		EmitRow:                   &timer{ts.EmitRow.AddChild(scope)},
		DownstreamClientSend:      &timer{ts.DownstreamClientSend.AddChild(scope)},
		KVFeedWaitForTableEvent:   &timer{ts.KVFeedWaitForTableEvent.AddChild(scope)},
		KVFeedBuffer:              &timer{ts.KVFeedBuffer.AddChild(scope)},
		RangefeedBufferValue:      &timer{ts.RangefeedBufferValue.AddChild(scope)},
		RangefeedBufferCheckpoint: &timer{ts.RangefeedBufferCheckpoint.AddChild(scope)},
		PTSManage:                 &timer{ts.PTSManage.AddChild(scope)},
		PTSManageError:            &timer{ts.PTSManageError.AddChild(scope)},
		PTSCreate:                 &timer{ts.PTSCreate.AddChild(scope)},
	}
}

type ScopedTimers struct {
	CheckpointJobProgress     *timer
	FrontierPersistence       *timer
	Encode                    *timer
	EmitRow                   *timer
	DownstreamClientSend      *timer
	KVFeedWaitForTableEvent   *timer
	KVFeedBuffer              *timer
	PTSCreate                 *timer
	PTSManage                 *timer
	PTSManageError            *timer
	RangefeedBufferValue      *timer
	RangefeedBufferCheckpoint *timer
}

var NoopScopedTimers = &ScopedTimers{}

type timer struct {
	hist *aggmetric.Histogram
}

func (t *timer) Start() TimerHandle {
	if t == nil {
		return TimerHandle{}
	}
	return TimerHandle{start: crtime.NowMono(), hist: t.hist}
}

type TimerHandle struct {
	start crtime.Mono
	hist  *aggmetric.Histogram
}

// End records the elapsed time and returns the duration.
func (th TimerHandle) End() time.Duration {
	if th.hist == nil {
		return 0
	}
	elapsed := th.start.Elapsed()
	th.hist.RecordValue(elapsed.Nanoseconds())
	return elapsed
}

func (t *timer) Time(cb func()) {
	defer t.Start().End()
	cb()
}
