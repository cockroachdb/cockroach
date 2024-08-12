// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timers

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO: should we add a label for sink type? or other things maybe?

var TimersEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel, "changefeed.timers.enabled",
	"enables debugging timers for changefeeds with the `changefeed.stage.latency.*` metrics. You may also want to set `server.child_metrics.enabled` to debug a specific feed",
	false,
	settings.WithPublic)

type Scope string
type Stage string

// Since tsdb doesn't support labels, and in the interest of supportability via
// console only, we must make metrics for each stage. Which means we have to
// hardcode them.
//
// However, we still need to be using aggmetrics to support the `scope` label.
//
// TODO: we can get around hardcoding them by extending the metrics registry
// reflection functionality or by passing the registry into here. Neither seems
// worth it currently though.

const (
	StageUnknown                   Stage = ""
	StageCheckpointJobProgress     Stage = "checkpoint_job_progress"
	StageEncode                    Stage = "encode"
	StageEmitRow                   Stage = "emit_row"
	StageKVFeedWaitForTableEvent   Stage = "kvfeed_wait_for_table_event"
	StageKVFeedCheckCopyBoundary   Stage = "kvfeed_check_copy_boundary"
	StageKVFeedBuffer              Stage = "kvfeed_buffer"
	StageRangefeedBufferValue      Stage = "rangefeed_buffer_value"
	StageRangefeedBufferCheckpoint Stage = "rangefeed_buffer_checkpoint"
)

// I don't love having to repeat this. TODO: is there a better way?
var AllStages = []Stage{
	StageCheckpointJobProgress,
	StageEncode,
	StageEmitRow,
	StageKVFeedWaitForTableEvent,
	StageKVFeedCheckCopyBoundary,
	StageKVFeedBuffer,
	StageRangefeedBufferValue,
	StageRangefeedBufferCheckpoint,
}

type Timers struct {
	Latencies  map[Stage]*aggmetric.AggHistogram
	settings   *settings.Values
	components struct {
		syncutil.RWMutex
		m map[Stage]*labeledTimer
	}
	ts timeutil.TimeSource
}

func (*Timers) MetricStruct() {}

func New(histogramWindow time.Duration, settings *settings.Values) *Timers {
	metaFor := func(stage Stage) metric.Metadata {
		baseMeta := metric.Metadata{Name: "changefeed.stage.latency", Help: "Latency of changefeed stage", Unit: metric.Unit_NANOSECONDS, Measurement: "Latency"}
		baseMeta.Name = fmt.Sprintf("%s.%s", baseMeta.Name, stage)
		baseMeta.Help = fmt.Sprintf("%s: %s", baseMeta.Help, stage)
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

	t := &Timers{
		Latencies: map[Stage]*aggmetric.AggHistogram{},
		settings:  settings,
		ts:        timeutil.DefaultTimeSource{},
	}
	t.components.m = make(map[Stage]*labeledTimer)
	for _, stage := range AllStages {
		t.Latencies[stage] = aggmetric.NewHistogram(histogramOptsFor(metaFor(stage)), "scope")
	}

	return t
}

func (t *Timers) Scoped(scope Scope) *ScopedTimers {
	return &ScopedTimers{scope: scope, Timers: t}
}

type ScopedTimers struct {
	scope Scope
	*Timers
}

func (st *ScopedTimers) For(stage Stage) *labeledTimer {
	if !TimersEnabled.Get(st.settings) {
		return nil
	}
	if _, ok := st.Latencies[stage]; !ok {
		log.Warningf(context.Background(), "tried to start timer for unknown stage %s", stage)
		return nil
	}

	inited := func() bool {
		st.components.RLock()
		defer st.components.RUnlock()
		_, ok := st.components.m[stage]
		return ok
	}()

	if !inited {
		st.initStage(stage)
	}

	st.components.RLock()
	defer st.components.RUnlock()

	return st.components.m[stage]
}

func (st *ScopedTimers) initStage(stage Stage) {
	st.components.Lock()
	defer st.components.Unlock()

	st.components.m[stage] = &labeledTimer{ts: st.ts, h: st.Latencies[stage].AddChild(string(st.scope), string(stage))}
}

type labeledTimer struct {
	h  *aggmetric.Histogram
	ts timeutil.TimeSource
}

func (lt *labeledTimer) StartTimer() (stop func()) {
	if lt == nil {
		return nop
	}
	start := lt.ts.Now()
	return func() {
		lt.h.RecordValue(timeutil.Since(start).Nanoseconds())
	}
}

func (lt *labeledTimer) Time(cb func()) {
	if lt == nil {
		cb()
		return
	}
	start := lt.ts.Now()
	cb()
	lt.h.RecordValue(timeutil.Since(start).Nanoseconds())
}

var nop = func() {}
