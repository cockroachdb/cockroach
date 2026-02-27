// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmmetrics

// This file defines thin wrappers around the vector metric types in
// pkg/util/metric. Vector types (GaugeVec, CounterVec) wrap the
// corresponding metric.*Vec and clear all children on Reset, so
// only values set since the last flush are emitted. On flush, vec
// children are emitted as read-only metricSnapshot values; the
// parent vec is reset after a successful write.

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	prometheusgo "github.com/prometheus/client_model/go"
)

// Verify interface compliance.
var (
	_ MetricVec      = (*GaugeVec)(nil)
	_ MetricVec      = (*CounterVec)(nil)
	_ MetricVec      = (*WriteStopwatchVec)(nil)
	_ WritableMetric = (*metricSnapshot)(nil)
)

// metricSnapshot is a read-only snapshot of a vec child's data,
// satisfying WritableMetric so it can be passed to MetricStore.Write
// alongside scalar metrics. The optional typeString field overrides
// the value returned by MetricTypeString (e.g. "STOPWATCH" for
// stopwatch vec children).
type metricSnapshot struct {
	*metric.VecChildSnapshot
	metricType *prometheusgo.MetricType
	typeString string // if non-empty, MetricTypeString returns this
}

func (s *metricSnapshot) Value() int64                      { return s.VecChildSnapshot.Value }
func (s *metricSnapshot) GetType() *prometheusgo.MetricType { return s.metricType }
func (s *metricSnapshot) GetLabels() map[string]string      { return s.Labels }

// GaugeVec wraps metric.GaugeVec for the cluster metrics writer.
// All children present at flush time are written; Reset clears all
// children so only values set since the last flush are emitted.
type GaugeVec struct {
	*metric.GaugeVec
	metric.IsNonExportableMetric
	IsClusterMetric
}

// NewGaugeVec creates a new GaugeVec with the given metadata and label
// names. The metadata is automatically registered so that the cmreader
// can materialize this metric from the rangefeed.
func NewGaugeVec(metadata metric.Metadata, labelNames ...string) *GaugeVec {
	ensureLabeledClusterMetricRegistered(metadata.Name, metadata, labelNames)
	return &GaugeVec{
		GaugeVec: metric.NewExportedGaugeVec(metadata, labelNames),
	}
}

// Each calls f for every child gauge.
func (v *GaugeVec) Each(f func(WritableMetric)) {
	v.GaugeVec.EachChild(func(s *metric.VecChildSnapshot) {
		f(&metricSnapshot{
			VecChildSnapshot: s,
			metricType:       prometheusgo.MetricType_GAUGE.Enum(),
		})
	})
}

// Reset clears all children. Called after a successful flush.
func (v *GaugeVec) Reset() {
	v.GaugeVec.Clear()
}

// Inspect passes the GaugeVec itself (not the embedded metric.GaugeVec).
func (v *GaugeVec) Inspect(f func(interface{})) { f(v) }

// CounterVec wraps metric.CounterVec for the cluster metrics writer.
// All children present at flush time are written; Reset clears all
// children so only the delta since the last flush is emitted.
type CounterVec struct {
	*metric.CounterVec
	metric.IsNonExportableMetric
	IsClusterMetric
}

// NewCounterVec creates a new CounterVec with the given metadata and
// label names. The metadata is automatically registered so that the
// cmreader can materialize this metric from the rangefeed.
func NewCounterVec(metadata metric.Metadata, labelNames ...string) *CounterVec {
	ensureLabeledClusterMetricRegistered(metadata.Name, metadata, labelNames)
	return &CounterVec{
		CounterVec: metric.NewExportedCounterVec(metadata, labelNames),
	}
}

// Each calls f for every child counter.
func (v *CounterVec) Each(f func(WritableMetric)) {
	v.CounterVec.EachChild(func(s *metric.VecChildSnapshot) {
		f(&metricSnapshot{
			VecChildSnapshot: s,
			metricType:       prometheusgo.MetricType_COUNTER.Enum(),
		})
	})
}

// Reset clears all counter values and children. Called after a
// successful flush. Like the scalar Counter, values are reset to
// zero so only the delta since the last flush is written each time.
func (v *CounterVec) Reset() {
	v.CounterVec.Clear()
}

// Inspect passes the CounterVec itself (not the embedded metric.CounterVec).
func (v *CounterVec) Inspect(f func(interface{})) { f(v) }

// WriteStopwatchVec wraps metric.GaugeVec for labeled stopwatch
// metrics. Each child records a unix-second timestamp set via
// SetStartTime. All children present at flush time are written;
// Reset clears all children so only values set since the last
// flush are emitted.
type WriteStopwatchVec struct {
	*metric.GaugeVec
	metric.IsNonExportableMetric
	IsClusterMetric
	timeSource timeutil.TimeSource
}

// NewWriteStopwatchVec creates a new WriteStopwatchVec with the
// given metadata, time source, and label names.
func NewWriteStopwatchVec(
	metadata metric.Metadata, timeSource timeutil.TimeSource, labelNames ...string,
) *WriteStopwatchVec {
	ensureLabeledClusterMetricRegistered(metadata.Name, metadata, labelNames)
	return &WriteStopwatchVec{
		GaugeVec:   metric.NewExportedGaugeVec(metadata, labelNames),
		timeSource: timeSource,
	}
}

// SetStartTime records the current time as the start time for the
// given label set.
func (v *WriteStopwatchVec) SetStartTime(labels map[string]string) {
	v.GaugeVec.Update(labels, v.timeSource.Now().Unix())
}

// Each calls f for every child stopwatch.
func (v *WriteStopwatchVec) Each(f func(WritableMetric)) {
	v.GaugeVec.EachChild(func(s *metric.VecChildSnapshot) {
		f(&metricSnapshot{
			VecChildSnapshot: s,
			metricType:       prometheusgo.MetricType_GAUGE.Enum(),
			typeString:       "STOPWATCH",
		})
	})
}

// Reset clears all children. Called after a successful flush.
func (v *WriteStopwatchVec) Reset() {
	v.GaugeVec.Clear()
}

// Inspect passes the WriteStopwatchVec itself.
func (v *WriteStopwatchVec) Inspect(f func(interface{})) { f(v) }
