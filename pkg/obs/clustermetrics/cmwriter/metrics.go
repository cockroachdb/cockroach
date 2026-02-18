// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

// This file defines thin wrappers around the metric types in
// pkg/util/metric. Each wrapper embeds the underlying metric type so
// the cluster metrics writer knows which metrics have been touched
// since the last flush. Only changed metrics are written to
// system.cluster_metrics on each flush interval.
//
// Scalar types (Counter, Gauge, Stopwatch) track dirtiness with an
// atomic bool. Vector types (GaugeVec, CounterVec) wrap the
// corresponding metric.*Vec and clear all children on Reset, so
// only values set since the last flush are emitted. On flush, vec
// children are emitted as read-only metricSnapshot values; the
// parent vec is reset after a successful write.

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
)

// metricTypeString converts a metric type enum to the string stored in
// the cluster_metrics table.
func metricTypeString(t *prometheusgo.MetricType) string {
	if *t == metric.MetricType_STOPWATCH {
		return "STOPWATCH"
	}
	return t.String()
}

// WritableMetric provides the data needed to write a metric to storage.
// Both scalar Metric types and metricSnapshot (from vec children) satisfy
// this interface.
type WritableMetric interface {
	// GetName returns the metric name.
	GetName(useStaticLabels bool) string
	// Value returns the current value of the metric.
	Value() int64
	// GetType returns the metric type. For standard metrics this is a
	// prometheus enum value; for stopwatch it is the custom
	// metric.MetricType_STOPWATCH sentinel. Use metricTypeString to
	// convert to the storage string.
	GetType() *prometheusgo.MetricType
	// GetLabels returns the label key-value pairs for this metric.
	// Returns nil for unlabeled metrics.
	GetLabels() map[string]string
}

// Metric is the interface for scalar cluster metrics. It extends
// WritableMetric with dirty tracking and reset for the flush lifecycle.
// Embedding NonExportableMetric prevents these metrics from being
// accidentally registered with node/store/tenant prometheus registries.
type Metric interface {
	metric.Iterable
	metric.NonExportableMetric
	WritableMetric
	// IsDirty returns true if the metric has changed since the last flush.
	IsDirty() bool
	// Reset resets the metric's state after a successful flush.
	// For counters, this resets the count to zero and clears the dirty flag.
	// For gauges, this clears the dirty flag (value is retained).
	Reset()
}

// MetricVec is the interface for vector metrics that contain labeled
// children. The writer iterates children via Each() and resets the
// parent after a successful flush. Reset clears all children so
// only values set since the last flush are emitted next time.
// Embedding NonExportableMetric prevents these metrics from being
// accidentally registered with node/store/tenant prometheus registries.
type MetricVec interface {
	metric.Iterable
	metric.NonExportableMetric
	// Each calls f for every child metric.
	Each(f func(WritableMetric))
	// Reset clears all children. Called after a successful flush.
	Reset()
}

// Verify interface compliance.
var (
	_ Metric    = (*Counter)(nil)
	_ Metric    = (*Gauge)(nil)
	_ Metric    = (*Stopwatch)(nil)
	_ MetricVec = (*GaugeVec)(nil)
	_ MetricVec = (*CounterVec)(nil)
)

// ------------------------------------------------------------
// Scalar types
// ------------------------------------------------------------

// Counter wraps a metric.Counter to satisfy the Metric interface.
// Tracks whether it has been updated since the last flush via a dirty flag.
type Counter struct {
	*metric.Counter
	metric.IsNonExportableMetric
	dirty atomic.Bool
}

// NewCounter creates a new Counter with the given metadata.
func NewCounter(metadata metric.Metadata) *Counter {
	return &Counter{
		Counter: metric.NewCounter(metadata),
	}
}

// Inc increments the counter and marks it as dirty.
func (c *Counter) Inc(i int64) {
	c.Counter.Inc(i)
	c.dirty.Store(true)
}

// Value returns the current count.
func (c *Counter) Value() int64 {
	return c.Counter.Count()
}

// IsDirty returns true if the counter has been incremented since the last flush.
func (c *Counter) IsDirty() bool {
	return c.dirty.Load()
}

// Reset resets the counter to zero and clears the dirty flag.
// Called after a successful flush.
func (c *Counter) Reset() {
	c.Counter.Clear()
	c.dirty.Store(false)
}

// Inspect calls the given closure with the Counter itself.
// This overrides the embedded Counter's Inspect to ensure the registry
// sees Counter rather than the underlying metric.Counter.
func (c *Counter) Inspect(f func(interface{})) {
	f(c)
}

// GetLabels returns nil for unlabeled counters.
func (c *Counter) GetLabels() map[string]string {
	return nil
}

// Gauge wraps a metric.Gauge to track whether it has been updated
// since the last flush. Only gauges that have been explicitly updated will
// be written to the store.
type Gauge struct {
	*metric.Gauge
	metric.IsNonExportableMetric
	dirty atomic.Bool
}

// NewGauge creates a new Gauge with the given metadata.
func NewGauge(metadata metric.Metadata) *Gauge {
	return &Gauge{
		Gauge: metric.NewGauge(metadata),
	}
}

// Update updates the gauge's value and marks it as dirty.
func (g *Gauge) Update(v int64) {
	g.Gauge.Update(v)
	g.dirty.Store(true)
}

// Inc increments the gauge's value and marks it as dirty.
func (g *Gauge) Inc(i int64) {
	g.Gauge.Inc(i)
	g.dirty.Store(true)
}

// Dec decrements the gauge's value and marks it as dirty.
func (g *Gauge) Dec(i int64) {
	g.Gauge.Dec(i)
	g.dirty.Store(true)
}

// Value returns the current value of the gauge.
func (g *Gauge) Value() int64 {
	return g.Gauge.Value()
}

// IsDirty returns true if the gauge has been updated since the last flush.
func (g *Gauge) IsDirty() bool {
	return g.dirty.Load()
}

// Reset clears the dirty flag. Called after a successful flush.
// This does not reset the gauge value itself.
func (g *Gauge) Reset() {
	g.dirty.Store(false)
}

// Inspect calls the given closure with the Gauge itself.
// This overrides the embedded Gauge's Inspect to ensure the registry
// sees Gauge rather than the underlying metric.Gauge.
func (g *Gauge) Inspect(f func(interface{})) {
	f(g)
}

// GetLabels returns nil for unlabeled gauges.
func (g *Gauge) GetLabels() map[string]string {
	return nil
}

// Stopwatch wraps metric.Stopwatch with dirty tracking. Value() returns
// the stored unix timestamp; Elapsed() returns seconds since Start().
type Stopwatch struct {
	*metric.Stopwatch
	metric.IsNonExportableMetric
	dirty atomic.Bool
}

// NewStopwatch creates a new Stopwatch with the given metadata.
func NewStopwatch(metadata metric.Metadata) *Stopwatch {
	return &Stopwatch{Stopwatch: metric.NewStopwatch(metadata)}
}

// Start records the current time as the start time and marks dirty.
func (s *Stopwatch) Start() {
	s.Stopwatch.Start()
	s.dirty.Store(true)
}

// IsDirty returns true if Start() has been called since the last flush.
func (s *Stopwatch) IsDirty() bool {
	return s.dirty.Load()
}

// Reset clears the dirty flag. Called after a successful flush.
func (s *Stopwatch) Reset() {
	s.dirty.Store(false)
}

// Inspect calls the given closure with the Stopwatch itself.
func (s *Stopwatch) Inspect(f func(interface{})) {
	f(s)
}

// GetLabels returns nil for unlabeled stopwatches.
func (s *Stopwatch) GetLabels() map[string]string {
	return nil
}

// ------------------------------------------------------------
// Vector types
// ------------------------------------------------------------

// metricSnapshot is a read-only snapshot of a vec child's data,
// satisfying WritableMetric so it can be passed to MetricStore.Write
// alongside scalar metrics.
type metricSnapshot struct {
	*metric.VecChildSnapshot
	metricType *prometheusgo.MetricType
}

var _ WritableMetric = (*metricSnapshot)(nil)

func (s *metricSnapshot) Value() int64                      { return s.VecChildSnapshot.Value }
func (s *metricSnapshot) GetType() *prometheusgo.MetricType { return s.metricType }
func (s *metricSnapshot) GetLabels() map[string]string      { return s.Labels }

// GaugeVec wraps metric.GaugeVec for the cluster metrics writer.
// All children present at flush time are written; Reset clears all
// children so only values set since the last flush are emitted.
type GaugeVec struct {
	*metric.GaugeVec
	metric.IsNonExportableMetric
}

// NewGaugeVec creates a new GaugeVec with the given metadata and label names.
func NewGaugeVec(metadata metric.Metadata, labelNames ...string) *GaugeVec {
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
}

// NewCounterVec creates a new CounterVec with the given metadata and
// label names.
func NewCounterVec(metadata metric.Metadata, labelNames ...string) *CounterVec {
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
