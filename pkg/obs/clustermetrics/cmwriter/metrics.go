// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

// This file defines thin wrappers around the metric types in
// pkg/util/metric. Each wrapper embeds the underlying metric type and
// adds a dirty flag so the cluster metrics writer knows which metrics
// have been touched since the last flush. Only dirty metrics are
// written to system.cluster_metrics on each flush interval.
//
// Scalar types (Counter, Gauge, Stopwatch) track dirtiness with an
// atomic bool. Vector types (GaugeVec, CounterVec) wrap the
// corresponding metric.*Vec and maintain a map of dirty label
// combinations. On flush, dirty vec children are emitted as
// read-only metricSnapshot values; the parent vec is reset after
// a successful write.

import (
	"sort"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	prometheusgo "github.com/prometheus/client_model/go"
)

// metricTypeString converts a metric type to its lowercase string
// representation for storage in the cluster_metrics table. It handles
// the custom stopwatch sentinel and falls through to the protobuf
// String() for standard prometheus types.
func metricTypeString(t *prometheusgo.MetricType) string {
	if *t == metric.MetricType_STOPWATCH {
		return "stopwatch"
	}
	return strings.ToLower(t.String())
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
type Metric interface {
	metric.Iterable
	WritableMetric
	// IsDirty returns true if the metric has changed since the last flush.
	IsDirty() bool
	// Reset resets the metric's state after a successful flush.
	// For counters, this resets the count to zero and clears the dirty flag.
	// For gauges, this clears the dirty flag (value is retained).
	Reset()
}

// MetricVec is the interface for vector metrics that contain labeled
// children. The writer iterates dirty children via Each() and resets
// the parent after a successful flush.
type MetricVec interface {
	metric.Iterable
	// Each calls f for every dirty child metric.
	Each(f func(WritableMetric))
	// Reset clears all dirty flags. Called after a successful flush.
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

// encodeLabelValues encodes label values into a string key using the
// ordered label names from the parent vec.
func encodeLabelValues(labelNames []string, values map[string]string) string {
	parts := make([]string, len(labelNames))
	for i, l := range labelNames {
		parts[i] = values[l]
	}
	return strings.Join(parts, "\x00")
}

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

// GaugeVec wraps metric.GaugeVec with per-label dirty tracking for
// the cluster metrics writer.
type GaugeVec struct {
	*metric.GaugeVec
	labelNames []string
	mu         struct {
		syncutil.Mutex
		dirty map[string]bool
	}
}

// NewGaugeVec creates a new GaugeVec with the given metadata and label names.
func NewGaugeVec(metadata metric.Metadata, labelNames ...string) *GaugeVec {
	sorted := make([]string, len(labelNames))
	copy(sorted, labelNames)
	sort.Strings(sorted)
	v := &GaugeVec{
		GaugeVec:   metric.NewExportedGaugeVec(metadata, sorted),
		labelNames: sorted,
	}
	v.mu.dirty = make(map[string]bool)
	return v
}

func (v *GaugeVec) markDirty(labels map[string]string) {
	key := encodeLabelValues(v.labelNames, labels)
	v.mu.Lock()
	defer v.mu.Unlock()
	v.mu.dirty[key] = true
}

// Update sets the value for the given label combination and marks dirty.
func (v *GaugeVec) Update(labels map[string]string, value int64) {
	v.GaugeVec.Update(labels, value)
	v.markDirty(labels)
}

// Inc increments the gauge for the given label combination and marks dirty.
func (v *GaugeVec) Inc(labels map[string]string, value int64) {
	v.GaugeVec.Inc(labels, value)
	v.markDirty(labels)
}

// Dec decrements the gauge for the given label combination and marks dirty.
func (v *GaugeVec) Dec(labels map[string]string, value int64) {
	v.GaugeVec.Dec(labels, value)
	v.markDirty(labels)
}

// Each calls f for every dirty child gauge.
func (v *GaugeVec) Each(f func(WritableMetric)) {
	v.GaugeVec.EachChild(func(s *metric.VecChildSnapshot) {
		key := encodeLabelValues(v.labelNames, s.Labels)
		v.mu.Lock()
		isDirty := v.mu.dirty[key]
		v.mu.Unlock()
		if !isDirty {
			return
		}
		f(&metricSnapshot{VecChildSnapshot: s, metricType: prometheusgo.MetricType_GAUGE.Enum()})
	})
}

// Reset clears all dirty flags. Called after a successful flush.
func (v *GaugeVec) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.mu.dirty = make(map[string]bool)
}

// Inspect passes the GaugeVec itself (not the embedded metric.GaugeVec).
func (v *GaugeVec) Inspect(f func(interface{})) { f(v) }

// CounterVec wraps metric.CounterVec with per-label dirty tracking for
// the cluster metrics writer.
type CounterVec struct {
	*metric.CounterVec
	labelNames []string
	mu         struct {
		syncutil.Mutex
		dirty map[string]bool
	}
}

// NewCounterVec creates a new CounterVec with the given metadata and
// label names.
func NewCounterVec(metadata metric.Metadata, labelNames ...string) *CounterVec {
	sorted := make([]string, len(labelNames))
	copy(sorted, labelNames)
	sort.Strings(sorted)
	v := &CounterVec{
		CounterVec: metric.NewExportedCounterVec(metadata, sorted),
		labelNames: sorted,
	}
	v.mu.dirty = make(map[string]bool)
	return v
}

func (v *CounterVec) markDirty(labels map[string]string) {
	key := encodeLabelValues(v.labelNames, labels)
	v.mu.Lock()
	defer v.mu.Unlock()
	v.mu.dirty[key] = true
}

// Inc increments the counter for the given label combination and marks dirty.
func (v *CounterVec) Inc(labels map[string]string, delta int64) {
	v.CounterVec.Inc(labels, delta)
	v.markDirty(labels)
}

// Each calls f for every dirty child counter.
func (v *CounterVec) Each(f func(WritableMetric)) {
	v.CounterVec.EachChild(func(s *metric.VecChildSnapshot) {
		key := encodeLabelValues(v.labelNames, s.Labels)
		v.mu.Lock()
		isDirty := v.mu.dirty[key]
		v.mu.Unlock()
		if !isDirty {
			return
		}
		f(&metricSnapshot{VecChildSnapshot: s, metricType: prometheusgo.MetricType_COUNTER.Enum()})
	})
}

// Reset clears all dirty flags. Called after a successful flush.
func (v *CounterVec) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.mu.dirty = make(map[string]bool)
}

// Inspect passes the CounterVec itself (not the embedded metric.CounterVec).
func (v *CounterVec) Inspect(f func(interface{})) { f(v) }
