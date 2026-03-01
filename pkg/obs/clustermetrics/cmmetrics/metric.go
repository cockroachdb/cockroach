// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmmetrics

// This file defines thin wrappers around the scalar metric types in
// pkg/util/metric. Each wrapper embeds the underlying metric type so
// the cluster metrics writer knows which metrics have been touched
// since the last flush. Only changed metrics are written to
// system.cluster_metrics on each flush interval.

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	prometheusgo "github.com/prometheus/client_model/go"
)

// Verify interface compliance.
var (
	_ Metric = (*Counter)(nil)
	_ Metric = (*Gauge)(nil)
	_ Metric = (*WriteStopwatch)(nil)
)

// Counter wraps a metric.Counter to satisfy the Metric interface.
// Tracks whether it has been updated since the last flush via a dirty flag.
type Counter struct {
	*metric.Counter
	metric.IsNonExportableMetric
	IsClusterMetric
	dirty atomic.Bool
}

// NewCounter creates a new Counter with the given metadata. The
// metadata is automatically registered so that the cmreader can
// materialize this metric from the rangefeed.
func NewCounter(metadata metric.Metadata) *Counter {
	ensureClusterMetricRegistered(metadata.Name, metadata)
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
	IsClusterMetric
	dirty atomic.Bool
}

// NewGauge creates a new Gauge with the given metadata. The
// metadata is automatically registered so that the cmreader can
// materialize this metric from the rangefeed.
func NewGauge(metadata metric.Metadata) *Gauge {
	ensureClusterMetricRegistered(metadata.Name, metadata)
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

// WriteStopwatch wraps a metric.Gauge to record the unix-second
// timestamp at which Start() was called. The stored value is the
// timestamp itself, not elapsed time. Dirty tracking ensures that
// only updated stopwatches are flushed to storage.
type WriteStopwatch struct {
	*metric.Gauge
	metric.IsNonExportableMetric
	IsClusterMetric
	dirty      atomic.Bool
	timeSource timeutil.TimeSource
}

// NewWriteStopwatch creates a new WriteStopwatch with the given
// metadata and time source. The metadata is automatically registered
// so that the cmreader can materialize this metric from the rangefeed.
func NewWriteStopwatch(metadata metric.Metadata, timeSource timeutil.TimeSource) *WriteStopwatch {
	ensureClusterMetricRegistered(metadata.Name, metadata)
	return &WriteStopwatch{
		Gauge:      metric.NewGauge(metadata),
		timeSource: timeSource,
	}
}

// SetStartTime records the current time as the start time and marks
// dirty.
func (s *WriteStopwatch) SetStartTime() {
	s.Gauge.Update(s.timeSource.Now().Unix())
	s.dirty.Store(true)
}

// Value returns the stored unix timestamp (seconds), or 0 if not started.
func (s *WriteStopwatch) Value() int64 {
	return s.Gauge.Value()
}

// IsDirty returns true if SetStartTime has been called since the last flush.
func (s *WriteStopwatch) IsDirty() bool {
	return s.dirty.Load()
}

// Reset clears the dirty flag. Called after a successful flush.
func (s *WriteStopwatch) Reset() {
	s.dirty.Store(false)
}

// Inspect calls the given closure with the WriteStopwatch itself.
func (s *WriteStopwatch) Inspect(f func(interface{})) {
	f(s)
}

// GetType returns GAUGE. Although the storage layer persists a distinct
// "STOPWATCH" type string (via MetricTypeString), the metric itself behaves as
// a gauge for prometheus and TSDB purposes, but should also should not be used
// for either.
func (s *WriteStopwatch) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// GetLabels returns nil for unlabeled stopwatches.
func (s *WriteStopwatch) GetLabels() map[string]string {
	return nil
}
