// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Metric is the interface for metrics that can be written to the
// cluster metrics store. It provides a uniform way for the Writer to
// get values, check for changes, and reset state after flushing.
type Metric interface {
	metric.Iterable
	// Get returns the current value of the metric.
	Get() int64
	// Type returns the metric type string (e.g. "counter", "gauge").
	Type() string
	// IsDirty returns true if the metric has changed since the last flush.
	IsDirty() bool
	// Reset resets the metric's state after a successful flush.
	// For counters, this resets the count to zero and clears the dirty flag.
	// For gauges, this clears the dirty flag (value is retained).
	Reset()
}

// Verify interface compliance.
var (
	_ Metric = (*Counter)(nil)
	_ Metric = (*Gauge)(nil)
)

// Counter wraps a metric.Counter to satisfy the Metric interface.
// Tracks whether it has been updated since the last flush via a dirty flag.
type Counter struct {
	*metric.Counter
	dirty atomic.Bool
}

// Gauge wraps a metric.Gauge to track whether it has been updated
// since the last flush. Only gauges that have been explicitly updated will
// be written to the store.
type Gauge struct {
	*metric.Gauge
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

// Get returns the current count.
func (c *Counter) Get() int64 {
	return c.Counter.Count()
}

// Type returns "counter".
func (c *Counter) Type() string {
	return "counter"
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

// Get returns the current value of the gauge.
func (g *Gauge) Get() int64 {
	return g.Gauge.Value()
}

// Type returns "gauge".
func (g *Gauge) Type() string {
	return "gauge"
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
