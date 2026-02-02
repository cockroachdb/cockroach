// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// ClusterMetric is the interface for metrics that can be written to the
// cluster metrics store. It provides a uniform way for the Writer to
// get values, check for changes, and clear state after flushing.
type ClusterMetric interface {
	metric.Iterable
	// Get returns the current value of the metric.
	Get() int64
	// IsDirty returns true if the metric has changed since the last flush.
	IsDirty() bool
	// Clear resets the metric's state after a successful flush.
	// For counters, this resets the count to zero.
	// For gauges, this clears the dirty flag (value is retained).
	Clear()
}

// Verify interface compliance.
var (
	_ ClusterMetric = (*ClusterCounter)(nil)
	_ ClusterMetric = (*ClusterGauge)(nil)
)

// ClusterCounter wraps a metric.Counter to satisfy the ClusterMetric interface.
// A counter is considered dirty if its count is greater than zero.
type ClusterCounter struct {
	*metric.Counter
}

// NewClusterCounter creates a new ClusterCounter with the given metadata.
func NewClusterCounter(metadata metric.Metadata) *ClusterCounter {
	return &ClusterCounter{
		Counter: metric.NewCounter(metadata),
	}
}

// Get returns the current count.
func (c *ClusterCounter) Get() int64 {
	return c.Counter.Count()
}

// IsDirty returns true if the counter has a non-zero count.
func (c *ClusterCounter) IsDirty() bool {
	return c.Counter.Count() > 0
}

// Clear resets the counter to zero.
func (c *ClusterCounter) Clear() {
	c.Counter.Clear()
}

// Inspect calls the given closure with the ClusterCounter itself.
// This overrides the embedded Counter's Inspect to ensure the registry
// sees ClusterCounter rather than the underlying Counter.
func (c *ClusterCounter) Inspect(f func(interface{})) {
	f(c)
}

// ClusterGauge wraps a metric.Gauge to track whether it has been updated
// since the last flush. Only gauges that have been explicitly updated will
// be written to the store.
type ClusterGauge struct {
	*metric.Gauge
	dirty atomic.Bool
}

// NewClusterGauge creates a new ClusterGauge with the given metadata.
func NewClusterGauge(metadata metric.Metadata) *ClusterGauge {
	return &ClusterGauge{
		Gauge: metric.NewGauge(metadata),
	}
}

// Update updates the gauge's value and marks it as dirty.
func (g *ClusterGauge) Update(v int64) {
	g.Gauge.Update(v)
	g.dirty.Store(true)
}

// Inc increments the gauge's value and marks it as dirty.
func (g *ClusterGauge) Inc(i int64) {
	g.Gauge.Inc(i)
	g.dirty.Store(true)
}

// Dec decrements the gauge's value and marks it as dirty.
func (g *ClusterGauge) Dec(i int64) {
	g.Gauge.Dec(i)
	g.dirty.Store(true)
}

// Get returns the current value of the gauge.
func (g *ClusterGauge) Get() int64 {
	return g.Gauge.Value()
}

// IsDirty returns true if the gauge has been updated since the last flush.
func (g *ClusterGauge) IsDirty() bool {
	return g.dirty.Load()
}

// Clear clears the dirty flag. Called after a successful flush.
// This does not reset the gauge value itself.
func (g *ClusterGauge) Clear() {
	g.dirty.Store(false)
}

// Inspect calls the given closure with the ClusterGauge itself.
// This overrides the embedded Gauge's Inspect to ensure the registry
// sees ClusterGauge rather than the underlying Gauge.
func (g *ClusterGauge) Inspect(f func(interface{})) {
	f(g)
}
