// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/gogo/protobuf/proto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// AggCounter maintains a value as the sum of its children. The counter will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggCounter struct {
	g metric.Counter
	childSet
}

var _ metric.Iterable = (*AggCounter)(nil)
var _ metric.PrometheusIterable = (*AggCounter)(nil)
var _ metric.PrometheusExportable = (*AggCounter)(nil)

// NewCounter constructs a new AggCounter.
func NewCounter(metadata metric.Metadata, childLabels ...string) *AggCounter {
	c := &AggCounter{g: *metric.NewCounter(metadata)}
	c.init(childLabels)
	return c
}

// GetName is part of the metric.Iterable interface.
func (c *AggCounter) GetName() string { return c.g.GetName() }

// GetHelp is part of the metric.Iterable interface.
func (c *AggCounter) GetHelp() string { return c.g.GetHelp() }

// GetMeasurement is part of the metric.Iterable interface.
func (c *AggCounter) GetMeasurement() string { return c.g.GetMeasurement() }

// GetUnit is part of the metric.Iterable interface.
func (c *AggCounter) GetUnit() metric.Unit { return c.g.GetUnit() }

// GetMetadata is part of the metric.Iterable interface.
func (c *AggCounter) GetMetadata() metric.Metadata { return c.g.GetMetadata() }

// Inspect is part of the metric.Iterable interface.
func (c *AggCounter) Inspect(f func(interface{})) { f(c) }

// GetType is part of the metric.PrometheusExportable interface.
func (c *AggCounter) GetType() *io_prometheus_client.MetricType {
	return c.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (c *AggCounter) GetLabels() []*io_prometheus_client.LabelPair {
	return c.g.GetLabels()
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (c *AggCounter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return c.g.ToPrometheusMetric()
}

// Count returns the aggregate count of all of its current and past children.
func (c *AggCounter) Count() int64 {
	return c.g.Count()
}

// AddChild adds a Counter to this AggCounter. This method panics if a Counter
// already exists for this set of labelVals.
func (c *AggCounter) AddChild(labelVals ...string) *Counter {
	child := &Counter{
		parent:           c,
		labelValuesSlice: labelValuesSlice(labelVals),
	}
	c.add(child)
	return child
}

// Counter is a child of a AggCounter. When it is incremented, so too is the
// parent. When metrics are collected by prometheus, each of the children will
// appear with a distinct label, however, when cockroach internally collects
// metrics, only the parent is collected.
type Counter struct {
	parent *AggCounter
	labelValuesSlice
	value int64
}

// ToPrometheusMetric constructs a prometheus metric for this Counter.
func (g *Counter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Counter: &io_prometheus_client.Counter{
			Value: proto.Float64(float64(g.Value())),
		},
	}
}

// Unlink unlinks this child from the parent, i.e. the parent will no longer
// track this child (i.e. won't generate labels for it, etc). However, the child
// will continue to be functional and reference the parent, meaning updates to
// it will be reflected in the aggregate stored in the parent.
//
// See tenantrate.TestUseAfterRelease.
func (g *Counter) Unlink() {
	g.parent.remove(g)
}

// Value returns the AggCounter's current value.
func (g *Counter) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

// Inc increments the AggCounter's value.
func (g *Counter) Inc(i int64) {
	g.parent.g.Inc(i)
	atomic.AddInt64(&g.value, i)
}

// AggCounterFloat64 maintains a value as the sum of its children. The counter will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggCounterFloat64 struct {
	g metric.CounterFloat64
	childSet
}

var _ metric.Iterable = (*AggCounterFloat64)(nil)
var _ metric.PrometheusIterable = (*AggCounterFloat64)(nil)
var _ metric.PrometheusExportable = (*AggCounterFloat64)(nil)

// NewCounterFloat64 constructs a new AggCounterFloat64.
func NewCounterFloat64(metadata metric.Metadata, childLabels ...string) *AggCounterFloat64 {
	c := &AggCounterFloat64{g: *metric.NewCounterFloat64(metadata)}
	c.init(childLabels)
	return c
}

// GetName is part of the metric.Iterable interface.
func (c *AggCounterFloat64) GetName() string { return c.g.GetName() }

// GetHelp is part of the metric.Iterable interface.
func (c *AggCounterFloat64) GetHelp() string { return c.g.GetHelp() }

// GetMeasurement is part of the metric.Iterable interface.
func (c *AggCounterFloat64) GetMeasurement() string { return c.g.GetMeasurement() }

// GetUnit is part of the metric.Iterable interface.
func (c *AggCounterFloat64) GetUnit() metric.Unit { return c.g.GetUnit() }

// GetMetadata is part of the metric.Iterable interface.
func (c *AggCounterFloat64) GetMetadata() metric.Metadata { return c.g.GetMetadata() }

// Inspect is part of the metric.Iterable interface.
func (c *AggCounterFloat64) Inspect(f func(interface{})) { f(c) }

// GetType is part of the metric.PrometheusExportable interface.
func (c *AggCounterFloat64) GetType() *io_prometheus_client.MetricType {
	return c.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (c *AggCounterFloat64) GetLabels() []*io_prometheus_client.LabelPair {
	return c.g.GetLabels()
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (c *AggCounterFloat64) ToPrometheusMetric() *io_prometheus_client.Metric {
	return c.g.ToPrometheusMetric()
}

// Count returns the aggregate count of all of its current and past children.
func (c *AggCounterFloat64) Count() float64 {
	return c.g.Count()
}

// AddChild adds a Counter to this AggCounter. This method panics if a Counter
// already exists for this set of labelVals.
func (c *AggCounterFloat64) AddChild(labelVals ...string) *CounterFloat64 {
	child := &CounterFloat64{
		parent:           c,
		labelValuesSlice: labelValuesSlice(labelVals),
	}
	c.add(child)
	return child
}

// CounterFloat64 is a child of a AggCounter. When it is incremented, so too is the
// parent. When metrics are collected by prometheus, each of the children will
// appear with a distinct label, however, when cockroach internally collects
// metrics, only the parent is collected.
type CounterFloat64 struct {
	parent *AggCounterFloat64
	labelValuesSlice
	value metric.CounterFloat64
}

// ToPrometheusMetric constructs a prometheus metric for this Counter.
func (g *CounterFloat64) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Counter: &io_prometheus_client.Counter{
			Value: proto.Float64(g.Value()),
		},
	}
}

// Unlink unlinks this child from the parent, i.e. the parent will no longer
// track this child (i.e. won't generate labels for it, etc). However, the child
// will continue to be functional and reference the parent, meaning updates to
// it will be reflected in the aggregate stored in the parent.
//
// See tenantrate.TestUseAfterRelease.
func (g *CounterFloat64) Unlink() {
	g.parent.remove(g)
}

// Value returns the AggCounter's current value.
func (g *CounterFloat64) Value() float64 {
	return g.value.Count()
}

// Inc increments the AggCounter's value.
func (g *CounterFloat64) Inc(i float64) {
	g.parent.g.Inc(i)
	g.value.Inc(i)
}

// UpdateIfHigher sets the counter's value only if it's higher
// than the currently set one. It's assumed the caller holds
func (g *CounterFloat64) UpdateIfHigher(i float64) {
	g.value.UpdateIfHigher(i)
}
