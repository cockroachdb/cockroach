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
	c.initWithBTreeStorageType(childLabels)
	return c
}

// GetName is part of the metric.Iterable interface.
func (c *AggCounter) GetName(useStaticLabels bool) string { return c.g.GetName(useStaticLabels) }

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
func (c *AggCounter) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return c.g.GetLabels(useStaticLabels)
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

// RemoveChild removes a Gauge from this AggGauge. This method panics if a Gauge
// does not exist for this set of labelVals.
func (g *AggCounter) RemoveChild(labelVals ...string) {
	key := &Counter{labelValuesSlice: labelValuesSlice(labelVals)}
	g.remove(key)
}

// Counter is a child of a AggCounter. When it is incremented, so too is the
// parent. When metrics are collected by prometheus, each of the children will
// appear with a distinct label, however, when cockroach internally collects
// metrics, only the parent is collected.
type Counter struct {
	parent *AggCounter
	labelValuesSlice
	value atomic.Int64
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
	return g.value.Load()
}

// Inc increments the AggCounter's value.
func (g *Counter) Inc(i int64) {
	g.parent.g.Inc(i)
	g.value.Add(i)
}

// UpdateIfHigher updates the AggCounter's value.
//
// This method may not perform well under high concurrency,
// so it should only be used if the Counter is not expected
// to be frequently Update'd or Inc'd.
func (g *Counter) UpdateIfHigher(newValue int64) {
	var delta int64
	for {
		delta = newValue - g.value.Load()
		if delta <= 0 {
			return
		}
		if g.value.CompareAndSwap(newValue-delta, newValue) {
			break
		}
		// Raced with concurrent update, try again.
	}
	g.parent.g.Inc(delta) // delta > 0
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
	c.initWithBTreeStorageType(childLabels)
	return c
}

// GetName is part of the metric.Iterable interface.
func (c *AggCounterFloat64) GetName(useStaticLabels bool) string { return c.g.GetName(useStaticLabels) }

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
func (c *AggCounterFloat64) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return c.g.GetLabels(useStaticLabels)
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
// than the currently set one.
func (g *CounterFloat64) UpdateIfHigher(newValue float64) {
	old, updated := g.value.UpdateIfHigher(newValue)
	if !updated {
		return
	}
	g.parent.g.Inc(newValue - old)
}

// SQLCounter maintains a value as the sum of its children. The counter will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface. SQLCounter differs from AggCounter in that
// a SQLCounter creates child metrics dynamically while AggCounter needs the
// child creation up front.
type SQLCounter struct {
	g metric.Counter
	*SQLMetric
}

var _ metric.Iterable = (*SQLCounter)(nil)
var _ metric.PrometheusReinitialisable = (*SQLCounter)(nil)
var _ metric.PrometheusExportable = (*SQLCounter)(nil)

// NewSQLCounter constructs a new SQLCounter.
func NewSQLCounter(metadata metric.Metadata) *SQLCounter {
	c := &SQLCounter{
		g: *metric.NewCounter(metadata),
	}
	c.SQLMetric = NewSQLMetric(metric.LabelConfigDisabled)
	return c
}

// GetType is part of the metric.PrometheusExportable interface.
func (c *SQLCounter) GetType() *io_prometheus_client.MetricType {
	return c.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (c *SQLCounter) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return c.g.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (c *SQLCounter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return c.g.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (c *SQLCounter) GetName(useStaticLabels bool) string {
	return c.g.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (c *SQLCounter) GetHelp() string {
	return c.g.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (c *SQLCounter) GetMeasurement() string {
	return c.g.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (c *SQLCounter) GetUnit() metric.Unit {
	return c.g.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (c *SQLCounter) GetMetadata() metric.Metadata {
	return c.g.GetMetadata()
}

// Inspect is part of the metric.Iterable interface.
func (c *SQLCounter) Inspect(f func(interface{})) {
	f(c)
}

// Clear resets the counter to zero.
func (c *SQLCounter) Clear() {
	c.g.Clear()
}

// Count returns the aggregate count of all of its current and past children.
func (c *SQLCounter) Count() int64 {
	return c.g.Count()
}

// Inc increments the counter value by i for the given label values. If a
// counter with the given label values doesn't exist yet, it creates a new
// counter based on labelConfig and increments it. Inc increments parent metrics
// irrespective of labelConfig.
func (c *SQLCounter) Inc(i int64, db, app string) {
	c.g.Inc(i)

	childMetric, isChildMetricEnabled := c.getChildByLabelConfig(c.createChildCounter, db, app)
	if !isChildMetricEnabled {
		return
	}

	childMetric.(*SQLChildCounter).Inc(i)
}

func (c *SQLCounter) createChildCounter(labelValues labelValuesSlice) ChildMetric {
	return &SQLChildCounter{
		labelValuesSlice: labelValues,
	}
}

// SQLChildCounter is a child of a SQLCounter. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type SQLChildCounter struct {
	labelValuesSlice
	value metric.Counter
}

// ToPrometheusMetric constructs a prometheus metric for this Counter.
func (s *SQLChildCounter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Counter: &io_prometheus_client.Counter{
			Value: proto.Float64(float64(s.Value())),
		},
	}
}

// Value returns the SQLChildCounter's current value.
func (s *SQLChildCounter) Value() int64 {
	return s.value.Count()
}

// Inc increments the SQLChildCounter's value.
func (s *SQLChildCounter) Inc(i int64) {
	s.value.Inc(i)
}
