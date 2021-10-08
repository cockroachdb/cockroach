// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aggmetric

import (
	"math"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/gogo/protobuf/proto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// AggGauge maintains a value as the sum of its children. The gauge will report
// to crdb-internal time series only the aggregate sum of all of its children,
// while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggGauge struct {
	g metric.Gauge
	childSet
}

var _ metric.Iterable = (*AggGauge)(nil)
var _ metric.PrometheusIterable = (*AggGauge)(nil)
var _ metric.PrometheusExportable = (*AggGauge)(nil)

// NewGauge constructs a new AggGauge.
func NewGauge(metadata metric.Metadata, childLabels ...string) *AggGauge {
	g := &AggGauge{g: *metric.NewGauge(metadata)}
	g.init(childLabels)
	return g
}

// GetName is part of the metric.Iterable interface.
func (g *AggGauge) GetName() string { return g.g.GetName() }

// GetHelp is part of the metric.Iterable interface.
func (g *AggGauge) GetHelp() string { return g.g.GetHelp() }

// GetMeasurement is part of the metric.Iterable interface.
func (g *AggGauge) GetMeasurement() string { return g.g.GetMeasurement() }

// GetUnit is part of the metric.Iterable interface.
func (g *AggGauge) GetUnit() metric.Unit { return g.g.GetUnit() }

// GetMetadata is part of the metric.Iterable interface.
func (g *AggGauge) GetMetadata() metric.Metadata { return g.g.GetMetadata() }

// Inspect is part of the metric.Iterable interface.
func (g *AggGauge) Inspect(f func(interface{})) { f(g) }

// GetType is part of the metric.PrometheusExportable interface.
func (g *AggGauge) GetType() *io_prometheus_client.MetricType {
	return g.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (g *AggGauge) GetLabels() []*io_prometheus_client.LabelPair {
	return g.g.GetLabels()
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (g *AggGauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return g.g.ToPrometheusMetric()
}

// Value returns the aggregate sum of all of its current children.
func (g *AggGauge) Value() int64 {
	return g.g.Value()
}

// AddChild adds a Gauge to this AggGauge. This method panics if a Gauge
// already exists for this set of labelVals.
func (g *AggGauge) AddChild(labelVals ...string) *Gauge {
	child := &Gauge{
		parent:           g,
		labelValuesSlice: labelValuesSlice(labelVals),
	}
	g.add(child)
	return child
}

// Gauge is a child of a AggGauge. When it is incremented or decremented, so
// too is the parent. When metrics are collected by prometheus, each of the
// children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type Gauge struct {
	labelValuesSlice
	parent *AggGauge
	value  int64
}

// ToPrometheusMetric constructs a prometheus metric for this Gauge.
func (g *Gauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Gauge: &io_prometheus_client.Gauge{
			Value: proto.Float64(float64(g.Value())),
		},
	}
}

// Destroy is used to destroy the gauge associated with this child. The value of
// the Gauge will be decremented from its parent.
func (g *Gauge) Destroy() {
	g.Update(0)
	g.parent.remove(g)
}

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

// Inc increments the gauge's value.
func (g *Gauge) Inc(i int64) {
	g.parent.g.Inc(i)
	atomic.AddInt64(&g.value, i)
}

// Dec decrements the gauge's value.
func (g *Gauge) Dec(i int64) {
	g.Inc(-i)
}

// Update sets the gauge's value.
func (g *Gauge) Update(val int64) {
	old := atomic.SwapInt64(&g.value, val)
	g.parent.g.Inc(val - old)
}

// AggGaugeFloat64 maintains a value as the sum of its children. The gauge will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggGaugeFloat64 struct {
	g metric.GaugeFloat64
	childSet
}

var _ metric.Iterable = (*AggGaugeFloat64)(nil)
var _ metric.PrometheusIterable = (*AggGaugeFloat64)(nil)
var _ metric.PrometheusExportable = (*AggGaugeFloat64)(nil)

// NewGaugeFloat64 constructs a new AggGaugeFloat64.
func NewGaugeFloat64(metadata metric.Metadata, childLabels ...string) *AggGaugeFloat64 {
	g := &AggGaugeFloat64{g: *metric.NewGaugeFloat64(metadata)}
	g.init(childLabels)
	return g
}

// GetName is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) GetName() string { return g.g.GetName() }

// GetHelp is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) GetHelp() string { return g.g.GetHelp() }

// GetMeasurement is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) GetMeasurement() string { return g.g.GetMeasurement() }

// GetUnit is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) GetUnit() metric.Unit { return g.g.GetUnit() }

// GetMetadata is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) GetMetadata() metric.Metadata { return g.g.GetMetadata() }

// Inspect is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) Inspect(f func(interface{})) { f(g) }

// GetType is part of the metric.PrometheusExportable interface.
func (g *AggGaugeFloat64) GetType() *io_prometheus_client.MetricType {
	return g.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (g *AggGaugeFloat64) GetLabels() []*io_prometheus_client.LabelPair {
	return g.g.GetLabels()
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (g *AggGaugeFloat64) ToPrometheusMetric() *io_prometheus_client.Metric {
	return g.g.ToPrometheusMetric()
}

// Value returns the aggregate sum of all of its current children.
func (g *AggGaugeFloat64) Value() float64 {
	return g.g.Value()
}

// AddChild adds a GaugeFloat64 to this AggGaugeFloat64. This method panics if a GaugeFloat64
// already exists for this set of labelVals.
func (g *AggGaugeFloat64) AddChild(labelVals ...string) *GaugeFloat64 {
	child := &GaugeFloat64{
		parent:           g,
		labelValuesSlice: labelValuesSlice(labelVals),
	}
	g.add(child)
	return child
}

// GaugeFloat64 is a child of a AggGaugeFloat64. When it is incremented or
// decremented, so too is the parent. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when
// cockroach internally collects metrics, only the parent is collected.
type GaugeFloat64 struct {
	labelValuesSlice
	parent *AggGaugeFloat64
	bits   uint64
}

// ToPrometheusMetric constructs a prometheus metric for this GaugeFloat64.
func (g *GaugeFloat64) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Gauge: &io_prometheus_client.Gauge{
			Value: proto.Float64(g.Value()),
		},
	}
}

// Destroy is used to destroy the gauge associated with this child. The value of
// the GaugeFloat64 will be decremented from its parent.
func (g *GaugeFloat64) Destroy() {
	g.Update(0)
	g.parent.remove(g)
}

// Value returns the gauge's current value.
func (g *GaugeFloat64) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.bits))
}

// Update sets the gauge's value.
func (g *GaugeFloat64) Update(v float64) {
	oldBits := atomic.SwapUint64(&g.bits, math.Float64bits(v))
	g.parent.g.Inc(v - math.Float64frombits(oldBits))
}
