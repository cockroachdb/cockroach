// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// AggGauge maintains a value as the sum of its children. The gauge will report
// to crdb-internal time series only the aggregate sum of all of its children,
// while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggGauge struct {
	g *metric.Gauge
	*baseAggMetric
}

var _ metric.Iterable = (*AggGauge)(nil)
var _ metric.PrometheusIterable = (*AggGauge)(nil)
var _ metric.PrometheusExportable = (*AggGauge)(nil)

// NewGauge constructs a new AggGauge.
func NewGauge(metadata metric.Metadata, childLabels ...string) *AggGauge {
	gauge := metric.NewGauge(metadata)
	base := newBaseAggMetric(gauge)
	g := &AggGauge{g: gauge, baseAggMetric: base}
	g.initWithBTreeStorageType(childLabels)
	return g
}

// NewFunctionalGauge constructs a new AggGauge whose value is determined when
// asked for by calling the provided function with the current values of every
// child of the AggGauge.
func NewFunctionalGauge(
	metadata metric.Metadata, f func(childValues []int64) int64, childLabels ...string,
) *AggGauge {
	g := &AggGauge{}
	gaugeFn := func() int64 {
		values := make([]int64, 0)
		g.childSet.mu.Lock()
		defer g.childSet.mu.Unlock()
		g.childSet.mu.children.ForEach(func(metric ChildMetric) {
			cg := metric.(*Gauge)
			values = append(values, cg.Value())
		})
		return f(values)
	}
	g.g = metric.NewFunctionalGauge(metadata, gaugeFn)
	g.baseAggMetric = newBaseAggMetric(g.g)
	g.initWithBTreeStorageType(childLabels)
	return g
}

// Inspect is part of the metric.Iterable interface.
func (g *AggGauge) Inspect(f func(interface{})) { f(g) }

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

// AddFunctionalChild adds a Gauge to this AggGauge where the value is
// determined when asked for. This method panics if a Gauge already exists for
// this set of labelVals.
func (g *AggGauge) AddFunctionalChild(fn func() int64, labelVals ...string) *Gauge {
	child := &Gauge{
		parent:           g,
		labelValuesSlice: labelValuesSlice(labelVals),
		fn:               fn,
	}
	g.add(child)
	return child
}

// Inc increments the Gauge value by i for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and increments it. Panics if the number of label values doesn't
// match the number of labels defined for this Gauge.
func (g *AggGauge) Inc(i int64, labelVals ...string) {
	child := g.getOrCreateChild(labelVals...)
	child.Inc(i)
}

// Dec decrements the Gauge value by i for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and decrements it. Panics if the number of label values doesn't
// match the number of labels defined for this Gauge.
func (g *AggGauge) Dec(i int64, labelVals ...string) {
	child := g.getOrCreateChild(labelVals...)
	child.Dec(i)
}

// Update updates the Gauge value by val for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and updates it. Panics if the number of label values doesn't
// match the number of labels defined for this Gauge.
func (g *AggGauge) Update(val int64, labelVals ...string) {
	child := g.getOrCreateChild(labelVals...)
	child.Update(val)
}

// UpdateFn updates the Gauge value by val for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and updates it. Panics if the number of label values doesn't
// match the number of labels defined for this Gauge.
func (g *AggGauge) UpdateFn(f func() int64, labelVals ...string) {
	child := g.getOrCreateChild(labelVals...)
	child.UpdateFn(f)
}

// GetChild returns the gauge for a set of given label values
// if it exists. If the labels specified are incorrect, or if
// the child doesn't exist, it returns a nil value.
func (g *AggGauge) GetChild(labelVals ...string) *Gauge {
	child, ok := g.get(labelVals...)
	if !ok {
		return nil
	}
	return child.(*Gauge)
}

func (g *AggGauge) getOrCreateChild(labelVals ...string) *Gauge {
	if len(g.labels) != len(labelVals) {
		panic(errors.AssertionFailedf(
			"cannot increment child with %d label values %v to a metric with %d labels %v",
			len(labelVals), labelVals, len(g.labels), g.labels))
	}

	// If the child already exists then return it.
	if child, ok := g.get(labelVals...); ok {
		return child.(*Gauge)
	}

	// Otherwise, create a new child then return it.
	child := g.AddChild(labelVals...)
	return child
}

// RemoveChild removes a Gauge from this AggGauge. This method panics if a Gauge
// does not exist for this set of labelVals.
func (g *AggGauge) RemoveChild(labelVals ...string) {
	key := &Gauge{labelValuesSlice: labelValuesSlice(labelVals)}
	g.remove(key)
}

// Gauge is a child of a AggGauge. When it is incremented or decremented, so
// too is the parent. When metrics are collected by prometheus, each of the
// children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type Gauge struct {
	labelValuesSlice
	parent *AggGauge
	value  int64
	fn     func() int64
}

// ToPrometheusMetric constructs a prometheus metric for this Gauge.
func (g *Gauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Gauge: &io_prometheus_client.Gauge{
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
func (g *Gauge) Unlink() {
	g.parent.remove(g)
}

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 {
	if g.fn != nil {
		return g.fn()
	}
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

// UpdateFn updates the function on the gauge.
func (g *Gauge) UpdateFn(fn func() int64) {
	g.fn = fn
}

// AggGaugeFloat64 maintains a value as the sum of its children. The gauge will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggGaugeFloat64 struct {
	g *metric.GaugeFloat64
	*baseAggMetric
}

var _ metric.Iterable = (*AggGaugeFloat64)(nil)
var _ metric.PrometheusIterable = (*AggGaugeFloat64)(nil)
var _ metric.PrometheusExportable = (*AggGaugeFloat64)(nil)

// NewGaugeFloat64 constructs a new AggGaugeFloat64.
func NewGaugeFloat64(metadata metric.Metadata, childLabels ...string) *AggGaugeFloat64 {
	gaugeFloat64 := metric.NewGaugeFloat64(metadata)
	base := newBaseAggMetric(gaugeFloat64)
	g := &AggGaugeFloat64{g: gaugeFloat64, baseAggMetric: base}
	g.initWithBTreeStorageType(childLabels)
	return g
}

// Inspect is part of the metric.Iterable interface.
func (g *AggGaugeFloat64) Inspect(f func(interface{})) { f(g) }

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

// Unlink unlinks this child from the parent, i.e. the parent will no longer
// track this child (i.e. won't generate labels for it, etc). However, the child
// will continue to be functional and reference the parent, meaning updates to
// it will be reflected in the aggregate stored in the parent.
//
// See tenantrate.TestUseAfterRelease.
func (g *GaugeFloat64) Unlink() {
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

// SQLGauge maintains a gauge as the sum of its children. The gauge will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface. SQLGauge differs from AggGauge in that
// a SQLGauge creates child metrics dynamically while AggGauge needs the
// child creation up front.
type SQLGauge struct {
	g *metric.Gauge
	*SQLMetric
}

var _ metric.Iterable = (*SQLGauge)(nil)
var _ metric.PrometheusReinitialisable = (*SQLGauge)(nil)
var _ metric.PrometheusExportable = (*SQLGauge)(nil)

func NewSQLGauge(metadata metric.Metadata) *SQLGauge {
	gauge := metric.NewGauge(metadata)
	g := &SQLGauge{
		g: gauge,
	}
	g.SQLMetric = NewSQLMetric(metric.LabelConfigDisabled, gauge)
	return g
}

// Inspect is part of the metric.Iterable interface.
func (sg *SQLGauge) Inspect(f func(interface{})) {
	f(sg)
}

// Value returns the aggregate sum of all of its current children.
func (sg *SQLGauge) Value() int64 {
	return sg.g.Value()
}

// Update updates the Gauge value by i for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and updates it. Update increments parent metrics
// irrespective of labelConfig.
func (sg *SQLGauge) Update(val int64, db, app string) {
	childMetric, isChildMetricEnabled := sg.getChildByLabelConfig(sg.createChildGauge, db, app)

	// If the label configuration is either LabelConfigDisabled or unrecognised,
	// then only update aggregated gauge value.
	if !isChildMetricEnabled {
		sg.g.Update(val)
		return
	}

	delta := val - childMetric.(*SQLChildGauge).Value()
	sg.g.Inc(delta)
	childMetric.(*SQLChildGauge).Update(val)
}

// Inc increments the Gauge value by i for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and increments it. Inc increments parent metrics
// irrespective of labelConfig.
func (sg *SQLGauge) Inc(i int64, db, app string) {
	sg.g.Inc(i)

	childMetric, isChildMetricEnabled := sg.getChildByLabelConfig(sg.createChildGauge, db, app)
	if !isChildMetricEnabled {
		return
	}
	childMetric.(*SQLChildGauge).Inc(i)
}

// Dec decrements the Gauge value by i for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and decrements it. Dec decrements parent metrics
// // irrespective of labelConfig.
func (sg *SQLGauge) Dec(i int64, db, app string) {
	sg.g.Dec(i)

	childMetric, isChildMetricEnabled := sg.getChildByLabelConfig(sg.createChildGauge, db, app)
	if !isChildMetricEnabled {
		return
	}
	childMetric.(*SQLChildGauge).Dec(i)
}

func (sg *SQLGauge) createChildGauge(labelValues labelValuesSlice) ChildMetric {
	return &SQLChildGauge{
		labelValuesSlice: labelValues,
	}
}

// SQLChildGauge is a child of a SQLGauge. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type SQLChildGauge struct {
	labelValuesSlice
	gauge metric.Gauge
}

// ToPrometheusMetric constructs a prometheus metric for this Gauge.
func (scg *SQLChildGauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Gauge: &io_prometheus_client.Gauge{
			Value: proto.Float64(float64(scg.Value())),
		},
	}
}

// Value returns the SQLChildGauge's current gauge.
func (scg *SQLChildGauge) Value() int64 {
	return scg.gauge.Value()
}

// Update sets the gauge's value.
func (scg *SQLChildGauge) Update(val int64) {
	scg.gauge.Update(val)
}

// Inc increments the gauge's value.
func (scg *SQLChildGauge) Inc(i int64) {
	scg.gauge.Inc(i)
}

// Dec decrements the gauge's value.
func (scg *SQLChildGauge) Dec(i int64) {
	scg.gauge.Dec(i)
}

// HighCardinalityGauge is similar to AggGauge but uses cache storage instead of B-tree,
// allowing for automatic eviction of less frequently used child metrics.
// This is useful when dealing with high cardinality metrics that might exceed resource limits.
type HighCardinalityGauge struct {
	g *metric.Gauge
	*baseAggMetric
	labelSliceCache *metric.LabelSliceCache
}

var _ metric.Iterable = (*HighCardinalityGauge)(nil)
var _ metric.PrometheusEvictable = (*HighCardinalityGauge)(nil)

// NewHighCardinalityGauge constructs a new HighCardinalityGauge that uses cache storage
// with eviction for child metrics. The opts parameter contains the metadata and allows configuring
// the maximum number of label combinations (MaxLabelValues) and retention time (RetentionTimeTillEviction).
// If opts has zero values for MaxLabelValues or RetentionTimeTillEviction, defaults will be used.
func NewHighCardinalityGauge(
	opts metric.HighCardinalityMetricOptions, childLabels ...string,
) *HighCardinalityGauge {
	gauge := metric.NewGauge(opts.Metadata)
	base := newBaseAggMetric(gauge)
	g := &HighCardinalityGauge{g: gauge, baseAggMetric: base}
	g.initWithCacheStorageType(childLabels, opts.Metadata.Name, opts)
	return g
}

// Inspect is part of the metric.Iterable interface.
func (g *HighCardinalityGauge) Inspect(f func(interface{})) { f(g) }

// Value returns the aggregate sum of all of its current children.
func (g *HighCardinalityGauge) Value() int64 {
	return g.g.Value()
}

// Inc increments the gauge value by i for the given label values. If a
// gauge with the given label values doesn't exist yet, it creates a new
// gauge and increments it. Inc increments parent metrics as well.
func (g *HighCardinalityGauge) Inc(i int64, labelValues ...string) {
	g.g.Inc(i)

	childMetric := g.GetOrAddChild(labelValues...)

	if childMetric != nil {
		childMetric.Inc(i)
	}
}

// Dec decrements the gauge value by i for the given label values. If a
// gauge with the given label values doesn't exist yet, it creates a new
// gauge and decrements it. Dec decrements parent metrics as well.
func (g *HighCardinalityGauge) Dec(i int64, labelValues ...string) {
	g.g.Dec(i)

	childMetric := g.GetOrAddChild(labelValues...)

	if childMetric != nil {
		childMetric.Dec(i)
	}
}

// Update sets the gauge value to val for the given label values. If a
// gauge with the given label values doesn't exist yet, it creates a new
// gauge and sets it. Update updates parent metrics as well.
//
// The parent metric value represents the sum of all children. The parent
// metric is updated by the delta (new - old) to maintain the aggregate
// sum of all children.
// For example, if a child gauge changes from 10 to 25, the
// parent is incremented by 15, preserving the total sum across all children.
func (g *HighCardinalityGauge) Update(val int64, labelValues ...string) {
	childMetric := g.GetOrAddChild(labelValues...)

	if childMetric != nil {
		old := childMetric.Value()
		// Increment the parent by the delta of the child metric.
		g.g.Inc(val - old)
		childMetric.Update(val)
	}
}

// Each is part of the metric.PrometheusIterable interface.
func (g *HighCardinalityGauge) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	g.EachWithLabels(labels, f, g.labelSliceCache)
}

// InitializeMetrics is part of the PrometheusEvictable interface.
func (g *HighCardinalityGauge) InitializeMetrics(labelCache *metric.LabelSliceCache) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.labelSliceCache = labelCache
}

// GetOrAddChild returns the existing child gauge for the given label values,
// or creates a new one if it doesn't exist. This is the preferred method for
// cache-based storage to avoid panics on existing keys.
func (g *HighCardinalityGauge) GetOrAddChild(labelVals ...string) *HighCardinalityChildGauge {

	if len(labelVals) == 0 {
		return nil
	}

	// Create a LabelSliceCacheKey from the tenantID.
	key := metric.LabelSliceCacheKey(metricKey(labelVals...))

	child := g.getOrAddWithLabelSliceCache(g.GetMetadata().Name, g.createHighCardinalityChildGauge, g.labelSliceCache, labelVals...)

	g.labelSliceCache.Upsert(key, &metric.LabelSliceCacheValue{
		LabelValues: labelVals,
	})

	return child.(*HighCardinalityChildGauge)
}

func (g *HighCardinalityGauge) createHighCardinalityChildGauge(
	key uint64, cache *metric.LabelSliceCache,
) LabelSliceCachedChildMetric {
	return &HighCardinalityChildGauge{
		LabelSliceCacheKey: metric.LabelSliceCacheKey(key),
		LabelSliceCache:    cache,
		createdAt:          timeutil.Now(),
	}
}

// HighCardinalityChildGauge is a child of a HighCardinalityGauge. When metrics are
// collected by prometheus, each of the children will appear with a distinct label,
// however, when cockroach internally collects metrics, only the parent is collected.
type HighCardinalityChildGauge struct {
	metric.LabelSliceCacheKey
	value metric.Gauge
	*metric.LabelSliceCache
	createdAt time.Time
}

func (g *HighCardinalityChildGauge) CreatedAt() time.Time {
	return g.createdAt
}

func (g *HighCardinalityChildGauge) DecrementLabelSliceCacheReference() {
	g.LabelSliceCache.DecrementAndDeleteIfZero(g.LabelSliceCacheKey)
}

// ToPrometheusMetric constructs a prometheus metric for this HighCardinalityChildGauge.
func (g *HighCardinalityChildGauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Gauge: &io_prometheus_client.Gauge{
			Value: proto.Float64(float64(g.Value())),
		},
	}
}

func (g *HighCardinalityChildGauge) labelValues() []string {
	lv, ok := g.LabelSliceCache.Get(g.LabelSliceCacheKey)
	if !ok {
		return nil
	}
	return lv.LabelValues
}

// Value returns the HighCardinalityChildGauge's current value.
func (g *HighCardinalityChildGauge) Value() int64 {
	return g.value.Value()
}

// Inc increments the HighCardinalityChildGauge's value.
func (g *HighCardinalityChildGauge) Inc(i int64) {
	g.value.Inc(i)
}

// Dec decrements the HighCardinalityChildGauge's value.
func (g *HighCardinalityChildGauge) Dec(i int64) {
	g.value.Dec(i)
}

// Update sets the HighCardinalityChildGauge's value.
func (g *HighCardinalityChildGauge) Update(val int64) {
	g.value.Update(val)
}
