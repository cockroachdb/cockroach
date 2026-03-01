// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// AggCounter maintains a value as the sum of its children. The counter will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggCounter struct {
	g *metric.Counter
	*baseAggMetric
}

var _ metric.Iterable = (*AggCounter)(nil)
var _ metric.PrometheusIterable = (*AggCounter)(nil)
var _ metric.PrometheusExportable = (*AggCounter)(nil)

// NewCounter constructs a new AggCounter.
func NewCounter(metadata metric.Metadata, childLabels ...string) *AggCounter {
	counter := metric.NewCounter(metadata)
	base := newBaseAggMetric(counter)
	c := &AggCounter{g: counter, baseAggMetric: base}
	c.initWithBTreeStorageType(childLabels)
	return c
}

// Inspect is part of the metric.Iterable interface.
func (c *AggCounter) Inspect(f func(interface{})) { f(c) }

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

// RemoveChild removes a Counter from this AggCounter. This method panics if a Counter
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
	g *metric.CounterFloat64
	*baseAggMetric
}

var _ metric.Iterable = (*AggCounterFloat64)(nil)
var _ metric.PrometheusIterable = (*AggCounterFloat64)(nil)
var _ metric.PrometheusExportable = (*AggCounterFloat64)(nil)

// NewCounterFloat64 constructs a new AggCounterFloat64.
func NewCounterFloat64(metadata metric.Metadata, childLabels ...string) *AggCounterFloat64 {
	counterFloat64 := metric.NewCounterFloat64(metadata)
	base := newBaseAggMetric(counterFloat64)
	c := &AggCounterFloat64{g: counterFloat64, baseAggMetric: base}
	c.initWithBTreeStorageType(childLabels)
	return c
}

// Inspect is part of the metric.Iterable interface.
func (c *AggCounterFloat64) Inspect(f func(interface{})) { f(c) }

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
	g *metric.Counter
	*SQLMetric
}

var _ metric.Iterable = (*SQLCounter)(nil)
var _ metric.PrometheusReinitialisable = (*SQLCounter)(nil)
var _ metric.PrometheusExportable = (*SQLCounter)(nil)

// NewSQLCounter constructs a new SQLCounter.
func NewSQLCounter(metadata metric.Metadata) *SQLCounter {
	counter := metric.NewCounter(metadata)
	c := &SQLCounter{
		g: counter,
	}
	c.SQLMetric = NewSQLMetric(metric.LabelConfigDisabled, counter)
	return c
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

// HighCardinalityCounter is similar to AggCounter but uses cache storage instead of B-tree,
// allowing for automatic eviction of less frequently used child metrics.
// This is useful when dealing with high cardinality metrics that might exceed resource limits.
type HighCardinalityCounter struct {
	g *metric.Counter
	*baseAggMetric
	labelSliceCache *metric.LabelSliceCache
}

var _ metric.Iterable = (*HighCardinalityCounter)(nil)
var _ metric.PrometheusEvictable = (*HighCardinalityCounter)(nil)

// NewHighCardinalityCounter constructs a new HighCardinalityCounter that uses cache storage
// with eviction for child metrics. The opts parameter contains the metadata and allows configuring
// the maximum number of label combinations (MaxLabelValues) and retention time (RetentionTimeTillEviction).
// If opts has zero values for MaxLabelValues or RetentionTimeTillEviction, defaults will be used.
func NewHighCardinalityCounter(
	opts metric.HighCardinalityMetricOptions, childLabels ...string,
) *HighCardinalityCounter {
	counter := metric.NewCounter(opts.Metadata)
	base := newBaseAggMetric(counter)
	c := &HighCardinalityCounter{g: counter, baseAggMetric: base}
	c.initWithCacheStorageType(childLabels, opts.Metadata.Name, opts)
	return c
}

// Inspect is part of the metric.Iterable interface.
func (c *HighCardinalityCounter) Inspect(f func(interface{})) { f(c) }

// Count returns the aggregate count of all of its current and past children.
func (c *HighCardinalityCounter) Count() int64 {
	return c.g.Count()
}

// Inc increments the counter value by i for the given label values. If a
// counter with the given label values doesn't exist yet, it creates a new
// counter and increments it. Inc increments parent metrics as well.
func (c *HighCardinalityCounter) Inc(i int64, labelValues ...string) {
	c.g.Inc(i)

	childMetric := c.GetOrAddChild(labelValues...)

	if childMetric != nil {
		childMetric.Inc(i)
	}

}

// Each is part of the metric.PrometheusIterable interface.
func (c *HighCardinalityCounter) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	c.EachWithLabels(labels, f, c.labelSliceCache)
}

// InitializeMetrics is part of the PrometheusEvictable interface.
func (c *HighCardinalityCounter) InitializeMetrics(labelCache *metric.LabelSliceCache) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.labelSliceCache = labelCache
}

// GetOrAddChild returns the existing child counter for the given label values,
// or creates a new one if it doesn't exist. This is the preferred method for
// cache-based storage to avoid panics on existing keys.
func (c *HighCardinalityCounter) GetOrAddChild(labelVals ...string) *HighCardinalityChildCounter {

	if len(labelVals) == 0 {
		return nil
	}

	// Create a LabelSliceCacheKey from the tenantID.
	key := metric.LabelSliceCacheKey(metricKey(labelVals...))

	child := c.getOrAddWithLabelSliceCache(c.GetMetadata().Name, c.createHighCardinalityChildCounter, c.labelSliceCache, labelVals...)

	c.labelSliceCache.Upsert(key, &metric.LabelSliceCacheValue{
		LabelValues: labelVals,
	})

	return child.(*HighCardinalityChildCounter)
}

func (c *HighCardinalityCounter) createHighCardinalityChildCounter(
	key uint64, cache *metric.LabelSliceCache,
) LabelSliceCachedChildMetric {
	return &HighCardinalityChildCounter{
		LabelSliceCacheKey: metric.LabelSliceCacheKey(key),
		LabelSliceCache:    cache,
		createdAt:          timeutil.Now(),
	}
}

// HighCardinalityChildCounter is a child of a HighCardinalityCounter. When metrics are
// collected by prometheus, each of the children will appear with a distinct label,
// however, when cockroach internally collects  metrics, only the parent is collected.
type HighCardinalityChildCounter struct {
	metric.LabelSliceCacheKey
	value metric.Counter
	*metric.LabelSliceCache
	createdAt time.Time
}

func (c *HighCardinalityChildCounter) CreatedAt() time.Time {
	return c.createdAt
}

func (c *HighCardinalityChildCounter) DecrementLabelSliceCacheReference() {
	c.LabelSliceCache.DecrementAndDeleteIfZero(c.LabelSliceCacheKey)
}

// ToPrometheusMetric constructs a prometheus metric for this HighCardinalityChildCounter.
func (c *HighCardinalityChildCounter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Counter: &io_prometheus_client.Counter{
			Value: proto.Float64(float64(c.Value())),
		},
	}
}

func (c *HighCardinalityChildCounter) labelValues() []string {
	lv, ok := c.LabelSliceCache.Get(c.LabelSliceCacheKey)
	if !ok {
		return nil
	}
	return lv.LabelValues
}

// Value returns the HighCardinalityChildCounter's current value.
func (c *HighCardinalityChildCounter) Value() int64 {
	return c.value.Count()
}

// Inc increments the HighCardinalityChildCounter's value.
func (c *HighCardinalityChildCounter) Inc(i int64) {
	c.value.Inc(i)
}
