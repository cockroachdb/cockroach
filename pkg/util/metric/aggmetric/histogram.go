// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/tick"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	prometheusgo "github.com/prometheus/client_model/go"
)

var now = timeutil.Now

// TestingSetNow changes the clock used by the metric system. For use by
// testing to precisely control the clock. Also sets the time in the `tick`
// package, and pkg/util/metric.
//
// TODO(obs): I know this is janky. It's temporary. An upcoming patch will
// merge this package, pkg/util/metric, and pkg/util/aggmetric, after which
// we can get rid of all this TestingSetNow chaining.
func TestingSetNow(f func() time.Time) func() {
	tickNowResetFn := tick.TestingSetNow(f)
	metricNowResetFn := metric.TestingSetNow(f)
	origNow := now
	now = f
	return func() {
		now = origNow
		tickNowResetFn()
		metricNowResetFn()
	}
}

// AggHistogram maintains a value as the sum of its children. The histogram will
// report to crdb-internal time series only the aggregate histogram of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface.
type AggHistogram struct {
	h      metric.IHistogram
	create func() metric.IHistogram
	*baseAggMetric
	ticker struct {
		// We use a RWMutex, because we don't want child histograms to contend when
		// recording values, unless we're rotating histograms for the parent & children.
		// In this instance, the "writer" for the RWMutex is the ticker, and the "readers"
		// are all the child histograms recording their values.
		syncutil.RWMutex
		*tick.Ticker
	}
}

var _ metric.Iterable = (*AggHistogram)(nil)
var _ metric.PrometheusIterable = (*AggHistogram)(nil)
var _ metric.PrometheusExportable = (*AggHistogram)(nil)
var _ metric.WindowedHistogram = (*AggHistogram)(nil)
var _ metric.CumulativeHistogram = (*AggHistogram)(nil)

// NewHistogram constructs a new AggHistogram.
func NewHistogram(opts metric.HistogramOptions, childLabels ...string) *AggHistogram {
	create := func() metric.IHistogram {
		return metric.NewHistogram(opts)
	}
	h := create()
	base := newBaseAggMetric(h)
	a := &AggHistogram{
		h:             h,
		create:        create,
		baseAggMetric: base,
	}
	a.ticker.Ticker = tick.NewTicker(
		now(),
		opts.Duration/metric.WindowedHistogramWrapNum,
		func() {
			// Atomically rotate the histogram window for the
			// parent histogram, and all the child histograms.
			a.h.Tick()
			a.childSet.apply(func(childItem MetricItem) {
				childHist, ok := childItem.(*Histogram)
				if !ok {
					panic(errors.AssertionFailedf(
						"unable to assert type of child for histogram %q when rotating histogram windows",
						opts.Metadata.Name))
				}
				childHist.h.Tick()
			})
		})
	a.initWithBTreeStorageType(childLabels)
	return a
}

// Inspect is part of the metric.Iterable interface.
func (a *AggHistogram) Inspect(f func(interface{})) {
	func() {
		a.ticker.Lock()
		defer a.ticker.Unlock()
		tick.MaybeTick(&a.ticker)
	}()
	f(a)
}

// CumulativeSnapshot is part of the metric.CumulativeHistogram interface.
func (a *AggHistogram) CumulativeSnapshot() metric.HistogramSnapshot {
	return a.h.CumulativeSnapshot()
}

// WindowedSnapshot is part of the metric.WindowedHistogram interface.
func (a *AggHistogram) WindowedSnapshot() metric.HistogramSnapshot {
	return a.h.WindowedSnapshot()
}

// AddChild adds a Histogram to this AggHistogram. This method panics if a Histogram
// already exists for this set of labelVals.
func (a *AggHistogram) AddChild(labelVals ...string) *Histogram {
	child := &Histogram{
		parent:           a,
		labelValuesSlice: labelValuesSlice(labelVals),
		h:                a.create(),
	}
	a.add(child)
	return child
}

// EachChild iterates over all child histograms, calling the provided function for each one.
// This allows direct access to child histogram objects and their individual snapshots.
// The labelNames parameter will be set to the label names configured for this histogram.
func (a *AggHistogram) EachChild(f func(labelNames, labelVals []string, child *Histogram)) {
	labelNames := a.labels
	a.apply(func(item MetricItem) {
		if h, ok := item.(*Histogram); ok {
			f(labelNames, h.labelValues(), h)
		}
	})
}

// Histogram is a child of a AggHistogram. When values are recorded, so too is the
// parent. When metrics are collected by prometheus, each of the children will
// appear with a distinct label, however, when cockroach internally collects
// metrics, only the parent is collected.
type Histogram struct {
	parent *AggHistogram
	labelValuesSlice
	h metric.IHistogram
}

// ToPrometheusMetric constructs a prometheus metric for this Histogram.
func (g *Histogram) ToPrometheusMetric() *prometheusgo.Metric {
	return g.h.ToPrometheusMetric()
}

// Unlink unlinks this child from the parent, i.e. the parent will no longer
// track this child (i.e. won't generate labels for it, etc). However, the child
// will continue to be functional and reference the parent, meaning updates to
// it will be reflected in the aggregate stored in the parent.
//
// See tenantrate.TestUseAfterRelease.
func (g *Histogram) Unlink() {
	g.parent.remove(g)
}

// RecordValue adds the given value to the histogram. Recording a value in
// excess of the configured maximum value for that histogram results in
// recording the maximum value instead.
func (g *Histogram) RecordValue(v int64) {
	g.parent.ticker.RLock()
	defer g.parent.ticker.RUnlock()
	g.h.RecordValue(v)
	g.parent.h.RecordValue(v)
}

// CumulativeSnapshot returns the cumulative snapshot for this child histogram.
// Returns an empty snapshot if the underlying histogram doesn't implement CumulativeHistogram.
func (g *Histogram) CumulativeSnapshot() metric.HistogramSnapshot {
	if ch, ok := g.h.(metric.CumulativeHistogram); ok {
		return ch.CumulativeSnapshot()
	}
	return metric.HistogramSnapshot{}
}

// WindowedSnapshot returns the windowed snapshot for this child histogram.
// Returns an empty snapshot if the underlying histogram doesn't implement WindowedHistogram.
func (g *Histogram) WindowedSnapshot() metric.HistogramSnapshot {
	if wh, ok := g.h.(metric.WindowedHistogram); ok {
		return wh.WindowedSnapshot()
	}
	return metric.HistogramSnapshot{}
}

// SQLHistogram maintains a histogram as the sum of its children. The histogram will
// report to crdb-internal time series only the aggregate sum of all of its
// children, while its children are additionally exported to prometheus via the
// PrometheusIterable interface. SQLHistogram differs from AggHistogram in that
// a SQLHistogram creates child metrics dynamically while AggHistogram needs the
// child creation up front.
type SQLHistogram struct {
	h      metric.IHistogram
	create func() metric.IHistogram
	*SQLMetric
	ticker struct {
		// We use a RWMutex, because we don't want child histograms to contend when
		// recording values, unless we're rotating histograms for the parent & children.
		// In this instance, the "writer" for the RWMutex is the ticker, and the "readers"
		// are all the child histograms recording their values.
		syncutil.RWMutex
		*tick.Ticker
	}
}

var _ metric.Iterable = (*SQLHistogram)(nil)
var _ metric.PrometheusReinitialisable = (*SQLHistogram)(nil)
var _ metric.PrometheusExportable = (*SQLHistogram)(nil)
var _ metric.WindowedHistogram = (*SQLHistogram)(nil)
var _ metric.CumulativeHistogram = (*SQLHistogram)(nil)

func NewSQLHistogram(opts metric.HistogramOptions) *SQLHistogram {
	create := func() metric.IHistogram {
		return metric.NewHistogram(opts)
	}
	histogram := create()
	s := &SQLHistogram{
		h:      histogram,
		create: create,
	}
	s.SQLMetric = NewSQLMetric(metric.LabelConfigDisabled, histogram)
	s.ticker.Ticker = tick.NewTicker(
		now(),
		opts.Duration/metric.WindowedHistogramWrapNum,
		func() {
			// Atomically rotate the histogram window for the
			// parent histogram, and all the child histograms.
			s.h.Tick()
			s.apply(func(childMetric ChildMetric) {
				childHist, ok := childMetric.(*SQLChildHistogram)
				if !ok {
					panic(errors.AssertionFailedf(
						"unable to assert type of child for histogram %q when rotating histogram windows",
						opts.Metadata.Name))
				}
				childHist.h.Tick()
			})
		})
	return s
}

// Inspect is part of the metric.Iterable interface.
func (sh *SQLHistogram) Inspect(f func(interface{})) {
	func() {
		sh.ticker.Lock()
		defer sh.ticker.Unlock()
		tick.MaybeTick(&sh.ticker)
	}()
	f(sh)
}

// apply applies the given applyFn to every item in children
func (sh *SQLHistogram) apply(applyFn func(childMetric ChildMetric)) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.mu.children.ForEach(func(metric ChildMetric) {
		applyFn(metric.(*SQLChildHistogram))
	})
}

// RecordValue records the Histogram value for the given label values. If a
// Histogram with the given label values doesn't exist yet, it creates a new
// Histogram and record against it. RecordValue records value in parent metrics
// irrespective of labelConfig.
func (sh *SQLHistogram) RecordValue(v int64, db, app string) {
	childMetric, isChildMetricEnabled := sh.getChildByLabelConfig(sh.createChildHistogram, db, app)
	sh.ticker.RLock()
	defer sh.ticker.RUnlock()

	sh.h.RecordValue(v)
	if !isChildMetricEnabled {
		return
	}
	childMetric.(*SQLChildHistogram).RecordValue(v)
}

// CumulativeSnapshot is part of the metric.CumulativeHistogram interface.
func (sh *SQLHistogram) CumulativeSnapshot() metric.HistogramSnapshot {
	return sh.h.CumulativeSnapshot()
}

// WindowedSnapshot is part of the metric.WindowedHistogram interface.
func (sh *SQLHistogram) WindowedSnapshot() metric.HistogramSnapshot {
	return sh.h.WindowedSnapshot()
}

func (sh *SQLHistogram) createChildHistogram(labelValues labelValuesSlice) ChildMetric {
	return &SQLChildHistogram{
		h:                sh.create(),
		labelValuesSlice: labelValues,
	}
}

// SQLChildHistogram is a child of a SQLHistogram. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type SQLChildHistogram struct {
	labelValuesSlice
	h metric.IHistogram
}

// ToPrometheusMetric constructs a prometheus metric for this Histogram.
func (sch *SQLChildHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	return sch.h.ToPrometheusMetric()
}

// RecordValue sets the histogram's value.
func (sch *SQLChildHistogram) RecordValue(v int64) {
	sch.h.RecordValue(v)
}

// Value returns the SQLChildHistogram's current gauge.
func (sch *SQLChildHistogram) Value() metric.HistogramSnapshot {
	return sch.h.CumulativeSnapshot()
}

// HighCardinalityHistogram is similar to AggHistogram but uses cache storage instead of B-tree,
// allowing for automatic eviction of less frequently used child metrics.
// This is useful when dealing with high cardinality metrics that might exceed resource limits.
type HighCardinalityHistogram struct {
	h      metric.IHistogram
	create func() metric.IHistogram
	*baseAggMetric
	labelSliceCache *metric.LabelSliceCache
	ticker          struct {
		// We use a RWMutex, because we don't want child histograms to contend when
		// recording values, unless we're rotating histograms for the parent & children.
		// In this instance, the "writer" for the RWMutex is the ticker, and the "readers"
		// are all the child histograms recording their values.
		syncutil.RWMutex
		*tick.Ticker
	}
}

var _ metric.Iterable = (*HighCardinalityHistogram)(nil)
var _ metric.PrometheusEvictable = (*HighCardinalityHistogram)(nil)
var _ metric.WindowedHistogram = (*HighCardinalityHistogram)(nil)
var _ metric.CumulativeHistogram = (*HighCardinalityHistogram)(nil)

// NewHighCardinalityHistogram constructs a new HighCardinalityHistogram that uses cache storage
// with eviction for child metrics. The HighCardinalityOpts field in opts allows configuring
// the maximum number of label combinations (MaxLabelValues) and retention time (RetentionTimeTillEviction).
// If HighCardinalityOpts is not provided or has zero values, defaults will be used.
func NewHighCardinalityHistogram(
	opts metric.HistogramOptions, childLabels ...string,
) *HighCardinalityHistogram {
	create := func() metric.IHistogram {
		return metric.NewHistogram(opts)
	}
	hch := create()
	base := newBaseAggMetric(hch)
	h := &HighCardinalityHistogram{
		h:             hch,
		create:        create,
		baseAggMetric: base,
	}
	h.ticker.Ticker = tick.NewTicker(
		now(),
		opts.Duration/metric.WindowedHistogramWrapNum,
		func() {
			// Atomically rotate the histogram window for the
			// parent histogram, and all the child histograms.
			h.h.Tick()
			h.childSet.apply(func(childItem MetricItem) {
				childHist, ok := childItem.(*HighCardinalityChildHistogram)
				if !ok {
					panic(errors.AssertionFailedf(
						"unable to assert type of child for histogram %q when rotating histogram windows",
						opts.Metadata.Name))
				}
				childHist.h.Tick()
			})
		})
	h.initWithCacheStorageType(childLabels, opts.Metadata.Name, opts.HighCardinalityOpts)
	return h
}

// Inspect is part of the metric.Iterable interface.
func (h *HighCardinalityHistogram) Inspect(f func(interface{})) {
	func() {
		h.ticker.Lock()
		defer h.ticker.Unlock()
		tick.MaybeTick(&h.ticker)
	}()
	f(h)
}

// CumulativeSnapshot is part of the metric.CumulativeHistogram interface.
func (h *HighCardinalityHistogram) CumulativeSnapshot() metric.HistogramSnapshot {
	return h.h.CumulativeSnapshot()
}

// WindowedSnapshot is part of the metric.WindowedHistogram interface.
func (h *HighCardinalityHistogram) WindowedSnapshot() metric.HistogramSnapshot {
	return h.h.WindowedSnapshot()
}

// RecordValue records the histogram value for the given label values. If a
// histogram with the given label values doesn't exist yet, it creates a new
// histogram and records against it. RecordValue records value in parent metrics as well.
func (h *HighCardinalityHistogram) RecordValue(v int64, labelValues ...string) {
	childMetric := h.GetOrAddChild(labelValues...)

	h.ticker.RLock()
	defer h.ticker.RUnlock()

	h.h.RecordValue(v)
	if childMetric != nil {
		childMetric.RecordValue(v)
	}
}

// Each is part of the metric.PrometheusIterable interface.
func (h *HighCardinalityHistogram) Each(
	labels []*prometheusgo.LabelPair, f func(metric *prometheusgo.Metric),
) {
	h.EachWithLabels(labels, f, h.labelSliceCache)
}

// InitializeMetrics is part of the PrometheusEvictable interface.
func (h *HighCardinalityHistogram) InitializeMetrics(labelCache *metric.LabelSliceCache) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.labelSliceCache = labelCache
}

// GetOrAddChild returns the existing child histogram for the given label values,
// or creates a new one if it doesn't exist. This is the preferred method for
// cache-based storage to avoid panics on existing keys.
func (h *HighCardinalityHistogram) GetOrAddChild(
	labelVals ...string,
) *HighCardinalityChildHistogram {
	if len(labelVals) == 0 {
		return nil
	}

	// Create a LabelSliceCacheKey from the labelVals.
	key := metric.LabelSliceCacheKey(metricKey(labelVals...))

	child := h.getOrAddWithLabelSliceCache(h.GetMetadata().Name, h.createHighCardinalityChildHistogram, h.labelSliceCache, labelVals...)

	h.labelSliceCache.Upsert(key, &metric.LabelSliceCacheValue{
		LabelValues: labelVals,
	})

	return child.(*HighCardinalityChildHistogram)
}

func (h *HighCardinalityHistogram) createHighCardinalityChildHistogram(
	key uint64, cache *metric.LabelSliceCache,
) LabelSliceCachedChildMetric {
	return &HighCardinalityChildHistogram{
		LabelSliceCacheKey: metric.LabelSliceCacheKey(key),
		LabelSliceCache:    cache,
		h:                  h.create(),
		createdAt:          timeutil.Now(),
	}
}

// HighCardinalityChildHistogram is a child of a HighCardinalityHistogram. When metrics are
// collected by prometheus, each of the children will appear with a distinct label,
// however, when cockroach internally collects metrics, only the parent is collected.
type HighCardinalityChildHistogram struct {
	metric.LabelSliceCacheKey
	h metric.IHistogram
	*metric.LabelSliceCache
	createdAt time.Time
}

func (h *HighCardinalityChildHistogram) CreatedAt() time.Time {
	return h.createdAt
}

func (h *HighCardinalityChildHistogram) DecrementLabelSliceCacheReference() {
	h.LabelSliceCache.DecrementAndDeleteIfZero(h.LabelSliceCacheKey)
}

// ToPrometheusMetric constructs a prometheus metric for this HighCardinalityChildHistogram.
func (h *HighCardinalityChildHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	return h.h.ToPrometheusMetric()
}

func (h *HighCardinalityChildHistogram) labelValues() []string {
	lv, ok := h.LabelSliceCache.Get(h.LabelSliceCacheKey)
	if !ok {
		return nil
	}
	return lv.LabelValues
}

// RecordValue records the histogram value.
func (h *HighCardinalityChildHistogram) RecordValue(v int64) {
	h.h.RecordValue(v)
}

// CumulativeSnapshot returns the cumulative histogram snapshot.
func (h *HighCardinalityChildHistogram) CumulativeSnapshot() metric.HistogramSnapshot {
	return h.h.CumulativeSnapshot()
}

// WindowedSnapshot returns the windowed histogram snapshot.
func (h *HighCardinalityChildHistogram) WindowedSnapshot() metric.HistogramSnapshot {
	return h.h.WindowedSnapshot()
}
