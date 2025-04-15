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
	childSet
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
	a := &AggHistogram{
		h:      create(),
		create: create,
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

// GetName is part of the metric.Iterable interface.
func (a *AggHistogram) GetName(useStaticLabels bool) string { return a.h.GetName(useStaticLabels) }

// GetHelp is part of the metric.Iterable interface.
func (a *AggHistogram) GetHelp() string { return a.h.GetHelp() }

// GetMeasurement is part of the metric.Iterable interface.
func (a *AggHistogram) GetMeasurement() string { return a.h.GetMeasurement() }

// GetUnit is part of the metric.Iterable interface.
func (a *AggHistogram) GetUnit() metric.Unit { return a.h.GetUnit() }

// GetMetadata is part of the metric.Iterable interface.
func (a *AggHistogram) GetMetadata() metric.Metadata { return a.h.GetMetadata() }

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

// GetType is part of the metric.PrometheusExportable interface.
func (a *AggHistogram) GetType() *prometheusgo.MetricType {
	return a.h.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (a *AggHistogram) GetLabels(useStaticLabels bool) []*prometheusgo.LabelPair {
	return a.h.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (a *AggHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	return a.h.ToPrometheusMetric()
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
var _ metric.PrometheusIterable = (*SQLHistogram)(nil)
var _ metric.PrometheusExportable = (*SQLHistogram)(nil)
var _ metric.WindowedHistogram = (*SQLHistogram)(nil)
var _ metric.CumulativeHistogram = (*SQLHistogram)(nil)

func NewSQLHistogram(opts metric.HistogramOptions) *SQLHistogram {
	create := func() metric.IHistogram {
		return metric.NewHistogram(opts)
	}
	s := &SQLHistogram{
		h:      create(),
		create: create,
	}
	s.SQLMetric = NewSQLMetric(LabelConfigDisabled)
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

// apply applies the given applyFn to every item in children
func (sh *SQLHistogram) apply(applyFn func(childMetric ChildMetric)) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.mu.children.Do(func(e interface{}) {
		applyFn(sh.mu.children.GetChildMetric(e).(*SQLChildHistogram))
	})
}

// GetType is part of the metric.PrometheusExportable interface.
func (sh *SQLHistogram) GetType() *prometheusgo.MetricType {
	return sh.h.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (sh *SQLHistogram) GetLabels(useStaticLabels bool) []*prometheusgo.LabelPair {
	return sh.h.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (sh *SQLHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	return sh.h.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (sh *SQLHistogram) GetName(useStaticLabels bool) string {
	return sh.h.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (sh *SQLHistogram) GetHelp() string {
	return sh.h.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (sh *SQLHistogram) GetMeasurement() string {
	return sh.h.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (sh *SQLHistogram) GetUnit() metric.Unit {
	return sh.h.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (sh *SQLHistogram) GetMetadata() metric.Metadata {
	return sh.h.GetMetadata()
}

// Inspect is part of the metric.Iterable interface.
func (sh *SQLHistogram) Inspect(f func(interface{})) {
	f(sh)
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
