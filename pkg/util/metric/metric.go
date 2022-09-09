// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"encoding/json"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	metrics "github.com/rcrowley/go-metrics"
)

const (
	// TestSampleInterval is passed to histograms during tests which don't
	// want to concern themselves with supplying a "correct" interval.
	TestSampleInterval = time.Duration(math.MaxInt64)
)

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// GetName returns the fully-qualified name of the metric.
	GetName() string
	// GetHelp returns the help text for the metric.
	GetHelp() string
	// GetMeasurement returns the label for the metric, which describes the entity
	// it measures.
	GetMeasurement() string
	// GetUnit returns the unit that should be used to display the metric
	// (e.g. in bytes).
	GetUnit() Unit
	// GetMetadata returns the metric's metadata, which can be used in charts.
	GetMetadata() Metadata
	// Inspect calls the given closure with each contained item.
	Inspect(func(interface{}))
}

// PrometheusExportable is the standard interface for an individual metric
// that can be exported to prometheus.
type PrometheusExportable interface {
	// GetName is a method on Metadata
	GetName() string
	// GetHelp is a method on Metadata
	GetHelp() string
	// GetType returns the prometheus type enum for this metric.
	GetType() *prometheusgo.MetricType
	// GetLabels is a method on Metadata
	GetLabels() []*prometheusgo.LabelPair
	// ToPrometheusMetric returns a filled-in prometheus metric of the right type
	// for the given metric. It does not fill in labels.
	// The implementation must return thread-safe data to the caller, i.e.
	// usually a copy of internal state.
	ToPrometheusMetric() *prometheusgo.Metric
}

// PrometheusIterable is an extension of PrometheusExportable to indicate that
// this metric is comprised of children metrics which augment the parent's
// label values.
//
// The motivating use-case for this interface is the existence of tenants. We'd
// like to capture per-tenant metrics and expose them to prometheus while not
// polluting the internal tsdb.
type PrometheusIterable interface {
	PrometheusExportable

	// Each takes a slice of label pairs associated with the parent metric and
	// calls the passed function with each of the children metrics.
	Each([]*prometheusgo.LabelPair, func(metric *prometheusgo.Metric))
}

// GetName returns the metric's name.
func (m *Metadata) GetName() string {
	return m.Name
}

// GetHelp returns the metric's help string.
func (m *Metadata) GetHelp() string {
	return m.Help
}

// GetMeasurement returns the entity measured by the metric.
func (m *Metadata) GetMeasurement() string {
	return m.Measurement
}

// GetUnit returns the metric's unit of measurement.
func (m *Metadata) GetUnit() Unit {
	return m.Unit
}

// GetLabels returns the metric's labels. For rationale behind the conversion
// from metric.LabelPair to prometheusgo.LabelPair, see the LabelPair comment
// in pkg/util/metric/metric.proto.
func (m *Metadata) GetLabels() []*prometheusgo.LabelPair {
	lps := make([]*prometheusgo.LabelPair, len(m.Labels))
	// x satisfies the field XXX_unrecognized in prometheusgo.LabelPair.
	var x []byte
	for i, v := range m.Labels {
		lps[i] = &prometheusgo.LabelPair{Name: v.Name, Value: v.Value, XXX_unrecognized: x}
	}
	return lps
}

// AddLabel adds a label/value pair for this metric.
func (m *Metadata) AddLabel(name, value string) {
	m.Labels = append(m.Labels,
		&LabelPair{
			Name:  proto.String(exportedLabel(name)),
			Value: proto.String(value),
		})
}

var _ Iterable = &Gauge{}
var _ Iterable = &GaugeFloat64{}
var _ Iterable = &Counter{}

var _ json.Marshaler = &Gauge{}
var _ json.Marshaler = &GaugeFloat64{}
var _ json.Marshaler = &Counter{}
var _ json.Marshaler = &Registry{}

var _ PrometheusExportable = &Gauge{}
var _ PrometheusExportable = &GaugeFloat64{}
var _ PrometheusExportable = &Counter{}

type periodic interface {
	nextTick() time.Time
	tick()
}

var now = timeutil.Now

// TestingSetNow changes the clock used by the metric system. For use by
// testing to precisely control the clock.
func TestingSetNow(f func() time.Time) func() {
	origNow := now
	now = f
	return func() {
		now = origNow
	}
}

func maybeTick(m periodic) {
	for m.nextTick().Before(now()) {
		m.tick()
	}
}

// TODO(irfansharif): Figure out how to export runtime scheduler latencies as a
// prometheus histogram? Can we use a functional histogram? And maintain deltas
// underneath? What does prometheus/client_golang do?

// NewHistogram is a prometheus-backed histogram. Depending on the value of
// opts.Buckets, this is suitable for recording any kind of quantity. Common
// sensible choices are {IO,Network}LatencyBuckets.
func NewHistogram(meta Metadata, windowDuration time.Duration, buckets []float64) *Histogram {
	// TODO(obs-inf): prometheus supports labeled histograms but they require more
	// plumbing and don't fit into the PrometheusObservable interface any more.
	opts := prometheus.HistogramOpts{
		Buckets: buckets,
	}
	cum := prometheus.NewHistogram(opts)
	h := &Histogram{
		Metadata: meta,
		cum:      cum,
	}
	h.windowed.tickHelper = &tickHelper{
		nextT:        now(),
		tickInterval: windowDuration,
		onTick: func() {
			h.windowed.prev = h.windowed.cur
			h.windowed.cur = prometheus.NewHistogram(opts)
		},
	}
	h.windowed.tickHelper.onTick()
	return h
}

var _ periodic = (*Histogram)(nil)
var _ PrometheusExportable = (*Histogram)(nil)

// Histogram is a prometheus-backed histogram. It collects observed values by
// keeping bucketed counts. For convenience, internally two sets of buckets are
// kept: A cumulative set (i.e. data is never evicted) and a windowed set (which
// keeps only recently collected samples).
//
// New buckets are created using TestHistogramBuckets.
type Histogram struct {
	Metadata
	cum prometheus.Histogram

	// TODO(obs-inf): the way we implement windowed histograms is not great. If
	// the windowed histogram is pulled right after a tick, it will be mostly
	// empty. We could add a third bucket and represent the merged view of the two
	// most recent buckets to avoid that. Or we could "just" double the rotation
	// interval (so that the histogram really collects for 20s when we expect to
	// persist the contents every 10s). Really it would make more sense to
	// explicitly rotate the histogram atomically with collecting its contents,
	// but that is now how we have set it up right now. It should be doable
	// though, since there is only one consumer of windowed histograms - our
	// internal timeseries system.
	windowed struct {
		// prometheus.Histogram is thread safe, so we only
		// need an RLock to record into it. But write lock
		// is held while rotating.
		syncutil.RWMutex
		*tickHelper
		prev, cur prometheus.Histogram
	}
}

func (h *Histogram) nextTick() time.Time {
	h.windowed.RLock()
	defer h.windowed.RUnlock()
	return h.windowed.nextTick()
}

func (h *Histogram) tick() {
	h.windowed.Lock()
	defer h.windowed.Unlock()
	h.windowed.tick()
}

// Windowed returns a copy of the current windowed histogram.
func (h *Histogram) Windowed() prometheus.Histogram {
	h.windowed.RLock()
	defer h.windowed.RUnlock()
	return h.windowed.cur
}

// RecordValue adds the given value to the histogram.
func (h *Histogram) RecordValue(n int64) {
	v := float64(n)
	h.cum.Observe(v)

	h.windowed.RLock()
	defer h.windowed.RUnlock()
	h.windowed.cur.Observe(v)
}

// GetType returns the prometheus type enum for this metric.
func (h *Histogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (h *Histogram) ToPrometheusMetric() *prometheusgo.Metric {
	m := &prometheusgo.Metric{}
	if err := h.cum.Write(m); err != nil {
		panic(err)
	}
	return m
}

// ToPrometheusMetricWindowed returns a filled-in prometheus metric of the right type.
func (h *Histogram) ToPrometheusMetricWindowed() *prometheusgo.Metric {
	h.windowed.Lock()
	defer h.windowed.Unlock()
	m := &prometheusgo.Metric{}
	if err := h.windowed.cur.Write(m); err != nil {
		panic(err)
	}
	return m
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *Histogram) GetMetadata() Metadata {
	return h.Metadata
}

// Inspect calls the closure.
func (h *Histogram) Inspect(f func(interface{})) {
	h.windowed.Lock()
	maybeTick(&h.windowed)
	h.windowed.Unlock()
	f(h)
}

// TotalCount returns the (cumulative) number of samples.
func (h *Histogram) TotalCount() int64 {
	return int64(h.ToPrometheusMetric().Histogram.GetSampleCount())
}

// TotalCountWindowed returns the number of samples in the current window.
func (h *Histogram) TotalCountWindowed() int64 {
	return int64(h.ToPrometheusMetricWindowed().Histogram.GetSampleCount())
}

// TotalSum returns the (cumulative) number of samples.
func (h *Histogram) TotalSum() float64 {
	return h.ToPrometheusMetric().Histogram.GetSampleSum()
}

// TotalSumWindowed returns the number of samples in the current window.
func (h *Histogram) TotalSumWindowed() float64 {
	return h.ToPrometheusMetricWindowed().Histogram.GetSampleSum()
}

// Mean returns the (cumulative) mean of samples.
func (h *Histogram) Mean() float64 {
	return h.TotalSum() / float64(h.TotalCount())
}

// ValueAtQuantileWindowed takes a quantile value [0,100] and returns the
// interpolated value at that quantile for the windowed histogram.
//
// https://github.com/prometheus/prometheus/blob/d91621890a2ccb3191a6d74812cc1827dd4093bf/promql/quantile.go#L75
// This function is mostly taken from a prometheus internal function that
// does the same thing. There are a few differences for our use case:
//  1. As a user of the prometheus go client library, we don't have access
//     to the implicit +Inf bucket, so we don't need special cases to deal
//     with the quantiles that include the +Inf bucket.
//  2. Since the prometheus client library ensures buckets are in a strictly
//     increasing order at creation, we do not sort them.
func (h *Histogram) ValueAtQuantileWindowed(q float64) float64 {
	m := h.ToPrometheusMetricWindowed()

	buckets := m.Histogram.Bucket
	n := float64(*m.Histogram.SampleCount)
	if n == 0 {
		return 0
	}

	rank := uint64(((q / 100) * n) + 0.5)
	b := sort.Search(len(buckets)-1, func(i int) bool { return *buckets[i].CumulativeCount >= rank })

	var (
		bucketStart float64
		bucketEnd   = *buckets[b].UpperBound
		count       = *buckets[b].CumulativeCount
	)

	// Calculate the linearly interpolated value within the bucket
	if b > 0 {
		bucketStart = *buckets[b-1].UpperBound
		count -= *buckets[b-1].CumulativeCount
		rank -= *buckets[b-1].CumulativeCount
	}
	val := bucketStart + (bucketEnd-bucketStart)*(float64(rank)/float64(count))
	if math.IsNaN(val) || math.IsInf(val, -1) {
		return 0
	}

	// should not extrapolate past the upper bound of the largest bucket
	if val > *buckets[len(buckets)-1].UpperBound {
		return *buckets[len(buckets)-1].UpperBound
	}

	return val
}

// A Counter holds a single mutable atomic value.
type Counter struct {
	Metadata
	metrics.Counter
}

// NewCounter creates a counter.
func NewCounter(metadata Metadata) *Counter {
	return &Counter{metadata, metrics.NewCounter()}
}

// Dec overrides the metric.Counter method. This method should NOT be
// used and serves only to prevent misuse of the metric type.
func (c *Counter) Dec(int64) {
	// From https://prometheus.io/docs/concepts/metric_types/#counter
	// > Counters should not be used to expose current counts of items
	// > whose number can also go down, e.g. the number of currently
	// > running goroutines. Use gauges for this use case.
	panic("Counter should not be decremented, use a Gauge instead")
}

// GetType returns the prometheus type enum for this metric.
func (c *Counter) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// Inspect calls the given closure with the empty string and itself.
func (c *Counter) Inspect(f func(interface{})) { f(c) }

// MarshalJSON marshals to JSON.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Counter.Count())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (c *Counter) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Counter: &prometheusgo.Counter{Value: proto.Float64(float64(c.Counter.Count()))},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (c *Counter) GetMetadata() Metadata {
	baseMetadata := c.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_COUNTER
	return baseMetadata
}

// A Gauge atomically stores a single integer value.
type Gauge struct {
	Metadata
	value *int64
	fn    func() int64
}

// NewGauge creates a Gauge.
func NewGauge(metadata Metadata) *Gauge {
	return &Gauge{metadata, new(int64), nil}
}

// NewFunctionalGauge creates a Gauge metric whose value is determined when
// asked for by calling the provided function.
// Note that Update, Inc, and Dec should NOT be called on a Gauge returned
// from NewFunctionalGauge.
func NewFunctionalGauge(metadata Metadata, f func() int64) *Gauge {
	return &Gauge{metadata, nil, f}
}

// Snapshot returns a read-only copy of the gauge.
func (g *Gauge) Snapshot() metrics.Gauge {
	return metrics.GaugeSnapshot(g.Value())
}

// Update updates the gauge's value.
func (g *Gauge) Update(v int64) {
	atomic.StoreInt64(g.value, v)
}

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 {
	if g.fn != nil {
		return g.fn()
	}
	return atomic.LoadInt64(g.value)
}

// Inc increments the gauge's value.
func (g *Gauge) Inc(i int64) {
	atomic.AddInt64(g.value, i)
}

// Dec decrements the gauge's value.
func (g *Gauge) Dec(i int64) {
	atomic.AddInt64(g.value, -i)
}

// GetType returns the prometheus type enum for this metric.
func (g *Gauge) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with the empty string and itself.
func (g *Gauge) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Value())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (g *Gauge) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(float64(g.Value()))},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (g *Gauge) GetMetadata() Metadata {
	baseMetadata := g.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_GAUGE
	return baseMetadata
}

// A GaugeFloat64 atomically stores a single float64 value.
type GaugeFloat64 struct {
	Metadata
	bits *uint64
	fn   func() float64
}

// NewGaugeFloat64 creates a GaugeFloat64.
func NewGaugeFloat64(metadata Metadata) *GaugeFloat64 {
	return &GaugeFloat64{metadata, new(uint64), nil}
}

// NewFunctionalGaugeFloat64 creates a GaugeFloat64 metric whose value is
// determined when asked for by calling the provided function.
// Note that Update, Inc, and Dec should NOT be called on a Gauge returned
// from NewFunctionalGaugeFloat64.
func NewFunctionalGaugeFloat64(metadata Metadata, f func() float64) *GaugeFloat64 {
	return &GaugeFloat64{metadata, nil, f}
}

// Snapshot returns a read-only copy of the gauge.
func (g *GaugeFloat64) Snapshot() metrics.GaugeFloat64 {
	return metrics.GaugeFloat64Snapshot(g.Value())
}

// Update updates the gauge's value.
func (g *GaugeFloat64) Update(v float64) {
	atomic.StoreUint64(g.bits, math.Float64bits(v))
}

// Value returns the gauge's current value.
func (g *GaugeFloat64) Value() float64 {
	if g.fn != nil {
		return g.fn()
	}
	return math.Float64frombits(atomic.LoadUint64(g.bits))
}

// Inc increments the gauge's value.
func (g *GaugeFloat64) Inc(delta float64) {
	for {
		oldBits := atomic.LoadUint64(g.bits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + delta)
		if atomic.CompareAndSwapUint64(g.bits, oldBits, newBits) {
			return
		}
	}
}

// Dec decrements the gauge's value.
func (g *GaugeFloat64) Dec(delta float64) {
	g.Inc(-delta)
}

// GetType returns the prometheus type enum for this metric.
func (g *GaugeFloat64) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with itself.
func (g *GaugeFloat64) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *GaugeFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Value())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (g *GaugeFloat64) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(g.Value())},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (g *GaugeFloat64) GetMetadata() Metadata {
	baseMetadata := g.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_GAUGE
	return baseMetadata
}
