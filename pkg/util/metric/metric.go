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
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/gogo/protobuf/proto"
	prometheusgo "github.com/prometheus/client_model/go"
	metrics "github.com/rcrowley/go-metrics"
)

const (
	// MaxLatency is the maximum value tracked in latency histograms. Higher
	// values will be recorded as this value instead.
	MaxLatency = 10 * time.Second

	// TestSampleInterval is passed to histograms during tests which don't
	// want to concern themselves with supplying a "correct" interval.
	TestSampleInterval = time.Duration(math.MaxInt64)

	// The number of histograms to keep in rolling window.
	histWrapNum = 2
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
var _ Iterable = &Histogram{}
var _ Iterable = &Rate{}

var _ json.Marshaler = &Gauge{}
var _ json.Marshaler = &GaugeFloat64{}
var _ json.Marshaler = &Counter{}
var _ json.Marshaler = &Registry{}
var _ json.Marshaler = &Rate{}

var _ PrometheusExportable = &Gauge{}
var _ PrometheusExportable = &GaugeFloat64{}
var _ PrometheusExportable = &Counter{}
var _ PrometheusExportable = &Histogram{}
var _ PrometheusExportable = &Rate{}

type periodic interface {
	nextTick() time.Time
	tick()
}

var _ periodic = &Rate{}

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

func cloneHistogram(in *hdrhistogram.Histogram) *hdrhistogram.Histogram {
	return hdrhistogram.Import(in.Export())
}

func maybeTick(m periodic) {
	for m.nextTick().Before(now()) {
		m.tick()
	}
}

// A Histogram collects observed values by keeping bucketed counts. For
// convenience, internally two sets of buckets are kept: A cumulative set (i.e.
// data is never evicted) and a windowed set (which keeps only recently
// collected samples).
//
// Top-level methods generally apply to the cumulative buckets; the windowed
// variant is exposed through the Windowed method.
type Histogram struct {
	Metadata
	maxVal int64
	mu     struct {
		syncutil.Mutex
		cumulative *hdrhistogram.Histogram
		sliding    *slidingHistogram
	}
}

// NewHistogram initializes a given Histogram. The contained windowed histogram
// rotates every 'duration'; both the windowed and the cumulative histogram
// track nonnegative values up to 'maxVal' with 'sigFigs' decimal points of
// precision.
func NewHistogram(metadata Metadata, duration time.Duration, maxVal int64, sigFigs int) *Histogram {
	dHist := newSlidingHistogram(duration, maxVal, sigFigs)
	h := &Histogram{
		Metadata: metadata,
		maxVal:   maxVal,
	}
	h.mu.cumulative = hdrhistogram.New(0, maxVal, sigFigs)
	h.mu.sliding = dHist
	return h
}

// NewLatency is a convenience function which returns a histogram with
// suitable defaults for latency tracking. Values are expressed in ns,
// are truncated into the interval [0, MaxLatency] and are recorded
// with one digit of precision (i.e. errors of <10ms at 100ms, <6s at 60s).
//
// The windowed portion of the Histogram retains values for approximately
// histogramWindow.
func NewLatency(metadata Metadata, histogramWindow time.Duration) *Histogram {
	return NewHistogram(
		metadata, histogramWindow, MaxLatency.Nanoseconds(), 1,
	)
}

// Windowed returns a copy of the current windowed histogram data and its
// rotation interval.
func (h *Histogram) Windowed() (*hdrhistogram.Histogram, time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneHistogram(h.mu.sliding.Current()), h.mu.sliding.duration
}

// Snapshot returns a copy of the cumulative (i.e. all-time samples) histogram
// data.
func (h *Histogram) Snapshot() *hdrhistogram.Histogram {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneHistogram(h.mu.cumulative)
}

// RecordValue adds the given value to the histogram. Recording a value in
// excess of the configured maximum value for that histogram results in
// recording the maximum value instead.
func (h *Histogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.mu.sliding.RecordValue(v) != nil {
		_ = h.mu.sliding.RecordValue(h.maxVal)
	}
	if h.mu.cumulative.RecordValue(v) != nil {
		_ = h.mu.cumulative.RecordValue(h.maxVal)
	}
}

// TotalCount returns the (cumulative) number of samples.
func (h *Histogram) TotalCount() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.cumulative.TotalCount()
}

// Min returns the minimum.
func (h *Histogram) Min() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.cumulative.Min()
}

// Inspect calls the closure with the empty string and the receiver.
func (h *Histogram) Inspect(f func(interface{})) {
	h.mu.Lock()
	maybeTick(h.mu.sliding)
	h.mu.Unlock()
	f(h)
}

// GetType returns the prometheus type enum for this metric.
func (h *Histogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (h *Histogram) ToPrometheusMetric() *prometheusgo.Metric {
	hist := &prometheusgo.Histogram{}

	h.mu.Lock()
	maybeTick(h.mu.sliding)
	bars := h.mu.cumulative.Distribution()
	hist.Bucket = make([]*prometheusgo.Bucket, 0, len(bars))

	var cumCount uint64
	var sum float64
	for _, bar := range bars {
		if bar.Count == 0 {
			// No need to expose trivial buckets.
			continue
		}
		upperBound := float64(bar.To)
		sum += upperBound * float64(bar.Count)

		cumCount += uint64(bar.Count)
		curCumCount := cumCount // need a new alloc thanks to bad proto code

		hist.Bucket = append(hist.Bucket, &prometheusgo.Bucket{
			CumulativeCount: &curCumCount,
			UpperBound:      &upperBound,
		})
	}
	hist.SampleCount = &cumCount
	hist.SampleSum = &sum // can do better here; we approximate in the loop
	h.mu.Unlock()

	return &prometheusgo.Metric{
		Histogram: hist,
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *Histogram) GetMetadata() Metadata {
	baseMetadata := h.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_HISTOGRAM
	return baseMetadata
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
	metrics.GaugeFloat64
}

// NewGaugeFloat64 creates a GaugeFloat64.
func NewGaugeFloat64(metadata Metadata) *GaugeFloat64 {
	return &GaugeFloat64{metadata, metrics.NewGaugeFloat64()}
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

// A Rate is a exponential weighted moving average.
type Rate struct {
	Metadata
	mu       syncutil.Mutex // protects fields below
	curSum   float64
	wrapped  ewma.MovingAverage
	interval time.Duration
	nextT    time.Time
}

// NewRate creates an EWMA rate on the given timescale. Timescales at
// or below 2s are illegal and will cause a panic.
func NewRate(metadata Metadata, timescale time.Duration) *Rate {
	const tickInterval = time.Second
	if timescale <= 2*time.Second {
		panic(fmt.Sprintf("EWMA with per-second ticks makes no sense on timescale %s", timescale))
	}
	avgAge := float64(timescale) / float64(2*tickInterval)
	return &Rate{
		Metadata: metadata,
		interval: tickInterval,
		nextT:    now(),
		wrapped:  ewma.NewMovingAverage(avgAge),
	}
}

// GetType returns the prometheus type enum for this metric.
func (e *Rate) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with itself.
func (e *Rate) Inspect(f func(interface{})) { f(e) }

// MarshalJSON marshals to JSON.
func (e *Rate) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Value())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (e *Rate) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(e.Value())},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (e *Rate) GetMetadata() Metadata {
	baseMetadata := e.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_GAUGE
	return baseMetadata
}

// Value returns the current value of the Rate.
func (e *Rate) Value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	maybeTick(e)
	return e.wrapped.Value()
}

func (e *Rate) nextTick() time.Time {
	return e.nextT
}

func (e *Rate) tick() {
	e.nextT = e.nextT.Add(e.interval)
	e.wrapped.Add(e.curSum)
	e.curSum = 0
}

// Add adds the given measurement to the Rate.
func (e *Rate) Add(v float64) {
	e.mu.Lock()
	maybeTick(e)
	e.curSum += v
	e.mu.Unlock()
}
