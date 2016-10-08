// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package metric

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/codahale/hdrhistogram"
	"github.com/gogo/protobuf/proto"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/rcrowley/go-metrics"

	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	// MaxLatency is the maximum value tracked in latency histograms. Higher
	// values will be recorded as this value instead.
	MaxLatency = 10 * time.Second

	// Data will leave windowed histograms after approximately this duration.
	latencyRotationInterval = 20 * time.Second
	// The number of histograms to keep in rolling window.
	histWrapNum = 2
)

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// GetName returns the fully-qualified name of the metric.
	GetName() string
	// GetHelp returns the help text for the metric.
	GetHelp() string
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
	ToPrometheusMetric() *prometheusgo.Metric
}

// Metadata holds metadata about a metric. It must be embedded in
// each metric object.
type Metadata struct {
	Name, Help string
	labels     []*prometheusgo.LabelPair
}

// GetName returns the metric's name.
func (m *Metadata) GetName() string {
	return m.Name
}

// GetHelp returns the metric's help string.
func (m *Metadata) GetHelp() string {
	return m.Help
}

// GetLabels returns the metric's labels.
func (m *Metadata) GetLabels() []*prometheusgo.LabelPair {
	return m.labels
}

// AddLabel adds a label/value pair for this metric.
func (m *Metadata) AddLabel(name, value string) {
	m.labels = append(m.labels,
		&prometheusgo.LabelPair{
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
var _ json.Marshaler = &Rate{}
var _ json.Marshaler = &Registry{}

var _ PrometheusExportable = &Gauge{}
var _ PrometheusExportable = &GaugeFloat64{}
var _ PrometheusExportable = &Counter{}
var _ PrometheusExportable = &Histogram{}

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

// TODO(tschottdorf): remove this method when
// https://github.com/codahale/hdrhistogram/pull/22 is merged and the
// dependency bumped.
func cloneHistogram(in *hdrhistogram.Histogram) *hdrhistogram.Histogram {
	out := in.Export()
	out.Counts = append([]int64(nil), out.Counts...)
	return hdrhistogram.Import(out)
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

// NewLatency is a convenience function which returns a histograms with
// suitable defaults for latency tracking. Values are expressed in ns,
// are truncated into the interval [0, MaxLatency] and are recorded
// with one digit of precision (i.e. errors of <10ms at 100ms, <6s at 60s).
func NewLatency(metadata Metadata) *Histogram {
	return NewHistogram(
		metadata, latencyRotationInterval, MaxLatency.Nanoseconds(), 1,
	)
}

// Windowed returns a copy of the current windowed histogram data.
func (h *Histogram) Windowed() *hdrhistogram.Histogram {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneHistogram(h.mu.sliding.Current())
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

// A Counter holds a single mutable atomic value.
type Counter struct {
	Metadata
	metrics.Counter
}

// NewCounter creates a counter.
func NewCounter(metadata Metadata) *Counter {
	return &Counter{metadata, metrics.NewCounter()}
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

// A Gauge atomically stores a single integer value.
type Gauge struct {
	Metadata
	metrics.Gauge
}

// NewGauge creates a Gauge.
func NewGauge(metadata Metadata) *Gauge {
	return &Gauge{metadata, metrics.NewGauge()}
}

// GetType returns the prometheus type enum for this metric.
func (g *Gauge) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with the empty string and itself.
func (g *Gauge) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Gauge.Value())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (g *Gauge) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(float64(g.Gauge.Value()))},
	}
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

// Inspect calls the given closure with the empty string and itself.
func (g *GaugeFloat64) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *GaugeFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.GaugeFloat64.Value())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (g *GaugeFloat64) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(g.GaugeFloat64.Value())},
	}
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

// Inspect calls the given closure with the empty string and the Rate's current
// value.
//
// TODO(mrtracy): Fix this to pass the Rate object itself to 'f', to
// match the 'visitor' behavior as the other metric types (currently, it passes
// the current value of the Rate as a float64.)
func (e *Rate) Inspect(f func(interface{})) {
	e.mu.Lock()
	maybeTick(e)
	v := e.wrapped.Value()
	e.mu.Unlock()
	f(v)
}

// MarshalJSON marshals to JSON.
func (e *Rate) MarshalJSON() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	maybeTick(e)
	return json.Marshal(e.wrapped.Value())
}
