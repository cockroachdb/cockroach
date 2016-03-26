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
	"sync"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/codahale/hdrhistogram"
	"github.com/rcrowley/go-metrics"

	"github.com/cockroachdb/cockroach/util/timeutil"
)

const histWrapNum = 4 // number of histograms to keep in rolling window

// A TimeScale is a named duration.
type TimeScale struct {
	name string
	d    time.Duration
}

// Name returns the name of the TimeScale.
func (ts TimeScale) Name() string {
	return ts.name
}

var (
	// Scale1M is a 1 minute window for windowed stats (e.g. Rates and Histograms).
	Scale1M = TimeScale{"1m", 1 * time.Minute}

	// Scale10M is a 10 minute window for windowed stats (e.g. Rates and Histograms).
	Scale10M = TimeScale{"10m", 10 * time.Minute}

	// Scale1H is a 1 hour window for windowed stats (e.g. Rates and Histograms).
	Scale1H = TimeScale{"1h", time.Hour}
)

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// Each calls the given closure with each contained item.
	Each(func(string, interface{}))
}

var _ Iterable = &Gauge{}
var _ Iterable = &GaugeFloat64{}
var _ Iterable = &Counter{}
var _ Iterable = &Histogram{}
var _ Iterable = &Rate{}

var _ json.Marshaler = &Gauge{}
var _ json.Marshaler = &GaugeFloat64{}
var _ json.Marshaler = &Counter{}
var _ json.Marshaler = &Histogram{}
var _ json.Marshaler = &Rate{}
var _ json.Marshaler = &Registry{}

type periodic interface {
	nextTick() time.Time
	tick()
}

var _ periodic = &Histogram{}
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

func maybeTick(m periodic) {
	for m.nextTick().Before(now()) {
		m.tick()
	}
}

// A Histogram is a wrapper around an hdrhistogram.WindowedHistogram.
type Histogram struct {
	maxVal int64

	mu       sync.Mutex
	windowed *hdrhistogram.WindowedHistogram
	nextT    time.Time
	duration time.Duration
}

// NewHistogram creates a new windowed HDRHistogram with the given parameters.
// Data is kept in the active window for approximately the given duration.
// See the the documentation for hdrhistogram.WindowedHistogram for details.
func NewHistogram(duration time.Duration, maxVal int64, sigFigs int) *Histogram {
	h := &Histogram{}
	h.maxVal = int64(maxVal)
	h.nextT = now()
	h.duration = duration

	h.windowed = hdrhistogram.NewWindowed(histWrapNum, 0, h.maxVal, sigFigs)
	return h
}

func (h *Histogram) tick() {
	h.nextT = h.nextT.Add(h.duration / histWrapNum)
	h.windowed.Rotate()
}

func (h *Histogram) nextTick() time.Time {
	return h.nextT
}

// Duration returns the duration that this histogram spans.
func (h *Histogram) Duration() time.Duration {
	return h.duration
}

// TODO(pmattis): Histogram.Duration is neither used or tested. Silence unused
// warning.
var _ = (*Histogram).Duration

// MarshalJSON outputs to JSON.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.Current().CumulativeDistribution())
}

// RecordValue adds the given value to the histogram, truncating if necessary.
func (h *Histogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	for h.windowed.Current.RecordValue(v) != nil {
		v = h.maxVal
	}
}

// Current returns a copy of the data currently in the window.
func (h *Histogram) Current() *hdrhistogram.Histogram {
	h.mu.Lock()
	maybeTick(h)
	export := h.windowed.Merge().Export()
	// Make a deep copy of export.Counts, because (*hdrhistogram.Histogram).Export() does
	// a shallow copy of the histogram counts, which leads to data races when multiple goroutines
	// call this method.
	// TODO(cdo): Remove this when I've gotten a chance to submit a PR for the proper fix
	// to hdrhistogram.
	export.Counts = append([]int64(nil), export.Counts...)
	h.mu.Unlock()
	return hdrhistogram.Import(export)
}

// Each calls the closure with the empty string and the receiver.
func (h *Histogram) Each(f func(string, interface{})) {
	h.mu.Lock()
	maybeTick(h)
	h.mu.Unlock()
	f("", h)
}

// Histograms is a map of Histogram metrics.
type Histograms map[TimeScale]*Histogram

// RecordValue calls through to each individual Histogram.
func (hs Histograms) RecordValue(v int64) {
	for _, h := range hs {
		h.RecordValue(v)
	}
}

// A Counter holds a single mutable atomic value.
type Counter struct {
	metrics.Counter
}

// NewCounter creates a counter.
func NewCounter() *Counter {
	return &Counter{metrics.NewCounter()}
}

// Each calls the given closure with the empty string and itself.
func (c *Counter) Each(f func(string, interface{})) { f("", c) }

// MarshalJSON marshals to JSON.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Counter.Count())
}

// A Gauge atomically stores a single integer value.
type Gauge struct {
	metrics.Gauge
}

// NewGauge creates a Gauge.
func NewGauge() *Gauge {
	g := &Gauge{metrics.NewGauge()}
	return g
}

// Each calls the given closure with the empty string and itself.
func (g *Gauge) Each(f func(string, interface{})) { f("", g) }

// MarshalJSON marshals to JSON.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Gauge.Value())
}

// A GaugeFloat64 atomically stores a single float64 value.
type GaugeFloat64 struct {
	metrics.GaugeFloat64
}

// NewGaugeFloat64 creates a GaugeFloat64.
func NewGaugeFloat64() *GaugeFloat64 {
	g := &GaugeFloat64{metrics.NewGaugeFloat64()}
	return g
}

// Each calls the given closure with the empty string and itself.
func (g *GaugeFloat64) Each(f func(string, interface{})) { f("", g) }

// MarshalJSON marshals to JSON.
func (g *GaugeFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.GaugeFloat64.Value())
}

// A Rate is a exponential weighted moving average.
type Rate struct {
	mu       sync.Mutex // protects fields below
	curSum   float64
	wrapped  ewma.MovingAverage
	interval time.Duration
	nextT    time.Time
}

// NewRate creates an EWMA rate on the given timescale. Timescales at
// or below 2s are illegal and will cause a panic.
func NewRate(timescale time.Duration) *Rate {
	const tickInterval = time.Second
	if timescale <= 2*time.Second {
		panic(fmt.Sprintf("EWMA with per-second ticks makes no sense on timescale %s", timescale))
	}
	avgAge := float64(timescale) / float64(2*tickInterval)

	return &Rate{
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

// Each calls the given closure with the empty string and the Rate's current
// value. TODO(mrtracy): Fix this to pass the Rate object itself to 'f', to
// match the 'visitor' behavior as the other metric types (currently, it passes
// the current value of the Rate as a float64.)
func (e *Rate) Each(f func(string, interface{})) {
	e.mu.Lock()
	maybeTick(e)
	v := e.wrapped.Value()
	e.mu.Unlock()
	f("", v)
}

// MarshalJSON marshals to JSON.
func (e *Rate) MarshalJSON() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	maybeTick(e)
	return json.Marshal(e.wrapped.Value())
}

// Rates is a counter and associated EWMA backed rates at different time scales.
type Rates struct {
	*Counter
	Rates map[TimeScale]*Rate
}

// Add adds the given value to all contained objects.
func (es Rates) Add(v int64) {
	es.Counter.Inc(v)
	for _, e := range es.Rates {
		e.Add(float64(v))
	}
}
