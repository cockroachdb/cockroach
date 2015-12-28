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
	"sync"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/codahale/hdrhistogram"
	"github.com/rcrowley/go-metrics"
)

type timeScale struct {
	name string
	d    time.Duration
}

var scale1M = timeScale{"1m", 1 * time.Minute}
var scale10M = timeScale{"10m", 10 * time.Minute}
var scale1H = timeScale{"1h", time.Hour}

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// Each calls the given closure with each contained item. The closure must
	// copy values it plans to use after returning.
	Each(func(string, interface{}))
}

var _ Iterable = &Gauge{}
var _ Iterable = &Counter{}
var _ Iterable = &Histogram{}
var _ Iterable = &Rate{}

var _ json.Marshaler = &Gauge{}
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

// Unit is a base unit for a histogram.
type Unit int64

// UnitMs is the number of milliseconds in a nanosecond.
const UnitMs = Unit(time.Millisecond)

// MaxVal is a maximum value for a histogram.
type MaxVal int64

// MaxMinute truncates histogram values larger than one minute.
const MaxMinute = MaxVal(time.Minute)

func maybeTick(m periodic) {
	for m.nextTick().Before(time.Now()) {
		m.tick()
	}
}

// A Histogram is a wrapper around an hdrhistogram.WindowedHistogram.
type Histogram struct {
	unit   int64
	maxVal int64

	mu       sync.Mutex
	windowed *hdrhistogram.WindowedHistogram
	interval time.Duration
	nextT    time.Time
}

func (h *Histogram) tick() {
	h.nextT = h.nextT.Add(h.interval)
	h.windowed.Rotate()
}

func (h *Histogram) nextTick() time.Time {
	return h.nextT
}

// MarshalJSON outputs to JSON.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	return json.Marshal(h.windowed.Merge().CumulativeDistribution())
}

// RecordValue adds the given value to the histogram, truncating if necessary.
func (h *Histogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	v /= h.unit
	for h.windowed.Current.RecordValue(v) != nil {
		v = h.maxVal / h.unit
	}
}

// Each calls the closure with the empty string and the (locked) receiver.
func (h *Histogram) Each(f func(string, interface{})) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	f("", h)
}

// Histograms is a slice of Histogram metrics.
type Histograms []*Histogram

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

// Each calls the given closure with the empty string and itself.
func (c *Counter) Each(f func(string, interface{})) { f("", c) }

// MarshalJSON marshals to JSON.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Counter.Count())
}

// A Gauge atomically stores a single value.
type Gauge struct {
	metrics.Gauge
}

// Each calls the given closure with the empty string and itself.
func (g *Gauge) Each(f func(string, interface{})) { f("", g) }

// MarshalJSON marshals to JSON.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Gauge.Value())
}

// A Rate is a exponential weighted moving average.
type Rate struct {
	mu       sync.Mutex // protects fields below
	curSum   float64
	wrapped  ewma.MovingAverage
	interval time.Duration
	nextT    time.Time
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

// Each calls the given closure with the empty string and the Rate.
func (e *Rate) Each(f func(string, interface{})) {
	e.mu.Lock()
	defer e.mu.Unlock()
	maybeTick(e)
	f("", e.wrapped.Value())
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
	Rates []*Rate
}

// Add adds the given value to all contained objects.
func (es Rates) Add(v int64) {
	es.Counter.Inc(v)
	for _, e := range es.Rates {
		e.Add(float64(v))
	}
}
