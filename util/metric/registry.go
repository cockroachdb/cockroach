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
)

// A Registry bundles up various iterables (i.e. typically metrics or other
// registries) to provide a single point of access to them.
type Registry struct {
	sync.Mutex
	tracked map[string]Iterable
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		tracked: map[string]Iterable{},
	}
}

// Add links the given Iterable into this registry using the given format
// string. The individual items in the registry will be formatted via
// fmt.Sprintf(format, <name>). As a special case, metrics itself also implement
// Iterable and can thus be added to a registry.
func (r *Registry) Add(format string, item Iterable) {
	r.Lock()
	r.tracked[format] = item
	r.Unlock()
}

// Each calls the given closure for all metrics.
func (r *Registry) Each(f func(name string, val interface{})) {
	r.Lock()
	defer r.Unlock()
	for format, registry := range r.tracked {
		registry.Each(func(name string, v interface{}) {
			if name == "" {
				f(format, v)
			} else {
				f(fmt.Sprintf(format, name), v)
			}
		})
	}
}

// MarshalJSON marshals to JSON.
func (r *Registry) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	r.Each(func(name string, v interface{}) {
		m[name] = v
	})
	return json.Marshal(m)
}

// Histogram registers a new windowed HDRHistogram with the given
// parameters. Data is kept in the active window for approximately the given
// duration.
func (r *Registry) Histogram(name string, duration time.Duration, unit Unit, maxVal MaxVal, sigFigs int) *Histogram {
	const n = 4
	h := &Histogram{}
	h.maxVal = int64(maxVal)
	h.unit = int64(unit)
	h.interval = duration / n
	h.nextT = time.Now()

	h.windowed = hdrhistogram.NewWindowed(n, 0, h.maxVal/h.unit, sigFigs)
	r.Add(name, h)
	return h
}

// Latency is a convenience function which registers histograms with
// suitable defaults for latency tracking on millisecond to minute time scales.
// The generated names of the metric can be controlled via the given format
// string.
func (r *Registry) Latency(format string) Histograms {
	windows := []timeScale{scale1M, scale10M, scale1H}
	hs := make([]*Histogram, 0, 3)
	for _, w := range windows {
		h := r.Histogram(fmt.Sprintf(format, w.name), w.d, UnitMs, MaxMinute, 2)
		hs = append(hs, h)
	}
	return hs
}

// Counter registers a new counter under the given name.
func (r *Registry) Counter(name string) *Counter {
	c := &Counter{metrics.NewCounter()}
	r.Add(name, c)
	return c
}

// Gauge registers a new Gauge with the given name.
func (r *Registry) Gauge(name string) *Gauge {
	g := &Gauge{metrics.NewGauge()}
	r.Add(name, g)
	return g
}

// Rate registers an EWMA rate over the given timescale. Timescales at
// or below 2s are illegal and will cause a panic.
func (r *Registry) Rate(name string, timescale time.Duration) *Rate {
	const tickInterval = time.Second
	if timescale <= 2*time.Second {
		panic(fmt.Sprintf("EWMA with per-second ticks makes no sense on timescale %s", timescale))
	}
	avgAge := float64(timescale) / float64(2*tickInterval)

	e := &Rate{
		interval: tickInterval,
		nextT:    time.Now(),
		wrapped:  ewma.NewMovingAverage(avgAge),
	}
	r.Add(name, e)
	return e
}

// Rates returns a slice of EWMAs with the given format string and
// various "standard" timescales.
func (r *Registry) Rates(format string) Rates {
	scales := []timeScale{scale1M, scale10M, scale1H}
	es := make([]*Rate, 0, len(scales))
	for _, scale := range scales {
		es = append(es, r.Rate(fmt.Sprintf(format, scale.name),
			scale.d))
	}
	return es
}
