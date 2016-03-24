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
	"errors"
	"fmt"
	"sync"
	"time"
)

const sep = "-"

// DefaultTimeScales are the durations used for helpers which create windowed
// metrics in bulk (such as Latency or Rates).
var DefaultTimeScales = []TimeScale{Scale1M, Scale10M, Scale1H}

// A Registry bundles up various iterables (i.e. typically metrics or other
// registries) to provide a single point of access to them.
//
// A Registry can be added to another Registry through the Add/MustAdd methods. This allows a
// hierarchy of Registry instances to be created.
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
// fmt.Sprintf(format, <name>). As a special case, *Registry implements
// Iterable and can thus be added.
// Metric types in this package have helpers that allow them to be created
// and registered in a single step. Add is called manually only when adding
// a registry to another, or when integrating metrics defined elsewhere.
func (r *Registry) Add(format string, item Iterable) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.tracked[format]; ok {
		return errors.New("format string already in use")
	}
	r.tracked[format] = item
	return nil
}

// MustAdd calls Add and panics on error.
func (r *Registry) MustAdd(format string, item Iterable) {
	if err := r.Add(format, item); err != nil {
		panic(fmt.Sprintf("error adding %s: %s", format, err))
	}
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

// Histogram registers a new windowed HDRHistogram with the given parameters.
// Data is kept in the active window for approximately the given duration.
func (r *Registry) Histogram(name string, duration time.Duration, maxVal int64,
	sigFigs int) *Histogram {
	h := NewHistogram(duration, maxVal, sigFigs)
	r.MustAdd(name, h)
	return h
}

// Latency is a convenience function which registers histograms with
// suitable defaults for latency tracking. Values are expressed in ns,
// are truncated into the interval [0, time.Minute] and are recorded
// with two digits of precision (i.e. errors of <1ms at 100ms, <.6s at 1m).
// The generated names of the metric will begin with the given prefix.
//
// TODO(mrtracy,tschottdorf): need to discuss roll-ups and generally how (and
// which) information flows between metrics and time series.
func (r *Registry) Latency(prefix string) Histograms {
	windows := DefaultTimeScales
	hs := make(Histograms)
	for _, w := range windows {
		hs[w] = r.Histogram(prefix+sep+w.name, w.d, int64(time.Minute), 2)
	}
	return hs
}

// Counter registers new counter to the registry.
func (r *Registry) Counter(name string) *Counter {
	c := NewCounter()
	r.MustAdd(name, c)
	return c
}

// GetCounter returns the Counter in this registry with the given name. If a
// Counter with this name is not present (including if a non-Counter Iterable is
// registered with the name), nil is returned.
func (r *Registry) GetCounter(name string) *Counter {
	r.Lock()
	defer r.Unlock()
	iterable, ok := r.tracked[name]
	if !ok {
		return nil
	}
	counter, ok := iterable.(*Counter)
	if !ok {
		return nil
	}
	return counter
}

// Gauge registers a new Gauge with the given name.
func (r *Registry) Gauge(name string) *Gauge {
	g := NewGauge()
	r.MustAdd(name, g)
	return g
}

// GetGauge returns the Gauge in this registry with the given name. If a Gauge
// with this name is not present (including if a non-Gauge Iterable is
// registered with the name), nil is returned.
func (r *Registry) GetGauge(name string) *Gauge {
	r.Lock()
	defer r.Unlock()
	iterable, ok := r.tracked[name]
	if !ok {
		return nil
	}
	gauge, ok := iterable.(*Gauge)
	if !ok {
		return nil
	}
	return gauge
}

// GaugeFloat64 registers a new GaugeFloat64 with the given name.
func (r *Registry) GaugeFloat64(name string) *GaugeFloat64 {
	g := NewGaugeFloat64()
	r.MustAdd(name, g)
	return g
}

// Rate creates an EWMA rate over the given timescale. The comments on NewRate
// apply.
func (r *Registry) Rate(name string, timescale time.Duration) *Rate {
	e := NewRate(timescale)
	r.MustAdd(name, e)
	return e
}

// GetRate returns the Rate in this registry with the given name. If a Rate with
// this name is not present (including if a non-Rate Iterable is registered with
// the name), nil is returned.
func (r *Registry) GetRate(name string) *Rate {
	r.Lock()
	defer r.Unlock()
	iterable, ok := r.tracked[name]
	if !ok {
		return nil
	}
	rate, ok := iterable.(*Rate)
	if !ok {
		return nil
	}
	return rate
}

// Rates registers and returns a new Rates instance, which contains a set of EWMA-based rates
// with generally useful time scales and a cumulative counter.
func (r *Registry) Rates(prefix string) Rates {
	scales := DefaultTimeScales
	es := make(map[TimeScale]*Rate)
	for _, scale := range scales {
		es[scale] = r.Rate(prefix+sep+scale.name, scale.d)
	}
	c := r.Counter(prefix + sep + "count")
	return Rates{Counter: c, Rates: es}
}
