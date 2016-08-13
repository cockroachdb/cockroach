// Copyright 2016 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package metric

import "time"

// A TimeScale is a named duration.
type TimeScale struct {
	name string
	d    time.Duration
}

// DefaultTimeScales are the durations used for helpers which create windowed
// metrics in bulk (such as Latency or Rates).
var DefaultTimeScales = []TimeScale{Scale1M, Scale10M, Scale1H}

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

// Histograms is a map of Histogram metrics.
type Histograms map[TimeScale]*Histogram

// metricGroup defines a metric that is composed of multiple metrics.
// It can be used directly by the code updating metrics, and expands
// to multiple individual metrics to add to a registry.
type metricGroup interface {
	// iterate will run the callback for every individual metric in the metric group.
	iterate(func(Iterable))
}

// NewLatency is a convenience function which registers histograms with
// suitable defaults for latency tracking. Values are expressed in ns,
// are truncated into the interval [0, time.Minute] and are recorded
// with two digits of precision (i.e. errors of <1ms at 100ms, <.6s at 1m).
// The generated names of the metric will begin with the given prefix.
//
// TODO(mrtracy,tschottdorf): need to discuss roll-ups and generally how (and
// which) information flows between metrics and time series.
func NewLatency(metadata Metadata) Histograms {
	windows := DefaultTimeScales
	hs := make(Histograms)
	for _, w := range windows {
		hs[w] = NewHistogram(Metadata{metadata.Name + sep + w.name, metadata.Help},
			w.d, int64(time.Minute), 2)
	}
	return hs
}

// RecordValue calls through to each individual Histogram.
func (hs Histograms) RecordValue(v int64) {
	for _, h := range hs {
		h.RecordValue(v)
	}
}

// iterate runs the callback function with individual histograms.
func (hs Histograms) iterate(cb func(Iterable)) {
	for _, h := range hs {
		cb(h)
	}
}

// Rates is a counter and associated EWMA backed rates at different time scales.
type Rates struct {
	*Counter
	Rates map[TimeScale]*Rate
}

// NewRates registers and returns a new Rates instance, which contains a set of EWMA-based rates
// with generally useful time scales and a cumulative counter.
func NewRates(metadata Metadata) Rates {
	scales := DefaultTimeScales
	es := make(map[TimeScale]*Rate)
	for _, scale := range scales {
		es[scale] = NewRate(Metadata{metadata.Name + sep + scale.name, metadata.Help}, scale.d)
	}
	c := NewCounter(Metadata{metadata.Name + sep + "count", metadata.Help})
	return Rates{Counter: c, Rates: es}
}

// Add adds the given value to all contained objects.
func (es Rates) Add(v int64) {
	es.Counter.Inc(v)
	for _, e := range es.Rates {
		e.Add(float64(v))
	}
}

// iterate runs the callback function with the counter and the individual rates.
func (es Rates) iterate(cb func(Iterable)) {
	cb(es.Counter)
	for _, e := range es.Rates {
		cb(e)
	}
}
