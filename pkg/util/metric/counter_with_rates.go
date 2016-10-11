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

package metric

import "time"

// A TimeScale is a named duration.
type TimeScale struct {
	name string
	d    time.Duration
}

// DefaultTimeScales are the durations used for helpers which create windowed
// metrics in bulk (such as Rates).
var DefaultTimeScales = []TimeScale{Scale1M, Scale10M, Scale1H}

var (
	// Scale1M is a 1 minute window for windowed stats (e.g. Rates and Histograms).
	Scale1M = TimeScale{"1m", 1 * time.Minute}

	// Scale10M is a 10 minute window for windowed stats (e.g. Rates and Histograms).
	Scale10M = TimeScale{"10m", 10 * time.Minute}

	// Scale1H is a 1 hour window for windowed stats (e.g. Rates and Histograms).
	Scale1H = TimeScale{"1h", time.Hour}
)

// CounterWithRates is a counter and associated EWMA backed rates at different
// time scales (which are ignored when visiting the object, and are thus not
// exported).
type CounterWithRates struct {
	*Counter
	Rates map[TimeScale]*Rate
}

// NewCounterWithRates registers and returns a new Counter along with some
// a set of EWMA-based rates at time scales specified by DefaultTimeScales.
// These rates are provided for convenience and are not exported to metrics
// frameworks. If they are not required, a Counter should be used instead.
func NewCounterWithRates(metadata Metadata) *CounterWithRates {
	scales := DefaultTimeScales
	es := make(map[TimeScale]*Rate)
	for _, scale := range scales {
		es[scale] = NewRate(scale.d)
	}
	c := NewCounter(
		Metadata{
			Name:   metadata.Name,
			Help:   metadata.Help,
			labels: metadata.labels,
		})
	return &CounterWithRates{Counter: c, Rates: es}
}

// Inc increments the counter.
func (c *CounterWithRates) Inc(v int64) {
	c.Counter.Inc(v)
	for _, e := range c.Rates {
		e.Add(float64(v))
	}
}
