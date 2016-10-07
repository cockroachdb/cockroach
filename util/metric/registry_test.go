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
	"testing"
	"time"
)

func (r *Registry) findMetricByName(name string) Iterable {
	for _, metric := range r.tracked {
		if metric.GetName() == name {
			return metric
		}
	}
	return nil
}

// getCounter returns the Counter in this registry with the given name. If a
// Counter with this name is not present (including if a non-Counter Iterable is
// registered with the name), nil is returned.
func (r *Registry) getCounter(name string) *Counter {
	r.Lock()
	defer r.Unlock()
	iterable := r.findMetricByName(name)
	if iterable == nil {
		return nil
	}
	counter, ok := iterable.(*Counter)
	if !ok {
		return nil
	}
	return counter
}

// getGauge returns the Gauge in this registry with the given name. If a Gauge
// with this name is not present (including if a non-Gauge Iterable is
// registered with the name), nil is returned.
func (r *Registry) getGauge(name string) *Gauge {
	r.Lock()
	defer r.Unlock()
	iterable := r.findMetricByName(name)
	if iterable == nil {
		return nil
	}
	gauge, ok := iterable.(*Gauge)
	if !ok {
		return nil
	}
	return gauge
}

// getRate returns the Rate in this registry with the given name. If a Rate with
// this name is not present (including if a non-Rate Iterable is registered with
// the name), nil is returned.
func (r *Registry) getRate(name string) *Rate {
	r.Lock()
	defer r.Unlock()
	iterable := r.findMetricByName(name)
	if iterable == nil {
		return nil
	}
	rate, ok := iterable.(*Rate)
	if !ok {
		return nil
	}
	return rate
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	topGauge := NewGauge(Metadata{Name: "top.gauge"})
	r.AddMetric(topGauge)

	r.AddMetric(NewGaugeFloat64(Metadata{Name: "top.floatgauge"}))

	topCounter := NewCounter(Metadata{Name: "top.counter"})
	r.AddMetric(topCounter)

	topRate := NewRate(Metadata{Name: "top.rate"}, time.Minute)
	r.AddMetric(topRate)

	r.AddMetricGroup(NewRates(Metadata{Name: "top.rates"}))
	r.AddMetric(NewHistogram(Metadata{Name: "top.histogram"}, time.Minute, 1000, 3))

	r.AddMetric(NewGauge(Metadata{Name: "bottom.gauge"}))
	r.AddMetricGroup(NewRates(Metadata{Name: "bottom.rates"}))
	ms := &struct {
		StructGauge     *Gauge
		StructGauge64   *GaugeFloat64
		StructCounter   *Counter
		StructHistogram *Histogram
		StructRate      *Rate
		StructRates     Rates
		// A few extra ones: either not exported, or not metric objects.
		privateStructGauge   *Gauge
		privateStructGauge64 *GaugeFloat64
		NotAMetric           int
		AlsoNotAMetric       string
		ReallyNotAMetric     *Registry
	}{
		StructGauge:          NewGauge(Metadata{Name: "struct.gauge"}),
		StructGauge64:        NewGaugeFloat64(Metadata{Name: "struct.gauge64"}),
		StructCounter:        NewCounter(Metadata{Name: "struct.counter"}),
		StructHistogram:      NewHistogram(Metadata{Name: "struct.histogram"}, time.Minute, 1000, 3),
		StructRate:           NewRate(Metadata{Name: "struct.rate"}, time.Minute),
		StructRates:          NewRates(Metadata{Name: "struct.rates"}),
		privateStructGauge:   NewGauge(Metadata{Name: "struct.private-gauge"}),
		privateStructGauge64: NewGaugeFloat64(Metadata{Name: "struct.private-gauge64"}),
		NotAMetric:           0,
		AlsoNotAMetric:       "foo",
		ReallyNotAMetric:     NewRegistry(),
	}
	r.AddMetricStruct(ms)

	expNames := map[string]struct{}{
		"top.rate":           {},
		"top.rates-count":    {},
		"top.rates-1m":       {},
		"top.rates-10m":      {},
		"top.rates-1h":       {},
		"top.histogram":      {},
		"top.gauge":          {},
		"top.floatgauge":     {},
		"top.counter":        {},
		"bottom.gauge":       {},
		"bottom.rates-count": {},
		"bottom.rates-1m":    {},
		"bottom.rates-10m":   {},
		"bottom.rates-1h":    {},
		"struct.gauge":       {},
		"struct.gauge64":     {},
		"struct.counter":     {},
		"struct.histogram":   {},
		"struct.rate":        {},
		"struct.rates-count": {},
		"struct.rates-1m":    {},
		"struct.rates-10m":   {},
		"struct.rates-1h":    {},
	}

	r.Each(func(name string, _ interface{}) {
		if _, exist := expNames[name]; !exist {
			t.Errorf("unexpected name: %s", name)
		}
		delete(expNames, name)
	})
	if len(expNames) > 0 {
		t.Fatalf("missed names: %v", expNames)
	}

	// Test get functions
	if g := r.getGauge("top.gauge"); g != topGauge {
		t.Errorf("getGauge returned %v, expected %v", g, topGauge)
	}
	if g := r.getGauge("bad"); g != nil {
		t.Errorf("getGauge returned non-nil %v, expected nil", g)
	}
	if g := r.getGauge("top.histogram"); g != nil {
		t.Errorf("getGauge returned non-nil %v of type %T when requesting non-gauge, expected nil", g, g)
	}

	if c := r.getCounter("top.counter"); c != topCounter {
		t.Errorf("getCounter returned %v, expected %v", c, topCounter)
	}
	if c := r.getCounter("bad"); c != nil {
		t.Errorf("getCounter returned non-nil %v, expected nil", c)
	}
	if c := r.getCounter("top.histogram"); c != nil {
		t.Errorf("getCounter returned non-nil %v of type %T when requesting non-counter, expected nil", c, c)
	}

	if r := r.getRate("top.rate"); r != topRate {
		t.Errorf("getRate returned %v, expected %v", r, topRate)
	}
	if r := r.getRate("bad"); r != nil {
		t.Errorf("getRate returned non-nil %v, expected nil", r)
	}
	if r := r.getRate("top.histogram"); r != nil {
		t.Errorf("getRate returned non-nil %v of type %T when requesting non-rate, expected nil", r, r)
	}
}
