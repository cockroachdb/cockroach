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
// For the purpose of this method, a CounterWithRate is a Counter.
func (r *Registry) getCounter(name string) *Counter {
	r.Lock()
	defer r.Unlock()
	iterable := r.findMetricByName(name)
	if iterable == nil {
		return nil
	}

	switch t := iterable.(type) {
	case *Counter:
		return t
	case *CounterWithRates:
		return t.Counter
	default:
	}
	return nil
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

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	topGauge := NewGauge(Metadata{Name: "top.gauge"})
	r.AddMetric(topGauge)

	r.AddMetric(NewGaugeFloat64(Metadata{Name: "top.floatgauge"}))

	topCounter := NewCounter(Metadata{Name: "top.counter"})
	r.AddMetric(topCounter)

	topCounterWithRates := NewCounterWithRates(Metadata{Name: "top.counterwithrates"})
	r.AddMetric(topCounterWithRates)

	r.AddMetric(NewHistogram(Metadata{Name: "top.histogram"}, time.Minute, 1000, 3))

	r.AddMetric(NewGauge(Metadata{Name: "bottom.gauge"}))
	ms := &struct {
		StructGauge            *Gauge
		StructGauge64          *GaugeFloat64
		StructCounter          *Counter
		StructHistogram        *Histogram
		StructCounterWithRates *CounterWithRates
		// A few extra ones: either not exported, or not metric objects.
		privateStructGauge   *Gauge
		privateStructGauge64 *GaugeFloat64
		NotAMetric           int
		AlsoNotAMetric       string
		ReallyNotAMetric     *Registry
	}{
		StructGauge:            NewGauge(Metadata{Name: "struct.gauge"}),
		StructGauge64:          NewGaugeFloat64(Metadata{Name: "struct.gauge64"}),
		StructCounter:          NewCounter(Metadata{Name: "struct.counter"}),
		StructHistogram:        NewHistogram(Metadata{Name: "struct.histogram"}, time.Minute, 1000, 3),
		StructCounterWithRates: NewCounterWithRates(Metadata{Name: "struct.counterwithrates"}),
		privateStructGauge:     NewGauge(Metadata{Name: "struct.private-gauge"}),
		privateStructGauge64:   NewGaugeFloat64(Metadata{Name: "struct.private-gauge64"}),
		NotAMetric:             0,
		AlsoNotAMetric:         "foo",
		ReallyNotAMetric:       NewRegistry(),
	}
	r.AddMetricStruct(ms)

	expNames := map[string]struct{}{
		"top.counterwithrates":    {},
		"top.histogram":           {},
		"top.gauge":               {},
		"top.floatgauge":          {},
		"top.counter":             {},
		"bottom.gauge":            {},
		"struct.gauge":            {},
		"struct.gauge64":          {},
		"struct.counter":          {},
		"struct.histogram":        {},
		"struct.counterwithrates": {},
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
	if c, e := r.getCounter("top.counterwithrates"), topCounterWithRates.Counter; c != e {
		t.Errorf("getCounter returned %v, expected %v", c, e)
	}
	if c := r.getCounter("bad"); c != nil {
		t.Errorf("getCounter returned non-nil %v, expected nil", c)
	}
	if c := r.getCounter("top.histogram"); c != nil {
		t.Errorf("getCounter returned non-nil %v of type %T when requesting non-counter, expected nil", c, c)
	}
}
