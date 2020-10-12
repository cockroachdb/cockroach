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

	switch t := iterable.(type) {
	case *Counter:
		return t
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

type NestedStruct struct {
	NestedStructGauge *Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (NestedStruct) MetricStruct() {}

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	topGauge := NewGauge(Metadata{Name: "top.gauge"})
	r.AddMetric(topGauge)

	r.AddMetric(NewGaugeFloat64(Metadata{Name: "top.floatgauge"}))

	topCounter := NewCounter(Metadata{Name: "top.counter"})
	r.AddMetric(topCounter)

	r.AddMetric(NewHistogram(Metadata{Name: "top.histogram"}, time.Minute, 1000, 3))

	r.AddMetric(NewGauge(Metadata{Name: "bottom.gauge"}))
	ms := &struct {
		StructGauge         *Gauge
		StructGauge64       *GaugeFloat64
		StructCounter       *Counter
		StructHistogram     *Histogram
		NestedStructGauge   NestedStruct
		ArrayStructCounters [4]*Counter
		// Ensure that nil struct values in arrays are safe.
		NestedStructArray [2]*NestedStruct
		// A few extra ones: either not exported, or not metric objects.
		privateStructGauge            *Gauge
		privateStructGauge64          *GaugeFloat64
		privateStructCounter          *Counter
		privateStructHistogram        *Histogram
		privateNestedStructGauge      NestedStruct
		privateArrayStructCounters    [2]*Counter
		NotAMetric                    int
		AlsoNotAMetric                string
		ReallyNotAMetric              *Registry
		DefinitelyNotAnArrayOfMetrics [2]int
	}{
		StructGauge:     NewGauge(Metadata{Name: "struct.gauge"}),
		StructGauge64:   NewGaugeFloat64(Metadata{Name: "struct.gauge64"}),
		StructCounter:   NewCounter(Metadata{Name: "struct.counter"}),
		StructHistogram: NewHistogram(Metadata{Name: "struct.histogram"}, time.Minute, 1000, 3),
		NestedStructGauge: NestedStruct{
			NestedStructGauge: NewGauge(Metadata{Name: "nested.struct.gauge"}),
		},
		ArrayStructCounters: [...]*Counter{
			NewCounter(Metadata{Name: "array.struct.counter.0"}),
			NewCounter(Metadata{Name: "array.struct.counter.1"}),
			nil, // skipped
			NewCounter(Metadata{Name: "array.struct.counter.3"}),
		},
		NestedStructArray: [2]*NestedStruct{
			0: nil, // skipped
			1: {
				NestedStructGauge: NewGauge(Metadata{Name: "nested.struct.array.1.gauge"}),
			},
		},
		privateStructGauge:     NewGauge(Metadata{Name: "private.struct.gauge"}),
		privateStructGauge64:   NewGaugeFloat64(Metadata{Name: "private.struct.gauge64"}),
		privateStructCounter:   NewCounter(Metadata{Name: "private.struct.counter"}),
		privateStructHistogram: NewHistogram(Metadata{Name: "private.struct.histogram"}, time.Minute, 1000, 3),
		privateNestedStructGauge: NestedStruct{
			NestedStructGauge: NewGauge(Metadata{Name: "private.nested.struct.gauge"}),
		},
		privateArrayStructCounters: [...]*Counter{
			NewCounter(Metadata{Name: "private.array.struct.counter.0"}),
			NewCounter(Metadata{Name: "private.array.struct.counter.1"}),
		},
		NotAMetric:                    0,
		AlsoNotAMetric:                "foo",
		ReallyNotAMetric:              NewRegistry(),
		DefinitelyNotAnArrayOfMetrics: [...]int{1, 2},
	}
	r.AddMetricStruct(ms)

	expNames := map[string]struct{}{
		"top.histogram":               {},
		"top.gauge":                   {},
		"top.floatgauge":              {},
		"top.counter":                 {},
		"bottom.gauge":                {},
		"struct.gauge":                {},
		"struct.gauge64":              {},
		"struct.counter":              {},
		"struct.histogram":            {},
		"nested.struct.gauge":         {},
		"array.struct.counter.0":      {},
		"array.struct.counter.1":      {},
		"array.struct.counter.3":      {},
		"nested.struct.array.1.gauge": {},
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
}
