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

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	topGauge := NewGauge(MetricMetadata{"top.gauge", ""})
	r.AddMetric(topGauge)

	r.AddMetric(NewGaugeFloat64(MetricMetadata{"top.floatgauge", ""}))

	topCounter := NewCounter(MetricMetadata{"top.counter", ""})
	r.AddMetric(topCounter)

	topRate := NewRate(MetricMetadata{"top.rate", ""}, time.Minute)
	r.AddMetric(topRate)

	r.AddMetricGroup(NewRates(MetricMetadata{"top.rates", ""}))
	r.AddMetric(NewHistogram(MetricMetadata{"top.hist", ""}, time.Minute, 1000, 3))
	r.AddMetricGroup(NewLatency(MetricMetadata{"top.latency", ""}))

	r.AddMetric(NewGauge(MetricMetadata{"bottom.gauge", ""}))
	r.AddMetricGroup(NewRates(MetricMetadata{"bottom.rates", ""}))

	expNames := map[string]struct{}{
		"top.rate":           {},
		"top.rates-count":    {},
		"top.rates-1m":       {},
		"top.rates-10m":      {},
		"top.rates-1h":       {},
		"top.hist":           {},
		"top.latency-1m":     {},
		"top.latency-10m":    {},
		"top.latency-1h":     {},
		"top.gauge":          {},
		"top.floatgauge":     {},
		"top.counter":        {},
		"bottom.gauge":       {},
		"bottom.rates-count": {},
		"bottom.rates-1m":    {},
		"bottom.rates-10m":   {},
		"bottom.rates-1h":    {},
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
	if g := r.GetGauge("top.gauge"); g != topGauge {
		t.Errorf("GetGauge returned %v, expected %v", g, topGauge)
	}
	if g := r.GetGauge("bad"); g != nil {
		t.Errorf("GetGauge returned non-nil %v, expected nil", g)
	}
	if g := r.GetGauge("top.hist"); g != nil {
		t.Errorf("GetGauge returned non-nil %v of type %T when requesting non-gauge, expected nil", g, g)
	}

	if c := r.GetCounter("top.counter"); c != topCounter {
		t.Errorf("GetCounter returned %v, expected %v", c, topCounter)
	}
	if c := r.GetCounter("bad"); c != nil {
		t.Errorf("GetCounter returned non-nil %v, expected nil", c)
	}
	if c := r.GetCounter("top.hist"); c != nil {
		t.Errorf("GetCounter returned non-nil %v of type %T when requesting non-counter, expected nil", c, c)
	}

	if r := r.GetRate("top.rate"); r != topRate {
		t.Errorf("GetRate returned %v, expected %v", r, topRate)
	}
	if r := r.GetRate("bad"); r != nil {
		t.Errorf("GetRate returned non-nil %v, expected nil", r)
	}
	if r := r.GetRate("top.hist"); r != nil {
		t.Errorf("GetRate returned non-nil %v of type %T when requesting non-rate, expected nil", r, r)
	}
}
