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
	sub := NewRegistry()

	_ = r.Gauge("top.gauge")
	_ = r.Rate("top.rate", time.Minute)
	_ = r.Rates("top.rates")
	_ = r.Histogram("top.hist", time.Minute, 1000, 3)
	_ = r.Latency("top.latency")

	_ = sub.Gauge("gauge")
	r.MustAdd("bottom.%s#1", sub)
	if err := r.Add("bottom.%s#1", sub); err == nil {
		t.Fatalf("expected failure on double-add")
	}
	_ = sub.Rates("rates")

	expNames := map[string]struct{}{
		"top.rate":             {},
		"top.rates-count":      {},
		"top.rates-1m":         {},
		"top.rates-10m":        {},
		"top.rates-1h":         {},
		"top.hist":             {},
		"top.latency-1m":       {},
		"top.latency-10m":      {},
		"top.latency-1h":       {},
		"top.gauge":            {},
		"bottom.gauge#1":       {},
		"bottom.rates-count#1": {},
		"bottom.rates-1m#1":    {},
		"bottom.rates-10m#1":   {},
		"bottom.rates-1h#1":    {},
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
}
