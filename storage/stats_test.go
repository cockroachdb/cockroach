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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/storage/engine"
)

func TestRangeStatsEmpty(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	s := tc.rng.stats
	if s.elapsedNanos != 0 || s.intentCount != 0 {
		t.Errorf("expected elapsed nanos and intent count to initialize to 0: %+v", s)
	}
}

func TestRangeStatsInit(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	ms := &engine.MVCCStats{
		ElapsedNanos: 10,
		IntentCount:  5,
	}
	ms.SetStats(tc.engine, 1)

	s, err := newRangeStats(1, tc.engine)
	if err != nil {
		t.Fatal(err)
	}
	if s.elapsedNanos != 10 || s.intentCount != 5 {
		t.Errorf("expected elapsed nanos=10 and intent count=5: %+v", s)
	}
}

func TestRangeStatsMerge(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	ms := &engine.MVCCStats{
		LiveBytes:    1,
		KeyBytes:     1,
		ValBytes:     1,
		IntentBytes:  1,
		LiveCount:    1,
		KeyCount:     1,
		ValCount:     1,
		IntentCount:  1,
		IntentAge:    1,
		ElapsedNanos: 1,
	}
	tc.rng.stats.MergeMVCCStats(tc.engine, ms, 10)
	tc.rng.stats.Update(ms)
	expMS := &engine.MVCCStats{
		LiveBytes:    1,
		KeyBytes:     1,
		ValBytes:     1,
		IntentBytes:  1,
		LiveCount:    1,
		KeyCount:     1,
		ValCount:     1,
		IntentCount:  1,
		IntentAge:    1,
		ElapsedNanos: 10,
	}
	ms, err := engine.MVCCGetRangeStats(tc.engine, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, expMS) {
		t.Errorf("expected %+v; got %+v", expMS, ms)
	}

	// Merge again, but with 10 more ns.
	tc.rng.stats.MergeMVCCStats(tc.engine, ms, 20)
	tc.rng.stats.Update(ms)
	expMS = &engine.MVCCStats{
		LiveBytes:    2,
		KeyBytes:     2,
		ValBytes:     2,
		IntentBytes:  2,
		LiveCount:    2,
		KeyCount:     2,
		ValCount:     2,
		IntentCount:  2,
		IntentAge:    12,
		ElapsedNanos: 20,
	}
	ms, err = engine.MVCCGetRangeStats(tc.engine, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, expMS) {
		t.Errorf("expected %+v; got %+v", expMS, ms)
	}

	if tc.rng.stats.elapsedNanos != 20 || tc.rng.stats.intentCount != 2 {
		t.Errorf("expected elapsedNanos==20, intentCount==2; got %+v", tc.rng.stats)
	}
}

func TestRangeStatsClear(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	expMS := &engine.MVCCStats{
		LiveBytes:    1,
		KeyBytes:     1,
		ValBytes:     1,
		IntentBytes:  1,
		LiveCount:    1,
		KeyCount:     1,
		ValCount:     1,
		IntentCount:  1,
		IntentAge:    1,
		ElapsedNanos: 1,
	}
	tc.rng.stats.SetMVCCStats(tc.engine, expMS)
	ms, err := engine.MVCCGetRangeStats(tc.engine, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, expMS) {
		t.Errorf("expected %+v; got %+v", expMS, ms)
	}

	if err := tc.rng.stats.Clear(tc.engine); err != nil {
		t.Fatal(err)
	}
	ms, err = engine.MVCCGetRangeStats(tc.engine, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, &engine.MVCCStats{}) {
		t.Errorf("expected %+v; got %+v", &engine.MVCCStats{}, ms)
	}
}
