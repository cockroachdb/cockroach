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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestRangeStatsEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	s := tc.rng.stats
	if !reflect.DeepEqual(s.MVCCStats, engine.MVCCStats{}) {
		t.Errorf("expected empty stats; got %+v", s.MVCCStats)
	}
}

func TestRangeStatsInit(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	ms := engine.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        2,
		ValBytes:        3,
		IntentBytes:     4,
		LiveCount:       5,
		KeyCount:        6,
		ValCount:        7,
		IntentCount:     8,
		IntentAge:       9,
		GCBytesAge:      10,
		LastUpdateNanos: 11,
	}
	if err := engine.MVCCSetRangeStats(tc.engine, 1, &ms); err != nil {
		t.Fatal(err)
	}
	s, err := newRangeStats(1, tc.engine)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, s.MVCCStats) {
		t.Errorf("mvcc stats mismatch %+v != %+v", ms, s.MVCCStats)
	}
}

func TestRangeStatsMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()
	ms := engine.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        2,
		ValBytes:        2,
		IntentBytes:     1,
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		IntentCount:     1,
		IntentAge:       1,
		GCBytesAge:      1,
		LastUpdateNanos: 1 * 1E9,
	}
	// Merge 9 seconds later.
	if err := tc.rng.stats.MergeMVCCStats(tc.engine, &ms, 10*1E9); err != nil {
		t.Fatal(err)
	}
	expMS := engine.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        2,
		ValBytes:        2,
		IntentBytes:     1,
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		IntentCount:     1,
		IntentAge:       1,
		GCBytesAge:      1,
		LastUpdateNanos: 10 * 1E9,
	}
	if err := engine.MVCCGetRangeStats(tc.engine, 1, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, expMS) {
		t.Errorf("expected %+v; got %+v", expMS, ms)
	}

	// Merge again, ten seconds after the last one.
	if err := tc.rng.stats.MergeMVCCStats(tc.engine, &ms, 20*1E9); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{
		LiveBytes:       2,
		KeyBytes:        4,
		ValBytes:        4,
		IntentBytes:     2,
		LiveCount:       2,
		KeyCount:        2,
		ValCount:        2,
		IntentCount:     2,
		IntentAge:       12,
		GCBytesAge:      32,
		LastUpdateNanos: 20 * 1E9,
	}
	if err := engine.MVCCGetRangeStats(tc.engine, 1, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, expMS) {
		t.Errorf("expected %+v; got %+v", expMS, ms)
	}
	if !reflect.DeepEqual(tc.rng.stats.MVCCStats, expMS) {
		t.Errorf("expected %+v; got %+v", expMS, tc.rng.stats.MVCCStats)
	}

	// Finally, test the issue described in #3234: Updates happening with non-
	// or slowly-increasing timestamp. In that case, we do the usual counting
	// but don't update age and last updated timestamp. Not updating the latter
	// makes sense in those cases since age increases only each second, and
	// recomputing in a close loop could leave the age constant otherwise.
	for i, newNow := range []int64{10 * 1E9, 20 * 1E9, 20*1E9 + (1E9 - 1)} {
		ms = engine.MVCCStats{
			KeyBytes:    1000,
			IntentCount: 1000,
		}
		expMS.KeyBytes += 1000
		expMS.IntentCount += 1000
		origMS := ms
		if err := tc.rng.stats.MergeMVCCStats(tc.engine, &ms, newNow); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ms, origMS) {
			t.Errorf("%d: expected no age update after <1s, but %+v changed to %+v", i, origMS, ms)
		}
		if !reflect.DeepEqual(tc.rng.stats.MVCCStats, expMS) {
			t.Errorf("%d: expected %+v, got %+v", i, expMS, tc.rng.stats.MVCCStats)
		}
	}
}
