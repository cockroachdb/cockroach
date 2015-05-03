// Copyright 2014 The Cockroach Authors.
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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

const (
	maxClockOffset = 250 * time.Millisecond
)

func TestTimestampCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// First simulate a read of just "a" at time 0.
	tc.Add(proto.Key("a"), nil, clock.Now(), nil, true)
	// Although we added "a" at time 0, the internal cache should still
	// be empty because the t=0 < lowWater.
	if tc.cache.Len() > 0 {
		t.Errorf("expected cache to be empty, but contains %d elements", tc.cache.Len())
	}
	// Verify GetMax returns the lowWater mark which is maxClockOffset.
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"a\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("notincache"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"notincache\"")
	}

	// Advance the clock and verify same low water mark.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"a\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("notincache"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"notincache\"")
	}

	// Sim a read of "b"-"c" at time maxClockOffset + 1.
	ts := clock.Now()
	tc.Add(proto.Key("b"), proto.Key("c"), ts, nil, true)

	// Verify all permutations of direct and range access.
	if rTS, _ := tc.GetMax(proto.Key("b"), nil, nil); !rTS.Equal(ts) {
		t.Errorf("expected current time for key \"b\"; got %s", rTS)
	}
	if rTS, _ := tc.GetMax(proto.Key("bb"), nil, nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bb\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"c\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("b"), proto.Key("c"), nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"b\"-\"c\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("bb"), proto.Key("bz"), nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bb\"-\"bz\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("b"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"a\"-\"b\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("bb"), nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"a\"-\"bb\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("d"), nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"a\"-\"d\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("bz"), proto.Key("c"), nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bz\"-\"c\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("bz"), proto.Key("d"), nil); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bz\"-\"d\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), proto.Key("d"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"c\"-\"d\"")
	}
}

// TestTimestampCacheSetLowWater verifies that setting the low
// water mark moves max timestamps forward as appropriate.
func TestTimestampCacheSetLowWater(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 10.
	manual.Set(maxClockOffset.Nanoseconds() + 10)
	aTS := clock.Now()
	tc.Add(proto.Key("a"), nil, aTS, nil, true)

	// Increment time by 10ns and add another key.
	manual.Increment(10)
	bTS := clock.Now()
	tc.Add(proto.Key("b"), nil, bTS, nil, true)

	// Increment time by 10ns and add another key.
	manual.Increment(10)
	cTS := clock.Now()
	tc.Add(proto.Key("c"), nil, cTS, nil, true)

	// Set low water mark.
	tc.SetLowWater(bTS)

	// Verify looking up key "a" returns the new low water mark ("a"'s timestamp).
	for i, test := range []struct {
		key   proto.Key
		expTS proto.Timestamp
	}{
		{proto.Key("a"), bTS},
		{proto.Key("b"), bTS},
		{proto.Key("c"), cTS},
		{proto.Key("d"), bTS},
	} {
		if rTS, _ := tc.GetMax(test.key, nil, nil); !rTS.Equal(test.expTS) {
			t.Errorf("%d: expected ts %s, got %s", i, test.expTS, rTS)
		}
	}

	// Try setting a lower low water mark than the previous value.
	tc.SetLowWater(aTS)
	if rTS, _ := tc.GetMax(proto.Key("d"), nil, nil); !rTS.Equal(bTS) {
		t.Errorf("setting lower low water mark should not be allowed; expected %s; got %s", bTS, rTS)
	}
}

// TestTimestampCacheEviction verifies the eviction of
// timestamp cache entries after MinTSCacheWindow interval.
func TestTimestampCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	aTS := clock.Now()
	tc.Add(proto.Key("a"), nil, aTS, nil, true)

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.Add(proto.Key("b"), nil, clock.Now(), nil, true)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, _ := tc.GetMax(proto.Key("c"), nil, nil); !rTS.Equal(aTS) {
		t.Errorf("expected low water mark %s, got %s", aTS, rTS)
	}
}

func TestTimestampCacheMergeInto(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)

	testCases := []struct {
		useClear bool
		expLen   int
	}{
		{true, 3},
		{false, 5},
	}
	for _, test := range testCases {
		tc1 := NewTimestampCache(clock)
		tc2 := NewTimestampCache(clock)

		bfTS := clock.Now()
		tc2.Add(proto.Key("b"), proto.Key("f"), bfTS, nil, true)

		adTS := clock.Now()
		tc1.Add(proto.Key("a"), proto.Key("d"), adTS, nil, true)

		beTS := clock.Now()
		tc1.Add(proto.Key("b"), proto.Key("e"), beTS, nil, true)

		aaTS := clock.Now()
		tc2.Add(proto.Key("aa"), nil, aaTS, nil, true)

		cTS := clock.Now()
		tc1.Add(proto.Key("c"), nil, cTS, nil, true)

		tc1.MergeInto(tc2, test.useClear)

		if tc2.cache.Len() != test.expLen {
			t.Errorf("expected merged length of %d; got %d", test.expLen, tc2.cache.Len())
		}
		if !tc2.latest.Equal(tc1.latest) {
			t.Errorf("expected latest to be updated to %s; got %s", tc1.latest, tc2.latest)
		}

		if rTS, _ := tc2.GetMax(proto.Key("a"), nil, nil); !rTS.Equal(adTS) {
			t.Error("expected \"a\" to have adTS timestamp")
		}
		if rTS, _ := tc2.GetMax(proto.Key("b"), nil, nil); !rTS.Equal(beTS) {
			t.Error("expected \"b\" to have beTS timestamp")
		}
		if test.useClear {
			if rTS, _ := tc2.GetMax(proto.Key("aa"), nil, nil); !rTS.Equal(adTS) {
				t.Error("expected \"aa\" to have adTS timestamp")
			}
		} else {
			if rTS, _ := tc2.GetMax(proto.Key("aa"), nil, nil); !rTS.Equal(aaTS) {
				t.Error("expected \"aa\" to have aaTS timestamp")
			}
			if rTS, _ := tc2.GetMax(proto.Key("a"), proto.Key("c"), nil); !rTS.Equal(aaTS) {
				t.Error("expected \"a\"-\"c\" to have aaTS timestamp")
			}
		}
	}
}

// TestTimestampCacheLayeredIntervals verifies the maximum timestamp
// is chosen if previous entries have ranges which are layered over
// each other.
func TestTimestampCacheLayeredIntervals(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)
	manual.Set(maxClockOffset.Nanoseconds() + 1)

	adTS := clock.Now()
	tc.Add(proto.Key("a"), proto.Key("d"), adTS, nil, true)

	beTS := clock.Now()
	tc.Add(proto.Key("b"), proto.Key("e"), beTS, nil, true)

	cTS := clock.Now()
	tc.Add(proto.Key("c"), nil, cTS, nil, true)

	// Try different sub ranges.
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, nil); !rTS.Equal(adTS) {
		t.Error("expected \"a\" to have adTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("b"), nil, nil); !rTS.Equal(beTS) {
		t.Error("expected \"b\" to have beTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), nil, nil); !rTS.Equal(cTS) {
		t.Error("expected \"b\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("d"), nil, nil); !rTS.Equal(beTS) {
		t.Error("expected \"d\" to have beTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("b"), nil); !rTS.Equal(adTS) {
		t.Error("expected \"a\"-\"b\" to have adTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("c"), nil); !rTS.Equal(beTS) {
		t.Error("expected \"a\"-\"c\" to have beTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("d"), nil); !rTS.Equal(cTS) {
		t.Error("expected \"a\"-\"d\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("b"), proto.Key("d"), nil); !rTS.Equal(cTS) {
		t.Error("expected \"b\"-\"d\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), proto.Key("d"), nil); !rTS.Equal(cTS) {
		t.Error("expected \"c\"-\"d\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("c0"), proto.Key("d"), nil); !rTS.Equal(beTS) {
		t.Error("expected \"c0\"-\"d\" to have beTS timestamp")
	}
}

func TestTimestampCacheClear(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	ts := clock.Now()
	tc.Add(proto.Key("a"), nil, ts, nil, true)

	// Clear the cache, which will reset the low water mark to
	// the current time + maxClockOffset.
	tc.Clear(clock)

	// Fetching any keys should give current time + maxClockOffset
	expTS := clock.Timestamp()
	expTS.WallTime += maxClockOffset.Nanoseconds()
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, nil); !rTS.Equal(expTS) {
		t.Error("expected \"a\" to have cleared timestamp")
	}
}

// TestTimestampCacheReplacements verifies that a newer entry
// in the timestamp cache which completely "covers" an older
// entry will replace it.
func TestTimestampCacheReplacements(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	txn1ID := util.NewUUID4()
	txn2ID := util.NewUUID4()

	ts1 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts1, nil, true)
	if ts, _ := tc.GetMax(proto.Key("a"), nil, nil); !ts.Equal(ts1) {
		t.Errorf("expected %s; got %s", ts1, ts)
	}
	// Write overlapping value with txn1 and verify with txn1--we should get
	// low water mark, not ts1.
	ts2 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts2, txn1ID, true)
	if ts, _ := tc.GetMax(proto.Key("a"), nil, txn1ID); !ts.Equal(tc.lowWater) {
		t.Errorf("expected low water (empty) time; got %s", ts)
	}
	// Write range which overlaps "a" with txn2 and verify with txn2--we should
	// get low water mark, not ts2.
	ts3 := clock.Now()
	tc.Add(proto.Key("a"), proto.Key("c"), ts3, txn2ID, true)
	if ts, _ := tc.GetMax(proto.Key("a"), nil, txn2ID); !ts.Equal(tc.lowWater) {
		t.Errorf("expected low water (empty) time; got %s", ts)
	}
	// Also, verify txn1 sees ts3.
	if ts, _ := tc.GetMax(proto.Key("a"), nil, txn1ID); !ts.Equal(ts3) {
		t.Errorf("expected %s; got %s", ts3, ts)
	}
	// Now, write to "b" with a higher timestamp and no txn. Should be
	// visible to all txns.
	ts4 := clock.Now()
	tc.Add(proto.Key("b"), nil, ts4, nil, true)
	if ts, _ := tc.GetMax(proto.Key("b"), nil, nil); !ts.Equal(ts4) {
		t.Errorf("expected %s; got %s", ts4, ts)
	}
	if ts, _ := tc.GetMax(proto.Key("b"), nil, txn1ID); !ts.Equal(ts4) {
		t.Errorf("expected %s; got %s", ts4, ts)
	}
	// Finally, write an earlier version of "a"; should simply get
	// tossed and we should see ts4 still.
	tc.Add(proto.Key("b"), nil, ts1, nil, true)
	if ts, _ := tc.GetMax(proto.Key("b"), nil, nil); !ts.Equal(ts4) {
		t.Errorf("expected %s; got %s", ts4, ts)
	}
}

// TestTimestampCacheWithTxnID verifies that timestamps matching
// the specified txn ID are ignored.
func TestTimestampCacheWithTxnID(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	// Add two successive txn entries.
	txn1ID := util.NewUUID4()
	txn2ID := util.NewUUID4()
	ts1 := clock.Now()
	tc.Add(proto.Key("a"), proto.Key("c"), ts1, txn1ID, true)
	ts2 := clock.Now()
	// This entry will remove "a"-"b" from the cache.
	tc.Add(proto.Key("b"), proto.Key("d"), ts2, txn2ID, true)

	// Fetching with no transaction gets latest value.
	if ts, _ := tc.GetMax(proto.Key("b"), nil, nil); !ts.Equal(ts2) {
		t.Errorf("expected %s; got %s", ts2, ts)
	}
	// Fetching with txn ID "1" gets most recent.
	if ts, _ := tc.GetMax(proto.Key("b"), nil, txn1ID); !ts.Equal(ts2) {
		t.Errorf("expected %s; got %s", ts2, ts)
	}
	// Fetching with txn ID "2" skips most recent.
	if ts, _ := tc.GetMax(proto.Key("b"), nil, txn2ID); !ts.Equal(ts1) {
		t.Errorf("expected %s; got %s", ts1, ts)
	}
}

// TestTimestampCacheReadVsWrite verifies that the timestamp cache
// can differentiate between read and write timestamp.
func TestTimestampCacheReadVsWrite(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	// Add read-only non-txn entry at current time.
	ts1 := clock.Now()
	tc.Add(proto.Key("a"), proto.Key("b"), ts1, nil, true)

	// Add two successive txn entries; one read-only and one read-write.
	txn1ID := util.NewUUID4()
	txn2ID := util.NewUUID4()
	ts2 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts2, txn1ID, true)
	ts3 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts3, txn2ID, false)

	// Fetching with no transaction gets latest values.
	if rTS, wTS := tc.GetMax(proto.Key("a"), nil, nil); !rTS.Equal(ts2) || !wTS.Equal(ts3) {
		t.Errorf("expected %s %s; got %s %s", ts2, ts3, rTS, wTS)
	}
	// Fetching with txn ID "1" gets original for read and most recent for write.
	if rTS, wTS := tc.GetMax(proto.Key("a"), nil, txn1ID); !rTS.Equal(ts1) || !wTS.Equal(ts3) {
		t.Errorf("expected %s %s; got %s %s", ts1, ts3, rTS, wTS)
	}
	// Fetching with txn ID "2" gets ts2 for read and low water mark for write.
	if rTS, wTS := tc.GetMax(proto.Key("a"), nil, txn2ID); !rTS.Equal(ts2) || !wTS.Equal(tc.lowWater) {
		t.Errorf("expected %s %s; got %s %s", ts2, tc.lowWater, rTS, wTS)
	}
}
