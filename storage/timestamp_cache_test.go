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
	"crypto/md5"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
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
	tc.Add(proto.Key("a"), nil, clock.Now(), proto.NoTxnMD5, true)
	// Although we added "a" at time 0, the internal cache should still
	// be empty because the t=0 < lowWater.
	if tc.cache.Len() > 0 {
		t.Errorf("expected cache to be empty, but contains %d elements", tc.cache.Len())
	}
	// Verify GetMax returns the lowWater mark which is maxClockOffset.
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"a\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("notincache"), nil, proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"notincache\"")
	}

	// Advance the clock and verify same low water mark.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"a\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("notincache"), nil, proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"notincache\"")
	}

	// Sim a read of "b"-"c" at time maxClockOffset + 1.
	ts := clock.Now()
	tc.Add(proto.Key("b"), proto.Key("c"), ts, proto.NoTxnMD5, true)

	// Verify all permutations of direct and range access.
	if rTS, _ := tc.GetMax(proto.Key("b"), nil, proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Errorf("expected current time for key \"b\"; got %+v", rTS)
	}
	if rTS, _ := tc.GetMax(proto.Key("bb"), nil, proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bb\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), nil, proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"c\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("b"), proto.Key("c"), proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"b\"-\"c\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("bb"), proto.Key("bz"), proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bb\"-\"bz\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("b"), proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"a\"-\"b\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("bb"), proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"a\"-\"bb\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("d"), proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"a\"-\"d\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("bz"), proto.Key("c"), proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bz\"-\"c\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("bz"), proto.Key("d"), proto.NoTxnMD5); !rTS.Equal(ts) {
		t.Error("expected current time for key \"bz\"-\"d\"")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), proto.Key("d"), proto.NoTxnMD5); rTS.WallTime != maxClockOffset.Nanoseconds() {
		t.Error("expected maxClockOffset for key \"c\"-\"d\"")
	}
}

// TestTimestampCacheEviction verifies the eviction of
// timestamp cache entries after minCacheWindow interval.
func TestTimestampCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	aTS := clock.Now()
	tc.Add(proto.Key("a"), nil, aTS, proto.NoTxnMD5, true)

	// Increment time by the minCacheWindow and add another key.
	manual.Increment(minCacheWindow.Nanoseconds())
	tc.Add(proto.Key("b"), nil, clock.Now(), proto.NoTxnMD5, true)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, _ := tc.GetMax(proto.Key("c"), nil, proto.NoTxnMD5); !rTS.Equal(aTS) {
		t.Errorf("expected low water mark %+v, got %+v", aTS, rTS)
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
		tc2.Add(proto.Key("b"), proto.Key("f"), bfTS, proto.NoTxnMD5, true)

		adTS := clock.Now()
		tc1.Add(proto.Key("a"), proto.Key("d"), adTS, proto.NoTxnMD5, true)

		beTS := clock.Now()
		tc1.Add(proto.Key("b"), proto.Key("e"), beTS, proto.NoTxnMD5, true)

		aaTS := clock.Now()
		tc2.Add(proto.Key("aa"), nil, aaTS, proto.NoTxnMD5, true)

		cTS := clock.Now()
		tc1.Add(proto.Key("c"), nil, cTS, proto.NoTxnMD5, true)

		tc1.MergeInto(tc2, test.useClear)

		if tc2.cache.Len() != test.expLen {
			t.Errorf("expected merged length of %d; got %d", test.expLen, tc2.cache.Len())
		}
		if !tc2.latest.Equal(tc1.latest) {
			t.Errorf("expected latest to be updated to %s; got %s", tc1.latest, tc2.latest)
		}

		if rTS, _ := tc2.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); !rTS.Equal(adTS) {
			t.Error("expected \"a\" to have adTS timestamp")
		}
		if rTS, _ := tc2.GetMax(proto.Key("b"), nil, proto.NoTxnMD5); !rTS.Equal(beTS) {
			t.Error("expected \"b\" to have beTS timestamp")
		}
		if test.useClear {
			if rTS, _ := tc2.GetMax(proto.Key("aa"), nil, proto.NoTxnMD5); !rTS.Equal(adTS) {
				t.Error("expected \"aa\" to have adTS timestamp")
			}
		} else {
			if rTS, _ := tc2.GetMax(proto.Key("aa"), nil, proto.NoTxnMD5); !rTS.Equal(aaTS) {
				t.Error("expected \"aa\" to have aaTS timestamp")
			}
			if rTS, _ := tc2.GetMax(proto.Key("a"), proto.Key("c"), proto.NoTxnMD5); !rTS.Equal(aaTS) {
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
	tc.Add(proto.Key("a"), proto.Key("d"), adTS, proto.NoTxnMD5, true)

	beTS := clock.Now()
	tc.Add(proto.Key("b"), proto.Key("e"), beTS, proto.NoTxnMD5, true)

	cTS := clock.Now()
	tc.Add(proto.Key("c"), nil, cTS, proto.NoTxnMD5, true)

	// Try different sub ranges.
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); !rTS.Equal(adTS) {
		t.Error("expected \"a\" to have adTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("b"), nil, proto.NoTxnMD5); !rTS.Equal(beTS) {
		t.Error("expected \"b\" to have beTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), nil, proto.NoTxnMD5); !rTS.Equal(cTS) {
		t.Error("expected \"b\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("d"), nil, proto.NoTxnMD5); !rTS.Equal(beTS) {
		t.Error("expected \"d\" to have beTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("b"), proto.NoTxnMD5); !rTS.Equal(adTS) {
		t.Error("expected \"a\"-\"b\" to have adTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("c"), proto.NoTxnMD5); !rTS.Equal(beTS) {
		t.Error("expected \"a\"-\"c\" to have beTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("a"), proto.Key("d"), proto.NoTxnMD5); !rTS.Equal(cTS) {
		t.Error("expected \"a\"-\"d\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("b"), proto.Key("d"), proto.NoTxnMD5); !rTS.Equal(cTS) {
		t.Error("expected \"b\"-\"d\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("c"), proto.Key("d"), proto.NoTxnMD5); !rTS.Equal(cTS) {
		t.Error("expected \"c\"-\"d\" to have cTS timestamp")
	}
	if rTS, _ := tc.GetMax(proto.Key("c0"), proto.Key("d"), proto.NoTxnMD5); !rTS.Equal(beTS) {
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
	tc.Add(proto.Key("a"), nil, ts, proto.NoTxnMD5, true)

	// Clear the cache, which will reset the low water mark to
	// the current time + maxClockOffset.
	tc.Clear(clock)

	// Fetching any keys should give current time + maxClockOffset
	expTS := clock.Timestamp()
	expTS.WallTime += maxClockOffset.Nanoseconds()
	if rTS, _ := tc.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); !rTS.Equal(expTS) {
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

	txn1MD5 := md5.Sum([]byte("txn1"))
	txn2MD5 := md5.Sum([]byte("txn2"))

	ts1 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts1, proto.NoTxnMD5, true)
	if ts, _ := tc.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); !ts.Equal(ts1) {
		t.Errorf("expected %s; got %s", ts1, ts)
	}
	// Write overlapping value with txn1 and verify with txn1--we should get
	// low water mark, not ts1.
	ts2 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts2, txn1MD5, true)
	if ts, _ := tc.GetMax(proto.Key("a"), nil, txn1MD5); !ts.Equal(tc.lowWater) {
		t.Errorf("expected low water (empty) time; got %s", ts)
	}
	// Write range which overlaps "a" with txn2 and verify with txn2--we should
	// get low water mark, not ts2.
	ts3 := clock.Now()
	tc.Add(proto.Key("a"), proto.Key("c"), ts3, txn2MD5, true)
	if ts, _ := tc.GetMax(proto.Key("a"), nil, txn2MD5); !ts.Equal(tc.lowWater) {
		t.Errorf("expected low water (empty) time; got %s", ts)
	}
	// Also, verify txn1 sees ts3.
	if ts, _ := tc.GetMax(proto.Key("a"), nil, txn1MD5); !ts.Equal(ts3) {
		t.Errorf("expected %s; got %s", ts3, ts)
	}
	// Now, write to "b" with a higher timestamp and no txn. Should be
	// visible to all txns.
	ts4 := clock.Now()
	tc.Add(proto.Key("b"), nil, ts4, proto.NoTxnMD5, true)
	if ts, _ := tc.GetMax(proto.Key("b"), nil, proto.NoTxnMD5); !ts.Equal(ts4) {
		t.Errorf("expected %s; got %s", ts4, ts)
	}
	if ts, _ := tc.GetMax(proto.Key("b"), nil, txn1MD5); !ts.Equal(ts4) {
		t.Errorf("expected %s; got %s", ts4, ts)
	}
	// Finally, write an earlier version of "a"; should simply get
	// tossed and we should see ts4 still.
	tc.Add(proto.Key("b"), nil, ts1, proto.NoTxnMD5, true)
	if ts, _ := tc.GetMax(proto.Key("b"), nil, proto.NoTxnMD5); !ts.Equal(ts4) {
		t.Errorf("expected %s; got %s", ts4, ts)
	}
}

// TestTimestampCacheWithTxnMD5 verifies that timestamps matching
// a specified MD5 of the txn ID are ignored.
func TestTimestampCacheWithTxnMD5(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	// Add two successive txn entries.
	txn1MD5 := md5.Sum([]byte("txn1"))
	txn2MD5 := md5.Sum([]byte("txn2"))
	ts1 := clock.Now()
	tc.Add(proto.Key("a"), proto.Key("c"), ts1, txn1MD5, true)
	ts2 := clock.Now()
	// This entry will remove "a"-"b" from the cache.
	tc.Add(proto.Key("b"), proto.Key("d"), ts2, txn2MD5, true)

	// Fetching with no transaction gets latest value.
	if ts, _ := tc.GetMax(proto.Key("b"), nil, proto.NoTxnMD5); !ts.Equal(ts2) {
		t.Errorf("expected %s; got %s", ts2, ts)
	}
	// Fetching with txn MD5 "1" gets most recent.
	if ts, _ := tc.GetMax(proto.Key("b"), nil, txn1MD5); !ts.Equal(ts2) {
		t.Errorf("expected %s; got %s", ts2, ts)
	}
	// Fetching with txn MD5 "2" skips most recent.
	if ts, _ := tc.GetMax(proto.Key("b"), nil, txn2MD5); !ts.Equal(ts1) {
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
	tc.Add(proto.Key("a"), proto.Key("b"), ts1, proto.NoTxnMD5, true)

	// Add two successive txn entries; one read-only and one read-write.
	txn1MD5 := md5.Sum([]byte("txn1"))
	txn2MD5 := md5.Sum([]byte("txn2"))
	ts2 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts2, txn1MD5, true)
	ts3 := clock.Now()
	tc.Add(proto.Key("a"), nil, ts3, txn2MD5, false)

	// Fetching with no transaction gets latest values.
	if rTS, wTS := tc.GetMax(proto.Key("a"), nil, proto.NoTxnMD5); !rTS.Equal(ts2) || !wTS.Equal(ts3) {
		t.Errorf("expected %s %s; got %s %s", ts2, ts3, rTS, wTS)
	}
	// Fetching with txn MD5 "1" gets original for read and most recent for write.
	if rTS, wTS := tc.GetMax(proto.Key("a"), nil, txn1MD5); !rTS.Equal(ts1) || !wTS.Equal(ts3) {
		t.Errorf("expected %s %s; got %s %s", ts1, ts3, rTS, wTS)
	}
	// Fetching with txn MD5 "2" gets ts2 for read and low water mark for write.
	if rTS, wTS := tc.GetMax(proto.Key("a"), nil, txn2MD5); !rTS.Equal(ts2) || !wTS.Equal(tc.lowWater) {
		t.Errorf("expected %s %s; got %s %s", ts2, tc.lowWater, rTS, wTS)
	}
}
