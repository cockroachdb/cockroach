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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	maxClockOffset = 250 * time.Millisecond
)

func TestTimestampCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// First simulate a read of just "a" at time 0.
	tc.Add(roachpb.Key("a"), nil, clock.Now(), nil, true)
	// Although we added "a" at time 0, the internal cache should still
	// be empty because the t=0 < lowWater.
	if tc.rCache.Len() > 0 {
		t.Errorf("expected cache to be empty, but contains %d elements", tc.rCache.Len())
	}
	// Verify GetMax returns the lowWater mark which is maxClockOffset.
	if rTS, ok := tc.GetMaxRead(roachpb.Key("a"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"a\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("notincache"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"notincache\"; ok=%t", ok)
	}

	// Advance the clock and verify same low water mark.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	if rTS, ok := tc.GetMaxRead(roachpb.Key("a"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"a\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("notincache"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"notincache\"; ok=%t", ok)
	}

	// Sim a read of "b"-"c" at time maxClockOffset + 1.
	ts := clock.Now()
	tc.Add(roachpb.Key("b"), roachpb.Key("c"), ts, nil, true)

	// Verify all permutations of direct and range access.
	if rTS, ok := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"b\"; got %s; ok=%t", rTS, ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("bb"), nil, nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bb\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("c"), nil, nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"c\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("c"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"b\"-\"c\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("bb"), roachpb.Key("bz"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bb\"-\"bz\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("b"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"a\"-\"b\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("bb"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"a\"-\"bb\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("d"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"a\"-\"d\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("bz"), roachpb.Key("c"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bz\"-\"c\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("bz"), roachpb.Key("d"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bz\"-\"d\"; ok=%t", ok)
	}
	if rTS, ok := tc.GetMaxRead(roachpb.Key("c"), roachpb.Key("d"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"c\"-\"d\"; ok=%t", ok)
	}
}

// TestTimestampCacheSetLowWater verifies that setting the low
// water mark moves max timestamps forward as appropriate.
func TestTimestampCacheSetLowWater(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 10.
	manual.Set(maxClockOffset.Nanoseconds() + 10)
	aTS := clock.Now()
	tc.Add(roachpb.Key("a"), nil, aTS, nil, true)

	// Increment time by 10ns and add another key.
	manual.Increment(10)
	bTS := clock.Now()
	tc.Add(roachpb.Key("b"), nil, bTS, nil, true)

	// Increment time by 10ns and add another key.
	manual.Increment(10)
	cTS := clock.Now()
	tc.Add(roachpb.Key("c"), nil, cTS, nil, true)

	// Set low water mark.
	tc.SetLowWater(bTS)

	// Verify looking up key "a" returns the new low water mark ("a"'s timestamp).
	for i, test := range []struct {
		key   roachpb.Key
		expTS roachpb.Timestamp
		expOK bool
	}{
		{roachpb.Key("a"), bTS, false},
		{roachpb.Key("b"), bTS, false},
		{roachpb.Key("c"), cTS, true},
		{roachpb.Key("d"), bTS, false},
	} {
		if rTS, ok := tc.GetMaxRead(test.key, nil, nil); !rTS.Equal(test.expTS) || ok != test.expOK {
			t.Errorf("%d: expected ts %s, got %s; exp ok=%t; got %t", i, test.expTS, rTS, test.expOK, ok)
		}
	}

	// Try setting a lower low water mark than the previous value.
	tc.SetLowWater(aTS)
	if rTS, ok := tc.GetMaxRead(roachpb.Key("d"), nil, nil); !rTS.Equal(bTS) || ok {
		t.Errorf("setting lower low water mark should not be allowed; expected %s; got %s; ok=%t", bTS, rTS, ok)
	}
}

// TestTimestampCacheEviction verifies the eviction of
// timestamp cache entries after MinTSCacheWindow interval.
func TestTimestampCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)
	tc.evictionSizeThreshold = 0

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	aTS := clock.Now()
	tc.Add(roachpb.Key("a"), nil, aTS, nil, true)

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.Add(roachpb.Key("b"), nil, clock.Now(), nil, true)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, ok := tc.GetMaxRead(roachpb.Key("c"), nil, nil); !rTS.Equal(aTS) || ok {
		t.Errorf("expected low water mark %s, got %s; ok=%t", aTS, rTS, ok)
	}
}

// TestTimestampCacheNoEviction verifies that even after
// the MinTSCacheWindow interval, if the cache has not hit
// its size threshold, it will not evict entries.
func TestTimestampCacheNoEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	aTS := clock.Now()
	tc.Add(roachpb.Key("a"), nil, aTS, nil, true)

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.Add(roachpb.Key("b"), nil, clock.Now(), nil, true)

	// Verify that the cache still has 2 entries in it
	if l, want := tc.Len(), 2; l != want {
		t.Errorf("expected %d entries to remain, got %d", want, l)
	}
}

func TestTimestampCacheMergeInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)

	testCases := []struct {
		useClear bool
		expLen   int
	}{
		{true, 4},
		{false, 7},
	}
	for _, test := range testCases {
		tc1 := NewTimestampCache(clock)
		tc2 := NewTimestampCache(clock)

		bfTS := clock.Now()
		tc2.Add(roachpb.Key("b"), roachpb.Key("f"), bfTS, nil, true)

		adTS := clock.Now()
		tc1.Add(roachpb.Key("a"), roachpb.Key("d"), adTS, nil, true)

		beTS := clock.Now()
		tc1.Add(roachpb.Key("b"), roachpb.Key("e"), beTS, nil, true)

		aaTS := clock.Now()
		tc2.Add(roachpb.Key("aa"), nil, aaTS, nil, true)

		cTS := clock.Now()
		tc1.Add(roachpb.Key("c"), nil, cTS, nil, true)

		tc1.MergeInto(tc2, test.useClear)

		if tc2.rCache.Len() != test.expLen {
			t.Errorf("expected merged length of %d; got %d", test.expLen, tc2.rCache.Len())
		}
		if !tc2.latest.Equal(tc1.latest) {
			t.Errorf("expected latest to be updated to %s; got %s", tc1.latest, tc2.latest)
		}

		if rTS, ok := tc2.GetMaxRead(roachpb.Key("a"), nil, nil); !rTS.Equal(adTS) || !ok {
			t.Errorf("expected \"a\" to have adTS timestamp; ok=%t", ok)
		}
		if rTS, ok := tc2.GetMaxRead(roachpb.Key("b"), nil, nil); !rTS.Equal(beTS) || !ok {
			t.Errorf("expected \"b\" to have beTS timestamp; ok=%t", ok)
		}
		if test.useClear {
			if rTS, ok := tc2.GetMaxRead(roachpb.Key("aa"), nil, nil); !rTS.Equal(adTS) || !ok {
				t.Errorf("expected \"aa\" to have adTS timestamp; ok=%t", ok)
			}
		} else {
			if rTS, ok := tc2.GetMaxRead(roachpb.Key("aa"), nil, nil); !rTS.Equal(aaTS) || !ok {
				t.Errorf("expected \"aa\" to have aaTS timestamp; ok=%t", ok)
			}
			if rTS, ok := tc2.GetMaxRead(roachpb.Key("a"), roachpb.Key("c"), nil); !rTS.Equal(aaTS) || !ok {
				t.Errorf("expected \"a\"-\"c\" to have aaTS timestamp; ok=%t", ok)
			}

			if !tc2.latest.Equal(cTS) {
				t.Error("expected \"aa\" to have cTS timestamp")
			}
			if !tc1.latest.Equal(cTS) {
				t.Error("expected \"a\"-\"c\" to have cTS timestamp")
			}
		}
	}
}

type layeredIntervalTestCase struct {
	actions   []func(tc *TimestampCache, ts roachpb.Timestamp)
	validator func(t *testing.T, tc *TimestampCache, tss []roachpb.Timestamp)
}

// layeredIntervalTestCase1 tests the left partial overlap and old containing
// new cases for adding intervals to the interval cache when tested in order,
// and tests the cases' inverses when tested in reverse.
var layeredIntervalTestCase1 = layeredIntervalTestCase{
	actions: []func(tc *TimestampCache, ts roachpb.Timestamp){
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// No overlap forwards.
			// Right partial overlap backwards.
			tc.Add(roachpb.Key("a"), roachpb.Key("bb"), ts, nil, true)
		},
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// Left partial overlap forwards.
			// New contains old backwards.
			tc.Add(roachpb.Key("b"), roachpb.Key("e"), ts, nil, true)
		},
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// Old contains new forwards.
			// No overlap backwards.
			tc.Add(roachpb.Key("c"), nil, ts, nil, true)
		},
	},
	validator: func(t *testing.T, tc *TimestampCache, tss []roachpb.Timestamp) {
		abbTS := tss[0]
		beTS := tss[1]
		cTS := tss[2]

		// Try different sub ranges.
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), nil, nil); !rTS.Equal(abbTS) {
			t.Errorf("expected \"a\" to have abbTS %v timestamp, found %v", abbTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !rTS.Equal(beTS) {
			t.Errorf("expected \"b\" to have beTS %v timestamp, found %v", beTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c"), nil, nil); !rTS.Equal(cTS) {
			t.Errorf("expected \"c\" to have cTS %v timestamp, found %v", cTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("d"), nil, nil); !rTS.Equal(beTS) {
			t.Errorf("expected \"d\" to have beTS %v timestamp, found %v", beTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("b"), nil); !rTS.Equal(abbTS) {
			t.Errorf("expected \"a\"-\"b\" to have abbTS %v timestamp, found %v", cTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("c"), nil); !rTS.Equal(beTS) {
			t.Errorf("expected \"a\"-\"c\" to have beTS %v timestamp, found %v", beTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("d"), nil); !rTS.Equal(cTS) {
			t.Errorf("expected \"a\"-\"d\" to have cTS %v timestamp, found %v", cTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("d"), nil); !rTS.Equal(cTS) {
			t.Errorf("expected \"b\"-\"d\" to have cTS %v timestamp, found %v", cTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c"), roachpb.Key("d"), nil); !rTS.Equal(cTS) {
			t.Errorf("expected \"c\"-\"d\" to have cTS %v timestamp, found %v", cTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c0"), roachpb.Key("d"), nil); !rTS.Equal(beTS) {
			t.Errorf("expected \"c0\"-\"d\" to have beTS %v timestamp, found %v", beTS, rTS)
		}
	},
}

// layeredIntervalTestCase2 tests the right partial overlap and new containing
// old cases for adding intervals to the interval cache when tested in order,
// and tests the cases' inverses when tested in reverse.
var layeredIntervalTestCase2 = layeredIntervalTestCase{
	actions: []func(tc *TimestampCache, ts roachpb.Timestamp){
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// No overlap forwards.
			// Old contains new backwards.
			tc.Add(roachpb.Key("d"), roachpb.Key("f"), ts, nil, true)
		},
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// New contains old forwards.
			// Left partial overlap backwards.
			tc.Add(roachpb.Key("b"), roachpb.Key("f"), ts, nil, true)
		},
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// Right partial overlap forwards.
			// No overlap backwards.
			tc.Add(roachpb.Key("a"), roachpb.Key("c"), ts, nil, true)
		},
	},
	validator: func(t *testing.T, tc *TimestampCache, tss []roachpb.Timestamp) {
		bfTS := tss[1]
		acTS := tss[2]

		// Try different sub ranges.
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), nil, nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"a\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"b\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c"), nil, nil); !rTS.Equal(bfTS) {
			t.Errorf("expected \"c\" to have bfTS %v timestamp, found %v", bfTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("d"), nil, nil); !rTS.Equal(bfTS) {
			t.Errorf("expected \"d\" to have bfTS %v timestamp, found %v", bfTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("c"), nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"a\"-\"c\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("d"), nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"b\"-\"d\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c"), roachpb.Key("d"), nil); !rTS.Equal(bfTS) {
			t.Errorf("expected \"c\"-\"d\" to have bfTS %v timestamp, found %v", bfTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c0"), roachpb.Key("d"), nil); !rTS.Equal(bfTS) {
			t.Errorf("expected \"c0\"-\"d\" to have bfTS %v timestamp, found %v", bfTS, rTS)
		}
	},
}

// layeredIntervalTestCase3 tests a right partial overlap with a shared end
// for adding intervals to the interval cache when tested in order, and
// tests a left partial overlap with a shared end when tested in reverse.
var layeredIntervalTestCase3 = layeredIntervalTestCase{
	actions: []func(tc *TimestampCache, ts roachpb.Timestamp){
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// No overlap forwards.
			// Right partial overlap backwards.
			tc.Add(roachpb.Key("a"), roachpb.Key("c"), ts, nil, true)
		},
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// Left partial overlap forwards.
			// No overlap backwards.
			tc.Add(roachpb.Key("b"), roachpb.Key("c"), ts, nil, true)
		},
	},
	validator: func(t *testing.T, tc *TimestampCache, tss []roachpb.Timestamp) {
		acTS := tss[0]
		bcTS := tss[1]

		// Try different sub ranges.
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), nil, nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"a\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !rTS.Equal(bcTS) {
			t.Errorf("expected \"b\" to have bcTS %v timestamp, found %v", bcTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c"), nil, nil); !rTS.Equal(tc.lowWater) {
			t.Errorf("expected \"c\" to have low water %v timestamp, found %v", tc.lowWater, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("c"), nil); !rTS.Equal(bcTS) {
			t.Errorf("expected \"a\"-\"c\" to have bcTS %v timestamp, found %v", bcTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("b"), nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"a\"-\"b\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("c"), nil); !rTS.Equal(bcTS) {
			t.Errorf("expected \"b\"-\"c\" to have bcTS %v timestamp, found %v", bcTS, rTS)
		}
	},
}

// layeredIntervalTestCase4 tests a left partial overlap with a shared start
// for adding intervals to the interval cache when tested in order, and
// tests a right partial overlap with a shared start when tested in reverse.
var layeredIntervalTestCase4 = layeredIntervalTestCase{
	actions: []func(tc *TimestampCache, ts roachpb.Timestamp){
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// No overlap forwards.
			// Left partial overlap backwards.
			tc.Add(roachpb.Key("a"), roachpb.Key("c"), ts, nil, true)
		},
		func(tc *TimestampCache, ts roachpb.Timestamp) {
			// Right partial overlap forwards.
			// No overlap backwards.
			tc.Add(roachpb.Key("a"), roachpb.Key("b"), ts, nil, true)
		},
	},
	validator: func(t *testing.T, tc *TimestampCache, tss []roachpb.Timestamp) {
		acTS := tss[0]
		abTS := tss[1]

		// Try different sub ranges.
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), nil, nil); !rTS.Equal(abTS) {
			t.Errorf("expected \"a\" to have abTS %v timestamp, found %v", abTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"b\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("c"), nil, nil); !rTS.Equal(tc.lowWater) {
			t.Errorf("expected \"c\" to have low water %v timestamp, found %v", tc.lowWater, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("c"), nil); !rTS.Equal(abTS) {
			t.Errorf("expected \"a\"-\"c\" to have abTS %v timestamp, found %v", abTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("b"), nil); !rTS.Equal(abTS) {
			t.Errorf("expected \"a\"-\"b\" to have abTS %v timestamp, found %v", abTS, rTS)
		}
		if rTS, _ := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("c"), nil); !rTS.Equal(acTS) {
			t.Errorf("expected \"b\"-\"c\" to have acTS %v timestamp, found %v", acTS, rTS)
		}
	},
}

// TestTimestampCacheLayeredIntervals verifies the maximum timestamp
// is chosen if previous entries have ranges which are layered over
// each other.
//
// The test uses the layeredIntervalTestCase struct to allow reordering
// of interval insertions while keeping each interval's timestamp fixed.
// This can be used to verify that only the provided timestamp is used to
// determine layering, and that the interval insertion order is irrelevant.
func TestTimestampCacheLayeredIntervals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(0)
	tc := NewTimestampCache(clock)

	for _, testCase := range []layeredIntervalTestCase{
		layeredIntervalTestCase1,
		layeredIntervalTestCase2,
		layeredIntervalTestCase3,
		layeredIntervalTestCase4,
	} {
		// Perform actions in order and validate.
		tc.Clear(clock)
		tss := make([]roachpb.Timestamp, len(testCase.actions))
		for i := range testCase.actions {
			tss[i] = clock.Now()
		}
		for i, action := range testCase.actions {
			action(tc, tss[i])
		}
		testCase.validator(t, tc, tss)

		// Perform actions out of order and validate.
		tc.Clear(clock)
		for i := range testCase.actions {
			// Recreate timestamps because Clear() sets lowWater to Now().
			tss[i] = clock.Now()
		}
		for i := len(testCase.actions) - 1; i >= 0; i-- {
			testCase.actions[i](tc, tss[i])
		}
		testCase.validator(t, tc, tss)
	}
}

func TestTimestampCacheClear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := NewTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	ts := clock.Now()
	tc.Add(roachpb.Key("a"), nil, ts, nil, true)

	// Clear the cache, which will reset the low water mark to
	// the current time + maxClockOffset.
	tc.Clear(clock)

	// Fetching any keys should give current time + maxClockOffset
	expTS := clock.Timestamp()
	expTS.WallTime += maxClockOffset.Nanoseconds()
	if rTS, ok := tc.GetMaxRead(roachpb.Key("a"), nil, nil); !rTS.Equal(expTS) || ok {
		t.Errorf("expected \"a\" to have cleared timestamp; exp ok=false; got %t", ok)
	}
}

// TestTimestampCacheReplacements verifies that a newer entry
// in the timestamp cache which completely "covers" an older
// entry will replace it.
func TestTimestampCacheReplacements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	txn1ID := uuid.NewV4()
	txn2ID := uuid.NewV4()

	ts1 := clock.Now()
	tc.Add(roachpb.Key("a"), nil, ts1, nil, true)
	if ts, ok := tc.GetMaxRead(roachpb.Key("a"), nil, nil); !ts.Equal(ts1) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts1, ts, ok)
	}
	// Write overlapping value with txn1 and verify with txn1--we should get
	// low water mark, not ts1.
	ts2 := clock.Now()
	tc.Add(roachpb.Key("a"), nil, ts2, txn1ID, true)
	if ts, ok := tc.GetMaxRead(roachpb.Key("a"), nil, txn1ID); !ts.Equal(tc.lowWater) || ok {
		t.Errorf("expected low water (empty) time; got %s; ok=%t", ts, ok)
	}
	// Write range which overlaps "a" with txn2 and verify with txn2--we should
	// get low water mark, not ts2.
	ts3 := clock.Now()
	tc.Add(roachpb.Key("a"), roachpb.Key("c"), ts3, txn2ID, true)
	if ts, ok := tc.GetMaxRead(roachpb.Key("a"), nil, txn2ID); !ts.Equal(tc.lowWater) || ok {
		t.Errorf("expected low water (empty) time; got %s; ok=%t", ts, ok)
	}
	// Also, verify txn1 sees ts3.
	if ts, ok := tc.GetMaxRead(roachpb.Key("a"), nil, txn1ID); !ts.Equal(ts3) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts3, ts, ok)
	}
	// Now, write to "b" with a higher timestamp and no txn. Should be
	// visible to all txns.
	ts4 := clock.Now()
	tc.Add(roachpb.Key("b"), nil, ts4, nil, true)
	if ts, ok := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !ts.Equal(ts4) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts4, ts, ok)
	}
	if ts, ok := tc.GetMaxRead(roachpb.Key("b"), nil, txn1ID); !ts.Equal(ts4) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts4, ts, ok)
	}
	// Finally, write an earlier version of "a"; should simply get
	// tossed and we should see ts4 still.
	tc.Add(roachpb.Key("b"), nil, ts1, nil, true)
	if ts, ok := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !ts.Equal(ts4) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts4, ts, ok)
	}
}

// TestTimestampCacheWithTxnID verifies that timestamps matching
// the specified txn ID are ignored.
func TestTimestampCacheWithTxnID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	// Add two successive txn entries.
	txn1ID := uuid.NewV4()
	txn2ID := uuid.NewV4()
	ts1 := clock.Now()
	tc.Add(roachpb.Key("a"), roachpb.Key("c"), ts1, txn1ID, true)
	ts2 := clock.Now()
	// This entry will remove "a"-"b" from the cache.
	tc.Add(roachpb.Key("b"), roachpb.Key("d"), ts2, txn2ID, true)

	// Fetching with no transaction gets latest value.
	if ts, ok := tc.GetMaxRead(roachpb.Key("b"), nil, nil); !ts.Equal(ts2) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts2, ts, ok)
	}
	// Fetching with txn ID "1" gets most recent.
	if ts, ok := tc.GetMaxRead(roachpb.Key("b"), nil, txn1ID); !ts.Equal(ts2) || !ok {
		t.Errorf("expected %s; got %s; ok=%t", ts2, ts, ok)
	}
	// Fetching with txn ID "2" skips most recent.
	if ts, ok := tc.GetMaxRead(roachpb.Key("b"), nil, txn2ID); !ts.Equal(tc.lowWater) || ok {
		t.Errorf("expected %s; got %s; ok=%t", ts1, ts, ok)
	}
}

// TestTimestampCacheReadVsWrite verifies that the timestamp cache
// can differentiate between read and write timestamp.
func TestTimestampCacheReadVsWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	// Add read-only non-txn entry at current time.
	ts1 := clock.Now()
	tc.Add(roachpb.Key("a"), roachpb.Key("b"), ts1, nil, true)

	// Add two successive txn entries; one read-only and one read-write.
	txn1ID := uuid.NewV4()
	txn2ID := uuid.NewV4()
	ts2 := clock.Now()
	tc.Add(roachpb.Key("a"), nil, ts2, txn1ID, true)
	ts3 := clock.Now()
	tc.Add(roachpb.Key("a"), nil, ts3, txn2ID, false)

	// Fetching with no transaction gets latest values.
	rTS, rOK := tc.GetMaxRead(roachpb.Key("a"), nil, nil)
	wTS, wOK := tc.GetMaxWrite(roachpb.Key("a"), nil, nil)
	if !rTS.Equal(ts2) || !wTS.Equal(ts3) || !rOK || !wOK {
		t.Errorf("expected %s %s; got %s %s; rOK=%t, wOK=%t", ts2, ts3, rTS, wTS, rOK, wOK)
	}
	// Fetching with txn ID "1" gets low water mark for read and most recent for write.
	rTS, rOK = tc.GetMaxRead(roachpb.Key("a"), nil, txn1ID)
	wTS, wOK = tc.GetMaxWrite(roachpb.Key("a"), nil, txn1ID)
	if !rTS.Equal(tc.lowWater) || !wTS.Equal(ts3) || rOK || !wOK {
		t.Errorf("expected %s %s; got %s %s; rOK=%t, wOK=%t", ts1, ts3, rTS, wTS, rOK, wOK)
	}
	// Fetching with txn ID "2" gets ts2 for read and low water mark for write.
	rTS, rOK = tc.GetMaxRead(roachpb.Key("a"), nil, txn2ID)
	wTS, wOK = tc.GetMaxWrite(roachpb.Key("a"), nil, txn2ID)
	if !rTS.Equal(ts2) || !wTS.Equal(tc.lowWater) || !rOK || wOK {
		t.Errorf("expected %s %s; got %s %s; rOK=%t, wOK=%t", ts2, tc.lowWater, rTS, wTS, rOK, wOK)
	}
}

func BenchmarkTimestampCacheInsertion(b *testing.B) {
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := NewTimestampCache(clock)

	for i := 0; i < b.N; i++ {
		tc.Clear(clock)

		cdTS := clock.Now()
		tc.Add(roachpb.Key("c"), roachpb.Key("d"), cdTS, nil, true)

		beTS := clock.Now()
		tc.Add(roachpb.Key("b"), roachpb.Key("e"), beTS, nil, true)

		adTS := clock.Now()
		tc.Add(roachpb.Key("a"), roachpb.Key("d"), adTS, nil, true)

		cfTS := clock.Now()
		tc.Add(roachpb.Key("c"), roachpb.Key("f"), cfTS, nil, true)
	}
}
