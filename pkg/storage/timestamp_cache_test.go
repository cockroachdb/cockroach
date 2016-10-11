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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	maxClockOffset = 250 * time.Millisecond
)

func TestTimestampCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := newTimestampCache(clock)

	// First simulate a read of just "a" at time 0.
	tc.add(roachpb.Key("a"), nil, clock.Now(), nil, true)
	// Although we added "a" at time 0, the internal cache should still
	// be empty because the t=0 < lowWater.
	if tc.rCache.Len() > 0 {
		t.Errorf("expected cache to be empty, but contains %d elements", tc.rCache.Len())
	}
	// Verify GetMax returns the lowWater mark which is maxClockOffset.
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("a"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"a\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("notincache"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"notincache\"; ok=%t", ok)
	}

	// Advance the clock and verify same low water mark.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("a"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"a\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("notincache"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"notincache\"; ok=%t", ok)
	}

	// Sim a read of "b"-"c" at time maxClockOffset + 1.
	ts := clock.Now()
	tc.add(roachpb.Key("b"), roachpb.Key("c"), ts, nil, true)

	// Verify all permutations of direct and range access.
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("b"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"b\"; got %s; ok=%t", rTS, ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("bb"), nil); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bb\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("c"), nil); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"c\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("c")); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"b\"-\"c\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("bb"), roachpb.Key("bz")); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bb\"-\"bz\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("b")); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
		t.Errorf("expected maxClockOffset for key \"a\"-\"b\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("bb")); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"a\"-\"bb\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("d")); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"a\"-\"d\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("bz"), roachpb.Key("c")); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bz\"-\"c\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("bz"), roachpb.Key("d")); !rTS.Equal(ts) || !ok {
		t.Errorf("expected current time for key \"bz\"-\"d\"; ok=%t", ok)
	}
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("c"), roachpb.Key("d")); rTS.WallTime != maxClockOffset.Nanoseconds() || ok {
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
	tc := newTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 10.
	manual.Set(maxClockOffset.Nanoseconds() + 10)
	aTS := clock.Now()
	tc.add(roachpb.Key("a"), nil, aTS, nil, true)

	// Increment time by 10ns and add another key.
	manual.Increment(10)
	bTS := clock.Now()
	tc.add(roachpb.Key("b"), nil, bTS, nil, true)

	// Increment time by 10ns and add another key.
	manual.Increment(10)
	cTS := clock.Now()
	tc.add(roachpb.Key("c"), nil, cTS, nil, true)

	// Set low water mark.
	tc.SetLowWater(bTS)

	// Verify looking up key "a" returns the new low water mark ("a"'s timestamp).
	for i, test := range []struct {
		key   roachpb.Key
		expTS hlc.Timestamp
		expOK bool
	}{
		{roachpb.Key("a"), bTS, false},
		{roachpb.Key("b"), bTS, false},
		{roachpb.Key("c"), cTS, true},
		{roachpb.Key("d"), bTS, false},
	} {
		if rTS, _, ok := tc.GetMaxRead(test.key, nil); !rTS.Equal(test.expTS) || ok != test.expOK {
			t.Errorf("%d: expected ts %s, got %s; exp ok=%t; got %t", i, test.expTS, rTS, test.expOK, ok)
		}
	}

	// Try setting a lower low water mark than the previous value.
	tc.SetLowWater(aTS)
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("d"), nil); !rTS.Equal(bTS) || ok {
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
	tc := newTimestampCache(clock)
	tc.evictionSizeThreshold = 0

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	aTS := clock.Now()
	tc.add(roachpb.Key("a"), nil, aTS, nil, true)

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.add(roachpb.Key("b"), nil, clock.Now(), nil, true)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("c"), nil); !rTS.Equal(aTS) || ok {
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
	tc := newTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	aTS := clock.Now()
	tc.add(roachpb.Key("a"), nil, aTS, nil, true)
	tc.AddRequest(cacheRequest{
		reads:     []roachpb.Span{{Key: roachpb.Key("c")}},
		timestamp: aTS,
	})

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.add(roachpb.Key("b"), nil, clock.Now(), nil, true)
	tc.AddRequest(cacheRequest{
		reads:     []roachpb.Span{{Key: roachpb.Key("d")}},
		timestamp: clock.Now(),
	})

	// Verify that the cache still has 4 entries in it
	if l, want := tc.len(), 4; l != want {
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
		tc1 := newTimestampCache(clock)
		tc2 := newTimestampCache(clock)

		bfTS := clock.Now()
		tc2.add(roachpb.Key("b"), roachpb.Key("f"), bfTS, nil, true)

		adTS := clock.Now()
		tc1.add(roachpb.Key("a"), roachpb.Key("d"), adTS, nil, true)

		beTS := clock.Now()
		tc1.add(roachpb.Key("b"), roachpb.Key("e"), beTS, nil, true)

		aaTS := clock.Now()
		tc2.add(roachpb.Key("aa"), nil, aaTS, nil, true)

		cTS := clock.Now()
		tc1.add(roachpb.Key("c"), nil, cTS, nil, true)

		tc1.MergeInto(tc2, test.useClear)

		if tc2.rCache.Len() != test.expLen {
			t.Errorf("expected merged length of %d; got %d", test.expLen, tc2.rCache.Len())
		}
		if !tc2.latest.Equal(tc1.latest) {
			t.Errorf("expected latest to be updated to %s; got %s", tc1.latest, tc2.latest)
		}

		if rTS, _, ok := tc2.GetMaxRead(roachpb.Key("a"), nil); !rTS.Equal(adTS) || !ok {
			t.Errorf("expected \"a\" to have adTS timestamp; ok=%t", ok)
		}
		if rTS, _, ok := tc2.GetMaxRead(roachpb.Key("b"), nil); !rTS.Equal(beTS) || !ok {
			t.Errorf("expected \"b\" to have beTS timestamp; ok=%t", ok)
		}
		if test.useClear {
			if rTS, _, ok := tc2.GetMaxRead(roachpb.Key("aa"), nil); !rTS.Equal(adTS) || !ok {
				t.Errorf("expected \"aa\" to have adTS timestamp; ok=%t", ok)
			}
		} else {
			if rTS, _, ok := tc2.GetMaxRead(roachpb.Key("aa"), nil); !rTS.Equal(aaTS) || !ok {
				t.Errorf("expected \"aa\" to have aaTS timestamp; ok=%t", ok)
			}
			if rTS, _, ok := tc2.GetMaxRead(roachpb.Key("a"), roachpb.Key("c")); !rTS.Equal(aaTS) || !ok {
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

type txnState struct {
	ts hlc.Timestamp
	id *uuid.UUID
}

type layeredIntervalTestCase struct {
	spans     []roachpb.Span
	validator func(t *testing.T, tc *timestampCache, txns []txnState)
}

// assertTS is a helper function for layeredIntervalTestCase
// validators. It queries the timestamp cache for the given keys and
// reports a test error if it doesn't match the given timestamp and
// transaction ID.
func assertTS(
	t *testing.T,
	tc *timestampCache,
	start, end roachpb.Key,
	expectedTS hlc.Timestamp,
	expectedTxnID *uuid.UUID,
) {
	var keys string
	if len(end) == 0 {
		keys = fmt.Sprintf("%q", start)
	} else {
		keys = fmt.Sprintf("%q-%q", start, end)
	}
	ts, txnID, _ := tc.GetMaxRead(start, end)
	if !ts.Equal(expectedTS) {
		t.Errorf("expected %s to have timestamp %v, found %v", keys, expectedTS, ts)
	}
	if expectedTxnID == nil {
		if txnID != nil {
			t.Errorf("expected %s to have no txn id, but found %s", keys, txnID.Short())
		}
	} else {
		if txnID == nil {
			t.Errorf("expected %s to have txn id %s, but found nil", keys, expectedTxnID.Short())
		} else if *txnID != *expectedTxnID {
			t.Errorf("expected %s to have txn id %s, but found %s",
				keys, expectedTxnID.Short(), txnID.Short())
		}
	}
}

// nilIfSimul returns nil if this test involves multiple transactions
// with the same timestamp (i.e. the timestamps in txns are identical
// but the transaction ids are not), and the given txnID if they are
// not. This is because timestampCache.GetMaxRead must not return a
// transaction ID when two different transactions have the same timestamp.
func nilIfSimul(txns []txnState, txnID *uuid.UUID) *uuid.UUID {
	if txns[0].ts.Equal(txns[1].ts) && *txns[0].id != *txns[1].id {
		return nil
	}
	return txnID
}

// layeredIntervalTestCase1 tests the left partial overlap and old containing
// new cases for adding intervals to the interval cache when tested in order,
// and tests the cases' inverses when tested in reverse.
var layeredIntervalTestCase1 = layeredIntervalTestCase{
	spans: []roachpb.Span{
		// No overlap forwards.
		// Right partial overlap backwards.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("bb")},
		// Left partial overlap forwards.
		// New contains old backwards.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("e")},
		// Old contains new forwards.
		// No overlap backwards.
		{Key: roachpb.Key("c")},
	},
	validator: func(t *testing.T, tc *timestampCache, txns []txnState) {
		abbTx, beTx, cTx := txns[0], txns[1], txns[2]

		assertTS(t, tc, roachpb.Key("a"), nil, abbTx.ts, abbTx.id)
		assertTS(t, tc, roachpb.Key("b"), nil, beTx.ts, nilIfSimul(txns, beTx.id))
		assertTS(t, tc, roachpb.Key("c"), nil, cTx.ts, nilIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("d"), nil, beTx.ts, beTx.id)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("b"), abbTx.ts, abbTx.id)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), beTx.ts, nilIfSimul(txns, beTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("d"), cTx.ts, nilIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("d"), cTx.ts, nilIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("c"), roachpb.Key("d"), cTx.ts, nilIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("c0"), roachpb.Key("d"), beTx.ts, beTx.id)
	},
}

// layeredIntervalTestCase2 tests the right partial overlap and new containing
// old cases for adding intervals to the interval cache when tested in order,
// and tests the cases' inverses when tested in reverse.
var layeredIntervalTestCase2 = layeredIntervalTestCase{
	spans: []roachpb.Span{
		// No overlap forwards.
		// Old contains new backwards.
		{Key: roachpb.Key("d"), EndKey: roachpb.Key("f")},
		// New contains old forwards.
		// Left partial overlap backwards.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("f")},
		// Right partial overlap forwards.
		// No overlap backwards.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
	},
	validator: func(t *testing.T, tc *timestampCache, txns []txnState) {
		_, bfTx, acTx := txns[0], txns[1], txns[2]

		assertTS(t, tc, roachpb.Key("a"), nil, acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("b"), nil, acTx.ts, nilIfSimul(txns, acTx.id))
		assertTS(t, tc, roachpb.Key("c"), nil, bfTx.ts, bfTx.id)
		assertTS(t, tc, roachpb.Key("d"), nil, bfTx.ts, nilIfSimul(txns, bfTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), acTx.ts, nilIfSimul(txns, acTx.id))
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("d"), acTx.ts, nilIfSimul(txns, acTx.id))
		assertTS(t, tc, roachpb.Key("c"), roachpb.Key("d"), bfTx.ts, bfTx.id)
		assertTS(t, tc, roachpb.Key("c0"), roachpb.Key("d"), bfTx.ts, bfTx.id)
	},
}

// layeredIntervalTestCase3 tests a right partial overlap with a shared end
// for adding intervals to the interval cache when tested in order, and
// tests a left partial overlap with a shared end when tested in reverse.
var layeredIntervalTestCase3 = layeredIntervalTestCase{
	spans: []roachpb.Span{
		// No overlap forwards.
		// Right partial overlap backwards.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		// Left partial overlap forwards.
		// No overlap backwards.
		{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
	},
	validator: func(t *testing.T, tc *timestampCache, txns []txnState) {
		acTx, bcTx := txns[0], txns[1]

		assertTS(t, tc, roachpb.Key("a"), nil, acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("b"), nil, bcTx.ts, nilIfSimul(txns, bcTx.id))
		assertTS(t, tc, roachpb.Key("c"), nil, tc.lowWater, nil)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), bcTx.ts, nilIfSimul(txns, bcTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("b"), acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("c"), bcTx.ts, nilIfSimul(txns, bcTx.id))
	},
}

// layeredIntervalTestCase4 tests a left partial overlap with a shared start
// for adding intervals to the interval cache when tested in order, and
// tests a right partial overlap with a shared start when tested in reverse.
var layeredIntervalTestCase4 = layeredIntervalTestCase{
	spans: []roachpb.Span{
		// No overlap forwards.
		// Left partial overlap backwards.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		// Right partial overlap forwards.
		// No overlap backwards.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	},
	validator: func(t *testing.T, tc *timestampCache, txns []txnState) {
		acTx, abTx := txns[0], txns[1]

		assertTS(t, tc, roachpb.Key("a"), nil, abTx.ts, nilIfSimul(txns, abTx.id))
		assertTS(t, tc, roachpb.Key("b"), nil, acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("c"), nil, tc.lowWater, nil)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), abTx.ts, nilIfSimul(txns, abTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("b"), abTx.ts, nilIfSimul(txns, abTx.id))
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("c"), acTx.ts, acTx.id)
	},
}

var layeredIntervalTestCase5 = layeredIntervalTestCase{
	spans: []roachpb.Span{
		// Two identical spans
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	},
	validator: func(t *testing.T, tc *timestampCache, txns []txnState) {
		assertTS(t, tc, roachpb.Key("a"), nil, txns[1].ts, nilIfSimul(txns, txns[1].id))
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
	tc := newTimestampCache(clock)

	// Run each test case in several configurations.
	for testCaseIdx, testCase := range []layeredIntervalTestCase{
		layeredIntervalTestCase1,
		layeredIntervalTestCase2,
		layeredIntervalTestCase3,
		layeredIntervalTestCase4,
		layeredIntervalTestCase5,
	} {
		t.Logf("test case %d", testCaseIdx+1)

		// In simultaneous runs, each span in the test case is given the
		// same time. Otherwise each gets a distinct timestamp (in the
		// order of definition).
		for _, simultaneous := range []bool{false, true} {
			t.Logf("simultaneous: %v", simultaneous)

			// In reverse runs, spans are inserted into the timestamp cache
			// out of order (so spans with higher timestamps are inserted
			// before those with lower timestamps). In simultaneous+reverse
			// runs, timestamps are all the same, but running in both
			// directions is still necessary to exercise all branches in the
			// code.
			for _, reverse := range []bool{false, true} {
				t.Logf("reverse: %v", reverse)

				// In sameTxn runs, all spans are inserted as a part of the
				// same transaction; otherwise each is a separate transaction.
				for _, sameTxn := range []bool{false, true} {
					t.Logf("sameTxn: %v", sameTxn)

					txns := make([]txnState, len(testCase.spans))
					if sameTxn {
						id := uuid.NewV4()
						for i := range testCase.spans {
							txns[i].id = id
						}
					} else {
						for i := range testCase.spans {
							txns[i].id = uuid.NewV4()
						}
					}

					tc.Clear(clock)
					if simultaneous {
						now := clock.Now()
						for i := range txns {
							txns[i].ts = now
						}
					} else {
						for i := range txns {
							txns[i].ts = clock.Now()
						}
					}

					if reverse {
						for i := len(testCase.spans) - 1; i >= 0; i-- {
							tc.add(testCase.spans[i].Key, testCase.spans[i].EndKey, txns[i].ts, txns[i].id, true)
						}
					} else {
						for i := range testCase.spans {
							tc.add(testCase.spans[i].Key, testCase.spans[i].EndKey, txns[i].ts, txns[i].id, true)
						}
					}
					testCase.validator(t, tc, txns)
				}
			}
		}
	}
}

func TestTimestampCacheClear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxClockOffset)
	tc := newTimestampCache(clock)

	// Increment time to the maxClockOffset low water mark + 1.
	manual.Set(maxClockOffset.Nanoseconds() + 1)
	ts := clock.Now()
	tc.add(roachpb.Key("a"), nil, ts, nil, true)

	// Clear the cache, which will reset the low water mark to
	// the current time + maxClockOffset.
	tc.Clear(clock)

	// Fetching any keys should give current time + maxClockOffset
	expTS := clock.Timestamp()
	expTS.WallTime += maxClockOffset.Nanoseconds()
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("a"), nil); !rTS.Equal(expTS) || ok {
		t.Errorf("expected \"a\" to have cleared timestamp; exp ok=false; got %t", ok)
	}
}

// TestTimestampCacheReadVsWrite verifies that the timestamp cache
// can differentiate between read and write timestamp.
func TestTimestampCacheReadVsWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := newTimestampCache(clock)

	// Add read-only non-txn entry at current time.
	ts1 := clock.Now()
	tc.add(roachpb.Key("a"), roachpb.Key("b"), ts1, nil, true)

	// Add two successive txn entries; one read-only and one read-write.
	txn1ID := uuid.NewV4()
	txn2ID := uuid.NewV4()
	ts2 := clock.Now()
	tc.add(roachpb.Key("a"), nil, ts2, txn1ID, true)
	ts3 := clock.Now()
	tc.add(roachpb.Key("a"), nil, ts3, txn2ID, false)

	rTS, _, rOK := tc.GetMaxRead(roachpb.Key("a"), nil)
	wTS, _, wOK := tc.GetMaxWrite(roachpb.Key("a"), nil)
	if !rTS.Equal(ts2) || !wTS.Equal(ts3) || !rOK || !wOK {
		t.Errorf("expected %s %s; got %s %s; rOK=%t, wOK=%t", ts2, ts3, rTS, wTS, rOK, wOK)
	}
}

// TestTimestampCacheEqualTimestamp verifies that in the event of two
// non-overlapping transactions with equal timestamps, the returned
// timestamp is not owned by either one.
func TestTimestampCacheEqualTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := newTimestampCache(clock)

	txn1 := uuid.NewV4()
	txn2 := uuid.NewV4()

	// Add two non-overlapping transactions at the same timestamp.
	ts1 := clock.Now()
	tc.add(roachpb.Key("a"), roachpb.Key("b"), ts1, txn1, true)
	tc.add(roachpb.Key("b"), roachpb.Key("c"), ts1, txn2, true)

	// When querying either side separately, the transaction ID is returned.
	if ts, txn, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("b")); !ts.Equal(ts1) {
		t.Errorf("expected 'a'-'b' to have timestamp %s, but found %s", ts1, ts)
	} else if *txn != *txn1 {
		t.Errorf("expected 'a'-'b' to have txn id %s, but found %s", txn1, txn)
	}
	if ts, txn, _ := tc.GetMaxRead(roachpb.Key("b"), roachpb.Key("c")); !ts.Equal(ts1) {
		t.Errorf("expected 'b'-'c' to have timestamp %s, but found %s", ts1, ts)
	} else if *txn != *txn2 {
		t.Errorf("expected 'b'-'c' to have txn id %s, but found %s", txn2, txn)
	}

	// Querying a span that overlaps both returns a nil txn ID; neither
	// can proceed here.
	if ts, txn, _ := tc.GetMaxRead(roachpb.Key("a"), roachpb.Key("c")); !ts.Equal(ts1) {
		t.Errorf("expected 'a'-'c' to have timestamp %s, but found %s", ts1, ts)
	} else if txn != nil {
		t.Errorf("expected 'a'-'c' to have nil txn id, but found %s", txn)
	}
}

func BenchmarkTimestampCacheInsertion(b *testing.B) {
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	tc := newTimestampCache(clock)

	for i := 0; i < b.N; i++ {
		tc.Clear(clock)

		cdTS := clock.Now()
		tc.add(roachpb.Key("c"), roachpb.Key("d"), cdTS, nil, true)

		beTS := clock.Now()
		tc.add(roachpb.Key("b"), roachpb.Key("e"), beTS, nil, true)

		adTS := clock.Now()
		tc.add(roachpb.Key("a"), roachpb.Key("d"), adTS, nil, true)

		cfTS := clock.Now()
		tc.add(roachpb.Key("c"), roachpb.Key("f"), cfTS, nil, true)
	}
}
