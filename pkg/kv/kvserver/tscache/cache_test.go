// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tscache

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

var cacheImplConstrs = []func(clock *hlc.Clock) Cache{
	func(clock *hlc.Clock) Cache { return newTreeImpl(clock) },
	func(clock *hlc.Clock) Cache { return newSklImpl(clock) },
}

func forEachCacheImpl(
	t *testing.T, fn func(t *testing.T, tc Cache, clock *hlc.Clock, manual *hlc.ManualClock),
) {
	for _, constr := range cacheImplConstrs {
		const baseTS = 100
		manual := hlc.NewManualClock(baseTS)
		clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

		tc := constr(clock)
		tcName := reflect.TypeOf(tc).Elem().Name()
		t.Run(tcName, func(t *testing.T) {
			fn(t, tc, clock, manual)
		})
	}
}

func TestTimestampCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	forEachCacheImpl(t, func(t *testing.T, tc Cache, clock *hlc.Clock, manual *hlc.ManualClock) {
		baseTS := manual.UnixNano()

		// First simulate a read of just "a" at time 50.
		tc.Add(roachpb.Key("a"), nil, hlc.Timestamp{WallTime: 50}, noTxnID)
		// Verify GetMax returns the lowWater mark.
		if rTS, rTxnID := tc.GetMax(roachpb.Key("a"), nil); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"a\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("notincache"), nil); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"notincache\"; txnID=%s", rTxnID)
		}

		// Advance the clock and verify same low water mark.
		manual.Increment(100)
		if rTS, rTxnID := tc.GetMax(roachpb.Key("a"), nil); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"a\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("notincache"), nil); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"notincache\"; txnID=%s", rTxnID)
		}

		// Sim a read of "b"-"c" at a time above the low-water mark.
		ts := clock.Now()
		tc.Add(roachpb.Key("b"), roachpb.Key("c"), ts, noTxnID)

		// Verify all permutations of direct and range access.
		if rTS, rTxnID := tc.GetMax(roachpb.Key("b"), nil); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"b\"; got %s; txnID=%s", rTS, rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("bb"), nil); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"bb\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("c"), nil); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"c\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("b"), roachpb.Key("c")); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"b\"-\"c\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("bb"), roachpb.Key("bz")); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"bb\"-\"bz\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("a"), roachpb.Key("b")); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"a\"-\"b\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("a"), roachpb.Key("bb")); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"a\"-\"bb\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("a"), roachpb.Key("d")); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"a\"-\"d\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("bz"), roachpb.Key("c")); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"bz\"-\"c\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("bz"), roachpb.Key("d")); rTS != ts || rTxnID != noTxnID {
			t.Errorf("expected current time for key \"bz\"-\"d\"; txnID=%s", rTxnID)
		}
		if rTS, rTxnID := tc.GetMax(roachpb.Key("c"), roachpb.Key("d")); rTS.WallTime != baseTS || rTxnID != noTxnID {
			t.Errorf("expected baseTS for key \"c\"-\"d\"; txnID=%s", rTxnID)
		}
	})
}

type txnState struct {
	ts hlc.Timestamp
	id uuid.UUID
}

type layeredIntervalTestCase struct {
	spans     []roachpb.Span
	validator func(t *testing.T, tc Cache, txns []txnState)
}

// assertTS is a helper function for layeredIntervalTestCase
// validators. It queries the timestamp cache for the given keys and
// reports a test error if it doesn't match the given timestamp and
// transaction ID.
func assertTS(
	t *testing.T, tc Cache, start, end roachpb.Key, expectedTS hlc.Timestamp, expectedTxnID uuid.UUID,
) {
	var keys string
	if len(end) == 0 {
		keys = fmt.Sprintf("%q", start)
	} else {
		keys = fmt.Sprintf("%q-%q", start, end)
	}
	ts, txnID := tc.GetMax(start, end)
	if ts != expectedTS {
		t.Errorf("expected %s to have timestamp %v, found %v", keys, expectedTS, ts)
	}
	if txnID != expectedTxnID {
		t.Errorf("expected %s to have txn id %s, but found %s", keys, expectedTxnID.Short(), txnID.Short())
	}
}

// zeroIfSimul returns a zero UUID if this test involves multiple transactions
// with the same timestamp (i.e. the timestamps in txns are identical but the
// transaction ids are not), and the given txnID if they are not. This is
// because timestampCache.GetMax must not return a transaction ID when two
// different transactions have the same timestamp.
func zeroIfSimul(txns []txnState, txnID uuid.UUID) uuid.UUID {
	if txns[0].ts == txns[1].ts && txns[0].id != txns[1].id {
		return noTxnID
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
	validator: func(t *testing.T, tc Cache, txns []txnState) {
		abbTx, beTx, cTx := txns[0], txns[1], txns[2]

		assertTS(t, tc, roachpb.Key("a"), nil, abbTx.ts, abbTx.id)
		assertTS(t, tc, roachpb.Key("b"), nil, beTx.ts, zeroIfSimul(txns, beTx.id))
		assertTS(t, tc, roachpb.Key("c"), nil, cTx.ts, zeroIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("d"), nil, beTx.ts, beTx.id)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("b"), abbTx.ts, abbTx.id)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), beTx.ts, zeroIfSimul(txns, beTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("d"), cTx.ts, zeroIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("d"), cTx.ts, zeroIfSimul(txns, cTx.id))
		assertTS(t, tc, roachpb.Key("c"), roachpb.Key("d"), cTx.ts, zeroIfSimul(txns, cTx.id))
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
	validator: func(t *testing.T, tc Cache, txns []txnState) {
		_, bfTx, acTx := txns[0], txns[1], txns[2]

		assertTS(t, tc, roachpb.Key("a"), nil, acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("b"), nil, acTx.ts, zeroIfSimul(txns, acTx.id))
		assertTS(t, tc, roachpb.Key("c"), nil, bfTx.ts, bfTx.id)
		assertTS(t, tc, roachpb.Key("d"), nil, bfTx.ts, zeroIfSimul(txns, bfTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), acTx.ts, zeroIfSimul(txns, acTx.id))
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("d"), acTx.ts, zeroIfSimul(txns, acTx.id))
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
	validator: func(t *testing.T, tc Cache, txns []txnState) {
		acTx, bcTx := txns[0], txns[1]

		assertTS(t, tc, roachpb.Key("a"), nil, acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("b"), nil, bcTx.ts, zeroIfSimul(txns, bcTx.id))
		assertTS(t, tc, roachpb.Key("c"), nil, tc.getLowWater(), noTxnID)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), bcTx.ts, zeroIfSimul(txns, bcTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("b"), acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("c"), bcTx.ts, zeroIfSimul(txns, bcTx.id))
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
	validator: func(t *testing.T, tc Cache, txns []txnState) {
		acTx, abTx := txns[0], txns[1]

		assertTS(t, tc, roachpb.Key("a"), nil, abTx.ts, zeroIfSimul(txns, abTx.id))
		assertTS(t, tc, roachpb.Key("b"), nil, acTx.ts, acTx.id)
		assertTS(t, tc, roachpb.Key("c"), nil, tc.getLowWater(), noTxnID)
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("c"), abTx.ts, zeroIfSimul(txns, abTx.id))
		assertTS(t, tc, roachpb.Key("a"), roachpb.Key("b"), abTx.ts, zeroIfSimul(txns, abTx.id))
		assertTS(t, tc, roachpb.Key("b"), roachpb.Key("c"), acTx.ts, acTx.id)
	},
}

var layeredIntervalTestCase5 = layeredIntervalTestCase{
	spans: []roachpb.Span{
		// Two identical spans
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	},
	validator: func(t *testing.T, tc Cache, txns []txnState) {
		assertTS(t, tc, roachpb.Key("a"), nil, txns[1].ts, zeroIfSimul(txns, txns[1].id))
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

	forEachCacheImpl(t, func(t *testing.T, tc Cache, clock *hlc.Clock, manual *hlc.ManualClock) {
		// Run each test case in several configurations.
		for _, testCase := range []layeredIntervalTestCase{
			layeredIntervalTestCase1,
			layeredIntervalTestCase2,
			layeredIntervalTestCase3,
			layeredIntervalTestCase4,
			layeredIntervalTestCase5,
		} {
			t.Run("", func(t *testing.T) {
				// In simultaneous runs, each span in the test case is given the same
				// time. Otherwise each gets a distinct timestamp (in the order of
				// definition).
				testutils.RunTrueAndFalse(t, "simultaneous", func(t *testing.T, simultaneous bool) {
					// In reverse runs, spans are inserted into the timestamp cache out
					// of order (so spans with higher timestamps are inserted before
					// those with lower timestamps). In simultaneous+reverse runs,
					// timestamps are all the same, but running in both directions is
					// still necessary to exercise all branches in the code.
					testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
						// In sameTxn runs, all spans are inserted as a part of the same
						// transaction; otherwise each is a separate transaction.
						testutils.RunTrueAndFalse(t, "sameTxn", func(t *testing.T, sameTxn bool) {
							defer func() {
								tc.clear(clock.Now())
							}()

							txns := make([]txnState, len(testCase.spans))
							if sameTxn {
								id := uuid.MakeV4()
								for i := range testCase.spans {
									txns[i].id = id
								}
							} else {
								for i := range testCase.spans {
									txns[i].id = uuid.MakeV4()
								}
							}

							tc.clear(clock.Now())
							if simultaneous {
								now := clock.Now()
								for i := range txns {
									txns[i].ts = now
								}
							} else {
								manual.Increment(1)
								for i := range txns {
									txns[i].ts = clock.Now()
								}
							}

							if reverse {
								for i := len(testCase.spans) - 1; i >= 0; i-- {
									tc.Add(testCase.spans[i].Key, testCase.spans[i].EndKey, txns[i].ts, txns[i].id)
								}
							} else {
								for i := range testCase.spans {
									tc.Add(testCase.spans[i].Key, testCase.spans[i].EndKey, txns[i].ts, txns[i].id)
								}
							}
							testCase.validator(t, tc, txns)
						})
					})
				})
			})
		}
	})
}

func TestTimestampCacheClear(t *testing.T) {
	defer leaktest.AfterTest(t)()

	forEachCacheImpl(t, func(t *testing.T, tc Cache, clock *hlc.Clock, manual *hlc.ManualClock) {
		key := roachpb.Key("a")

		ts := clock.Now()
		tc.Add(key, nil, ts, noTxnID)

		manual.Increment(5000000)

		expTS := clock.Now()
		// Clear the cache, which will reset the low water mark to
		// the current time.
		tc.clear(expTS)

		// Fetching any keys should give current time.
		if rTS, rTxnID := tc.GetMax(key, nil); rTxnID != noTxnID {
			t.Errorf("%s unexpectedly associated to txn %s", key, rTxnID)
		} else if rTS != expTS {
			t.Errorf("expected %s, got %s", rTS, expTS)
		}
	})
}

// TestTimestampCacheEqualTimestamp verifies that in the event of two
// non-overlapping transactions with equal timestamps, the returned
// timestamp is not owned by either one.
func TestTimestampCacheEqualTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	forEachCacheImpl(t, func(t *testing.T, tc Cache, clock *hlc.Clock, manual *hlc.ManualClock) {
		txn1 := uuid.MakeV4()
		txn2 := uuid.MakeV4()

		// Add two non-overlapping transactions at the same timestamp.
		ts1 := clock.Now()
		tc.Add(roachpb.Key("a"), roachpb.Key("b"), ts1, txn1)
		tc.Add(roachpb.Key("b"), roachpb.Key("c"), ts1, txn2)

		// When querying either side separately, the transaction ID is returned.
		if ts, txn := tc.GetMax(roachpb.Key("a"), roachpb.Key("b")); ts != ts1 {
			t.Errorf("expected 'a'-'b' to have timestamp %s, but found %s", ts1, ts)
		} else if txn != txn1 {
			t.Errorf("expected 'a'-'b' to have txn id %s, but found %s", txn1, txn)
		}
		if ts, txn := tc.GetMax(roachpb.Key("b"), roachpb.Key("c")); ts != ts1 {
			t.Errorf("expected 'b'-'c' to have timestamp %s, but found %s", ts1, ts)
		} else if txn != txn2 {
			t.Errorf("expected 'b'-'c' to have txn id %s, but found %s", txn2, txn)
		}

		// Querying a span that overlaps both returns a nil txn ID; neither
		// can proceed here.
		if ts, txn := tc.GetMax(roachpb.Key("a"), roachpb.Key("c")); ts != ts1 {
			t.Errorf("expected 'a'-'c' to have timestamp %s, but found %s", ts1, ts)
		} else if txn != (noTxnID) {
			t.Errorf("expected 'a'-'c' to have zero txn id, but found %s", txn)
		}
	})
}

// TestTimestampCacheLargeKeys verifies that the timestamp cache implementations
// can support arbitrarily large keys lengths. This is important because we don't
// place a hard limit on this anywhere else.
func TestTimestampCacheLargeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	forEachCacheImpl(t, func(t *testing.T, tc Cache, clock *hlc.Clock, manual *hlc.ManualClock) {
		keyStart := roachpb.Key(make([]byte, 5*maximumSklPageSize))
		keyEnd := keyStart.Next()
		ts1 := clock.Now()
		txn1 := uuid.MakeV4()

		tc.Add(keyStart, keyEnd, ts1, txn1)
		if ts, txn := tc.GetMax(keyStart, keyEnd); ts != ts1 {
			t.Errorf("expected key range to have timestamp %s, but found %s", ts1, ts)
		} else if txn != txn1 {
			t.Errorf("expected key range to have txn id %s, but found %s", txn1, txn)
		}
	})
}

// TestTimestampCacheImplsIdentical verifies that all timestamp cache
// implementations return the same results for the same inputs, even under
// concurrent load.
func TestTimestampCacheImplsIdentical(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer util.EnableRacePreemptionPoints()()

	// Run one subtest using a real clock to generate timestamps and one subtest
	// using a fake clock to generate timestamps. The former is good for
	// simulating real conditions while the latter is good for testing timestamp
	// collisions.
	testutils.RunTrueAndFalse(t, "useClock", func(t *testing.T, useClock bool) {
		clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
		caches := make([]Cache, len(cacheImplConstrs))
		start := clock.Now()
		for i, constr := range cacheImplConstrs {
			tc := constr(clock)
			tc.clear(start) // set low water mark
			caches[i] = tc
		}

		// Context cancellations are used to shutdown goroutines and prevent
		// deadlocks once any test failures are found. errgroup.WithContext will
		// cancel the context either when any goroutine returns an error or when
		// all goroutines finish and Wait returns.
		doneWG, ctx := errgroup.WithContext(context.Background())

		// We run a goroutine for each slot. Goroutines insert new value over
		// random intervals, but verify that the value in their slot always
		// ratchets.
		slots := 4 * runtime.GOMAXPROCS(0)
		if util.RaceEnabled {
			// We add in a lot of preemption points when race detection
			// is enabled, so things will already be very slow. Reduce
			// the concurrency to that we don't time out.
			slots /= 2
		}

		// semC and retC force all goroutines to work in lockstep, first adding
		// intervals to all caches together, then reading from all caches
		// together.
		semC, retC := make(chan struct{}), make(chan struct{})
		go func() {
			populate := func() {
				for i := 0; i < slots; i++ {
					select {
					case semC <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
			populate()

			left := slots
			for {
				select {
				case <-retC:
					left--
					if left == 0 {
						// Reset left count and populate.
						left = slots
						populate()
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		for i := 0; i < slots; i++ {
			i := i
			doneWG.Go(func() error {
				rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
				slotKey := []byte(fmt.Sprintf("%05d", i))
				txnID := uuid.MakeV4()
				maxVal := cacheValue{}

				rounds := 1000
				if util.RaceEnabled {
					// Reduce the number of rounds for race builds.
					rounds /= 2
				}
				for j := 0; j < rounds; j++ {
					// This is a lot of log output so only un-comment to debug.
					// t.Logf("goroutine %d at iter %d", i, j)

					// Wait for all goroutines to synchronize.
					select {
					case <-semC:
					case <-ctx.Done():
						return nil
					}

					// Add the same random range to each cache.
					from, middle, to := randRange(rng, slots+1)
					if bytes.Equal(from, to) {
						to = nil
					}

					ts := start.Add(int64(j), 100).WithSynthetic(false)
					if useClock {
						ts = clock.Now()
					}

					newVal := cacheValue{ts: ts, txnID: txnID}
					for _, tc := range caches {
						// This is a lot of log output so only un-comment to debug.
						// t.Logf("adding (%T) [%s,%s) = %s", tc, string(from), string(to), newVal)
						tc.Add(from, to, ts, txnID)
					}

					// Return semaphore.
					select {
					case retC <- struct{}{}:
					case <-ctx.Done():
						return nil
					}

					// Wait for all goroutines to synchronize.
					select {
					case <-semC:
					case <-ctx.Done():
						return nil
					}

					// Check the value for the newly added interval. Should be
					// equal across all caches and be a ratcheted version of the
					// interval added above.
					var err error
					if _, err = identicalAndRatcheted(caches, from, to, newVal); err != nil {
						return errors.Wrapf(err, "interval=[%s,%s)", string(from), string(to))
					}

					// Check the value for the start key of the newly added
					// interval. Should be equal across all caches and be a
					// ratcheted version of the interval added above.
					if _, err = identicalAndRatcheted(caches, from, nil, newVal); err != nil {
						return errors.Wrapf(err, "startKey=%s", string(from))
					}

					// Check the value right after the start key of the newly
					// added interval, if possible. Should be equal across all
					// caches and be a ratcheted version of the interval added
					// above.
					if middle != nil {
						if _, err = identicalAndRatcheted(caches, middle, nil, newVal); err != nil {
							return errors.Wrapf(err, "middleKey=%s", string(middle))
						}
					}

					// Check the value for the goroutine's slot. Should be equal
					// across all caches and be a ratcheted version of the
					// maximum value we've seen in the slot.
					if maxVal, err = identicalAndRatcheted(caches, slotKey, nil, maxVal); err != nil {
						return errors.Wrapf(err, "slotKey=%s", string(slotKey))
					}

					// Return semaphore.
					select {
					case retC <- struct{}{}:
					case <-ctx.Done():
						return nil
					}
				}
				return nil
			})
		}
		if err := doneWG.Wait(); err != nil {
			t.Fatal(err)
		}
	})
}

// identicalAndRatcheted asserts that all caches have identical values for the
// specified range and that the value is a ratcheted version of previous value.
// It returns an error if the assertion fails and the value found if it doesn't.
func identicalAndRatcheted(
	caches []Cache, from, to roachpb.Key, prevVal cacheValue,
) (cacheValue, error) {
	var vals []cacheValue
	for _, tc := range caches {
		keyTS, keyTxnID := tc.GetMax(from, to)
		vals = append(vals, cacheValue{ts: keyTS, txnID: keyTxnID})
	}

	// Assert same values for each cache.
	firstVal := vals[0]
	firstCache := caches[0]
	for i := 1; i < len(caches); i++ {
		if !reflect.DeepEqual(firstVal, vals[i]) {
			return firstVal, errors.Errorf("expected %s (%T) and %s (%T) to be equal",
				firstVal, firstCache, vals[i], caches[i])
		}
	}

	// Assert that the value is a ratcheted version of prevVal.
	// See assertRatchet.
	if _, ratchet := ratchetValue(firstVal, prevVal); ratchet {
		return firstVal, errors.Errorf("ratchet inversion from %s to %s", prevVal, firstVal)
	}

	return firstVal, nil
}

func BenchmarkTimestampCacheInsertion(b *testing.B) {
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := New(clock)

	for i := 0; i < b.N; i++ {
		cdTS := clock.Now()
		tc.Add(roachpb.Key("c"), roachpb.Key("d"), cdTS, noTxnID)

		beTS := clock.Now()
		tc.Add(roachpb.Key("b"), roachpb.Key("e"), beTS, noTxnID)

		adTS := clock.Now()
		tc.Add(roachpb.Key("a"), roachpb.Key("d"), adTS, noTxnID)

		cfTS := clock.Now()
		tc.Add(roachpb.Key("c"), roachpb.Key("f"), cfTS, noTxnID)
	}
}
