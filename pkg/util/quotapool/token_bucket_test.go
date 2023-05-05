// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestTokenBucket(t *testing.T) {
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := timeutil.NewManualTime(t0)

	var tb TokenBucket
	tb.Init(10, 20, mt)

	check := func(expected Tokens) {
		t.Helper()
		const eps = 1e-10
		tb.Update()
		if delta := tb.Available() - expected; delta > eps || delta < -eps {
			t.Fatalf("expected current amount %v, got %v", expected, tb.current)
		}
	}

	checkFulfill := func(amount Tokens, expected time.Duration) {
		t.Helper()
		ok, tryAgainAfter := tb.TryToFulfill(amount)
		if ok {
			if expected != 0 {
				t.Fatalf("expected not to be fulfilled")
			}
		} else {
			if expected == 0 {
				t.Fatalf("expected to be fulfilled")
			} else if tryAgainAfter.Round(time.Microsecond) != expected.Round(time.Microsecond) {
				t.Fatalf("expected tryAgainAfter %v, got %v", expected, tryAgainAfter)
			}
		}
	}

	checkExhausted := func(expDur time.Duration) {
		t.Helper()
		if got := tb.Exhausted(); got != expDur {
			t.Fatalf("expected exhausted duration %s, got %s", expDur, got)
		}
	}

	check(20)
	tb.Adjust(-10)
	check(10)
	tb.Adjust(5)
	check(15)
	tb.Adjust(20)
	check(20)

	mt.Advance(time.Second)
	check(20)
	tb.Adjust(-15)
	check(5)

	mt.Advance(time.Second)
	check(15)
	mt.Advance(time.Second)
	check(20)

	checkFulfill(15, 0)
	checkFulfill(15, time.Second)

	mt.Advance(10 * time.Second)
	// Now put the bucket into debt with a huge ask.
	checkFulfill(120, 0)
	checkFulfill(10, 11*time.Second)

	mt.Advance(100 * time.Second)

	// A full bucket should remain full.
	tb.UpdateConfig(100, 1000)
	checkFulfill(1000, 0)
	checkFulfill(100, 1*time.Second)

	tb.UpdateConfig(10, 20)
	check(-980)
	checkFulfill(20, 100*time.Second)

	// Verify that resetting the bucket resets it to the burst size.
	tb.Reset()
	check(20)

	tb.UpdateConfig(100, 100)
	tb.Reset()
	check(100)

	// Ensure that the exhaustion metric behaves as expected.
	initialExhausted := tb.Exhausted()
	// Put the token bucket into debt.
	tb.Adjust(-110)
	check(-10)
	// Advance the clock by 20ms, but it should still be in debt.
	mt.Advance(20 * time.Millisecond)
	check(-8)
	// Verify that we've accumulated this 20ms into our exhaustion value.
	checkExhausted(initialExhausted + 20*time.Millisecond)
	// Advance the clock by just enough to no longer be exhausted.
	mt.Advance(90 * time.Millisecond)
	check(1)
	// Verify that we've accumulated the 90ms into our exhaustion metric.
	checkExhausted(initialExhausted + (20+90)*time.Millisecond)
	// Add more tokens by advancing the clock.
	mt.Advance(200 * time.Millisecond)
	check(21)
	// Check that our exhaustion duration is unchanged, since we've stayed in
	// the positive.
	checkExhausted(initialExhausted + (20+90)*time.Millisecond)
}
