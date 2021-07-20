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
		tb.update()
		if delta := tb.current - expected; delta > eps || delta < -eps {
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
}
