// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tscache

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestTreeImplEviction verifies the eviction of timestamp cache entries after
// MinRetentionWindow interval.
func TestTreeImplEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(manual)
	tc := newTreeImpl(clock)
	defer tc.clear(clock.Now())

	tc.maxBytes = 0

	// Increment time to the low water mark + 1.
	manual.Advance(1)
	aTS := clock.Now()
	tc.Add(ctx, roachpb.Key("a"), nil, aTS, noTxnID)

	// Increment time by the MinRetentionWindow and add another key.
	manual.Advance(MinRetentionWindow)
	tc.Add(ctx, roachpb.Key("b"), nil, clock.Now(), noTxnID)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, rTxnID := tc.GetMax(ctx, roachpb.Key("c"), nil); rTS != aTS || rTxnID != noTxnID {
		t.Errorf("expected low water mark %s, got %s; txnID=%s", aTS, rTS, rTxnID)
	}
}

// TestTreeImplNoEviction verifies that even after the MinRetentionWindow
// interval, if the cache has not hit its size threshold, it will not evict
// entries.
func TestTreeImplNoEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(manual)
	tc := newTreeImpl(clock)
	defer tc.clear(clock.Now())

	// Increment time to the low water mark + 1.
	manual.Advance(1)
	aTS := clock.Now()
	tc.Add(ctx, roachpb.Key("a"), nil, aTS, noTxnID)

	// Increment time by the MinRetentionWindow and add another key.
	manual.Advance(MinRetentionWindow)
	tc.Add(ctx, roachpb.Key("b"), nil, clock.Now(), noTxnID)

	// Verify that the cache still has 2 entries in it
	if l, want := tc.len(), 2; l != want {
		t.Errorf("expected %d entries to remain, got %d", want, l)
	}
}
