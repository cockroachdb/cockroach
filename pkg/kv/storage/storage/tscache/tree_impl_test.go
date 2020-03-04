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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestTreeImplEviction verifies the eviction of timestamp cache entries after
// MinRetentionWindow interval.
func TestTreeImplEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := newTreeImpl(clock)
	defer tc.clear(clock.Now())

	tc.maxBytes = 0

	// Increment time to the low water mark + 1.
	manual.Increment(1)
	aTS := clock.Now()
	tc.Add(roachpb.Key("a"), nil, aTS, noTxnID)

	// Increment time by the MinRetentionWindow and add another key.
	manual.Increment(MinRetentionWindow.Nanoseconds())
	tc.Add(roachpb.Key("b"), nil, clock.Now(), noTxnID)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, rTxnID := tc.GetMax(roachpb.Key("c"), nil); rTS != aTS || rTxnID != noTxnID {
		t.Errorf("expected low water mark %s, got %s; txnID=%s", aTS, rTS, rTxnID)
	}
}

// TestTreeImplNoEviction verifies that even after the MinRetentionWindow
// interval, if the cache has not hit its size threshold, it will not evict
// entries.
func TestTreeImplNoEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := newTreeImpl(clock)
	defer tc.clear(clock.Now())

	// Increment time to the low water mark + 1.
	manual.Increment(1)
	aTS := clock.Now()
	tc.Add(roachpb.Key("a"), nil, aTS, noTxnID)

	// Increment time by the MinRetentionWindow and add another key.
	manual.Increment(MinRetentionWindow.Nanoseconds())
	tc.Add(roachpb.Key("b"), nil, clock.Now(), noTxnID)

	// Verify that the cache still has 2 entries in it
	if l, want := tc.len(), 2; l != want {
		t.Errorf("expected %d entries to remain, got %d", want, l)
	}
}
