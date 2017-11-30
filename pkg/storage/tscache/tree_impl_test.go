// Copyright 2017 The Cockroach Authors.
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
	tc.Add(roachpb.Key("a"), nil, aTS, noTxnID, true)

	// Increment time by the MinRetentionWindow and add another key.
	manual.Increment(MinRetentionWindow.Nanoseconds())
	tc.Add(roachpb.Key("b"), nil, clock.Now(), noTxnID, true)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, rTxnID := tc.GetMaxRead(roachpb.Key("c"), nil); rTS != aTS || rTxnID != noTxnID {
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
	tc.Add(roachpb.Key("a"), nil, aTS, noTxnID, true)

	// Increment time by the MinRetentionWindow and add another key.
	manual.Increment(MinRetentionWindow.Nanoseconds())
	tc.Add(roachpb.Key("b"), nil, clock.Now(), noTxnID, true)

	// Verify that the cache still has 2 entries in it
	if l, want := tc.len(), 2; l != want {
		t.Errorf("expected %d entries to remain, got %d", want, l)
	}
}
